using System.Buffers;
using System.Text.Json;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Serialization;
using Microsoft.Extensions.Logging;

namespace EventIngestor.Pipeline.Sinks;

/// <summary>
/// Appends batches as NDJSON to size-rotated files.
/// </summary>
/// <remarks>
/// Each batch is serialized into a reusable buffer and written with a single
/// <see cref="FileStream.WriteAsync(ReadOnlyMemory{byte}, CancellationToken)"/> call, so the
/// FileStream's own buffer is disabled. Writers are serialized with a semaphore because several
/// batch processors share the sink.
/// </remarks>
public sealed partial class FileEventSink : IEventSink, IAsyncDisposable
{
    private readonly FileSinkOptions _options;
    private readonly ILogger<FileEventSink> _logger;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ArrayBufferWriter<byte> _buffer = new(64 * 1024);
    private readonly Utf8JsonWriter _jsonWriter;

    private FileStream? _stream;
    private int _fileCounter;
    private bool _disposed;

    public FileEventSink(FileSinkOptions options, ILogger<FileEventSink> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _options = options;
        _logger = logger;
        _jsonWriter = new Utf8JsonWriter(_buffer);
        Directory.CreateDirectory(options.OutputDirectory);
    }

    public string Name => "file";

    public string? CurrentFile => _stream?.Name;

    public async ValueTask WriteBatchAsync(EventBatch batch, CancellationToken cancellationToken)
    {
        if (batch.IsEmpty)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _buffer.ResetWrittenCount();
            EventSerializer.WriteNdjson(_buffer, _jsonWriter, batch.Span);

            var stream = _stream ??= OpenNewFile();
            await stream.WriteAsync(_buffer.WrittenMemory, cancellationToken).ConfigureAwait(false);

            if (_options.FlushToDisk)
            {
                stream.Flush(flushToDisk: true);
            }

            if (stream.Length >= _options.MaxFileBytes)
            {
                LogRotated(_logger, stream.Name, stream.Length);
                await stream.DisposeAsync().ConfigureAwait(false);
                _stream = null;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (_stream is not null)
            {
                await _stream.DisposeAsync().ConfigureAwait(false);
                _stream = null;
            }

            await _jsonWriter.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    private FileStream OpenNewFile()
    {
        var counter = Interlocked.Increment(ref _fileCounter);
        var fileName = $"events-{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Environment.ProcessId}-{counter:D4}.ndjson";
        var path = Path.Combine(_options.OutputDirectory, fileName);

        LogOpened(_logger, path);
        return new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            Share = FileShare.Read,
            BufferSize = 0,
            Options = FileOptions.Asynchronous | FileOptions.SequentialScan,
        });
    }

    [LoggerMessage(EventId = 3001, Level = LogLevel.Information, Message = "File sink writing to {Path}")]
    private static partial void LogOpened(ILogger logger, string path);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Information, Message = "Rotating {Path} at {Bytes} bytes")]
    private static partial void LogRotated(ILogger logger, string path, long bytes);
}
