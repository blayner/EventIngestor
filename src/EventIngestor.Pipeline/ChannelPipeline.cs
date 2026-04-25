using System.Threading.Channels;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;

namespace EventIngestor.Pipeline;

/// <summary>
/// <see cref="IEventPipeline"/> backed by a bounded <see cref="Channel{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="BoundedChannelFullMode.Wait"/> is used on purpose: <c>TryWrite</c> fails fast when the
/// buffer is full, which gives the transports an explicit backpressure signal they can turn into
/// a 429 / RESOURCE_EXHAUSTED. The drop modes would silently lose data (and, once events are pooled,
/// leak pooled instances).
/// </para>
/// <para>
/// Continuations run asynchronously so a producer completing a write never ends up executing
/// consumer code (and flush I/O) on a request thread.
/// </para>
/// </remarks>
public sealed class ChannelPipeline : IEventPipeline
{
    private readonly Channel<IngestEvent> _channel;

    public ChannelPipeline(PipelineOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        Capacity = options.ChannelCapacity;
        _channel = Channel.CreateBounded<IngestEvent>(new BoundedChannelOptions(options.ChannelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = options.ConsumerCount == 1,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });
    }

    public int Capacity { get; }

    public int Count => _channel.Reader.Count;

    public ChannelReader<IngestEvent> Reader => _channel.Reader;

    public Task Completion => _channel.Reader.Completion;

    public bool TryEnqueue(IngestEvent evt) => _channel.Writer.TryWrite(evt);

    public async ValueTask<bool> EnqueueAsync(IngestEvent evt, CancellationToken cancellationToken = default)
    {
        // WaitToWriteAsync + TryWrite instead of WriteAsync: WriteAsync throws
        // ChannelClosedException on completion, this returns false instead.
        while (await _channel.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_channel.Writer.TryWrite(evt))
            {
                return true;
            }
        }

        return false;
    }

    public void Complete(Exception? error = null) => _channel.Writer.TryComplete(error);
}
