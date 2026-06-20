using System.IO.Pipelines;
using System.Text;
using BenchmarkDotNet.Attributes;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;
using EventIngestor.Core.Serialization;
using EventIngestor.Pipeline;

namespace EventIngestor.Benchmarks;

/// <summary>
/// NDJSON body to events-in-the-channel, per line:
/// the "obvious" StreamReader.ReadLine + UTF-16 string + re-encode approach versus
/// <see cref="NdjsonPipelineReader"/> (PipeReader + SequenceReader, in-place parsing, pooled events).
/// </summary>
[MemoryDiagnoser]
public class ParserBenchmarks
{
    private const int Lines = 10_000;

    private byte[] _ndjson = [];
    private ChannelPipeline _pipeline = null!;
    private LockFreeObjectPool<IngestEvent> _pool = null!;
    private NdjsonPipelineReader _reader = null!;

    [Params(256, 2048)]
    public int PayloadBytes { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _ndjson = SampleData.Ndjson(Lines, PayloadBytes);

        var options = new PipelineOptions { ChannelCapacity = Lines * 2, ConsumerCount = 1 };
        _pipeline = new ChannelPipeline(options);
        _pool = new LockFreeObjectPool<IngestEvent>(() => new IngestEvent(), maxRetained: Lines * 2);
        var writer = new PipelineWriter(_pipeline, options, NullPipelineObserver.Instance, _pool);
        _reader = new NdjsonPipelineReader(writer, options, new Utf8StringCache());
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Lines)]
    public async Task<int> StreamReader_ReadLine()
    {
        using var stream = new MemoryStream(_ndjson, writable: false);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        var accepted = 0;
        while (await reader.ReadLineAsync() is { } line)
        {
            if (line.Length == 0)
            {
                continue;
            }

            var evt = new IngestEvent();
            if (EventParser.TryParse(Encoding.UTF8.GetBytes(line), evt) == ParseStatus.Ok && _pipeline.TryEnqueue(evt))
            {
                accepted++;
            }
        }

        Drain(returnToPool: false);
        return accepted;
    }

    [Benchmark(OperationsPerInvoke = Lines)]
    public async Task<int> PipeReader_SequenceReader()
    {
        using var stream = new MemoryStream(_ndjson, writable: false);
        var pipeReader = PipeReader.Create(stream, new StreamPipeReaderOptions(bufferSize: 64 * 1024));

        var result = await _reader.ReadAsync(pipeReader);
        await pipeReader.CompleteAsync();

        Drain(returnToPool: true);
        return result.Accepted;
    }

    /// <summary>Stands in for the batch processors so the channel never fills up between iterations.</summary>
    private void Drain(bool returnToPool)
    {
        while (_pipeline.Reader.TryRead(out var evt))
        {
            if (returnToPool)
            {
                _pool.Return(evt);
            }
        }
    }
}
