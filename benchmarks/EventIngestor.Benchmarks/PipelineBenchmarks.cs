using BenchmarkDotNet.Attributes;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Pipeline;
using EventIngestor.Pipeline.Sinks;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventIngestor.Benchmarks;

/// <summary>
/// Per-event cost of the channel + batching machinery alone: pre-built events, one producer,
/// N batch processors, a sink that does nothing. Results are reported per event.
/// </summary>
[MemoryDiagnoser]
public class PipelineBenchmarks
{
    private const int EventCount = 100_000;

    private IngestEvent[] _events = [];

    [Params(1, 4)]
    public int Consumers { get; set; }

    [Params(100, 1_000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void Setup() => _events = SampleData.Events(EventCount, payloadBytes: 256);

    [Benchmark(OperationsPerInvoke = EventCount)]
    public async Task ChannelToNullSink()
    {
        var options = new PipelineOptions
        {
            ChannelCapacity = 16_384,
            BatchSize = BatchSize,
            ConsumerCount = Consumers,
            FlushInterval = TimeSpan.FromMilliseconds(50),
        };

        var pipeline = new ChannelPipeline(options);
        IEventSink[] sinks = [new NullEventSink()];

        var workers = new Task[Consumers];
        for (var i = 0; i < workers.Length; i++)
        {
            workers[i] = new BatchProcessor(i, pipeline.Reader, sinks, options, NullPipelineObserver.Instance, NullLogger.Instance)
                .RunAsync(CancellationToken.None);
        }

        foreach (var evt in _events)
        {
            if (!pipeline.TryEnqueue(evt))
            {
                await pipeline.EnqueueAsync(evt);
            }
        }

        pipeline.Complete();
        await Task.WhenAll(workers);
    }
}
