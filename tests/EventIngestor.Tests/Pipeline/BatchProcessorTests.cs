using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Pipeline;
using EventIngestor.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventIngestor.Tests.Pipeline;

public class BatchProcessorTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static IngestEvent NewEvent() => new() { Id = Guid.NewGuid(), Source = "test", Type = "t" };

    private static (ChannelPipeline Pipeline, BatchProcessor Processor) Create(
        PipelineOptions options,
        IEventSink sink,
        IPipelineObserver? observer = null)
    {
        var pipeline = new ChannelPipeline(options);
        var processor = new BatchProcessor(0, pipeline.Reader, [sink], options, observer ?? NullPipelineObserver.Instance, NullLogger.Instance);
        return (pipeline, processor);
    }

    [Fact]
    public async Task Flushes_full_batches_and_the_remainder_on_completion()
    {
        var sink = new CollectingSink();
        var (pipeline, processor) = Create(new PipelineOptions { BatchSize = 10, FlushInterval = TimeSpan.FromSeconds(30), ConsumerCount = 1 }, sink);

        for (var i = 0; i < 25; i++)
        {
            Assert.True(pipeline.TryEnqueue(NewEvent()));
        }

        var run = processor.RunAsync(CancellationToken.None);
        await Eventually.True(() => sink.Batches.Count == 2, because: "two full batches should flush without waiting for the interval");

        pipeline.Complete();
        await run.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Equal(new[] { 10, 10, 5 }, sink.Batches.Select(b => b.Ids.Length));
        Assert.Equal(new[] { 1L, 2L, 3L }, sink.Batches.Select(b => b.Sequence));
    }

    [Fact]
    public async Task Flushes_a_partial_batch_once_the_interval_elapses()
    {
        var sink = new CollectingSink();
        var (pipeline, processor) = Create(new PipelineOptions { BatchSize = 1_000, FlushInterval = TimeSpan.FromMilliseconds(50), ConsumerCount = 1 }, sink);
        using var cts = new CancellationTokenSource();
        var run = processor.RunAsync(cts.Token);

        pipeline.TryEnqueue(NewEvent());
        pipeline.TryEnqueue(NewEvent());
        pipeline.TryEnqueue(NewEvent());

        await Eventually.True(() => sink.EventCount == 3, TimeSpan.FromSeconds(2));
        Assert.Single(sink.Batches);

        cts.Cancel();
        await run.WaitAsync(TimeSpan.FromSeconds(5), Ct);
    }

    [Fact]
    public async Task Retries_a_failing_sink_until_it_succeeds()
    {
        var sink = new FlakySink(failures: 2);
        var observer = new RecordingObserver();
        var options = new PipelineOptions { BatchSize = 5, ConsumerCount = 1, MaxFlushRetries = 3, RetryBaseDelay = TimeSpan.FromMilliseconds(1) };
        var (pipeline, processor) = Create(options, sink, observer);

        for (var i = 0; i < 5; i++)
        {
            pipeline.TryEnqueue(NewEvent());
        }

        pipeline.Complete();
        await processor.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Equal(5, sink.Inner.EventCount);
        Assert.Equal(2, observer.BatchFailures);
        Assert.Equal(1, observer.BatchesFlushed);
        Assert.Equal(0, observer.BatchesDropped);
    }

    [Fact]
    public async Task Drops_the_batch_after_exhausting_retries_and_keeps_going()
    {
        var sink = new FlakySink(failures: int.MaxValue);
        var observer = new RecordingObserver();
        var options = new PipelineOptions { BatchSize = 2, ConsumerCount = 1, MaxFlushRetries = 1, RetryBaseDelay = TimeSpan.FromMilliseconds(1) };
        var (pipeline, processor) = Create(options, sink, observer);

        for (var i = 0; i < 4; i++)
        {
            pipeline.TryEnqueue(NewEvent());
        }

        pipeline.Complete();
        await processor.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Equal(2, observer.BatchesDropped);
        Assert.Equal(4, sink.Calls); // 2 batches x (1 attempt + 1 retry)
    }

    [Fact]
    public async Task Drains_buffered_events_when_cancelled()
    {
        var sink = new CollectingSink();
        var options = new PipelineOptions { BatchSize = 100, FlushInterval = TimeSpan.FromSeconds(30), ConsumerCount = 1 };
        var (pipeline, processor) = Create(options, sink);
        using var cts = new CancellationTokenSource();

        var run = processor.RunAsync(cts.Token);
        for (var i = 0; i < 42; i++)
        {
            pipeline.TryEnqueue(NewEvent());
        }

        await Task.Delay(50, Ct);
        cts.Cancel();
        await run.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Equal(42, sink.EventCount);
    }

    [Fact]
    public async Task Competing_processors_deliver_every_event_exactly_once()
    {
        const int eventCount = 10_000;
        var sink = new CollectingSink();
        var options = new PipelineOptions { ChannelCapacity = 1_024, BatchSize = 64, ConsumerCount = 4 };
        var pipeline = new ChannelPipeline(options);
        var runs = Enumerable.Range(0, options.ConsumerCount)
            .Select(i => new BatchProcessor(i, pipeline.Reader, [sink], options, NullPipelineObserver.Instance, NullLogger.Instance)
                .RunAsync(CancellationToken.None))
            .ToArray();

        var expected = new HashSet<Guid>();
        for (var i = 0; i < eventCount; i++)
        {
            var evt = NewEvent();
            expected.Add(evt.Id);
            Assert.True(await pipeline.EnqueueAsync(evt, Ct));
        }

        pipeline.Complete();
        await Task.WhenAll(runs).WaitAsync(TimeSpan.FromSeconds(10), Ct);

        var delivered = sink.AllIds.ToList();
        Assert.Equal(eventCount, delivered.Count);
        Assert.True(expected.SetEquals(delivered));
    }
}
