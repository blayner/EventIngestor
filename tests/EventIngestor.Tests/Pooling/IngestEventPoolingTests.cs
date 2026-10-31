using System.Text;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;
using EventIngestor.Core.Serialization;
using EventIngestor.Pipeline;
using EventIngestor.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace EventIngestor.Tests.Pooling;

public class IngestEventPoolingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Reset_clears_every_field()
    {
        var evt = new IngestEvent();
        Assert.Equal(ParseStatus.Ok, EventParser.TryParse("""{"source":"a","type":"b","payload":{"x":1}}"""u8, evt));

        Assert.True(evt.TryReset());

        Assert.Equal(Guid.Empty, evt.Id);
        Assert.Equal(default, evt.Timestamp);
        Assert.Equal(string.Empty, evt.Source);
        Assert.Equal(string.Empty, evt.Type);
        Assert.True(evt.Payload.IsEmpty);
    }

    [Fact]
    public void Small_payload_buffers_are_kept_across_reuse()
    {
        var evt = new IngestEvent();
        evt.SetPayload("{\"a\":1}"u8);
        var before = evt.Payload;

        evt.TryReset();
        evt.SetPayload("{\"b\":2}"u8);

        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(before, out var first));
        Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(evt.Payload, out var second));
        Assert.Same(first.Array, second.Array);
        Assert.Equal("{\"b\":2}", Encoding.UTF8.GetString(evt.Payload.Span));
    }

    [Fact]
    public void Generated_ids_are_time_ordered()
    {
        var evt = new IngestEvent();
        evt.EnsureDefaults(DateTimeOffset.UtcNow);

        Assert.Equal(7, evt.Id.Version);
    }

    [Fact]
    public void Parser_interns_source_and_type_through_the_cache()
    {
        var cache = new Utf8StringCache();
        var a = new IngestEvent();
        var b = new IngestEvent();

        EventParser.TryParse("""{"source":"billing","type":"invoice.paid"}"""u8, a, stringCache: cache);
        EventParser.TryParse("""{"source":"billing","type":"invoice.paid"}"""u8, b, stringCache: cache);

        Assert.Same(a.Source, b.Source);
        Assert.Same(a.Type, b.Type);
    }

    [Fact]
    public async Task Batch_processor_returns_flushed_events_to_the_pool()
    {
        var pool = new LockFreeObjectPool<IngestEvent>(() => new IngestEvent(), maxRetained: 100);
        var options = new PipelineOptions { BatchSize = 10, ConsumerCount = 1 };
        var pipeline = new ChannelPipeline(options);
        var processor = new BatchProcessor(0, pipeline.Reader, [new CollectingSink()], options, NullPipelineObserver.Instance, NullLogger.Instance, pool);

        for (var i = 0; i < 10; i++)
        {
            var evt = pool.Rent();
            evt.Source = "s";
            evt.Type = "t";
            pipeline.TryEnqueue(evt);
        }

        pipeline.Complete();
        await processor.RunAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Equal(10, pool.Retained);
        Assert.Equal(10, pool.Created);
    }

    [Fact]
    public async Task Throttled_events_go_back_to_the_pool()
    {
        var pool = new LockFreeObjectPool<IngestEvent>(() => new IngestEvent(), maxRetained: 100);
        var options = new PipelineOptions { ChannelCapacity = 1, ConsumerCount = 1, EnqueueTimeout = TimeSpan.Zero };
        var writer = new PipelineWriter(new ChannelPipeline(options), options, new RecordingObserver(), pool);
        var events = Enumerable.Range(0, 3).Select(_ => writer.Rent()).ToList();

        var result = await writer.WriteAsync(events, Transports.Http, Ct);

        Assert.Equal(new IngestResult(1, 0, 2), result);
        Assert.Equal(2, pool.Retained);
    }
}
