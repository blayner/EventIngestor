using EventIngestor.Core.Models;
using EventIngestor.Pipeline;

namespace EventIngestor.Tests.Pipeline;

public class ChannelPipelineTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static IngestEvent NewEvent() => new() { Source = "test", Type = "t" };

    [Fact]
    public void TryEnqueue_fails_fast_when_the_buffer_is_full()
    {
        var pipeline = new ChannelPipeline(new PipelineOptions { ChannelCapacity = 2, ConsumerCount = 1 });

        Assert.True(pipeline.TryEnqueue(NewEvent()));
        Assert.True(pipeline.TryEnqueue(NewEvent()));
        Assert.False(pipeline.TryEnqueue(NewEvent()));
        Assert.Equal(2, pipeline.Count);
    }

    [Fact]
    public async Task EnqueueAsync_waits_until_a_consumer_frees_space()
    {
        var pipeline = new ChannelPipeline(new PipelineOptions { ChannelCapacity = 1, ConsumerCount = 1 });
        Assert.True(pipeline.TryEnqueue(NewEvent()));

        var pending = pipeline.EnqueueAsync(NewEvent(), Ct).AsTask();
        await Task.Delay(50, Ct);
        Assert.False(pending.IsCompleted);

        Assert.True(pipeline.Reader.TryRead(out _));

        Assert.True(await pending.WaitAsync(TimeSpan.FromSeconds(5), Ct));
        Assert.Equal(1, pipeline.Count);
    }

    [Fact]
    public async Task EnqueueAsync_returns_false_once_completed()
    {
        var pipeline = new ChannelPipeline(new PipelineOptions { ChannelCapacity = 1, ConsumerCount = 1 });
        Assert.True(pipeline.TryEnqueue(NewEvent()));

        var pending = pipeline.EnqueueAsync(NewEvent(), Ct).AsTask();
        pipeline.Complete();

        Assert.False(await pending.WaitAsync(TimeSpan.FromSeconds(5), Ct));
        Assert.False(pipeline.TryEnqueue(NewEvent()));
    }

    [Fact]
    public async Task EnqueueAsync_honours_cancellation()
    {
        var pipeline = new ChannelPipeline(new PipelineOptions { ChannelCapacity = 1, ConsumerCount = 1 });
        Assert.True(pipeline.TryEnqueue(NewEvent()));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pipeline.EnqueueAsync(NewEvent(), cts.Token).AsTask());
    }

    [Fact]
    public async Task Buffered_events_are_still_readable_after_completion()
    {
        var pipeline = new ChannelPipeline(new PipelineOptions { ChannelCapacity = 10, ConsumerCount = 1 });
        pipeline.TryEnqueue(NewEvent());
        pipeline.TryEnqueue(NewEvent());
        pipeline.Complete();

        var read = 0;
        await foreach (var _ in pipeline.Reader.ReadAllAsync(Ct))
        {
            read++;
        }

        Assert.Equal(2, read);
        Assert.True(pipeline.Completion.IsCompletedSuccessfully);
    }

    [Theory]
    [InlineData(0, 100, 1)]
    [InlineData(100, 0, 1)]
    [InlineData(100, 100, 0)]
    public void Rejects_invalid_options(int capacity, int batchSize, int consumers)
    {
        var options = new PipelineOptions { ChannelCapacity = capacity, BatchSize = batchSize, ConsumerCount = consumers };

        Assert.Throws<ArgumentException>(() => new ChannelPipeline(options));
    }
}
