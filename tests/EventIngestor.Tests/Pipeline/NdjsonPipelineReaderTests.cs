using System.IO.Pipelines;
using System.Text;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Pipeline;
using EventIngestor.Tests.TestDoubles;

namespace EventIngestor.Tests.Pipeline;

public class NdjsonPipelineReaderTests
{
    private static (NdjsonPipelineReader Reader, ChannelPipeline Pipeline, RecordingObserver Observer) Create(PipelineOptions? options = null)
    {
        options ??= new PipelineOptions { ChannelCapacity = 10_000, ConsumerCount = 1 };
        var pipeline = new ChannelPipeline(options);
        var observer = new RecordingObserver();
        var writer = new PipelineWriter(pipeline, options, observer);
        return (new NdjsonPipelineReader(writer, options), pipeline, observer);
    }

    private static List<IngestEvent> Drain(ChannelPipeline pipeline)
    {
        var events = new List<IngestEvent>();
        while (pipeline.Reader.TryRead(out var evt))
        {
            events.Add(evt);
        }

        return events;
    }

    private static string Line(int n, string payload = "{\"n\":0}") =>
        $$"""{"source":"svc","type":"evt.{{n}}","payload":{{payload}}}""";

    /// <summary>Feeds <paramref name="body"/> through a pipe with tiny segments so lines straddle segment boundaries.</summary>
    private static async Task<IngestResult> FeedInChunksAsync(NdjsonPipelineReader reader, string body, int chunkSize)
    {
        var pipe = new Pipe(new PipeOptions(minimumSegmentSize: 16));
        var read = reader.ReadAsync(pipe.Reader).AsTask();

        var bytes = Encoding.UTF8.GetBytes(body);
        for (var offset = 0; offset < bytes.Length; offset += chunkSize)
        {
            var size = Math.Min(chunkSize, bytes.Length - offset);
            await pipe.Writer.WriteAsync(bytes.AsMemory(offset, size));
        }

        await pipe.Writer.CompleteAsync();
        return await read.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Parses_lf_crlf_and_a_final_line_without_newline()
    {
        var (reader, pipeline, _) = Create();
        var body = Line(1) + "\n" + Line(2) + "\r\n" + Line(3) + "\n" + Line(4);

        var result = await reader.ReadAsync(PipeReader.Create(new MemoryStream(Encoding.UTF8.GetBytes(body))));

        Assert.Equal(new IngestResult(4, 0, 0), result);
        Assert.Equal(new[] { "evt.1", "evt.2", "evt.3", "evt.4" }, Drain(pipeline).Select(e => e.Type));
    }

    [Fact]
    public async Task Handles_lines_split_across_many_segments()
    {
        var (reader, pipeline, _) = Create();
        var lines = Enumerable.Range(0, 50).Select(i => Line(i, $$"""{"data":"{{new string('x', i * 10)}}"}""")).ToArray();

        var result = await FeedInChunksAsync(reader, string.Join("\n", lines) + "\n", chunkSize: 7);

        Assert.Equal(50, result.Accepted);
        var events = Drain(pipeline);
        Assert.Equal(lines.Length, events.Count);
        Assert.Equal($$"""{"data":"{{new string('x', 490)}}"}""", Encoding.UTF8.GetString(events[^1].Payload.Span));
    }

    [Fact]
    public async Task Invalid_lines_are_rejected_without_failing_the_stream()
    {
        var (reader, pipeline, observer) = Create();
        var body = string.Join("\n", Line(1), "{not json", Line(2), """{"source":"missing-type"}""") + "\n";

        var result = await reader.ReadAsync(PipeReader.Create(new MemoryStream(Encoding.UTF8.GetBytes(body))));

        Assert.Equal(new IngestResult(2, 2, 0), result);
        Assert.Equal(2, observer.RejectedCount(RejectReason.InvalidEvent));
        Assert.Equal(2, Drain(pipeline).Count);
    }

    [Fact]
    public async Task Blank_lines_are_ignored()
    {
        var (reader, _, _) = Create();
        var body = "\n\n" + Line(1) + "\n   \n\t\r\n" + Line(2) + "\n\n";

        var result = await reader.ReadAsync(PipeReader.Create(new MemoryStream(Encoding.UTF8.GetBytes(body))));

        Assert.Equal(new IngestResult(2, 0, 0), result);
    }

    [Fact]
    public async Task Oversized_line_is_skipped_and_parsing_resumes_after_it()
    {
        var options = new PipelineOptions { ChannelCapacity = 100, ConsumerCount = 1, MaxPayloadBytes = 64, MaxLineBytes = 128 };
        var (reader, pipeline, observer) = Create(options);
        var huge = Line(2, $$"""{"blob":"{{new string('z', 400)}}"}""");
        var body = Line(1) + "\n" + huge + "\n" + Line(3) + "\n";

        var result = await FeedInChunksAsync(reader, body, chunkSize: 32);

        Assert.Equal(new IngestResult(2, 1, 0), result);
        Assert.Equal(1, observer.RejectedCount(RejectReason.PayloadTooLarge));
        Assert.Equal(new[] { "evt.1", "evt.3" }, Drain(pipeline).Select(e => e.Type));
    }

    [Fact]
    public async Task Reports_throttled_events_when_the_pipeline_is_full()
    {
        var options = new PipelineOptions { ChannelCapacity = 2, ConsumerCount = 1, EnqueueTimeout = TimeSpan.Zero };
        var (reader, _, observer) = Create(options);
        var body = string.Join("\n", Enumerable.Range(0, 5).Select(i => Line(i)));

        var result = await reader.ReadAsync(PipeReader.Create(new MemoryStream(Encoding.UTF8.GetBytes(body))));

        Assert.Equal(new IngestResult(2, 0, 3), result);
        Assert.Equal(3, observer.RejectedCount(RejectReason.Backpressure));
    }
}
