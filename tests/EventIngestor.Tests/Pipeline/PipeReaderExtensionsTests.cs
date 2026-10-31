using System.IO.Pipelines;
using System.Text;
using EventIngestor.Pipeline;

namespace EventIngestor.Tests.Pipeline;

public class PipeReaderExtensionsTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Reads_a_body_written_in_several_chunks()
    {
        var pipe = new Pipe(new PipeOptions(minimumSegmentSize: 16));
        var read = pipe.Reader.ReadToEndAsync(maxBytes: 1024, Ct).AsTask();

        foreach (var part in new[] { "{\"source\":", "\"a\",\"type\"", ":\"b\"}" })
        {
            await pipe.Writer.WriteAsync(Encoding.UTF8.GetBytes(part), Ct);
        }

        await pipe.Writer.CompleteAsync();
        using var body = await read;

        Assert.False(body.IsTooLarge);
        Assert.Equal("{\"source\":\"a\",\"type\":\"b\"}", Encoding.UTF8.GetString(body.Span));
    }

    [Fact]
    public async Task Stops_reading_once_the_limit_is_exceeded()
    {
        var reader = PipeReader.Create(new MemoryStream(new byte[4096]));

        using var body = await reader.ReadToEndAsync(maxBytes: 100, Ct);

        Assert.True(body.IsTooLarge);
        Assert.Equal(0, body.Length);
    }

    [Fact]
    public async Task Empty_body_yields_an_empty_buffer()
    {
        using var body = await PipeReader.Create(new MemoryStream()).ReadToEndAsync(maxBytes: 100, Ct);

        Assert.False(body.IsTooLarge);
        Assert.True(body.Span.IsEmpty);
    }
}
