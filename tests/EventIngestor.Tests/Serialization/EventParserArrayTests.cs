using System.Text;
using EventIngestor.Core.Models;
using EventIngestor.Core.Serialization;

namespace EventIngestor.Tests.Serialization;

public class EventParserArrayTests
{
    private static (ParseStatus Status, List<IngestEvent> Accepted, List<ParseStatus> Rejected) Parse(string json, int maxPayloadBytes = EventLimits.DefaultMaxPayloadBytes)
    {
        var accepted = new List<IngestEvent>();
        var rejected = new List<ParseStatus>();
        var status = EventParser.ParseArray(
            Encoding.UTF8.GetBytes(json),
            () => new IngestEvent(),
            (_, reason) => rejected.Add(reason),
            accepted,
            maxPayloadBytes);
        return (status, accepted, rejected);
    }

    [Fact]
    public void Parses_every_element()
    {
        var (status, accepted, rejected) = Parse("""[{"source":"a","type":"t1"},{"source":"b","type":"t2","payload":[1,2]}]""");

        Assert.Equal(ParseStatus.Ok, status);
        Assert.Equal(new[] { "t1", "t2" }, accepted.Select(e => e.Type));
        Assert.Equal("[1,2]", Encoding.UTF8.GetString(accepted[1].Payload.Span));
        Assert.Empty(rejected);
    }

    [Fact]
    public void Empty_array_is_valid()
    {
        var (status, accepted, _) = Parse("[]");

        Assert.Equal(ParseStatus.Ok, status);
        Assert.Empty(accepted);
    }

    [Fact]
    public void Invalid_elements_are_skipped_wherever_the_failure_happened()
    {
        // Failures at the start (bad id), in a nested value (source is an object), at the end
        // (missing type), non-object elements, and an oversized payload.
        const string json = """
            [
              {"id":"bad","source":"a","type":"t","payload":{"deep":{"x":[1,2]}}},
              {"source":{"nested":{"k":"v"}},"type":"t"},
              {"source":"a"},
              42,
              [1,[2]],
              {"source":"a","type":"t","payload":{"blob":"0123456789012345678901234567890123456789"}},
              {"source":"ok","type":"last"}
            ]
            """;

        var (status, accepted, rejected) = Parse(json, maxPayloadBytes: 32);

        Assert.Equal(ParseStatus.Ok, status);
        Assert.Equal("last", Assert.Single(accepted).Type);
        Assert.Equal(
            new[] { ParseStatus.InvalidField, ParseStatus.InvalidField, ParseStatus.MissingType, ParseStatus.InvalidJson, ParseStatus.InvalidJson, ParseStatus.PayloadTooLarge },
            rejected);
    }

    [Theory]
    [InlineData("""{"source":"a","type":"b"}""")]
    [InlineData("""[{"source":"a","type":"b"}""")]
    [InlineData("""[{"source":"a","type":"b"}] []""")]
    [InlineData("")]
    public void Malformed_arrays_are_reported(string json)
    {
        Assert.Equal(ParseStatus.InvalidJson, Parse(json).Status);
    }
}
