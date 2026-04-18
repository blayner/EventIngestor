using System.Text;
using EventIngestor.Core.Models;
using EventIngestor.Core.Serialization;

namespace EventIngestor.Tests.Serialization;

public class EventParserTests
{
    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);

    [Fact]
    public void Parses_all_known_fields()
    {
        var json = Utf8("""{"id":"3f2b8c1e-5d4a-4f6b-9c7d-1e2f3a4b5c6d","timestamp":"2026-04-18T10:00:00Z","source":"checkout-api","type":"order.created","payload":{"orderId":42,"items":[1,2]}}""");
        var evt = new IngestEvent();

        var status = EventParser.TryParse(json, evt);

        Assert.Equal(ParseStatus.Ok, status);
        Assert.Equal(Guid.Parse("3f2b8c1e-5d4a-4f6b-9c7d-1e2f3a4b5c6d"), evt.Id);
        Assert.Equal(new DateTimeOffset(2026, 4, 18, 10, 0, 0, TimeSpan.Zero), evt.Timestamp);
        Assert.Equal("checkout-api", evt.Source);
        Assert.Equal("order.created", evt.Type);
        Assert.Equal("""{"orderId":42,"items":[1,2]}""", Encoding.UTF8.GetString(evt.Payload.Span));
        Assert.NotEqual(default, evt.ReceivedAt);
    }

    [Fact]
    public void Payload_is_captured_verbatim_including_inner_whitespace()
    {
        var json = Utf8("""{"source":"a","type":"b","payload": { "nested" : { "x" : [1, 2 ,3] } } }""");
        var evt = new IngestEvent();

        Assert.Equal(ParseStatus.Ok, EventParser.TryParse(json, evt));
        Assert.Equal("""{ "nested" : { "x" : [1, 2 ,3] } }""", Encoding.UTF8.GetString(evt.Payload.Span));
    }

    [Theory]
    [InlineData("\"plain text\"")]
    [InlineData("12345")]
    [InlineData("true")]
    [InlineData("[1,{\"a\":2}]")]
    public void Primitive_and_array_payloads_are_supported(string payload)
    {
        var json = Utf8($$"""{"source":"a","type":"b","payload":{{payload}}}""");
        var evt = new IngestEvent();

        Assert.Equal(ParseStatus.Ok, EventParser.TryParse(json, evt));
        Assert.Equal(payload, Encoding.UTF8.GetString(evt.Payload.Span));
    }

    [Fact]
    public void Null_payload_results_in_empty_payload()
    {
        var evt = new IngestEvent();

        Assert.Equal(ParseStatus.Ok, EventParser.TryParse(Utf8("""{"source":"a","type":"b","payload":null}"""), evt));
        Assert.True(evt.Payload.IsEmpty);
    }

    [Fact]
    public void Generates_id_and_timestamp_when_absent()
    {
        var evt = new IngestEvent();

        Assert.Equal(ParseStatus.Ok, EventParser.TryParse(Utf8("""{"source":"a","type":"b"}"""), evt));
        Assert.NotEqual(Guid.Empty, evt.Id);
        Assert.Equal(evt.ReceivedAt, evt.Timestamp);
    }

    [Fact]
    public void Accepts_unix_millisecond_timestamps()
    {
        var evt = new IngestEvent();

        Assert.Equal(ParseStatus.Ok, EventParser.TryParse(Utf8("""{"source":"a","type":"b","timestamp":1776506400000}"""), evt));
        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1776506400000), evt.Timestamp);
    }

    [Fact]
    public void Unknown_properties_are_ignored_whatever_their_shape()
    {
        var json = Utf8("""{"extra":{"deep":[{"a":1}]},"source":"a","flag":true,"type":"b","list":[1,2,3]}""");
        var evt = new IngestEvent();

        Assert.Equal(ParseStatus.Ok, EventParser.TryParse(json, evt));
        Assert.Equal("a", evt.Source);
        Assert.Equal("b", evt.Type);
    }

    [Theory]
    [InlineData("""{"type":"b"}""", ParseStatus.MissingSource)]
    [InlineData("""{"source":"a"}""", ParseStatus.MissingType)]
    [InlineData("""{"source":"","type":"b"}""", ParseStatus.InvalidField)]
    [InlineData("""{"source":42,"type":"b"}""", ParseStatus.InvalidField)]
    [InlineData("""{"source":"a","type":"b","id":"not-a-guid"}""", ParseStatus.InvalidField)]
    [InlineData("""{"source":"a","type":"b","timestamp":"yesterday"}""", ParseStatus.InvalidField)]
    [InlineData("""{"source":"a","type":"b","timestamp":-1}""", ParseStatus.InvalidField)]
    [InlineData("""{"source":"a",""", ParseStatus.InvalidJson)]
    [InlineData("""[{"source":"a","type":"b"}]""", ParseStatus.InvalidJson)]
    [InlineData("""{"source":"a","type":"b"} {"source":"c","type":"d"}""", ParseStatus.InvalidJson)]
    [InlineData("", ParseStatus.InvalidJson)]
    public void Reports_precise_status_for_invalid_input(string json, ParseStatus expected)
    {
        Assert.Equal(expected, EventParser.TryParse(Utf8(json), new IngestEvent()));
    }

    [Fact]
    public void Rejects_payload_over_the_limit()
    {
        var json = Utf8("""{"source":"a","type":"b","payload":{"data":"0123456789"}}""");

        Assert.Equal(ParseStatus.PayloadTooLarge, EventParser.TryParse(json, new IngestEvent(), maxPayloadBytes: 10));
    }

    [Fact]
    public void Rejects_names_longer_than_the_limit()
    {
        var longSource = new string('s', EventLimits.MaxNameLength + 1);
        var json = Utf8($$"""{"source":"{{longSource}}","type":"b"}""");

        Assert.Equal(ParseStatus.InvalidField, EventParser.TryParse(json, new IngestEvent()));
    }

    [Theory]
    [InlineData("{\"a\":1}", true)]
    [InlineData("[1,2]", true)]
    [InlineData("\"x\"", true)]
    [InlineData("{\"a\":", false)]
    [InlineData("{} {}", false)]
    [InlineData("", false)]
    public void IsValidJsonValue_detects_single_well_formed_values(string json, bool expected)
    {
        Assert.Equal(expected, EventParser.IsValidJsonValue(Utf8(json)));
    }
}
