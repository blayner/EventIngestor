using System.Buffers;
using System.Text;
using System.Text.Json;
using EventIngestor.Core.Models;
using EventIngestor.Core.Serialization;

namespace EventIngestor.Tests.Serialization;

public class EventSerializerTests
{
    private static IngestEvent Parse(string json)
    {
        var evt = new IngestEvent();
        Assert.Equal(ParseStatus.Ok, EventParser.TryParse(Encoding.UTF8.GetBytes(json), evt));
        return evt;
    }

    [Fact]
    public void Round_trip_preserves_fields_and_payload_bytes()
    {
        var original = Parse("""{"id":"0b8f8a7e-2c1d-4e3f-8a9b-0c1d2e3f4a5b","timestamp":"2026-04-18T10:00:00+00:00","source":"billing","type":"invoice.paid","payload":{"amount":199.9,"currency":"BRL"}}""");

        var json = EventSerializer.SerializeToUtf8Bytes(original);
        var copy = new IngestEvent();

        Assert.Equal(ParseStatus.Ok, EventParser.TryParse(json, copy));
        Assert.Equal(original.Id, copy.Id);
        Assert.Equal(original.Timestamp, copy.Timestamp);
        Assert.Equal(original.Source, copy.Source);
        Assert.Equal(original.Type, copy.Type);
        Assert.True(original.Payload.Span.SequenceEqual(copy.Payload.Span));
    }

    [Fact]
    public void Empty_payload_is_written_as_null()
    {
        var evt = Parse("""{"source":"a","type":"b"}""");

        using var doc = JsonDocument.Parse(EventSerializer.SerializeToUtf8Bytes(evt));

        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("payload").ValueKind);
    }

    [Fact]
    public void WriteNdjson_emits_one_line_per_event_with_a_reused_writer()
    {
        var events = new[]
        {
            Parse("""{"source":"a","type":"t1","payload":{"n":1}}"""),
            Parse("""{"source":"b","type":"t2","payload":{"n":2}}"""),
            Parse("""{"source":"c","type":"t3"}"""),
        };
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);

        EventSerializer.WriteNdjson(buffer, writer, events);

        var lines = Encoding.UTF8.GetString(buffer.WrittenSpan).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(3, lines.Length);
        for (var i = 0; i < lines.Length; i++)
        {
            using var doc = JsonDocument.Parse(lines[i]);
            Assert.Equal(events[i].Type, doc.RootElement.GetProperty("type").GetString());
        }
    }

    [Fact]
    public void Source_generated_context_matches_the_wire_format()
    {
        const string json = """{"source":"a","type":"b","payload":{"k":"v"}}""";

        var dto = JsonSerializer.Deserialize(json, IngestorJsonContext.Default.IngestEventDto);

        Assert.NotNull(dto);
        Assert.Equal("a", dto!.Source);
        Assert.Equal("v", dto.Payload!.Value.GetProperty("k").GetString());
    }
}
