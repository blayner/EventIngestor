using System.Buffers;
using System.Text.Json;
using EventIngestor.Core.Models;

namespace EventIngestor.Core.Serialization;

/// <summary>
/// Writes events back to JSON. The payload is emitted verbatim with
/// <see cref="Utf8JsonWriter.WriteRawValue(ReadOnlySpan{byte}, bool)"/>, so a round trip
/// never re-encodes producer data.
/// </summary>
public static class EventSerializer
{
    private static readonly JsonEncodedText IdName = JsonEncodedText.Encode("id");
    private static readonly JsonEncodedText TimestampName = JsonEncodedText.Encode("timestamp");
    private static readonly JsonEncodedText SourceName = JsonEncodedText.Encode("source");
    private static readonly JsonEncodedText TypeName = JsonEncodedText.Encode("type");
    private static readonly JsonEncodedText ReceivedAtName = JsonEncodedText.Encode("receivedAt");
    private static readonly JsonEncodedText PayloadName = JsonEncodedText.Encode("payload");

    private static ReadOnlySpan<byte> NewLine => "\n"u8;

    public static void Write(Utf8JsonWriter writer, IngestEvent evt)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(evt);

        writer.WriteStartObject();
        writer.WriteString(IdName, evt.Id);
        writer.WriteString(TimestampName, evt.Timestamp);
        writer.WriteString(SourceName, evt.Source);
        writer.WriteString(TypeName, evt.Type);
        writer.WriteString(ReceivedAtName, evt.ReceivedAt);
        writer.WritePropertyName(PayloadName);

        if (evt.Payload.IsEmpty)
        {
            writer.WriteNullValue();
        }
        else
        {
            // Payloads are validated on the way in, so re-validation here would be wasted work.
            writer.WriteRawValue(evt.Payload.Span, skipInputValidation: true);
        }

        writer.WriteEndObject();
    }

    /// <summary>
    /// Writes events as newline-delimited JSON. The <paramref name="writer"/> is reset onto
    /// <paramref name="output"/> for every event so it can be reused across calls without
    /// tripping the "single root value" validation.
    /// </summary>
    public static void WriteNdjson(IBufferWriter<byte> output, Utf8JsonWriter writer, ReadOnlySpan<IngestEvent> events)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(writer);

        foreach (var evt in events)
        {
            writer.Reset(output);
            Write(writer, evt);
            writer.Flush();
            output.Write(NewLine);
        }
    }

    public static byte[] SerializeToUtf8Bytes(IngestEvent evt)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            Write(writer, evt);
        }

        return buffer.WrittenSpan.ToArray();
    }
}
