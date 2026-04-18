using System.Text.Json;
using EventIngestor.Core.Models;

namespace EventIngestor.Core.Serialization;

/// <summary>
/// Forward-only parser for the event wire format:
/// <code>{"id":"...","timestamp":"...","source":"...","type":"...","payload":{...}}</code>
/// </summary>
/// <remarks>
/// Uses <see cref="Utf8JsonReader"/> directly instead of <see cref="JsonSerializer"/> so the
/// payload is captured as a raw byte slice (no <see cref="JsonElement"/>/<see cref="JsonDocument"/>
/// allocation) and unknown properties are skipped without being materialized.
/// </remarks>
public static class EventParser
{
    private const long MaxUnixMilliseconds = 253_402_300_799_999; // 9999-12-31T23:59:59.999Z

    private static readonly JsonReaderOptions ReaderOptions = new()
    {
        CommentHandling = JsonCommentHandling.Disallow,
        AllowTrailingCommas = false,
        MaxDepth = 64,
    };

    private static ReadOnlySpan<byte> IdProperty => "id"u8;
    private static ReadOnlySpan<byte> TimestampProperty => "timestamp"u8;
    private static ReadOnlySpan<byte> SourceProperty => "source"u8;
    private static ReadOnlySpan<byte> TypeProperty => "type"u8;
    private static ReadOnlySpan<byte> PayloadProperty => "payload"u8;

    /// <summary>
    /// Parses a single JSON object into <paramref name="target"/>.
    /// </summary>
    public static ParseStatus TryParse(
        ReadOnlySpan<byte> utf8Json,
        IngestEvent target,
        int maxPayloadBytes = EventLimits.DefaultMaxPayloadBytes)
    {
        ArgumentNullException.ThrowIfNull(target);

        var reader = new Utf8JsonReader(utf8Json, ReaderOptions);
        try
        {
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                return ParseStatus.InvalidJson;
            }

            var status = ReadObject(ref reader, utf8Json, target, maxPayloadBytes);
            if (status != ParseStatus.Ok)
            {
                return status;
            }

            // Anything after the closing brace (other than whitespace) is an error.
            return reader.Read() ? ParseStatus.InvalidJson : ParseStatus.Ok;
        }
        catch (JsonException)
        {
            return ParseStatus.InvalidJson;
        }
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="utf8Json"/> is exactly one well-formed JSON value.
    /// Used to validate payloads that arrive as opaque bytes (gRPC).
    /// </summary>
    public static bool IsValidJsonValue(ReadOnlySpan<byte> utf8Json)
    {
        if (utf8Json.IsEmpty)
        {
            return false;
        }

        var reader = new Utf8JsonReader(utf8Json, ReaderOptions);
        try
        {
            if (!reader.Read())
            {
                return false;
            }

            reader.Skip();
            return !reader.Read();
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// Reads the properties of an object whose <see cref="JsonTokenType.StartObject"/> token has
    /// already been consumed. On success the reader is left on the matching
    /// <see cref="JsonTokenType.EndObject"/>.
    /// </summary>
    private static ParseStatus ReadObject(
        ref Utf8JsonReader reader,
        ReadOnlySpan<byte> json,
        IngestEvent target,
        int maxPayloadBytes)
    {
        var hasSource = false;
        var hasType = false;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                if (!hasSource)
                {
                    return ParseStatus.MissingSource;
                }

                if (!hasType)
                {
                    return ParseStatus.MissingType;
                }

                target.EnsureDefaults(DateTimeOffset.UtcNow);
                return ParseStatus.Ok;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                return ParseStatus.InvalidJson;
            }

            if (reader.ValueTextEquals(SourceProperty))
            {
                reader.Read();
                var source = ReadName(ref reader);
                if (source is null)
                {
                    return ParseStatus.InvalidField;
                }

                target.Source = source;
                hasSource = true;
            }
            else if (reader.ValueTextEquals(TypeProperty))
            {
                reader.Read();
                var type = ReadName(ref reader);
                if (type is null)
                {
                    return ParseStatus.InvalidField;
                }

                target.Type = type;
                hasType = true;
            }
            else if (reader.ValueTextEquals(PayloadProperty))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.Null)
                {
                    continue;
                }

                // TokenStartIndex points at '{', '[', the opening quote or the first digit;
                // after Skip() BytesConsumed points just past the end of the value.
                var start = reader.TokenStartIndex;
                reader.Skip();
                var length = reader.BytesConsumed - start;

                if (length > maxPayloadBytes)
                {
                    return ParseStatus.PayloadTooLarge;
                }

                target.SetPayload(json.Slice((int)start, (int)length));
            }
            else if (reader.ValueTextEquals(IdProperty))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.Null)
                {
                    continue;
                }

                if (reader.TokenType != JsonTokenType.String || !reader.TryGetGuid(out var id))
                {
                    return ParseStatus.InvalidField;
                }

                target.Id = id;
            }
            else if (reader.ValueTextEquals(TimestampProperty))
            {
                reader.Read();
                if (reader.TokenType == JsonTokenType.Null)
                {
                    continue;
                }

                if (!TryReadTimestamp(ref reader, out var timestamp))
                {
                    return ParseStatus.InvalidField;
                }

                target.Timestamp = timestamp;
            }
            else
            {
                // Unknown property: skip its value, whatever its shape.
                reader.Read();
                reader.Skip();
            }
        }

        return ParseStatus.InvalidJson;
    }

    private static string? ReadName(ref Utf8JsonReader reader)
    {
        if (reader.TokenType != JsonTokenType.String)
        {
            return null;
        }

        var length = reader.HasValueSequence ? reader.ValueSequence.Length : reader.ValueSpan.Length;
        if (length == 0 || length > EventLimits.MaxNameLength)
        {
            return null;
        }

        return reader.GetString();
    }

    private static bool TryReadTimestamp(ref Utf8JsonReader reader, out DateTimeOffset value)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.String:
                return reader.TryGetDateTimeOffset(out value);

            case JsonTokenType.Number when reader.TryGetInt64(out var unixMs) && unixMs is >= 0 and <= MaxUnixMilliseconds:
                value = DateTimeOffset.FromUnixTimeMilliseconds(unixMs);
                return true;

            default:
                value = default;
                return false;
        }
    }
}
