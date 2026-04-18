namespace EventIngestor.Core.Models;

/// <summary>
/// A single event accepted by the ingestion service.
/// </summary>
/// <remarks>
/// The payload is kept as raw UTF-8 JSON and is never materialized into a DOM:
/// the service does not care about the payload shape, it only moves bytes.
/// </remarks>
public sealed class IngestEvent
{
    private byte[] _payload = [];

    public Guid Id { get; set; }

    /// <summary>When the event happened, according to the producer.</summary>
    public DateTimeOffset Timestamp { get; set; }

    /// <summary>Logical producer of the event, e.g. <c>checkout-api</c>.</summary>
    public string Source { get; set; } = string.Empty;

    /// <summary>Event type, e.g. <c>order.created</c>.</summary>
    public string Type { get; set; } = string.Empty;

    /// <summary>When the event reached this service.</summary>
    public DateTimeOffset ReceivedAt { get; set; }

    /// <summary>Raw UTF-8 JSON payload. Empty when the producer sent no payload.</summary>
    public ReadOnlyMemory<byte> Payload => _payload;

    public void SetPayload(ReadOnlySpan<byte> payload) => _payload = payload.ToArray();

    /// <summary>
    /// Fills server-side defaults once all producer-supplied fields have been read.
    /// </summary>
    public void EnsureDefaults(DateTimeOffset receivedAt)
    {
        if (Id == Guid.Empty)
        {
            Id = Guid.NewGuid();
        }

        ReceivedAt = receivedAt;

        if (Timestamp == default)
        {
            Timestamp = receivedAt;
        }
    }
}
