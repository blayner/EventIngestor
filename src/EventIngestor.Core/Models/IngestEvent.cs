using System.Buffers;
using EventIngestor.Core.Pooling;

namespace EventIngestor.Core.Models;

/// <summary>
/// A single event accepted by the ingestion service.
/// </summary>
/// <remarks>
/// <para>
/// The payload is kept as raw UTF-8 JSON and is never materialized into a DOM:
/// the service does not care about the payload shape, it only moves bytes.
/// </para>
/// <para>
/// Instances are pooled (<see cref="LockFreeObjectPool{T}"/>). The payload lives in a buffer rented
/// from <see cref="ArrayPool{T}.Shared"/> that the instance keeps across reuses as long as it is
/// small, so in steady state neither the event nor its payload storage is allocated per request.
/// </para>
/// </remarks>
public sealed class IngestEvent : IPoolable
{
    /// <summary>
    /// Payload buffers larger than this are handed back to the ArrayPool on reset rather than kept by
    /// the pooled instance, so idle pooled events cannot pin large arrays.
    /// </summary>
    public const int MaxRetainedPayloadBytes = 4 * 1024;

    private const int MinimumBufferSize = 256;

    private byte[]? _payloadBuffer;
    private int _payloadLength;

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
    public ReadOnlyMemory<byte> Payload =>
        _payloadBuffer is null ? ReadOnlyMemory<byte>.Empty : new ReadOnlyMemory<byte>(_payloadBuffer, 0, _payloadLength);

    public void SetPayload(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty)
        {
            _payloadLength = 0;
            return;
        }

        if (_payloadBuffer is null || _payloadBuffer.Length < payload.Length)
        {
            ReturnPayloadBuffer();
            _payloadBuffer = ArrayPool<byte>.Shared.Rent(Math.Max(payload.Length, MinimumBufferSize));
        }

        payload.CopyTo(_payloadBuffer);
        _payloadLength = payload.Length;
    }

    /// <summary>
    /// Fills server-side defaults once all producer-supplied fields have been read.
    /// </summary>
    public void EnsureDefaults(DateTimeOffset receivedAt)
    {
        if (Id == Guid.Empty)
        {
            // Version 7: time-ordered, so generated ids sort (and index) by arrival.
            Id = Guid.CreateVersion7(receivedAt);
        }

        ReceivedAt = receivedAt;

        if (Timestamp == default)
        {
            Timestamp = receivedAt;
        }
    }

    public bool TryReset()
    {
        Id = Guid.Empty;
        Timestamp = default;
        Source = string.Empty;
        Type = string.Empty;
        ReceivedAt = default;
        _payloadLength = 0;

        if (_payloadBuffer is { Length: > MaxRetainedPayloadBytes })
        {
            ReturnPayloadBuffer();
        }

        return true;
    }

    private void ReturnPayloadBuffer()
    {
        if (_payloadBuffer is not null)
        {
            ArrayPool<byte>.Shared.Return(_payloadBuffer);
            _payloadBuffer = null;
        }
    }
}
