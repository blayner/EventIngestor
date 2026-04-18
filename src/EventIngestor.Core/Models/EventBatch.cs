namespace EventIngestor.Core.Models;

/// <summary>
/// A fixed-capacity group of events flushed to sinks as a unit.
/// </summary>
/// <remarks>
/// A batch is meant to be reused: the consumer fills it, flushes it and calls
/// <see cref="Clear"/>. The backing array is allocated once per batch instance.
/// Sinks must not keep references to the batch or its events after the flush returns.
/// </remarks>
public sealed class EventBatch
{
    private readonly IngestEvent[] _items;

    public EventBatch(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        _items = new IngestEvent[capacity];
    }

    public int Capacity => _items.Length;

    public int Count { get; private set; }

    public bool IsEmpty => Count == 0;

    public bool IsFull => Count == _items.Length;

    /// <summary>Monotonic sequence number assigned by the consumer when the batch is sealed.</summary>
    public long Sequence { get; private set; }

    /// <summary>Id of the consumer that produced the batch.</summary>
    public int WorkerId { get; private set; }

    public DateTimeOffset SealedAt { get; private set; }

    public ReadOnlySpan<IngestEvent> Span => _items.AsSpan(0, Count);

    public ReadOnlyMemory<IngestEvent> Memory => _items.AsMemory(0, Count);

    public IngestEvent this[int index] =>
        (uint)index < (uint)Count ? _items[index] : throw new ArgumentOutOfRangeException(nameof(index));

    public void Add(IngestEvent evt)
    {
        ArgumentNullException.ThrowIfNull(evt);

        if (IsFull)
        {
            throw new InvalidOperationException($"Batch is full (capacity {Capacity}).");
        }

        _items[Count++] = evt;
    }

    public void Seal(int workerId, long sequence, DateTimeOffset sealedAt)
    {
        WorkerId = workerId;
        Sequence = sequence;
        SealedAt = sealedAt;
    }

    /// <summary>Drops references to the events so the batch can be refilled.</summary>
    public void Clear()
    {
        Array.Clear(_items, 0, Count);
        Count = 0;
        Sequence = 0;
        SealedAt = default;
    }
}
