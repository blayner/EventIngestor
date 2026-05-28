using System.Collections.Concurrent;

namespace EventIngestor.Core.Pooling;

/// <summary>
/// Bounded, thread-safe object pool.
/// </summary>
/// <remarks>
/// <para>
/// Same shape as <c>Microsoft.Extensions.ObjectPool.DefaultObjectPool</c>: a single "fast slot"
/// claimed with one <see cref="Interlocked.CompareExchange{T}(ref T, T, T)"/> serves the common
/// rent/return ping-pong without touching the queue; overflow goes to a
/// <see cref="ConcurrentQueue{T}"/> capped at <c>maxRetained</c> items. Beyond that, returned
/// objects are dropped and left to the GC, so a burst cannot permanently inflate the heap.
/// </para>
/// <para>
/// Kept in-house (instead of taking the Microsoft.Extensions.ObjectPool dependency) to expose
/// created/dropped counters for metrics and the pooling benchmarks.
/// </para>
/// </remarks>
public sealed class LockFreeObjectPool<T> where T : class
{
    private readonly Func<T> _factory;
    private readonly int _maxRetained;
    private readonly ConcurrentQueue<T> _items = new();

    private T? _fastItem;
    private int _queuedCount;
    private long _created;
    private long _dropped;

    public LockFreeObjectPool(Func<T> factory, int maxRetained)
    {
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentOutOfRangeException.ThrowIfNegative(maxRetained);

        _factory = factory;
        _maxRetained = maxRetained;
    }

    /// <summary>Approximate number of idle objects held by the pool (queue plus fast slot).</summary>
    public int Retained => Volatile.Read(ref _queuedCount) + (Volatile.Read(ref _fastItem) is null ? 0 : 1);

    /// <summary>Objects created because the pool was empty.</summary>
    public long Created => Interlocked.Read(ref _created);

    /// <summary>Objects discarded on return because the pool was full or the reset failed.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    public T Rent()
    {
        var item = _fastItem;
        if (item is not null && Interlocked.CompareExchange(ref _fastItem, null, item) == item)
        {
            return item;
        }

        if (_items.TryDequeue(out item))
        {
            Interlocked.Decrement(ref _queuedCount);
            return item;
        }

        Interlocked.Increment(ref _created);
        return _factory();
    }

    public void Return(T item)
    {
        ArgumentNullException.ThrowIfNull(item);

        if (item is IPoolable poolable && !poolable.TryReset())
        {
            Interlocked.Increment(ref _dropped);
            return;
        }

        if (_fastItem is null && Interlocked.CompareExchange(ref _fastItem, item, null) is null)
        {
            return;
        }

        if (Interlocked.Increment(ref _queuedCount) <= _maxRetained)
        {
            _items.Enqueue(item);
            return;
        }

        Interlocked.Decrement(ref _queuedCount);
        Interlocked.Increment(ref _dropped);
    }
}
