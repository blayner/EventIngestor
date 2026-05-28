using System.Collections.Concurrent;
using EventIngestor.Core.Pooling;

namespace EventIngestor.Tests.Pooling;

public class LockFreeObjectPoolTests
{
    private sealed class Resettable : IPoolable
    {
        public int Value { get; set; }

        public bool AllowReuse { get; set; } = true;

        public bool TryReset()
        {
            Value = 0;
            return AllowReuse;
        }
    }

    [Fact]
    public void Creates_objects_when_empty_and_reuses_returned_ones()
    {
        var pool = new LockFreeObjectPool<object>(() => new object(), maxRetained: 4);

        var first = pool.Rent();
        pool.Return(first);
        var second = pool.Rent();

        Assert.Same(first, second);
        Assert.Equal(1, pool.Created);
    }

    [Fact]
    public void Retains_at_most_max_plus_the_fast_slot()
    {
        var pool = new LockFreeObjectPool<object>(() => new object(), maxRetained: 2);
        var items = Enumerable.Range(0, 5).Select(_ => pool.Rent()).ToList();

        items.ForEach(pool.Return);

        Assert.Equal(3, pool.Retained);
        Assert.Equal(2, pool.Dropped);
    }

    [Fact]
    public void Resets_poolable_objects_on_return()
    {
        var pool = new LockFreeObjectPool<Resettable>(() => new Resettable(), maxRetained: 4);
        var item = pool.Rent();
        item.Value = 42;

        pool.Return(item);

        Assert.Equal(0, pool.Rent().Value);
    }

    [Fact]
    public void Drops_objects_whose_reset_fails()
    {
        var pool = new LockFreeObjectPool<Resettable>(() => new Resettable(), maxRetained: 4);
        var item = pool.Rent();
        item.AllowReuse = false;

        pool.Return(item);

        Assert.Equal(0, pool.Retained);
        Assert.Equal(1, pool.Dropped);
        Assert.NotSame(item, pool.Rent());
    }

    [Fact]
    public async Task Never_hands_the_same_instance_to_two_renters()
    {
        var pool = new LockFreeObjectPool<object>(() => new object(), maxRetained: 16);
        var inUse = new ConcurrentDictionary<object, byte>(ReferenceEqualityComparer.Instance);
        var violations = 0;

        var workers = Enumerable.Range(0, Environment.ProcessorCount * 2).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < 20_000; i++)
            {
                var item = pool.Rent();
                if (!inUse.TryAdd(item, 0))
                {
                    Interlocked.Increment(ref violations);
                }

                inUse.TryRemove(item, out byte _);
                pool.Return(item);
            }
        }));

        await Task.WhenAll(workers);

        Assert.Equal(0, violations);
        Assert.True(pool.Retained <= 17);
    }
}
