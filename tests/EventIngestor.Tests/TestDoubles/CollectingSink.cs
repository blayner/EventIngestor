using System.Collections.Concurrent;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;

namespace EventIngestor.Tests.TestDoubles;

/// <summary>Copies what it receives, since batches are reused after the call returns.</summary>
internal sealed class CollectingSink : IEventSink
{
    private readonly ConcurrentQueue<(long Sequence, Guid[] Ids)> _batches = new();

    public string Name => "collecting";

    public IReadOnlyCollection<(long Sequence, Guid[] Ids)> Batches => _batches;

    public int EventCount => _batches.Sum(b => b.Ids.Length);

    public IEnumerable<Guid> AllIds => _batches.SelectMany(b => b.Ids);

    public ValueTask WriteBatchAsync(EventBatch batch, CancellationToken cancellationToken)
    {
        var ids = new Guid[batch.Count];
        for (var i = 0; i < batch.Count; i++)
        {
            ids[i] = batch[i].Id;
        }

        _batches.Enqueue((batch.Sequence, ids));
        return ValueTask.CompletedTask;
    }
}

/// <summary>Fails the first <paramref name="failures"/> calls, then delegates to an inner collecting sink.</summary>
internal sealed class FlakySink(int failures) : IEventSink
{
    private int _calls;

    public CollectingSink Inner { get; } = new();

    public int Calls => Volatile.Read(ref _calls);

    public string Name => "flaky";

    public ValueTask WriteBatchAsync(EventBatch batch, CancellationToken cancellationToken)
    {
        if (Interlocked.Increment(ref _calls) <= failures)
        {
            throw new IOException("Simulated sink failure.");
        }

        return Inner.WriteBatchAsync(batch, cancellationToken);
    }
}
