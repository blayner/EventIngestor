using System.Collections.Concurrent;
using EventIngestor.Core.Abstractions;

namespace EventIngestor.Tests.TestDoubles;

internal sealed class RecordingObserver : IPipelineObserver
{
    private int _accepted;
    private int _flushed;
    private int _failed;
    private int _dropped;

    public ConcurrentDictionary<RejectReason, int> Rejected { get; } = new();

    public int Accepted => Volatile.Read(ref _accepted);

    public int BatchesFlushed => Volatile.Read(ref _flushed);

    public int BatchFailures => Volatile.Read(ref _failed);

    public int BatchesDropped => Volatile.Read(ref _dropped);

    public int RejectedCount(RejectReason reason) => Rejected.TryGetValue(reason, out var count) ? count : 0;

    public void EventsAccepted(string transport, int count) => Interlocked.Add(ref _accepted, count);

    public void EventsRejected(string transport, RejectReason reason, int count) =>
        Rejected.AddOrUpdate(reason, count, (_, current) => current + count);

    public void BatchFlushed(string sink, int size, TimeSpan duration) => Interlocked.Increment(ref _flushed);

    public void BatchFailed(string sink, int size, int attempt, Exception exception) => Interlocked.Increment(ref _failed);

    public void BatchDropped(string sink, int size) => Interlocked.Increment(ref _dropped);
}
