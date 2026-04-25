using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;

namespace EventIngestor.Pipeline.Sinks;

/// <summary>Discards every batch. Used for benchmarks and as the fallback when no sink is configured.</summary>
public sealed class NullEventSink : IEventSink
{
    public string Name => "null";

    public ValueTask WriteBatchAsync(EventBatch batch, CancellationToken cancellationToken) => ValueTask.CompletedTask;
}
