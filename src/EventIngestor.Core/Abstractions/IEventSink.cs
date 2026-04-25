using EventIngestor.Core.Models;

namespace EventIngestor.Core.Abstractions;

/// <summary>
/// Destination for flushed batches (file, log, message broker, ...).
/// </summary>
/// <remarks>
/// Contract:
/// <list type="bullet">
/// <item>The batch and its events are only valid for the duration of the call. They are reused
/// (and their payload buffers recycled) as soon as the returned task completes, so a sink that
/// needs the data later must copy it.</item>
/// <item>Throwing signals a transient failure: the processor retries with backoff and eventually
/// drops the batch for this sink. A sink must therefore be safe to call again with the same batch.</item>
/// <item>Implementations may be called concurrently by several processors.</item>
/// </list>
/// </remarks>
public interface IEventSink
{
    string Name { get; }

    ValueTask WriteBatchAsync(EventBatch batch, CancellationToken cancellationToken);
}
