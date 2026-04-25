using System.Threading.Channels;
using EventIngestor.Core.Models;

namespace EventIngestor.Core.Abstractions;

/// <summary>
/// Bounded in-memory buffer between the transports (producers) and the batch processors (consumers).
/// </summary>
public interface IEventPipeline
{
    /// <summary>Maximum number of events buffered before producers are pushed back.</summary>
    int Capacity { get; }

    /// <summary>Events currently buffered.</summary>
    int Count { get; }

    /// <summary>Consumer side of the pipeline.</summary>
    ChannelReader<IngestEvent> Reader { get; }

    /// <summary>Completes when the pipeline has been completed and fully drained.</summary>
    Task Completion { get; }

    /// <summary>Non-blocking write. Returns <c>false</c> when the buffer is full or completed.</summary>
    bool TryEnqueue(IngestEvent evt);

    /// <summary>
    /// Waits for free space and writes the event. Returns <c>false</c> if the pipeline
    /// was completed while waiting.
    /// </summary>
    ValueTask<bool> EnqueueAsync(IngestEvent evt, CancellationToken cancellationToken = default);

    /// <summary>Stops accepting events. Already buffered events are still delivered to consumers.</summary>
    void Complete(Exception? error = null);
}
