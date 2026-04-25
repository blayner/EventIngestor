using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using Microsoft.Extensions.Logging;

namespace EventIngestor.Pipeline.Sinks;

/// <summary>Logs a one-line summary per batch. Handy during development.</summary>
public sealed partial class LoggingEventSink(ILogger<LoggingEventSink> logger) : IEventSink
{
    public string Name => "logging";

    public ValueTask WriteBatchAsync(EventBatch batch, CancellationToken cancellationToken)
    {
        if (logger.IsEnabled(LogLevel.Information) && !batch.IsEmpty)
        {
            var first = batch[0];
            LogBatch(logger, batch.Sequence, batch.WorkerId, batch.Count, first.Source, first.Type);
        }

        return ValueTask.CompletedTask;
    }

    [LoggerMessage(
        EventId = 2001,
        Level = LogLevel.Information,
        Message = "Batch {Sequence} from worker {WorkerId}: {Count} events (first: {Source}/{Type})")]
    private static partial void LogBatch(ILogger logger, long sequence, int workerId, int count, string source, string type);
}
