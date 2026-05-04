using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;

namespace EventIngestor.Pipeline;

/// <summary>
/// Producer-side entry point shared by every transport. Centralizes the backpressure policy
/// and the accepted/rejected accounting so gRPC and HTTP behave the same way.
/// </summary>
/// <remarks>
/// Policy: try a non-blocking write first (the common case); if the channel is full, wait at most
/// <see cref="PipelineOptions.EnqueueTimeout"/> for space; after that the event is throttled and the
/// caller is expected to retry later. Within a batch, once one event has timed out the remaining
/// ones only get a non-blocking attempt, so a saturated pipeline costs a request at most one timeout.
/// </remarks>
public sealed class PipelineWriter
{
    private readonly IEventPipeline _pipeline;
    private readonly IPipelineObserver _observer;
    private readonly TimeSpan _enqueueTimeout;

    public PipelineWriter(IEventPipeline pipeline, PipelineOptions options, IPipelineObserver observer)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(observer);

        _pipeline = pipeline;
        _observer = observer;
        _enqueueTimeout = options.EnqueueTimeout;
    }

    private enum EnqueueOutcome
    {
        Accepted,
        Throttled,
        ShuttingDown,
    }

    public async ValueTask<bool> WriteAsync(IngestEvent evt, string transport, CancellationToken cancellationToken = default)
    {
        var outcome = _pipeline.TryEnqueue(evt)
            ? EnqueueOutcome.Accepted
            : await EnqueueSlowAsync(evt, cancellationToken).ConfigureAwait(false);

        if (outcome == EnqueueOutcome.Accepted)
        {
            _observer.EventsAccepted(transport, 1);
            return true;
        }

        _observer.EventsRejected(transport, ToReason(outcome), 1);
        return false;
    }

    public async ValueTask<IngestResult> WriteAsync(
        IReadOnlyList<IngestEvent> events,
        string transport,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(events);

        var accepted = 0;
        var throttled = 0;
        var refusal = EnqueueOutcome.Throttled;
        var saturated = false;

        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];

            if (_pipeline.TryEnqueue(evt))
            {
                accepted++;
                continue;
            }

            if (!saturated)
            {
                var outcome = await EnqueueSlowAsync(evt, cancellationToken).ConfigureAwait(false);
                if (outcome == EnqueueOutcome.Accepted)
                {
                    accepted++;
                    continue;
                }

                saturated = true;
                refusal = outcome;
            }

            throttled++;
        }

        if (accepted > 0)
        {
            _observer.EventsAccepted(transport, accepted);
        }

        if (throttled > 0)
        {
            _observer.EventsRejected(transport, ToReason(refusal), throttled);
        }

        return new IngestResult(accepted, 0, throttled);
    }

    /// <summary>Records an event refused before reaching the pipeline (e.g. invalid input).</summary>
    public void Reject(IngestEvent evt, string transport, RejectReason reason)
    {
        _observer.EventsRejected(transport, reason, 1);
    }

    private async ValueTask<EnqueueOutcome> EnqueueSlowAsync(IngestEvent evt, CancellationToken cancellationToken)
    {
        if (_enqueueTimeout == TimeSpan.Zero)
        {
            return EnqueueOutcome.Throttled;
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(_enqueueTimeout);

        try
        {
            return await _pipeline.EnqueueAsync(evt, timeout.Token).ConfigureAwait(false)
                ? EnqueueOutcome.Accepted
                : EnqueueOutcome.ShuttingDown;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return EnqueueOutcome.Throttled;
        }
    }

    private static RejectReason ToReason(EnqueueOutcome outcome) =>
        outcome == EnqueueOutcome.ShuttingDown ? RejectReason.ShuttingDown : RejectReason.Backpressure;
}
