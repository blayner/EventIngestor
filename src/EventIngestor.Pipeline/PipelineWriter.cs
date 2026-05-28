using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;

namespace EventIngestor.Pipeline;

/// <summary>
/// Producer-side entry point shared by every transport. Centralizes the backpressure policy,
/// the accepted/rejected accounting and the ownership of pooled events.
/// </summary>
/// <remarks>
/// <para>
/// Policy: try a non-blocking write first (the common case); if the channel is full, wait at most
/// <see cref="PipelineOptions.EnqueueTimeout"/> for space; after that the event is throttled and the
/// caller is expected to retry later. Within a batch, once one event has timed out the remaining
/// ones only get a non-blocking attempt, so a saturated pipeline costs a request at most one timeout.
/// </para>
/// <para>
/// Ownership: an event handed to this class is owned by it. Accepted events belong to the pipeline
/// (and are returned to the pool by the batch processor after the flush); refused events are
/// returned to the pool here. Callers must not touch an event after passing it in.
/// </para>
/// </remarks>
public sealed class PipelineWriter
{
    private readonly IEventPipeline _pipeline;
    private readonly IPipelineObserver _observer;
    private readonly LockFreeObjectPool<IngestEvent>? _eventPool;
    private readonly TimeSpan _enqueueTimeout;

    public PipelineWriter(
        IEventPipeline pipeline,
        PipelineOptions options,
        IPipelineObserver observer,
        LockFreeObjectPool<IngestEvent>? eventPool = null)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(observer);

        _pipeline = pipeline;
        _observer = observer;
        _eventPool = eventPool;
        _enqueueTimeout = options.EnqueueTimeout;
    }

    private enum EnqueueOutcome
    {
        Accepted,
        Throttled,
        ShuttingDown,
    }

    /// <summary>Gets an event to populate: pooled when a pool is configured.</summary>
    public IngestEvent Rent() => _eventPool?.Rent() ?? new IngestEvent();

    public async ValueTask<bool> WriteAsync(IngestEvent evt, string transport, CancellationToken cancellationToken = default)
    {
        EnqueueOutcome outcome;
        try
        {
            outcome = _pipeline.TryEnqueue(evt)
                ? EnqueueOutcome.Accepted
                : await EnqueueSlowAsync(evt, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Release(evt);
            throw;
        }

        if (outcome == EnqueueOutcome.Accepted)
        {
            _observer.EventsAccepted(transport, 1);
            return true;
        }

        _observer.EventsRejected(transport, ToReason(outcome), 1);
        Release(evt);
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

        var i = 0;
        try
        {
            for (; i < events.Count; i++)
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
                Release(evt);
            }
        }
        catch (OperationCanceledException)
        {
            // Caller went away mid-batch: events [0, i) are already in the pipeline or recycled,
            // the rest are still ours to recycle.
            for (var j = i; j < events.Count; j++)
            {
                Release(events[j]);
            }

            throw;
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

    /// <summary>Records an event refused before reaching the pipeline (e.g. invalid input) and recycles it.</summary>
    public void Reject(IngestEvent evt, string transport, RejectReason reason)
    {
        _observer.EventsRejected(transport, reason, 1);
        Release(evt);
    }

    /// <summary>Records a rejection for input that never became an event (e.g. an oversized line).</summary>
    public void Reject(string transport, RejectReason reason)
    {
        _observer.EventsRejected(transport, reason, 1);
    }

    /// <summary>Returns an event that will not be written (e.g. the request was aborted) to the pool.</summary>
    public void Release(IngestEvent evt) => _eventPool?.Return(evt);

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
