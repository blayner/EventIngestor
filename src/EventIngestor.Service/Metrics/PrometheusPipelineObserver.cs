using System.Collections.Concurrent;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;
using Prometheus;

namespace EventIngestor.Service.Metrics;

/// <summary>
/// Exports pipeline signals as Prometheus metrics.
/// </summary>
/// <remarks>
/// Labelled children are cached per label set: <c>WithLabels</c> does a lookup (and allocates a
/// label array) on every call, which is not something to do per accepted request. Gauges that
/// mirror state (queue depth, pool size) are refreshed right before each scrape instead of on
/// every change.
/// </remarks>
public sealed class PrometheusPipelineObserver : IPipelineObserver
{
    private static readonly Counter AcceptedCounter = Prometheus.Metrics.CreateCounter(
        "ingestor_events_accepted_total",
        "Events accepted into the pipeline.",
        new CounterConfiguration { LabelNames = ["transport"] });

    private static readonly Counter RejectedCounter = Prometheus.Metrics.CreateCounter(
        "ingestor_events_rejected_total",
        "Events refused, by reason (invalid_event, payload_too_large, backpressure, shutting_down).",
        new CounterConfiguration { LabelNames = ["transport", "reason"] });

    private static readonly Histogram BatchSize = Prometheus.Metrics.CreateHistogram(
        "ingestor_batch_size_events",
        "Events per flushed batch.",
        new HistogramConfiguration { LabelNames = ["sink"], Buckets = Histogram.ExponentialBuckets(1, 2, 14) });

    private static readonly Histogram FlushDuration = Prometheus.Metrics.CreateHistogram(
        "ingestor_batch_flush_duration_seconds",
        "Time to write one batch to a sink (successful attempts only).",
        new HistogramConfiguration { LabelNames = ["sink"], Buckets = Histogram.ExponentialBuckets(0.0001, 2, 18) });

    private static readonly Counter BatchFailures = Prometheus.Metrics.CreateCounter(
        "ingestor_batch_failures_total",
        "Failed batch write attempts (each retry counts).",
        new CounterConfiguration { LabelNames = ["sink"] });

    private static readonly Counter EventsDropped = Prometheus.Metrics.CreateCounter(
        "ingestor_events_dropped_total",
        "Events lost for a sink after exhausting retries.",
        new CounterConfiguration { LabelNames = ["sink"] });

    private static readonly Gauge QueueDepth = Prometheus.Metrics.CreateGauge(
        "ingestor_pipeline_queue_depth", "Events buffered in the channel.");

    private static readonly Gauge QueueCapacity = Prometheus.Metrics.CreateGauge(
        "ingestor_pipeline_queue_capacity", "Channel capacity.");

    private static readonly Gauge PoolRetained = Prometheus.Metrics.CreateGauge(
        "ingestor_event_pool_retained", "Idle event instances held by the pool.");

    private static readonly Counter PoolCreated = Prometheus.Metrics.CreateCounter(
        "ingestor_event_pool_created_total", "Event instances allocated because the pool was empty.");

    private static readonly Counter PoolDropped = Prometheus.Metrics.CreateCounter(
        "ingestor_event_pool_dropped_total", "Event instances discarded because the pool was full.");

    private readonly ConcurrentDictionary<string, Counter.Child> _accepted = new();
    private readonly ConcurrentDictionary<(string Transport, RejectReason Reason), Counter.Child> _rejected = new();
    private readonly ConcurrentDictionary<string, SinkMetrics> _sinks = new();

    public PrometheusPipelineObserver(IEventPipeline pipeline, LockFreeObjectPool<IngestEvent> eventPool)
    {
        QueueCapacity.Set(pipeline.Capacity);

        Prometheus.Metrics.DefaultRegistry.AddBeforeCollectCallback(() =>
        {
            QueueDepth.Set(pipeline.Count);
            PoolRetained.Set(eventPool.Retained);
            PoolCreated.IncTo(eventPool.Created);
            PoolDropped.IncTo(eventPool.Dropped);
        });
    }

    public void EventsAccepted(string transport, int count) =>
        _accepted.GetOrAdd(transport, static t => AcceptedCounter.WithLabels(t)).Inc(count);

    public void EventsRejected(string transport, RejectReason reason, int count) =>
        _rejected.GetOrAdd((transport, reason), static key => RejectedCounter.WithLabels(key.Transport, key.Reason.ToLabel())).Inc(count);

    public void BatchFlushed(string sink, int size, TimeSpan duration)
    {
        var metrics = Sink(sink);
        metrics.Size.Observe(size);
        metrics.Duration.Observe(duration.TotalSeconds);
    }

    public void BatchFailed(string sink, int size, int attempt, Exception exception) => Sink(sink).Failures.Inc();

    public void BatchDropped(string sink, int size) => Sink(sink).Dropped.Inc(size);

    private SinkMetrics Sink(string sink) => _sinks.GetOrAdd(sink, static name => new SinkMetrics(
        BatchSize.WithLabels(name),
        FlushDuration.WithLabels(name),
        BatchFailures.WithLabels(name),
        EventsDropped.WithLabels(name)));

    private sealed record SinkMetrics(Histogram.Child Size, Histogram.Child Duration, Counter.Child Failures, Counter.Child Dropped);
}
