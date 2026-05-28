using System.Diagnostics;
using System.Threading.Channels;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;
using Microsoft.Extensions.Logging;

namespace EventIngestor.Pipeline;

/// <summary>
/// Single consumer loop: reads events from the channel, accumulates them into a batch and
/// flushes the batch to every sink when it is full or when the oldest event in it has waited
/// <see cref="PipelineOptions.FlushInterval"/>.
/// </summary>
/// <remarks>
/// <para>
/// One processor owns exactly one <see cref="EventBatch"/> and flushes it inline, so batches are
/// reused and the steady state allocates nothing per event. Throughput scales by running several
/// processors against the same channel (<see cref="PipelineOptions.ConsumerCount"/>).
/// </para>
/// <para>
/// Delivery to sinks is at-least-once: a failed write is retried with exponential backoff and a
/// batch interrupted by shutdown is flushed again during the drain.
/// </para>
/// </remarks>
public sealed partial class BatchProcessor
{
    private const double MaxBackoffMilliseconds = 5_000;

    private readonly int _workerId;
    private readonly ChannelReader<IngestEvent> _reader;
    private readonly IEventSink[] _sinks;
    private readonly PipelineOptions _options;
    private readonly IPipelineObserver _observer;
    private readonly ILogger _logger;
    private readonly LockFreeObjectPool<IngestEvent>? _eventPool;
    private readonly EventBatch _batch;
    private readonly long _flushIntervalTicks;

    private CancellationTokenSource? _deadlineCts;
    private long _sequence;

    public BatchProcessor(
        int workerId,
        ChannelReader<IngestEvent> reader,
        IReadOnlyList<IEventSink> sinks,
        PipelineOptions options,
        IPipelineObserver observer,
        ILogger logger,
        LockFreeObjectPool<IngestEvent>? eventPool = null)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(sinks);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(observer);
        ArgumentNullException.ThrowIfNull(logger);

        if (sinks.Count == 0)
        {
            throw new ArgumentException("At least one sink is required.", nameof(sinks));
        }

        _workerId = workerId;
        _reader = reader;
        _sinks = sinks.ToArray();
        _options = options;
        _observer = observer;
        _logger = logger;
        _eventPool = eventPool;
        _batch = new EventBatch(options.BatchSize);
        _flushIntervalTicks = (long)(options.FlushInterval.TotalSeconds * Stopwatch.Frequency);
    }

    private enum WaitSignal
    {
        DataAvailable,
        Timeout,
        Completed,
    }

    public int WorkerId => _workerId;

    public long BatchesFlushed => Interlocked.Read(ref _sequence);

    /// <summary>
    /// Runs until the channel is completed and drained, or until <paramref name="stoppingToken"/>
    /// is cancelled — in which case whatever is still buffered is flushed within
    /// <see cref="PipelineOptions.DrainTimeout"/>.
    /// </summary>
    public async Task RunAsync(CancellationToken stoppingToken)
    {
        LogStarted(_logger, _workerId, _options.BatchSize, _options.FlushInterval.TotalMilliseconds);
        var deadline = 0L;

        try
        {
            while (true)
            {
                // Fast path: take everything already buffered without going async.
                while (!_batch.IsFull && _reader.TryRead(out var evt))
                {
                    if (_batch.IsEmpty)
                    {
                        deadline = Stopwatch.GetTimestamp() + _flushIntervalTicks;
                    }

                    _batch.Add(evt);
                }

                if (_batch.IsFull || (!_batch.IsEmpty && Stopwatch.GetTimestamp() >= deadline))
                {
                    await FlushAsync(stoppingToken).ConfigureAwait(false);
                    continue;
                }

                WaitSignal signal;
                if (_batch.IsEmpty)
                {
                    // Nothing pending: no deadline, just wait for data.
                    signal = await _reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false)
                        ? WaitSignal.DataAvailable
                        : WaitSignal.Completed;
                }
                else
                {
                    signal = await WaitUntilAsync(deadline, stoppingToken).ConfigureAwait(false);
                }

                if (signal == WaitSignal.Completed)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host is stopping; fall through to the drain.
        }

        await DrainAsync().ConfigureAwait(false);
        LogStopped(_logger, _workerId, BatchesFlushed);
    }

    /// <summary>
    /// Waits for more data, but not past the flush deadline of the current batch.
    /// </summary>
    /// <remarks>
    /// The deadline token is created once per batch (not once per wait), so a trickle of events
    /// into a partially filled batch does not allocate a CancellationTokenSource per event.
    /// </remarks>
    private async ValueTask<WaitSignal> WaitUntilAsync(long deadline, CancellationToken stoppingToken)
    {
        if (_deadlineCts is null)
        {
            var remaining = Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(), deadline);
            if (remaining <= TimeSpan.Zero)
            {
                return WaitSignal.Timeout;
            }

            _deadlineCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            _deadlineCts.CancelAfter(remaining);
        }

        try
        {
            return await _reader.WaitToReadAsync(_deadlineCts.Token).ConfigureAwait(false)
                ? WaitSignal.DataAvailable
                : WaitSignal.Completed;
        }
        catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
        {
            return WaitSignal.Timeout;
        }
    }

    private async ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        _batch.Seal(_workerId, Interlocked.Increment(ref _sequence), DateTimeOffset.UtcNow);

        foreach (var sink in _sinks)
        {
            await WriteWithRetryAsync(sink, cancellationToken).ConfigureAwait(false);
        }

        ReleaseBatch();
    }

    private async ValueTask WriteWithRetryAsync(IEventSink sink, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            var start = Stopwatch.GetTimestamp();
            try
            {
                await sink.WriteBatchAsync(_batch, cancellationToken).ConfigureAwait(false);
                _observer.BatchFlushed(sink.Name, _batch.Count, Stopwatch.GetElapsedTime(start));
                return;
            }
            catch (Exception ex) when (!(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                _observer.BatchFailed(sink.Name, _batch.Count, attempt, ex);

                if (attempt > _options.MaxFlushRetries)
                {
                    LogBatchDropped(_logger, ex, _batch.Sequence, _batch.Count, sink.Name, attempt);
                    _observer.BatchDropped(sink.Name, _batch.Count);
                    return;
                }

                var delay = ComputeBackoff(attempt);
                LogFlushRetry(_logger, ex, sink.Name, _batch.Sequence, attempt, delay.TotalMilliseconds);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Exponential backoff (base * 2^(attempt-1)) with jitter in [50%, 100%), capped at 5 s.</summary>
    private TimeSpan ComputeBackoff(int attempt)
    {
        var exponential = _options.RetryBaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1);
        var jittered = exponential * (0.5 + (Random.Shared.NextDouble() * 0.5));
        return TimeSpan.FromMilliseconds(Math.Min(jittered, MaxBackoffMilliseconds));
    }

    private void ReleaseBatch()
    {
        // Every sink has finished with the batch (sinks must not retain events), so the events
        // and their payload buffers can be recycled.
        if (_eventPool is not null)
        {
            foreach (var evt in _batch.Span)
            {
                _eventPool.Return(evt);
            }
        }

        _batch.Clear();
        _deadlineCts?.Dispose();
        _deadlineCts = null;
    }

    private async ValueTask DrainAsync()
    {
        using var drainCts = new CancellationTokenSource(_options.DrainTimeout);

        try
        {
            while (true)
            {
                while (!_batch.IsFull && _reader.TryRead(out var evt))
                {
                    _batch.Add(evt);
                }

                if (_batch.IsEmpty)
                {
                    break;
                }

                await FlushAsync(drainCts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            var pending = _batch.Count + (_reader.CanCount ? _reader.Count : 0);
            LogDrainTimedOut(_logger, _workerId, pending);
        }
        finally
        {
            _deadlineCts?.Dispose();
            _deadlineCts = null;
        }
    }

    [LoggerMessage(EventId = 1001, Level = LogLevel.Information,
        Message = "Batch processor {WorkerId} started (batch size {BatchSize}, flush interval {FlushIntervalMs} ms)")]
    private static partial void LogStarted(ILogger logger, int workerId, int batchSize, double flushIntervalMs);

    [LoggerMessage(EventId = 1002, Level = LogLevel.Information,
        Message = "Batch processor {WorkerId} stopped after {Batches} batches")]
    private static partial void LogStopped(ILogger logger, int workerId, long batches);

    [LoggerMessage(EventId = 1003, Level = LogLevel.Warning,
        Message = "Sink {Sink} failed to write batch {Sequence} (attempt {Attempt}), retrying in {DelayMs} ms")]
    private static partial void LogFlushRetry(ILogger logger, Exception exception, string sink, long sequence, int attempt, double delayMs);

    [LoggerMessage(EventId = 1004, Level = LogLevel.Error,
        Message = "Dropping batch {Sequence} ({Count} events) for sink {Sink} after {Attempts} attempts")]
    private static partial void LogBatchDropped(ILogger logger, Exception exception, long sequence, int count, string sink, int attempts);

    [LoggerMessage(EventId = 1005, Level = LogLevel.Error,
        Message = "Batch processor {WorkerId} hit the drain timeout with {Pending} events still buffered")]
    private static partial void LogDrainTimedOut(ILogger logger, int workerId, int pending);
}
