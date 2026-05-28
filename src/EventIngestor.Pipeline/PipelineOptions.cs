using EventIngestor.Core.Models;

namespace EventIngestor.Pipeline;

public sealed class PipelineOptions
{
    public const string SectionName = "Ingestor:Pipeline";

    /// <summary>
    /// Bounded channel capacity. This is the memory budget of the service: once full,
    /// producers are pushed back (429 / RESOURCE_EXHAUSTED) instead of growing the heap.
    /// </summary>
    public int ChannelCapacity { get; set; } = 100_000;

    /// <summary>Maximum number of events per flushed batch.</summary>
    public int BatchSize { get; set; } = 1_000;

    /// <summary>Maximum time an event waits in a partially filled batch.</summary>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Number of concurrent batch processors reading from the channel.</summary>
    public int ConsumerCount { get; set; } = Math.Max(1, Environment.ProcessorCount / 2);

    /// <summary>How long a producer waits for free space before the event is throttled.</summary>
    public TimeSpan EnqueueTimeout { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Retries per sink before a batch is dropped for that sink.</summary>
    public int MaxFlushRetries { get; set; } = 3;

    /// <summary>Base delay of the exponential backoff between flush retries.</summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Time budget to flush buffered events during shutdown.</summary>
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(10);

    public int MaxPayloadBytes { get; set; } = EventLimits.DefaultMaxPayloadBytes;

    /// <summary>Maximum length of a single NDJSON line (payload plus envelope).</summary>
    public int MaxLineBytes { get; set; } = 512 * 1024;

    /// <summary>
    /// Maximum number of idle <c>IngestEvent</c> instances kept for reuse. Each retained instance may
    /// hold a payload buffer of up to <c>IngestEvent.MaxRetainedPayloadBytes</c>, which bounds the
    /// memory the pool can pin while idle.
    /// </summary>
    public int EventPoolSize { get; set; } = 16_384;

    public IEnumerable<string> GetValidationErrors()
    {
        if (EventPoolSize < 0)
        {
            yield return $"{nameof(EventPoolSize)} cannot be negative.";
        }

        if (ChannelCapacity <= 0)
        {
            yield return $"{nameof(ChannelCapacity)} must be positive.";
        }

        if (BatchSize <= 0)
        {
            yield return $"{nameof(BatchSize)} must be positive.";
        }

        if (FlushInterval <= TimeSpan.Zero)
        {
            yield return $"{nameof(FlushInterval)} must be positive.";
        }

        if (ConsumerCount is <= 0 or > 256)
        {
            yield return $"{nameof(ConsumerCount)} must be between 1 and 256.";
        }

        if (EnqueueTimeout < TimeSpan.Zero)
        {
            yield return $"{nameof(EnqueueTimeout)} cannot be negative.";
        }

        if (MaxFlushRetries < 0)
        {
            yield return $"{nameof(MaxFlushRetries)} cannot be negative.";
        }

        if (RetryBaseDelay < TimeSpan.Zero)
        {
            yield return $"{nameof(RetryBaseDelay)} cannot be negative.";
        }

        if (MaxPayloadBytes <= 0)
        {
            yield return $"{nameof(MaxPayloadBytes)} must be positive.";
        }

        if (MaxLineBytes <= MaxPayloadBytes)
        {
            yield return $"{nameof(MaxLineBytes)} must be greater than {nameof(MaxPayloadBytes)}.";
        }
    }

    public void Validate()
    {
        var errors = GetValidationErrors().ToArray();
        if (errors.Length > 0)
        {
            throw new ArgumentException("Invalid pipeline options: " + string.Join(" ", errors));
        }
    }
}
