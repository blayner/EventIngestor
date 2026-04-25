namespace EventIngestor.Core.Abstractions;

/// <summary>
/// Hook for instrumentation. Keeps the pipeline free of any metrics library: the host
/// decides how (and whether) these signals are exported.
/// </summary>
/// <remarks>Implementations are called on hot paths and must be cheap and thread-safe.</remarks>
public interface IPipelineObserver
{
    void EventsAccepted(string transport, int count);

    void EventsRejected(string transport, RejectReason reason, int count);

    void BatchFlushed(string sink, int size, TimeSpan duration);

    void BatchFailed(string sink, int size, int attempt, Exception exception);

    void BatchDropped(string sink, int size);
}

public sealed class NullPipelineObserver : IPipelineObserver
{
    public static readonly NullPipelineObserver Instance = new();

    private NullPipelineObserver()
    {
    }

    public void EventsAccepted(string transport, int count)
    {
    }

    public void EventsRejected(string transport, RejectReason reason, int count)
    {
    }

    public void BatchFlushed(string sink, int size, TimeSpan duration)
    {
    }

    public void BatchFailed(string sink, int size, int attempt, Exception exception)
    {
    }

    public void BatchDropped(string sink, int size)
    {
    }
}
