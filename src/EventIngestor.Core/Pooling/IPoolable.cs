namespace EventIngestor.Core.Pooling;

/// <summary>An object that can clear its state before going back into a pool.</summary>
public interface IPoolable
{
    /// <summary>
    /// Resets the instance. Returning <c>false</c> tells the pool the instance must not be reused.
    /// </summary>
    bool TryReset();
}
