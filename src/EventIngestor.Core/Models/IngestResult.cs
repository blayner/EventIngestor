namespace EventIngestor.Core.Models;

/// <summary>
/// Outcome of an ingestion request.
/// </summary>
/// <param name="Accepted">Events written to the pipeline.</param>
/// <param name="Rejected">Events refused because they were invalid.</param>
/// <param name="Throttled">Valid events refused because the pipeline was saturated.</param>
public readonly record struct IngestResult(int Accepted, int Rejected, int Throttled)
{
    public int Total => Accepted + Rejected + Throttled;

    public static IngestResult operator +(IngestResult left, IngestResult right) =>
        new(left.Accepted + right.Accepted, left.Rejected + right.Rejected, left.Throttled + right.Throttled);
}
