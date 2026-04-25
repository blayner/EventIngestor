namespace EventIngestor.Core.Abstractions;

/// <summary>Transport labels used in metrics.</summary>
public static class Transports
{
    public const string Grpc = "grpc";
    public const string Http = "http";
    public const string Ndjson = "ndjson";
}
