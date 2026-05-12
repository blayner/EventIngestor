namespace EventIngestor.Pipeline.Sinks;

public sealed class FileSinkOptions
{
    public const string SectionName = "Ingestor:Sinks:File";

    public bool Enabled { get; set; }

    public string OutputDirectory { get; set; } = "data/events";

    /// <summary>Size after which the current file is closed and a new one is started.</summary>
    public long MaxFileBytes { get; set; } = 256L * 1024 * 1024;

    /// <summary>
    /// fsync after every batch. Trades throughput for durability against power loss;
    /// without it a process crash is still safe, a kernel crash is not.
    /// </summary>
    public bool FlushToDisk { get; set; }
}
