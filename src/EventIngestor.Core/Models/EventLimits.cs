namespace EventIngestor.Core.Models;

public static class EventLimits
{
    /// <summary>Maximum length, in UTF-8 bytes, of <c>source</c> and <c>type</c>.</summary>
    public const int MaxNameLength = 128;

    /// <summary>Default maximum size of a single event payload.</summary>
    public const int DefaultMaxPayloadBytes = 256 * 1024;
}
