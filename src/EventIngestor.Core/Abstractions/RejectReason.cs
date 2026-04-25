using EventIngestor.Core.Serialization;

namespace EventIngestor.Core.Abstractions;

public enum RejectReason
{
    InvalidEvent,
    PayloadTooLarge,
    Backpressure,
    ShuttingDown,
}

public static class RejectReasonExtensions
{
    public static RejectReason ToRejectReason(this ParseStatus status) => status switch
    {
        ParseStatus.PayloadTooLarge => RejectReason.PayloadTooLarge,
        ParseStatus.Ok => throw new ArgumentException("A successful parse is not a rejection.", nameof(status)),
        _ => RejectReason.InvalidEvent,
    };

    /// <summary>Low-cardinality label used by metrics and logs.</summary>
    public static string ToLabel(this RejectReason reason) => reason switch
    {
        RejectReason.InvalidEvent => "invalid_event",
        RejectReason.PayloadTooLarge => "payload_too_large",
        RejectReason.Backpressure => "backpressure",
        RejectReason.ShuttingDown => "shutting_down",
        _ => "unknown",
    };
}
