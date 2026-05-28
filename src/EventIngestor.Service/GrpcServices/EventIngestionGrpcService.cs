using System.Text;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Serialization;
using EventIngestor.Pipeline;
using EventIngestor.Service.Protos;
using Grpc.Core;

namespace EventIngestor.Service.GrpcServices;

public sealed class EventIngestionGrpcService(
    PipelineWriter writer,
    PipelineOptions options,
    ILogger<EventIngestionGrpcService> logger) : EventIngestion.EventIngestionBase
{
    /// <summary>Events from a client stream are forwarded to the pipeline in chunks of this size.</summary>
    private const int StreamChunkSize = 256;

    public override async Task<IngestReply> Ingest(IngestRequest request, ServerCallContext context)
    {
        var evt = writer.Rent();
        var rejection = Populate(request, evt);
        if (rejection is { } reason)
        {
            writer.Reject(evt, Transports.Grpc, reason);
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"Event rejected: {reason.ToLabel()}."));
        }

        if (!await writer.WriteAsync(evt, Transports.Grpc, context.CancellationToken))
        {
            throw new RpcException(new Status(StatusCode.ResourceExhausted, "Ingestion pipeline is saturated, retry with backoff."));
        }

        return new IngestReply { Accepted = 1 };
    }

    public override async Task<IngestReply> IngestBatch(IngestBatchRequest request, ServerCallContext context)
    {
        var events = new List<IngestEvent>(request.Events.Count);
        var rejected = 0;

        foreach (var item in request.Events)
        {
            var evt = writer.Rent();
            if (Populate(item, evt) is { } reason)
            {
                rejected++;
                writer.Reject(evt, Transports.Grpc, reason);
                continue;
            }

            events.Add(evt);
        }

        var result = await writer.WriteAsync(events, Transports.Grpc, context.CancellationToken);
        return ToReply(result with { Rejected = result.Rejected + rejected });
    }

    public override async Task<IngestReply> IngestStream(IAsyncStreamReader<IngestRequest> requestStream, ServerCallContext context)
    {
        var total = default(IngestResult);
        var chunk = new List<IngestEvent>(StreamChunkSize);
        var ct = context.CancellationToken;

        await foreach (var item in requestStream.ReadAllAsync(ct))
        {
            var evt = writer.Rent();
            if (Populate(item, evt) is { } reason)
            {
                total += new IngestResult(0, 1, 0);
                writer.Reject(evt, Transports.Grpc, reason);
                continue;
            }

            chunk.Add(evt);
            if (chunk.Count == StreamChunkSize)
            {
                total += await writer.WriteAsync(chunk, Transports.Grpc, ct);
                chunk.Clear();
            }
        }

        if (chunk.Count > 0)
        {
            total += await writer.WriteAsync(chunk, Transports.Grpc, ct);
        }

        logger.LogDebug(
            "Stream from {Peer} completed: {Accepted} accepted, {Rejected} rejected, {Throttled} throttled",
            context.Peer, total.Accepted, total.Rejected, total.Throttled);

        return ToReply(total);
    }

    /// <summary>Validates the request and copies it into <paramref name="evt"/>. Returns the rejection reason, if any.</summary>
    private RejectReason? Populate(IngestRequest request, IngestEvent evt)
    {
        if (!IsValidName(request.Source) || !IsValidName(request.Type))
        {
            return RejectReason.InvalidEvent;
        }

        var payload = request.Payload.Span;
        if (payload.Length > options.MaxPayloadBytes)
        {
            return RejectReason.PayloadTooLarge;
        }

        // Payload bytes are written raw to sinks later on, so they must be valid JSON now.
        if (!payload.IsEmpty && !EventParser.IsValidJsonValue(payload))
        {
            return RejectReason.InvalidEvent;
        }

        var id = Guid.Empty;
        if (request.Id.Length > 0 && !Guid.TryParse(request.Id, out id))
        {
            return RejectReason.InvalidEvent;
        }

        if (!TryGetTimestamp(request, out var timestamp))
        {
            return RejectReason.InvalidEvent;
        }

        evt.Id = id;
        evt.Timestamp = timestamp;
        evt.Source = request.Source;
        evt.Type = request.Type;
        evt.SetPayload(payload);
        evt.EnsureDefaults(DateTimeOffset.UtcNow);
        return null;
    }

    private static bool TryGetTimestamp(IngestRequest request, out DateTimeOffset timestamp)
    {
        timestamp = default;
        if (request.Timestamp is null)
        {
            return true;
        }

        try
        {
            timestamp = request.Timestamp.ToDateTimeOffset();
            return true;
        }
        catch (InvalidOperationException)
        {
            // Out-of-range or non-normalized protobuf Timestamp.
            return false;
        }
    }

    private static bool IsValidName(string value) =>
        value.Length > 0 && Encoding.UTF8.GetByteCount(value) <= EventLimits.MaxNameLength;

    private static IngestReply ToReply(IngestResult result) => new()
    {
        Accepted = (uint)result.Accepted,
        Rejected = (uint)result.Rejected,
        Throttled = (uint)result.Throttled,
    };
}
