using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;
using EventIngestor.Core.Serialization;
using EventIngestor.Pipeline;
using Microsoft.AspNetCore.Mvc;

namespace EventIngestor.Service.Controllers;

/// <summary>
/// HTTP ingestion endpoints. Bodies are read straight from <see cref="HttpRequest.BodyReader"/> and
/// parsed with <see cref="EventParser"/>; MVC model binding is deliberately not used, so a request never
/// goes through a DOM or a DTO.
/// </summary>
/// <remarks>
/// Responses: <c>202</c> with per-event counts when everything was accepted or rejected as invalid;
/// <c>429</c> with the same counts plus <c>Retry-After</c> when any event was throttled (the client
/// should retry only those); <c>400</c>/<c>413</c> when the request as a whole is unusable.
/// </remarks>
[ApiController]
[Route("api/events")]
public sealed class EventsController(
    PipelineWriter writer,
    NdjsonPipelineReader ndjsonReader,
    Utf8StringCache stringCache,
    PipelineOptions options) : ControllerBase
{
    /// <summary>Ingests a single JSON event.</summary>
    [HttpPost]
    [ProducesResponseType<IngestResult>(StatusCodes.Status202Accepted)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status413PayloadTooLarge)]
    [ProducesResponseType<IngestResult>(StatusCodes.Status429TooManyRequests)]
    public async Task<IActionResult> IngestOne(CancellationToken cancellationToken)
    {
        using var body = await Request.BodyReader.ReadToEndAsync(options.MaxLineBytes, cancellationToken);
        if (body.IsTooLarge)
        {
            writer.Reject(Transports.Http, RejectReason.PayloadTooLarge);
            return PayloadTooLarge();
        }

        var evt = writer.Rent();
        var status = EventParser.TryParse(body.Span, evt, options.MaxPayloadBytes, stringCache);
        if (status != ParseStatus.Ok)
        {
            writer.Reject(evt, Transports.Http, status.ToRejectReason());
            return status == ParseStatus.PayloadTooLarge ? PayloadTooLarge() : InvalidRequest($"Event rejected: {status}.");
        }

        if (!await writer.WriteAsync(evt, Transports.Http, cancellationToken))
        {
            return Throttled(new IngestResult(0, 0, 1));
        }

        return Accepted(new IngestResult(1, 0, 0));
    }

    /// <summary>Ingests a JSON array of events. Invalid elements are counted and skipped.</summary>
    [HttpPost("batch")]
    [ProducesResponseType<IngestResult>(StatusCodes.Status202Accepted)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status400BadRequest)]
    [ProducesResponseType<ProblemDetails>(StatusCodes.Status413PayloadTooLarge)]
    [ProducesResponseType<IngestResult>(StatusCodes.Status429TooManyRequests)]
    public async Task<IActionResult> IngestBatch(CancellationToken cancellationToken)
    {
        using var body = await Request.BodyReader.ReadToEndAsync(options.MaxRequestBytes, cancellationToken);
        if (body.IsTooLarge)
        {
            writer.Reject(Transports.Http, RejectReason.PayloadTooLarge);
            return PayloadTooLarge();
        }

        var events = new List<IngestEvent>();
        var rejected = 0;
        var status = EventParser.ParseArray(
            body.Span,
            writer.Rent,
            (evt, reason) =>
            {
                rejected++;
                writer.Reject(evt, Transports.Http, reason.ToRejectReason());
            },
            events,
            options.MaxPayloadBytes,
            stringCache);

        if (status != ParseStatus.Ok)
        {
            foreach (var evt in events)
            {
                writer.Release(evt);
            }

            return InvalidRequest("Body must be a well-formed JSON array of events.");
        }

        var written = await writer.WriteAsync(events, Transports.Http, cancellationToken);
        var result = written with { Rejected = rejected };
        return result.Throttled > 0 ? Throttled(result) : Accepted(result);
    }

    /// <summary>
    /// Streams newline-delimited JSON. The body is parsed incrementally, so there is no request size
    /// limit: memory per request is bounded by the maximum line length.
    /// </summary>
    [HttpPost("ndjson")]
    [DisableRequestSizeLimit]
    [ProducesResponseType<IngestResult>(StatusCodes.Status202Accepted)]
    [ProducesResponseType<IngestResult>(StatusCodes.Status429TooManyRequests)]
    public async Task<IActionResult> IngestNdjson(CancellationToken cancellationToken)
    {
        var result = await ndjsonReader.ReadAsync(Request.BodyReader, Transports.Ndjson, cancellationToken);
        return result.Throttled > 0 ? Throttled(result) : Accepted(result);
    }

    private ObjectResult Throttled(IngestResult result)
    {
        Response.Headers.RetryAfter = "1";
        return StatusCode(StatusCodes.Status429TooManyRequests, result);
    }

    private ObjectResult PayloadTooLarge() =>
        Problem(
            statusCode: StatusCodes.Status413PayloadTooLarge,
            title: "Payload too large",
            detail: $"Events are limited to {options.MaxPayloadBytes} payload bytes and requests to {options.MaxRequestBytes} bytes.");

    private ObjectResult InvalidRequest(string detail) =>
        Problem(statusCode: StatusCodes.Status400BadRequest, title: "Invalid request", detail: detail);
}
