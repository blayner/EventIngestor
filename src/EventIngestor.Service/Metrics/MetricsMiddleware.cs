using System.Diagnostics;
using System.Globalization;
using Prometheus;

namespace EventIngestor.Service.Metrics;

/// <summary>
/// Request duration and concurrency for ingestion traffic (REST and gRPC).
/// </summary>
/// <remarks>
/// The endpoint label is the route template (or the gRPC method path), never the raw URL, to keep
/// label cardinality bounded. Scrapes and health probes are excluded so they do not skew latency.
/// For gRPC the status label is the HTTP status (200); the gRPC status lives in the trailers.
/// </remarks>
public sealed class MetricsMiddleware(RequestDelegate next)
{
    private static readonly Histogram RequestDuration = Prometheus.Metrics.CreateHistogram(
        "ingestor_http_request_duration_seconds",
        "Duration of ingestion requests.",
        new HistogramConfiguration
        {
            LabelNames = ["method", "endpoint", "status"],
            Buckets = Histogram.ExponentialBuckets(0.0005, 2, 16),
        });

    private static readonly Gauge InFlight = Prometheus.Metrics.CreateGauge(
        "ingestor_http_requests_in_flight",
        "Ingestion requests currently being processed.");

    public async Task InvokeAsync(HttpContext context)
    {
        var path = context.Request.Path;
        if (path.StartsWithSegments("/metrics") || path.StartsWithSegments("/health"))
        {
            await next(context);
            return;
        }

        var start = Stopwatch.GetTimestamp();
        InFlight.Inc();
        try
        {
            await next(context);
        }
        finally
        {
            InFlight.Dec();

            var endpoint = (context.GetEndpoint() as RouteEndpoint)?.RoutePattern.RawText ?? "unmatched";
            RequestDuration
                .WithLabels(context.Request.Method, endpoint, StatusLabel(context.Response.StatusCode))
                .Observe(Stopwatch.GetElapsedTime(start).TotalSeconds);
        }
    }

    private static string StatusLabel(int statusCode) => statusCode switch
    {
        200 => "200",
        202 => "202",
        400 => "400",
        413 => "413",
        429 => "429",
        500 => "500",
        503 => "503",
        _ => statusCode.ToString(CultureInfo.InvariantCulture),
    };
}
