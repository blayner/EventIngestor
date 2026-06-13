using System.Net;
using System.Net.Http.Json;
using System.Text;
using EventIngestor.Core.Models;

namespace EventIngestor.Tests.Api;

public sealed class ApiTests(IngestorFactory factory) : IClassFixture<IngestorFactory>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static StringContent Json(string body, string mediaType = "application/json") =>
        new(body, Encoding.UTF8, mediaType);

    [Fact]
    public async Task Single_event_is_accepted()
    {
        using var client = factory.CreateClient();

        var response = await client.PostAsync("/api/events", Json("""{"source":"checkout-api","type":"order.created","payload":{"orderId":1}}"""), Ct);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(new IngestResult(1, 0, 0), await response.Content.ReadFromJsonAsync<IngestResult>(Ct));
    }

    [Fact]
    public async Task Invalid_event_returns_problem_details()
    {
        using var client = factory.CreateClient();

        var response = await client.PostAsync("/api/events", Json("""{"type":"no-source"}"""), Ct);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal("application/problem+json", response.Content.Headers.ContentType?.MediaType);
    }

    [Fact]
    public async Task Batch_reports_per_event_counts()
    {
        using var client = factory.CreateClient();
        const string body = """[{"source":"a","type":"t"},{"source":"b"},{"source":"c","type":"t"}]""";

        var response = await client.PostAsync("/api/events/batch", Json(body), Ct);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(new IngestResult(2, 1, 0), await response.Content.ReadFromJsonAsync<IngestResult>(Ct));
    }

    [Fact]
    public async Task Malformed_batch_is_rejected_as_a_whole()
    {
        using var client = factory.CreateClient();

        var response = await client.PostAsync("/api/events/batch", Json("""[{"source":"a","type":"t"}"""), Ct);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Ndjson_stream_is_ingested_line_by_line()
    {
        using var client = factory.CreateClient();
        var body = string.Join("\n", Enumerable.Range(0, 3).Select(i => $$"""{"source":"s","type":"t{{i}}"}"""));

        var response = await client.PostAsync("/api/events/ndjson", Json(body, "application/x-ndjson"), Ct);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        Assert.Equal(new IngestResult(3, 0, 0), await response.Content.ReadFromJsonAsync<IngestResult>(Ct));
    }

    [Fact]
    public async Task Readiness_reports_pipeline_state()
    {
        using var client = factory.CreateClient();

        // Workers start asynchronously; give them a moment on a cold host.
        HttpResponseMessage? response = null;
        for (var attempt = 0; attempt < 50; attempt++)
        {
            response = await client.GetAsync("/health/ready", Ct);
            if (response.StatusCode == HttpStatusCode.OK)
            {
                break;
            }

            await Task.Delay(20, Ct);
        }

        Assert.Equal(HttpStatusCode.OK, response!.StatusCode);
        var json = await response.Content.ReadAsStringAsync(Ct);
        Assert.Contains("\"pipeline\"", json);
        Assert.Contains("\"activeWorkers\"", json);
    }

    [Fact]
    public async Task Liveness_is_always_ok()
    {
        using var client = factory.CreateClient();

        var response = await client.GetAsync("/health/live", Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Metrics_expose_ingestion_counters()
    {
        using var client = factory.CreateClient();
        await client.PostAsync("/api/events", Json("""{"source":"m","type":"t"}"""), Ct);

        var metrics = await client.GetStringAsync("/metrics", Ct);

        Assert.Contains("ingestor_events_accepted_total", metrics);
        Assert.Contains("ingestor_pipeline_queue_capacity", metrics);
        Assert.Contains("ingestor_http_request_duration_seconds", metrics);
    }
}
