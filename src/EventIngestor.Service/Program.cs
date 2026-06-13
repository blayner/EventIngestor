using EventIngestor.Core.Serialization;
using EventIngestor.Service.Extensions;
using EventIngestor.Service.GrpcServices;
using EventIngestor.Service.Health;
using EventIngestor.Service.Metrics;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEventIngestion(builder.Configuration);

builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = 16 * 1024 * 1024;
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});

builder.Services
    .AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.TypeInfoResolverChain.Insert(0, IngestorJsonContext.Default));

builder.Services.AddProblemDetails();
builder.Services.AddOpenApi();

builder.Services
    .AddHealthChecks()
    .AddCheck<PipelineHealthCheck>("pipeline", tags: ["ready"]);

var app = builder.Build();

app.UseRouting();
app.UseMiddleware<MetricsMiddleware>();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapGrpcService<EventIngestionGrpcService>();
app.MapControllers();

// Liveness: the process is up and serving requests. Readiness: the pipeline is draining.
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = _ => false });
app.MapHealthChecks("/health/ready", new HealthCheckOptions
{
    Predicate = check => check.Tags.Contains("ready"),
    ResponseWriter = HealthResponseWriter.WriteAsync,
});

app.MapMetrics();

app.Run();

/// <summary>Exposed for WebApplicationFactory in integration tests.</summary>
public partial class Program
{
}
