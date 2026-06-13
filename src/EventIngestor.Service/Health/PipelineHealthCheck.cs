using EventIngestor.Core.Abstractions;
using EventIngestor.Service.Hosting;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace EventIngestor.Service.Health;

/// <summary>
/// Readiness of the ingestion pipeline.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><b>Unhealthy</b>: no batch processor is running, so nothing is being drained.</item>
/// <item><b>Degraded</b>: the channel is at least <see cref="SaturationThreshold"/> full; requests are
/// still served but throttling is imminent. Reported as 200 so a load balancer keeps the instance,
/// while alerting can key off the status.</item>
/// </list>
/// </remarks>
public sealed class PipelineHealthCheck(IEventPipeline pipeline, PipelineHostedService host) : IHealthCheck
{
    public const double SaturationThreshold = 0.9;

    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var depth = pipeline.Count;
        var utilization = (double)depth / pipeline.Capacity;
        var data = new Dictionary<string, object>
        {
            ["queueDepth"] = depth,
            ["queueCapacity"] = pipeline.Capacity,
            ["utilization"] = Math.Round(utilization, 4),
            ["activeWorkers"] = host.ActiveWorkers,
            ["configuredWorkers"] = host.ConfiguredWorkers,
        };

        if (host.ActiveWorkers == 0)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("No batch processor is running.", data: data));
        }

        if (utilization >= SaturationThreshold)
        {
            return Task.FromResult(HealthCheckResult.Degraded($"Pipeline is {utilization:P0} full.", data: data));
        }

        return Task.FromResult(HealthCheckResult.Healthy("Pipeline is draining normally.", data));
    }
}
