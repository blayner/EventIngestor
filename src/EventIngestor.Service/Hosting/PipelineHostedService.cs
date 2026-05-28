using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;
using EventIngestor.Pipeline;

namespace EventIngestor.Service.Hosting;

/// <summary>
/// Owns the lifecycle of the batch processors.
/// </summary>
/// <remarks>
/// Shutdown order matters: the pipeline is completed first so producers get an immediate
/// "shutting down" refusal instead of filling a buffer nobody will read, then the processors are
/// cancelled and drain what is left within <see cref="PipelineOptions.DrainTimeout"/>.
/// </remarks>
public sealed class PipelineHostedService(
    IEventPipeline pipeline,
    IEnumerable<IEventSink> sinks,
    PipelineOptions options,
    IPipelineObserver observer,
    LockFreeObjectPool<IngestEvent> eventPool,
    ILoggerFactory loggerFactory) : BackgroundService
{
    private readonly ILogger _logger = loggerFactory.CreateLogger<PipelineHostedService>();
    private int _activeWorkers;

    public int ConfiguredWorkers => options.ConsumerCount;

    public int ActiveWorkers => Volatile.Read(ref _activeWorkers);

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var sinkArray = sinks.ToArray();
        _logger.LogInformation(
            "Starting {Workers} batch processors writing to [{Sinks}]",
            options.ConsumerCount,
            string.Join(", ", sinkArray.Select(s => s.Name)));

        var workers = new Task[options.ConsumerCount];
        for (var i = 0; i < workers.Length; i++)
        {
            var processor = new BatchProcessor(
                i,
                pipeline.Reader,
                sinkArray,
                options,
                observer,
                loggerFactory.CreateLogger<BatchProcessor>(),
                eventPool);

            workers[i] = RunWorkerAsync(processor, stoppingToken);
        }

        return Task.WhenAll(workers);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        pipeline.Complete();
        await base.StopAsync(cancellationToken);
    }

    private async Task RunWorkerAsync(BatchProcessor processor, CancellationToken stoppingToken)
    {
        // Leave the synchronous part of StartAsync immediately.
        await Task.Yield();

        Interlocked.Increment(ref _activeWorkers);
        try
        {
            await processor.RunAsync(stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogCritical(ex, "Batch processor {WorkerId} crashed", processor.WorkerId);
        }
        finally
        {
            Interlocked.Decrement(ref _activeWorkers);
        }
    }
}
