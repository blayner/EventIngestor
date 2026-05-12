using EventIngestor.Core.Abstractions;
using EventIngestor.Pipeline;
using EventIngestor.Pipeline.Sinks;
using EventIngestor.Service.Hosting;
using Microsoft.Extensions.Options;

namespace EventIngestor.Service.Extensions;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddEventIngestion(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<PipelineOptions>()
            .Bind(configuration.GetSection(PipelineOptions.SectionName))
            .Validate(o => !o.GetValidationErrors().Any(), $"Invalid '{PipelineOptions.SectionName}' configuration.")
            .ValidateOnStart();
        services.AddSingleton(sp => sp.GetRequiredService<IOptions<PipelineOptions>>().Value);

        services.AddSingleton<IPipelineObserver>(NullPipelineObserver.Instance);
        services.AddSingleton<ChannelPipeline>();
        services.AddSingleton<IEventPipeline>(sp => sp.GetRequiredService<ChannelPipeline>());
        services.AddSingleton<PipelineWriter>();

        services.AddEventSinks(configuration);

        services.AddSingleton<PipelineHostedService>();
        services.AddHostedService(sp => sp.GetRequiredService<PipelineHostedService>());

        return services;
    }

    private static void AddEventSinks(this IServiceCollection services, IConfiguration configuration)
    {
        var fileOptions = configuration.GetSection(FileSinkOptions.SectionName).Get<FileSinkOptions>() ?? new FileSinkOptions();
        var loggingEnabled = configuration.GetValue("Ingestor:Sinks:Logging:Enabled", false);

        if (fileOptions.Enabled)
        {
            services.AddSingleton(fileOptions);
            services.AddSingleton<IEventSink, FileEventSink>();
        }

        if (loggingEnabled)
        {
            services.AddSingleton<IEventSink, LoggingEventSink>();
        }

        if (!fileOptions.Enabled && !loggingEnabled)
        {
            services.AddSingleton<IEventSink, NullEventSink>();
        }
    }
}
