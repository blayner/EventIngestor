using EventIngestor.Core.Abstractions;
using EventIngestor.Pipeline;
using EventIngestor.Service.GrpcServices;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<PipelineOptions>(builder.Configuration.GetSection(PipelineOptions.SectionName));
builder.Services.AddSingleton(sp => sp.GetRequiredService<IOptions<PipelineOptions>>().Value);
builder.Services.AddSingleton<IPipelineObserver>(NullPipelineObserver.Instance);
builder.Services.AddSingleton<ChannelPipeline>();
builder.Services.AddSingleton<IEventPipeline>(sp => sp.GetRequiredService<ChannelPipeline>());
builder.Services.AddSingleton<PipelineWriter>();

builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = 16 * 1024 * 1024;
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});

var app = builder.Build();

app.MapGrpcService<EventIngestionGrpcService>();
app.MapGet("/", () => "EventIngestor: gRPC on :8081");

app.Run();
