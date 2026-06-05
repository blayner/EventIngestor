using EventIngestor.Core.Serialization;
using EventIngestor.Service.Extensions;
using EventIngestor.Service.GrpcServices;

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

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.MapGrpcService<EventIngestionGrpcService>();
app.MapControllers();

app.Run();

/// <summary>Exposed for WebApplicationFactory in integration tests.</summary>
public partial class Program
{
}
