using EventIngestor.Service.Extensions;
using EventIngestor.Service.GrpcServices;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEventIngestion(builder.Configuration);

builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = 16 * 1024 * 1024;
    options.EnableDetailedErrors = builder.Environment.IsDevelopment();
});

var app = builder.Build();

app.MapGrpcService<EventIngestionGrpcService>();
app.MapGet("/", () => "EventIngestor: gRPC on :8081");

app.Run();
