var builder = WebApplication.CreateBuilder(args);

var app = builder.Build();

app.MapGet("/", () => "EventIngestor");

app.Run();
