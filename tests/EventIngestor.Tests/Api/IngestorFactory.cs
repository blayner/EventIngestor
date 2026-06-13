using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;

namespace EventIngestor.Tests.Api;

/// <summary>
/// In-memory host. The "Testing" environment loads appsettings.Testing.json, which disables the
/// file sink and shortens the flush interval.
/// </summary>
public sealed class IngestorFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder) => builder.UseEnvironment("Testing");
}
