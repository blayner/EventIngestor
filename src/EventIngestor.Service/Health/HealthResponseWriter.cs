using System.Text.Json;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace EventIngestor.Service.Health;

/// <summary>Writes a <see cref="HealthReport"/> as JSON (status, duration and each check's data).</summary>
public static class HealthResponseWriter
{
    public static async Task WriteAsync(HttpContext context, HealthReport report)
    {
        context.Response.ContentType = "application/json; charset=utf-8";

        await using var writer = new Utf8JsonWriter(context.Response.BodyWriter);
        writer.WriteStartObject();
        writer.WriteString("status", report.Status.ToString());
        writer.WriteNumber("durationMs", report.TotalDuration.TotalMilliseconds);
        writer.WriteStartObject("checks");

        foreach (var (name, entry) in report.Entries)
        {
            writer.WriteStartObject(name);
            writer.WriteString("status", entry.Status.ToString());
            if (entry.Description is not null)
            {
                writer.WriteString("description", entry.Description);
            }

            if (entry.Data.Count > 0)
            {
                writer.WriteStartObject("data");
                foreach (var (key, value) in entry.Data)
                {
                    writer.WritePropertyName(key);
                    switch (value)
                    {
                        case int i:
                            writer.WriteNumberValue(i);
                            break;
                        case double d:
                            writer.WriteNumberValue(d);
                            break;
                        default:
                            writer.WriteStringValue(value?.ToString());
                            break;
                    }
                }

                writer.WriteEndObject();
            }

            writer.WriteEndObject();
        }

        writer.WriteEndObject();
        writer.WriteEndObject();
        await writer.FlushAsync();
    }
}
