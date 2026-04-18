using System.Text.Json;
using System.Text.Json.Serialization;
using EventIngestor.Core.Models;

namespace EventIngestor.Core.Serialization;

/// <summary>
/// DTO shape of an event for callers that prefer <see cref="JsonSerializer"/> over
/// <see cref="EventParser"/> (tooling, tests and the serialization benchmarks baseline).
/// </summary>
public sealed record IngestEventDto(
    Guid? Id,
    DateTimeOffset? Timestamp,
    string Source,
    string Type,
    JsonElement? Payload);

/// <summary>
/// Source-generated serialization metadata: no reflection at runtime, trimming/AOT friendly.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
[JsonSerializable(typeof(IngestEventDto))]
[JsonSerializable(typeof(IngestEventDto[]))]
[JsonSerializable(typeof(IngestResult))]
public partial class IngestorJsonContext : JsonSerializerContext
{
}
