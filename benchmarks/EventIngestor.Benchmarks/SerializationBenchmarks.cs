using System.Buffers;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;
using EventIngestor.Core.Serialization;

namespace EventIngestor.Benchmarks;

/// <summary>
/// Event (de)serialization: <see cref="JsonSerializer"/> with reflection and with source-generated
/// metadata (both materialize the payload as a <see cref="JsonElement"/>) versus the forward-only
/// <see cref="EventParser"/> that slices the payload as raw bytes.
/// </summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class SerializationBenchmarks
{
    private static readonly JsonSerializerOptions ReflectionOptions = new(JsonSerializerDefaults.Web);

    private readonly ArrayBufferWriter<byte> _output = new(64 * 1024);
    private readonly Utf8StringCache _stringCache = new();
    private readonly IngestEvent _target = new();

    private byte[] _json = [];
    private IngestEvent _event = new();
    private IngestEventDto _dto = null!;
    private Utf8JsonWriter _writer = null!;

    [Params(128, 4096)]
    public int PayloadBytes { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _json = SampleData.EventJson(PayloadBytes);
        _event = new IngestEvent();
        EventParser.TryParse(_json, _event);
        _dto = JsonSerializer.Deserialize(_json, IngestorJsonContext.Default.IngestEventDto)!;
        _writer = new Utf8JsonWriter(_output);
    }

    [GlobalCleanup]
    public void Cleanup() => _writer.Dispose();

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Deserialize")]
    public IngestEventDto? JsonSerializer_Reflection() =>
        JsonSerializer.Deserialize<IngestEventDto>(_json, ReflectionOptions);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public IngestEventDto? JsonSerializer_SourceGenerated() =>
        JsonSerializer.Deserialize(_json, IngestorJsonContext.Default.IngestEventDto);

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public ParseStatus EventParser_Utf8JsonReader()
    {
        _target.TryReset();
        return EventParser.TryParse(_json, _target);
    }

    [Benchmark]
    [BenchmarkCategory("Deserialize")]
    public ParseStatus EventParser_WithStringCache()
    {
        _target.TryReset();
        return EventParser.TryParse(_json, _target, stringCache: _stringCache);
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Serialize")]
    public int JsonSerializer_SourceGenerated_Write()
    {
        _output.ResetWrittenCount();
        _writer.Reset(_output);
        JsonSerializer.Serialize(_writer, _dto, IngestorJsonContext.Default.IngestEventDto);
        return _output.WrittenCount;
    }

    [Benchmark]
    [BenchmarkCategory("Serialize")]
    public int EventSerializer_RawPayload()
    {
        _output.ResetWrittenCount();
        _writer.Reset(_output);
        EventSerializer.Write(_writer, _event);
        _writer.Flush();
        return _output.WrittenCount;
    }
}
