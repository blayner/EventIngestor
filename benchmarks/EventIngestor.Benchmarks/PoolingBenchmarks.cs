using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;

namespace EventIngestor.Benchmarks;

/// <summary>
/// Cost of materializing one event: a fresh object plus a copied payload array and decoded strings
/// (what a straightforward implementation does) versus a pooled event, a reused ArrayPool buffer and
/// interned strings.
/// </summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class PoolingBenchmarks
{
    private static readonly byte[] SourceUtf8 = "checkout-api"u8.ToArray();
    private static readonly byte[] TypeUtf8 = "order.created"u8.ToArray();

    private readonly LockFreeObjectPool<IngestEvent> _pool = new(() => new IngestEvent(), maxRetained: 1_024);
    private readonly Utf8StringCache _stringCache = new();

    private byte[] _payload = [];

    [Params(256, 2048)]
    public int PayloadBytes { get; set; }

    [GlobalSetup]
    public void Setup() => _payload = SampleData.Payload(PayloadBytes);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Event")]
    public UnpooledEvent Allocate_PerEvent() => new()
    {
        Source = Encoding.UTF8.GetString(SourceUtf8),
        Type = Encoding.UTF8.GetString(TypeUtf8),
        Payload = _payload.AsSpan().ToArray(),
    };

    [Benchmark]
    [BenchmarkCategory("Event")]
    public int Pooled_Event()
    {
        var evt = _pool.Rent();
        evt.Source = _stringCache.GetOrAdd(SourceUtf8);
        evt.Type = _stringCache.GetOrAdd(TypeUtf8);
        evt.SetPayload(_payload);
        var length = evt.Payload.Length;
        _pool.Return(evt);
        return length;
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Strings")]
    public string Encoding_GetString() => Encoding.UTF8.GetString(TypeUtf8);

    [Benchmark]
    [BenchmarkCategory("Strings")]
    public string Utf8StringCache_GetOrAdd() => _stringCache.GetOrAdd(TypeUtf8);

    public sealed class UnpooledEvent
    {
        public required string Source { get; init; }

        public required string Type { get; init; }

        public required byte[] Payload { get; init; }
    }
}
