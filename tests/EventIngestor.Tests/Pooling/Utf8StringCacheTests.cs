using System.Text;
using EventIngestor.Core.Pooling;

namespace EventIngestor.Tests.Pooling;

public class Utf8StringCacheTests
{
    [Fact]
    public void Returns_the_same_instance_for_repeated_values()
    {
        var cache = new Utf8StringCache();

        var first = cache.GetOrAdd("checkout-api"u8);
        var second = cache.GetOrAdd("checkout-api"u8);

        Assert.Equal("checkout-api", first);
        Assert.Same(first, second);
    }

    [Fact]
    public void Decodes_non_ascii_values()
    {
        var cache = new Utf8StringCache();
        var bytes = Encoding.UTF8.GetBytes("pagamento.concluído");

        Assert.Equal("pagamento.concluído", cache.GetOrAdd(bytes));
    }

    [Fact]
    public void Stays_correct_when_every_value_collides()
    {
        var cache = new Utf8StringCache(capacity: 1);

        for (var round = 0; round < 3; round++)
        {
            Assert.Equal("a", cache.GetOrAdd("a"u8));
            Assert.Equal("b", cache.GetOrAdd("b"u8));
        }
    }

    [Fact]
    public void Values_over_the_max_length_bypass_the_cache()
    {
        var cache = new Utf8StringCache(maxLength: 4);

        var first = cache.GetOrAdd("too-long"u8);
        var second = cache.GetOrAdd("too-long"u8);

        Assert.Equal(first, second);
        Assert.NotSame(first, second);
    }

    [Fact]
    public void Empty_input_returns_string_empty()
    {
        Assert.Same(string.Empty, new Utf8StringCache().GetOrAdd(ReadOnlySpan<byte>.Empty));
    }
}
