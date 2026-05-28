using System.Numerics;
using System.Text;

namespace EventIngestor.Core.Pooling;

/// <summary>
/// Direct-mapped cache from UTF-8 bytes to <see cref="string"/> instances.
/// </summary>
/// <remarks>
/// <para>
/// <c>source</c> and <c>type</c> have very low cardinality (a few dozen services, a few hundred event
/// types) but appear on every event. Decoding them per event means two string allocations per event
/// that die young; with this cache the steady state allocates none.
/// </para>
/// <para>
/// Each slot holds an immutable entry, so readers and writers race safely without locks: a reader
/// sees either the old or the new entry and verifies the bytes before using it. Collisions simply
/// overwrite the slot (it is a cache, not a dictionary), and inputs longer than <c>maxLength</c>
/// bypass it entirely.
/// </para>
/// </remarks>
public sealed class Utf8StringCache
{
    private readonly Entry?[] _entries;
    private readonly int _mask;
    private readonly int _maxLength;

    public Utf8StringCache(int capacity = 1024, int maxLength = 128)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxLength);

        var size = (int)BitOperations.RoundUpToPowerOf2((uint)capacity);
        _entries = new Entry?[size];
        _mask = size - 1;
        _maxLength = maxLength;
    }

    public string GetOrAdd(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
        {
            return string.Empty;
        }

        if (utf8.Length > _maxLength)
        {
            return Encoding.UTF8.GetString(utf8);
        }

        var hash = new HashCode();
        hash.AddBytes(utf8);
        var index = hash.ToHashCode() & _mask;

        var entry = Volatile.Read(ref _entries[index]);
        if (entry is not null && utf8.SequenceEqual(entry.Utf8))
        {
            return entry.Value;
        }

        var value = Encoding.UTF8.GetString(utf8);
        Volatile.Write(ref _entries[index], new Entry(utf8.ToArray(), value));
        return value;
    }

    private sealed class Entry(byte[] utf8, string value)
    {
        public byte[] Utf8 { get; } = utf8;

        public string Value { get; } = value;
    }
}
