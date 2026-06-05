using System.Buffers;

namespace EventIngestor.Pipeline;

/// <summary>
/// Contiguous copy of a request body held in an <see cref="ArrayPool{T}"/> array.
/// Dispose exactly once to give the array back.
/// </summary>
public readonly struct PooledBuffer : IDisposable
{
    private readonly byte[]? _array;
    private readonly int _length;

    private PooledBuffer(byte[]? array, int length, bool isTooLarge)
    {
        _array = array;
        _length = length;
        IsTooLarge = isTooLarge;
    }

    /// <summary>Marker returned when the body exceeded the allowed size; holds no data.</summary>
    public static PooledBuffer TooLarge => new(null, 0, isTooLarge: true);

    public bool IsTooLarge { get; }

    public int Length => _length;

    public ReadOnlySpan<byte> Span => _array is null ? ReadOnlySpan<byte>.Empty : _array.AsSpan(0, _length);

    public static PooledBuffer CopyFrom(in ReadOnlySequence<byte> source)
    {
        if (source.IsEmpty)
        {
            return default;
        }

        var length = checked((int)source.Length);
        var array = ArrayPool<byte>.Shared.Rent(length);
        source.CopyTo(array);
        return new PooledBuffer(array, length, isTooLarge: false);
    }

    public void Dispose()
    {
        if (_array is not null)
        {
            ArrayPool<byte>.Shared.Return(_array);
        }
    }
}
