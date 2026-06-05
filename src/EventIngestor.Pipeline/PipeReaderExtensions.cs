using System.IO.Pipelines;

namespace EventIngestor.Pipeline;

public static class PipeReaderExtensions
{
    /// <summary>
    /// Reads the whole body into a pooled buffer, giving up as soon as it grows past
    /// <paramref name="maxBytes"/> (so an oversized body is never fully buffered).
    /// </summary>
    /// <remarks>
    /// Data is left in the pipe until the body is complete (<c>AdvanceTo(start, end)</c>), then copied
    /// once. For single-document endpoints that is cheaper and simpler than parsing incrementally.
    /// </remarks>
    public static async ValueTask<PooledBuffer> ReadToEndAsync(
        this PipeReader reader,
        long maxBytes,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);

        while (true)
        {
            var result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            var buffer = result.Buffer;

            if (buffer.Length > maxBytes)
            {
                reader.AdvanceTo(buffer.End);
                return PooledBuffer.TooLarge;
            }

            if (result.IsCompleted || result.IsCanceled)
            {
                var copy = PooledBuffer.CopyFrom(buffer);
                reader.AdvanceTo(buffer.End);
                return copy;
            }

            reader.AdvanceTo(buffer.Start, buffer.End);
        }
    }
}
