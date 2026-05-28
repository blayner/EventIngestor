using System.Buffers;
using System.IO.Pipelines;
using EventIngestor.Core.Abstractions;
using EventIngestor.Core.Models;
using EventIngestor.Core.Pooling;
using EventIngestor.Core.Serialization;

namespace EventIngestor.Pipeline;

/// <summary>
/// Streams newline-delimited JSON from a <see cref="PipeReader"/> straight into the pipeline.
/// </summary>
/// <remarks>
/// <para>
/// Compared to <c>StreamReader.ReadLine()</c> this never decodes the body to UTF-16, never allocates
/// a string per line and never copies a line that sits in a single buffer segment: lines are located
/// with <see cref="SequenceReader{T}"/> and parsed in place by <see cref="EventParser"/>. Only lines
/// that straddle two segments are copied, to the stack when small or to a pooled array otherwise.
/// </para>
/// <para>
/// The request body is never buffered as a whole, so memory per request is bounded by
/// <see cref="PipelineOptions.MaxLineBytes"/> regardless of body size. A line longer than that is
/// rejected and skipped up to the next newline instead of failing the whole request.
/// </para>
/// </remarks>
public sealed class NdjsonPipelineReader
{
    private const int StackallocThreshold = 512;
    private const byte NewLine = (byte)'\n';

    private readonly PipelineWriter _writer;
    private readonly PipelineOptions _options;
    private readonly Utf8StringCache? _stringCache;

    public NdjsonPipelineReader(PipelineWriter writer, PipelineOptions options, Utf8StringCache? stringCache = null)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(options);

        _writer = writer;
        _options = options;
        _stringCache = stringCache;
    }

    private static ReadOnlySpan<byte> Whitespace => " \t\r"u8;

    public async ValueTask<IngestResult> ReadAsync(
        PipeReader reader,
        string transport = Transports.Ndjson,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var parsed = new List<IngestEvent>(capacity: 128);
        var state = new ParseState();
        var total = default(IngestResult);
        var handedOff = false;

        try
        {
            while (true)
            {
                var result = await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                var buffer = result.Buffer;

                ParseBuffer(ref buffer, result.IsCompleted, parsed, ref state, transport);

                // Consumed up to the start of the incomplete trailing line; examined everything,
                // so the next ReadAsync only returns once more data has arrived.
                reader.AdvanceTo(buffer.Start, result.Buffer.End);

                if (state.Rejected > 0)
                {
                    total += new IngestResult(0, state.Rejected, 0);
                    state.Rejected = 0;
                }

                if (parsed.Count > 0)
                {
                    // WriteAsync takes ownership of every event in the list, including on failure.
                    handedOff = true;
                    total += await _writer.WriteAsync(parsed, transport, cancellationToken).ConfigureAwait(false);
                    parsed.Clear();
                    handedOff = false;
                }

                if (result.IsCompleted || result.IsCanceled)
                {
                    break;
                }
            }
        }
        catch when (!handedOff)
        {
            // Failed while parsing (e.g. the read was cancelled): recycle what we still own.
            foreach (var evt in parsed)
            {
                _writer.Release(evt);
            }

            throw;
        }

        return total;
    }

    /// <summary>
    /// Parses every complete line in <paramref name="buffer"/> and slices it down to the unconsumed remainder.
    /// Kept synchronous because <see cref="SequenceReader{T}"/> is a ref struct.
    /// </summary>
    private void ParseBuffer(
        ref ReadOnlySequence<byte> buffer,
        bool isCompleted,
        List<IngestEvent> output,
        ref ParseState state,
        string transport)
    {
        var reader = new SequenceReader<byte>(buffer);

        if (state.SkippingOversizedLine)
        {
            if (!reader.TryAdvanceTo(NewLine, advancePastDelimiter: true))
            {
                buffer = buffer.Slice(buffer.End);
                return;
            }

            state.SkippingOversizedLine = false;
        }

        while (reader.TryReadTo(out ReadOnlySequence<byte> line, NewLine, advancePastDelimiter: true))
        {
            ParseLine(line, output, ref state, transport);
        }

        if (isCompleted && !reader.End)
        {
            // Last line without a trailing newline.
            ParseLine(reader.UnreadSequence, output, ref state, transport);
            reader.AdvanceToEnd();
        }

        buffer = buffer.Slice(reader.Position);

        if (!isCompleted && buffer.Length > _options.MaxLineBytes)
        {
            state.Rejected++;
            state.SkippingOversizedLine = true;
            _writer.Reject(transport, RejectReason.PayloadTooLarge);
            buffer = buffer.Slice(buffer.End);
        }
    }

    private void ParseLine(in ReadOnlySequence<byte> line, List<IngestEvent> output, ref ParseState state, string transport)
    {
        if (line.IsEmpty)
        {
            return;
        }

        if (line.Length > _options.MaxLineBytes)
        {
            state.Rejected++;
            _writer.Reject(transport, RejectReason.PayloadTooLarge);
            return;
        }

        if (line.IsSingleSegment)
        {
            ParseSpan(line.FirstSpan, output, ref state, transport);
            return;
        }

        var length = (int)line.Length;
        byte[]? rented = null;
        Span<byte> scratch = length <= StackallocThreshold
            ? stackalloc byte[StackallocThreshold]
            : (rented = ArrayPool<byte>.Shared.Rent(length));

        try
        {
            line.CopyTo(scratch);
            ParseSpan(scratch[..length], output, ref state, transport);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private void ParseSpan(ReadOnlySpan<byte> line, List<IngestEvent> output, ref ParseState state, string transport)
    {
        var trimmed = line.Trim(Whitespace);
        if (trimmed.IsEmpty)
        {
            return;
        }

        var evt = _writer.Rent();
        var status = EventParser.TryParse(trimmed, evt, _options.MaxPayloadBytes, _stringCache);
        if (status == ParseStatus.Ok)
        {
            output.Add(evt);
            return;
        }

        state.Rejected++;
        _writer.Reject(evt, transport, status.ToRejectReason());
    }

    private struct ParseState
    {
        public int Rejected;
        public bool SkippingOversizedLine;
    }
}
