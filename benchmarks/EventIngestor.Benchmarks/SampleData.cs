using System.Text;
using EventIngestor.Core.Models;
using EventIngestor.Core.Serialization;

namespace EventIngestor.Benchmarks;

internal static class SampleData
{
    private static readonly string[] Sources = ["checkout-api", "billing", "inventory", "notifications"];
    private static readonly string[] Types = ["order.created", "order.paid", "invoice.issued", "stock.reserved", "email.sent"];

    /// <summary>A JSON payload of roughly <paramref name="bytes"/> bytes.</summary>
    public static byte[] Payload(int bytes)
    {
        var filler = new string('x', Math.Max(0, bytes - 11)); // {"data":""} is 11 bytes
        return Encoding.UTF8.GetBytes($$"""{"data":"{{filler}}"}""");
    }

    public static byte[] EventJson(int payloadBytes, int seed = 0)
    {
        var payload = Encoding.UTF8.GetString(Payload(payloadBytes));
        var source = Sources[seed % Sources.Length];
        var type = Types[seed % Types.Length];
        return Encoding.UTF8.GetBytes(
            $$"""{"id":"0197a3c2-5b1e-7c4d-9a8b-{{seed:x12}}","timestamp":"2026-06-20T12:00:00Z","source":"{{source}}","type":"{{type}}","payload":{{payload}}}""");
    }

    public static byte[] Ndjson(int lines, int payloadBytes)
    {
        using var stream = new MemoryStream();
        for (var i = 0; i < lines; i++)
        {
            stream.Write(EventJson(payloadBytes, i));
            stream.WriteByte((byte)'\n');
        }

        return stream.ToArray();
    }

    public static IngestEvent[] Events(int count, int payloadBytes)
    {
        var events = new IngestEvent[count];
        for (var i = 0; i < count; i++)
        {
            events[i] = new IngestEvent();
            if (EventParser.TryParse(EventJson(payloadBytes, i), events[i]) != ParseStatus.Ok)
            {
                throw new InvalidOperationException("Sample event failed to parse.");
            }
        }

        return events;
    }
}
