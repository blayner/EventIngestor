namespace EventIngestor.Tests.TestDoubles;

internal static class Eventually
{
    public static async Task True(Func<bool> condition, TimeSpan? timeout = null, string? because = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail(because ?? "Condition was not met in time.");
            }

            await Task.Delay(10);
        }
    }
}
