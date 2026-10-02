using ImageEx.Cache;

internal static class CopyTests
{
    internal static async Task RunAsync()
    {
        var payload = IdleTimeoutTests.Pattern(12);
        using (var source = new DelayedStream(payload, TimeSpan.FromMilliseconds(100)))
        using (var destination = new MemoryStream())
        {
            var count = await ImageExCacheManager.CopyOriginalSourceAsync(source, destination, 12,
                CancellationToken.None, TimeSpan.FromSeconds(1));
            if (count != 12 || !destination.ToArray().AsSpan().SequenceEqual(payload)) throw new Exception("FAIL: progress copy.");
            Console.WriteLine("PASS: progress exceeds one idle budget and exact limit succeeds");
        }
        // The guard token only bounds the case. Idle expiry must settle the read, not the guard.
        using (var guard = new CancellationTokenSource(IdleTimeoutTests.Guard))
        {
            await ExpectFailure<OperationCanceledException>(new DelayedStream(new byte[1], Timeout.InfiniteTimeSpan),
                1, guard.Token, TimeSpan.FromMilliseconds(50), "stalled read expires",
                error => error.CancellationToken != guard.Token);
        }
        await ExpectFailure<IOException>(new MemoryStream(new byte[13]),
            12, CancellationToken.None, TimeSpan.FromSeconds(1), "source byte limit");
        using (var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50)))
        {
            await ExpectFailure<OperationCanceledException>(new DelayedStream(new byte[1], Timeout.InfiniteTimeSpan),
                1, cancellation.Token, TimeSpan.FromSeconds(5), "caller cancellation interrupts read");
        }
        using (var source = new MemoryStream())
        using (var destination = new MemoryStream())
        {
            var count = await ImageExCacheManager.CopyOriginalSourceAsync(source, destination, 1, CancellationToken.None);
            if (count != 0 || destination.Length != 0) throw new Exception("FAIL: empty copy.");
            Console.WriteLine("PASS: empty source returns zero");
        }
    }

    private static async Task ExpectFailure<T>(Stream source, long limit, CancellationToken token,
        TimeSpan timeout, string name, Func<T, bool>? accept = null) where T : Exception
    {
        using (source)
        using (var destination = new MemoryStream())
        {
            try
            {
                await ImageExCacheManager.CopyOriginalSourceAsync(source, destination, limit, token, timeout);
            }
            catch (T error) when (accept == null || accept(error))
            {
                if (destination.Length > limit) throw new Exception("FAIL: oversized output.");
                Console.WriteLine("PASS: " + name);
                return;
            }
        }
        throw new Exception("FAIL: " + name);
    }
}
