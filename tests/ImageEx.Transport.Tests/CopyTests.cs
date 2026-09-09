using ImageEx.Cache;

internal static class CopyTests
{
    internal static async Task RunAsync()
    {
        using (var source = new DelayedStream(12, TimeSpan.FromMilliseconds(100)))
        using (var destination = new MemoryStream())
        {
            var count = await ImageExCacheManager.CopyOriginalSourceAsync(source, destination, 12,
                CancellationToken.None, TimeSpan.FromMilliseconds(500));
            if (count != 12 || destination.Length != 12) throw new Exception("FAIL: progress copy.");
            Console.WriteLine("PASS: progress exceeds one idle budget and exact limit succeeds");
        }
        await ExpectFailure<OperationCanceledException>(new DelayedStream(1, Timeout.InfiniteTimeSpan),
            1, CancellationToken.None, TimeSpan.FromMilliseconds(50), "stalled read expires");
        await ExpectFailure<IOException>(new MemoryStream(new byte[13]),
            12, CancellationToken.None, TimeSpan.FromSeconds(1), "source byte limit");
        using (var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50)))
        {
            await ExpectFailure<OperationCanceledException>(new DelayedStream(1, Timeout.InfiniteTimeSpan),
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
        TimeSpan timeout, string name) where T : Exception
    {
        using (source)
        using (var destination = new MemoryStream())
        {
            try
            {
                await ImageExCacheManager.CopyOriginalSourceAsync(source, destination, limit, token, timeout);
            }
            catch (T)
            {
                if (destination.Length > limit) throw new Exception("FAIL: oversized output.");
                Console.WriteLine("PASS: " + name);
                return;
            }
        }
        throw new Exception("FAIL: " + name);
    }

    private sealed class DelayedStream(int length, TimeSpan delay) : MemoryStream(new byte[length])
    {
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Delay(delay, cancellationToken);
            return await base.ReadAsync(buffer[..Math.Min(1, buffer.Length)], cancellationToken);
        }
    }
}
