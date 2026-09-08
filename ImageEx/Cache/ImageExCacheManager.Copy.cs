#nullable enable

using System.Buffers;

namespace ImageEx.Cache;

internal sealed partial class ImageExCacheManager
{
    internal static async Task<long> CopyOriginalSourceAsync(Stream source, Stream destination, long maximumBytes, CancellationToken token, TimeSpan? idleTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumBytes);
        using var idle = CancellationTokenSource.CreateLinkedTokenSource(token);
        var timeout = idleTimeout ?? TimeSpan.FromSeconds(30);
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        long total = 0;
        try
        {
            while (true)
            {
                var count = (int)Math.Min(buffer.Length, maximumBytes - total + 1);
                idle.CancelAfter(timeout);
                var read = await source.ReadAsync(buffer.AsMemory(0, count), idle.Token).ConfigureAwait(false);
                idle.CancelAfter(Timeout.InfiniteTimeSpan);
                if (read == 0) return total;
                total += read;
                if (total > maximumBytes) throw new IOException("Original image exceeds source byte limit.");
                await destination.WriteAsync(buffer.AsMemory(0, read), token).ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
