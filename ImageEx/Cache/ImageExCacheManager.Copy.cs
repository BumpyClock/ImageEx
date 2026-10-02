#nullable enable

using System.Buffers;
using System.Net.Http;

namespace ImageEx.Cache;

internal sealed partial class ImageExCacheManager
{
    internal static async Task<long> CopyOriginalSourceAsync(Stream source, Stream destination, long maximumBytes, CancellationToken token, TimeSpan? idleTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumBytes);
        var timeout = idleTimeout ?? ImageExCacheConstants.SourceIdleTimeout;
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        long total = 0;
        try
        {
            while (true)
            {
                var count = (int)Math.Min(buffer.Length, maximumBytes - total + 1);
                var read = await ReadSourceAsync(source, buffer.AsMemory(0, count), timeout, token).ConfigureAwait(false);
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

    // Each body operation gets a fresh deadline, so an expired deadline never reaches the next read.
    // Upstream cancellation wins and carries the upstream token.
    // Idle expiry is a TaskCanceledException that does not carry the upstream token, so the cached retry filter treats it as a timeout.
    internal static async Task<Stream> OpenSourceStreamAsync(HttpContent content, TimeSpan idleTimeout, CancellationToken token)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(idleTimeout, TimeSpan.Zero);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(idleTimeout);
        Stream stream;
        try
        {
            stream = await content.ReadAsStreamAsync(deadline.Token).ConfigureAwait(false);
        }
        catch (Exception error) when (ClassifyBodyFailure(error, deadline, token) is { } replacement)
        {
            throw replacement;
        }

        if (token.IsCancellationRequested)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
        }

        return stream;
    }

    // The read is always awaited, so a pooled buffer never returns to the pool while a read still owns it.
    internal static async Task<int> ReadSourceAsync(Stream source, Memory<byte> buffer, TimeSpan idleTimeout, CancellationToken token)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(idleTimeout, TimeSpan.Zero);
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(idleTimeout);
        int read;
        try
        {
            read = await source.ReadAsync(buffer, deadline.Token).ConfigureAwait(false);
        }
        catch (Exception error) when (ClassifyBodyFailure(error, deadline, token) is { } replacement)
        {
            throw replacement;
        }

        token.ThrowIfCancellationRequested();
        return read;
    }

    // Returns the exception to throw instead of error, or null to rethrow error unchanged.
    private static Exception? ClassifyBodyFailure(Exception error, CancellationTokenSource deadline, CancellationToken token)
    {
        // Read the deadline before the upstream token. Upstream cancellation sets its own state before it
        // cancels the linked deadline, so a canceled deadline with a clear upstream token means the timer fired.
        var deadlineCanceled = deadline.IsCancellationRequested;
        if (token.IsCancellationRequested)
        {
            return error is OperationCanceledException canceled && canceled.CancellationToken == token
                ? null
                : new OperationCanceledException("Image source body operation was canceled.", error, token);
        }

        // A stalled stream can report its canceled operation as any exception type, such as IOException.
        return deadlineCanceled
            ? new TaskCanceledException("Image source body made no progress within the idle timeout.",
                new TimeoutException("Image source body idle timeout elapsed.", error), deadline.Token)
            : null;
    }
}
