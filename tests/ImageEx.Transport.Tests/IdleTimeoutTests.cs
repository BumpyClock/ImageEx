using System.Diagnostics;
using System.Net;
using ImageEx.Cache;

// Each case passes a guard token as the caller token. A missing deadline surfaces as guard cancellation, not a hang.
internal static class IdleTimeoutTests
{
    internal static readonly TimeSpan Idle = TimeSpan.FromMilliseconds(100);
    internal static readonly TimeSpan Guard = TimeSpan.FromSeconds(5);

    internal static async Task<int> RunAsync()
    {
        var passed = 0;
        var payload = Pattern(8);

        await ExpectIdleExpiry("stalled stream acquisition expires", async token =>
        {
            using var stream = await ImageExCacheManager.OpenSourceStreamAsync(new StalledContent(), Idle, token);
        });
        passed++;

        foreach (var (stallAt, name) in new[] { (0, "first read"), (3, "mid-body read after a prefix"), (payload.Length, "final EOF read") })
        {
            using var destination = new MemoryStream();
            await ExpectIdleExpiry($"stalled {name} expires", token =>
                ImageExCacheManager.CopyOriginalSourceAsync(new StallStream(payload, stallAt), destination, 64, token, Idle));
            Check(destination.ToArray().AsSpan().SequenceEqual(payload.AsSpan(0, stallAt)), $"stalled {name} keeps the written prefix");
            passed++;
        }

        using (var caller = new CancellationTokenSource())
        {
            var stream = new StallStream(payload, 0, onStall: caller.Cancel);
            var error = await Capture(() => ImageExCacheManager.ReadSourceAsync(stream, new byte[8], Idle, caller.Token));
            Check(error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token,
                "caller cancellation during a read carries the caller token");
            passed++;
        }

        using (var caller = new CancellationTokenSource())
        {
            // Both the deadline and the caller have fired before the read observes cancellation.
            var stream = new RaceStream(Idle * 3, caller);
            var error = await Capture(() => ImageExCacheManager.ReadSourceAsync(stream, new byte[8], Idle, caller.Token));
            Check(error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token,
                "caller cancellation wins a race with idle expiry");
            passed++;
        }

        using (var caller = new CancellationTokenSource())
        {
            var stream = new StallStream(payload, int.MaxValue, onRead: caller.Cancel);
            var error = await Capture(() => ImageExCacheManager.ReadSourceAsync(stream, new byte[8], Idle, caller.Token));
            Check(error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token,
                "caller cancellation during a completed read rejects its result");
            passed++;
        }

        using (var caller = new CancellationTokenSource())
        {
            var content = new StalledContent(onStall: caller.Cancel);
            var error = await Capture(() => ImageExCacheManager.OpenSourceStreamAsync(content, Idle, caller.Token));
            Check(error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token,
                "caller cancellation during acquisition carries the caller token");
            passed++;
        }

        using (var caller = new CancellationTokenSource())
        {
            var stream = new StallStream(payload, int.MaxValue);
            var content = new ImmediateContent(stream, onOpen: caller.Cancel);
            var error = await Capture(() => ImageExCacheManager.OpenSourceStreamAsync(content, Idle, caller.Token));
            Check(error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token && stream.Disposed,
                "caller cancellation after acquisition disposes the stream");
            passed++;
        }

        using (var guard = new CancellationTokenSource(Guard))
        using (var destination = new MemoryStream())
        {
            var source = Pattern(20);
            var watch = Stopwatch.StartNew();
            var count = await ImageExCacheManager.CopyOriginalSourceAsync(
                new DelayedStream(source, Idle / 4), destination, source.Length, guard.Token, Idle);
            Check(count == source.Length && destination.ToArray().AsSpan().SequenceEqual(source) && watch.Elapsed > Idle * 3,
                "progress across several idle budgets copies the exact payload");
            passed++;
        }

        return passed;
    }

    internal static byte[] Pattern(int length)
    {
        var bytes = new byte[length];
        for (var i = 0; i < bytes.Length; i++) bytes[i] = (byte)(i * 37 + 11);
        return bytes;
    }

    private static async Task ExpectIdleExpiry(string name, Func<CancellationToken, Task> body)
    {
        using var guard = new CancellationTokenSource(Guard);
        var watch = Stopwatch.StartNew();
        var error = await Capture(() => body(guard.Token));
        Check(error is TaskCanceledException { InnerException: TimeoutException } expired
            && expired.CancellationToken != guard.Token && !guard.IsCancellationRequested && watch.Elapsed < Guard / 2,
            name + $" ({error?.GetType().Name ?? "no exception"}, {watch.ElapsedMilliseconds} ms)");
    }

    private static async Task<Exception?> Capture(Func<Task> body)
    {
        try
        {
            await body();
            return null;
        }
        catch (Exception error)
        {
            return error;
        }
    }

    private static void Check(bool value, string name)
    {
        if (!value) throw new Exception("FAIL: " + name);
        Console.WriteLine("PASS: " + name);
    }
}

// Returns one byte per read and stalls every read once stallAt bytes have been returned, including the EOF read.
internal sealed class StallStream(byte[] data, int stallAt, Action? onStall = null, Action? onRead = null) : Stream
{
    private int _position;

    public bool Disposed { get; private set; }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_position >= stallAt)
        {
            onStall?.Invoke();
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        onRead?.Invoke();
        if (_position >= data.Length || buffer.Length == 0) return 0;
        buffer.Span[0] = data[_position++];
        return 1;
    }

    protected override void Dispose(bool disposing)
    {
        Disposed = true;
        base.Dispose(disposing);
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}

// Ignores its token until both the deadline and the caller have fired, then observes cancellation.
internal sealed class RaceStream(TimeSpan wait, CancellationTokenSource caller) : MemoryStream(new byte[1])
{
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await Task.Delay(wait, CancellationToken.None);
        caller.Cancel();
        cancellationToken.ThrowIfCancellationRequested();
        throw new Exception("The read token did not observe cancellation.");
    }
}

// Returns one byte per read after a delay, so the transfer outlasts several idle budgets while each read stays within one.
internal sealed class DelayedStream(byte[] data, TimeSpan delay) : MemoryStream(data, writable: false)
{
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await Task.Delay(delay, cancellationToken);
        return await base.ReadAsync(buffer[..Math.Min(1, buffer.Length)], cancellationToken);
    }
}

// Headers complete, then stream acquisition never completes unless its token cancels.
internal sealed class StalledContent(Action? onStall = null) : HttpContent
{
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => throw new NotSupportedException();
    protected override bool TryComputeLength(out long length) { length = 0; return false; }
    protected override Task<Stream> CreateContentReadStreamAsync() => throw new NotSupportedException();

    protected override async Task<Stream> CreateContentReadStreamAsync(CancellationToken cancellationToken)
    {
        onStall?.Invoke();
        await Task.Delay(Timeout.Infinite, cancellationToken);
        throw new Exception("Unreachable");
    }
}

// Returns the supplied stream after running a callback, without observing its token.
internal sealed class ImmediateContent(Stream stream, Action? onOpen = null) : HttpContent
{
    protected override Task SerializeToStreamAsync(Stream target, TransportContext? context) => throw new NotSupportedException();
    protected override bool TryComputeLength(out long length) { length = 0; return false; }
    protected override Task<Stream> CreateContentReadStreamAsync() => CreateContentReadStreamAsync(CancellationToken.None);

    protected override Task<Stream> CreateContentReadStreamAsync(CancellationToken cancellationToken)
    {
        onOpen?.Invoke();
        return Task.FromResult(stream);
    }
}
