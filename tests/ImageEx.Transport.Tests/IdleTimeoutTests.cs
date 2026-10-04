using System.Diagnostics;
using System.Net;
using ImageEx.Cache;

// Each case passes a guard token as the caller token. A missing deadline surfaces as guard cancellation, not a hang.
// Outcomes are decided by exception identity and explicit signals, not by upper bounds on elapsed time.
internal static class IdleTimeoutTests
{
    internal static readonly TimeSpan Idle = TimeSpan.FromMilliseconds(100);
    internal static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

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

        // A stalled operation can report its canceled token as a non-cancellation error. Idle expiry still applies.
        foreach (var (failure, name) in new (Func<Exception>, string)[]
        {
            (() => new IOException("Connection aborted."), nameof(IOException)),
            (() => new ObjectDisposedException("source"), nameof(ObjectDisposedException))
        })
        {
            await ExpectIdleExpiry($"stalled read reporting {name} expires", token =>
                ImageExCacheManager.ReadSourceAsync(new StallStream(payload, 0, failWith: failure), new byte[8], Idle, token),
                failure().GetType());
            await ExpectIdleExpiry($"stalled acquisition reporting {name} expires", async token =>
            {
                using var stream = await ImageExCacheManager.OpenSourceStreamAsync(new StalledContent(failWith: failure), Idle, token);
            }, failure().GetType());
            passed += 2;
        }

        using (var caller = new CancellationTokenSource(Guard))
        {
            var stream = new StallStream(payload, 0, onStall: caller.Cancel);
            var error = await Capture(() => ImageExCacheManager.ReadSourceAsync(stream, new byte[8], Idle, caller.Token));
            Check(error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token,
                "caller cancellation during a read carries the caller token");
            passed++;
        }

        using (var caller = new CancellationTokenSource(Guard))
        {
            var stream = new StallStream(payload, 0, onStall: caller.Cancel, failWith: () => new IOException("Connection aborted."));
            var error = await Capture(() => ImageExCacheManager.ReadSourceAsync(stream, new byte[8], Idle, caller.Token));
            Check(error is OperationCanceledException canceled and not TaskCanceledException && canceled.CancellationToken == caller.Token,
                "caller cancellation wins over a read that reports IOException");
            passed++;
        }

        using (var caller = new CancellationTokenSource(Guard))
        {
            // The read wakes only when its token cancels. The caller cancels after the deadline woke the read.
            var stream = new DeadlineRaceStream(caller);
            var error = await Capture(() => ImageExCacheManager.ReadSourceAsync(stream, new byte[8], Idle, caller.Token));
            Check(stream.WokeByDeadline && error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token,
                "caller cancellation wins a race with a fired idle deadline");
            passed++;
        }

        using (var caller = new CancellationTokenSource(Guard))
        {
            var stream = new StallStream(payload, int.MaxValue, onRead: caller.Cancel);
            var error = await Capture(() => ImageExCacheManager.ReadSourceAsync(stream, new byte[8], Idle, caller.Token));
            Check(error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token,
                "caller cancellation during a completed read rejects its result");
            passed++;
        }

        using (var caller = new CancellationTokenSource(Guard))
        {
            var content = new StalledContent(onStall: caller.Cancel);
            var error = await Capture(() => ImageExCacheManager.OpenSourceStreamAsync(content, Idle, caller.Token));
            Check(error is OperationCanceledException canceled && canceled.CancellationToken == caller.Token,
                "caller cancellation during acquisition carries the caller token");
            passed++;
        }

        using (var caller = new CancellationTokenSource(Guard))
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
            // Each read waits a twentieth of the idle budget. The whole copy takes at least four idle budgets.
            var idle = TimeSpan.FromMilliseconds(500);
            var source = Pattern(80);
            var watch = Stopwatch.StartNew();
            var count = await ImageExCacheManager.CopyOriginalSourceAsync(
                new DelayedStream(source, idle / 20), destination, source.Length, guard.Token, idle);
            Check(count == source.Length && destination.ToArray().AsSpan().SequenceEqual(source) && watch.Elapsed >= idle * 4,
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

    private static async Task ExpectIdleExpiry(string name, Func<CancellationToken, Task> body, Type? reported = null)
    {
        using var guard = new CancellationTokenSource(Guard);
        var error = await Capture(() => body(guard.Token));
        Check(error is TaskCanceledException { InnerException: TimeoutException timeout } expired
            && expired.CancellationToken != guard.Token
            && (reported == null || timeout.InnerException?.GetType() == reported),
            name + $" ({error?.GetType().Name ?? "no exception"})");
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
// A stalled read ends only when its token cancels. failWith replaces the cancellation with another exception.
internal sealed class StallStream(byte[] data, int stallAt, Action? onStall = null, Action? onRead = null,
    Func<Exception>? failWith = null) : Stream
{
    private int _position;

    public bool Disposed { get; private set; }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (_position >= stallAt)
        {
            onStall?.Invoke();
            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException) when (failWith != null)
            {
                throw failWith();
            }
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

// Wakes only when its read token cancels. It records whether the caller was still clear at that point,
// which means the idle deadline woke it. It then cancels the caller before reporting cancellation.
internal sealed class DeadlineRaceStream(CancellationTokenSource caller) : MemoryStream(new byte[1])
{
    public bool WokeByDeadline { get; private set; }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            WokeByDeadline = !caller.IsCancellationRequested;
            caller.Cancel();
            throw;
        }

        throw new Exception("Unreachable");
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
internal sealed class StalledContent(Action? onStall = null, Func<Exception>? failWith = null) : HttpContent
{
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => throw new NotSupportedException();
    protected override bool TryComputeLength(out long length) { length = 0; return false; }
    protected override Task<Stream> CreateContentReadStreamAsync() => throw new NotSupportedException();

    protected override async Task<Stream> CreateContentReadStreamAsync(CancellationToken cancellationToken)
    {
        onStall?.Invoke();
        try
        {
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }
        catch (OperationCanceledException) when (failWith != null)
        {
            throw failWith();
        }

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
