using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// Describes one response body after its headers arrive. Every wait honors the read token.
/// A stalled wait ends only when its token cancels or the test aborts the script.
/// </summary>
internal sealed class BodyScript
{
    private static readonly ConcurrentBag<BodyScript> s_live = new();
    private readonly CancellationTokenSource _abort = new();
    private readonly TaskCompletionSource _stalled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private BodyScript()
    {
        s_live.Add(this);
    }

    public bool StallsAcquisition { get; private init; }

    /// <summary>Bytes delivered before every later read stalls. Null delivers the whole body.</summary>
    public int? StallAfterBytes { get; private init; }

    /// <summary>A stalled read reports IOException instead of cancellation when its token cancels.</summary>
    public bool ReportsIOException { get; private init; }

    public bool IsGated { get; private init; }

    public bool HoldsCancellation { get; private init; }

    public Action? OnStall { get; private init; }

    public TimeSpan ReadDelay { get; private init; }

    public int ChunkBytes { get; private init; } = int.MaxValue;

    /// <summary>Completes when the body first stalls or first waits at its gate.</summary>
    public Task Stalled => _stalled.Task;

    public Task Disposed => _disposed.Task;

    public static BodyScript Complete() => new();

    public static BodyScript StallAcquisition() => new() { StallsAcquisition = true };

    /// <param name="onStall">Runs inside the pending stalled read, before it waits. The operation cannot settle while it runs.</param>
    public static BodyScript StallAfter(int bytes, bool reportIOException = false, Action? onStall = null)
        => new() { StallAfterBytes = bytes, ReportsIOException = reportIOException, OnStall = onStall };

    public static BodyScript Progressing(TimeSpan readDelay, int chunkBytes) => new() { ReadDelay = readDelay, ChunkBytes = chunkBytes };

    /// <summary>The first read waits until the test releases the body, then the whole body arrives.</summary>
    public static BodyScript Gated() => new() { IsGated = true };

    /// <summary>Holds a canceled read until Release, so disposal must wait for transport cleanup.</summary>
    public static BodyScript HoldCanceledRead() => new() { StallAfterBytes = 0, HoldsCancellation = true };

    public Task WaitUntilStalledAsync() => TestWait.ForTaskAsync(Stalled, "the response body to stall after its headers");

    public Task WaitUntilCanceledAsync() => TestWait.ForTaskAsync(_canceled.Task, "the response body to observe cancellation");

    public void Release() => _released.TrySetResult();

    internal void MarkDisposed() => _disposed.TrySetResult();

    /// <summary>Ends every stalled wait of every script with an IOException, so a failing test can still dispose its manager.</summary>
    public static void AbortAll()
    {
        while (s_live.TryTake(out var script))
        {
            script._released.TrySetResult();
            script._abort.Cancel();
        }
    }

    internal async Task StallAsync(CancellationToken token)
    {
        OnStall?.Invoke();
        _stalled.TrySetResult();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(token, _abort.Token);
        try
        {
            await Task.Delay(Timeout.Infinite, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_abort.IsCancellationRequested && !token.IsCancellationRequested)
        {
            throw new IOException("The test aborted a stalled body.");
        }
        catch (OperationCanceledException) when (HoldsCancellation && token.IsCancellationRequested)
        {
            _canceled.TrySetResult();
            await _released.Task.ConfigureAwait(false);
            throw;
        }
        catch (OperationCanceledException) when (ReportsIOException)
        {
            throw new IOException("The stalled connection was aborted.");
        }
    }

    internal async Task WaitAtGateAsync(CancellationToken token)
    {
        _stalled.TrySetResult();
        await _released.Task.WaitAsync(token).ConfigureAwait(false);
    }
}

internal sealed class ScriptedContent(byte[] body, BodyScript script) : HttpContent
{
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) => throw new NotSupportedException();

    protected override bool TryComputeLength(out long length)
    {
        length = body.Length;
        return true;
    }

    protected override Task<Stream> CreateContentReadStreamAsync() => CreateContentReadStreamAsync(CancellationToken.None);

    protected override async Task<Stream> CreateContentReadStreamAsync(CancellationToken cancellationToken)
    {
        if (script.StallsAcquisition)
        {
            await script.StallAsync(cancellationToken).ConfigureAwait(false);
        }

        return new ScriptedStream(body, script);
    }
}

internal sealed class ScriptedStream(byte[] body, BodyScript script) : Stream
{
    private int _position;
    private bool _gatePassed;

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (script.IsGated && !_gatePassed)
        {
            await script.WaitAtGateAsync(cancellationToken).ConfigureAwait(false);
            _gatePassed = true;
        }

        var limit = script.StallAfterBytes ?? body.Length;
        if (script.StallAfterBytes is { } stallAt && _position >= stallAt)
        {
            await script.StallAsync(cancellationToken).ConfigureAwait(false);
        }

        if (script.ReadDelay > TimeSpan.Zero)
        {
            await Task.Delay(script.ReadDelay, cancellationToken).ConfigureAwait(false);
        }

        var count = Math.Min(Math.Min(buffer.Length, script.ChunkBytes), Math.Min(limit, body.Length) - _position);
        if (count <= 0) return 0;
        body.AsMemory(_position, count).CopyTo(buffer);
        _position += count;
        return count;
    }

    public override int Read(byte[] buffer, int offset, int count)
        => ReadAsync(buffer.AsMemory(offset, count)).AsTask().GetAwaiter().GetResult();

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing) script.MarkDisposed();
        base.Dispose(disposing);
    }
}
