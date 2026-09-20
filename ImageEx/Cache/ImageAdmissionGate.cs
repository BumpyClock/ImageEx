#nullable enable

namespace ImageEx.Cache;

/// <summary>Shares a fixed capacity while admitting visible work ahead of queued speculation.</summary>
internal sealed class ImageAdmissionGate(int capacity) : IDisposable
{
    private readonly object _gate = new();
    private readonly LinkedList<Waiter> _waiting = new();
    private int _available = capacity > 0 ? capacity : throw new ArgumentOutOfRangeException(nameof(capacity));
    private bool _disposed;

    private sealed class Waiter(Func<bool> visible)
    {
        public Func<bool> Visible { get; } = visible;
        public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public LinkedListNode<Waiter>? Node { get; set; }
    }

    public Task WaitAsync(CancellationToken token, Func<bool>? visible = null)
    {
        token.ThrowIfCancellationRequested();
        Waiter waiter;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_available > 0)
            {
                _available--;
                return Task.CompletedTask;
            }
            waiter = new Waiter(visible ?? (() => true));
            waiter.Node = _waiting.AddLast(waiter);
        }
        return WaitWithCancellationAsync(waiter, token);
    }

    private async Task WaitWithCancellationAsync(Waiter waiter, CancellationToken token)
    {
        using var registration = token.Register(() =>
        {
            lock (_gate)
            {
                if (waiter.Node?.List == null) return;
                _waiting.Remove(waiter.Node);
                waiter.Completion.TrySetCanceled(token);
            }
        });
        await waiter.Completion.Task.ConfigureAwait(false);
    }

    public void Release()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var next = _waiting.First;
            for (var node = _waiting.First; node != null; node = node.Next)
            {
                if (!node.Value.Visible()) continue;
                next = node;
                break;
            }
            if (next != null)
            {
                _waiting.Remove(next);
                next.Value.Completion.TrySetResult();
            }
            else
            {
                if (_available == capacity) throw new SemaphoreFullException();
                _available++;
            }
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            foreach (var waiter in _waiting) waiter.Completion.TrySetException(new ObjectDisposedException(nameof(ImageAdmissionGate)));
            _waiting.Clear();
        }
    }
}
