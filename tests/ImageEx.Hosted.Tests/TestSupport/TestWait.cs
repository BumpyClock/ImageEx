using Microsoft.UI.Dispatching;

namespace ImageEx.Hosted.Tests;

internal static class TestWait
{
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(10);

    public static async Task ForConditionAsync(Func<bool> predicate, TimeSpan? timeout = null)
    {
        var limit = timeout ?? DefaultTimeout;
        using var cts = new CancellationTokenSource(limit);
        try
        {
            while (!predicate())
            {
                await Task.Delay(PollInterval, cts.Token);
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            throw new TimeoutException($"Condition was not met within {limit.TotalMilliseconds:0}ms.");
        }
    }

    // Completes after every dispatcher item queued at normal or higher priority has run.
    public static async Task ForDispatcherIdleAsync(DispatcherQueue dispatcherQueue, int passes = 2)
    {
        for (var i = 0; i < passes; i++)
        {
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!dispatcherQueue.TryEnqueue(DispatcherQueuePriority.Low, () => completion.TrySetResult()))
            {
                throw new InvalidOperationException("The dispatcher rejected the idle marker.");
            }

            await completion.Task.WaitAsync(DefaultTimeout);
        }
    }
}
