using Microsoft.UI.Dispatching;

namespace ImageEx.Hosted.Tests;

internal static class TestWait
{
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(10);

    public static async Task ForConditionAsync(Func<bool> predicate, string description, TimeSpan? timeout = null)
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
            Assert.Fail($"Timed out after {limit.TotalSeconds:0}s waiting for {description}.");
        }
    }

    public static async Task ForTaskAsync(Task task, string description)
    {
        if (await Task.WhenAny(task, Task.Delay(DefaultTimeout)) != task)
        {
            Assert.Fail($"Timed out after {DefaultTimeout.TotalSeconds:0}s waiting for {description}.");
        }
    }

    // Each pass completes after the dispatcher runs every item queued above low priority.
    public static async Task ForDispatcherIdleAsync(DispatcherQueue dispatcherQueue, int passes = 3)
    {
        for (var i = 0; i < passes; i++)
        {
            var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!dispatcherQueue.TryEnqueue(DispatcherQueuePriority.Low, () => completion.TrySetResult()))
            {
                throw new InvalidOperationException("The dispatcher rejected the idle marker.");
            }

            await ForTaskAsync(completion.Task, "the dispatcher to drain");
        }
    }
}
