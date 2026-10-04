namespace ImageEx.Hosted.Tests;

/// <summary>
/// The production control with its resolver extension points observed.
/// Each resolve still runs the production resolver. A held URI resolves its real image,
/// then waits for the test and returns that image even when its request was retired.
/// </summary>
public sealed class ObservedImageEx : ImageExControl
{
    private readonly Dictionary<string, LateCompletion> _held = new(StringComparer.Ordinal);
    private readonly List<Task> _resolves = new();

    internal LateCompletion HoldCompletion(Uri uri)
    {
        var completion = new LateCompletion();
        _held[uri.AbsoluteUri] = completion;
        return completion;
    }

    /// <summary>
    /// Waits for every resolve this control started. The control continues each one on the dispatcher.
    /// </summary>
    internal async Task WhenResolvesCompleteAsync()
    {
        foreach (var resolve in _resolves.ToArray())
        {
            await TestWait.ForTaskAsync(resolve, "a resolve started by the control to complete");
        }
    }

    protected override Task<ImageLoadResult> ResolveImageAsync(Uri imageUri, CancellationToken token)
    {
        var resolve = _held.Remove(imageUri.AbsoluteUri, out var completion)
            ? ResolveThenHoldAsync(imageUri, completion)
            : base.ResolveImageAsync(imageUri, token);
        _resolves.Add(resolve);
        return resolve;
    }

    protected override Task<ImageLoadResult> ResolveImageRequestAsync(ImageRequest request, CancellationToken token)
    {
        var resolve = base.ResolveImageRequestAsync(request, token);
        _resolves.Add(resolve);
        return resolve;
    }

    private async Task<ImageLoadResult> ResolveThenHoldAsync(Uri imageUri, LateCompletion completion)
    {
        // An uncancelable token lets the production manager decode a real image for the retired request.
        var result = await base.ResolveImageAsync(imageUri, CancellationToken.None);
        completion.MarkResolved(result);
        await completion.Released;
        return result;
    }
}

public sealed class LateCompletion
{
    private readonly TaskCompletionSource<ImageLoadResult> _resolved = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Released => _released.Task;

    public void MarkResolved(ImageLoadResult result) => _resolved.TrySetResult(result);

    public void Release() => _released.TrySetResult();

    internal async Task<ImageLoadResult> WaitUntilResolvedAsync()
    {
        await TestWait.ForTaskAsync(_resolved.Task, "the held request to resolve its image");
        return await _resolved.Task;
    }
}
