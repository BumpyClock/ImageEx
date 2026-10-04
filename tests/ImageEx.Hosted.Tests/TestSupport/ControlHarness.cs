using Microsoft.UI.Dispatching;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media;
using Windows.Foundation;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// Hosts one production ImageEx control in a window and records its public outcomes.
/// </summary>
internal sealed class ControlHarness : IAsyncDisposable
{
    private readonly Grid _host;
    private bool _unloaded;

    private ControlHarness(ObservedImageEx control, Window window, Grid host)
    {
        Control = control;
        Window = window;
        _host = host;
        control.ImageExFailed += (_, _) => FailedCount++;
        control.ImageExOpened += (_, _) => OpenedCount++;
        control.Unloaded += (_, _) => _unloaded = true;
    }

    public ObservedImageEx Control { get; }

    public Window Window { get; }

    public int FailedCount { get; private set; }

    public int OpenedCount { get; private set; }

    public DispatcherQueue Dispatcher => Control.DispatcherQueue;

    public string? CurrentState
    {
        get
        {
            var root = (FrameworkElement)VisualTreeHelper.GetChild(Control, 0);
            return VisualStateManager.GetVisualStateGroups(root)
                .Single(group => group.Name == "CommonStates")
                .CurrentState?.Name;
        }
    }

    public ImageSource? DisplayedSource => FindImagePart(Control)!.Source;

    /// <summary>
    /// Creates a loaded control with the production template. The managed cache is enabled.
    /// </summary>
    public static async Task<ControlHarness> CreateAsync(ImageExCacheManager? manager, Action<ImageExControl>? configure = null)
    {
        var control = new ObservedImageEx
        {
            CacheManagerOverride = manager,
            IsCacheEnabled = true,
            EnableDiskCache = true,
            EnableLazyLoading = false,
            Width = 200,
            Height = 200
        };
        configure?.Invoke(control);
        var host = new Grid();
        host.Children.Add(control);
        var window = new Window { Content = host };
        var harness = new ControlHarness(control, window, host);
        try
        {
            window.Activate();
            await TestWait.ForConditionAsync(() => control.IsLoaded, "the control to load");
            control.ApplyTemplate();
            Assert.IsNotNull(FindImagePart(control), "The production template has no Image part.");

            // Drain the deferred initial viewport check before a test assigns a source.
            await TestWait.ForDispatcherIdleAsync(harness.Dispatcher);
            return harness;
        }
        catch
        {
            window.Close();
            throw;
        }
    }

    public Task WaitForOpenedAsync(int count = 1)
        => TestWait.ForConditionAsync(
            () => OpenedCount >= count && CurrentState == "Loaded" && DisplayedSource != null,
            $"ImageExOpened #{count} with the Loaded state and a displayed image");

    /// <summary>
    /// Waits for every resolve the control started, then for the dispatcher to run their continuations.
    /// </summary>
    public async Task SettleAsync()
    {
        await Control.WhenResolvesCompleteAsync();
        await TestWait.ForDispatcherIdleAsync(Dispatcher);
    }

    public async Task RemoveFromTreeAsync()
    {
        _host.Children.Remove(Control);
        await TestWait.ForConditionAsync(() => _unloaded && !Control.IsLoaded, "the control to unload");
        await TestWait.ForDispatcherIdleAsync(Dispatcher);
    }

    public bool TryGetNaturalSize(out Size size) => Control.TryGetNaturalSize(out size);

    public ValueTask DisposeAsync()
    {
        Window.Close();
        return ValueTask.CompletedTask;
    }

    private static Image? FindImagePart(DependencyObject parent)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);
            if (child is Image { Name: "Image" } image)
            {
                return image;
            }

            if (FindImagePart(child) is { } nested)
            {
                return nested;
            }
        }

        return null;
    }
}
