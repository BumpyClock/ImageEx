using System.Net;
using Windows.Foundation;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// A current direct-URI load that resolves without an image fails once. Retired requests stay silent.
/// </summary>
[TestClass]
public sealed class DirectLoadCompletionTests
{
    [UITestMethod]
    public Task Direct_empty_response_fails_once()
        => AssertDirectLoadFailsOnceAsync((handler, uri) => handler.Serve(uri, Array.Empty<byte>()));

    [UITestMethod]
    public Task Direct_http_failure_fails_once()
        => AssertDirectLoadFailsOnceAsync((handler, uri) => handler.ServeStatus(uri, HttpStatusCode.NotFound));

    [UITestMethod]
    public Task Direct_corrupt_raster_fails_once()
        => AssertDirectLoadFailsOnceAsync((handler, uri) => handler.Serve(uri, ImageFixtures.Corrupt));

    [UITestMethod]
    public async Task Clearing_direct_source_retires_request_silently()
    {
        using var directory = new TempCacheDirectory();
        var handler = new FixtureHandler();
        var uri = TestUris.Next("cleared.png");
        var gate = handler.ServeGated(uri, await ImageFixtures.PngAsync(64, 32, Bgra.Red));
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager);
            harness.Control.Source = uri;
            await gate.WaitUntilStartedAsync();

            harness.Control.Source = null;
            gate.Release();
            await manager.DisposeAsync();
            await harness.SettleAsync();

            AssertSilentAndEmpty(harness, "Unloaded");
        }
        finally
        {
            gate.Release();
            await manager.DisposeAsync();
        }
    }

    [UITestMethod]
    public async Task Replacement_rejects_late_failure()
    {
        using var directory = new TempCacheDirectory();
        var handler = new FixtureHandler();
        var retired = TestUris.Next("retired.png");
        var replacement = TestUris.Next("replacement.png");
        var gate = handler.ServeGated(retired, Array.Empty<byte>(), status: HttpStatusCode.NotFound);
        handler.Serve(replacement, await ImageFixtures.PngAsync(64, 32, Bgra.Blue));
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager, control =>
            {
                control.DecodePixelWidth = 64;
                control.DecodePixelHeight = 32;
            });
            harness.Control.Source = retired;
            await gate.WaitUntilStartedAsync();
            harness.Control.Source = replacement;
            await harness.WaitForOpenedAsync();
            var displayed = harness.DisplayedSource;

            gate.Release();
            await manager.DisposeAsync();
            await harness.SettleAsync();

            Assert.AreEqual(0, harness.FailedCount, "The retired request published a failure.");
            Assert.AreEqual("Loaded", harness.CurrentState);
            Assert.AreSame(displayed, harness.DisplayedSource, "The retired request changed the displayed image.");
            ImageFixtures.AssertColor(ImageFixtures.AssertRaster(displayed, 64, 32, "replacement"), Bgra.Blue, "replacement");
            Assert.IsTrue(harness.TryGetNaturalSize(out var size));
            Assert.AreEqual(new Size(64, 32), size);
        }
        finally
        {
            gate.Release();
            await manager.DisposeAsync();
        }
    }

    [UITestMethod]
    public async Task Replacement_cancels_a_pending_download_silently()
    {
        using var directory = new TempCacheDirectory();
        var handler = new FixtureHandler();
        var retired = TestUris.Next("retired-landscape.png");
        var replacement = TestUris.Next("replacement-portrait.png");
        var gate = handler.ServeGated(retired, await ImageFixtures.PngAsync(320, 160, Bgra.Red));
        handler.Serve(replacement, await ImageFixtures.PngAsync(40, 80, Bgra.Blue));
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager);
            harness.Control.Source = retired;
            await gate.WaitUntilStartedAsync();
            harness.Control.Source = replacement;
            await harness.WaitForOpenedAsync();
            var displayed = harness.DisplayedSource;

            gate.Release();
            await manager.DisposeAsync();
            await harness.SettleAsync();

            Assert.AreEqual(0, harness.FailedCount);
            Assert.AreEqual("Loaded", harness.CurrentState);
            Assert.AreSame(displayed, harness.DisplayedSource, "The retired request changed the displayed image.");
            ImageFixtures.AssertColor(ImageFixtures.AssertRaster(displayed, 40, 80, "replacement"), Bgra.Blue, "replacement");
            Assert.IsTrue(harness.TryGetNaturalSize(out var size));
            Assert.AreEqual(new Size(40, 80), size, "The retired request changed the natural size.");
        }
        finally
        {
            gate.Release();
            await manager.DisposeAsync();
        }
    }

    [UITestMethod]
    public async Task Unload_retires_a_pending_success_silently()
    {
        var png = await ImageFixtures.PngAsync(64, 32, Bgra.Red);
        await AssertUnloadRetiresSilentlyAsync((handler, uri) => handler.ServeGated(uri, png));
    }

    [UITestMethod]
    public Task Unload_retires_a_pending_failure_silently()
        => AssertUnloadRetiresSilentlyAsync(
            (handler, uri) => handler.ServeGated(uri, Array.Empty<byte>(), status: HttpStatusCode.NotFound));

    // The late-completion tests below deliver a real decoded image after its request was retired.
    [UITestMethod]
    public async Task Replacement_rejects_a_resolved_late_image_and_natural_size()
    {
        using var directory = new TempCacheDirectory();
        var retired = TestUris.Next("late-landscape.png");
        var replacement = TestUris.Next("replacement-portrait.png");
        var handler = new FixtureHandler()
            .Serve(retired, await ImageFixtures.PngAsync(320, 160, Bgra.Red))
            .Serve(replacement, await ImageFixtures.PngAsync(40, 80, Bgra.Blue));
        var manager = new ImageExCacheManager(directory.Path, handler);
        LateCompletion? late = null;
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager);
            late = harness.Control.HoldCompletion(retired);
            harness.Control.Source = retired;
            AssertLateImage(await late.WaitUntilResolvedAsync(), 320, 160);

            harness.Control.Source = replacement;
            await harness.WaitForOpenedAsync();
            var displayed = harness.DisplayedSource;
            var openedBeforeRelease = harness.OpenedCount;

            late.Release();
            await harness.SettleAsync();

            Assert.AreSame(displayed, harness.DisplayedSource, "The late completion replaced the displayed image.");
            ImageFixtures.AssertColor(ImageFixtures.AssertRaster(harness.DisplayedSource, 40, 80, "replacement"), Bgra.Blue, "replacement");
            Assert.IsTrue(harness.TryGetNaturalSize(out var size));
            Assert.AreEqual(new Size(40, 80), size, "The late completion changed the natural size.");
            Assert.AreEqual(0, harness.FailedCount);
            Assert.AreEqual(openedBeforeRelease, harness.OpenedCount, "The late completion opened an image.");
            Assert.AreEqual("Loaded", harness.CurrentState);
        }
        finally
        {
            late?.Release();
            await manager.DisposeAsync();
        }
    }

    [UITestMethod]
    public async Task Clearing_rejects_a_resolved_late_image()
    {
        await AssertResolvedLateImageRejectedAsync(
            harness =>
            {
                harness.Control.Source = null;
                return Task.CompletedTask;
            },
            expectedState: "Unloaded");
    }

    [UITestMethod]
    public async Task Unload_rejects_a_resolved_late_image()
    {
        await AssertResolvedLateImageRejectedAsync(harness => harness.RemoveFromTreeAsync(), expectedState: null);
    }

    private static async Task AssertResolvedLateImageRejectedAsync(Func<ControlHarness, Task> retire, string? expectedState)
    {
        using var directory = new TempCacheDirectory();
        var retired = TestUris.Next("late.png");
        var handler = new FixtureHandler().Serve(retired, await ImageFixtures.PngAsync(320, 160, Bgra.Red));
        var manager = new ImageExCacheManager(directory.Path, handler);
        LateCompletion? late = null;
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager);
            late = harness.Control.HoldCompletion(retired);
            harness.Control.Source = retired;
            AssertLateImage(await late.WaitUntilResolvedAsync(), 320, 160);

            await retire(harness);
            late.Release();
            await harness.SettleAsync();

            Assert.IsNull(harness.DisplayedSource, "The late completion attached its image.");
            Assert.IsFalse(harness.TryGetNaturalSize(out var size), $"The late completion reported natural size {size}.");
            Assert.AreEqual(0, harness.FailedCount, "ImageExFailed count.");
            Assert.AreEqual(0, harness.OpenedCount, "The late completion opened an image.");
            if (expectedState != null)
            {
                Assert.AreEqual(expectedState, harness.CurrentState);
            }
        }
        finally
        {
            late?.Release();
            await manager.DisposeAsync();
        }
    }

    // Proves the held request produced a real image, so the control alone must reject it.
    private static void AssertLateImage(ImageLoadResult result, double naturalWidth, double naturalHeight)
    {
        Assert.IsNotNull(result.Image, "Precondition: the held request did not resolve an image.");
        ImageFixtures.AssertNaturalSize(result.Image, naturalWidth, naturalHeight, "held request");
    }

    private static async Task AssertDirectLoadFailsOnceAsync(Action<FixtureHandler, Uri> route)
    {
        using var directory = new TempCacheDirectory();
        var handler = new FixtureHandler();
        var uri = TestUris.Next("failure.png");
        route(handler, uri);
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager);
            harness.Control.Source = uri;
            await TestWait.ForConditionAsync(() => harness.FailedCount > 0, "ImageExFailed from the current direct load");
            await manager.DisposeAsync();
            await harness.SettleAsync();

            Assert.AreEqual(1, harness.FailedCount, "ImageExFailed count.");
            Assert.AreEqual(0, harness.OpenedCount, "ImageExOpened count.");
            Assert.AreEqual("Failed", harness.CurrentState);
            Assert.IsNull(harness.DisplayedSource);
            Assert.IsFalse(harness.TryGetNaturalSize(out _));
        }
        finally
        {
            await manager.DisposeAsync();
        }
    }

    private static async Task AssertUnloadRetiresSilentlyAsync(Func<FixtureHandler, Uri, Gate> route)
    {
        using var directory = new TempCacheDirectory();
        var handler = new FixtureHandler();
        var uri = TestUris.Next("unloaded.png");
        var gate = route(handler, uri);
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager);
            harness.Control.Source = uri;
            await gate.WaitUntilStartedAsync();

            await harness.RemoveFromTreeAsync();
            gate.Release();
            await manager.DisposeAsync();
            await harness.SettleAsync();

            Assert.AreEqual(0, harness.FailedCount, "The unloaded request published a failure.");
            Assert.AreEqual(0, harness.OpenedCount, "The unloaded request opened an image.");
            Assert.IsNull(harness.DisplayedSource);
            Assert.IsFalse(harness.TryGetNaturalSize(out _));
        }
        finally
        {
            gate.Release();
            await manager.DisposeAsync();
        }
    }

    internal static void AssertSilentAndEmpty(ControlHarness harness, string expectedState)
    {
        Assert.AreEqual(0, harness.FailedCount, "ImageExFailed count.");
        Assert.AreEqual(0, harness.OpenedCount, "ImageExOpened count.");
        Assert.AreEqual(expectedState, harness.CurrentState);
        Assert.IsNull(harness.DisplayedSource);
        Assert.IsFalse(harness.TryGetNaturalSize(out _));
    }
}
