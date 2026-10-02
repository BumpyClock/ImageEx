using System.Net;
using Windows.Foundation;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// Ordered ImageRequest sources keep their displayed-image and candidate-fallback behavior.
/// </summary>
[TestClass]
public sealed class OrderedRequestTests
{
    [UITestMethod]
    public async Task First_candidate_displays_without_failure()
    {
        var first = TestUris.Next("first.png");
        var second = TestUris.Next("second.png");
        var handler = new FixtureHandler()
            .Serve(first, await ImageFixtures.PngAsync(48, 24, Bgra.Blue))
            .Serve(second, await ImageFixtures.PngAsync(24, 48, Bgra.Red));

        await AssertRequestDisplaysAsync(
            handler,
            new ImageRequest(new[] { new ImageRequestCandidate(first), new ImageRequestCandidate(second) }),
            48, 24, Bgra.Blue);
    }

    [UITestMethod]
    public async Task Http_and_decode_failures_advance_to_original()
    {
        var missing = TestUris.Next("missing.png");
        var corrupt = TestUris.Next("corrupt.png");
        var original = TestUris.Next("original.png");

        // The original candidate exceeds the 8 MiB cached-mode source limit.
        var handler = new FixtureHandler()
            .ServeStatus(missing, HttpStatusCode.NotFound)
            .Serve(corrupt, ImageFixtures.Corrupt)
            .Serve(original, await ImageFixtures.PaddedPngAsync(40, 20, Bgra.Green, 9 * 1024 * 1024));

        await AssertRequestDisplaysAsync(
            handler,
            new ImageRequest(new[]
            {
                new ImageRequestCandidate(missing),
                new ImageRequestCandidate(corrupt),
                new ImageRequestCandidate(original, ImageRequestMode.Original)
            }),
            40, 20, Bgra.Green);
    }

    [UITestMethod]
    public async Task Invalid_svg_candidate_advances_to_raster()
    {
        var svg = TestUris.Next("invalid.svg");
        var raster = TestUris.Next("raster.png");
        var handler = new FixtureHandler()
            .Serve(svg, "<svg"u8.ToArray(), "image/svg+xml")
            .Serve(raster, await ImageFixtures.PngAsync(32, 16, Bgra.Green));

        await AssertRequestDisplaysAsync(
            handler,
            new ImageRequest(new[] { new ImageRequestCandidate(svg), new ImageRequestCandidate(raster) }),
            32, 16, Bgra.Green);
    }

    [UITestMethod]
    public async Task Cleared_request_keeps_later_candidates_offscreen()
    {
        using var directory = new TempCacheDirectory();
        var first = TestUris.Next("first.png");
        var second = TestUris.Next("second.png");
        var handler = new FixtureHandler().Serve(second, await ImageFixtures.PngAsync(32, 32, Bgra.Blue));
        var gate = handler.ServeGated(first, Array.Empty<byte>(), status: HttpStatusCode.NotFound);
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager);
            harness.Control.Source = new ImageRequest(new[] { new ImageRequestCandidate(first), new ImageRequestCandidate(second) });
            await gate.WaitUntilStartedAsync();

            harness.Control.Source = null;
            gate.Release();
            await manager.DisposeAsync();
            await harness.SettleAsync();

            DirectLoadCompletionTests.AssertSilentAndEmpty(harness, "Unloaded");
        }
        finally
        {
            gate.Release();
            await manager.DisposeAsync();
        }
    }

    private static async Task AssertRequestDisplaysAsync(FixtureHandler handler, ImageRequest request, int width, int height, Bgra color)
    {
        using var directory = new TempCacheDirectory();
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager, control =>
            {
                control.DecodePixelWidth = width;
                control.DecodePixelHeight = height;
            });
            harness.Control.Source = request;
            await harness.WaitForOpenedAsync();
            await manager.DisposeAsync();
            await harness.SettleAsync();

            Assert.AreEqual(0, harness.FailedCount, "An ordered request published a failure.");
            Assert.AreEqual("Loaded", harness.CurrentState);
            ImageFixtures.AssertColor(ImageFixtures.AssertRaster(harness.DisplayedSource, width, height, "ordered request"), color, "ordered request");
            Assert.IsTrue(harness.TryGetNaturalSize(out var size));
            Assert.AreEqual(new Size(width, height), size);
        }
        finally
        {
            await manager.DisposeAsync();
        }
    }
}
