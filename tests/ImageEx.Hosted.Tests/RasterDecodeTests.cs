using Microsoft.UI.Dispatching;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Media.Imaging;
using Windows.Foundation;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// Raster decode targets stay within the natural size and the pixel and byte budgets on every acquisition path.
/// </summary>
[TestClass]
public sealed class RasterDecodeTests
{
    [UITestMethod]
    public async Task Tiny_raster_never_enlarges_on_any_acquisition_path()
    {
        var png = await ImageFixtures.PngAsync(8, 8, Bgra.Red);
        await AssertDecodeAsync(png, 8, 8, Bgra.Red, new DecodeCase(800, 800, DecodePixelType.Physical, 1.0, 8, 8));
    }

    [UITestMethod]
    public async Task Raster_ceiling_preserves_the_requested_box_aspect()
    {
        var png = await ImageFixtures.PngAsync(320, 160, Bgra.Green);
        await AssertDecodeAsync(png, 320, 160, Bgra.Green, new DecodeCase(800, 800, DecodePixelType.Physical, 1.0, 160, 160));
    }

    [UITestMethod]
    public async Task Physical_requests_ignore_dpi_scale()
    {
        var png = await ImageFixtures.PngAsync(4096, 2048, Bgra.Blue);
        await AssertDecodeAsync(png, 4096, 2048, Bgra.Blue, new DecodeCase(400, 200, DecodePixelType.Physical, 2.0, 400, 200));
    }

    [UITestMethod]
    public async Task Logical_requests_scale_before_aspect_inference()
    {
        var png = await ImageFixtures.PngAsync(4096, 2048, Bgra.Blue);
        await AssertDecodeAsync(png, 4096, 2048, Bgra.Blue,
            new DecodeCase(400, 200, DecodePixelType.Logical, 2.0, 800, 400),
            new DecodeCase(400, 0, DecodePixelType.Logical, 2.0, 800, 400),
            new DecodeCase(0, 200, DecodePixelType.Logical, 2.0, 800, 400));
    }

    [UITestMethod]
    public async Task Fallback_dpi_applies_once()
    {
        var wide = await ImageFixtures.PngAsync(4096, 2048, Bgra.Blue);
        await AssertDecodeAsync(wide, 4096, 2048, Bgra.Blue,
            new DecodeCase(0, 0, DecodePixelType.Physical, 2.0, 800, 400),
            new DecodeCase(0, 0, DecodePixelType.Logical, 2.0, 800, 400));

        var square = await ImageFixtures.PngAsync(2048, 2048, Bgra.Green);
        await AssertDecodeAsync(square, 2048, 2048, Bgra.Green,
            new DecodeCase(-1, -1, DecodePixelType.Logical, 1.5, 600, 600));
    }

    [UITestMethod]
    public async Task Fractional_dpi_rounding_and_clamping_survive_decode()
    {
        var png = await ImageFixtures.PngAsync(2048, 1024, Bgra.Gray);
        await AssertDecodeAsync(png, 2048, 1024, Bgra.Gray,
            new DecodeCase(3, 1, DecodePixelType.Logical, 1.5, 4, 2),
            new DecodeCase(3, 0, DecodePixelType.Logical, 1.5, 4, 2),
            new DecodeCase(0, 3, DecodePixelType.Logical, 1.5, 8, 4),
            new DecodeCase(5, 0, DecodePixelType.Physical, 1.5, 5, 2),
            new DecodeCase(100, 0, DecodePixelType.Logical, 0.0, 50, 25),
            new DecodeCase(100, 0, DecodePixelType.Logical, 8.0, 400, 200));
    }

    [UITestMethod]
    public async Task Tall_missing_axis_decodes_as_portrait()
    {
        var png = await ImageFixtures.PngAsync(768, 3840, Bgra.Red);
        await AssertDecodeAsync(png, 768, 3840, Bgra.Red, new DecodeCase(384, 0, DecodePixelType.Physical, 1.0, 384, 1920));
    }

    [UITestMethod]
    public async Task Raster_buffers_respect_pixel_and_byte_budgets()
    {
        var square = await ImageFixtures.PngAsync(4096, 4096, Bgra.Green);
        await AssertDecodeAsync(square, 4096, 4096, Bgra.Green,
            new DecodeCase(4096, 4096, DecodePixelType.Physical, 1.0, 1448, 1448));

        var wide = await ImageFixtures.PngAsync(4096, 2048, Bgra.Blue);
        await AssertDecodeAsync(wide, 4096, 2048, Bgra.Blue,
            new DecodeCase(2048, 1024, DecodePixelType.Logical, 2.0, 2048, 1024));
    }

    [UITestMethod]
    public async Task Cache_manager_cancellation_settles()
    {
        using var directory = new TempCacheDirectory();
        var png = await ImageFixtures.PngAsync(16, 16, Bgra.Red);
        var throwing = TestUris.Next("throwing.png");
        var silent = TestUris.Next("silent.png");
        var original = TestUris.Next("original.png");
        var handler = new FixtureHandler();
        var gates = new[]
        {
            handler.ServeGated(throwing, png),
            handler.ServeGated(silent, png),
            handler.ServeGated(original, png)
        };
        var dispatcher = DispatcherQueue.GetForCurrentThread();
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            using (var cts = new CancellationTokenSource())
            {
                var pending = manager.GetOrLoadImageAsync(throwing, 16, 16, DecodePixelType.Physical, cts.Token, dispatcher);
                await gates[0].WaitUntilStartedAsync();
                cts.Cancel();
                await TestWait.ForTaskAsync(pending, "the canceled acquisition to settle");
                await Assert.ThrowsAsync<OperationCanceledException>(() => pending);
            }

            using (var cts = new CancellationTokenSource())
            {
                var pending = manager.GetOrLoadImageAsync(silent, 16, 16, DecodePixelType.Physical, cts.Token, dispatcher,
                    returnNullOnCancellation: true);
                await gates[1].WaitUntilStartedAsync();
                cts.Cancel();
                await TestWait.ForTaskAsync(pending, "the canceled acquisition to settle");
                Assert.IsNull((await pending).Image);
            }

            using (var cts = new CancellationTokenSource())
            {
                var pending = manager.GetOrLoadOriginalImageAsync(original, 16, 16, DecodePixelType.Physical, cts.Token, dispatcher);
                await gates[2].WaitUntilStartedAsync();
                cts.Cancel();
                await TestWait.ForTaskAsync(pending, "the canceled original acquisition to settle");
                await Assert.ThrowsAsync<OperationCanceledException>(() => pending);
            }

            foreach (var gate in gates)
            {
                gate.Release();
            }

            // Cancellation does not poison later acquisitions of the same sources.
            foreach (var uri in new[] { throwing, silent })
            {
                var result = await manager.GetOrLoadImageAsync(uri, 16, 16, DecodePixelType.Physical, CancellationToken.None, dispatcher);
                ImageFixtures.AssertColor(ImageFixtures.AssertRaster(result.Image, 16, 16, $"retry {uri.Segments[^1]}"), Bgra.Red, "retry");
            }

            var retried = await manager.GetOrLoadOriginalImageAsync(original, 16, 16, DecodePixelType.Physical, CancellationToken.None, dispatcher);
            ImageFixtures.AssertRaster(retried.Image, 16, 16, "retry original");
        }
        finally
        {
            foreach (var gate in gates)
            {
                gate.Release();
            }

            await manager.DisposeAsync();
        }
    }

    internal readonly record struct DecodeCase(
        int DecodeWidth,
        int DecodeHeight,
        DecodePixelType DecodeType,
        double DpiScale,
        int ExpectedWidth,
        int ExpectedHeight)
    {
        public override string ToString() => $"request {DecodeWidth}x{DecodeHeight} {DecodeType} at DPI {DpiScale}";
    }

    private static async Task AssertDecodeAsync(byte[] png, int naturalWidth, int naturalHeight, Bgra color, params DecodeCase[] cases)
    {
        foreach (var decodeCase in cases)
        {
            foreach (var (path, image) in await DecodeOnEveryPathAsync(png, "image/png", ".png", decodeCase))
            {
                var context = $"{decodeCase} via {path}";
                var bitmap = ImageFixtures.AssertRaster(image, decodeCase.ExpectedWidth, decodeCase.ExpectedHeight, context);
                ImageFixtures.AssertColor(bitmap, color, context);
                ImageFixtures.AssertNaturalSize(bitmap, naturalWidth, naturalHeight, context);
            }
        }
    }

    /// <summary>
    /// Decodes one source through download, warm disk cache, original download, and warm original cache.
    /// Warm acquisitions use a fresh manager whose transport always fails.
    /// </summary>
    internal static async Task<List<(string Path, ImageSource? Image)>> DecodeOnEveryPathAsync(
        byte[] body,
        string contentType,
        string extension,
        DecodeCase decodeCase)
    {
        using var directory = new TempCacheDirectory();
        var uri = TestUris.Next("source" + extension);
        var dispatcher = DispatcherQueue.GetForCurrentThread();
        var results = new List<(string, ImageSource?)>();
        var (width, height, type, dpi) = (decodeCase.DecodeWidth, decodeCase.DecodeHeight, decodeCase.DecodeType, decodeCase.DpiScale);

        var seeding = new ImageExCacheManager(directory.Path, new FixtureHandler().Serve(uri, body, contentType));
        try
        {
            results.Add(("download", (await seeding.GetOrLoadImageAsync(uri, width, height, type, CancellationToken.None, dispatcher, dpi)).Image));
            results.Add(("original download", (await seeding.GetOrLoadOriginalImageAsync(uri, width, height, type, CancellationToken.None, dispatcher, dpi)).Image));
        }
        finally
        {
            await seeding.DisposeAsync();
        }

        var offline = new ImageExCacheManager(directory.Path, new UnavailableHandler());
        try
        {
            results.Add(("warm disk cache", (await offline.GetOrLoadImageAsync(uri, width, height, type, CancellationToken.None, dispatcher, dpi)).Image));
            results.Add(("warm original cache", (await offline.GetOrLoadOriginalImageAsync(uri, width, height, type, CancellationToken.None, dispatcher, dpi)).Image));
        }
        finally
        {
            await offline.DisposeAsync();
        }

        return results;
    }
}
