using Microsoft.UI.Xaml.Media.Imaging;
using Windows.Foundation;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// SVG and animated GIF sources keep their displayed behavior beside the raster ceiling.
/// </summary>
[TestClass]
public sealed class VectorAndAnimationTests
{
    [UITestMethod]
    public async Task Svg_keeps_its_display_and_natural_size()
    {
        var svg = ImageFixtures.Svg(64, 48, "#ff0000");
        var decodeCase = new RasterDecodeTests.DecodeCase(800, 800, DecodePixelType.Physical, 1.0, 0, 0);
        foreach (var (path, image) in await RasterDecodeTests.DecodeOnEveryPathAsync(svg, "image/svg+xml", ".svg", decodeCase))
        {
            Assert.IsInstanceOfType<SvgImageSource>(image, $"{path}: expected an SVG source.");
            ImageFixtures.AssertNaturalSize(image, 64, 48, path);
        }

        var uri = TestUris.Next("vector.svg");
        await using var harness = await LoadDirectAsync(uri, svg, "image/svg+xml");
        Assert.IsInstanceOfType<SvgImageSource>(harness.DisplayedSource);
        Assert.IsTrue(harness.TryGetNaturalSize(out var size));
        Assert.AreEqual(new Size(64, 48), size);
    }

    [UITestMethod]
    public async Task Animated_gif_keeps_playback_and_natural_size()
    {
        var gif = await ImageFixtures.AnimatedGifAsync(16, Bgra.Red, Bgra.Blue);
        var decodeCase = new RasterDecodeTests.DecodeCase(800, 800, DecodePixelType.Physical, 1.0, 16, 16);
        foreach (var (path, image) in await RasterDecodeTests.DecodeOnEveryPathAsync(gif, "image/gif", ".gif", decodeCase))
        {
            Assert.IsInstanceOfType<BitmapImage>(image, $"{path}: expected an animated bitmap.");
            Assert.IsTrue(((BitmapImage)image).IsAnimatedBitmap, $"{path}: the bitmap is not animated.");
            ImageFixtures.AssertNaturalSize(image, 16, 16, path);
        }

        var uri = TestUris.Next("animated.gif");
        await using var harness = await LoadDirectAsync(uri, gif, "image/gif", control =>
        {
            control.DecodePixelWidth = 800;
            control.DecodePixelHeight = 800;
        });
        var bitmap = harness.DisplayedSource as BitmapImage;
        Assert.IsNotNull(bitmap, "The control does not display the animated bitmap.");
        Assert.IsTrue(bitmap.IsAnimatedBitmap);
        await TestWait.ForConditionAsync(() => bitmap.IsPlaying, "the displayed GIF to play");
        Assert.IsTrue(harness.TryGetNaturalSize(out var size));
        Assert.AreEqual(new Size(16, 16), size);
    }

    private static async Task<ControlHarness> LoadDirectAsync(Uri uri, byte[] body, string contentType, Action<ImageExControl>? configure = null)
    {
        using var directory = new TempCacheDirectory();
        var manager = new ImageExCacheManager(directory.Path, new FixtureHandler().Serve(uri, body, contentType));
        ControlHarness? harness = null;
        try
        {
            harness = await ControlHarness.CreateAsync(manager, configure);
            harness.Control.Source = uri;
            await harness.WaitForOpenedAsync();
            await manager.DisposeAsync();
            await harness.SettleAsync();
            Assert.AreEqual(0, harness.FailedCount);
            Assert.AreEqual("Loaded", harness.CurrentState);
            return harness;
        }
        catch
        {
            if (harness != null)
            {
                await harness.DisposeAsync();
            }

            throw;
        }
        finally
        {
            await manager.DisposeAsync();
        }
    }
}
