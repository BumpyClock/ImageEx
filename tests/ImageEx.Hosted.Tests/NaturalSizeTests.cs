using Microsoft.UI.Xaml.Media.Imaging;
using Windows.Foundation;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// ImageExBase.TryGetNaturalSize reports the attached image's natural size, never a decode size.
/// </summary>
[TestClass]
public sealed class NaturalSizeTests
{
    [UITestMethod]
    public async Task Direct_and_ordered_loads_report_natural_size_independent_of_decode_size()
    {
        using var directory = new TempCacheDirectory();
        var png = await ImageFixtures.PngAsync(320, 160, Bgra.Green);
        var direct = TestUris.Next("direct.png");
        var cached = TestUris.Next("ordered-cached.png");
        var original = TestUris.Next("ordered-original.png");
        var handler = new FixtureHandler().Serve(direct, png).Serve(cached, png).Serve(original, png);
        var manager = new ImageExCacheManager(directory.Path, handler);
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager, control =>
            {
                control.DecodePixelWidth = 80;
                control.DecodePixelHeight = 40;
            });

            var sources = new (string Name, object Source)[]
            {
                ("direct URI", direct),
                ("ImageRequest cached candidate", new ImageRequest(new[] { new ImageRequestCandidate(cached) })),
                ("ImageRequest original candidate", new ImageRequest(new[] { new ImageRequestCandidate(original, ImageRequestMode.Original) }))
            };
            for (var i = 0; i < sources.Length; i++)
            {
                harness.Control.Source = sources[i].Source;
                await harness.WaitForOpenedAsync(i + 1);

                var bitmap = ImageFixtures.AssertRaster(harness.DisplayedSource, 80, 40, sources[i].Name);
                ImageFixtures.AssertColor(bitmap, Bgra.Green, sources[i].Name);
                Assert.IsTrue(harness.TryGetNaturalSize(out var size), $"{sources[i].Name}: no natural size.");
                Assert.AreEqual(new Size(320, 160), size, $"{sources[i].Name}: natural size.");
            }

            harness.Control.Source = null;
            await harness.SettleAsync();
            Assert.IsNull(harness.DisplayedSource);
            Assert.IsFalse(harness.TryGetNaturalSize(out _), "A cleared source still reports a natural size.");
            Assert.AreEqual(0, harness.FailedCount);
        }
        finally
        {
            await manager.DisposeAsync();
        }
    }

    [UITestMethod]
    [DataRow(32, 0, DisplayName = "width only")]
    [DataRow(0, 16, DisplayName = "height only")]
    [DataRow(32, 16, DisplayName = "both axes")]
    public async Task Platform_resized_bitmap_has_no_natural_size(int decodeWidth, int decodeHeight)
    {
        await using var harness = await ControlHarness.CreateAsync(manager: null, control =>
        {
            control.DecodePixelWidth = decodeWidth;
            control.DecodePixelHeight = decodeHeight;
        });
        harness.Control.Source = new Uri("ms-appx:///Fixtures/natural-64x32.png");
        await harness.WaitForOpenedAsync();
        await TestWait.ForConditionAsync(
            () => harness.DisplayedSource is BitmapImage { PixelWidth: > 0, PixelHeight: > 0 },
            "the platform bitmap to decode");

        Assert.IsFalse(harness.TryGetNaturalSize(out var size), $"A platform-resized bitmap reported {size}.");
    }

    [UITestMethod]
    public async Task Unresized_platform_bitmap_reports_its_natural_size()
    {
        await using var harness = await ControlHarness.CreateAsync(manager: null);
        harness.Control.Source = new Uri("ms-appx:///Fixtures/natural-64x32.png");
        await harness.WaitForOpenedAsync();
        await TestWait.ForConditionAsync(
            () => harness.DisplayedSource is BitmapImage { PixelWidth: > 0, PixelHeight: > 0 },
            "the platform bitmap to decode");

        Assert.IsTrue(harness.TryGetNaturalSize(out var size));
        Assert.AreEqual(new Size(64, 32), size);
    }
}
