using Windows.Foundation;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// Intentional DisableHttpImages suppression keeps a direct load silent.
/// </summary>
[TestClass]
public sealed class SuppressionTests
{
    [UITestMethod]
    public async Task Direct_http_suppression_is_silent_and_reversible()
    {
        using var directory = new TempCacheDirectory();
        var png = await ImageFixtures.PngAsync(48, 24, Bgra.Blue);
        var suppressed = TestUris.Next("suppressed.png");
        var restored = TestUris.Next("restored.png");
        var handler = new FixtureHandler().Serve(suppressed, png).Serve(restored, png);
        var manager = new ImageExCacheManager(directory.Path, handler);
        var previousOverride = ImageExDiagnostics.DisableHttpImagesOverride;
        try
        {
            await using var harness = await ControlHarness.CreateAsync(manager);

            ImageExDiagnostics.DisableHttpImagesOverride = true;
            harness.Control.Source = suppressed;
            await harness.SettleAsync();
            DirectLoadCompletionTests.AssertSilentAndEmpty(harness, "Unloaded");

            ImageExDiagnostics.DisableHttpImagesOverride = null;
            Assert.IsFalse(ImageExDiagnostics.DisableHttpImages, "The process environment suppresses HTTP images.");
            harness.Control.Source = restored;
            await harness.WaitForOpenedAsync();
            ImageFixtures.AssertColor(ImageFixtures.AssertRaster(harness.DisplayedSource, 48, 24, "restored load"), Bgra.Blue, "restored load");
            Assert.IsTrue(harness.TryGetNaturalSize(out var size));
            Assert.AreEqual(new Size(48, 24), size);
            Assert.AreEqual(0, harness.FailedCount);
        }
        finally
        {
            ImageExDiagnostics.DisableHttpImagesOverride = previousOverride;
            await manager.DisposeAsync();
        }
    }
}
