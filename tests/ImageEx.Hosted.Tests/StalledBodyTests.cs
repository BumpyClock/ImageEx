using System.Diagnostics;
using Microsoft.UI.Dispatching;
using Microsoft.UI.Xaml.Media.Imaging;

namespace ImageEx.Hosted.Tests;

/// <summary>
/// Response bodies that stall after their headers settle through the production manager's idle timeout,
/// retries, and download slots. Each wait is bounded by TestWait, so a missing deadline fails instead of hanging.
/// </summary>
[TestClass]
public sealed class StalledBodyTests
{
    // Thumbnail termination is about three idle budgets plus 600 ms of retry delay. TestWait allows 10 seconds.
    private static readonly TimeSpan Idle = TimeSpan.FromMilliseconds(250);

    public TestContext TestContext { get; set; } = null!;
    private const int Size = 16;

    // Criterion 1: headers followed by a stalled body terminate within the idle and retry bounds.
    [UITestMethod]
    public Task Stalled_stream_acquisition_fails_and_enters_backoff()
        => AssertStallFailsThenBacksOffAsync(BodyScript.StallAcquisition);

    [UITestMethod]
    public Task Stalled_first_read_fails_and_enters_backoff()
        => AssertStallFailsThenBacksOffAsync(() => BodyScript.StallAfter(0));

    [UITestMethod]
    public Task Stalled_read_after_a_prefix_fails_and_enters_backoff()
        => AssertStallFailsThenBacksOffAsync(() => BodyScript.StallAfter(32));

    [UITestMethod]
    public async Task Two_stalled_attempts_then_success_returns_the_image()
    {
        var uri = TestUris.Next("third-attempt.png");
        var handler = new FixtureHandler().ServeScripted(uri, await Png(Bgra.Green),
            BodyScript.StallAfter(32), BodyScript.StallAcquisition(), BodyScript.Complete());
        await using var scope = new ManagerScope(handler, Idle);

        var result = await scope.LoadAsync(uri);

        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(result.Image, Size, Size, "third attempt"), Bgra.Green, "third attempt");
    }

    [UITestMethod]
    public async Task Stall_reporting_IOException_retries_and_returns_the_image()
    {
        var uri = TestUris.Next("io-stall.png");
        var handler = new FixtureHandler().ServeScripted(uri, await Png(Bgra.Blue),
            BodyScript.StallAfter(32, reportIOException: true), BodyScript.Complete());
        await using var scope = new ManagerScope(handler, Idle);

        var result = await scope.LoadAsync(uri);

        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(result.Image, Size, Size, "after IOException stall"), Bgra.Blue, "after IOException stall");
    }

    // Criterion 2: two stalled downloads release both default slots.
    [UITestMethod]
    public async Task Two_stalled_thumbnails_release_both_download_slots()
    {
        var stalledA = TestUris.Next("stalled-a.png");
        var stalledB = TestUris.Next("stalled-b.png");
        var thumbnailA = TestUris.Next("thumbnail-a.png");
        var thumbnailB = TestUris.Next("thumbnail-b.png");
        var original = TestUris.Next("original.png");
        var png = await Png(Bgra.Red);
        var handler = new FixtureHandler()
            .ServeScripted(stalledA, png, BodyScript.StallAfter(0))
            .ServeScripted(stalledB, png, BodyScript.StallAcquisition())
            .Serve(original, await Png(Bgra.Blue));

        // A gated request holds its download slot while it waits for headers. The header timeout is 30 seconds.
        var gates = new[] { handler.ServeGated(thumbnailA, await Png(Bgra.Green)), handler.ServeGated(thumbnailB, await Png(Bgra.Gray)) };
        await using var scope = new ManagerScope(handler, Idle);
        try
        {
            var stalled = new[] { scope.Manager.GetOrLoadImageAsync(stalledA, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher),
                scope.Manager.GetOrLoadImageAsync(stalledB, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher) };
            await TestWait.ForTaskAsync(Task.WhenAll(stalled), "both stalled downloads to terminate");
            Assert.IsNull((await stalled[0]).Image);
            Assert.IsNull((await stalled[1]).Image);

            // Both later loads must hold a slot at the same time. A single leaked slot keeps the second gate unreached.
            var loads = new[] { scope.Manager.GetOrLoadImageAsync(thumbnailA, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher),
                scope.Manager.GetOrLoadImageAsync(thumbnailB, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher) };
            await gates[0].WaitUntilStartedAsync();
            await gates[1].WaitUntilStartedAsync();
            foreach (var gate in gates)
            {
                gate.Release();
            }

            await TestWait.ForTaskAsync(Task.WhenAll(loads), "both concurrent thumbnail loads to settle");
            ImageFixtures.AssertColor(ImageFixtures.AssertRaster((await loads[0]).Image, Size, Size, "thumbnail A"), Bgra.Green, "thumbnail A");
            ImageFixtures.AssertColor(ImageFixtures.AssertRaster((await loads[1]).Image, Size, Size, "thumbnail B"), Bgra.Gray, "thumbnail B");

            var pending = scope.Manager.GetOrLoadOriginalImageAsync(original, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher);
            await TestWait.ForTaskAsync(pending, "the original load after the stalls");
            ImageFixtures.AssertColor(ImageFixtures.AssertRaster((await pending).Image, Size, Size, "original"), Bgra.Blue, "original");
        }
        finally
        {
            foreach (var gate in gates)
            {
                gate.Release();
            }
        }
    }

    // Criterion 3: an ordered request displays a later candidate after a stalled candidate exhausts retries.
    [UITestMethod]
    public async Task Stalled_candidate_falls_back_to_the_later_candidate()
    {
        var stalled = TestUris.Next("stalled.png");
        var later = TestUris.Next("later.png");
        var handler = new FixtureHandler()
            .ServeScripted(stalled, await ImageFixtures.PngAsync(48, 24, Bgra.Blue), BodyScript.StallAfter(32))
            .Serve(later, await ImageFixtures.PngAsync(24, 48, Bgra.Red));
        await using var scope = new ManagerScope(handler, Idle);
        await using var harness = await ControlHarness.CreateAsync(scope.Manager, control =>
        {
            control.DecodePixelWidth = 24;
            control.DecodePixelHeight = 48;
        });

        harness.Control.Source = new ImageRequest(new[] { new ImageRequestCandidate(stalled), new ImageRequestCandidate(later) });
        await harness.WaitForOpenedAsync();
        await harness.SettleAsync();

        Assert.AreEqual(0, harness.FailedCount, "The fallback published a failure.");
        Assert.AreEqual("Loaded", harness.CurrentState);
        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(harness.DisplayedSource, 24, 48, "later candidate"), Bgra.Red, "later candidate");
    }

    // Criterion 4: a progressing body outlasts several idle budgets. Each read waits a twentieth of the budget.
    [UITestMethod]
    public async Task Progressing_body_outlasts_several_idle_budgets()
    {
        var idle = TimeSpan.FromMilliseconds(500);
        var uri = TestUris.Next("progressing.png");
        // At least 128 one-byte reads of at least 25 ms each take at least 3.2 seconds, more than six idle budgets.
        var png = await ImageFixtures.PaddedPngAsync(Size, Size, Bgra.Gray, Math.Max((await Png(Bgra.Gray)).Length, 128));

        var handler = new FixtureHandler().ServeScripted(uri, png, BodyScript.Progressing(idle / 20, chunkBytes: 1));
        await using var scope = new ManagerScope(handler, idle);

        var watch = Stopwatch.StartNew();
        var result = await scope.LoadAsync(uri);
        watch.Stop();

        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(result.Image, Size, Size, "progressing body"), Bgra.Gray, "progressing body");
        Assert.IsTrue(watch.Elapsed >= idle * 4, $"The body took {watch.ElapsedMilliseconds} ms, under four idle budgets.");
    }

    // Criterion 5: canceling one waiter leaves the other waiter with the completed image.
    [UITestMethod]
    public async Task Canceling_one_waiter_keeps_the_shared_body_for_the_other()
    {
        var uri = TestUris.Next("shared.png");
        var gate = BodyScript.Gated();
        var handler = new FixtureHandler().ServeScripted(uri, await Png(Bgra.Green), gate);
        await using var scope = new ManagerScope(handler, bodyIdleTimeout: null);

        using var canceled = new CancellationTokenSource();
        var first = scope.Manager.GetOrLoadImageAsync(uri, Size, Size, DecodePixelType.Physical, canceled.Token, scope.Dispatcher);
        var second = scope.Manager.GetOrLoadImageAsync(uri, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher);
        await gate.WaitUntilStalledAsync();

        canceled.Cancel();
        await TestWait.ForTaskAsync(first, "the canceled waiter to settle");
        await Assert.ThrowsAsync<OperationCanceledException>(() => first);
        Assert.IsFalse(second.IsCompleted, "The surviving waiter settled before its body arrived.");

        gate.Release();
        await TestWait.ForTaskAsync(second, "the surviving waiter to receive the image");
        ImageFixtures.AssertColor(ImageFixtures.AssertRaster((await second).Image, Size, Size, "survivor"), Bgra.Green, "survivor");
        Assert.IsTrue(Directory.EnumerateFiles(scope.CacheDirectory, "*.png").Any(), "The shared body was not written to the disk cache.");
    }

    // Criterion 6: canceling every waiter settles them, and later loads succeed without backoff or a leaked slot.
    [UITestMethod]
    public async Task Canceling_every_waiter_settles_and_later_loads_succeed()
    {
        var uri = TestUris.Next("abandoned.png");
        var other = TestUris.Next("other.png");
        var stall = BodyScript.StallAfter(32);
        var handler = new FixtureHandler()
            .ServeScripted(uri, await Png(Bgra.Red), stall, BodyScript.Complete())
            .Serve(other, await Png(Bgra.Blue));

        // One slot, so a slot leaked by the abandoned download would block both later loads.
        await using var scope = new ManagerScope(handler, bodyIdleTimeout: null, maxConcurrentDownloads: 1);

        var waiters = new List<(CancellationTokenSource Cancellation, Task<ImageExCacheManager.CacheResult> Load, bool ReturnsNull)>();
        for (var i = 0; i < 3; i++)
        {
            var cancellation = new CancellationTokenSource();
            var returnsNull = i % 2 == 1;
            waiters.Add((cancellation, scope.Manager.GetOrLoadImageAsync(uri, Size, Size, DecodePixelType.Physical, cancellation.Token,
                scope.Dispatcher, returnNullOnCancellation: returnsNull), returnsNull));
        }

        await stall.WaitUntilStalledAsync();
        foreach (var waiter in waiters)
        {
            waiter.Cancellation.Cancel();
        }

        foreach (var waiter in waiters)
        {
            await TestWait.ForTaskAsync(waiter.Load, "a canceled waiter to settle");
            if (waiter.ReturnsNull)
            {
                Assert.IsNull((await waiter.Load).Image);
            }
            else
            {
                await Assert.ThrowsAsync<OperationCanceledException>(() => waiter.Load);
            }

            waiter.Cancellation.Dispose();
        }

        var again = await scope.LoadAsync(uri);
        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(again.Image, Size, Size, "same URL"), Bgra.Red, "same URL");
        var unrelated = await scope.LoadAsync(other);
        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(unrelated.Image, Size, Size, "other URL"), Bgra.Blue, "other URL");
    }

    // Criterion 7: replacement and unload during a stalled body keep late completions off the control.
    [UITestMethod]
    public async Task Replacement_during_a_stalled_body_keeps_the_replacement()
    {
        var retired = TestUris.Next("retired.png");
        var replacement = TestUris.Next("replacement.png");
        var gate = BodyScript.Gated();
        var handler = new FixtureHandler()
            .ServeScripted(retired, await Png(Bgra.Blue), gate)
            .Serve(replacement, await Png(Bgra.Red));
        await using var scope = new ManagerScope(handler, bodyIdleTimeout: null);
        await using var harness = await ControlHarness.CreateAsync(scope.Manager, control =>
        {
            control.DecodePixelWidth = Size;
            control.DecodePixelHeight = Size;
        });

        harness.Control.Source = retired;
        await gate.WaitUntilStalledAsync();
        harness.Control.Source = replacement;
        await harness.WaitForOpenedAsync();
        var displayed = harness.DisplayedSource;

        gate.Release();
        await harness.SettleAsync();

        Assert.AreEqual(0, harness.FailedCount, "The retired request published a failure.");
        Assert.AreEqual(1, harness.OpenedCount, "The retired request opened an image.");
        Assert.AreSame(displayed, harness.DisplayedSource, "The retired request replaced the displayed image.");
        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(harness.DisplayedSource, Size, Size, "replacement"), Bgra.Red, "replacement");
    }

    [UITestMethod]
    public async Task Unload_during_a_stalled_body_stays_empty()
    {
        var uri = TestUris.Next("unloaded.png");
        var gate = BodyScript.Gated();
        var handler = new FixtureHandler().ServeScripted(uri, await Png(Bgra.Blue), gate);
        await using var scope = new ManagerScope(handler, bodyIdleTimeout: null);
        await using var harness = await ControlHarness.CreateAsync(scope.Manager);

        harness.Control.Source = uri;
        await gate.WaitUntilStalledAsync();
        await harness.RemoveFromTreeAsync();

        gate.Release();
        await harness.SettleAsync();

        Assert.AreEqual(0, harness.FailedCount, "The unloaded request published a failure.");
        Assert.AreEqual(0, harness.OpenedCount, "The unloaded request opened an image.");
        Assert.IsNull(harness.DisplayedSource);
        Assert.IsFalse(harness.TryGetNaturalSize(out _));
    }

    // Criterion 8: an original body that ends early leaves no partial temporary file.
    // Each stalled script observes the temporary file from inside its pending read. Production cannot settle
    // that read, or delete the file, while the observation runs, so the precondition does not race cleanup.
    // The prefix exceeds the 64 KiB file buffer, so at least one write has reached the disk.
    [UITestMethod]
    public async Task Original_cancellation_during_the_body_leaves_no_temporary_file()
    {
        var uri = TestUris.Next("canceled-original.png");
        var observation = new PrefixObservation();
        var stall = BodyScript.StallAfter(OriginalPrefixBytes, onStall: observation.Capture);
        var handler = new FixtureHandler().ServeScripted(uri, await OriginalPng(Bgra.Green), stall, BodyScript.Complete());
        await using var scope = new ManagerScope(handler, bodyIdleTimeout: null);
        observation.Directory = scope.CacheDirectory;

        using (var cancellation = new CancellationTokenSource())
        {
            var pending = scope.Manager.GetOrLoadOriginalImageAsync(uri, Size, Size, DecodePixelType.Physical, cancellation.Token, scope.Dispatcher);
            await stall.WaitUntilStalledAsync();
            observation.AssertPrefixWritten();
            cancellation.Cancel();
            await TestWait.ForTaskAsync(pending, "the canceled original to settle");
            await Assert.ThrowsAsync<OperationCanceledException>(() => pending);
        }

        Assert.IsFalse(scope.TemporaryOriginalFiles().Any(), "Cancellation left a temporary original file.");
        var retried = await scope.LoadOriginalAsync(uri);
        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(retried.Image, Size, Size, "original retry"), Bgra.Green, "original retry");
    }

    [UITestMethod]
    public async Task Original_shutdown_during_the_body_leaves_no_temporary_file()
    {
        var uri = TestUris.Next("shutdown-original.png");
        var observation = new PrefixObservation();
        var stall = BodyScript.StallAfter(OriginalPrefixBytes, onStall: observation.Capture);
        var handler = new FixtureHandler().ServeScripted(uri, await OriginalPng(Bgra.Green), stall);
        await using var scope = new ManagerScope(handler, bodyIdleTimeout: null);
        observation.Directory = scope.CacheDirectory;

        var pending = scope.Manager.GetOrLoadOriginalImageAsync(uri, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher);
        await stall.WaitUntilStalledAsync();
        observation.AssertPrefixWritten();

        await TestWait.ForTaskAsync(scope.Manager.DisposeAsync().AsTask(), "manager disposal during an original body");
        await TestWait.ForTaskAsync(pending, "the original to settle after shutdown");
        Assert.IsNull((await pending).Image);
        Assert.IsFalse(scope.TemporaryOriginalFiles().Any(), "Shutdown left a temporary original file.");
    }

    [UITestMethod]
    public async Task Original_idle_expiry_after_a_prefix_leaves_no_temporary_file()
    {
        var uri = TestUris.Next("expired-original.png");
        var next = TestUris.Next("next-original.png");
        var observation = new PrefixObservation();
        var stall = BodyScript.StallAfter(OriginalPrefixBytes, onStall: observation.Capture);
        var handler = new FixtureHandler()
            .ServeScripted(uri, await OriginalPng(Bgra.Green), stall)
            .Serve(next, await Png(Bgra.Red));
        await using var scope = new ManagerScope(handler, Idle);
        observation.Directory = scope.CacheDirectory;

        var pending = scope.Manager.GetOrLoadOriginalImageAsync(uri, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher);
        await TestWait.ForTaskAsync(pending, "the stalled original to expire");
        Assert.IsNull((await pending).Image);
        observation.AssertPrefixWritten();
        Assert.IsFalse(scope.TemporaryOriginalFiles().Any(), "Idle expiry left a temporary original file.");

        var loaded = await scope.LoadOriginalAsync(next);
        ImageFixtures.AssertColor(ImageFixtures.AssertRaster(loaded.Image, Size, Size, "next original"), Bgra.Red, "next original");
    }

    // Supporting case: disposal does not cancel a cached body after its headers. The idle timeout settles it.
    [UITestMethod]
    public async Task Shutdown_during_a_stalled_thumbnail_body_settles_through_the_idle_timeout()
    {
        var uri = TestUris.Next("shutdown-thumbnail.png");
        var stall = BodyScript.StallAfter(0);
        var handler = new FixtureHandler().ServeScripted(uri, await Png(Bgra.Red), stall);
        await using var scope = new ManagerScope(handler, Idle);

        var pending = scope.Manager.GetOrLoadImageAsync(uri, Size, Size, DecodePixelType.Physical, CancellationToken.None, scope.Dispatcher);
        await stall.WaitUntilStalledAsync();
        var watch = Stopwatch.StartNew();
        await TestWait.ForTaskAsync(scope.Manager.DisposeAsync().AsTask(), "manager disposal during a stalled thumbnail body");
        watch.Stop();
        TestContext.WriteLine($"Disposal during a stalled thumbnail body took {watch.ElapsedMilliseconds} ms with a {Idle.TotalMilliseconds} ms idle timeout.");

        await TestWait.ForTaskAsync(pending, "the stalled thumbnail to settle after shutdown");
        Assert.IsNull((await pending).Image);
    }

    private static Task<byte[]> Png(Bgra color) => ImageFixtures.PngAsync(Size, Size, color);

    private const int OriginalPrefixBytes = 96 * 1024;

    private static Task<byte[]> OriginalPng(Bgra color) => ImageFixtures.PaddedPngAsync(Size, Size, color, 128 * 1024);

    /// <summary>
    /// Records the temporary original files and their on-disk lengths from inside the pending stalled read.
    /// </summary>
    private sealed class PrefixObservation
    {
        private long[]? _lengths;

        public string? Directory { get; set; }

        public void Capture()
            => _lengths = System.IO.Directory.EnumerateFiles(Directory!, "original-*.tmp", SearchOption.AllDirectories)
                .Select(path => new FileInfo(path).Length)
                .ToArray();

        public void AssertPrefixWritten()
        {
            Assert.IsNotNull(_lengths, "The original body never reached its stall.");
            Assert.AreEqual(1, _lengths.Length, "Expected one temporary original file while the body was stalled.");
            Assert.IsTrue(_lengths[0] > 0, "The temporary original file held no written prefix while the body was stalled.");
        }
    }

    private static async Task AssertStallFailsThenBacksOffAsync(Func<BodyScript> stall)
    {
        var uri = TestUris.Next("stalled.png");

        // Three stalled attempts, then a body that would succeed if backoff did not apply.
        var handler = new FixtureHandler().ServeScripted(uri, await Png(Bgra.Red), stall(), stall(), stall(), BodyScript.Complete());
        await using var scope = new ManagerScope(handler, Idle);

        var failed = await scope.LoadAsync(uri);
        Assert.IsNull(failed.Image, "A stalled body produced an image.");
        var backedOff = await scope.LoadAsync(uri);
        Assert.IsNull(backedOff.Image, "The terminal stall did not enter failure backoff.");
    }

    /// <summary>
    /// Owns one manager and cache directory. Disposal aborts every stalled script first,
    /// so a failed test still releases its transport and drains the manager.
    /// </summary>
    private sealed class ManagerScope : IAsyncDisposable
    {
        private readonly TempCacheDirectory _directory = new();

        public ManagerScope(FixtureHandler handler, TimeSpan? bodyIdleTimeout, int maxConcurrentDownloads = 2)
        {
            Manager = new ImageExCacheManager(_directory.Path, handler, maxConcurrentDownloads, bodyIdleTimeout: bodyIdleTimeout);
            Dispatcher = DispatcherQueue.GetForCurrentThread();
        }

        public ImageExCacheManager Manager { get; }

        public DispatcherQueue Dispatcher { get; }

        public string CacheDirectory => _directory.Path;

        public IEnumerable<string> TemporaryOriginalFiles()
            => Directory.EnumerateFiles(_directory.Path, "original-*.tmp", SearchOption.AllDirectories);

        public async Task<ImageExCacheManager.CacheResult> LoadAsync(Uri uri)
        {
            var pending = Manager.GetOrLoadImageAsync(uri, Size, Size, DecodePixelType.Physical, CancellationToken.None, Dispatcher);
            await TestWait.ForTaskAsync(pending, $"the load of {uri.Segments[^1]} to settle");
            return await pending;
        }

        public async Task<ImageExCacheManager.CacheResult> LoadOriginalAsync(Uri uri)
        {
            var pending = Manager.GetOrLoadOriginalImageAsync(uri, Size, Size, DecodePixelType.Physical, CancellationToken.None, Dispatcher);
            await TestWait.ForTaskAsync(pending, $"the original load of {uri.Segments[^1]} to settle");
            return await pending;
        }

        public async ValueTask DisposeAsync()
        {
            BodyScript.AbortAll();

            // Disposal does not cancel a cached download that waits for a download slot, so a leaked slot
            // would keep disposal pending. The guard fails the test instead of hanging the lane.
            var disposal = Manager.DisposeAsync().AsTask();
            await TestWait.ForTaskAsync(disposal, "the manager to dispose");
            _directory.Dispose();
        }
    }
}
