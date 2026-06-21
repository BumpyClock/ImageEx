// ABOUTME: Singleton managing ImageEx disk cache lifecycle, in-flight deduplication,
// ABOUTME: shared HttpClient, and cleanup orchestration with TTL and LRU eviction.

#nullable enable

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime.InteropServices.WindowsRuntime;
using ImageEx;
using Microsoft.UI.Dispatching;
using Windows.Graphics.Imaging;
using Windows.Storage.Streams;

namespace ImageEx.Cache;

/// <summary>
/// Singleton managing the ImageEx disk cache lifecycle including downloads,
/// in-flight deduplication, and cleanup orchestration.
/// </summary>
/// <remarks>
/// This is a static singleton and is typically kept for app lifetime.
/// It still implements <see cref="IDisposable"/> so tests or explicit host shutdown can flush/tear down resources deterministically.
/// </remarks>
internal sealed partial class ImageExCacheManager : IDisposable
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    private static readonly Lazy<ImageExCacheManager> s_instance = new(() => new ImageExCacheManager());

    public static ImageExCacheManager Instance => s_instance.Value;

    private readonly ImageExDiskCache _diskCache;
    private readonly HttpClient _httpClient;
    private readonly ConcurrentDictionary<string, Lazy<SharedDownload>> _inFlightDownloads = new();
    private readonly ConcurrentDictionary<string, DateTimeOffset> _recentDownloadFailures = new();
    private readonly Dictionary<string, CacheWriteLock> _cacheWriteLocks = new();
    private readonly object _cacheWriteLocksGate = new();
    private readonly SemaphoreSlim _cleanupLock = new(1, 1);
    private readonly SemaphoreSlim _downloadConcurrency;
    private readonly SemaphoreSlim _decodeConcurrency;
    private DateTimeOffset _lastCleanup = DateTimeOffset.MinValue;
    private bool _initialSizeScanned;
    private bool _disposed;
    private const int TelemetrySampleInterval = 50;
    private const int MaxSmallDecodedImageCacheEntries = 128;
    private const long MaxSmallDecodedImageCacheBytes = 32L * 1024 * 1024;
    private const long MaxSmallDecodedImageCacheEntryBytes = 2L * 1024 * 1024;
    private const int MaxSmallDecodedImageCacheDimension = 512;
    private static readonly TimeSpan DownloadFailureBackoff = TimeSpan.FromMinutes(10);
    private readonly object _smallDecodedImageCacheLock = new();
    private readonly Dictionary<string, SmallDecodedImageCacheEntry> _smallDecodedImageCache = new();
    private readonly LinkedList<string> _smallDecodedImageCacheLru = new();
    private long _smallDecodedImageCacheBytes;
    private static long _telemetryRequestCount;
    private static long _telemetryCacheHitCount;
    private static long _telemetryCacheMissCount;
    private static long _telemetryDownloadStartCount;
    private static long _telemetryDownloadSuccessCount;
    private static long _telemetryDownloadFailureCount;
    private static long _telemetryDownloadByteCount;
    private static long _telemetryCacheWriteStartCount;
    private static long _telemetryCacheWriteCompleteCount;
    private static long _telemetryCacheWriteWaitMsTotal;
    private static long _telemetryActiveCacheWriteCount;
    private static long _telemetryPeakActiveCacheWriteCount;
    private static long _telemetryCacheWriteLockRemovalCount;
    private static long _telemetryFileDecodeStartCount;
    private static long _telemetryByteDecodeStartCount;
    private static long _telemetryDecodeSuccessCount;
    private static long _telemetryDecodeNullCount;
    private static long _telemetryDecodeCancelCount;
    private static long _telemetryDecodeFailureCount;
    private static long _telemetryActiveDecodeCount;
    private static long _telemetryPeakActiveDecodeCount;

    private sealed record SmallDecodedImageCacheEntry(ImageSource Image, LinkedListNode<string> LruNode, long SizeBytes);

    private sealed class CacheWriteLock : IDisposable
    {
        public SemaphoreSlim Semaphore { get; } = new(1, 1);

        public int ReferenceCount { get; set; }

        public void Dispose()
        {
            Semaphore.Dispose();
        }
    }

    /// <summary>
    /// Maximum cache age in days (configurable via DP).
    /// </summary>
    public int MaxCacheDays { get; set; } = ImageExCacheConstants.DefaultCacheDays;

    /// <summary>
    /// Maximum cache size in bytes (configurable via DP).
    /// </summary>
    public long MaxCacheSizeBytes { get; set; } = ImageExCacheConstants.DefaultCacheSizeBytes;

    private ImageExCacheManager()
        : this(ImageExCacheConstants.GetCacheDirectory())
    {
    }

    internal ImageExCacheManager(
        string cacheDirectory,
        HttpMessageHandler? httpMessageHandler = null,
        int maxConcurrentDownloads = 2,
        int maxConcurrentDecodes = 2)
    {
        _diskCache = new ImageExDiskCache(cacheDirectory);
        _httpClient = httpMessageHandler == null
            ? new HttpClient { Timeout = TimeSpan.FromSeconds(30) }
            : new HttpClient(httpMessageHandler) { Timeout = TimeSpan.FromSeconds(30) };
        maxConcurrentDownloads = Math.Max(1, maxConcurrentDownloads);
        maxConcurrentDecodes = Math.Max(1, maxConcurrentDecodes);
        _downloadConcurrency = new SemaphoreSlim(maxConcurrentDownloads, maxConcurrentDownloads);
        _decodeConcurrency = new SemaphoreSlim(maxConcurrentDecodes, maxConcurrentDecodes);
    }

    /// <summary>
    /// Result of a cache lookup, including whether it was a cache hit for shimmer skip.
    /// </summary>
    /// <param name="Image">The loaded ImageSource, or null on failure.</param>
    /// <param name="WasCacheHit">True if served from disk cache (skip shimmer).</param>
    public record CacheResult(ImageSource? Image, bool WasCacheHit);

    internal readonly record struct ImageExCacheDiagnosticsSnapshot(
        bool IsInitialized,
        int SmallDecodedImageCacheEntries,
        long SmallDecodedImageCacheBytes,
        int InFlightDownloads,
        int CacheWriteLockCount,
        long CacheWriteLockRemovals,
        int DiskCacheEntryCount,
        long DiskCacheBytes);

    private readonly record struct DownloadResult(byte[]? Bytes, string? ContentType);

    private readonly record struct DecodeDimensions(
        int TargetWidth,
        int TargetHeight,
        int NaturalWidth,
        int NaturalHeight);

    private sealed class SharedDownload : IDisposable
    {
        private readonly object _gate = new();
        private readonly CancellationTokenSource _cancellation = new();
        private int _waiterCount;
        private bool _cancelledForNoWaiters;

        public SharedDownload(Func<CancellationToken, Task<DownloadResult>> startDownload)
        {
            Task = startDownload(_cancellation.Token);
        }

        public Task<DownloadResult> Task { get; }

        public bool TryAddWaiter()
        {
            lock (_gate)
            {
                if (_cancelledForNoWaiters)
                {
                    return false;
                }

                _waiterCount++;
                return true;
            }
        }

        public bool ReleaseWaiterShouldCancel()
        {
            lock (_gate)
            {
                _waiterCount--;
                if (_waiterCount == 0 && !Task.IsCompleted)
                {
                    _cancelledForNoWaiters = true;
                    return true;
                }

                return false;
            }
        }

        public void Cancel()
        {
            try
            {
                _cancellation.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // Completion can dispose the shared CTS between last-waiter release and cancel.
            }
        }

        public void Dispose()
        {
            lock (_gate)
            {
                _cancellation.Dispose();
            }
        }
    }

    internal static ImageExCacheDiagnosticsSnapshot CaptureDiagnosticsSnapshot()
    {
        return s_instance.IsValueCreated
            ? s_instance.Value.CaptureDiagnosticsSnapshotCore()
            : new ImageExCacheDiagnosticsSnapshot(false, 0, 0, 0, 0, 0, 0, 0);
    }

    internal ImageExCacheDiagnosticsSnapshot CaptureInstanceDiagnosticsSnapshot()
    {
        return CaptureDiagnosticsSnapshotCore();
    }

    private ImageExCacheDiagnosticsSnapshot CaptureDiagnosticsSnapshotCore()
    {
        int smallDecodedCount;
        long smallDecodedBytes;
        lock (_smallDecodedImageCacheLock)
        {
            smallDecodedCount = _smallDecodedImageCache.Count;
            smallDecodedBytes = _smallDecodedImageCacheBytes;
        }

        int cacheWriteLockCount;
        lock (_cacheWriteLocksGate)
        {
            cacheWriteLockCount = _cacheWriteLocks.Count;
        }

        var diskEntries = _diskCache.GetAllEntries().ToArray();
        return new ImageExCacheDiagnosticsSnapshot(
            true,
            smallDecodedCount,
            smallDecodedBytes,
            _inFlightDownloads.Count,
            cacheWriteLockCount,
            Interlocked.Read(ref _telemetryCacheWriteLockRemovalCount),
            diskEntries.Length,
            diskEntries.Sum(entry => entry.Value.SizeBytes));
    }

    /// <summary>
    /// Gets an image from cache or downloads it, with in-flight deduplication.
    /// </summary>
    /// <param name="uri">Image URI to load.</param>
    /// <param name="decodeWidth">Decode pixel width.</param>
    /// <param name="decodeHeight">Decode pixel height.</param>
    /// <param name="decodeType">Decode pixel type.</param>
    /// <param name="token">Cancellation token.</param>
    /// <param name="dispatcherQueue">Optional dispatcher queue for UI thread marshaling.</param>
    /// <param name="dpiScale">Optional DPI scale factor (e.g., 1.0, 1.5, 2.0) for adaptive fallback sizing.</param>
    /// <returns>CacheResult with the image and cache hit status.</returns>
    public async Task<CacheResult> GetOrLoadImageAsync(
        Uri uri,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        CancellationToken token,
        DispatcherQueue? dispatcherQueue = null,
        double dpiScale = 1.0,
        bool returnNullOnCancellation = false)
    {
        if (token.IsCancellationRequested)
        {
            return CancelledResult(token, returnNullOnCancellation);
        }

        // Skip non-http URIs - return null to let base pipeline handle
        if (!uri.IsHttpUri())
            return new CacheResult(null, false);

        var isSvg = uri.AbsolutePath.EndsWith(".svg", StringComparison.OrdinalIgnoreCase);
        var cacheKey = ImageExDiskCache.ComputeSourceCacheKey(uri);
        if (TryGetSmallDecodedImage(uri, decodeWidth, decodeHeight, decodeType, isSvg, dpiScale, out var cachedSmallImage))
        {
            RecordCacheHit(uri, payloadBytes: 0, decodeWidth, decodeHeight, decodeType);
            return new CacheResult(cachedSmallImage, WasCacheHit: true);
        }

        RecordImageRequest(uri, decodeWidth, decodeHeight, decodeType);

        await _diskCache.EnsureMetadataLoadedAsync().ConfigureAwait(false);

        // 1. Try local cache
        var hasCacheEntry = _diskCache.TryGetEntry(cacheKey, out var entry) && entry != null;
        if (!hasCacheEntry)
        {
            hasCacheEntry = TryMigrateLegacyCacheEntry(cacheKey, uri, out entry);
        }

        if (hasCacheEntry && entry != null)
        {
            var filePath = _diskCache.GetFilePath(cacheKey, entry.Extension);
            var age = DateTimeOffset.UtcNow - entry.DownloadedUtc;

            if (age.TotalDays < MaxCacheDays && File.Exists(filePath))
            {
                try
                {
                    if (token.IsCancellationRequested)
                    {
                        return CancelledResult(token, returnNullOnCancellation);
                    }

                    var cachedIsSvg = entry.Extension == ".svg";
                    var image = await LoadFromFileAsync(cacheKey, filePath, cachedIsSvg, decodeWidth, decodeHeight, decodeType, dispatcherQueue, dpiScale, token, returnNullOnCancellation).ConfigureAwait(false);

                    if (image != null)
                    {
                        StoreSmallDecodedImage(uri, decodeWidth, decodeHeight, decodeType, cachedIsSvg, dpiScale, image);
                        _diskCache.UpdateAccessTime(cacheKey);
                        // Fire-and-forget to persist LRU update
                        _ = Task.Run(async () =>
                        {
                            try { await _diskCache.SaveMetadataAsync().ConfigureAwait(false); }
                            catch { /* Best effort */ }
                        });

                        RecordCacheHit(uri, entry.SizeBytes, decodeWidth, decodeHeight, decodeType);
                        ForgetDownloadFailure(cacheKey);
                        return new CacheResult(image, WasCacheHit: true);
                    }
                    else
                    {
                        if (token.IsCancellationRequested)
                        {
                            return CancelledResult(token, returnNullOnCancellation);
                        }

                        // Cached file failed to decode; remove and re-download.
                        RemoveCacheEntryIfDeleted(cacheKey, filePath);
                    }
                }
                catch (OperationCanceledException) when (!returnNullOnCancellation)
                {
                    throw;
                }
                catch (OperationCanceledException)
                {
                    return new CacheResult(null, false);
                }
                catch
                {
                    // Corrupt file - delete and re-download
                    RemoveCacheEntryIfDeleted(cacheKey, filePath);
                }
            }
            else
            {
                // Expired - clean up
                var expiredPath = _diskCache.GetFilePath(cacheKey, entry.Extension);
                RemoveCacheEntryIfDeleted(cacheKey, expiredPath);
            }
        }

        // 2. Download with in-flight deduplication
        if (IsRecentDownloadFailure(cacheKey))
        {
            return new CacheResult(null, false);
        }

        RecordCacheMiss(uri, decodeWidth, decodeHeight, decodeType);
        var result = await AwaitSharedDownloadAsync(cacheKey, uri, token, returnNullOnCancellation).ConfigureAwait(false);

        if (result.Bytes == null || token.IsCancellationRequested)
        {
            if (!token.IsCancellationRequested)
            {
                RememberDownloadFailure(cacheKey);
            }

            return new CacheResult(null, false);
        }

        ForgetDownloadFailure(cacheKey);

        // Detect SVG from content-type if URL didn't have .svg extension
        var detectedSvg = isSvg || result.ContentType == "image/svg+xml";
        var extension = ImageExDiskCache.GetExtension(uri, result.ContentType, detectedSvg);
        var newFilePath = _diskCache.GetFilePath(cacheKey, extension);

        // 3. Save to disk
        try
        {
            if (token.IsCancellationRequested)
            {
                return CancelledResult(token, returnNullOnCancellation);
            }

            await SaveDownloadedFileAsync(cacheKey, uri, newFilePath, extension, result.Bytes, token, returnNullOnCancellation).ConfigureAwait(false);

            // Fire-and-forget: persist metadata + cleanup check
            _ = Task.Run(async () =>
            {
                try
                {
                    await _diskCache.SaveMetadataAsync().ConfigureAwait(false);
                    await EnforceCleanupIfNeededAsync().ConfigureAwait(false);
                }
                catch { /* Best effort */ }
            });
        }
        catch (OperationCanceledException) when (!returnNullOnCancellation)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return new CacheResult(null, false);
        }
        catch
        {
            // Disk save failed - still return image from memory
        }

        // 4. Return image from downloaded bytes
        var loadedImage = await LoadFromBytesAsync(result.Bytes, detectedSvg, decodeWidth, decodeHeight, decodeType, dispatcherQueue, dpiScale, token, returnNullOnCancellation).ConfigureAwait(false);
        if (loadedImage != null)
        {
            StoreSmallDecodedImage(uri, decodeWidth, decodeHeight, decodeType, detectedSvg, dpiScale, loadedImage);
        }

        return new CacheResult(loadedImage, WasCacheHit: false);
    }

    private bool TryGetSmallDecodedImage(
        Uri uri,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        bool isSvg,
        double dpiScale,
        out ImageSource? image)
    {
        image = null;
        if (!TryEstimateDecodedImageCacheBytes(decodeWidth, decodeHeight, isSvg, out _))
        {
            return false;
        }

        var cacheKey = ComputeDecodedImageCacheKey(uri, decodeWidth, decodeHeight, decodeType, isSvg, dpiScale);
        lock (_smallDecodedImageCacheLock)
        {
            if (!_smallDecodedImageCache.TryGetValue(cacheKey, out var entry))
            {
                return false;
            }

            _smallDecodedImageCacheLru.Remove(entry.LruNode);
            _smallDecodedImageCacheLru.AddLast(entry.LruNode);
            image = entry.Image;
            return true;
        }
    }

    private void StoreSmallDecodedImage(
        Uri uri,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        bool isSvg,
        double dpiScale,
        ImageSource image)
    {
        if (!TryEstimateDecodedImageCacheBytes(decodeWidth, decodeHeight, isSvg, out var sizeBytes))
        {
            return;
        }

        var cacheKey = ComputeDecodedImageCacheKey(uri, decodeWidth, decodeHeight, decodeType, isSvg, dpiScale);
        lock (_smallDecodedImageCacheLock)
        {
            if (_smallDecodedImageCache.TryGetValue(cacheKey, out var existing))
            {
                _smallDecodedImageCacheLru.Remove(existing.LruNode);
                _smallDecodedImageCache.Remove(cacheKey);
                _smallDecodedImageCacheBytes -= existing.SizeBytes;
            }

            var node = _smallDecodedImageCacheLru.AddLast(cacheKey);
            _smallDecodedImageCache[cacheKey] = new SmallDecodedImageCacheEntry(image, node, sizeBytes);
            _smallDecodedImageCacheBytes += sizeBytes;

            while (_smallDecodedImageCache.Count > MaxSmallDecodedImageCacheEntries ||
                _smallDecodedImageCacheBytes > MaxSmallDecodedImageCacheBytes)
            {
                var first = _smallDecodedImageCacheLru.First;
                if (first == null)
                {
                    break;
                }

                _smallDecodedImageCacheLru.RemoveFirst();
                if (_smallDecodedImageCache.Remove(first.Value, out var removed))
                {
                    _smallDecodedImageCacheBytes -= removed.SizeBytes;
                }
            }
        }
    }

    internal static bool TryEstimateDecodedImageCacheBytes(int decodeWidth, int decodeHeight, bool isSvg, out long sizeBytes)
    {
        sizeBytes = 0;
        if (isSvg)
        {
            return false;
        }

        if (decodeWidth <= 0 && decodeHeight <= 0)
        {
            return false;
        }

        var estimateWidth = decodeWidth > 0 ? decodeWidth : decodeHeight;
        var estimateHeight = decodeHeight > 0 ? decodeHeight : decodeWidth;
        if (estimateWidth <= 0 ||
            estimateHeight <= 0 ||
            estimateWidth > MaxSmallDecodedImageCacheDimension ||
            estimateHeight > MaxSmallDecodedImageCacheDimension)
        {
            return false;
        }

        sizeBytes = (long)estimateWidth * estimateHeight * 4;
        return sizeBytes <= MaxSmallDecodedImageCacheEntryBytes;
    }

    private static string ComputeDecodedImageCacheKey(
        Uri uri,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        bool isSvg,
        double dpiScale)
    {
        var dpiBucket = (int)Math.Round(Math.Clamp(dpiScale, 0.5, 4.0) * 100);
        return $"{ImageExDiskCache.ComputeCacheKey(uri, decodeWidth, decodeHeight, decodeType, isSvg)}:{dpiBucket}";
    }

    private static CacheResult CancelledResult(CancellationToken token, bool returnNullOnCancellation)
    {
        if (returnNullOnCancellation)
        {
            return new CacheResult(null, false);
        }

        token.ThrowIfCancellationRequested();
        return new CacheResult(null, false);
    }

    private async Task<DownloadResult> AwaitSharedDownloadAsync(
        string cacheKey,
        Uri uri,
        CancellationToken token,
        bool returnNullOnCancellation)
    {
        while (true)
        {
            var lazyDownload = _inFlightDownloads.GetOrAdd(cacheKey, _ => CreateSharedDownloadLazy(cacheKey, uri));
            var sharedDownload = lazyDownload.Value;

            if (!sharedDownload.TryAddWaiter())
            {
                RemoveSharedDownload(cacheKey, lazyDownload);
                continue;
            }

            try
            {
                if (returnNullOnCancellation)
                {
                    var result = await WaitForDownloadOrCancellationAsync(sharedDownload.Task, token).ConfigureAwait(false);
                    return result ?? new DownloadResult(null, null);
                }

                return await sharedDownload.Task.WaitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (returnNullOnCancellation)
            {
                return new DownloadResult(null, null);
            }
            finally
            {
                if (sharedDownload.ReleaseWaiterShouldCancel())
                {
                    RemoveSharedDownload(cacheKey, lazyDownload);
                    sharedDownload.Cancel();
                }
            }
        }
    }

    private bool IsRecentDownloadFailure(string cacheKey)
    {
        if (!_recentDownloadFailures.TryGetValue(cacheKey, out var failedAt))
        {
            return false;
        }

        if (DateTimeOffset.UtcNow - failedAt < DownloadFailureBackoff)
        {
            return true;
        }

        _recentDownloadFailures.TryRemove(cacheKey, out _);
        return false;
    }

    private void RememberDownloadFailure(string cacheKey)
    {
        _recentDownloadFailures[cacheKey] = DateTimeOffset.UtcNow;
    }

    private void ForgetDownloadFailure(string cacheKey)
    {
        _recentDownloadFailures.TryRemove(cacheKey, out _);
    }

    private static async Task<DownloadResult?> WaitForDownloadOrCancellationAsync(
        Task<DownloadResult> downloadTask,
        CancellationToken token)
    {
        if (downloadTask.IsCompleted || !token.CanBeCanceled)
        {
            return await downloadTask.ConfigureAwait(false);
        }

        if (token.IsCancellationRequested)
        {
            return null;
        }

        var cancellationTask = Task.Delay(Timeout.InfiniteTimeSpan, token);
        var completedTask = await Task.WhenAny(downloadTask, cancellationTask).ConfigureAwait(false);
        if (completedTask != downloadTask)
        {
            return null;
        }

        return await downloadTask.ConfigureAwait(false);
    }

    private Lazy<SharedDownload> CreateSharedDownloadLazy(string cacheKey, Uri uri)
    {
        Lazy<SharedDownload>? lazyDownload = null;
        lazyDownload = new Lazy<SharedDownload>(
            () => StartSharedDownload(cacheKey, uri, lazyDownload),
            LazyThreadSafetyMode.ExecutionAndPublication);
        return lazyDownload;
    }

    private SharedDownload StartSharedDownload(string cacheKey, Uri uri, Lazy<SharedDownload>? lazyDownload)
    {
        var sharedDownload = new SharedDownload(token => DownloadAsync(uri, token));
        _ = sharedDownload.Task.ContinueWith(
            static (_, state) =>
            {
                var (manager, key, lazy, shared) =
                    ((ImageExCacheManager Manager, string Key, Lazy<SharedDownload>? Lazy, SharedDownload Shared))state!;
                if (lazy != null)
                {
                    manager.RemoveSharedDownload(key, lazy);
                }

                shared.Dispose();
            },
            (Manager: this, Key: cacheKey, Lazy: lazyDownload, Shared: sharedDownload),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return sharedDownload;
    }

    private void RemoveSharedDownload(string cacheKey, Lazy<SharedDownload> lazyDownload)
    {
        ((ICollection<KeyValuePair<string, Lazy<SharedDownload>>>)_inFlightDownloads)
            .Remove(new KeyValuePair<string, Lazy<SharedDownload>>(cacheKey, lazyDownload));
    }

    private async Task<DownloadResult> DownloadAsync(Uri uri, CancellationToken token)
    {
        const int maxAttempts = 3;
        const int baseDelayMs = 200;

        await _downloadConcurrency.WaitAsync(token).ConfigureAwait(false);
        try
        {
            RecordDownloadStarted(uri);
            for (int attempt = 0; attempt < maxAttempts; attempt++)
            {
                try
                {
                    using var response = await _httpClient.GetAsync(uri, token).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
                    var contentType = response.Content.Headers.ContentType?.MediaType;
                    var bytes = await response.Content.ReadAsByteArrayAsync(token).ConfigureAwait(false);
                    RecordDownloadCompleted(uri, bytes.Length);
                    return new DownloadResult(bytes, contentType);
                }
                catch (HttpRequestException ex) when (IsTransientError(ex) && attempt < maxAttempts - 1)
                {
                    var delayMs = baseDelayMs * (1 << attempt); // Exponential: 200ms, 400ms, 800ms
                    await Task.Delay(delayMs, token).ConfigureAwait(false);
                }
                catch (TaskCanceledException) when (!token.IsCancellationRequested && attempt < maxAttempts - 1)
                {
                    // Timeout (not user-requested cancellation)
                    var delayMs = baseDelayMs * (1 << attempt);
                    await Task.Delay(delayMs, token).ConfigureAwait(false);
                }
                catch
                {
                    // Permanent failure or last attempt - bail out
                    RecordDownloadFailed(uri);
                    return new DownloadResult(null, null);
                }
            }

            RecordDownloadFailed(uri);
            return new DownloadResult(null, null);
        }
        finally
        {
            _downloadConcurrency.Release();
        }
    }

    private bool TryMigrateLegacyCacheEntry(string sourceCacheKey, Uri uri, out CacheEntry? migratedEntry)
    {
        migratedEntry = null;
        var url = uri.OriginalString;
        var legacyEntries = _diskCache.GetAllEntries()
            .Where(entry => entry.Key != sourceCacheKey && string.Equals(entry.Value.Url, url, StringComparison.Ordinal))
            .OrderByDescending(entry => entry.Value.LastAccessUtc)
            .ToList();

        foreach (var legacyEntry in legacyEntries)
        {
            var legacyPath = _diskCache.GetFilePath(legacyEntry.Key, legacyEntry.Value.Extension);
            if (!File.Exists(legacyPath))
            {
                _diskCache.RemoveEntry(legacyEntry.Key);
                continue;
            }

            var sourcePath = _diskCache.GetFilePath(sourceCacheKey, legacyEntry.Value.Extension);
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(sourcePath)!);
                if (!File.Exists(sourcePath))
                {
                    File.Copy(legacyPath, sourcePath, overwrite: false);
                }

                var sourceFileInfo = new FileInfo(sourcePath);
                if (sourceFileInfo.Length == 0)
                {
                    RemoveCacheEntryIfDeleted(sourceCacheKey, sourcePath);
                    RemoveCacheEntryIfDeleted(legacyEntry.Key, legacyPath);
                    continue;
                }

                migratedEntry = new CacheEntry
                {
                    Url = legacyEntry.Value.Url,
                    Extension = legacyEntry.Value.Extension,
                    DownloadedUtc = legacyEntry.Value.DownloadedUtc,
                    LastAccessUtc = DateTimeOffset.UtcNow,
                    SizeBytes = sourceFileInfo.Length
                };

                _diskCache.AddOrUpdateEntry(sourceCacheKey, migratedEntry);
                RemoveLegacyEntriesForUrl(sourceCacheKey, url);
                return true;
            }
            catch (IOException)
            {
                // A racing decode/cleanup can touch old cache files. Skip and let download fill source cache.
            }
            catch (UnauthorizedAccessException)
            {
                // Treat inaccessible legacy files as unusable for migration.
            }
        }

        return false;
    }

    private void RemoveLegacyEntriesForUrl(string sourceCacheKey, string url)
    {
        foreach (var legacyEntry in _diskCache.GetAllEntries())
        {
            if (legacyEntry.Key == sourceCacheKey ||
                !string.Equals(legacyEntry.Value.Url, url, StringComparison.Ordinal))
            {
                continue;
            }

            var legacyPath = _diskCache.GetFilePath(legacyEntry.Key, legacyEntry.Value.Extension);
            RemoveCacheEntryIfDeleted(legacyEntry.Key, legacyPath);
        }
    }

    private void RemoveCacheEntryIfDeleted(string cacheKey, string filePath)
    {
        if (_diskCache.TryDeleteFile(filePath))
        {
            _diskCache.RemoveEntry(cacheKey);
        }
    }

    private async Task SaveDownloadedFileAsync(
        string cacheKey,
        Uri uri,
        string filePath,
        string extension,
        byte[] bytes,
        CancellationToken token,
        bool returnNullOnCancellation)
    {
        var waitStopwatch = Stopwatch.StartNew();
        var writeLock = RentCacheWriteLock(cacheKey);
        var entered = false;

        try
        {
            await writeLock.Semaphore.WaitAsync(returnNullOnCancellation ? CancellationToken.None : token).ConfigureAwait(false);
            entered = true;
            waitStopwatch.Stop();
            RecordCacheWriteStarted(bytes.Length, waitStopwatch.Elapsed);

            if (_diskCache.TryGetEntry(cacheKey, out var existingEntry) && existingEntry != null)
            {
                var existingPath = _diskCache.GetFilePath(cacheKey, existingEntry.Extension);
                if (File.Exists(existingPath))
                {
                    RecordCacheWriteCompleted(bytes.Length, skipped: true);
                    return;
                }
            }

            Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);
            await File.WriteAllBytesAsync(
                filePath,
                bytes,
                returnNullOnCancellation ? CancellationToken.None : token).ConfigureAwait(false);

            _diskCache.AddOrUpdateEntry(cacheKey, new CacheEntry
            {
                Url = uri.OriginalString,
                Extension = extension,
                DownloadedUtc = DateTimeOffset.UtcNow,
                LastAccessUtc = DateTimeOffset.UtcNow,
                SizeBytes = bytes.Length
            });

            RecordCacheWriteCompleted(bytes.Length, skipped: false);
        }
        finally
        {
            if (entered)
            {
                writeLock.Semaphore.Release();
                RecordCacheWriteEnded();
            }

            ReleaseCacheWriteLock(cacheKey, writeLock);
        }
    }

    private CacheWriteLock RentCacheWriteLock(string cacheKey)
    {
        lock (_cacheWriteLocksGate)
        {
            if (!_cacheWriteLocks.TryGetValue(cacheKey, out var writeLock))
            {
                writeLock = new CacheWriteLock();
                _cacheWriteLocks[cacheKey] = writeLock;
            }

            writeLock.ReferenceCount++;
            return writeLock;
        }
    }

    private void ReleaseCacheWriteLock(string cacheKey, CacheWriteLock writeLock)
    {
        var shouldDispose = false;
        lock (_cacheWriteLocksGate)
        {
            writeLock.ReferenceCount--;
            if (writeLock.ReferenceCount == 0
                && _cacheWriteLocks.TryGetValue(cacheKey, out var current)
                && ReferenceEquals(current, writeLock))
            {
                _cacheWriteLocks.Remove(cacheKey);
                shouldDispose = true;
            }
        }

        if (shouldDispose)
        {
            writeLock.Dispose();
            RecordCacheWriteLockRemoved();
        }
    }

    private static bool IsTransientError(HttpRequestException ex)
    {
        var statusCode = ex.StatusCode;
        if (statusCode == null) return true; // Network errors without status code

        // Retry transient errors
        return statusCode switch
        {
            System.Net.HttpStatusCode.RequestTimeout or       // 408
            System.Net.HttpStatusCode.TooManyRequests or      // 429
            System.Net.HttpStatusCode.InternalServerError or  // 500
            System.Net.HttpStatusCode.BadGateway or           // 502
            System.Net.HttpStatusCode.ServiceUnavailable or   // 503
            System.Net.HttpStatusCode.GatewayTimeout          // 504
                => true,
            _ => false
        };
    }

    private async Task<ImageSource?> LoadFromFileAsync(
        string cacheKey,
        string filePath,
        bool isSvg,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        DispatcherQueue? dispatcherQueue = null,
        double dpiScale = 1.0,
        CancellationToken token = default,
        bool returnNullOnCancellation = false)
    {
        if (token.IsCancellationRequested)
        {
            if (returnNullOnCancellation)
            {
                return null;
            }

            token.ThrowIfCancellationRequested();
        }

        try
        {
            var fileInfo = new FileInfo(filePath);
            if (!fileInfo.Exists || fileInfo.Length == 0)
            {
                return null;
            }

            try
            {
                await _decodeConcurrency.WaitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (returnNullOnCancellation)
            {
                RecordDecodeCanceled("file-wait");
                return null;
            }

            try
            {
                RecordDecodeStarted("file", fileInfo.Length, decodeWidth, decodeHeight, decodeType, isSvg);
                if (token.IsCancellationRequested)
                {
                    if (returnNullOnCancellation)
                    {
                        return null;
                    }

                    token.ThrowIfCancellationRequested();
                }

                using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, bufferSize: 4096, FileOptions.SequentialScan);
                var image = await LoadFromStreamAsync(fileStream, isSvg, decodeWidth, decodeHeight, decodeType, dispatcherQueue, dpiScale, token, returnNullOnCancellation).ConfigureAwait(false);
                RecordDecodeCompleted("file", image != null);
                return image;
            }
            finally
            {
                _decodeConcurrency.Release();
                RecordDecodeEnded();
            }
        }
        catch (OperationCanceledException) when (!returnNullOnCancellation)
        {
            RecordDecodeCanceled("file");
            throw;
        }
        catch (OperationCanceledException)
        {
            RecordDecodeCanceled("file");
            return null;
        }
        catch (Exception ex)
        {
            RecordDecodeFailed("file", ex);
            Debug.WriteLine($"[ImageExCache] Failed to decode cached file {filePath}: {ex.Message}");
            return null;
        }
    }

    private async Task<ImageSource?> LoadFromBytesAsync(
        byte[] bytes,
        bool isSvg,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        DispatcherQueue? dispatcherQueue = null,
        double dpiScale = 1.0,
        CancellationToken token = default,
        bool returnNullOnCancellation = false)
    {
        if (token.IsCancellationRequested)
        {
            if (returnNullOnCancellation)
            {
                return null;
            }

            token.ThrowIfCancellationRequested();
        }

        try
        {
            await _decodeConcurrency.WaitAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (returnNullOnCancellation)
        {
            RecordDecodeCanceled("bytes-wait");
            return null;
        }

        try
        {
            RecordDecodeStarted("bytes", bytes.Length, decodeWidth, decodeHeight, decodeType, isSvg);
            using var memoryStream = new MemoryStream(bytes);
            var image = await LoadFromStreamAsync(memoryStream, isSvg, decodeWidth, decodeHeight, decodeType, dispatcherQueue, dpiScale, token, returnNullOnCancellation).ConfigureAwait(false);
            RecordDecodeCompleted("bytes", image != null);
            return image;
        }
        finally
        {
            _decodeConcurrency.Release();
            RecordDecodeEnded();
        }
    }

    private static async Task<ImageSource?> LoadFromStreamAsync(
        Stream stream,
        bool isSvg,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        DispatcherQueue? dispatcherQueue,
        double dpiScale,
        CancellationToken token,
        bool returnNullOnCancellation)
    {
        if (token.IsCancellationRequested)
        {
            if (returnNullOnCancellation)
            {
                return null;
            }

            token.ThrowIfCancellationRequested();
        }

        if (isSvg)
        {
            using var svgStream = stream.AsRandomAccessStream();
            return await LoadSvgImageOnDispatcherAsync(svgStream, dispatcherQueue, token, returnNullOnCancellation).ConfigureAwait(false);
        }

        if (token.IsCancellationRequested)
        {
            if (returnNullOnCancellation)
            {
                return null;
            }

            token.ThrowIfCancellationRequested();
        }

        using var bitmapStream = stream.AsRandomAccessStream();
        var dimensions = await ResolveDecodeDimensionsAsync(bitmapStream, decodeWidth, decodeHeight, dpiScale, token, returnNullOnCancellation);
        if (dimensions == null)
        {
            return null;
        }

        if (ShouldPrescale(dimensions.Value))
        {
            bitmapStream.Seek(0);

            var prescaledBitmap = await CreatePrescaledBitmapAsync(
                bitmapStream,
                dimensions.Value,
                decodeType,
                dispatcherQueue,
                dpiScale,
                token,
                returnNullOnCancellation);
            if (prescaledBitmap == null)
            {
                return null;
            }

            return prescaledBitmap;
        }

        bitmapStream.Seek(0);
        return await LoadBitmapImageOnDispatcherAsync(
            bitmapStream,
            dimensions.Value.TargetWidth,
            dimensions.Value.TargetHeight,
            decodeType,
            dispatcherQueue,
            dpiScale,
            token,
            returnNullOnCancellation).ConfigureAwait(false);
    }

    private static bool ShouldPrescale(DecodeDimensions dimensions)
    {
        return dimensions.TargetWidth > 0 &&
            dimensions.TargetHeight > 0 &&
            dimensions.NaturalWidth > 0 &&
            dimensions.NaturalHeight > 0 &&
            dimensions.TargetWidth < dimensions.NaturalWidth &&
            dimensions.TargetHeight < dimensions.NaturalHeight;
    }

    private static async Task<DecodeDimensions?> ResolveDecodeDimensionsAsync(
        IRandomAccessStream stream,
        int decodeWidth,
        int decodeHeight,
        double dpiScale,
        CancellationToken token,
        bool returnNullOnCancellation)
    {
        if (token.IsCancellationRequested)
        {
            if (returnNullOnCancellation)
            {
                return null;
            }

            token.ThrowIfCancellationRequested();
        }

        try
        {
            var decoder = await BitmapDecoder.CreateAsync(stream);
            var naturalWidth = (int)decoder.OrientedPixelWidth;
            var naturalHeight = (int)decoder.OrientedPixelHeight;
            if (naturalWidth <= 0 || naturalHeight <= 0)
            {
                Debug.WriteLine("[ImageExCache] Failed to read natural image size for prescale.");
                return decodeWidth > 0 || decodeHeight > 0
                    ? null
                    : new DecodeDimensions(0, 0, 0, 0);
            }

            if (decodeWidth <= 0 && decodeHeight <= 0)
            {
                var fallbackWidth = ResolveFallbackDecodeWidth(dpiScale);
                var fallbackHeight = Math.Max(1, (int)Math.Round(fallbackWidth * (naturalHeight / (double)naturalWidth)));
                return new DecodeDimensions(fallbackWidth, fallbackHeight, naturalWidth, naturalHeight);
            }

            if (decodeWidth > 0 && decodeHeight > 0)
            {
                return new DecodeDimensions(decodeWidth, decodeHeight, naturalWidth, naturalHeight);
            }

            if (decodeWidth > 0)
            {
                var scaledHeight = Math.Max(1, (int)Math.Round(decodeWidth * (naturalHeight / (double)naturalWidth)));
                return new DecodeDimensions(decodeWidth, scaledHeight, naturalWidth, naturalHeight);
            }

            var scaledWidth = Math.Max(1, (int)Math.Round(decodeHeight * (naturalWidth / (double)naturalHeight)));
            return new DecodeDimensions(scaledWidth, decodeHeight, naturalWidth, naturalHeight);
        }
        catch (OperationCanceledException) when (returnNullOnCancellation)
        {
            return null;
        }
        catch (OperationCanceledException)
        {
            token.ThrowIfCancellationRequested();
            throw;
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[ImageExCache] Failed to resolve prescale dimensions: {ex.Message}");
            return decodeWidth > 0 || decodeHeight > 0
                ? null
                : new DecodeDimensions(0, 0, 0, 0);
        }
    }

    private static async Task<ImageSource?> CreatePrescaledBitmapAsync(
        IRandomAccessStream sourceStream,
        DecodeDimensions dimensions,
        DecodePixelType decodeType,
        DispatcherQueue? dispatcherQueue,
        double dpiScale,
        CancellationToken token,
        bool returnNullOnCancellation)
    {
        if (token.IsCancellationRequested)
        {
            if (returnNullOnCancellation)
            {
                return null;
            }

            token.ThrowIfCancellationRequested();
        }

        var stage = "start";
        try
        {
            stage = "decoder-create";
            var decoder = await BitmapDecoder.CreateAsync(sourceStream).AsTask(token);
            var sourceWidth = (int)decoder.PixelWidth;
            var sourceHeight = (int)decoder.PixelHeight;
            var orientationSwapsAxes = sourceWidth > 0 &&
                sourceHeight > 0 &&
                (sourceWidth != dimensions.NaturalWidth || sourceHeight != dimensions.NaturalHeight);

            var transformWidth = dimensions.TargetWidth;
            var transformHeight = dimensions.TargetHeight;
            if (orientationSwapsAxes)
            {
                (transformWidth, transformHeight) = (transformHeight, transformWidth);
            }

            var transform = new BitmapTransform
            {
                ScaledWidth = (uint)transformWidth,
                ScaledHeight = (uint)transformHeight,
                InterpolationMode = BitmapInterpolationMode.Fant
            };

            stage = "get-pixel-data";
            var pixelData = await decoder.GetPixelDataAsync(
                BitmapPixelFormat.Bgra8,
                BitmapAlphaMode.Premultiplied,
                transform,
                ExifOrientationMode.RespectExifOrientation,
                ColorManagementMode.DoNotColorManage).AsTask(token);

            var pixels = pixelData.DetachPixelData();
            var expectedBytes = checked(dimensions.TargetWidth * dimensions.TargetHeight * 4);
            if (pixels.Length != expectedBytes)
            {
                Debug.WriteLine(
                    "[ImageExCache] Prescale pixel size mismatch: " +
                    $"expected={expectedBytes} actual={pixels.Length} " +
                    $"target={dimensions.TargetWidth}x{dimensions.TargetHeight} " +
                    $"transform={transformWidth}x{transformHeight}");
                return null;
            }

            if (token.IsCancellationRequested)
            {
                if (returnNullOnCancellation)
                {
                    return null;
                }

                token.ThrowIfCancellationRequested();
            }

            stage = "writeable-bitmap";
            return await CreateWriteableBitmapOnDispatcherAsync(
                dispatcherQueue,
                dimensions,
                pixels,
                token,
                returnNullOnCancellation).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (returnNullOnCancellation)
        {
            return null;
        }
        catch (OperationCanceledException)
        {
            token.ThrowIfCancellationRequested();
            throw;
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[ImageExCache] Failed to prescale image stream at {stage}: {ex.GetType().Name} HResult=0x{ex.HResult:X8}: {ex.Message}");
            return null;
        }
    }

    private static Task<ImageSource?> LoadSvgImageOnDispatcherAsync(
        IRandomAccessStream svgStream,
        DispatcherQueue? dispatcherQueue,
        CancellationToken token,
        bool returnNullOnCancellation)
    {
        return RunOnDispatcherAsync<ImageSource?>(dispatcherQueue, async () =>
        {
            if (token.IsCancellationRequested)
            {
                if (returnNullOnCancellation)
                {
                    return null;
                }

                token.ThrowIfCancellationRequested();
            }

            var svg = new SvgImageSource();
            await svg.SetSourceAsync(svgStream);
            return svg;
        });
    }

    private static Task<ImageSource?> LoadBitmapImageOnDispatcherAsync(
        IRandomAccessStream bitmapStream,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        DispatcherQueue? dispatcherQueue,
        double dpiScale,
        CancellationToken token,
        bool returnNullOnCancellation)
    {
        return RunOnDispatcherAsync<ImageSource?>(dispatcherQueue, async () =>
        {
            if (token.IsCancellationRequested)
            {
                if (returnNullOnCancellation)
                {
                    return null;
                }

                token.ThrowIfCancellationRequested();
            }

            var bitmap = CreateBitmapImage(decodeWidth, decodeHeight, decodeType, dpiScale, BitmapCreateOptions.IgnoreImageCache);
            await bitmap.SetSourceAsync(bitmapStream);
            return bitmap;
        });
    }

    private static Task<ImageSource?> CreateWriteableBitmapOnDispatcherAsync(
        DispatcherQueue? dispatcherQueue,
        DecodeDimensions dimensions,
        byte[] pixels,
        CancellationToken token,
        bool returnNullOnCancellation)
    {
        return RunOnDispatcherAsync<ImageSource?>(dispatcherQueue, () =>
        {
            if (token.IsCancellationRequested)
            {
                if (returnNullOnCancellation)
                {
                    return null;
                }

                token.ThrowIfCancellationRequested();
            }

            var bitmap = new WriteableBitmap(dimensions.TargetWidth, dimensions.TargetHeight);
            using (var pixelBufferStream = bitmap.PixelBuffer.AsStream())
            {
                pixelBufferStream.Write(pixels, 0, pixels.Length);
            }

            bitmap.Invalidate();
            return bitmap;
        });
    }

    private static BitmapImage CreateBitmapImage(
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        double dpiScale,
        BitmapCreateOptions createOptions)
    {
        var targetWidth = decodeWidth;
        var targetHeight = decodeHeight;

        // VISUAL QUALITY & MEMORY TARGET:
        // When no explicit decode size is provided, use a DPI-aware fallback to balance quality and memory.
        // Base size of 400px @ 1x DPI provides good quality for typical UI scenarios (feed images, thumbnails).
        // Scales linearly with DPI: 1.0x=400px, 1.5x=600px, 2.0x=800px, 3.0x=1200px.
        // This avoids both full-resolution memory bloat and visible pixelation on high-DPI displays.
        // For precise control over decode size, callers should set DecodePixelWidth/Height on the ImageEx control.
        if (targetWidth <= 0 && targetHeight <= 0)
        {
            targetWidth = ResolveFallbackDecodeWidth(dpiScale);
        }

        var bitmap = new BitmapImage
        {
            DecodePixelType = decodeType,
            CreateOptions = createOptions
        };

        if (targetWidth > 0) bitmap.DecodePixelWidth = targetWidth;
        if (targetHeight > 0) bitmap.DecodePixelHeight = targetHeight;

        return bitmap;
    }

    private static int ResolveFallbackDecodeWidth(double dpiScale)
    {
        // Clamp DPI scale to reasonable bounds (0.5x - 4.0x) to prevent extreme decode sizes.
        var clampedDpiScale = Math.Max(0.5, Math.Min(4.0, dpiScale));
        return (int)Math.Round(400 * clampedDpiScale);
    }

    private static Task<T?> RunOnDispatcherAsync<T>(DispatcherQueue? dispatcherQueue, Func<T?> factory)
    {
        if (dispatcherQueue == null || dispatcherQueue.HasThreadAccess)
        {
            return SafeFactoryCall(factory);
        }

        var tcs = new TaskCompletionSource<T?>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!dispatcherQueue.TryEnqueue(() =>
            {
                try
                {
                    tcs.TrySetResult(factory());
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[ImageExCache] Failed to create image on UI thread: {ex.Message}");
                    tcs.TrySetResult(default);
                }
            }))
        {
            tcs.TrySetResult(default);
        }

        return tcs.Task;
    }

    private static Task<T?> RunOnDispatcherAsync<T>(DispatcherQueue? dispatcherQueue, Func<Task<T?>> factory)
    {
        if (dispatcherQueue == null || dispatcherQueue.HasThreadAccess)
        {
            return SafeFactoryCall(factory);
        }

        var tcs = new TaskCompletionSource<T?>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!dispatcherQueue.TryEnqueue(async () =>
            {
                try
                {
                    var result = await factory().ConfigureAwait(false);
                    tcs.TrySetResult(result);
                }
                catch (Exception ex)
                {
                    Debug.WriteLine($"[ImageExCache] Failed to create image on UI thread: {ex.Message}");
                    tcs.TrySetResult(default);
                }
            }))
        {
            tcs.TrySetResult(default);
        }

        return tcs.Task;
    }

    private static Task<T?> SafeFactoryCall<T>(Func<T?> factory)
    {
        try
        {
            return Task.FromResult(factory());
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[ImageExCache] Failed to decode image: {ex.Message}");
            return Task.FromResult<T?>(default);
        }
    }

    private static async Task<T?> SafeFactoryCall<T>(Func<Task<T?>> factory)
    {
        try
        {
            return await factory().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[ImageExCache] Failed to decode image: {ex.Message}");
            return default;
        }
    }

    private static void RecordImageRequest(Uri uri, int decodeWidth, int decodeHeight, DecodePixelType decodeType)
    {
        var requests = Interlocked.Increment(ref _telemetryRequestCount);
        if (ShouldSample(requests))
        {
            LogTelemetry("request", uri, decodeWidth, decodeHeight, decodeType, payloadBytes: 0);
        }
    }

    private static void RecordCacheHit(Uri uri, long payloadBytes, int decodeWidth, int decodeHeight, DecodePixelType decodeType)
    {
        var hits = Interlocked.Increment(ref _telemetryCacheHitCount);
        if (ShouldSample(hits))
        {
            LogTelemetry("cache-hit", uri, decodeWidth, decodeHeight, decodeType, payloadBytes);
        }
    }

    private static void RecordCacheMiss(Uri uri, int decodeWidth, int decodeHeight, DecodePixelType decodeType)
    {
        var misses = Interlocked.Increment(ref _telemetryCacheMissCount);
        if (ShouldSample(misses))
        {
            LogTelemetry("cache-miss", uri, decodeWidth, decodeHeight, decodeType, payloadBytes: 0);
        }
    }

    private static void RecordDownloadStarted(Uri uri)
    {
        var starts = Interlocked.Increment(ref _telemetryDownloadStartCount);
        if (ShouldSample(starts))
        {
            LogTelemetry("download-start", uri, decodeWidth: 0, decodeHeight: 0, DecodePixelType.Physical, payloadBytes: 0);
        }
    }

    private static void RecordDownloadCompleted(Uri uri, int byteCount)
    {
        Interlocked.Add(ref _telemetryDownloadByteCount, byteCount);
        var successes = Interlocked.Increment(ref _telemetryDownloadSuccessCount);
        if (ShouldSample(successes))
        {
            LogTelemetry("download-success", uri, decodeWidth: 0, decodeHeight: 0, DecodePixelType.Physical, byteCount);
        }
    }

    private static void RecordDownloadFailed(Uri uri)
    {
        var failures = Interlocked.Increment(ref _telemetryDownloadFailureCount);
        if (ShouldSampleFirstOrEvery(failures))
        {
            LogTelemetry("download-fail", uri, decodeWidth: 0, decodeHeight: 0, DecodePixelType.Physical, payloadBytes: 0);
        }
    }

    private static void RecordCacheWriteStarted(long payloadBytes, TimeSpan waitTime)
    {
        Interlocked.Add(ref _telemetryCacheWriteWaitMsTotal, (long)waitTime.TotalMilliseconds);
        var starts = Interlocked.Increment(ref _telemetryCacheWriteStartCount);
        var active = Interlocked.Increment(ref _telemetryActiveCacheWriteCount);
        var isNewPeak = TryUpdatePeakActiveCacheWrites(active);

        if (isNewPeak || ShouldSample(starts) || waitTime.TotalMilliseconds >= 100)
        {
            LogTelemetry(
                $"cache-write-start:waitMs={waitTime.TotalMilliseconds:F1}",
                uri: null,
                decodeWidth: 0,
                decodeHeight: 0,
                DecodePixelType.Physical,
                payloadBytes);
        }
    }

    private static void RecordCacheWriteCompleted(long payloadBytes, bool skipped)
    {
        var completes = Interlocked.Increment(ref _telemetryCacheWriteCompleteCount);
        if (skipped || ShouldSample(completes))
        {
            LogTelemetry(
                $"cache-write-complete:skipped={skipped}",
                uri: null,
                decodeWidth: 0,
                decodeHeight: 0,
                DecodePixelType.Physical,
                payloadBytes);
        }
    }

    private static void RecordCacheWriteEnded()
    {
        Interlocked.Decrement(ref _telemetryActiveCacheWriteCount);
    }

    private static void RecordCacheWriteLockRemoved()
    {
        var removals = Interlocked.Increment(ref _telemetryCacheWriteLockRemovalCount);
        if (ShouldSample(removals))
        {
            LogTelemetry(
                "cache-write-lock-removed",
                uri: null,
                decodeWidth: 0,
                decodeHeight: 0,
                DecodePixelType.Physical,
                payloadBytes: 0);
        }
    }

    private static void RecordDecodeStarted(
        string source,
        long payloadBytes,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        bool isSvg)
    {
        if (source == "file")
        {
            Interlocked.Increment(ref _telemetryFileDecodeStartCount);
        }
        else
        {
            Interlocked.Increment(ref _telemetryByteDecodeStartCount);
        }

        var active = Interlocked.Increment(ref _telemetryActiveDecodeCount);
        var isNewPeak = TryUpdatePeakActiveDecodes(active);
        var decodeStarts = Interlocked.Read(ref _telemetryFileDecodeStartCount)
            + Interlocked.Read(ref _telemetryByteDecodeStartCount);

        if (isNewPeak || ShouldSample(decodeStarts))
        {
            LogTelemetry(
                $"decode-start:{source}:svg={isSvg}",
                uri: null,
                decodeWidth,
                decodeHeight,
                decodeType,
                payloadBytes);
        }
    }

    private static bool TryUpdatePeakActiveCacheWrites(long activeWrites)
    {
        while (true)
        {
            var currentPeak = Interlocked.Read(ref _telemetryPeakActiveCacheWriteCount);
            if (activeWrites <= currentPeak)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _telemetryPeakActiveCacheWriteCount, activeWrites, currentPeak) == currentPeak)
            {
                return true;
            }
        }
    }

    private static void RecordDecodeCompleted(string source, bool success)
    {
        var count = success
            ? Interlocked.Increment(ref _telemetryDecodeSuccessCount)
            : Interlocked.Increment(ref _telemetryDecodeNullCount);

        if (!success || ShouldSample(count))
        {
            LogTelemetry(
                $"decode-complete:{source}:success={success}",
                uri: null,
                decodeWidth: 0,
                decodeHeight: 0,
                DecodePixelType.Physical,
                payloadBytes: 0);
        }
    }

    private static void RecordDecodeCanceled(string source)
    {
        var cancels = Interlocked.Increment(ref _telemetryDecodeCancelCount);
        if (ShouldSampleFirstOrEvery(cancels))
        {
            LogTelemetry(
                $"decode-cancel:{source}",
                uri: null,
                decodeWidth: 0,
                decodeHeight: 0,
                DecodePixelType.Physical,
                payloadBytes: 0);
        }
    }

    private static void RecordDecodeFailed(string source, Exception ex)
    {
        Interlocked.Increment(ref _telemetryDecodeFailureCount);
        Debug.WriteLine($"[ImageExTelemetry] Reason=decode-fail:{source} Error={ex.GetType().Name}:{ex.Message}");
        LogTelemetry(
            $"decode-fail:{source}",
            uri: null,
            decodeWidth: 0,
            decodeHeight: 0,
            DecodePixelType.Physical,
            payloadBytes: 0);
    }

    private static void RecordDecodeEnded()
    {
        Interlocked.Decrement(ref _telemetryActiveDecodeCount);
    }

    private static bool TryUpdatePeakActiveDecodes(long activeDecodes)
    {
        while (true)
        {
            var currentPeak = Interlocked.Read(ref _telemetryPeakActiveDecodeCount);
            if (activeDecodes <= currentPeak)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref _telemetryPeakActiveDecodeCount, activeDecodes, currentPeak) == currentPeak)
            {
                return true;
            }
        }
    }

    private static bool ShouldSample(long count)
    {
        return count > 0 && count % TelemetrySampleInterval == 0;
    }

    private static bool ShouldSampleFirstOrEvery(long count)
    {
        return count == 1 || ShouldSample(count);
    }

    private static void LogTelemetry(
        string reason,
        Uri? uri,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        long payloadBytes)
    {
        var memory = CaptureTelemetryMemory();
        var host = uri?.Host ?? string.Empty;
        Debug.WriteLine(
            "[ImageExTelemetry] " +
            $"Reason={reason} " +
            $"Host={host} " +
            $"DecodeWidth={decodeWidth} " +
            $"DecodeHeight={decodeHeight} " +
            $"DecodeType={decodeType} " +
            $"PayloadKB={payloadBytes / 1024.0:F1} " +
            $"Requests={Interlocked.Read(ref _telemetryRequestCount)} " +
            $"CacheHits={Interlocked.Read(ref _telemetryCacheHitCount)} " +
            $"CacheMisses={Interlocked.Read(ref _telemetryCacheMissCount)} " +
            $"DownloadStarts={Interlocked.Read(ref _telemetryDownloadStartCount)} " +
            $"DownloadSuccesses={Interlocked.Read(ref _telemetryDownloadSuccessCount)} " +
            $"DownloadFailures={Interlocked.Read(ref _telemetryDownloadFailureCount)} " +
            $"DownloadMB={ToMegabytes(Interlocked.Read(ref _telemetryDownloadByteCount)):F1} " +
            $"CacheWriteStarts={Interlocked.Read(ref _telemetryCacheWriteStartCount)} " +
            $"CacheWriteCompletes={Interlocked.Read(ref _telemetryCacheWriteCompleteCount)} " +
            $"CacheWriteWaitMsTotal={Interlocked.Read(ref _telemetryCacheWriteWaitMsTotal)} " +
            $"ActiveCacheWrites={Interlocked.Read(ref _telemetryActiveCacheWriteCount)} " +
            $"PeakActiveCacheWrites={Interlocked.Read(ref _telemetryPeakActiveCacheWriteCount)} " +
            $"CacheWriteLockRemovals={Interlocked.Read(ref _telemetryCacheWriteLockRemovalCount)} " +
            $"FileDecodeStarts={Interlocked.Read(ref _telemetryFileDecodeStartCount)} " +
            $"ByteDecodeStarts={Interlocked.Read(ref _telemetryByteDecodeStartCount)} " +
            $"DecodeSuccesses={Interlocked.Read(ref _telemetryDecodeSuccessCount)} " +
            $"DecodeNulls={Interlocked.Read(ref _telemetryDecodeNullCount)} " +
            $"DecodeCancels={Interlocked.Read(ref _telemetryDecodeCancelCount)} " +
            $"DecodeFailures={Interlocked.Read(ref _telemetryDecodeFailureCount)} " +
            $"ActiveDecodes={Interlocked.Read(ref _telemetryActiveDecodeCount)} " +
            $"PeakActiveDecodes={Interlocked.Read(ref _telemetryPeakActiveDecodeCount)} " +
            $"ManagedMB={memory.ManagedMB:F1} " +
            $"WorkingSetMB={memory.WorkingSetMB:F1} " +
            $"PrivateMB={memory.PrivateMB:F1}");
    }

    private static (double ManagedMB, double WorkingSetMB, double PrivateMB) CaptureTelemetryMemory()
    {
        var managedBytes = GC.GetTotalMemory(false);
        var workingSetBytes = 0L;
        var privateBytes = 0L;

        try
        {
            using var process = Process.GetCurrentProcess();
            workingSetBytes = process.WorkingSet64;
            privateBytes = process.PrivateMemorySize64;
        }
        catch
        {
            // Best-effort diagnostic only.
        }

        return (ToMegabytes(managedBytes), ToMegabytes(workingSetBytes), ToMegabytes(privateBytes));
    }

    private static double ToMegabytes(long bytes)
    {
        return bytes / (1024.0 * 1024.0);
    }

    private async Task EnforceCleanupIfNeededAsync()
    {
        var now = DateTimeOffset.UtcNow;
        if (now - _lastCleanup < TimeSpan.FromMinutes(ImageExCacheConstants.CleanupIntervalMinutes))
            return;

        if (!await _cleanupLock.WaitAsync(0).ConfigureAwait(false))
            return; // Another cleanup in progress

        try
        {
            _lastCleanup = now;
            var removed = 0;
            long freedBytes = 0;

            // Initial size scan if needed
            if (!_initialSizeScanned)
            {
                await _diskCache.EnsureMetadataLoadedAsync().ConfigureAwait(false);
                _initialSizeScanned = true;
            }

            // Phase 1: Remove expired entries (TTL)
            foreach (var kvp in _diskCache.GetAllEntries())
            {
                if ((now - kvp.Value.DownloadedUtc).TotalDays > MaxCacheDays)
                {
                    var path = _diskCache.GetFilePath(kvp.Key, kvp.Value.Extension);
                    if (_diskCache.TryDeleteFile(path))
                    {
                        _diskCache.RemoveEntry(kvp.Key);
                        freedBytes += kvp.Value.SizeBytes;
                        removed++;
                    }
                }
            }

            // Phase 2: LRU eviction if over size
            var totalSize = _diskCache.GetTotalSizeBytes();
            if (totalSize > MaxCacheSizeBytes)
            {
                var targetSize = (long)(MaxCacheSizeBytes * ImageExCacheConstants.CleanupTargetRatio);
                var sorted = _diskCache.GetAllEntries()
                    .OrderBy(e => e.Value.LastAccessUtc) // LRU first
                    .ToList();

                foreach (var kvp in sorted)
                {
                    if (totalSize <= targetSize) break;

                    var path = _diskCache.GetFilePath(kvp.Key, kvp.Value.Extension);
                    if (_diskCache.TryDeleteFile(path))
                    {
                        _diskCache.RemoveEntry(kvp.Key);
                        totalSize -= kvp.Value.SizeBytes;
                        freedBytes += kvp.Value.SizeBytes;
                        removed++;
                    }
                }
            }

            if (removed > 0)
            {
                await _diskCache.SaveMetadataAsync().ConfigureAwait(false);
                Debug.WriteLine($"[ImageExCache] Cleanup: {removed} files, {freedBytes / 1024 / 1024}MB freed");
            }
        }
        finally
        {
            _cleanupLock.Release();
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _httpClient.Dispose();
        _cleanupLock.Dispose();
        _downloadConcurrency.Dispose();
        _decodeConcurrency.Dispose();
        CacheWriteLock[] writeLocks;
        lock (_cacheWriteLocksGate)
        {
            writeLocks = _cacheWriteLocks.Values.ToArray();
            _cacheWriteLocks.Clear();
        }

        foreach (var writeLock in writeLocks)
        {
            writeLock.Dispose();
        }

        lock (_smallDecodedImageCacheLock)
        {
            _smallDecodedImageCache.Clear();
            _smallDecodedImageCacheLru.Clear();
            _smallDecodedImageCacheBytes = 0;
        }

        GC.SuppressFinalize(this);
    }
}
