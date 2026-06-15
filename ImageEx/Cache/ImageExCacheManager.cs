// ABOUTME: Singleton managing ImageEx disk cache lifecycle, in-flight deduplication,
// ABOUTME: shared HttpClient, and cleanup orchestration with TTL and LRU eviction.

#nullable enable

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net.Http;
using ImageEx;
using Microsoft.UI.Dispatching;

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
    public static ImageExCacheManager Instance { get; } = new();

    private readonly ImageExDiskCache _diskCache;
    private readonly HttpClient _httpClient;
    private readonly ConcurrentDictionary<string, Lazy<SharedDownload>> _inFlightDownloads = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _cacheWriteLocks = new();
    private readonly SemaphoreSlim _cleanupLock = new(1, 1);
    private readonly SemaphoreSlim _downloadConcurrency;
    private readonly SemaphoreSlim _decodeConcurrency;
    private DateTimeOffset _lastCleanup = DateTimeOffset.MinValue;
    private bool _initialSizeScanned;
    private bool _disposed;

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
        int maxConcurrentDownloads = 4,
        int maxConcurrentDecodes = 4)
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

    private readonly record struct DownloadResult(byte[]? Bytes, string? ContentType);

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

        await _diskCache.EnsureMetadataLoadedAsync().ConfigureAwait(false);

        // 1. Try local cache
        if (_diskCache.TryGetEntry(cacheKey, out var entry) && entry != null)
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
                    var image = await LoadFromFileAsync(filePath, cachedIsSvg, decodeWidth, decodeHeight, decodeType, dispatcherQueue, dpiScale, token, returnNullOnCancellation).ConfigureAwait(false);

                    if (image != null)
                    {
                        _diskCache.UpdateAccessTime(cacheKey);
                        // Fire-and-forget to persist LRU update
                        _ = Task.Run(async () =>
                        {
                            try { await _diskCache.SaveMetadataAsync().ConfigureAwait(false); }
                            catch { /* Best effort */ }
                        });

                        return new CacheResult(image, WasCacheHit: true);
                    }
                    else
                    {
                        // Cached file failed to decode; remove and re-download.
                        _diskCache.TryDeleteFile(filePath);
                        _diskCache.RemoveEntry(cacheKey);
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
                    _diskCache.TryDeleteFile(filePath);
                    _diskCache.RemoveEntry(cacheKey);
                }
            }
            else
            {
                // Expired - clean up
                var expiredPath = _diskCache.GetFilePath(cacheKey, entry.Extension);
                _diskCache.TryDeleteFile(expiredPath);
                _diskCache.RemoveEntry(cacheKey);
            }
        }

        // 2. Download with in-flight deduplication
        var result = await AwaitSharedDownloadAsync(cacheKey, uri, token, returnNullOnCancellation).ConfigureAwait(false);

        if (result.Bytes == null || token.IsCancellationRequested)
            return new CacheResult(null, false);

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
        return new CacheResult(loadedImage, WasCacheHit: false);
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
            for (int attempt = 0; attempt < maxAttempts; attempt++)
            {
                try
                {
                    using var response = await _httpClient.GetAsync(uri, token).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();
                    var contentType = response.Content.Headers.ContentType?.MediaType;
                    var bytes = await response.Content.ReadAsByteArrayAsync(token).ConfigureAwait(false);
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
                    return new DownloadResult(null, null);
                }
            }

            return new DownloadResult(null, null);
        }
        finally
        {
            _downloadConcurrency.Release();
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
        var writeLock = _cacheWriteLocks.GetOrAdd(cacheKey, static _ => new SemaphoreSlim(1, 1));
        await writeLock.WaitAsync(returnNullOnCancellation ? CancellationToken.None : token).ConfigureAwait(false);

        try
        {
            if (_diskCache.TryGetEntry(cacheKey, out var existingEntry) && existingEntry != null)
            {
                var existingPath = _diskCache.GetFilePath(cacheKey, existingEntry.Extension);
                if (File.Exists(existingPath))
                {
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
        }
        finally
        {
            writeLock.Release();
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
                return null;
            }

            try
            {
                return await RunOnDispatcherAsync(dispatcherQueue, async () =>
                {
                    if (token.IsCancellationRequested)
                    {
                        if (returnNullOnCancellation)
                        {
                            return default;
                        }

                        token.ThrowIfCancellationRequested();
                    }

                    using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                    if (fileStream.Length == 0)
                    {
                        return default;
                    }

                    return await LoadFromStreamAsync(fileStream, isSvg, decodeWidth, decodeHeight, decodeType, dpiScale, token, returnNullOnCancellation).ConfigureAwait(false);
                }).ConfigureAwait(false);
            }
            finally
            {
                _decodeConcurrency.Release();
            }
        }
        catch (OperationCanceledException) when (!returnNullOnCancellation)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        catch (Exception ex)
        {
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
            return null;
        }

        try
        {
            return await RunOnDispatcherAsync(dispatcherQueue, async () =>
            {
                using var memoryStream = new MemoryStream(bytes);
                return await LoadFromStreamAsync(memoryStream, isSvg, decodeWidth, decodeHeight, decodeType, dpiScale, token, returnNullOnCancellation).ConfigureAwait(false);
            }).ConfigureAwait(false);
        }
        finally
        {
            _decodeConcurrency.Release();
        }
    }

    private static async Task<ImageSource?> LoadFromStreamAsync(
        Stream stream,
        bool isSvg,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
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
            var svg = new SvgImageSource();
            await svg.SetSourceAsync(stream.AsRandomAccessStream());
            return svg;
        }

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
            // Clamp DPI scale to reasonable bounds (0.5x - 4.0x) to prevent extreme decode sizes.
            var clampedDpiScale = Math.Max(0.5, Math.Min(4.0, dpiScale));
            targetWidth = (int)Math.Round(400 * clampedDpiScale);
        }

        var bitmap = new BitmapImage
        {
            DecodePixelType = decodeType,
            CreateOptions = BitmapCreateOptions.IgnoreImageCache
        };

        if (targetWidth > 0) bitmap.DecodePixelWidth = targetWidth;
        if (targetHeight > 0) bitmap.DecodePixelHeight = targetHeight;

        if (token.IsCancellationRequested)
        {
            if (returnNullOnCancellation)
            {
                return null;
            }

            token.ThrowIfCancellationRequested();
        }

        await bitmap.SetSourceAsync(stream.AsRandomAccessStream());
        return bitmap;
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
                    _diskCache.TryDeleteFile(path);
                    _diskCache.RemoveEntry(kvp.Key);
                    freedBytes += kvp.Value.SizeBytes;
                    removed++;
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
                    _diskCache.TryDeleteFile(path);
                    _diskCache.RemoveEntry(kvp.Key);
                    totalSize -= kvp.Value.SizeBytes;
                    freedBytes += kvp.Value.SizeBytes;
                    removed++;
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
        GC.SuppressFinalize(this);
    }
}
