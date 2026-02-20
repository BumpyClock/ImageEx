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
internal sealed class ImageExCacheManager : IDisposable
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static ImageExCacheManager Instance { get; } = new();

    private readonly ImageExDiskCache _diskCache;
    private readonly HttpClient _httpClient;
    private readonly ConcurrentDictionary<string, Task<(byte[]? Bytes, string? ContentType)>> _inFlightDownloads = new();
    private readonly SemaphoreSlim _cleanupLock = new(1, 1);
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
    {
        _diskCache = new ImageExDiskCache(ImageExCacheConstants.GetCacheDirectory());
        _httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
    }

    /// <summary>
    /// Result of a cache lookup, including whether it was a cache hit for shimmer skip.
    /// </summary>
    /// <param name="Image">The loaded ImageSource, or null on failure.</param>
    /// <param name="WasCacheHit">True if served from disk cache (skip shimmer).</param>
    public record CacheResult(ImageSource? Image, bool WasCacheHit);

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
        double dpiScale = 1.0)
    {
        // Skip non-http URIs - return null to let base pipeline handle
        if (!uri.IsHttpUri())
            return new CacheResult(null, false);

        var isSvg = uri.AbsolutePath.EndsWith(".svg", StringComparison.OrdinalIgnoreCase);
        var cacheKey = ImageExDiskCache.ComputeCacheKey(uri, decodeWidth, decodeHeight, decodeType, isSvg);

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
                    token.ThrowIfCancellationRequested();
                    var cachedIsSvg = entry.Extension == ".svg";
                    var image = await LoadFromFileAsync(filePath, cachedIsSvg, decodeWidth, decodeHeight, decodeType, token, dispatcherQueue, dpiScale).ConfigureAwait(false);

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
                catch (OperationCanceledException)
                {
                    throw;
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
        var downloadTask = _inFlightDownloads.GetOrAdd(cacheKey, _ => DownloadAsync(uri, token));

        (byte[]? bytes, string? contentType) result;
        try
        {
            result = await downloadTask.ConfigureAwait(false);
        }
        finally
        {
            _inFlightDownloads.TryRemove(cacheKey, out _);
        }

        if (result.bytes == null || token.IsCancellationRequested)
            return new CacheResult(null, false);

        // Detect SVG from content-type if URL didn't have .svg extension
        var detectedSvg = isSvg || result.contentType == "image/svg+xml";
        var extension = ImageExDiskCache.GetExtension(uri, result.contentType, detectedSvg);
        var newFilePath = _diskCache.GetFilePath(cacheKey, extension);

        // 3. Save to disk
        try
        {
            token.ThrowIfCancellationRequested();
            Directory.CreateDirectory(ImageExCacheConstants.GetCacheDirectory());
            await File.WriteAllBytesAsync(newFilePath, result.bytes, token).ConfigureAwait(false);

            _diskCache.AddOrUpdateEntry(cacheKey, new CacheEntry
            {
                Url = uri.OriginalString,
                Extension = extension,
                DownloadedUtc = DateTimeOffset.UtcNow,
                LastAccessUtc = DateTimeOffset.UtcNow,
                SizeBytes = result.bytes.Length
            });

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
        catch (OperationCanceledException)
        {
            throw;
        }
        catch
        {
            // Disk save failed - still return image from memory
        }

        // 4. Return image from downloaded bytes
        var loadedImage = await LoadFromBytesAsync(result.bytes, detectedSvg, decodeWidth, decodeHeight, decodeType, dispatcherQueue, dpiScale).ConfigureAwait(false);
        return new CacheResult(loadedImage, WasCacheHit: false);
    }

    private async Task<(byte[]? bytes, string? contentType)> DownloadAsync(Uri uri, CancellationToken token)
    {
        const int maxAttempts = 3;
        const int baseDelayMs = 200;

        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            try
            {
                using var response = await _httpClient.GetAsync(uri, token).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
                var contentType = response.Content.Headers.ContentType?.MediaType;
                var bytes = await response.Content.ReadAsByteArrayAsync(token).ConfigureAwait(false);
                return (bytes, contentType);
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
                return (null, null);
            }
        }

        return (null, null);
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

    private static async Task<ImageSource?> LoadFromFileAsync(
        string filePath,
        bool isSvg,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        CancellationToken token = default,
        DispatcherQueue? dispatcherQueue = null,
        double dpiScale = 1.0)
    {
        token.ThrowIfCancellationRequested();
        try
        {
            var fileInfo = new FileInfo(filePath);
            if (!fileInfo.Exists || fileInfo.Length == 0)
            {
                return null;
            }

            var bytes = await File.ReadAllBytesAsync(filePath, token).ConfigureAwait(false);
            if (bytes.Length == 0)
            {
                return null;
            }

            return await LoadFromBytesAsync(bytes, isSvg, decodeWidth, decodeHeight, decodeType, dispatcherQueue, dpiScale).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"[ImageExCache] Failed to decode cached file {filePath}: {ex.Message}");
            return null;
        }
    }

    private static async Task<ImageSource?> LoadFromBytesAsync(
        byte[] bytes,
        bool isSvg,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        DispatcherQueue? dispatcherQueue = null,
        double dpiScale = 1.0)
    {
        return await RunOnDispatcherAsync(dispatcherQueue, async () =>
        {
            if (isSvg)
            {
                var svg = new SvgImageSource();
                using var svgStream = new MemoryStream(bytes);
                await svg.SetSourceAsync(svgStream.AsRandomAccessStream());
                return (ImageSource?)svg;
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

            using var bitmapStream = new MemoryStream(bytes);
            await bitmap.SetSourceAsync(bitmapStream.AsRandomAccessStream());
            return (ImageSource?)bitmap;
        }).ConfigureAwait(false);
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
        GC.SuppressFinalize(this);
    }
}
