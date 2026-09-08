#nullable enable

using System.Buffers;
using System.Diagnostics;
using System.Net.Http;
using Microsoft.UI.Dispatching;

namespace ImageEx.Cache;

internal sealed partial class ImageExCacheManager
{
    internal const long MaximumOriginalSourceBytes = 32L * 1024 * 1024;
    private readonly CancellationTokenSource _originalShutdown = new();

    internal async Task<CacheResult> GetOrLoadOriginalImageAsync(
        Uri uri, int decodeWidth, int decodeHeight, DecodePixelType decodeType,
        CancellationToken token, DispatcherQueue? dispatcherQueue = null,
        double dpiScale = 1.0, bool returnNullOnCancellation = false)
    {
        BeginOperation();
        var key = "original-" + ImageExDiskCache.ComputeSourceCacheKey(uri);
        CacheWriteLock? sourceLock = null;
        var entered = false;
        string? temporaryPath = null;
        try
        {
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(token, _originalShutdown.Token);
            var operationToken = lifetime.Token;
            operationToken.ThrowIfCancellationRequested();
            if (!uri.IsHttpUri()) return new CacheResult(null, false);
            RecordImageRequest(uri, decodeWidth, decodeHeight, decodeType);

            await _diskCache.EnsureMetadataLoadedAsync().ConfigureAwait(false);
            sourceLock = RentCacheWriteLock(key);
            await sourceLock.Semaphore.WaitAsync(operationToken).ConfigureAwait(false);
            entered = true;
            var uriIsSvg = uri.AbsolutePath.EndsWith(".svg", StringComparison.OrdinalIgnoreCase);
            if (TryGetSmallDecodedImage(uri, decodeWidth, decodeHeight, decodeType, uriIsSvg, dpiScale, out var decoded))
            {
                _diskCache.UpdateAccessTime(key);
                RecordCacheHit(uri, 0, decodeWidth, decodeHeight, decodeType);
                return new CacheResult(decoded, true);
            }
            if (_diskCache.TryGetEntry(key, out var entry) && entry != null)
            {
                var cached = await TryLoadDiskEntryAsync(key, uri, entry, decodeWidth, decodeHeight,
                    decodeType, dispatcherQueue, dpiScale, operationToken, returnNullOnCancellation: false).ConfigureAwait(false);
                if (cached != null)
                {
                    return new CacheResult(cached, true);
                }
            }
            if (IsRecentDownloadFailure(key)) return new CacheResult(null, false);
            RecordCacheMiss(uri, decodeWidth, decodeHeight, decodeType);

            string extension;
            bool isSvg;
            long size;
            await _downloadConcurrency.WaitAsync(operationToken).ConfigureAwait(false);
            try
            {
                // Headers-only completion requires an explicit deadline for the body as well.
                using var deadline = CancellationTokenSource.CreateLinkedTokenSource(operationToken);
                deadline.CancelAfter(TimeSpan.FromSeconds(30));
                operationToken.ThrowIfCancellationRequested();
                RecordDownloadStarted(uri);
                using var response = await _httpClient.GetAsync(uri, HttpCompletionOption.ResponseHeadersRead,
                    deadline.Token).ConfigureAwait(false);
                response.EnsureSuccessStatusCode();
                var contentType = response.Content.Headers.ContentType?.MediaType;
                isSvg = uri.AbsolutePath.EndsWith(".svg", StringComparison.OrdinalIgnoreCase) || contentType == "image/svg+xml";
                var sourceLimit = isSvg ? Math.Min(_maximumSourceBytes, MaximumOriginalSourceBytes) : MaximumOriginalSourceBytes;
                if (response.Content.Headers.ContentLength > sourceLimit)
                    throw new IOException("Original image exceeds source byte limit.");
                extension = ImageExDiskCache.GetExtension(uri, contentType, isSvg);
                var destination = _diskCache.GetFilePath(key, extension);
                Directory.CreateDirectory(Path.GetDirectoryName(destination)!);
                temporaryPath = destination + "." + Guid.NewGuid().ToString("N") + ".tmp";
                await using var source = await response.Content.ReadAsStreamAsync(deadline.Token).ConfigureAwait(false);
                await using var file = new FileStream(temporaryPath, FileMode.CreateNew, FileAccess.Write,
                    FileShare.None, 64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
                size = await CopyOriginalSourceAsync(source, file, sourceLimit, deadline.Token).ConfigureAwait(false);
                if (size == 0) throw new IOException("Original image is empty.");
                RecordDownloadCompleted(uri, checked((int)size));
            }
            catch (OperationCanceledException) when (operationToken.IsCancellationRequested)
            {
                throw;
            }
            catch
            {
                RecordDownloadFailed(uri);
                throw;
            }
            finally
            {
                _downloadConcurrency.Release();
            }

            var image = await LoadFromFileAsync(temporaryPath, isSvg, decodeWidth, decodeHeight, decodeType,
                dispatcherQueue, dpiScale, operationToken).ConfigureAwait(false);
            operationToken.ThrowIfCancellationRequested();
            if (image == null)
            {
                Debug.WriteLine($"[ImageEx] original-fallback failed host={uri.Host} reason=decode-null");
                RememberDownloadFailure(key);
                return new CacheResult(null, false);
            }
            var path = _diskCache.GetFilePath(key, extension);
            File.Move(temporaryPath, path, overwrite: true);
            temporaryPath = null;
            _diskCache.AddOrUpdateEntry(key, new CacheEntry
            {
                Url = uri.OriginalString, Extension = extension, SizeBytes = size,
                DownloadedUtc = DateTimeOffset.UtcNow, LastAccessUtc = DateTimeOffset.UtcNow
            });
            ForgetDownloadFailure(key);
            await StoreSmallDecodedImageAsync(uri, decodeWidth, decodeHeight, decodeType,
                isSvg, dpiScale, image, dispatcherQueue).ConfigureAwait(false);
            await EnforceCleanupIfNeededAsync().ConfigureAwait(false);
            return new CacheResult(image, false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return CancelledResult(token, returnNullOnCancellation);
        }
        catch (Exception error)
        {
            var reason = error is OperationCanceledException ? "timeout-or-shutdown" : "download-cache-or-decode";
            var status = error is HttpRequestException requestError ? requestError.StatusCode?.ToString() : null;
            Debug.WriteLine($"[ImageEx] original-fallback failed host={uri.Host} type={error.GetType().Name} reason={reason} status={status} hresult={error.HResult}");
            if (!_originalShutdown.IsCancellationRequested) RememberDownloadFailure(key);
            return new CacheResult(null, false);
        }
        finally
        {
            if (temporaryPath != null) _diskCache.TryDeleteFile(temporaryPath);
            if (entered) sourceLock!.Semaphore.Release();
            if (sourceLock != null) ReleaseCacheWriteLock(key, sourceLock);
            EndOperation();
        }
    }

    internal static void RemoveAbandonedOriginalFiles(string directory, DateTime cutoffUtc)
    {
        if (!Directory.Exists(directory)) return;
        foreach (var path in Directory.EnumerateFiles(directory, "original-*.tmp"))
        {
            try
            {
                if (File.GetLastWriteTimeUtc(path) >= cutoffUtc) continue;
                // Exclusive access also protects a transfer in another app process.
                using var abandoned = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None,
                    4096, FileOptions.DeleteOnClose);
            }
            catch (IOException) { }
            catch (UnauthorizedAccessException) { }
        }
    }

    internal static async Task<long> CopyOriginalSourceAsync(Stream source, Stream destination, long maximumBytes, CancellationToken token)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumBytes);
        var buffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
        long total = 0;
        try
        {
            while (true)
            {
                var count = (int)Math.Min(buffer.Length, maximumBytes - total + 1);
                var read = await source.ReadAsync(buffer.AsMemory(0, count), token).ConfigureAwait(false);
                if (read == 0) return total;
                total += read;
                if (total > maximumBytes) throw new IOException("Original image exceeds source byte limit.");
                await destination.WriteAsync(buffer.AsMemory(0, read), token).ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
