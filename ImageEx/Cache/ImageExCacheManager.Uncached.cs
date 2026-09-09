#nullable enable

using System.Diagnostics;
using System.Net.Http;
using Microsoft.UI.Dispatching;

namespace ImageEx.Cache;

internal sealed partial class ImageExCacheManager
{
    private static readonly HttpClient UncachedHttpClient = new() { Timeout = TimeSpan.FromSeconds(30) };
    private static readonly SemaphoreSlim UncachedConcurrency = new(4, 4);

    // This path must not create a cache manager or access source files or metadata.
    internal static async Task<CacheResult> GetUncachedImageAsync(
        ImageRequestCandidate candidate, int decodeWidth, int decodeHeight, DecodePixelType decodeType,
        CancellationToken token, DispatcherQueue? dispatcherQueue = null, double dpiScale = 1.0,
        HttpClient? httpClient = null)
    {
        await UncachedConcurrency.WaitAsync(token).ConfigureAwait(false);
        try
        {
            using var response = await (httpClient ?? UncachedHttpClient).GetAsync(
                candidate.Uri, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            var isSvg = candidate.Uri.AbsolutePath.EndsWith(".svg", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(response.Content.Headers.ContentType?.MediaType, "image/svg+xml", StringComparison.OrdinalIgnoreCase);
            var maximumBytes = candidate.Mode == ImageRequestMode.Original && !isSvg
                ? ImageExCacheConstants.MaximumOriginalSourceBytes : ImageExCacheConstants.DefaultMaximumSourceBytes;
            if (response.Content.Headers.ContentLength > maximumBytes)
            {
                throw new IOException("Image exceeds source byte limit.");
            }

            using var source = await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false);
            var declaredLength = response.Content.Headers.ContentLength;
            using var bytes = new MemoryStream(declaredLength is > 0
                ? checked((int)declaredLength.Value)
                : 64 * 1024);
            await CopyOriginalSourceAsync(source, bytes, maximumBytes, token).ConfigureAwait(false);
            bytes.Position = 0;
            var image = await LoadFromStreamAsync(bytes, isSvg, decodeWidth, decodeHeight, decodeType,
                dispatcherQueue, dpiScale, token, returnNullOnCancellation: false).ConfigureAwait(false);
            token.ThrowIfCancellationRequested();
            return new CacheResult(image, false);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception error)
        {
            Debug.WriteLine($"[ImageEx] uncached image failed host={candidate.Uri.Host} type={error.GetType().Name}");
            return new CacheResult(null, false);
        }
        finally
        {
            UncachedConcurrency.Release();
        }
    }
}
