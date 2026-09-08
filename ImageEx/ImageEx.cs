// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using ImageEx.Cache;
using Microsoft.UI.Dispatching;
using System.Diagnostics;

namespace ImageEx
{
    /// <summary>
    /// The ImageEx control extends the platform Image control to improve app performance and responsiveness.
    /// It downloads source images asynchronously and shows a loading indicator during each download.
    /// It stores each downloaded source in the app's local cache so later loads use fewer resources and finish sooner.
    /// </summary>
    public partial class ImageEx : ImageExBase
    {
        internal ImageExCacheManager CacheManagerOverride { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="ImageEx"/> class.
        /// </summary>
        public ImageEx()
        {
            DefaultStyleKey = typeof(ImageEx);
        }

        protected override async Task<ImageLoadResult> ResolveImageRequestAsync(ImageRequest request, CancellationToken token)
        {
            if (ImageExDiagnostics.DisableHttpImages)
            {
                return new ImageLoadResult(null, false);
            }

            var manager = CacheManagerOverride ?? ImageExCacheManager.Instance;
            manager.MaxCacheDays = DiskCacheDays;
            manager.MaxCacheSizeBytes = DiskCacheSizeMB * 1024L * 1024L;
            var dispatcher = ImageDispatcherQueue;
            var dpiScale = XamlRoot?.RasterizationScale ?? 1.0;
            foreach (var candidate in request.Candidates)
            {
                token.ThrowIfCancellationRequested();
                var result = candidate.Mode == ImageRequestMode.Original
                    ? await manager.GetOrLoadOriginalImageAsync(candidate.Uri, DecodePixelWidth, DecodePixelHeight,
                        DecodePixelType, token, dispatcher, dpiScale, returnNullOnCancellation: true)
                    : await manager.GetOrLoadImageAsync(candidate.Uri, DecodePixelWidth, DecodePixelHeight,
                        DecodePixelType, token, dispatcher, dpiScale, returnNullOnCancellation: true);
                token.ThrowIfCancellationRequested();
                Debug.WriteLine($"[ImageExRoute] Host={candidate.Uri.Host} Mode={candidate.Mode} Success={result.Image != null} CacheHit={result.WasCacheHit}");
                if (result.Image != null)
                {
                    return new ImageLoadResult(result.Image, result.WasCacheHit);
                }
            }

            return new ImageLoadResult(null, false);
        }

        /// <summary>
        /// Resolves an image and reports whether the memory or disk cache supplied it.
        /// </summary>
        /// <param name="imageUri">The URI of the image to load.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The resolved image and its cache status.</returns>
        protected override async Task<ImageLoadResult> ResolveImageAsync(Uri imageUri, CancellationToken token)
        {
            // Use the base memory-only cache when disk caching is disabled.
            if (!EnableDiskCache)
            {
                return await base.ResolveImageAsync(imageUri, token);
            }

            // Let the base control handle local and embedded resources.
            if (!imageUri.IsAbsoluteUri ||
                imageUri.Scheme is "ms-appx" or "ms-resource" or "ms-appdata" or "data" or "file")
            {
                return await base.ResolveImageAsync(imageUri, token);
            }

            if (ImageExDiagnostics.DisableHttpImages)
            {
                return new ImageLoadResult(null, IsCacheHit: false);
            }

            // Configure the cache manager from dependency properties.
            var manager = CacheManagerOverride ?? ImageExCacheManager.Instance;
            manager.MaxCacheDays = DiskCacheDays;
            manager.MaxCacheSizeBytes = DiskCacheSizeMB * 1024L * 1024L;

            // Get the DPI scale for adaptive decode sizing. Use 1.0 when XamlRoot is unavailable.
            var dpiScale = XamlRoot?.RasterizationScale ?? 1.0;

            var dispatcherQueue = ImageDispatcherQueue;
            if (dispatcherQueue == null)
            {
                Debug.WriteLine($"[ImageEx] Dispatcher unavailable for {imageUri}; disk-cache UI decode skipped.");
            }

            var result = await manager.GetOrLoadImageAsync(
                imageUri,
                DecodePixelWidth,
                DecodePixelHeight,
                DecodePixelType,
                token,
                dispatcherQueue,
                dpiScale,
                returnNullOnCancellation: true);

            // A newer source, null source, or unload can supersede this request.
            // LoadImageAsync drops this empty result after its request-state check.
            if (token.IsCancellationRequested)
            {
                return new ImageLoadResult(null, IsCacheHit: false);
            }

            if (result.Image != null)
            {
                return new ImageLoadResult(result.Image, result.WasCacheHit);
            }

            ImageExDiagnostics.RecordHttpFallback(imageUri, DecodePixelWidth, DecodePixelHeight, DecodePixelType);
            return new ImageLoadResult(null, IsCacheHit: false);
        }
    }
}
