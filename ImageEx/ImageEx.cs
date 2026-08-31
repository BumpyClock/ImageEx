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

        /// <summary>
        /// Provides cached image resources with optional disk caching and shimmer-skip optimization.
        /// </summary>
        /// <param name="imageUri">The URI of the image to load.</param>
        /// <param name="token">Cancellation token for the async operation.</param>
        /// <returns>The loaded ImageSource, or null to fall back to base behavior.</returns>
        protected override async Task<ImageSource> ProvideCachedResourceAsync(Uri imageUri, CancellationToken token)
        {
            // Use the base memory-only cache when disk caching is disabled.
            if (!EnableDiskCache)
            {
                return await base.ProvideCachedResourceAsync(imageUri, token);
            }

            // Let the base control handle local and embedded resources.
            if (!imageUri.IsAbsoluteUri ||
                imageUri.Scheme is "ms-appx" or "ms-resource" or "ms-appdata" or "data" or "file")
            {
                return await base.ProvideCachedResourceAsync(imageUri, token);
            }

            if (ImageExDiagnostics.DisableHttpImages)
            {
                return null;
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

            // If a newer source, a null source, or unload superseded this request, suppress the
            // cache-hit shimmer transition and fallback attach. Returning null lets
            // LoadImageAsync's IsRequestCurrent guard drop the stale result.
            if (token.IsCancellationRequested)
            {
                return null;
            }

            // Skip the shimmer animation on a cache hit by going directly to the Loaded state.
            if (result.WasCacheHit && result.Image != null)
            {
                VisualStateManager.GoToState(this, LoadedState, useTransitions: false);
            }

            if (result.Image != null)
            {
                return result.Image;
            }

            ImageExDiagnostics.RecordHttpFallback(imageUri, DecodePixelWidth, DecodePixelHeight, DecodePixelType);
            return null;
        }
    }
}
