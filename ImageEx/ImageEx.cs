// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using ImageEx.Cache;
using Microsoft.UI.Dispatching;

namespace ImageEx
{
    /// <summary>
    /// The ImageEx control extends the default Image platform control improving the performance and responsiveness of your Apps.
    /// Source images are downloaded asynchronously showing a load indicator while in progress.
    /// Once downloaded, the source image is stored in the App local cache to preserve resources and load time next time the image needs to be displayed.
    /// </summary>
    public partial class ImageEx : ImageExBase
    {
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
            // If disk caching disabled, use base (memory-only via BitmapImage built-in cache)
            if (!EnableDiskCache)
            {
                return await base.ProvideCachedResourceAsync(imageUri, token);
            }

            // Skip disk cache for local/embedded resources - let base handle these
            if (!imageUri.IsAbsoluteUri ||
                imageUri.Scheme is "ms-appx" or "ms-resource" or "ms-appdata" or "data" or "file")
            {
                return await base.ProvideCachedResourceAsync(imageUri, token);
            }

            if (ImageExDiagnostics.DisableHttpImages)
            {
                return null;
            }

            // Configure cache manager from dependency properties
            var manager = ImageExCacheManager.Instance;
            manager.MaxCacheDays = DiskCacheDays;
            manager.MaxCacheSizeBytes = DiskCacheSizeMB * 1024L * 1024L;

            // Get DPI scale for adaptive decode sizing (defaults to 1.0 if XamlRoot not available)
            var dpiScale = XamlRoot?.RasterizationScale ?? 1.0;

            var result = await manager.GetOrLoadImageAsync(
                imageUri,
                DecodePixelWidth,
                DecodePixelHeight,
                DecodePixelType,
                token,
                DispatcherQueue,
                dpiScale,
                returnNullOnCancellation: true);

            // If this request was superseded (newer Source, null Source, or unload), suppress both
            // the cache-hit shimmer-skip transition and any fallback attach. Returning null here
            // lets LoadImageAsync's IsRequestCurrent guard drop the stale result entirely.
            if (token.IsCancellationRequested)
            {
                return null;
            }

            // Skip shimmer animation on cache hit by jumping directly to Loaded state
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
