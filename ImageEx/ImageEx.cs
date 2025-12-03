// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using ImageEx.Cache;

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

            // Configure cache manager from dependency properties
            var manager = ImageExCacheManager.Instance;
            manager.MaxCacheDays = DiskCacheDays;
            manager.MaxCacheSizeBytes = DiskCacheSizeMB * 1024L * 1024L;

            var result = await manager.GetOrLoadImageAsync(
                imageUri,
                DecodePixelWidth,
                DecodePixelHeight,
                DecodePixelType,
                token);

            // Skip shimmer animation on cache hit by jumping directly to Loaded state
            if (result.WasCacheHit && result.Image != null)
            {
                VisualStateManager.GoToState(this, LoadedState, useTransitions: false);
            }

            // Return cached/downloaded image, or fall back to base behavior on failure
            return result.Image ?? await base.ProvideCachedResourceAsync(imageUri, token);
        }
    }
}