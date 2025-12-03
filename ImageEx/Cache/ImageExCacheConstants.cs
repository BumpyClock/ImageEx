// ABOUTME: Centralized constants and configuration defaults for ImageEx disk caching.
// ABOUTME: Provides cache directory path resolution using WinUI ApplicationData.

#nullable enable

using Windows.Storage;

namespace ImageEx.Cache;

/// <summary>
/// Constants and defaults for the ImageEx disk caching system.
/// </summary>
internal static class ImageExCacheConstants
{
    /// <summary>
    /// Default cache expiration in days.
    /// </summary>
    public const int DefaultCacheDays = 30;

    /// <summary>
    /// Default maximum cache size in megabytes.
    /// </summary>
    public const int DefaultCacheSizeMB = 500;

    /// <summary>
    /// Default maximum cache size in bytes.
    /// </summary>
    public const long DefaultCacheSizeBytes = DefaultCacheSizeMB * 1024L * 1024L;

    /// <summary>
    /// When cleaning up, reduce cache to this ratio of max size (80%).
    /// </summary>
    public const double CleanupTargetRatio = 0.8;

    /// <summary>
    /// Minimum interval between cleanup operations in minutes.
    /// </summary>
    public const int CleanupIntervalMinutes = 10;

    /// <summary>
    /// Name of the JSON file storing cache metadata.
    /// </summary>
    public const string MetadataFileName = "cache_metadata.json";

    /// <summary>
    /// Gets the cache directory path using WinUI-friendly ApplicationData.
    /// </summary>
    /// <returns>Full path to the image cache directory.</returns>
    public static string GetCacheDirectory()
        => Path.Combine(ApplicationData.Current.LocalFolder.Path, "DigestsApp", "ImageCache");
}
