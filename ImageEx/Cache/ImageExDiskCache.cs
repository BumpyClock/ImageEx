// ABOUTME: Handles low-level disk cache operations for ImageEx including file I/O,
// ABOUTME: cache key hashing, extension detection, and JSON metadata persistence.

#nullable enable

using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace ImageEx.Cache;

/// <summary>
/// Manages disk-level cache operations including file I/O, key computation, and metadata.
/// </summary>
internal sealed class ImageExDiskCache
{
    private readonly string _cacheDir;
    private readonly string _metadataPath;
    private readonly ConcurrentDictionary<string, CacheEntry> _metadata = new();
    private readonly SemaphoreSlim _metadataLock = new(1, 1);
    private bool _loaded;

    public ImageExDiskCache(string cacheDirectory)
    {
        _cacheDir = cacheDirectory;
        _metadataPath = Path.Combine(_cacheDir, ImageExCacheConstants.MetadataFileName);
    }

    /// <summary>
    /// Computes a cache key that includes decode parameters to avoid variant collisions.
    /// </summary>
    /// <param name="uri">The image URI.</param>
    /// <param name="decodeWidth">Decode pixel width.</param>
    /// <param name="decodeHeight">Decode pixel height.</param>
    /// <param name="decodeType">Decode pixel type (Physical/Logical).</param>
    /// <param name="isSvg">Whether the image is SVG format.</param>
    /// <returns>16-character hex cache key.</returns>
    public static string ComputeCacheKey(Uri uri, int decodeWidth, int decodeHeight, DecodePixelType decodeType, bool isSvg)
    {
        var input = $"{uri.OriginalString}|{decodeWidth}|{decodeHeight}|{(int)decodeType}|{isSvg}";
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(hash)[..16].ToLowerInvariant();
    }

    /// <summary>
    /// Determines file extension from content-type header, URL, or fallback.
    /// </summary>
    /// <param name="uri">The image URI.</param>
    /// <param name="contentType">Content-Type header from HTTP response.</param>
    /// <param name="isSvg">Whether SVG was detected.</param>
    /// <returns>File extension including the leading dot.</returns>
    public static string GetExtension(Uri uri, string? contentType, bool isSvg)
    {
        if (isSvg) return ".svg";

        // Prefer content-type over URI extension (handles extensionless URLs)
        if (!string.IsNullOrEmpty(contentType))
        {
            var ext = contentType.ToLowerInvariant() switch
            {
                "image/jpeg" => ".jpg",
                "image/png" => ".png",
                "image/gif" => ".gif",
                "image/webp" => ".webp",
                "image/bmp" => ".bmp",
                "image/svg+xml" => ".svg",
                _ => null
            };
            if (ext != null) return ext;
        }

        // Fallback to URI extension
        var uriExt = Path.GetExtension(uri.AbsolutePath).ToLowerInvariant();
        return uriExt switch
        {
            ".jpg" or ".jpeg" or ".png" or ".gif" or ".webp" or ".bmp" or ".svg" => uriExt,
            _ => ".bin" // Unknown - store raw bytes
        };
    }

    /// <summary>
    /// Gets the full file path for a cached image.
    /// </summary>
    public string GetFilePath(string cacheKey, string extension)
        => Path.Combine(_cacheDir, $"{cacheKey}{extension}");

    /// <summary>
    /// Ensures metadata is loaded from disk (lazy initialization).
    /// </summary>
    public async Task EnsureMetadataLoadedAsync()
    {
        if (_loaded) return;

        await _metadataLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_loaded) return;

            if (File.Exists(_metadataPath))
            {
                try
                {
                    var json = await File.ReadAllTextAsync(_metadataPath).ConfigureAwait(false);
                    var data = JsonSerializer.Deserialize<Dictionary<string, CacheEntry>>(json);
                    if (data != null)
                    {
                        foreach (var kvp in data)
                            _metadata.TryAdd(kvp.Key, kvp.Value);
                    }
                }
                catch
                {
                    // Corrupt metadata - start fresh
                }
            }
            _loaded = true;
        }
        finally
        {
            _metadataLock.Release();
        }
    }

    /// <summary>
    /// Tries to get a cache entry by key.
    /// </summary>
    public bool TryGetEntry(string cacheKey, out CacheEntry? entry)
        => _metadata.TryGetValue(cacheKey, out entry);

    /// <summary>
    /// Adds or updates a cache entry and sets its last access time.
    /// </summary>
    public void AddOrUpdateEntry(string cacheKey, CacheEntry entry)
    {
        entry.LastAccessUtc = DateTimeOffset.UtcNow;
        _metadata[cacheKey] = entry;
    }

    /// <summary>
    /// Updates the last access time for LRU tracking.
    /// </summary>
    public void UpdateAccessTime(string cacheKey)
    {
        if (_metadata.TryGetValue(cacheKey, out var entry))
            entry.LastAccessUtc = DateTimeOffset.UtcNow;
    }

    /// <summary>
    /// Removes a cache entry.
    /// </summary>
    public void RemoveEntry(string cacheKey)
        => _metadata.TryRemove(cacheKey, out _);

    /// <summary>
    /// Gets all cache entries for enumeration.
    /// </summary>
    public IEnumerable<KeyValuePair<string, CacheEntry>> GetAllEntries()
        => _metadata.ToArray();

    /// <summary>
    /// Gets the total size of all cached files in bytes.
    /// </summary>
    public long GetTotalSizeBytes()
        => _metadata.Values.Sum(e => e.SizeBytes);

    /// <summary>
    /// Persists metadata to disk.
    /// </summary>
    public async Task SaveMetadataAsync()
    {
        await _metadataLock.WaitAsync().ConfigureAwait(false);
        try
        {
            Directory.CreateDirectory(_cacheDir);
            var snapshot = _metadata.ToDictionary(k => k.Key, v => v.Value);
            var json = JsonSerializer.Serialize(snapshot, new JsonSerializerOptions { WriteIndented = false });
            await File.WriteAllTextAsync(_metadataPath, json).ConfigureAwait(false);
        }
        finally
        {
            _metadataLock.Release();
        }
    }

    /// <summary>
    /// Attempts to delete a cached file, ignoring errors.
    /// </summary>
    public void TryDeleteFile(string filePath)
    {
        try
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
        catch
        {
            // Best effort - ignore deletion failures
        }
    }
}

/// <summary>
/// Represents a cached image entry with metadata for TTL and LRU tracking.
/// </summary>
internal sealed class CacheEntry
{
    /// <summary>
    /// Original URL of the cached image.
    /// </summary>
    public string Url { get; set; } = string.Empty;

    /// <summary>
    /// File extension (including dot) for the cached file.
    /// </summary>
    public string Extension { get; set; } = string.Empty;

    /// <summary>
    /// When the image was originally downloaded (for TTL).
    /// </summary>
    public DateTimeOffset DownloadedUtc { get; set; }

    /// <summary>
    /// When the image was last accessed (for LRU).
    /// </summary>
    public DateTimeOffset LastAccessUtc { get; set; }

    /// <summary>
    /// Size of the cached file in bytes.
    /// </summary>
    public long SizeBytes { get; set; }
}
