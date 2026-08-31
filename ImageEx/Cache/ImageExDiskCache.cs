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
internal sealed class ImageExDiskCache : IAsyncDisposable
{
    private static readonly TimeSpan DefaultMetadataDebounce = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan DefaultMetadataMaximumDelay = TimeSpan.FromSeconds(5);

    private readonly string _cacheDir;
    private readonly string _metadataPath;
    private readonly ConcurrentDictionary<string, CacheEntry> _metadata = new();
    private readonly SemaphoreSlim _metadataLock = new(1, 1);
    private readonly SemaphoreSlim _metadataSignal = new(0, 1);
    private readonly object _writerGate = new();
    private readonly Func<IReadOnlyDictionary<string, CacheEntry>, CancellationToken, Task> _metadataWriter;
    private readonly TimeSpan _metadataDebounce;
    private readonly TimeSpan _metadataMaximumDelay;
    private Task _writerTask = Task.CompletedTask;
    private long _metadataVersion;
    private long _persistedMetadataVersion;
    private DateTimeOffset _firstDirtyUtc;
    private DateTimeOffset _lastDirtyUtc;
    private Exception? _lastWriterFailure;
    private bool _writerRunning;
    private bool _forceFlush;
    private bool _loaded;
    private bool _disposed;

    public ImageExDiskCache(
        string cacheDirectory,
        Func<IReadOnlyDictionary<string, CacheEntry>, CancellationToken, Task>? metadataWriter = null,
        TimeSpan? metadataDebounce = null,
        TimeSpan? metadataMaximumDelay = null)
    {
        _cacheDir = cacheDirectory;
        _metadataPath = Path.Combine(_cacheDir, ImageExCacheConstants.MetadataFileName);
        _metadataWriter = metadataWriter ?? WriteMetadataFileAsync;
        _metadataDebounce = metadataDebounce ?? DefaultMetadataDebounce;
        _metadataMaximumDelay = metadataMaximumDelay ?? DefaultMetadataMaximumDelay;
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
    /// Computes a cache key for the original source bytes, independent of decode parameters.
    /// </summary>
    /// <param name="uri">The image URI.</param>
    /// <returns>16-character hex cache key.</returns>
    public static string ComputeSourceCacheKey(Uri uri)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(uri.OriginalString));
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
                    var data = JsonSerializer.Deserialize(json, ImageExCacheJsonContext.Default.DictionaryStringCacheEntry);
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
        _metadata[cacheKey] = entry with { LastAccessUtc = DateTimeOffset.UtcNow };
        MarkMetadataDirty();
    }

    /// <summary>
    /// Updates the last access time for LRU tracking.
    /// </summary>
    public void UpdateAccessTime(string cacheKey)
        => UpdateAccessTime(cacheKey, DateTimeOffset.UtcNow, beforeUpdateAttempt: null);

    internal void UpdateAccessTime(
        string cacheKey,
        DateTimeOffset accessUtc,
        Action? beforeUpdateAttempt)
    {
        while (_metadata.TryGetValue(cacheKey, out var entry))
        {
            if (accessUtc <= entry.LastAccessUtc)
            {
                return;
            }

            var updatedEntry = entry with { LastAccessUtc = accessUtc };
            beforeUpdateAttempt?.Invoke();
            beforeUpdateAttempt = null;
            if (_metadata.TryUpdate(cacheKey, updatedEntry, entry))
            {
                MarkMetadataDirty();
                return;
            }
        }
    }

    /// <summary>
    /// Removes a cache entry.
    /// </summary>
    public void RemoveEntry(string cacheKey)
    {
        if (_metadata.TryRemove(cacheKey, out _))
        {
            MarkMetadataDirty();
        }
    }

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
    /// Persists every dirty metadata version to disk before this method returns.
    /// </summary>
    public async Task FlushMetadataAsync(CancellationToken cancellationToken = default)
    {
        Task writerTask;
        lock (_writerGate)
        {
            if (_persistedMetadataVersion >= _metadataVersion)
            {
                return;
            }

            _forceFlush = true;
            EnsureWriterStartedLocked();
            SignalWriterLocked();
            writerTask = _writerTask;
        }

        await writerTask.WaitAsync(cancellationToken).ConfigureAwait(false);

        lock (_writerGate)
        {
            if (_persistedMetadataVersion >= _metadataVersion)
            {
                return;
            }

            if (_lastWriterFailure is not null)
            {
                throw new IOException("ImageEx cache metadata flush failed.", _lastWriterFailure);
            }

            throw new IOException("ImageEx cache metadata flush ended before all versions were persisted.");
        }
    }

    private void MarkMetadataDirty()
    {
        lock (_writerGate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            var now = DateTimeOffset.UtcNow;
            if (_persistedMetadataVersion >= _metadataVersion)
            {
                _firstDirtyUtc = now;
            }

            _lastDirtyUtc = now;
            _metadataVersion++;
            _lastWriterFailure = null;
            EnsureWriterStartedLocked();
            SignalWriterLocked();
        }
    }

    private void EnsureWriterStartedLocked()
    {
        if (_writerRunning)
        {
            return;
        }

        _writerRunning = true;
        _writerTask = Task.Run(RunMetadataWriterAsync);
    }

    private void SignalWriterLocked()
    {
        if (_metadataSignal.CurrentCount == 0)
        {
            _metadataSignal.Release();
        }
    }

    private async Task RunMetadataWriterAsync()
    {
        while (true)
        {
            TimeSpan delay;
            long version;
            Dictionary<string, CacheEntry> snapshot;
            lock (_writerGate)
            {
                if (_persistedMetadataVersion >= _metadataVersion)
                {
                    _forceFlush = false;
                    _writerRunning = false;
                    return;
                }

                var now = DateTimeOffset.UtcNow;
                var dueUtc = _forceFlush
                    ? now
                    : Min(
                        _lastDirtyUtc + _metadataDebounce,
                        _firstDirtyUtc + _metadataMaximumDelay);
                delay = dueUtc > now ? dueUtc - now : TimeSpan.Zero;
                version = _metadataVersion;
                snapshot = _metadata.ToDictionary(entry => entry.Key, entry => entry.Value);
            }

            if (delay > TimeSpan.Zero && await _metadataSignal.WaitAsync(delay).ConfigureAwait(false))
            {
                continue;
            }

            while (_metadataSignal.Wait(0))
            {
            }

            try
            {
                await _metadataWriter(snapshot, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                lock (_writerGate)
                {
                    _lastWriterFailure = ex;
                    _forceFlush = false;
                    _writerRunning = false;
                }

                return;
            }

            lock (_writerGate)
            {
                _persistedMetadataVersion = Math.Max(_persistedMetadataVersion, version);
                _lastWriterFailure = null;
            }
        }
    }

    private static DateTimeOffset Min(DateTimeOffset left, DateTimeOffset right)
        => left <= right ? left : right;

    private async Task WriteMetadataFileAsync(
        IReadOnlyDictionary<string, CacheEntry> snapshot,
        CancellationToken cancellationToken)
    {
        await _metadataLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        var temporaryPath = Path.Combine(
            _cacheDir,
            $".{ImageExCacheConstants.MetadataFileName}.{Guid.NewGuid():N}.tmp");
        try
        {
            Directory.CreateDirectory(_cacheDir);
            var serializableSnapshot = snapshot as Dictionary<string, CacheEntry>
                ?? snapshot.ToDictionary(entry => entry.Key, entry => entry.Value);
            var json = JsonSerializer.Serialize(
                serializableSnapshot,
                ImageExCacheJsonContext.Default.DictionaryStringCacheEntry);
            await File.WriteAllTextAsync(temporaryPath, json, cancellationToken).ConfigureAwait(false);

            if (File.Exists(_metadataPath))
            {
                File.Replace(temporaryPath, _metadataPath, destinationBackupFileName: null);
            }
            else
            {
                File.Move(temporaryPath, _metadataPath);
            }
        }
        finally
        {
            if (File.Exists(temporaryPath))
            {
                File.Delete(temporaryPath);
            }

            _metadataLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        lock (_writerGate)
        {
            if (_disposed)
            {
                return;
            }
        }

        await FlushMetadataAsync().ConfigureAwait(false);

        lock (_writerGate)
        {
            _disposed = true;
        }

        _metadataSignal.Dispose();
        _metadataLock.Dispose();
    }

    /// <summary>
    /// Attempts to delete a cached file, ignoring errors.
    /// </summary>
    public bool TryDeleteFile(string filePath)
    {
        try
        {
            if (!File.Exists(filePath))
            {
                return true;
            }

            File.Delete(filePath);
            return true;
        }
        catch
        {
            // Best effort - caller decides whether metadata can be removed.
            return false;
        }
    }
}

/// <summary>
/// Represents a cached image entry with metadata for TTL and LRU tracking.
/// </summary>
internal sealed record CacheEntry
{
    /// <summary>
    /// Original URL of the cached image.
    /// </summary>
    public string Url { get; init; } = string.Empty;

    /// <summary>
    /// File extension (including dot) for the cached file.
    /// </summary>
    public string Extension { get; init; } = string.Empty;

    /// <summary>
    /// When the image was originally downloaded (for TTL).
    /// </summary>
    public DateTimeOffset DownloadedUtc { get; init; }

    /// <summary>
    /// When the image was last accessed (for LRU).
    /// </summary>
    public DateTimeOffset LastAccessUtc { get; init; }

    /// <summary>
    /// Size of the cached file in bytes.
    /// </summary>
    public long SizeBytes { get; init; }
}
