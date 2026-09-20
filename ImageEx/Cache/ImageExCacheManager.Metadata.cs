#nullable enable

using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Graphics.Imaging;

namespace ImageEx.Cache;

internal sealed partial class ImageExCacheManager
{
    internal ImageAcquisitionResult? TryGetKnownDimensions(ImageRequest request)
    {
        foreach (var candidate in request.Candidates)
        {
            var key = SourceKey(candidate);
            if (!_diskCache.TryGetEntry(key, out var entry) || entry is null
                || !ValidDimension(entry.SourceWidth) || !ValidDimension(entry.SourceHeight)
                || (DateTimeOffset.UtcNow - entry.DownloadedUtc).TotalDays >= MaxCacheDays
                || !File.Exists(_diskCache.GetFilePath(key, entry.Extension))) continue;
            return new(candidate, new(entry.SourceWidth, entry.SourceHeight,
                entry.DimensionsAreReduced ? ImageDimensionProvenance.ReducedDecode : ImageDimensionProvenance.NativeSource), true);
        }
        return null;
    }

    internal async Task<ImageAcquisitionResult> AcquireAsync(ImageRequest request,
        CancellationToken token, bool cacheOnly = false)
    {
        ArgumentNullException.ThrowIfNull(request);
        BeginOperation();
        var previousInterest = _speculativeInterest.Value;
        _speculativeInterest.Value = true;
        try
        {
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(token, _originalShutdown.Token);
            var operationToken = lifetime.Token;
            operationToken.ThrowIfCancellationRequested();
            await _diskCache.EnsureMetadataLoadedAsync().ConfigureAwait(false);
            foreach (var candidate in request.Candidates)
            {
                operationToken.ThrowIfCancellationRequested();
                CacheResult result;
                if (cacheOnly)
                {
                    var key = SourceKey(candidate);
                    if (!_diskCache.TryGetEntry(key, out var entry) || entry == null)
                    {
                        if (candidate.Mode == ImageRequestMode.Original ||
                            !TryMigrateLegacyCacheEntry(key, candidate.Uri, out entry) || entry == null)
                            continue;
                    }
                    var dimensions = await ReadCachedDimensionsAsync(key, entry, operationToken).ConfigureAwait(false);
                    result = new CacheResult(null, true, dimensions);
                }
                else
                {
                    result = candidate.Mode == ImageRequestMode.Original
                        ? await GetOrLoadOriginalImageAsync(candidate.Uri, 0, 0, DecodePixelType.Physical,
                            operationToken, metadataOnly: true).ConfigureAwait(false)
                        : await GetOrLoadImageAsync(candidate.Uri, 0, 0, DecodePixelType.Physical,
                            operationToken, metadataOnly: true).ConfigureAwait(false);
                }
                operationToken.ThrowIfCancellationRequested();
                if (result.Dimensions != null)
                    return new ImageAcquisitionResult(candidate, result.Dimensions, result.WasCacheHit);
            }
            return new ImageAcquisitionResult(null, null, false);
        }
        finally
        {
            _speculativeInterest.Value = previousInterest;
            EndOperation();
        }
    }

    private static string SourceKey(ImageRequestCandidate candidate) =>
        (candidate.Mode == ImageRequestMode.Original ? "original-" : string.Empty) +
        ImageExDiskCache.ComputeSourceCacheKey(candidate.Uri);

    private async Task<ImageDimensions?> ReadCachedDimensionsAsync(string key, CacheEntry entry, CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        var path = _diskCache.GetFilePath(key, entry.Extension);
        if ((DateTimeOffset.UtcNow - entry.DownloadedUtc).TotalDays >= MaxCacheDays || !File.Exists(path))
            return null;

        if (ValidDimension(entry.SourceWidth) && ValidDimension(entry.SourceHeight))
        {
            _diskCache.UpdateAccessTime(key);
            return new ImageDimensions(entry.SourceWidth, entry.SourceHeight,
                entry.DimensionsAreReduced ? ImageDimensionProvenance.ReducedDecode : ImageDimensionProvenance.NativeSource);
        }

        ImageDimensions? dimensions;
        try
        {
            dimensions = await ReadDimensionsAsync(new FileStream(path, FileMode.Open, FileAccess.Read,
                FileShare.Read | FileShare.Delete), entry.Extension == ".svg", token).ConfigureAwait(false);
        }
        catch (IOException)
        {
            return null;
        }
        token.ThrowIfCancellationRequested();
        if (dimensions != null && PersistDimensions(key, dimensions))
        {
            _diskCache.UpdateAccessTime(key);
            return dimensions;
        }
        return null;
    }

    private static bool ValidDimension(double value) => double.IsFinite(value) && value > 0;

    private bool PersistDimensions(string key, ImageDimensions dimensions)
    {
        if (!_diskCache.TryGetEntry(key, out var entry) || entry == null ||
            !File.Exists(_diskCache.GetFilePath(key, entry.Extension))) return false;
        _diskCache.AddOrUpdateEntry(key, entry with
        {
            SourceWidth = dimensions.Width,
            SourceHeight = dimensions.Height,
            DimensionsAreReduced = dimensions.Provenance == ImageDimensionProvenance.ReducedDecode
        });
        return true;
    }

    private void PersistImageDimensions(string key, ImageSource image)
    {
        // The weak metadata table is safe to read off the UI thread; it stores no bitmap pixels.
        if (ImageExSourceMetadata.TryGetNaturalSize(image, out var size))
            PersistDimensions(key, new ImageDimensions(size.Width, size.Height, ImageDimensionProvenance.NativeSource));
    }

    private async Task<ImageDimensions?> ReadDimensionsAsync(Stream stream, bool isSvg, CancellationToken token)
    {
        using (stream)
        {
            await _decodeConcurrency.WaitAsync(token, () => false).ConfigureAwait(false);
            try
            {
                token.ThrowIfCancellationRequested();
                if (isSvg)
                {
                    var size = SvgNaturalSize.Read(stream);
                    if (!ValidDimension(size.Width) || !ValidDimension(size.Height)) return null;
                    // Validate XML without a UI object. Native SVG rendering remains the control's job.
                    using var reader = System.Xml.XmlReader.Create(stream, new System.Xml.XmlReaderSettings
                    {
                        DtdProcessing = System.Xml.DtdProcessing.Prohibit,
                        XmlResolver = null,
                        MaxCharactersInDocument = _maximumSourceBytes
                    });
                    while (reader.Read()) token.ThrowIfCancellationRequested();
                    return new ImageDimensions(size.Width, size.Height, ImageDimensionProvenance.NativeSource);
                }

                using var random = stream.AsRandomAccessStream();
                var decoder = await BitmapDecoder.CreateAsync(random).AsTask(token).ConfigureAwait(false);
                var width = decoder.OrientedPixelWidth;
                var height = decoder.OrientedPixelHeight;
                if (width == 0 || height == 0) return null;
                // Validate the payload with a bounded one-pixel decode, then discard it. Native
                // source dimensions come from the decoder, never from the reduced validation pixel.
                var pixels = await decoder.GetPixelDataAsync(BitmapPixelFormat.Bgra8, BitmapAlphaMode.Premultiplied,
                    new BitmapTransform { ScaledWidth = 1, ScaledHeight = 1 },
                    ExifOrientationMode.RespectExifOrientation, ColorManagementMode.DoNotColorManage)
                    .AsTask(token).ConfigureAwait(false);
                token.ThrowIfCancellationRequested();
                if (pixels.DetachPixelData().Length != 4) return null;
                return new ImageDimensions(width, height, ImageDimensionProvenance.NativeSource);
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception) { return null; }
            finally
            {
                _decodeConcurrency.Release();
            }
        }
    }
}
