#nullable enable

namespace ImageEx;

/// <summary>Identifies which measurements describe the downloaded source.</summary>
public enum ImageDimensionProvenance
{
    Unknown,
    NativeSource,
    ReducedDecode
}

/// <summary>Lightweight source facts. No decoded image or control is retained.</summary>
public sealed record ImageDimensions(double Width, double Height, ImageDimensionProvenance Provenance);

/// <summary>A successful result refers to reusable cached payload bytes.</summary>
public sealed record ImageAcquisitionResult(ImageRequestCandidate? Candidate, ImageDimensions? Dimensions, bool WasCacheHit)
{
    public bool IsAvailable => Candidate is not null && Dimensions is not null;
}

/// <summary>Acquires ordered image candidates through the control's shared bounded cache.</summary>
public static class ImageAcquisition
{
    /// <summary>Reads already-loaded source metadata without downloading, decoding, or awaiting cache initialization.</summary>
    public static ImageAcquisitionResult? TryGetKnown(ImageRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return Cache.ImageExCacheManager.Instance.TryGetKnownDimensions(request);
    }

    /// <summary>
    /// Returns source facts without constructing controls or retaining bitmaps. Cancellation
    /// releases only this consumer's interest and makes no guarantee that bytes were saved.
    /// Cache-only requests never start a download.
    /// </summary>
    public static Task<ImageAcquisitionResult> AcquireAsync(ImageRequest request,
        CancellationToken cancellationToken = default, bool cacheOnly = false)
    {
        ArgumentNullException.ThrowIfNull(request);
        return Cache.ImageExCacheManager.Instance.AcquireAsync(request, cancellationToken, cacheOnly);
    }
}
