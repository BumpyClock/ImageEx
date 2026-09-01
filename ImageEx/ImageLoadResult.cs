#nullable enable

namespace ImageEx;

/// <summary>
/// Describes a resolved image and whether the cache supplied it.
/// </summary>
/// <param name="Image">The resolved image source.</param>
/// <param name="IsCacheHit">Whether the image came from the memory or disk cache.</param>
public sealed record ImageLoadResult(ImageSource? Image, bool IsCacheHit);
