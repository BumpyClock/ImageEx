using System.Runtime.CompilerServices;

namespace ImageEx;

/// <summary>
/// Provides source dimensions independent of the cache decode size.
/// </summary>
public static class ImageExSourceMetadata
{
    private sealed record Metadata(Size NaturalSize);

    private static readonly ConditionalWeakTable<ImageSource, Metadata> Sources = new();

    /// <summary>
    /// Gets the natural dimensions when the cache retained them.
    /// </summary>
    public static bool TryGetNaturalSize(ImageSource source, out Size size)
    {
        ArgumentNullException.ThrowIfNull(source);
        size = Sources.TryGetValue(source, out var metadata) ? metadata.NaturalSize : default;
        return size.Width > 0 && size.Height > 0;
    }

    internal static void RegisterDecodedSource(ImageSource source, Size size)
    {
        Sources.GetValue(source, _ => new Metadata(size));
    }

    internal static bool IsDecoded(ImageSource source) => Sources.TryGetValue(source, out _);
}
