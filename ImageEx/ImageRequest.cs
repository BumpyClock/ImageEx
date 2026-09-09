#nullable enable

namespace ImageEx;

/// <summary>
/// Selects the bounded download policy for an image candidate.
/// </summary>
public enum ImageRequestMode
{
    Cached,
    Original
}

/// <summary>
/// Describes one source in an ordered image request.
/// </summary>
public sealed record ImageRequestCandidate(Uri Uri, ImageRequestMode Mode = ImageRequestMode.Cached);

/// <summary>
/// Preserves the ordered sources for one image across control reloads.
/// </summary>
public sealed class ImageRequest
{
    public ImageRequest(IEnumerable<ImageRequestCandidate> candidates)
    {
        var copy = candidates.ToArray();
        if (copy.Length == 0 || copy.Any(candidate => candidate?.Uri is null ||
            !candidate.Uri.IsAbsoluteUri || !candidate.Uri.IsHttpUri()))
        {
            throw new ArgumentException("Image candidates must contain absolute HTTP or HTTPS URLs.", nameof(candidates));
        }

        Candidates = Array.AsReadOnly(copy);
    }

    public IReadOnlyList<ImageRequestCandidate> Candidates { get; }
}
