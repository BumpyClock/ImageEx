namespace ImageEx;

/// <summary>
/// Extension methods for <see cref="Uri"/>.
/// </summary>
internal static class UriExtensions
{
    /// <summary>
    /// Returns <c>true</c> when the URI is an absolute HTTP or HTTPS URI.
    /// </summary>
    internal static bool IsHttpUri(this Uri uri)
        => uri.IsAbsoluteUri && (uri.Scheme == "http" || uri.Scheme == "https");
}
