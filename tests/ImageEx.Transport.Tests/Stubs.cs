namespace Microsoft.UI.Dispatching
{
    public sealed class DispatcherQueue;
}

namespace Windows.Storage
{
    public sealed class ApplicationData
    {
        public static ApplicationData Current => throw new Exception("Uncached requests must not access app storage.");
        public ApplicationData LocalFolder => this;
        public string Path => throw new Exception("Uncached requests must not access app storage.");
    }
}

namespace ImageEx.Cache
{
    internal enum DecodePixelType { Physical }
    internal sealed record DecodedImage(long Bytes, bool IsSvg);
    internal sealed partial class ImageExCacheManager
    {
        internal sealed record CacheResult(DecodedImage? Image, bool WasCacheHit);

        // The test seam replaces WinUI decode only. Transport and bounds use production code.
        private static Task<DecodedImage?> LoadFromStreamAsync(Stream stream, bool isSvg,
            int decodeWidth, int decodeHeight, DecodePixelType decodeType,
            Microsoft.UI.Dispatching.DispatcherQueue? dispatcherQueue, double dpiScale,
            CancellationToken token, bool returnNullOnCancellation)
        {
            token.ThrowIfCancellationRequested();
            if (stream.Position != 0) throw new Exception("Decoder requires the source start.");
            return Task.FromResult<DecodedImage?>(new(stream.Length, isSvg));
        }
    }
}
