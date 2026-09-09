#nullable enable

using System.Runtime.CompilerServices;
using Microsoft.UI.Dispatching;
using Windows.Storage.Streams;

namespace ImageEx.Cache;

internal sealed partial class ImageExCacheManager
{
    private static readonly ConditionalWeakTable<BitmapImage, InMemoryRandomAccessStream> AnimatedStreams = new();

    private static async Task<ImageSource?> CreateAnimatedBitmapAsync(
        IRandomAccessStream sourceStream,
        DecodeDimensions dimensions,
        DispatcherQueue? dispatcherQueue,
        CancellationToken token)
    {
        // A native stream avoids a managed adapter whose lifetime ends before GIF playback.
        var nativeStream = new InMemoryRandomAccessStream();
        ImageSource? result = null;
        try
        {
            sourceStream.Seek(0);
            await RandomAccessStream.CopyAsync(sourceStream, nativeStream).AsTask(token).ConfigureAwait(false);
            nativeStream.Seek(0);
            result = await RunOnDispatcherAsync<ImageSource?>(dispatcherQueue, async () =>
            {
                token.ThrowIfCancellationRequested();
                var bitmap = new BitmapImage
                {
                    DecodePixelType = DecodePixelType.Physical,
                    DecodePixelWidth = dimensions.TargetWidth,
                    DecodePixelHeight = dimensions.TargetHeight,
                    AutoPlay = true
                };
                await bitmap.SetSourceAsync(nativeStream).AsTask(token);
                token.ThrowIfCancellationRequested();
                ImageExSourceMetadata.RegisterDecodedSource(bitmap, new Size(dimensions.NaturalWidth, dimensions.NaturalHeight));
                AnimatedStreams.Add(bitmap, nativeStream);
                return bitmap;
            }).ConfigureAwait(false);
            return result;
        }
        finally
        {
            if (result is null)
            {
                nativeStream.Dispose();
            }
        }
    }
}
