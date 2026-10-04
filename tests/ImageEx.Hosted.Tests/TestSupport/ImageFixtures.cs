using System.Collections.Concurrent;
using System.Runtime.InteropServices.WindowsRuntime;
using System.Text;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Media.Imaging;
using Windows.Foundation;
using Windows.Graphics.Imaging;
using Windows.Storage.Streams;

namespace ImageEx.Hosted.Tests;

internal readonly record struct Bgra(byte B, byte G, byte R, byte A)
{
    public static readonly Bgra Red = new(0, 0, 255, 255);
    public static readonly Bgra Green = new(0, 255, 0, 255);
    public static readonly Bgra Blue = new(255, 0, 0, 255);
    public static readonly Bgra Gray = new(127, 127, 127, 255);
}

/// <summary>
/// Creates real encoded images with the platform codecs and checks decoded output.
/// </summary>
internal static class ImageFixtures
{
    private const int MaxRasterDecodeBytes = 8 * 1024 * 1024;
    private const int MaxRasterDecodePixels = MaxRasterDecodeBytes / 4;
    private static readonly ConcurrentDictionary<(uint, uint, Bgra), Task<byte[]>> PngCache = new();

    public static Task<byte[]> PngAsync(uint width, uint height, Bgra color)
        => PngCache.GetOrAdd((width, height, color), key => EncodePngAsync(key.Item1, key.Item2, key.Item3));

    public static byte[] Corrupt => "not an encoded image"u8.ToArray();

    public static byte[] Svg(int width, int height, string fill)
        => Encoding.UTF8.GetBytes(
            $"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\">" +
            $"<rect width=\"{width}\" height=\"{height}\" fill=\"{fill}\"/></svg>");

    public static async Task<byte[]> PaddedPngAsync(uint width, uint height, Bgra color, int totalLength)
    {
        var png = await PngAsync(width, height, color);
        var padded = new byte[totalLength];
        png.CopyTo(padded, 0);
        return padded;
    }

    public static async Task<byte[]> AnimatedGifAsync(uint size, Bgra first, Bgra second)
    {
        using var stream = new InMemoryRandomAccessStream();
        var encoder = await BitmapEncoder.CreateAsync(BitmapEncoder.GifEncoderId, stream);
        await encoder.BitmapContainerProperties.SetPropertiesAsync(new BitmapPropertySet
        {
            ["/appext/Application"] = new BitmapTypedValue(Encoding.ASCII.GetBytes("NETSCAPE2.0"), Windows.Foundation.PropertyType.UInt8Array),
            ["/appext/Data"] = new BitmapTypedValue(new byte[] { 3, 1, 0, 0, 0 }, Windows.Foundation.PropertyType.UInt8Array)
        });

        var frames = new[] { first, second };
        for (var i = 0; i < frames.Length; i++)
        {
            encoder.SetPixelData(BitmapPixelFormat.Bgra8, BitmapAlphaMode.Ignore, size, size, 96, 96, Fill(size, size, frames[i]));
            await encoder.BitmapProperties.SetPropertiesAsync(new BitmapPropertySet
            {
                ["/grctlext/Delay"] = new BitmapTypedValue((ushort)20, Windows.Foundation.PropertyType.UInt16)
            });
            if (i < frames.Length - 1)
            {
                await encoder.GoToNextFrameAsync();
            }
        }

        await encoder.FlushAsync();
        return await ReadAllAsync(stream);
    }

    public static WriteableBitmap AssertRaster(ImageSource? source, int width, int height, string context)
    {
        Assert.IsInstanceOfType<WriteableBitmap>(source, $"{context}: expected a decoded raster.");
        var bitmap = (WriteableBitmap)source;
        Assert.AreEqual(width, bitmap.PixelWidth, $"{context}: decoded pixel width.");
        Assert.AreEqual(height, bitmap.PixelHeight, $"{context}: decoded pixel height.");
        Assert.AreEqual((uint)(width * height * 4), bitmap.PixelBuffer.Length, $"{context}: decoded buffer length.");
        Assert.IsTrue((long)bitmap.PixelWidth * bitmap.PixelHeight <= MaxRasterDecodePixels, $"{context}: pixel budget.");
        Assert.IsTrue(bitmap.PixelBuffer.Length <= MaxRasterDecodeBytes, $"{context}: byte budget.");
        return bitmap;
    }

    public static void AssertColor(WriteableBitmap bitmap, Bgra expected, string context)
    {
        var pixels = bitmap.PixelBuffer.ToArray();
        foreach (var index in new[] { 0, pixels.Length / 8 * 4, pixels.Length - 4 })
        {
            var actual = new Bgra(pixels[index], pixels[index + 1], pixels[index + 2], pixels[index + 3]);
            Assert.AreEqual(expected, actual, $"{context}: pixel at byte {index}.");
        }
    }

    public static void AssertNaturalSize(ImageSource? source, double width, double height, string context)
    {
        Assert.IsNotNull(source, $"{context}: expected an image.");
        Assert.IsTrue(ImageExSourceMetadata.TryGetNaturalSize(source, out var size), $"{context}: natural size is missing.");
        Assert.AreEqual(new Size(width, height), size, $"{context}: natural size.");
    }

    private static async Task<byte[]> EncodePngAsync(uint width, uint height, Bgra color)
    {
        using var stream = new InMemoryRandomAccessStream();
        var encoder = await BitmapEncoder.CreateAsync(BitmapEncoder.PngEncoderId, stream);
        encoder.SetPixelData(BitmapPixelFormat.Bgra8, BitmapAlphaMode.Premultiplied, width, height, 96, 96, Fill(width, height, color));
        await encoder.FlushAsync();
        return await ReadAllAsync(stream);
    }

    private static byte[] Fill(uint width, uint height, Bgra color)
    {
        var pixels = new byte[checked((int)(width * height * 4))];
        for (var i = 0; i < pixels.Length; i += 4)
        {
            pixels[i] = color.B;
            pixels[i + 1] = color.G;
            pixels[i + 2] = color.R;
            pixels[i + 3] = color.A;
        }

        return pixels;
    }

    private static async Task<byte[]> ReadAllAsync(InMemoryRandomAccessStream stream)
    {
        stream.Seek(0);
        var bytes = new byte[checked((int)stream.Size)];
        using var input = stream.AsStreamForRead();
        await input.ReadExactlyAsync(bytes);
        return bytes;
    }
}
