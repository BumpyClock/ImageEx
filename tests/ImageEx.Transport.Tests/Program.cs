using System.Net;
using System.Net.Http.Headers;
using ImageEx;
using ImageEx.Cache;

var cases = new (string Name, ImageRequestMode Mode, int Length, bool Chunked, string MediaType, bool Success)[]
{
    ("Cached success", ImageRequestMode.Cached, 1024, false, "image/png", true),
    ("Cached declared limit", ImageRequestMode.Cached, 8 * 1024 * 1024 + 1, false, "image/png", false),
    ("Cached chunked limit", ImageRequestMode.Cached, 8 * 1024 * 1024 + 1, true, "image/png", false),
    ("Original accepts larger raster", ImageRequestMode.Original, 9 * 1024 * 1024, true, "image/png", true),
    ("Original declared limit", ImageRequestMode.Original, 32 * 1024 * 1024 + 1, false, "image/png", false),
    ("Original chunked limit", ImageRequestMode.Original, 32 * 1024 * 1024 + 1, true, "image/png", false),
    ("Original SVG keeps smaller limit", ImageRequestMode.Original, 8 * 1024 * 1024 + 1, true, "image/svg+xml", false),
    ("SVG content type", ImageRequestMode.Original, 1024, true, "image/svg+xml", true)
};
foreach (var test in cases)
{
    using var client = new HttpClient(new FakeHandler(_ =>
    {
        HttpContent content = test.Chunked
            ? new UnknownLengthContent(new byte[test.Length])
            : new ByteArrayContent(new byte[test.Length]);
        content.Headers.ContentType = new MediaTypeHeaderValue(test.MediaType);
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content });
    }));
    var result = await Resolve(test.Mode, client);
    Assert((result.Image != null) == test.Success && !result.WasCacheHit, test.Name);
    if (test.Success)
    {
        Assert(result.Image!.Bytes == test.Length && result.Image.IsSvg == (test.MediaType == "image/svg+xml"), test.Name + " decode input");
    }
}

using (var client = new HttpClient(new FakeHandler(_ => Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound)))))
{
    Assert((await Resolve(ImageRequestMode.Cached, client)).Image == null, "HTTP failure permits fallback");
}
using (var cancellation = new CancellationTokenSource())
using (var client = new HttpClient(new FakeHandler(async token =>
{
    cancellation.Cancel();
    await Task.Delay(Timeout.Infinite, token);
    throw new Exception("Unreachable");
})))
{
    try
    {
        await Resolve(ImageRequestMode.Original, client, cancellation.Token);
        throw new Exception("FAIL: cancellation was swallowed.");
    }
    catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
    {
        Console.WriteLine("PASS: active request cancellation");
    }
}
await CopyTests.RunAsync();
Console.WriteLine("Passed 10 uncached transport scenarios and 5 copy scenarios. WinUI decode is stubbed.");

static Task<ImageExCacheManager.CacheResult> Resolve(ImageRequestMode mode, HttpClient client, CancellationToken token = default)
    => ImageExCacheManager.GetUncachedImageAsync(new(new Uri("https://test.invalid/source"), mode),
        64, 64, DecodePixelType.Physical, token, httpClient: client);

static void Assert(bool value, string name)
{
    if (!value) throw new Exception("FAIL: " + name);
    Console.WriteLine("PASS: " + name);
}

sealed class FakeHandler(Func<CancellationToken, Task<HttpResponseMessage>> response) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        => response(cancellationToken);
}

sealed class UnknownLengthContent(byte[] bytes) : HttpContent
{
    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        => stream.WriteAsync(bytes).AsTask();
    protected override bool TryComputeLength(out long length) { length = 0; return false; }
    protected override Task<Stream> CreateContentReadStreamAsync()
        => Task.FromResult<Stream>(new MemoryStream(bytes, writable: false));
}
