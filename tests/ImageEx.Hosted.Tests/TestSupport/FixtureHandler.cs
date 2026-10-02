using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;

namespace ImageEx.Hosted.Tests;

internal static class TestUris
{
    public static Uri Next(string fileName) => new($"https://imageex.test/{Guid.NewGuid():N}/{fileName}");
}

/// <summary>
/// Serves fixed responses by absolute URI. A gated route holds its first request until the test releases it.
/// </summary>
internal sealed class FixtureHandler : HttpMessageHandler
{
    private readonly ConcurrentDictionary<string, Route> _routes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ScriptedRoute> _scripted = new(StringComparer.Ordinal);

    public FixtureHandler Serve(Uri uri, byte[] body, string contentType = "image/png", HttpStatusCode status = HttpStatusCode.OK)
    {
        _routes[uri.AbsoluteUri] = new Route(body, contentType, status, gate: null);
        return this;
    }

    public FixtureHandler ServeStatus(Uri uri, HttpStatusCode status) => Serve(uri, Array.Empty<byte>(), status: status);

    /// <summary>
    /// Returns headers at once, then a PNG body that follows one script per request. The last script repeats.
    /// </summary>
    public FixtureHandler ServeScripted(Uri uri, byte[] body, params BodyScript[] attempts)
    {
        ArgumentOutOfRangeException.ThrowIfZero(attempts.Length);
        _scripted[uri.AbsoluteUri] = new ScriptedRoute(body, attempts);
        return this;
    }

    public Gate ServeGated(Uri uri, byte[] body, string contentType = "image/png", HttpStatusCode status = HttpStatusCode.OK)
    {
        var gate = new Gate();
        _routes[uri.AbsoluteUri] = new Route(body, contentType, status, gate);
        return gate;
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        if (_scripted.TryGetValue(request.RequestUri!.AbsoluteUri, out var scripted))
        {
            var scriptedContent = new ScriptedContent(scripted.Body, scripted.NextScript());
            scriptedContent.Headers.ContentType = new MediaTypeHeaderValue("image/png");
            return new HttpResponseMessage(HttpStatusCode.OK) { Content = scriptedContent };
        }

        if (!_routes.TryGetValue(request.RequestUri!.AbsoluteUri, out var route))
        {
            return new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new ByteArrayContent(Array.Empty<byte>()) };
        }

        var gate = route.TakeGate();
        if (gate != null)
        {
            gate.MarkStarted();
            await gate.Released.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        var content = new ByteArrayContent(route.Body);
        content.Headers.ContentType = new MediaTypeHeaderValue(route.ContentType);
        return new HttpResponseMessage(route.Status) { Content = content };
    }

    private sealed class ScriptedRoute(byte[] body, BodyScript[] attempts)
    {
        private int _next = -1;

        public byte[] Body { get; } = body;

        public BodyScript NextScript() => attempts[Math.Min(Interlocked.Increment(ref _next), attempts.Length - 1)];
    }

    private sealed class Route(byte[] body, string contentType, HttpStatusCode status, Gate? gate)
    {
        private Gate? _gate = gate;

        public byte[] Body { get; } = body;

        public string ContentType { get; } = contentType;

        public HttpStatusCode Status { get; } = status;

        public Gate? TakeGate() => Interlocked.Exchange(ref _gate, null);
    }
}

internal sealed class Gate
{
    private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Started => _started.Task;

    public Task Released => _released.Task;

    public void MarkStarted() => _started.TrySetResult();

    public void Release() => _released.TrySetResult();

    public Task WaitUntilStartedAsync() => TestWait.ForTaskAsync(Started, "the gated request to reach the transport");
}

/// <summary>
/// Fails every request, so only the encoded disk cache can supply an image.
/// </summary>
internal sealed class UnavailableHandler : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        => Task.FromException<HttpResponseMessage>(new HttpRequestException("Transport is unavailable in this test."));
}

internal sealed class TempCacheDirectory : IDisposable
{
    public TempCacheDirectory()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ixh", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
    }

    public string Path { get; }

    public void Dispose()
    {
        for (var attempt = 0; attempt < 5; attempt++)
        {
            try
            {
                if (Directory.Exists(Path))
                {
                    Directory.Delete(Path, recursive: true);
                }

                return;
            }
            catch (IOException) when (attempt < 4)
            {
                Thread.Sleep(50);
            }
            catch (UnauthorizedAccessException) when (attempt < 4)
            {
                Thread.Sleep(50);
            }
            catch (IOException)
            {
                return;
            }
            catch (UnauthorizedAccessException)
            {
                return;
            }
        }
    }
}
