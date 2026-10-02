# ImageEx: extended image control for UWP and WinUI apps

The ImageEx control extends the platform Image control. It loads source images asynchronously and shows a loading indicator during the load. It saves downloaded images in the app's local cache so later loads use fewer resources and finish sooner.

Cached candidates accept at most 8 MiB of source bytes. Original raster candidates accept at most 32 MiB. SVG candidates retain the 8 MiB limit. Both routes reject oversized declared responses and stop chunked responses at the limit.

Raster byte and file decodes use at most 2,097,152 physical pixels, or 8 MiB of BGRA data.
Logical decode dimensions use the supplied DPI scale, clamped to 0.5 through 4.0, before the limits apply.
Physical decode dimensions do not receive this scale.
With no explicit dimensions, the fallback width receives the DPI scale once.
A missing axis follows the natural aspect ratio of the source.
A raster decode never exceeds the natural dimensions of its source, and the pixel limit then applies to the result. There is no enlargement exception.
When the requested box exceeds the source on either axis, one scale reduces both axes. The box keeps its requested aspect ratio, even when the source differs.
The pixel limit does not bound codec buffers, SVG images, or native URI decodes.

Debug builds collect detailed image and cache logs with process-memory samples.
Release builds omit these log calls and their argument evaluation.
Diagnostic counters and the `CaptureSnapshot` API remain available in both configurations.

The project explicitly declares `win-x86`, `win-x64`, and `win-arm64` runtime identifiers so NuGet restore does not depend on package build imports.
The committed lockfile includes all three runtime targets. In PowerShell, use `dotnet restore ImageEx\ImageEx.csproj --locked-mode -p:Platform=x64` to require the recorded dependency graph.

This package contains a separate ImageEx control from the Windows Community Toolkit 7.x package.
Since this control was removed in Toolkit version 8.0, this package was created containing only this control and having no dependencies.
Originally developed by Microsoft.Toolkit
https://github.com/CommunityToolkit/WindowsCommunityToolkit

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL FOURSOFT BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

## Ordered image requests

`ImageRequest` tries candidates in order until one succeeds. `ImageRequestMode` selects the source byte limit.
Decoded memory entries remain separate for cached and original candidates, even with the same URI and decode dimensions.
Managed image downloads allow 30 seconds for response headers.
Each pending body read has a separate 30-second idle timeout. The timeout restarts for every read.
Opening the body stream also has a 30-second timeout. For original downloads, this budget is shared with the response headers.
Destination writes, decoding, and queueing for a download slot do not count against these timeouts.
A transfer can exceed 30 seconds in total if each read completes within its timeout. There is no limit on the total transfer duration.
Cached downloads make up to three attempts when the headers or the body time out. The delays between attempts are 200 ms and 400 ms.
A body operation counts as timed out when its idle timeout fires, whatever error the stream then reports.
Network errors without an HTTP status also retry. Other body errors end the load without another attempt.
After the last attempt, the load fails and the URL enters the ten-minute failure backoff.
A cached download keeps its download slot through all attempts and both delays.
A thumbnail body that stalls on every attempt holds its slot for about three idle timeouts plus the delays. At the default timeout, that is about 90.6 seconds. The load then fails and releases the slot.
Original downloads and downloads with `EnableDiskCache = false` make one attempt.
In an ordered request, a candidate that fails or times out advances to the next candidate.
A download releases its download slot when it succeeds, fails, times out, or is canceled, so a stalled body cannot hold a slot indefinitely.
Canceling a load or replacing its source stops the request as cancellation, not as a timeout.

Current disk entries use one URI source key, with separate `original-` keys for original-mode bytes.
Legacy decode-parameter entries are no longer scanned or migrated on source misses.
A URL with only a legacy entry downloads again and cannot use that entry offline if the download fails.
Legacy files and metadata remain until the existing age or size cleanup removes them.

With `EnableDiskCache = false`, ordered requests retain candidate order and mode through a bounded memory download and decode path.
This path does not create the disk cache manager or access cache files and metadata.
HTTP or decode failures advance to the next candidate. Cancellation stops the request.

SVG response media types use a case-insensitive comparison.
SVG metadata accepts a `DOCTYPE` declaration without external resource resolution or DTD entity expansion.

## Completion and natural size

A current direct-URI load that resolves without an image enters the failed visual state and raises `ImageExFailed` once, as ordered requests already do. Canceled, replaced, and unloaded requests stay silent. The `DIGESTS_IMAGEEX_DISABLE_HTTP_IMAGES` diagnostic switch suppresses HTTP loads without a failure event on direct loads.

`ImageExBase.TryGetNaturalSize(out Size)` reports the natural pixel size of the image currently attached to the control, from cache metadata when present and otherwise from the decoded bitmap. A bitmap the platform resized while decoding (a `DecodePixelWidth` or `DecodePixelHeight` on a direct load without the managed cache) has no natural size, so the method returns false. It returns false until an image is attached and after the source clears, so a late completion cannot change a replacement image dimensions.

## Regression checks

Run `dotnet run --project tests/ImageEx.Metadata.Tests/ImageEx.Metadata.Tests.csproj` for SVG metadata checks.
Run `dotnet run --project tests/ImageEx.Transport.Tests/ImageEx.Transport.Tests.csproj` for bounded transport checks.
These projects link production helpers. Transport checks use a decoder stub and do not validate WinUI playback or layout.
Run `dotnet build ImageEx/ImageEx.csproj -p:Platform=x64` to compile the WinUI library.

Run `pwsh -NoProfile -File scripts/run-winui-hosted-tests.ps1` for the packaged WinUI checks in `tests/ImageEx.Hosted.Tests`.
These tests host the production control and cache manager in a window and decode real images.
They cover direct-load completion, natural size, the raster decode ceiling and budgets, and `DisableHttpImages` suppression.
They also cover response bodies that stall after their headers. These checks include idle expiry, retries, failure backoff, download slot release, shared-waiter cancellation, candidate fallback, and original temporary-file cleanup.
These tests use a short internal idle timeout. No test waits for the real 30-second timeout.
The command needs Windows 11 with an interactive desktop session, PowerShell 7, the .NET 10 SDK, Visual Studio with `vstest.console.exe`, and the Windows App SDK 2.x runtime.
It builds the x64 test package and registers it as a development package named `ImageEx.Hosted.Tests`.
It keeps each run's raw results in `artifacts/test-results` and validates that run's own result.
It fails when the result is missing or incomplete, when any test is skipped or fails, or when a test method reports more than one result.
After a run passes, it copies the result to `artifacts/test-results/imageex-hosted.trx`.
If the build rewrites `ImageEx/packages.lock.json`, the command puts back the committed content and prints a notice.
