// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#nullable enable

using System.IO;
using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Media.Imaging;
using System.Diagnostics;
using System.Runtime.InteropServices;

namespace ImageEx
{
    public readonly record struct ImageExDiagnosticsSnapshot(
        long AttachCount,
        long DetachCount,
        long ActiveAttachedSources,
        long ActiveAttachedDecodedBytes,
        long PeakActiveAttachedDecodedBytes,
        long ImageOpenedCount,
        long OffscreenAttachedObservations,
        long OffscreenDetachCount,
        long LazyDeferredCount,
        long HttpFallbackCount,
        long HttpFallbackDecodedBytes,
        long BaseBitmapCreatedCount,
        bool DisableHttpImages,
        bool DisableHttpFallback);

    public static class ImageExDiagnostics
    {
        private const int SampleInterval = 25;
        private const long LargeDecodedImageBytes = 4L * 1024 * 1024;
        private static readonly bool s_disableHttpImages = IsEnabled("DIGESTS_IMAGEEX_DISABLE_HTTP_IMAGES");
        private static readonly bool s_disableHttpFallback = IsEnabled("DIGESTS_IMAGEEX_DISABLE_HTTP_FALLBACK");
        private static long s_attachCount;
        private static long s_detachCount;
        private static long s_activeAttachedSources;
        private static long s_activeAttachedDecodedBytes;
        private static long s_peakActiveAttachedDecodedBytes;
        private static long s_imageOpenedCount;
        private static long s_offscreenAttachedObservations;
        private static long s_offscreenDetachCount;
        private static long s_lazyDeferredCount;
        private static long s_httpFallbackCount;
        private static long s_httpFallbackDecodedBytes;
        private static long s_baseBitmapCreatedCount;

        public static bool DisableHttpImages => s_disableHttpImages;

        public static bool DisableHttpFallback => s_disableHttpFallback;

        public static ImageExDiagnosticsSnapshot CaptureSnapshot()
        {
            return new ImageExDiagnosticsSnapshot(
                Interlocked.Read(ref s_attachCount),
                Interlocked.Read(ref s_detachCount),
                Interlocked.Read(ref s_activeAttachedSources),
                Interlocked.Read(ref s_activeAttachedDecodedBytes),
                Interlocked.Read(ref s_peakActiveAttachedDecodedBytes),
                Interlocked.Read(ref s_imageOpenedCount),
                Interlocked.Read(ref s_offscreenAttachedObservations),
                Interlocked.Read(ref s_offscreenDetachCount),
                Interlocked.Read(ref s_lazyDeferredCount),
                Interlocked.Read(ref s_httpFallbackCount),
                Interlocked.Read(ref s_httpFallbackDecodedBytes),
                Interlocked.Read(ref s_baseBitmapCreatedCount),
                DisableHttpImages,
                DisableHttpFallback);
        }

        public static void LogSnapshot(string context)
        {
            LogSnapshot(context, CaptureSnapshot());
        }

        public static long EstimateDecodedBytes(ImageSource source)
        {
            return source is BitmapSource { PixelWidth: > 0, PixelHeight: > 0 } bitmap
                ? (long)bitmap.PixelWidth * bitmap.PixelHeight * 4
                : 0;
        }

        internal static void RecordAttach(
            ImageExBase control,
            ImageSource? previousSource,
            ImageSource? nextSource,
            long previousDecodedBytes,
            long nextDecodedBytes)
        {
            if (ReferenceEquals(previousSource, nextSource) && previousDecodedBytes == nextDecodedBytes)
            {
                return;
            }

            if (previousSource != null)
            {
                Interlocked.Increment(ref s_detachCount);
                Interlocked.Decrement(ref s_activeAttachedSources);
                Interlocked.Add(ref s_activeAttachedDecodedBytes, -previousDecodedBytes);
            }

            if (nextSource != null)
            {
                Interlocked.Increment(ref s_attachCount);
                Interlocked.Increment(ref s_activeAttachedSources);
                var activeBytes = Interlocked.Add(ref s_activeAttachedDecodedBytes, nextDecodedBytes);
                UpdatePeakActiveAttachedDecodedBytes(activeBytes);
            }

            var totalChanges = Interlocked.Read(ref s_attachCount) + Interlocked.Read(ref s_detachCount);
            var sourceBytes = nextSource != null ? nextDecodedBytes : previousDecodedBytes;
            if (ShouldLogSourceEvent(sourceBytes, totalChanges))
            {
                LogSourceEvent(control, "attach", nextSource ?? previousSource, sourceBytes);
            }
        }

        internal static void RecordAttachedBytesChanged(ImageExBase control, ImageSource? source, long previousBytes, long nextBytes)
        {
            var delta = nextBytes - previousBytes;
            if (delta == 0)
            {
                return;
            }

            var activeBytes = Interlocked.Add(ref s_activeAttachedDecodedBytes, delta);
            UpdatePeakActiveAttachedDecodedBytes(activeBytes);
            var openedCount = Interlocked.Increment(ref s_imageOpenedCount);

            if (ShouldLogSourceEvent(nextBytes, openedCount))
            {
                LogSourceEvent(control, "opened", source, nextBytes);
            }
        }

        internal static void RecordLazyDeferred(ImageExBase control)
        {
            var count = Interlocked.Increment(ref s_lazyDeferredCount);
            if (ShouldSample(count))
            {
                LogSnapshot($"lazy-deferred:{control.GetType().Name}");
            }
        }

        internal static void RecordOffscreenAttached(ImageExBase control)
        {
            var count = Interlocked.Increment(ref s_offscreenAttachedObservations);
            if (count == 1 || ShouldSample(count))
            {
                LogSnapshot($"offscreen-attached:{control.GetType().Name}");
            }
        }

        internal static void RecordOffscreenDetach(ImageExBase control)
        {
            var count = Interlocked.Increment(ref s_offscreenDetachCount);
            if (count == 1 || ShouldSample(count))
            {
                LogSnapshot($"offscreen-detach:{control.GetType().Name}");
            }
        }

        internal static void RecordHttpFallback(Uri uri, int decodeWidth, int decodeHeight, DecodePixelType decodeType)
        {
            var count = Interlocked.Increment(ref s_httpFallbackCount);
            if (count == 1 || ShouldSample(count))
            {
                Debug.WriteLine(
                    "[ImageExDiagnostics] " +
                    $"Context=http-fallback " +
                    $"Host={uri.Host} " +
                    $"DecodeWidth={decodeWidth} " +
                    $"DecodeHeight={decodeHeight} " +
                    $"DecodeType={decodeType} " +
                    $"HttpFallbacks={count} " +
                    $"DisableHttpFallback={DisableHttpFallback}");
                LogSnapshot("http-fallback");
            }
        }

        internal static void RecordHttpFallbackResult(ImageSource source)
        {
            var decodedBytes = EstimateDecodedBytes(source);
            if (decodedBytes > 0)
            {
                Interlocked.Add(ref s_httpFallbackDecodedBytes, decodedBytes);
            }
        }

        internal static void RecordBaseBitmapCreated(Uri uri, int decodeWidth, int decodeHeight, DecodePixelType decodeType)
        {
            var count = Interlocked.Increment(ref s_baseBitmapCreatedCount);
            if (uri.IsAbsoluteUri && uri.IsHttpUri())
            {
                Debug.WriteLine(
                    "[ImageExDiagnostics] " +
                    $"Context=base-bitmap-http " +
                    $"Host={uri.Host} " +
                    $"DecodeWidth={decodeWidth} " +
                    $"DecodeHeight={decodeHeight} " +
                    $"DecodeType={decodeType} " +
                    $"BaseBitmapCreated={count}");
            }
        }

        private static void LogSnapshot(string context, ImageExDiagnosticsSnapshot snapshot)
        {
            var cache = Cache.ImageExCacheManager.CaptureDiagnosticsSnapshot();
            var memory = CaptureMemory();
            Debug.WriteLine(
                "[ImageExDiagnostics] " +
                $"Context={context} " +
                $"AttachCount={snapshot.AttachCount} " +
                $"DetachCount={snapshot.DetachCount} " +
                $"ActiveAttachedSources={snapshot.ActiveAttachedSources} " +
                $"ActiveAttachedDecodedMB={ToMegabytes(snapshot.ActiveAttachedDecodedBytes):F1} " +
                $"PeakActiveAttachedDecodedMB={ToMegabytes(snapshot.PeakActiveAttachedDecodedBytes):F1} " +
                $"ImageOpenedCount={snapshot.ImageOpenedCount} " +
                $"OffscreenAttachedObservations={snapshot.OffscreenAttachedObservations} " +
                $"OffscreenDetachCount={snapshot.OffscreenDetachCount} " +
                $"LazyDeferredCount={snapshot.LazyDeferredCount} " +
                $"HttpFallbackCount={snapshot.HttpFallbackCount} " +
                $"HttpFallbackDecodedMB={ToMegabytes(snapshot.HttpFallbackDecodedBytes):F1} " +
                $"BaseBitmapCreatedCount={snapshot.BaseBitmapCreatedCount} " +
                $"CacheInitialized={cache.IsInitialized} " +
                $"SmallDecodedCacheEntries={cache.SmallDecodedImageCacheEntries} " +
                $"SmallDecodedCacheMB={ToMegabytes(cache.SmallDecodedImageCacheBytes):F1} " +
                $"InFlightDownloads={cache.InFlightDownloads} " +
                $"CacheWriteLockCount={cache.CacheWriteLockCount} " +
                $"CacheWriteLockRemovals={cache.CacheWriteLockRemovals} " +
                $"DiskCacheEntryCount={cache.DiskCacheEntryCount} " +
                $"DiskCacheMB={ToMegabytes(cache.DiskCacheBytes):F1} " +
                $"DisableHttpImages={snapshot.DisableHttpImages} " +
                $"DisableHttpFallback={snapshot.DisableHttpFallback} " +
                $"ManagedMB={memory.ManagedMB:F1} " +
                $"WorkingSetMB={memory.WorkingSetMB:F1} " +
                $"PrivateMB={memory.PrivateMB:F1}");
        }

        private static void UpdatePeakActiveAttachedDecodedBytes(long activeBytes)
        {
            while (true)
            {
                var currentPeak = Interlocked.Read(ref s_peakActiveAttachedDecodedBytes);
                if (activeBytes <= currentPeak)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref s_peakActiveAttachedDecodedBytes, activeBytes, currentPeak) == currentPeak)
                {
                    return;
                }
            }
        }

        private static bool ShouldSample(long count)
        {
            return count > 0 && count % SampleInterval == 0;
        }

        private static bool ShouldLogSourceEvent(long decodedBytes, long count)
        {
            return decodedBytes >= LargeDecodedImageBytes || ShouldSample(count);
        }

        private static bool IsEnabled(string name)
        {
            var value = Environment.GetEnvironmentVariable(name);
            return string.Equals(value, "1", StringComparison.Ordinal)
                || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase);
        }

        private static (double ManagedMB, double WorkingSetMB, double PrivateMB) CaptureMemory()
        {
            var managedBytes = GC.GetTotalMemory(false);
            var workingSetBytes = 0L;
            var privateBytes = 0L;

            try
            {
                using var process = Process.GetCurrentProcess();
                workingSetBytes = process.WorkingSet64;
                privateBytes = process.PrivateMemorySize64;
            }
            catch
            {
                // Best-effort diagnostic only.
            }

            return (ToMegabytes(managedBytes), ToMegabytes(workingSetBytes), ToMegabytes(privateBytes));
        }

        private static double ToMegabytes(long bytes)
        {
            return bytes / (1024.0 * 1024.0);
        }

        private static void LogSourceEvent(ImageExBase control, string context, ImageSource? source, long sourceDecodedBytes)
        {
            try
            {
                if (control.DispatcherQueue is { HasThreadAccess: false })
                {
                    return;
                }

                var bitmap = source as BitmapImage;
                var pixelWidth = 0;
                var pixelHeight = 0;
                if (source is BitmapSource bitmapSource)
                {
                    pixelWidth = bitmapSource.PixelWidth;
                    pixelHeight = bitmapSource.PixelHeight;
                }
                var decodeWidth = bitmap?.DecodePixelWidth ?? 0;
                var decodeHeight = bitmap?.DecodePixelHeight ?? 0;
                var decodeType = bitmap?.DecodePixelType.ToString() ?? string.Empty;
                var sourceType = source?.GetType().Name ?? string.Empty;
                var uri = TryGetSourceUri(source);
                var activeDecodedBytes = Interlocked.Read(ref s_activeAttachedDecodedBytes);

                Debug.WriteLine(
                    "[ImageExDiagnostics] " +
                    $"Context={context} " +
                    $"ControlType={control.GetType().Name} " +
                    $"SourceType={sourceType} " +
                    $"UriScheme={uri?.Scheme ?? string.Empty} " +
                    $"UriHost={uri?.Host ?? string.Empty} " +
                    $"UriExtension={GetUriExtension(uri)} " +
                    $"PixelWidth={pixelWidth} " +
                    $"PixelHeight={pixelHeight} " +
                    $"DecodePixelWidth={decodeWidth} " +
                    $"DecodePixelHeight={decodeHeight} " +
                    $"DecodePixelType={decodeType} " +
                    $"IsLoaded={control.IsLoaded} " +
                    $"EnableLazyLoading={control.EnableLazyLoading} " +
                    $"InViewport={control.IsInViewport} " +
                    $"SourceDecodedMB={ToMegabytes(sourceDecodedBytes):F1} " +
                    $"ActiveDecodedMB={ToMegabytes(activeDecodedBytes):F1}");
            }
            catch (Exception ex) when (ex is COMException or InvalidOperationException)
            {
                // Diagnostic logging must not fail image lifecycle paths.
            }
        }

        private static Uri? TryGetSourceUri(ImageSource? source)
        {
            if (source == null)
            {
                return null;
            }

            return source switch
            {
                BitmapImage bitmap when ImageExDeferredBitmapSourceRegistry.TryGetDeferredUriSource(bitmap, out var uri) => uri,
                SvgImageSource svg => svg.UriSource,
                _ => null
            };
        }

        private static string GetUriExtension(Uri? uri)
        {
            if (uri == null)
            {
                return string.Empty;
            }

            return Path.GetExtension(uri.AbsolutePath);
        }
    }
}
