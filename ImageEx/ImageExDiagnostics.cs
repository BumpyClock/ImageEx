using Microsoft.UI.Xaml.Media;
using Microsoft.UI.Xaml.Media.Imaging;
using System.Diagnostics;

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
            ImageSource previousSource,
            ImageSource nextSource,
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
            if (ShouldSample(totalChanges) || nextDecodedBytes >= 8 * 1024 * 1024)
            {
                LogSnapshot($"attach:{control.GetType().Name}");
            }
        }

        internal static void RecordAttachedBytesChanged(ImageExBase control, long previousBytes, long nextBytes)
        {
            var delta = nextBytes - previousBytes;
            if (delta == 0)
            {
                return;
            }

            var activeBytes = Interlocked.Add(ref s_activeAttachedDecodedBytes, delta);
            UpdatePeakActiveAttachedDecodedBytes(activeBytes);
            Interlocked.Increment(ref s_imageOpenedCount);

            if (ShouldSample(Interlocked.Read(ref s_imageOpenedCount)) || nextBytes >= 8 * 1024 * 1024)
            {
                LogSnapshot($"opened:{control.GetType().Name}");
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
    }
}
