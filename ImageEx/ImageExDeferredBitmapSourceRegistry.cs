// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#nullable enable

using System.Runtime.CompilerServices;

namespace ImageEx;

internal static class ImageExDeferredBitmapSourceRegistry
{
    private sealed class DeferredUriSource
    {
        public DeferredUriSource(Uri uri)
        {
            Uri = uri;
        }

        public Uri Uri { get; }
    }

    private static readonly ConditionalWeakTable<BitmapImage, DeferredUriSource> s_deferredUriSources = new();

    internal static BitmapImage CreateDeferredBitmapImage(
        Uri uri,
        int decodeWidth,
        int decodeHeight,
        DecodePixelType decodeType,
        BitmapCreateOptions createOptions)
    {
        var bitmap = new BitmapImage
        {
            DecodePixelType = decodeType,
            CreateOptions = createOptions
        };

        if (decodeWidth > 0)
        {
            bitmap.DecodePixelWidth = decodeWidth;
        }

        if (decodeHeight > 0)
        {
            bitmap.DecodePixelHeight = decodeHeight;
        }

        s_deferredUriSources.Add(bitmap, new DeferredUriSource(uri));
        return bitmap;
    }

    internal static bool TryApplyDeferredUriSource(ImageSource source)
    {
        return source is BitmapImage bitmap && TryApplyDeferredUriSource(bitmap);
    }

    internal static bool TryApplyDeferredUriSource(BitmapImage bitmap)
    {
        if (bitmap.UriSource != null)
        {
            s_deferredUriSources.Remove(bitmap);
            return false;
        }

        if (!s_deferredUriSources.TryGetValue(bitmap, out var deferredUriSource))
        {
            return false;
        }

        bitmap.UriSource = deferredUriSource.Uri;
        s_deferredUriSources.Remove(bitmap);
        return true;
    }

    internal static bool TryGetDeferredUriSource(ImageSource source, out Uri? uri)
    {
        if (source is BitmapImage bitmap && TryGetDeferredUriSource(bitmap, out uri))
        {
            return true;
        }

        if (source is SvgImageSource svg)
        {
            uri = svg.UriSource;
            return uri != null;
        }

        uri = null;
        return false;
    }

    internal static bool TryGetDeferredUriSource(BitmapImage bitmap, out Uri? uri)
    {
        if (bitmap.UriSource != null)
        {
            uri = bitmap.UriSource;
            return true;
        }

        if (s_deferredUriSources.TryGetValue(bitmap, out var deferredUriSource))
        {
            uri = deferredUriSource.Uri;
            return true;
        }

        uri = null;
        return false;
    }
}
