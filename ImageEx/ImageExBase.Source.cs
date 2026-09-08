// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// ReSharper disable AsyncVoidMethod
namespace ImageEx
{
    /// <summary>
    /// Base implementation for ImageEx.
    /// </summary>
    public partial class ImageExBase
    {
        /// <summary>
        /// Identifies the <see cref="Source"/> dependency property.
        /// </summary>
        public static readonly DependencyProperty SourceProperty = DependencyProperty.Register(nameof(Source), typeof(object), typeof(ImageExBase), new PropertyMetadata(null, SourceChanged));

        // Tracks new requests so the control can cancel custom cache loads.
        private CancellationTokenSource _tokenSource;
        private long _sourceRequestVersion;

        private object _lazyLoadingSource;
        private bool _loadedViewportCheckQueued;

        /// <summary>
        /// Gets or sets the source used by the image.
        /// </summary>
        public object Source
        {
            get { return GetValue(SourceProperty); }
            set { SetValue(SourceProperty, value); }
        }

        internal bool IsInViewport => _isInViewport;

        private void OnImageExUnloaded(object sender, RoutedEventArgs e)
        {
            // A queued Unloaded event can arrive after the control has loaded again.
            if (IsLoaded)
            {
                return;
            }

            _isInViewport = false;
            _lastEffectiveViewport = null;
            Interlocked.Increment(ref _viewportStateGeneration);
            CancelPendingOffscreenDetach();
            CleanupTokenSource();

            if (_currentImageSource != null)
            {
                AttachSource(null);
            }
        }

        private void OnImageExLoaded(object sender, RoutedEventArgs e)
        {
            if (_loadedViewportCheckQueued)
            {
                return;
            }

            // InlineUIContainer can raise Loaded before its viewport geometry settles.
            _loadedViewportCheckQueued = true;
            var dispatcherQueue = ImageDispatcherQueue;
            if (dispatcherQueue == null || !dispatcherQueue.TryEnqueue(() =>
            {
                _loadedViewportCheckQueued = false;
                RefreshLoadedSource();
            }))
            {
                _loadedViewportCheckQueued = false;
                System.Diagnostics.Debug.WriteLine("[ImageEx] Dispatcher rejected the initial viewport check.");
            }
        }

        private void RefreshLoadedSource()
        {
            if (!IsLoaded || Source == null)
            {
                return;
            }

            if (EnableLazyLoading)
            {
                InvalidateLazyLoading();
                if (!_isInViewport)
                {
                    return;
                }
            }

            RestartSourceIfNeeded();
        }

        private void RestartSourceIfNeeded()
        {
            if (!IsInitialized
                || Source == null
                || _tokenSource != null
                || HasAttachedSource())
            {
                return;
            }

            SetSource(Source);
        }

        private bool HasAttachedSource()
        {
            return Image switch
            {
                Image image => image.Source != null,
                ImageBrush brush => brush.ImageSource != null,
                _ => false
            };
        }

        private bool HasCurrentRequest()
        {
            var tokenSource = _tokenSource;
            if (tokenSource == null)
            {
                return false;
            }

            try
            {
                return !tokenSource.Token.IsCancellationRequested;
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
        }

        private void CleanupTokenSource()
        {
            var tokenSource = _tokenSource;
            _tokenSource = null;
            
            if (tokenSource != null)
            {
                try
                {
                    if (!tokenSource.Token.IsCancellationRequested)
                    {
                        tokenSource.Cancel();
                    }
                }
                catch (ObjectDisposedException)
                {
                    // The object is disposed. Ignore the callback.
                }
                finally
                {
                    tokenSource.Dispose();
                }
            }
        }

        private static void SourceChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var control = d as ImageExBase;

            if (control == null)
            {
                return;
            }

            if (e.OldValue == null || e.NewValue == null || !e.OldValue.Equals(e.NewValue))
            {
                Interlocked.Increment(ref control._viewportStateGeneration);
                control.CancelPendingOffscreenDetach();
                if (e.NewValue == null)
                {
                    control._lazyLoadingSource = null;
                    control.DetachLazyLoadingHandlers();
                    control.SetSource(e.NewValue);
                }
                else if (!control.EnableLazyLoading)
                {
                    control._lazyLoadingSource = null;
                    control.DetachLazyLoadingHandlers();
                    control.SetSource(e.NewValue);
                }
                else if (control._isInViewport)
                {
                    control._lazyLoadingSource = null;
                    control.AttachLazyLoadingHandlers();
                    control.SetSource(e.NewValue);
                }
                else
                {
                    control.DeferSourceUntilViewport(e.NewValue);
                }
            }
        }

        /// <summary>
        /// Assigns an <see cref="ImageSource"/> to the underlying <see cref="Image"/> in <see cref="ImageExBase"/>.
        /// </summary>
        /// <param name="source"><see cref="ImageSource"/> to assign to the image.</param>
        private void AttachSource(ImageSource source, bool shouldAnimateLoadedState = true)
        {
            var dispatcherQueue = ImageDispatcherQueue;
            if (dispatcherQueue is { HasThreadAccess: false })
            {
                if (!dispatcherQueue.TryEnqueue(() => AttachSource(source, shouldAnimateLoadedState)))
                {
                    return;
                }

                return;
            }

            if (source != null && !IsLoaded)
            {
                return;
            }

            var previousSource = _currentImageSource;
            var nextDecodedBytes = ImageExDiagnostics.EstimateDecodedBytes(source);
            ImageExDiagnostics.RecordAttach(
                this,
                previousSource,
                source,
                _diagnosticAttachedSourceBytes,
                nextDecodedBytes);
            _currentImageSource = source;
            _diagnosticAttachedSourceBytes = source == null ? 0 : nextDecodedBytes;
            _shouldAnimateCurrentImage = source == null || shouldAnimateLoadedState;

            // Setting the source here raises ImageOpened or ImageFailed because the control
            // registers handlers for both events. Call those methods directly only when another
            // path fails before it sets the source.
            if (Image is Image image)
            {
                image.Source = source;
            }
            else if (Image is ImageBrush brush)
            {
                brush.ImageSource = source;
            }

            ImageExDeferredBitmapSourceRegistry.TryApplyDeferredUriSource(source);

            if (source == null)
            {
                if (IsLoaded)
                {
                    VisualStateManager.GoToState(this, UnloadedState, true);
                }
            }
            else if (IsLoaded && (source is BitmapSource { PixelHeight: > 0, PixelWidth: > 0 } ||
                ImageExSourceMetadata.IsDecoded(source)))
            {
                UpdateDiagnosticAttachedSourceBytes();
                VisualStateManager.GoToState(this, LoadedState, _shouldAnimateCurrentImage);
                ImageExOpened?.Invoke(this, new ImageExOpenedEventArgs());
            }
        }

        private void DeferSourceUntilViewport(object source)
        {
            ImageExDiagnostics.RecordLazyDeferred(this);
            _lazyLoadingSource = source;
            AttachLazyLoadingHandlers();
            SetSource(null);
            InvalidateLazyLoading();
        }

        private void UpdateDiagnosticAttachedSourceBytes()
        {
            if (_currentImageSource == null)
            {
                return;
            }

            var decodedBytes = ImageExDiagnostics.EstimateDecodedBytes(_currentImageSource);
            if (decodedBytes == _diagnosticAttachedSourceBytes)
            {
                return;
            }

            ImageExDiagnostics.RecordAttachedBytesChanged(this, _currentImageSource, _diagnosticAttachedSourceBytes, decodedBytes);
            _diagnosticAttachedSourceBytes = decodedBytes;
        }

        private async void SetSource(object source)
        {
            var requestVersion = Interlocked.Increment(ref _sourceRequestVersion);
            CancellationTokenSource requestTokenSource = null;
            var requestToken = CancellationToken.None;
            try
            {
                if (!IsInitialized)
                {
                    return;
                }

                // Cancel the previous request and clear the field. A null _tokenSource
                // means no request is active. An earlier result then fails the
                // IsRequestCurrent guard in LoadImageAsync.
                var previousTokenSource = _tokenSource;
                _tokenSource = null;

                if (source == null)
                {
                    AttachSource(null);
                }

                if (previousTokenSource != null)
                {
                    if (!previousTokenSource.Token.IsCancellationRequested)
                    {
                        await previousTokenSource.CancelAsync();
                    }
                    previousTokenSource.Dispose();
                }

                if (!IsSourceRequestCurrent(requestVersion))
                {
                    return;
                }

                if (source == null)
                {
                    // No new request needs tracking. Keep _tokenSource null so an earlier
                    // result cannot attach.
                    return;
                }

                AttachSource(null);

                var newTokenSource = new CancellationTokenSource();
                _tokenSource = newTokenSource;
                requestTokenSource = newTokenSource;
                var newToken = newTokenSource.Token;
                requestToken = newToken;

                VisualStateManager.GoToState(this, LoadingState, true);
                if (source is ImageRequest imageRequest)
                {
                    var result = await ResolveImageRequestAsync(imageRequest, newToken);
                    if (CanAttachResolvedSource(requestVersion, newTokenSource, newToken, result.Image))
                    {
                        if (result.Image == null)
                        {
                            VisualStateManager.GoToState(this, FailedState, true);
                            ImageExFailed?.Invoke(this, new ImageExFailedEventArgs(new IOException("All image sources failed.")));
                        }
                        else
                        {
                            AttachSource(result.Image, shouldAnimateLoadedState: !result.IsCacheHit);
                        }
                    }

                    return;
                }

                var imageSource = source as ImageSource;
                if (imageSource != null)
                {
                    AttachSource(imageSource);

                    return;
                }
                var uri = source as Uri;
                if (uri == null)
                {
                    var url = source as string ?? source.ToString();
                    if (!Uri.TryCreate(url, UriKind.RelativeOrAbsolute, out uri))
                    {
                        VisualStateManager.GoToState(this, FailedState, true);
                        ImageExFailed?.Invoke(this, new ImageExFailedEventArgs(new UriFormatException("Invalid uri specified")));
                        return;
                    }
                }

                if (!uri.IsHttpUri() && !uri.IsAbsoluteUri)
                {
                    uri = new Uri("ms-appx:///" + uri.OriginalString.TrimStart('/'));
                }

                await LoadImageAsync(uri, requestVersion, newTokenSource, newToken);
            }
            catch (OperationCanceledException)
            {
                // Cancellation was requested. There is nothing to do.
            }
            catch (Exception e)
            {
                var requestIsCurrent = requestTokenSource == null
                    ? IsSourceRequestCurrent(requestVersion)
                    : IsRequestCurrent(requestVersion, requestTokenSource, requestToken);

                if (requestIsCurrent)
                {
                    VisualStateManager.GoToState(this, FailedState, true);
                    ImageExFailed?.Invoke(this, new ImageExFailedEventArgs(e));
                }
            }
            finally
            {
                CompleteSourceRequest(requestVersion, requestTokenSource);
            }
        }

        private void CompleteSourceRequest(long requestVersion, CancellationTokenSource requestTokenSource)
        {
            if (requestTokenSource == null
                || !IsSourceRequestCurrent(requestVersion)
                || !ReferenceEquals(_tokenSource, requestTokenSource))
            {
                return;
            }

            _tokenSource = null;
            requestTokenSource.Dispose();
        }

        private async Task LoadImageAsync(
            Uri imageUri,
            long requestVersion,
            CancellationTokenSource requestTokenSource,
            CancellationToken requestToken)
        {
            if (imageUri != null)
            {
                if (IsCacheEnabled)
                {
                    var result = await ResolveImageAsync(imageUri, requestToken);

                    if (CanAttachResolvedSource(requestVersion, requestTokenSource, requestToken, result.Image))
                    {
                        // Attach the image only while this request remains active.
                        AttachSource(result.Image, shouldAnimateLoadedState: !result.IsCacheHit);
                    }
                }
                else if (string.Equals(imageUri.Scheme, "data", StringComparison.OrdinalIgnoreCase))
                {
                    var source = imageUri.OriginalString;
                    const string base64Head = "base64,";
                    var index = source.IndexOf(base64Head, StringComparison.OrdinalIgnoreCase);
                    if (index >= 0)
                    {
                        var bytes = Convert.FromBase64String(source.Substring(index + base64Head.Length));
                        var bitmap = new BitmapImage();
                        await bitmap.SetSourceAsync(new MemoryStream(bytes).AsRandomAccessStream());

                        if (CanAttachResolvedSource(requestVersion, requestTokenSource, requestToken, bitmap))
                        {
                            AttachSource(bitmap);
                        }
                    }
                }
                else
                {
                    var determinedSource = GetDeterminedSource(imageUri);
                    if (IsLoaded)
                    {
                        AttachSource(determinedSource);
                    }
                }
            }
        }

        private bool CanAttachResolvedSource(
            long requestVersion,
            CancellationTokenSource requestTokenSource,
            CancellationToken requestToken,
            ImageSource source)
        {
            return IsRequestCurrent(requestVersion, requestTokenSource, requestToken)
                && (source == null || IsLoaded);
        }

        /// <summary>
        /// Returns true when <paramref name="requestTokenSource"/> still represents the active request.
        /// A newer Source change, a null assignment, or unload can supersede a request.
        /// The method also checks cancellation before async completion paths attach stale results
        /// or change the visual state.
        /// </summary>
        private bool IsRequestCurrent(
            long requestVersion,
            CancellationTokenSource requestTokenSource,
            CancellationToken requestToken)
        {
            if (!IsSourceRequestCurrent(requestVersion))
            {
                return false;
            }

            if (!ReferenceEquals(_tokenSource, requestTokenSource))
            {
                return false;
            }

            try
            {
                return !requestToken.IsCancellationRequested;
            }
            catch (ObjectDisposedException)
            {
                return false;
            }
        }

        private bool IsSourceRequestCurrent(long requestVersion)
        {
            return Interlocked.Read(ref _sourceRequestVersion) == requestVersion;
        }

        internal static ImageSource GetDeterminedSource(Uri uri)
        {
            if (uri.PathAndQuery.EndsWith(".svg", StringComparison.OrdinalIgnoreCase))
            {
                return new SvgImageSource(uri);
            }

            return ImageExDeferredBitmapSourceRegistry.CreateDeferredBitmapImage(
                uri,
                decodeWidth: 0,
                decodeHeight: 0,
                decodeType: DecodePixelType.Physical,
                createOptions: BitmapCreateOptions.IgnoreImageCache);
        }

        protected virtual Task<ImageLoadResult> ResolveImageRequestAsync(ImageRequest request, CancellationToken token)
        {
            throw new NotSupportedException("This control does not support ordered image requests.");
        }

        /// <summary>
        /// Override this method to provide a custom image resolution strategy for <see cref="ImageExBase"/>.
        /// The default implementation uses the platform image cache.
        /// The <see cref="CancellationToken"/> signals that the current request is no longer valid.
        /// For example, the container can be recycled before the original image loads.
        /// </summary>
        /// <param name="imageUri">The image URI.</param>
        /// <param name="token">The token that signals an outdated request.</param>
        /// <returns>The resolved image and its cache status.</returns>

        protected virtual Task<ImageLoadResult> ResolveImageAsync(Uri imageUri, CancellationToken token)
        {
            // Use the platform image cache provided by the Image control.
            ImageExDiagnostics.RecordBaseBitmapCreated(imageUri, DecodePixelWidth, DecodePixelHeight, DecodePixelType);
            var image = (ImageSource)ImageExDeferredBitmapSourceRegistry.CreateDeferredBitmapImage(
                imageUri,
                DecodePixelWidth,
                DecodePixelHeight,
                DecodePixelType,
                BitmapCreateOptions.None);
            return Task.FromResult(new ImageLoadResult(image, IsCacheHit: false));
        }
    }
}
