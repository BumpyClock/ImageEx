// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// ReSharper disable AsyncVoidMethod
namespace ImageEx
{
    /// <summary>
    /// Base code for ImageEx
    /// </summary>
    public partial class ImageExBase
    {
        /// <summary>
        /// Identifies the <see cref="Source"/> dependency property.
        /// </summary>
        public static readonly DependencyProperty SourceProperty = DependencyProperty.Register(nameof(Source), typeof(object), typeof(ImageExBase), new PropertyMetadata(null, SourceChanged));

        //// Used to track if we get a new request, so we can cancel any potential custom cache loading.
        private CancellationTokenSource _tokenSource;
        private long _sourceRequestVersion;

        private object _lazyLoadingSource;

        /// <summary>
        /// Gets or sets the source used by the image
        /// </summary>
        public object Source
        {
            get { return GetValue(SourceProperty); }
            set { SetValue(SourceProperty, value); }
        }

        private void OnImageExUnloaded(object sender, RoutedEventArgs e)
        {
            _isInViewport = false;
            CleanupTokenSource();

            if (_currentImageSource != null)
            {
                AttachSource(null);
            }
        }

        private void OnImageExLoaded(object sender, RoutedEventArgs e)
        {
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
                    // Already disposed, ignore
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
                if (e.NewValue == null)
                {
                    control._lazyLoadingSource = null;
                    control.DetachLazyLoadingHandlers();
                    control.SetSource(e.NewValue);
                }
                else if (!control.EnableLazyLoading || control._isInViewport)
                {
                    control._lazyLoadingSource = null;
                    control.DetachLazyLoadingHandlers();
                    control.SetSource(e.NewValue);
                }
                else
                {
                    control.DeferSourceUntilViewport(e.NewValue);
                }
            }
        }

        /// <summary>
        /// Method to call to assign an <see cref="ImageSource"/> value to the underlying <see cref="Image"/> powering <see cref="ImageExBase"/>.
        /// </summary>
        /// <param name="source"><see cref="ImageSource"/> to assign to the image.</param>
        private void AttachSource(ImageSource source)
        {
            _currentImageSource = source;

            // Setting the source at this point should call ImageExOpened/VisualStateManager.GoToState
            // as we register to both the ImageOpened/ImageFailed events of the underlying control.
            // We only need to call those methods if we fail in other cases before we get here.
            if (Image is Image image)
            {
                image.Source = source;
            }
            else if (Image is ImageBrush brush)
            {
                brush.ImageSource = source;
            }

            if (source == null)
            {
                _currentImageSource = null;
                VisualStateManager.GoToState(this, UnloadedState, true);
            }
            else if (source is BitmapSource { PixelHeight: > 0, PixelWidth: > 0 })
            {
                VisualStateManager.GoToState(this, LoadedState, true);
                ImageExOpened?.Invoke(this, new ImageExOpenedEventArgs());
            }
        }

        private void DeferSourceUntilViewport(object source)
        {
            _lazyLoadingSource = source;
            AttachLazyLoadingHandlers();
            SetSource(null);
            InvalidateLazyLoading();
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

                // Cancel any in-flight previous request, then clear the field. A null
                // _tokenSource signals no active request; any in-flight result from a prior
                // request will fail the IsRequestCurrent guard in LoadImageAsync.
                var previousTokenSource = _tokenSource;
                _tokenSource = null;

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

                AttachSource(null);

                if (source == null)
                {
                    // No new request to track. _tokenSource stays null so any in-flight result
                    // from the previous request cannot attach.
                    return;
                }

                var newTokenSource = new CancellationTokenSource();
                _tokenSource = newTokenSource;
                requestTokenSource = newTokenSource;
                var newToken = newTokenSource.Token;
                requestToken = newToken;

                VisualStateManager.GoToState(this, LoadingState, true);
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
                // Nothing to do as cancellation has been requested
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
                    var img = await ProvideCachedResourceAsync(imageUri, requestToken);

                    if (IsRequestCurrent(requestVersion, requestTokenSource, requestToken))
                    {
                        // Only attach our image if this is still the active request
                        AttachSource(img);
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

                        if (IsRequestCurrent(requestVersion, requestTokenSource, requestToken))
                        {
                            AttachSource(bitmap);
                        }
                    }
                }
                else
                {
                    AttachSource(GetDeterminedSource(imageUri));
                }
            }
        }

        /// <summary>
        /// Returns true when <paramref name="requestTokenSource"/> still represents the active
        /// request (i.e. it has not been superseded by a newer Source change, null assignment,
        /// or unload) and has not been cancelled. Used to gate stale attaches and visual-state
        /// transitions on async completion paths.
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

            return new BitmapImage(uri)
            {
                CreateOptions = BitmapCreateOptions.IgnoreImageCache
            };
        }

        /// <summary>
        /// This method is provided in case a developer would like their own custom caching strategy for <see cref="ImageExBase"/>.
        /// By default, it uses the built-in UWP cache provided by <see cref="BitmapImage"/> and
        /// the <see cref="Image"/> control itself. This method should return an <see cref="ImageSource"/>
        /// value of the image specified by the provided uri parameter.
        /// A <see cref="CancellationToken"/> is provided in case the current request is invalidated
        /// (e.g. the container is recycled before the original image loaded).
        /// </summary>
        /// <example>
        /// <code>
        ///     var propValues = new List&lt;KeyValuePair&lt;string, object>>();
        ///
        ///     if (DecodePixelHeight > 0)
        ///     {
        ///         propValues.Add(new KeyValuePair&lt;string, object>(nameof(DecodePixelHeight), DecodePixelHeight));
        ///     }
        ///     if (DecodePixelWidth > 0)
        ///     {
        ///         propValues.Add(new KeyValuePair&lt;string, object>(nameof(DecodePixelWidth), DecodePixelWidth));
        ///     }
        ///     if (propValues.Count > 0)
        ///     {
        ///         propValues.Add(new KeyValuePair&lt;string, object>(nameof(DecodePixelType), DecodePixelType));
        ///     }
        ///
        ///     // A token is provided here as well to cancel the request to the cache,
        ///     // if a new image is requested.
        ///     return await ImageCache.Instance.GetFromCacheAsync(imageUri, true, token, propValues);
        /// </code>
        /// </example>
        /// <param name="imageUri"><see cref="Uri"/> of the image to load from the cache.</param>
        /// <param name="token">A <see cref="CancellationToken"/> which is used to signal when the current request is outdated.</param>
        /// <returns><see cref="Task"/></returns>
        protected virtual Task<ImageSource> ProvideCachedResourceAsync(Uri imageUri, CancellationToken token)
        {
            // By default, we just use the built-in UWP image cache provided within the Image control.
            return Task.FromResult((ImageSource)new BitmapImage(imageUri));
        }
    }
}
