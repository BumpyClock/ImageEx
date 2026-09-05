// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// ReSharper disable MemberCanBePrivate.Global

using CommunityToolkit.WinUI;
using Microsoft.UI.Dispatching;

namespace ImageEx
{
    public static class Extensions
    {
        /// <summary>
        /// Determines whether one rectangle intersects another rectangle.
        /// </summary>
        /// <param name="rect1">The first rectangle to test.</param>
        /// <param name="rect2">The second rectangle to test.</param>
        /// <returns><see langword="true"/> when the rectangles intersect. Otherwise, <see langword="false"/>.</returns>
        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IntersectsWith(this Rect rect1, Rect rect2)
        {
            if (rect1.IsEmpty || rect2.IsEmpty)
            {
                return false;
            }

            return (rect1.Left <= rect2.Right) &&
                   (rect1.Right >= rect2.Left) &&
                   (rect1.Top <= rect2.Bottom) &&
                   (rect1.Bottom >= rect2.Top);
        }
    }

    /// <summary>
    /// Base implementation for ImageEx.
    /// </summary>
    [TemplateVisualState(Name = LoadingState, GroupName = CommonGroup)]
    [TemplateVisualState(Name = LoadedState, GroupName = CommonGroup)]
    [TemplateVisualState(Name = UnloadedState, GroupName = CommonGroup)]
    [TemplateVisualState(Name = FailedState, GroupName = CommonGroup)]
    [TemplatePart(Name = PartImage, Type = typeof(object))]
    public abstract partial class ImageExBase : Control, IAlphaMaskProvider
    {
        private bool _isInViewport;
        private bool _lazyLoadingHandlersAttached;
        private ImageSource _currentImageSource;
        private bool _shouldAnimateCurrentImage = true;
        private readonly DispatcherQueue _creationDispatcherQueue;
        private long _diagnosticAttachedSourceBytes;
        private CancellationTokenSource _offscreenDetachTokenSource;
        private long _viewportStateGeneration;
        private Rect? _lastEffectiveViewport;
        private static readonly TimeSpan OffscreenDetachGracePeriod = TimeSpan.FromMilliseconds(250);

        /// <summary>
        /// Name of the image element in the template.
        /// </summary>
        protected const string PartImage = "Image";

        /// <summary>
        /// Name of the visual-states element in the template.
        /// </summary>
        protected const string CommonGroup = "CommonStates";

        /// <summary>
        /// Name of the loading state in the template.
        /// </summary>
        protected const string LoadingState = "Loading";

        /// <summary>
        /// Name of the loaded state in the template.
        /// </summary>
        protected const string LoadedState = "Loaded";

        /// <summary>
        /// Name of the unloaded state in the template.
        /// </summary>
        protected const string UnloadedState = "Unloaded";

        /// <summary>
        /// Name of the failed state in the template.
        /// </summary>
        protected const string FailedState = "Failed";

        /// <summary>
        /// Gets the backing image object.
        /// </summary>
        protected object Image { get; private set; }

        /// <inheritdoc/>
        public bool WaitUntilLoaded => true;

        /// <summary>
        /// Initializes a new instance of the <see cref="ImageExBase"/> class.
        /// </summary>
        // ReSharper disable once PublicConstructorInAbstractClass
        public ImageExBase()
        {
            _creationDispatcherQueue = DispatcherQueue.GetForCurrentThread();
        }

        protected DispatcherQueue ImageDispatcherQueue => DispatcherQueue ?? _creationDispatcherQueue;

        private void ModifyImageHandler(Action<Image> imageHandlerUpdate, Action<ImageBrush> brushHandlerUpdate)
        {
            if (Image is Image image)
            {
                imageHandlerUpdate(image);
            }
            else if (Image is ImageBrush brush)
            {
                brushHandlerUpdate(brush);
            }
        }

        /// <summary>
        /// Attaches an image-opened event handler.
        /// </summary>
        /// <param name="handler">Routed event handler.</param>
        protected void AttachImageOpened(RoutedEventHandler handler)
        {
            ModifyImageHandler(
                image => image.ImageOpened += handler,
                brush => brush.ImageOpened += handler);
        }

        /// <summary>
        /// Removes an image-opened event handler.
        /// </summary>
        /// <param name="handler">Routed event handler.</param>
        protected void RemoveImageOpened(RoutedEventHandler handler)
        {
            ModifyImageHandler(
                image => image.ImageOpened -= handler,
                brush => brush.ImageOpened -= handler);
        }

        /// <summary>
        /// Attaches an image-failed event handler.
        /// </summary>
        /// <param name="handler">Exception event handler.</param>
        protected void AttachImageFailed(ExceptionRoutedEventHandler handler)
        {
            ModifyImageHandler(
                image => image.ImageFailed += handler,
                brush => brush.ImageFailed += handler);
        }

        /// <summary>
        /// Removes an image-failed event handler.
        /// </summary>
        /// <param name="handler">Exception event handler.</param>
        protected void RemoveImageFailed(ExceptionRoutedEventHandler handler)
        {
            ModifyImageHandler(
                image => image.ImageFailed -= handler,
                brush => brush.ImageFailed -= handler);
        }

        /// <summary>
        /// Update the visual state of the control when its template is changed.
        /// </summary>
        protected override void OnApplyTemplate()
        {
            RemoveImageOpened(OnImageOpened);
            RemoveImageFailed(OnImageFailed);

            Image = GetTemplateChild(PartImage);

            IsInitialized = true;

            ImageExInitialized?.Invoke(this, EventArgs.Empty);

            if (Source == null)
            {
                _lazyLoadingSource = null;
                DetachLazyLoadingHandlers();
                SetSource(Source);
            }
            else if (_currentImageSource != null)
            {
                if (EnableLazyLoading)
                {
                    AttachLazyLoadingHandlers();
                }
                else
                {
                    DetachLazyLoadingHandlers();
                }

                AttachSource(_currentImageSource, _shouldAnimateCurrentImage);
            }
            else if (HasCurrentRequest())
            {
                if (EnableLazyLoading)
                {
                    AttachLazyLoadingHandlers();
                }

                // Keep current request alive across template reapply.
            }
            else if (EnableLazyLoading && !_isInViewport)
            {
                if (_lazyLoadingSource == null || !Equals(_lazyLoadingSource, Source))
                {
                    DeferSourceUntilViewport(Source);
                }
                else
                {
                    AttachLazyLoadingHandlers();
                }
            }
            else
            {
                _lazyLoadingSource = null;
                if (EnableLazyLoading)
                {
                    AttachLazyLoadingHandlers();
                }
                else
                {
                    DetachLazyLoadingHandlers();
                }

                SetSource(Source);
            }

            AttachImageOpened(OnImageOpened);
            AttachImageFailed(OnImageFailed);
            
            Loaded -= OnImageExLoaded;
            Loaded += OnImageExLoaded;
            Unloaded -= OnImageExUnloaded;
            Unloaded += OnImageExUnloaded;

            base.OnApplyTemplate();
        }

        /// <summary>
        /// Underlying <see cref="Image.ImageOpened"/> event handler.
        /// </summary>
        /// <param name="sender">Image</param>
        /// <param name="e">Event Arguments</param>
        protected virtual void OnImageOpened(object sender, RoutedEventArgs e)
        {
            UpdateDiagnosticAttachedSourceBytes();
            VisualStateManager.GoToState(this, LoadedState, _shouldAnimateCurrentImage);
            ImageExOpened?.Invoke(this, new ImageExOpenedEventArgs());
        }

        /// <summary>
        /// Underlying <see cref="Image.ImageFailed"/> event handler.
        /// </summary>
        /// <param name="sender">Image</param>
        /// <param name="e">Event Arguments</param>
        protected virtual void OnImageFailed(object sender, ExceptionRoutedEventArgs e)
        {
            VisualStateManager.GoToState(this, FailedState, true);
            ImageExFailed?.Invoke(this, new ImageExFailedEventArgs(new Exception(e.ErrorMessage)));
        }

        private void ImageExBase_EffectiveViewportChanged(FrameworkElement sender, EffectiveViewportChangedEventArgs args)
        {
            _lastEffectiveViewport = args.EffectiveViewport;
            InvalidateLazyLoading(args.EffectiveViewport);
        }

        private void ImageExBase_LazyLoadingSizeChanged(object sender, SizeChangedEventArgs args)
        {
            InvalidateLazyLoading();
        }

        private void InvalidateLazyLoading()
        {
            if (!IsLoaded)
            {
                _isInViewport = false;
                return;
            }

            if (_lastEffectiveViewport is { } viewport)
            {
                InvalidateLazyLoading(viewport);
                return;
            }

            // InlineUIContainer is a logical parent, not a visual ancestor.
            FrameworkElement hostElement = null;
            for (var ancestor = VisualTreeHelper.GetParent(this);
                ancestor != null;
                ancestor = VisualTreeHelper.GetParent(ancestor))
            {
                if (ancestor is FrameworkElement element)
                {
                    hostElement = element;
                    if (element is ScrollViewer)
                    {
                        break;
                    }
                }
            }

            if (hostElement == null)
            {
                _isInViewport = false;
                return;
            }

            var controlRect = TransformToVisual(hostElement)
                .TransformBounds(new Rect(0, 0, ActualWidth, ActualHeight));
            var hostRect = ExpandLazyLoadingViewport(
                0,
                0,
                hostElement.ActualWidth,
                hostElement.ActualHeight);

            ApplyLazyLoadingViewportState(controlRect.IntersectsWith(hostRect));
        }

        private void InvalidateLazyLoading(Rect effectiveViewport)
        {
            if (!IsLoaded)
            {
                _isInViewport = false;
                return;
            }

            var controlRect = new Rect(0, 0, ActualWidth, ActualHeight);
            var viewportRect = ExpandLazyLoadingViewport(
                effectiveViewport.X,
                effectiveViewport.Y,
                effectiveViewport.Width,
                effectiveViewport.Height);

            ApplyLazyLoadingViewportState(controlRect.IntersectsWith(viewportRect));
        }

        private Rect ExpandLazyLoadingViewport(double x, double y, double width, double height)
        {
            var lazyLoadingThreshold = LazyLoadingThreshold;
            return new Rect(
                x - lazyLoadingThreshold,
                y - lazyLoadingThreshold,
                width + (2 * lazyLoadingThreshold),
                height + (2 * lazyLoadingThreshold));
        }

        private void ApplyLazyLoadingViewportState(bool isInViewport)
        {
            var generation = Interlocked.Increment(ref _viewportStateGeneration);
            if (isInViewport)
            {
                _isInViewport = true;
                CancelPendingOffscreenDetach();

                if (_lazyLoadingSource != null)
                {
                    var source = _lazyLoadingSource;
                    _lazyLoadingSource = null;
                    SetSource(source);
                }
                else if (DetachSourceWhenOutsideViewport
                    && Source != null
                    && !HasAttachedSource()
                    && !HasCurrentRequest())
                {
                    ImageExDiagnostics.RecordCacheReattach(this);
                    RestartSourceIfNeeded();
                }
            }
            else
            {
                _isInViewport = false;
                if (HasAttachedSource())
                {
                    ImageExDiagnostics.RecordOffscreenAttached(this);
                }

                SuspendSourceUntilViewport();
                if (DetachSourceWhenOutsideViewport && HasAttachedSource())
                {
                    ScheduleOffscreenDetach(generation);
                }
            }
        }

        internal void ApplyLazyLoadingViewportStateForTesting(bool isInViewport)
            => ApplyLazyLoadingViewportState(isInViewport);

        private void ScheduleOffscreenDetach(long generation)
        {
            CancelPendingOffscreenDetach();
            var tokenSource = new CancellationTokenSource();
            _offscreenDetachTokenSource = tokenSource;
            _ = DetachSourceAfterGracePeriodAsync(generation, tokenSource);
        }

        private async Task DetachSourceAfterGracePeriodAsync(
            long generation,
            CancellationTokenSource tokenSource)
        {
            var completeOnExit = true;
            try
            {
                await Task.Delay(OffscreenDetachGracePeriod, tokenSource.Token);
                if (tokenSource.IsCancellationRequested)
                {
                    return;
                }

                var dispatcherQueue = ImageDispatcherQueue;
                if (dispatcherQueue is { HasThreadAccess: false })
                {
                    if (dispatcherQueue.TryEnqueue(() =>
                    {
                        try
                        {
                            ApplyOffscreenDetach(generation, tokenSource);
                        }
                        finally
                        {
                            CompletePendingOffscreenDetach(tokenSource);
                        }
                    }))
                    {
                        completeOnExit = false;
                    }

                    return;
                }

                ApplyOffscreenDetach(generation, tokenSource);
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                if (completeOnExit)
                {
                    CompletePendingOffscreenDetach(tokenSource);
                }
            }
        }

        private void CompletePendingOffscreenDetach(CancellationTokenSource tokenSource)
        {
            if (ReferenceEquals(_offscreenDetachTokenSource, tokenSource))
            {
                _offscreenDetachTokenSource = null;
            }

            tokenSource.Dispose();
        }

        private void ApplyOffscreenDetach(long generation, CancellationTokenSource tokenSource)
        {
            if (!ReferenceEquals(_offscreenDetachTokenSource, tokenSource)
                || tokenSource.IsCancellationRequested
                || Interlocked.Read(ref _viewportStateGeneration) != generation
                || _isInViewport
                || !EnableLazyLoading
                || !DetachSourceWhenOutsideViewport
                || Source == null
                || !HasAttachedSource())
            {
                return;
            }

            ImageExDiagnostics.RecordOffscreenDetach(this);
            AttachSource(null);
        }

        private void CancelPendingOffscreenDetach()
        {
            var tokenSource = _offscreenDetachTokenSource;
            _offscreenDetachTokenSource = null;
            if (tokenSource == null)
            {
                return;
            }

            try
            {
                tokenSource.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private void SuspendSourceUntilViewport()
        {
            if (!EnableLazyLoading
                || Source == null
                || _lazyLoadingSource != null
                || HasAttachedSource())
            {
                return;
            }

            _lazyLoadingSource = Source;
            AttachLazyLoadingHandlers();
            SetSource(null);
        }

        private void AttachLazyLoadingHandlers()
        {
            if (_lazyLoadingHandlersAttached)
            {
                return;
            }

            EffectiveViewportChanged += ImageExBase_EffectiveViewportChanged;
            SizeChanged += ImageExBase_LazyLoadingSizeChanged;
            _lazyLoadingHandlersAttached = true;
        }

        private void DetachLazyLoadingHandlers()
        {
            if (!_lazyLoadingHandlersAttached)
            {
                return;
            }

            EffectiveViewportChanged -= ImageExBase_EffectiveViewportChanged;
            SizeChanged -= ImageExBase_LazyLoadingSizeChanged;
            _lastEffectiveViewport = null;
            _lazyLoadingHandlersAttached = false;
        }
    }
}
