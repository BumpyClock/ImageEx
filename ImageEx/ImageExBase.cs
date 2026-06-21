// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

// ReSharper disable MemberCanBePrivate.Global

using CommunityToolkit.WinUI;

namespace ImageEx
{
    public static class Extensions
    {
        /// <summary>
        /// Determines if a rectangle intersects with another rectangle.
        /// </summary>
        /// <param name="rect1">The first rectangle to test.</param>
        /// <param name="rect2">The second rectangle to test.</param>
        /// <returns>This method returns <see langword="true"/> if there is any intersection, otherwise <see langword="false"/>.</returns>
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
    /// Base Code for ImageEx
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
        private long _diagnosticAttachedSourceBytes;

        /// <summary>
        /// Image name in template
        /// </summary>
        protected const string PartImage = "Image";

        /// <summary>
        /// VisualStates name in template
        /// </summary>
        protected const string CommonGroup = "CommonStates";

        /// <summary>
        /// Loading state name in template
        /// </summary>
        protected const string LoadingState = "Loading";

        /// <summary>
        /// Loaded state name in template
        /// </summary>
        protected const string LoadedState = "Loaded";

        /// <summary>
        /// Unloaded state name in template
        /// </summary>
        protected const string UnloadedState = "Unloaded";

        /// <summary>
        /// Failed name in template
        /// </summary>
        protected const string FailedState = "Failed";

        /// <summary>
        /// Gets the backing image object
        /// </summary>
        protected object Image { get; private set; }

        /// <inheritdoc/>
        public bool WaitUntilLoaded => true;

        /// <summary>
        /// Initializes a new instance of the <see cref="ImageExBase"/> class.
        /// </summary>
        // ReSharper disable once PublicConstructorInAbstractClass
        public ImageExBase() { }

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
        /// Attach image opened event handler
        /// </summary>
        /// <param name="handler">Routed Event Handler</param>
        protected void AttachImageOpened(RoutedEventHandler handler)
        {
            ModifyImageHandler(
                image => image.ImageOpened += handler,
                brush => brush.ImageOpened += handler);
        }

        /// <summary>
        /// Remove image opened handler
        /// </summary>
        /// <param name="handler">RoutedEventHandler</param>
        protected void RemoveImageOpened(RoutedEventHandler handler)
        {
            ModifyImageHandler(
                image => image.ImageOpened -= handler,
                brush => brush.ImageOpened -= handler);
        }

        /// <summary>
        /// Attach image failed event handler
        /// </summary>
        /// <param name="handler">Exception Routed Event Handler</param>
        protected void AttachImageFailed(ExceptionRoutedEventHandler handler)
        {
            ModifyImageHandler(
                image => image.ImageFailed += handler,
                brush => brush.ImageFailed += handler);
        }

        /// <summary>
        /// Remove Image Failed handler
        /// </summary>
        /// <param name="handler">Exception Routed Event Handler</param>
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

                AttachSource(_currentImageSource);
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
            VisualStateManager.GoToState(this, LoadedState, true);
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
            InvalidateLazyLoading(args.EffectiveViewport);
        }

        private void InvalidateLazyLoading()
        {
            if (!IsLoaded)
            {
                _isInViewport = false;
                return;
            }

            // Find the first ascendant ScrollViewer, if not found, use the root element.
            FrameworkElement hostElement = null;
            var ascendants = this.FindAscendants().OfType<FrameworkElement>();
            foreach (var ascendant in ascendants)
            {
                hostElement = ascendant;
                if (hostElement is ScrollViewer)
                {
                    break;
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
            if (isInViewport)
            {
                _isInViewport = true;

                if (_lazyLoadingSource != null)
                {
                    var source = _lazyLoadingSource;
                    _lazyLoadingSource = null;
                    SetSource(source);
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
            }
        }

        private void SuspendSourceUntilViewport()
        {
            if (!EnableLazyLoading
                || Source == null
                || _lazyLoadingSource != null)
            {
                return;
            }

            var hadAttachedSource = HasAttachedSource();
            _lazyLoadingSource = Source;
            AttachLazyLoadingHandlers();
            if (hadAttachedSource)
            {
                ImageExDiagnostics.RecordOffscreenDetach(this);
            }

            SetSource(null);
        }

        private void AttachLazyLoadingHandlers()
        {
            if (_lazyLoadingHandlersAttached)
            {
                return;
            }

            EffectiveViewportChanged += ImageExBase_EffectiveViewportChanged;
            _lazyLoadingHandlersAttached = true;
        }

        private void DetachLazyLoadingHandlers()
        {
            if (!_lazyLoadingHandlersAttached)
            {
                return;
            }

            EffectiveViewportChanged -= ImageExBase_EffectiveViewportChanged;
            _lazyLoadingHandlersAttached = false;
        }
    }
}
