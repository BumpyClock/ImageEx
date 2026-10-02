using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.VisualStudio.TestTools.UnitTesting.AppContainer;

namespace ImageEx.Hosted.Tests;

public partial class UnitTestApp : Application
{
    private Window? _window;

    public UnitTestApp()
    {
        InitializeComponent();
    }

    protected override void OnLaunched(LaunchActivatedEventArgs args)
    {
        Microsoft.VisualStudio.TestPlatform.TestExecutor.UnitTestClient.CreateDefaultUI();

        _window = new Window { Title = "ImageEx Hosted Tests", Content = new Grid() };
        _window.Activate();
        UITestMethodAttribute.DispatcherQueue = _window.DispatcherQueue;

        Microsoft.VisualStudio.TestPlatform.TestExecutor.UnitTestClient.Run(Environment.CommandLine);
    }
}
