using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Media;



namespace ImageEx.Hosted.Tests;

[TestClass]
public sealed class TemplateTests
{
    [UITestMethod]
    public async Task Production_template_exposes_common_states()
    {
        var control = new ImageExControl { Width = 100, Height = 100 };
        var window = new Window { Content = control };
        try
        {
            window.Activate();
            await TestWait.ForConditionAsync(() => control.IsLoaded);
            control.ApplyTemplate();

            Assert.IsTrue(control.IsInitialized);
            var root = (FrameworkElement)VisualTreeHelper.GetChild(control, 0);
            var groups = VisualStateManager.GetVisualStateGroups(root);
            Assert.IsTrue(groups.Any(group => group.Name == "CommonStates"));
        }
        finally
        {
            window.Close();
        }
    }
}
