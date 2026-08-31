# ImageEx: extended image control for UWP and WinUI apps

The ImageEx control extends the platform Image control. It loads source images asynchronously and shows a loading indicator during the load. It saves downloaded images in the app's local cache so later loads use fewer resources and finish sooner.

ImageEx accepts at most 8 MiB of source bytes for each raster or SVG response. It rejects larger declared responses before reading and stops chunked responses at the limit.

This package contains a separate ImageEx control from the Windows Community Toolkit 7.x package.
Since this control was removed in Toolkit version 8.0, this package was created containing only this control and having no dependencies.
Originally developed by Microsoft.Toolkit
https://github.com/CommunityToolkit/WindowsCommunityToolkit

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL FOURSOFT BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
