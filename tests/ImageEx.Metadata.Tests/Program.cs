using System.Text;
using ImageEx.Cache;

var tests = new (string Name, string Svg, Size Expected)[]
{
    ("Explicit dimensions", "<svg width='320px' height='180' />", new(320, 180)),
    ("SVG 1.1 public DOCTYPE", "<!DOCTYPE svg PUBLIC '-//W3C//DTD SVG 1.1//EN' 'http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd'><svg width='320' height='180' />", new(320, 180)),
    ("DOCTYPE viewBox", "<!DOCTYPE svg><svg viewBox='0 0 640 360' />", new(640, 360)),
    ("Unavailable external DTD", "<!DOCTYPE svg SYSTEM 'file:///ImageEx-test-missing-directory/missing.dtd'><svg width='64' height='48' />", new(64, 48)),
    ("DTD default attributes are ignored", "<!DOCTYPE svg [<!ATTLIST svg width CDATA '999' height CDATA '999'>]><svg viewBox='0 0 64 48' />", new(64, 48)),
    ("DTD entities are not expanded", "<!DOCTYPE svg [<!ENTITY width '999'>]><svg width='&width;' height='48' />", default),
    ("External entities are not expanded", "<!DOCTYPE svg [<!ENTITY width SYSTEM 'file:///ImageEx-test-missing-directory/missing.txt'>]><svg width='&width;' height='48' />", default),
    ("Invalid XML", "<svg width='12", default),
    ("Invalid dimensions", "<svg width='NaN' height='48' />", default)
};

foreach (var (name, svg, expected) in tests)
{
    using var stream = new MemoryStream(Encoding.UTF8.GetBytes("prefix" + svg));
    stream.Position = 6;
    var actual = SvgNaturalSize.Read(stream);
    if (actual != expected || stream.Position != 6 || !stream.CanRead)
    {
        throw new Exception($"FAIL: {name}: expected {expected}, actual {actual}, position {stream.Position}.");
    }
    Console.WriteLine($"PASS: {name}");
}

Console.WriteLine($"Passed {tests.Length} SVG metadata tests.");

// The metadata parser needs only this value type from Windows.Foundation.
internal readonly record struct Size(double Width, double Height);
