using System.Globalization;
using System.Xml;

namespace ImageEx.Cache;

internal static class SvgNaturalSize
{
    internal static Size Read(Stream stream)
    {
        if (!stream.CanSeek)
        {
            return default;
        }

        var position = stream.Position;
        try
        {
            using var reader = XmlReader.Create(stream, new XmlReaderSettings
            {
                DtdProcessing = DtdProcessing.Prohibit,
                XmlResolver = null,
                CloseInput = false,
                MaxCharactersInDocument = 65536
            });
            if (reader.MoveToContent() != XmlNodeType.Element || reader.LocalName != "svg")
            {
                return default;
            }

            if (TryLength(reader.GetAttribute("width"), out var width) &&
                TryLength(reader.GetAttribute("height"), out var height))
            {
                return new Size(width, height);
            }

            var values = reader.GetAttribute("viewBox")?.Split(
                new[] { ' ', '\t', '\r', '\n', ',' }, StringSplitOptions.RemoveEmptyEntries);
            if (values is { Length: 4 } &&
                TryNumber(values[0], out _) && TryNumber(values[1], out _) &&
                TryNumber(values[2], out width) && TryNumber(values[3], out height) &&
                width > 0 && height > 0)
            {
                return new Size(width, height);
            }

            return default;
        }
        catch (XmlException)
        {
            // Metadata must not replace the native SVG decoder's format decision.
            return default;
        }
        finally
        {
            stream.Position = position;
        }
    }

    private static bool TryLength(string value, out double result)
    {
        value = value?.Trim();
        if (value?.EndsWith("px", StringComparison.Ordinal) == true)
        {
            value = value[..^2];
        }
        return TryNumber(value, out result) && result > 0;
    }

    private static bool TryNumber(string value, out double result) =>
        double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out result) &&
        double.IsFinite(result);
}
