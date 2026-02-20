// ABOUTME: System.Text.Json source generation context for ImageEx disk-cache metadata.
// ABOUTME: Enables AOT-safe serialization/deserialization for cache metadata dictionaries.

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace ImageEx.Cache;

[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(Dictionary<string, CacheEntry>))]
internal partial class ImageExCacheJsonContext : JsonSerializerContext
{
}
