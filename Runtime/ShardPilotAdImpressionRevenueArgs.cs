#nullable enable annotations
namespace ShardPilot
{
    /// <summary>
    /// One ad impression's revenue, supplied by named fields. RevenueMicros
    /// uses millionths of Currency; zero is valid. Required identifiers are
    /// validated before enqueue. Empty optional fields are omitted from the wire.
    /// </summary>
    public struct ShardPilotAdImpressionRevenueArgs
    {
        public string? ImpressionId;
        public string? Network;
        public long RevenueMicros;
        public string? Currency;
        public string? RevenuePrecision;
        public string? AdUnit;
        public string? AdFormat;
        public string? Placement;
    }
}
