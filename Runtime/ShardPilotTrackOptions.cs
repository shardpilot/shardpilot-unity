namespace ShardPilot
{
    public sealed class ShardPilotTrackOptions
    {
        /// <summary>Null selects version 1; explicit versions are 1..65535.</summary>
        public int? SchemaVersion { get; set; }
    }
}
