using System;
using System.Threading;

namespace ShardPilot
{
    /// <summary>Local delivery age; never serialized into a wire envelope.</summary>
    internal sealed class ShardPilotAnalyticsClock
    {
        // Matches the ingest service's default deduplication retention.
        internal const long DeduplicationWindowMs = 15L * 60 * 1000;
        internal const long IntakeRetentionMs = 7L * 24 * 60 * 60 * 1000;

        private sealed class ReportingState { internal int Expired; }
        private readonly ReportingState _reporting;
        internal readonly long IntakeMs;
        internal readonly long AmbiguousMs;

        internal ShardPilotAnalyticsClock(long intakeMs, long ambiguousMs)
            : this(intakeMs, ambiguousMs, new ReportingState()) { }

        private ShardPilotAnalyticsClock(long intakeMs, long ambiguousMs, ReportingState reporting)
        {
            IntakeMs = intakeMs;
            AmbiguousMs = ambiguousMs;
            _reporting = reporting;
        }

        internal ShardPilotAnalyticsClock WithAmbiguous(long value)
        {
            return new ShardPilotAnalyticsClock(IntakeMs, value, _reporting);
        }

        internal bool IsExpired(long now)
        {
            return Volatile.Read(ref _reporting.Expired) != 0 ||
                IntakeMs <= 0 || IntakeMs > now || now - IntakeMs >= IntakeRetentionMs ||
                AmbiguousMs < 0 || AmbiguousMs > now ||
                (AmbiguousMs > 0 && (AmbiguousMs < IntakeMs || now - AmbiguousMs >= DeduplicationWindowMs));
        }

        // A rejected provisional record may already have a restored in-memory
        // copy. Retire their shared state without counting a new age expiry.
        internal void MarkSettled()
        {
            Interlocked.Exchange(ref _reporting.Expired, 1);
        }

        internal bool MarkExpired()
        {
            return Interlocked.Exchange(ref _reporting.Expired, 1) == 0;
        }

        internal static long UnixMs(DateTime utc)
        {
            return new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeMilliseconds();
        }
    }
}
