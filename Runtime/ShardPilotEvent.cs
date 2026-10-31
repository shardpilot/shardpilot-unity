#nullable enable annotations
using System;
using System.Collections.Generic;

namespace ShardPilot
{
    public sealed class ShardPilotEvent
    {
        public string? Id { get; set; }
        public string? Name { get; set; }
        public int? SchemaVersion { get; set; }
        public DateTime TimestampUtc { get; set; }
        public string? UserId { get; set; }
        public string? AnonymousId { get; set; }
        public string? SessionId { get; set; }
        public long SessionSequence { get; set; }
        public string? Platform { get; set; }
        public string? AppVersion { get; set; }
        public string? AppBuild { get; set; }
        public string? OSName { get; set; }
        public string? OSVersion { get; set; }
        public string? DeviceModel { get; set; }
        public string? Locale { get; set; }
        public string? Country { get; set; }
        public string? ReleaseVersion { get; set; }
        public IDictionary<string, object?>? Context { get; set; }
        public IDictionary<string, object?>? Props { get; set; }

        /// <summary>
        /// The exact envelope JSON this event ships as inside the batch body:
        /// stamped exactly once — the FIRST serializer to produce one wins a
        /// compare-exchange (see ShardPilotJson.CanonicalEnvelope) and every
        /// racing or later caller uses that winner — or set verbatim when the
        /// event is restored from the offline spool. When present, the
        /// serializer emits it VERBATIM instead of re-writing the envelope,
        /// so every attempt — wire send, spool copy, re-send on a later
        /// launch — is the SAME string: same <c>event_id</c>, same
        /// <c>event_ts</c>, same everything, and the ingest service can
        /// de-duplicate by the stable event id. A FIELD (not a property) so
        /// the stamp can be an Interlocked compare-exchange.
        /// </summary>
        internal string? SerializedEnvelope;
        // Checked intake bytes; unlike SerializedEnvelope, not a replay marker.
        internal string? AdmittedEnvelope;
        // Capture caller-owned nested data before any owed lifecycle work.
        internal string CapturedProps, CapturedContext;

        /// <summary>
        /// Order of acceptance into this client's queue, starting at 1; 0 for
        /// an event restored from the spool. A flush answers for the events
        /// at or below the sequence current at its call.
        /// </summary>
        internal long IntakeSequence;
        // Process-local close ownership, stamped by the client at admission.
        // Deliberately neither cloned from caller input nor serialized.
        internal object? CloseOwner;
        // Process-local creation basis; owned copies retain it, wire/storage omit it.
        internal object? NonObjectionOrigin;
        internal ShardPilotSealedExperimentFact? SealedFact;

        internal ShardPilotAnalyticsClock? AnalyticsClock;

        internal ShardPilotEvent Clone()
        {
            return new ShardPilotEvent
            {
                Id = Id,
                Name = Name,
                SchemaVersion = SchemaVersion,
                TimestampUtc = TimestampUtc,
                UserId = UserId,
                AnonymousId = AnonymousId,
                SessionId = SessionId,
                SessionSequence = SessionSequence,
                Platform = Platform,
                AppVersion = AppVersion,
                AppBuild = AppBuild,
                OSName = OSName,
                OSVersion = OSVersion,
                DeviceModel = DeviceModel,
                Locale = Locale,
                Country = Country,
                ReleaseVersion = ReleaseVersion,
                SerializedEnvelope = SerializedEnvelope,
                AdmittedEnvelope = AdmittedEnvelope,
                CapturedProps = CapturedProps,
                CapturedContext = CapturedContext,
                SealedFact = SealedFact,
                AnalyticsClock = AnalyticsClock,
                NonObjectionOrigin = NonObjectionOrigin,
                Context = CloneMap(Context),
                Props = CloneMap(Props)
            };
        }

        internal static Dictionary<string, object?>? CloneMap(IDictionary<string, object?>? input)
        {
            if (input == null || input.Count == 0)
            {
                return null;
            }

            var copy = new Dictionary<string, object>(input.Count, StringComparer.Ordinal);
            foreach (var pair in input)
            {
                copy[pair.Key] = pair.Value;
            }

            return copy;
        }
    }
}
