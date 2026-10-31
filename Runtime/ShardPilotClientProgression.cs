// Input maps accept both existing non-null-value and nullable-value dictionaries.
// The invariant value argument stays oblivious; the optional map is annotated.
#nullable disable annotations
using EventProperties = System.Collections.Generic.IDictionary<string, object>;
#nullable enable annotations
using System;
using System.Collections.Generic;

namespace ShardPilot
{
    /// <summary>
    /// The typed progression verbs: <c>level_start</c>, <c>level_complete</c>
    /// and <c>level_fail</c> built from typed arguments instead of a
    /// hand-assembled props map, delivered through the same pipeline as
    /// <see cref="Track"/> — the consent-first gate, the session attach, the
    /// queue, the spool and the batch rules apply unchanged. Each verb
    /// validates what the canonical schema requires
    /// (analytics.level_start.v1 / level_complete.v1 / level_fail.v1) and
    /// refuses with a code instead of queueing an event the ingest would
    /// reject. The <c>out string error</c> overloads name the refusal; the
    /// short forms keep <see cref="Track"/>'s bool contract.
    /// </summary>
    public sealed partial class ShardPilotClient
    {
        private const string LevelStartEventName = "level_start";
        private const string LevelCompleteEventName = "level_complete";
        private const string LevelFailEventName = "level_fail";

        /// <summary>The schema's bounds: attempt is a count from 1 that fits
        /// 16 bits; duration and score are unsigned 32-bit integers.</summary>
        private const int MaxLevelAttempt = 65535;
        private const long MaxLevelUnsigned32 = 4294967295L;

        public bool TrackLevelStart(string levelId, int attempt, EventProperties? props = null)
        {
            string ignored;
            return TrackLevelStart(levelId, attempt, props, out ignored);
        }

        /// <summary>Emits <c>level_start</c>: <paramref name="levelId"/>
        /// (trimmed, required) and <paramref name="attempt"/> (1–65535).
        /// Extras in <paramref name="props"/> ride along; the typed keys are
        /// written after them and win.</summary>
        public bool TrackLevelStart(string levelId, int attempt, EventProperties? props, out string? error)
        {
            Dictionary<string, object> typed;
            if (!BuildLevelProps(levelId, attempt, props, out typed, out error))
            {
                return false;
            }

            return EnqueueTypedEvent(LevelStartEventName, typed, out error);
        }

        public bool TrackLevelComplete(string levelId, int attempt, long durationMs, long? score = null, EventProperties? props = null)
        {
            string ignored;
            return TrackLevelComplete(levelId, attempt, durationMs, score, props, out ignored);
        }

        /// <summary>Emits <c>level_complete</c>: the <c>level_start</c> fields
        /// plus <paramref name="durationMs"/> (0–4294967295) and an optional
        /// <paramref name="score"/> in the same range, omitted from the wire
        /// when not given rather than sent as 0.</summary>
        public bool TrackLevelComplete(string levelId, int attempt, long durationMs, long? score, EventProperties? props, out string? error)
        {
            Dictionary<string, object> typed;
            if (!BuildLevelProps(levelId, attempt, props, out typed, out error))
            {
                return false;
            }

            if (durationMs < 0 || durationMs > MaxLevelUnsigned32)
            {
                error = "invalid_duration";
                return false;
            }

            if (score.HasValue && (score.Value < 0 || score.Value > MaxLevelUnsigned32))
            {
                error = "invalid_score";
                return false;
            }

            typed["duration_ms"] = durationMs;
            if (score.HasValue)
            {
                typed["score"] = score.Value;
            }
            else
            {
                // An absent score means the key is ABSENT, even when the
                // caller's extras carry one: the promise is that nothing is
                // sent, and the schema types score as an integer, so an
                // inherited string would be rejected per event inside an
                // accepted batch.
                typed.Remove("score");
            }

            return EnqueueTypedEvent(LevelCompleteEventName, typed, out error);
        }

        public bool TrackLevelFail(string levelId, int attempt, long durationMs, string? failReason = null, EventProperties? props = null)
        {
            string ignored;
            return TrackLevelFail(levelId, attempt, durationMs, failReason, props, out ignored);
        }

        /// <summary>Emits <c>level_fail</c>: the <c>level_start</c> fields
        /// plus <paramref name="durationMs"/> (0–4294967295) and an optional
        /// <paramref name="failReason"/> (trimmed; omitted when empty).</summary>
        public bool TrackLevelFail(string levelId, int attempt, long durationMs, string? failReason, EventProperties? props, out string? error)
        {
            Dictionary<string, object> typed;
            if (!BuildLevelProps(levelId, attempt, props, out typed, out error))
            {
                return false;
            }

            if (durationMs < 0 || durationMs > MaxLevelUnsigned32)
            {
                error = "invalid_duration";
                return false;
            }

            typed["duration_ms"] = durationMs;
            var reason = ShardPilotConfig.Trim(failReason);
            if (!string.IsNullOrEmpty(reason))
            {
                typed["fail_reason"] = reason;
            }
            else
            {
                // As for score: an empty reason erases an inherited extra
                // rather than letting it ride under the reserved key.
                typed.Remove("fail_reason");
            }

            return EnqueueTypedEvent(LevelFailEventName, typed, out error);
        }

        /// <summary>Validates the fields every progression event requires and
        /// shapes the props: the caller's extras first (a defensive copy; the
        /// caller's map is never mutated), the typed keys after them, so a
        /// typed field cannot be contradicted through the map.</summary>
        private static bool BuildLevelProps(string levelId, int attempt, IDictionary<string, object> props, out Dictionary<string, object> typed, out string error)
        {
            typed = null;
            error = null;

            var id = ShardPilotConfig.Trim(levelId);
            if (string.IsNullOrEmpty(id))
            {
                error = "level_id_required";
                return false;
            }

            if (attempt < 1 || attempt > MaxLevelAttempt)
            {
                error = "invalid_attempt";
                return false;
            }

            typed = ShardPilotEvent.CloneMap(props) ?? new Dictionary<string, object>(4, StringComparer.Ordinal);
            typed["level_id"] = id;
            typed["attempt"] = attempt;
            return true;
        }

        /// <summary>Delivers a typed event exactly as <see cref="Track"/>
        /// does, naming the refusal: <c>shutdown</c>; the consent gate's own
        /// <c>consent_denied</c> / <c>consent_unknown</c>, recorded as a
        /// refusal exactly as the enqueue gate records them; or
        /// <c>event_too_large</c> for an envelope exceeding 2048 UTF-8 bytes; or
        /// <c>not_enqueued</c> when the pipeline refused after those checks
        /// passed — the client closed or the decision changed in the same
        /// instant, or a full queue with no spool (the stats say which).</summary>
        private bool EnqueueTypedEvent(string eventName, Dictionary<string, object> props, out string error)
        {
            error = null;
            if (_config.Source != ShardPilotSource.Client)
            {
                // The progression schemas pin source to the constant client
                // and the envelope carries the CONFIGURED source, so a
                // dedicated server's events could never satisfy the schema —
                // they would be rejected per event inside an accepted batch.
                // The Go SDK pins its two backend-lane verbs the same way.
                error = "source_not_client";
                return false;
            }

            lock (_gate)
            {
                if (_closed)
                {
                    error = "shutdown";
                    return false;
                }

                if (!TypedAnalyticsAdmittedLocked(eventName))
                {
                    error = ShardPilotConsent.IsDeniedState(_consentState)
                        ? ShardPilotConsent.DeniedError
                        : ShardPilotConsent.UnknownError;
                    _stats.RecordRefusal(error);
                    return false;
                }
            }

            string refusal = null;
            if (EnqueueEvent(new ShardPilotEvent { Name = eventName, Props = props }, refusal: code => refusal = code))
            {
                return true;
            }

            error = refusal ?? "not_enqueued";
            return false;
        }
    }
}
