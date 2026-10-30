using System;
using System.Collections.Generic;
using System.Threading;

namespace ShardPilot
{
    /// <summary>
    /// Local analytics consent state. CONSENT-FIRST: only an explicit
    /// Granted decision opens the analytics pipeline. Unknown (never
    /// decided, or the persisted decision could not be read) refuses events
    /// at enqueue exactly like Denied does — nothing is queued, nothing is
    /// spooled, and no analytics bytes reach the wire until the app records
    /// a grant through <see cref="ShardPilotClient.SetConsent"/>.
    /// </summary>
    public enum ShardPilotConsentState
    {
        Unknown = 0,
        Granted = 1,
        Denied = 2,

        /// <summary>
        /// An age-gate-forced denial: analytics-wise IDENTICAL to
        /// <see cref="Denied"/> — every gate treats both as the same denied
        /// state and refusals report the same <c>consent_denied</c> reason.
        /// The only difference is on the consent receipt, which carries
        /// <c>reason = "denied_forced_minor"</c> so the backend can tell a
        /// band-forced denial from a chosen one. Persisted and reloaded as
        /// its own state; ordinary decisions cannot remove the restriction.
        /// </summary>
        DeniedForcedMinor = 3
    }

    /// <summary>
    /// An explicit consent decision for
    /// <see cref="ShardPilotClient.SetConsent(ShardPilotConsentDecision)"/>.
    /// The accepted decisions are Granted, Denied and DeniedForcedMinor;
    /// anything outside this enum is refused
    /// with <c>invalid_consent</c>. Feature-detect the forced-minor member
    /// with <c>ShardPilotSdk.Supports("consent_state_denied_forced_minor")</c>.
    /// </summary>
    public enum ShardPilotConsentDecision
    {
        Unset = 0,
        Granted = 1,
        Denied = 2,
        DeniedForcedMinor = 3
    }

    /// <summary>
    /// The durable withheld-grant obligation, reloaded at construction —
    /// see <see cref="ShardPilotConsent.WithheldGrantStorageKeyPrefix"/>.
    /// </summary>
    internal sealed class ShardPilotWithheldGrantMarker
    {
        internal readonly string IdempotencyKey;
        internal readonly string DecidedAt;

        /// <summary>
        /// The obligation's minted decision sequence: carried so the standing-denial-vs-obligation ordering can
        /// compare FULL pairs — a same-stamp deny→grant (real on coarse
        /// UtcNow timers) is decided by the seq, exactly like every other
        /// pair comparison. 0 = a marker written before the seq rode along
        /// (or a tampered value): stamp-only ordering with ties to the
        /// denial, the fail-closed direction.
        /// </summary>
        internal readonly long DecisionSeq;

        internal readonly bool ReceiptDurable;
        internal readonly ShardPilotConsentNoticeSnapshot Notice;

        internal ShardPilotWithheldGrantMarker(
            string idempotencyKey, string decidedAt, long decisionSeq, bool receiptDurable,
            ShardPilotConsentNoticeSnapshot notice = null)
        {
            IdempotencyKey = idempotencyKey;
            DecidedAt = decidedAt;
            DecisionSeq = decisionSeq;
            ReceiptDurable = receiptDurable;
            Notice = notice;
        }
    }

    /// <summary>
    /// The FULL restored consent record: the state plus the decision pair
    /// and decision-time anonymous id the modern record shape carries. A
    /// LEGACY record (bare pre-pair value, absent record, or a modern
    /// record whose pair failed the plausibility gate) has
    /// <see cref="Legacy"/> true and no usable pair — legacy pairs never
    /// outrank stamped ones in any comparison.
    /// </summary>
    internal sealed class ShardPilotConsentRecord
    {
        internal readonly ShardPilotConsentState State;
        internal readonly string AnonymousId;
        internal readonly string DecidedAt;
        internal readonly long DecisionSeq;
        internal readonly bool Legacy;

        internal ShardPilotConsentRecord(
            ShardPilotConsentState state, string anonymousId, string decidedAt, long decisionSeq, bool legacy)
        {
            State = state;
            AnonymousId = anonymousId;
            DecidedAt = decidedAt;
            DecisionSeq = decisionSeq;
            Legacy = legacy;
        }

        internal static ShardPilotConsentRecord LegacyRecord(ShardPilotConsentState state)
        {
            return new ShardPilotConsentRecord(state, null, null, 0, legacy: true);
        }
    }

    /// <summary>
    /// The reloaded denial-marker witness — see
    /// <see cref="ShardPilotConsent.DenialMarkerStorageKey"/>. A MANGLED
    /// slot (unparseable, non-denied state) reloads with
    /// <see cref="Mangled"/> set so the caller takes the transient
    /// fail-closed arm — mangling never launders a denial witness away,
    /// but unjudgeable content also never imposes a definite flavor or
    /// rewrites anything durable.
    /// </summary>
    internal sealed class ShardPilotDenialMarker
    {
        internal readonly ShardPilotConsentState State;
        internal readonly string AnonymousId;
        internal readonly string DecidedAt;
        internal readonly long DecisionSeq;
        internal readonly bool Legacy;

        /// <summary>
        /// The slot held NON-EMPTY content that could not even name a
        /// denied state (garbled JSON, an unrecognized state value). A
        /// mangled occupant may still witness a denial but is UNJUDGEABLE:
        /// it takes the TRANSIENT fail-closed arm (over a granted restore
        /// only — no imposition of a definite flavor, no record rewrite,
        /// no retirement on this launch's evidence), exactly like a store
        /// read refusal.
        /// </summary>
        internal readonly bool Mangled;

        internal ShardPilotDenialMarker(
            ShardPilotConsentState state, string anonymousId, string decidedAt, long decisionSeq, bool legacy)
            : this(state, anonymousId, decidedAt, decisionSeq, legacy, mangled: false)
        {
        }

        private ShardPilotDenialMarker(
            ShardPilotConsentState state,
            string anonymousId,
            string decidedAt,
            long decisionSeq,
            bool legacy,
            bool mangled)
        {
            State = state;
            AnonymousId = anonymousId;
            DecidedAt = decidedAt;
            DecisionSeq = decisionSeq;
            Legacy = legacy;
            Mangled = mangled;
        }

        internal static ShardPilotDenialMarker CreateMangled()
        {
            return new ShardPilotDenialMarker(
                ShardPilotConsentState.Denied, null, null, 0, legacy: true, mangled: true);
        }
    }

    internal static class ShardPilotConsent
    {
        public const string StorageKey = "shardpilot.consent.analytics";

        // Independent of the latest decision and its retired receipt witnesses.
        // Ordinary consent writes and identity downgrades never clear this key.
        internal const string ForcedMinorStorageKey = "shardpilot.consent.forced-minor";

        internal static bool HasForcedMinorRestriction(IShardPilotKeyValueStore store, out bool readFailed)
        {
            readFailed = false;
            if (store == null) return false;
            try
            {
                var value = ShardPilotConfig.Trim(store.Get(ForcedMinorStorageKey));
                if (string.IsNullOrEmpty(value)) return false;
                if (value == ForcedMinorReason) return true;
            }
            catch
            {
                // An unreadable restriction cannot authorize an ordinary grant.
            }
            readFailed = true;
            return false;
        }

        /// <summary>
        /// The DENIAL-MARKER witness slot in key-value persistence: a
        /// sibling key beside the consent record, written AHEAD of a
        /// denial's state-first persist so a kill between the decision and
        /// its record write still leaves a durable witness. ONE slot,
        /// global like the consent record itself (the single-slot rule: a
        /// new denial's own write-ahead legitimately supersedes whatever
        /// the slot held). Retired only against durable convergence — the
        /// record write that carries the same-or-newer decision pair — or
        /// by the boot stale-guard; a valid FOREIGN-actor marker is kept
        /// inert. Written/retired only under the process-wide decision
        /// gate (boot imposition runs in the constructor, which the gate
        /// serializes against decisions via registration).
        /// </summary>
        public const string DenialMarkerStorageKey = "shardpilot.consent.denial-marker";

        /// <summary>
        /// The owed denial-witness backup holds the marker whose write was
        /// refused. It is independent of outbox scope so that a denial
        /// propagated across scopes remains recoverable.
        /// </summary>
        /// <summary>
        /// Prefix of the OWED denial-witness backup keys. PER ACTOR, because
        /// the obligation is: the marker SLOT holds
        /// one occupant by an older design, but a backup that also held one
        /// could not cover a second actor whose denial is refused at the same
        /// time — and a receipt-less actor has nothing else. The actor rides
        /// the key as a digest, since an anonymous id is length-checked but
        /// otherwise unconstrained and must never reach a storage key raw.
        /// </summary>
        public const string OwedDenialWitnessStorageKeyPrefix = "shardpilot.consent.owed-denial-witness.";

        internal static string OwedDenialWitnessStorageKeyFor(string anonymousId)
        {
            return OwedDenialWitnessStorageKeyPrefix +
                   ShardPilotConsentOutbox.ScopeHash(anonymousId ?? string.Empty, string.Empty, string.Empty);
        }

        private const string GrantedValue = "granted";
        private const string DeniedValue = "denied";
        private const string DeniedForcedMinorValue = "denied_forced_minor";

        /// <summary>
        /// The decision-seq mint ceiling: sequences
        /// saturate ONE below long.MaxValue so a floor raised from a
        /// corrupt/hostile stored value can never make the next mint
        /// overflow, and any stored value above the ceiling clamps to it.
        /// </summary>
        internal const long MaxConsentDecisionSeq = long.MaxValue - 1;

        private static long _decisionSeqCounter;

        /// <summary>
        /// The receipt <c>reason</c> a forced-minor denial carries — the only
        /// reason value this SDK ever produces.
        /// </summary>
        public const string ForcedMinorReason = "denied_forced_minor";

        /// <summary>
        /// The distinct refusal reason surfaced (via
        /// <see cref="ShardPilotStats.LastError"/> and the SDK log) when an
        /// event is refused because no consent decision has been recorded
        /// yet. Distinct from <see cref="DeniedError"/> so an integrator can
        /// tell "ask the player" apart from "the player said no".
        /// </summary>
        public const string UnknownError = "consent_unknown";

        /// <summary>
        /// The distinct refusal reason surfaced when an event is refused
        /// because the recorded consent decision is a denial.
        /// </summary>
        public const string DeniedError = "consent_denied";

        /// <summary>
        /// PROCESS-WIDE shadows of DENIALS that storage never took (mirroring
        /// <c>ShardPilotCrashSettings</c>' pending opt-out): the decision
        /// applies to the session, and the session is the process — a
        /// same-process re-initialization (or the registration-time refresh)
        /// must not reload the stale persisted grant and reopen the pipeline
        /// the player just closed. Keyed by the store's backend identity and
        /// the deciding actor, so another store's or another actor's write
        /// never clears it. Armed by a failed DENIAL persist (plain or
        /// forced-minor — the shadow keeps which) and by a client that tears
        /// down with the denial still unwitnessed; cleared by the next
        /// successful persist of that actor's explicit decision on that store.
        /// The inverse direction is deliberately never shadowed — a failed
        /// GRANT persist leaves later constructions fail-closed on the
        /// stored value. Guarded by <see cref="SessionDenialGate"/>.
        /// </summary>
        private static readonly Dictionary<string, SessionDenial> SessionDenials =
            new Dictionary<string, SessionDenial>(StringComparer.Ordinal);

        private static readonly object SessionDenialGate = new object();

        private sealed class SessionDenial
        {
            internal ShardPilotConsentState State;
            internal string DecidedAt;
            internal long DecisionSeq;
        }

        private static string SessionDenialKey(IShardPilotKeyValueStore store, string anonymousId)
        {
            return ShardPilotConsentOutbox.StoreIdentityToken(store) + "|" + (anonymousId ?? string.Empty);
        }

        private static void ArmSessionDenial(IShardPilotKeyValueStore store, string anonymousId,
            ShardPilotConsentState state, string decidedAt, long decisionSeq)
        {
#if SHARDPILOT_SDK_TEST_SUPPORT
            if (SuppressSessionDenialArmingForTesting) return;
#endif
            lock (SessionDenialGate)
            {
                SessionDenials[SessionDenialKey(store, anonymousId)] =
                    new SessionDenial { State = state, DecidedAt = decidedAt, DecisionSeq = decisionSeq };
            }
        }

        /// <summary>
        /// Keeps a standing denial in force for the rest of the process when
        /// a client tears down with it still unwitnessed in storage: a later
        /// client on the same store and actor boots denied and takes over the
        /// owed writes (<see cref="TryGetSessionDenial"/>).
        /// </summary>
        internal static void KeepSessionDenial(IShardPilotKeyValueStore store, string anonymousId,
            ShardPilotConsentState state, string decidedAt, long decisionSeq)
        {
            if (store != null && IsDeniedState(state))
            {
                ArmSessionDenial(store, anonymousId, state, decidedAt, decisionSeq);
            }
        }

        /// <summary>
        /// The denial an earlier client in this process left unwitnessed for
        /// this store and actor, if any.
        /// </summary>
        internal static bool TryGetSessionDenial(IShardPilotKeyValueStore store, string anonymousId,
            out string decidedAt, out long decisionSeq)
        {
            ShardPilotConsentState state;
            return TryGetSessionDenial(store, anonymousId, out state, out decidedAt, out decisionSeq);
        }

        internal static bool TryGetSessionDenial(IShardPilotKeyValueStore store, string anonymousId,
            out ShardPilotConsentState state, out string decidedAt, out long decisionSeq)
        {
            state = ShardPilotConsentState.Unknown;
            decidedAt = null;
            decisionSeq = 0;
            if (store == null) return false;
            lock (SessionDenialGate)
            {
                SessionDenial entry;
                if (!SessionDenials.TryGetValue(SessionDenialKey(store, anonymousId), out entry)) return false;
                state = entry.State;
                decidedAt = entry.DecidedAt;
                decisionSeq = entry.DecisionSeq;
                return true;
            }
        }

        /// <summary>
        /// The shadowed denial that rules this store's record: the record
        /// actor's own, else a pairless one. With no readable actor, any
        /// shadow on the store. Unknown when none.
        /// </summary>
        private static Dictionary<string, SessionDenial> CaptureSessionDenials(IShardPilotKeyValueStore store)
        {
            var captured = new Dictionary<string, SessionDenial>(StringComparer.Ordinal);
            if (store == null) return captured;
            var prefix = SessionDenialKey(store, null);
            lock (SessionDenialGate)
            {
                foreach (var pair in SessionDenials)
                {
                    if (pair.Key.StartsWith(prefix, StringComparison.Ordinal)) captured.Add(pair.Key, pair.Value);
                }
            }
            return captured;
        }

        private static ShardPilotConsentState SessionDenialFor(IShardPilotKeyValueStore store, string anonymousId,
            Dictionary<string, SessionDenial> captured = null)
        {
            lock (SessionDenialGate)
            {
                var entries = captured ?? SessionDenials;
                if (entries.Count == 0) return ShardPilotConsentState.Unknown;
                SessionDenial entry;
                if (!string.IsNullOrEmpty(anonymousId))
                {
                    if (entries.TryGetValue(SessionDenialKey(store, anonymousId), out entry) ||
                        entries.TryGetValue(SessionDenialKey(store, null), out entry))
                    {
                        return entry.State;
                    }

                    return ShardPilotConsentState.Unknown;
                }

                var prefix = SessionDenialKey(store, null);
                foreach (var pair in entries)
                {
                    if (pair.Key.StartsWith(prefix, StringComparison.Ordinal)) return pair.Value.State;
                }

                return ShardPilotConsentState.Unknown;
            }
        }

        /// <summary>
        /// A durable explicit decision supersedes the store's pending shadow
        /// for that actor (and a pairless one); a pairless decision clears
        /// every shadow on the store.
        /// </summary>
        private static void ClearSessionDenial(IShardPilotKeyValueStore store, string anonymousId, bool pairless)
        {
            lock (SessionDenialGate)
            {
                if (SessionDenials.Count == 0) return;
                if (!pairless)
                {
                    SessionDenials.Remove(SessionDenialKey(store, anonymousId));
                    SessionDenials.Remove(SessionDenialKey(store, null));
                    return;
                }

                var prefix = SessionDenialKey(store, null);
                var keys = new List<string>();
                foreach (var key in SessionDenials.Keys)
                {
                    if (key.StartsWith(prefix, StringComparison.Ordinal)) keys.Add(key);
                }

                for (var i = 0; i < keys.Count; i++) SessionDenials.Remove(keys[i]);
            }
        }

#if SHARDPILOT_SDK_TEST_SUPPORT
        // HEADLESS TEST BUILD ONLY, and held ONLY across a test teardown by
        // ShardPilotTestIsolationAttribute. A write that already passed a
        // client's own quiesce fence and is still inside the store call can
        // otherwise arm the pending denial AFTER the teardown's reset has run,
        // and then the next test reads a denial nobody made. Per-client fencing
        // cannot cover that one: the decision to write predates the fence.
        internal static volatile bool SuppressSessionDenialArmingForTesting;
#endif

        /// <summary>
        /// True for both denied flavors: every analytics/crash gate treats
        /// <see cref="ShardPilotConsentState.DeniedForcedMinor"/> exactly
        /// like <see cref="ShardPilotConsentState.Denied"/>.
        /// </summary>
        public static bool IsDeniedState(ShardPilotConsentState state)
        {
            return state == ShardPilotConsentState.Denied ||
                   state == ShardPilotConsentState.DeniedForcedMinor;
        }

        /// <summary>
        /// PROCESS-WIDE marker: an explicit GRANT was decided in this
        /// session and its per-scope consent receipts are being distributed.
        /// Receipt scope (workspace_id/app_id/environment_id) is baked into
        /// each receipt body, so every client records ITS OWN scope's
        /// receipt; the propagation snapshot only reaches clients that were
        /// live and registered when the grant was decided, and a client
        /// constructed/registered AFTER it consults this marker during its
        /// registration catch-up to arm its own scope's receipt.
        /// Written only under the process-wide decision gate (SetConsent
        /// and the registration refresh are mutually exclusive on it): a
        /// grant sets it, a denial clears it. It deliberately stays set for
        /// the rest of the session after a grant — a scope first seen late
        /// still needs its own consent row, and an extra receipt for a scope
        /// that already has one carries the identical decision (a fresh
        /// idempotency key), which the endpoint accepts harmlessly. It is
        /// never set by merely LOADING a persisted grant at construction —
        /// receipts exist only for explicit in-session decisions. The
        /// marker carries the grant's ORIGINAL decision time: every receipt
        /// armed from it — propagated or late-registered — stamps
        /// <c>decided_at</c> with the moment the player actually decided,
        /// never the arming/flush time, so the consent audit row is honest.
        /// The moment and immutable notice travel in one reference, atomically
        /// published so readers cannot combine two different decisions.
        /// </summary>
        internal sealed class SessionDecision
        {
            internal readonly DateTime DecidedAtUtc;
            internal readonly ShardPilotConsentNoticeSnapshot Notice;
            internal SessionDecision(DateTime decidedAtUtc, ShardPilotConsentNoticeSnapshot notice)
            { DecidedAtUtc = decidedAtUtc; Notice = notice; }
        }

        private static SessionDecision _sessionGrantDecision;
        internal static SessionDecision SessionGrantDecision { get { return Volatile.Read(ref _sessionGrantDecision); } }
        internal static bool SessionGrantReceiptsActive { get { return SessionGrantDecision != null; } }
        internal static DateTime? SessionGrantDecidedAtUtc
        {
            get { var decision = SessionGrantDecision; return decision == null ? (DateTime?)null : decision.DecidedAtUtc; }
        }
        internal static void RecordSessionGrantDecision(DateTime? decidedAtUtc, ShardPilotConsentNoticeSnapshot notice = null)
        {
            Volatile.Write(ref _sessionGrantDecision, decidedAtUtc.HasValue ? new SessionDecision(decidedAtUtc.Value, notice) : null);
        }

        /// <summary>
        /// Process-wide marker for a LIVE in-session DENIAL decision — the
        /// mirror of the grant marker above, carrying the denial's original
        /// decision moment. Consulted by the registration catch-up: a client
        /// registering AFTER a live denial's propagation snapshot reads the
        /// persisted denied state, and the never-mint-on-load rule is
        /// scoped to RELAUNCH-loaded decisions — a SAME-SESSION catch-up of
        /// a live denial is still that in-session decision arriving late, so
        /// the late registrant appends its own scope's denial receipt (with
        /// this original moment) when its trail would otherwise end granted.
        /// A relaunch has no marker (process state) and mints nothing, and a
        /// later grant clears it — the newest decision is the operative one.
        /// </summary>
        private static SessionDecision _sessionDenialDecision;
        internal static SessionDecision SessionDenialDecision { get { return Volatile.Read(ref _sessionDenialDecision); } }
        internal static DateTime? SessionDenialDecidedAtUtc
        {
            get { var decision = SessionDenialDecision; return decision == null ? (DateTime?)null : decision.DecidedAtUtc; }
        }
        internal static void RecordSessionDenialDecision(DateTime? decidedAtUtc, ShardPilotConsentNoticeSnapshot notice = null)
        {
            Volatile.Write(ref _sessionDenialDecision, decidedAtUtc.HasValue ? new SessionDecision(decidedAtUtc.Value, notice) : null);
        }

        /// <summary>
        /// Test seam: clears the process-wide pending denial and the
        /// process-wide grant/denial decision markers.
        /// </summary>
        internal static void ResetSessionStateForTesting()
        {
            lock (SessionDenialGate)
            {
                SessionDenials.Clear();
            }

            RecordSessionGrantDecision(null);
            RecordSessionDenialDecision(null);
            Interlocked.Exchange(ref _decisionSeqCounter, 0);
            lock (DowngradeGate)
            {
                PendingDowngradeStores.Clear();
            }
        }

        // Published before a decision applies, persists, and propagates. The
        // outermost call owns completion, including any reentrant decisions.
        internal sealed class DecisionWork
        {
            private readonly object _gate = new object();
            private bool _completed;
            private ShardPilotConsentState _state;
            private string _stamp;
            private long _sequence;
            internal bool Completed => Volatile.Read(ref _completed);

            internal void Accept(ShardPilotConsentState state, string stamp, long sequence)
            {
                lock (_gate) { _state = state; _stamp = stamp; _sequence = sequence; }
            }

            internal void Complete()
            {
                lock (_gate) Volatile.Write(ref _completed, true);
            }

            internal void KeepDenial(IShardPilotKeyValueStore store, string actor)
            {
                lock (_gate)
                {
                    // Completion and a deadline shadow serialize: a later grant
                    // cannot clear its shadow and then have this denial rearm it.
                    if (!_completed && IsDeniedState(_state))
                        KeepSessionDenial(store, actor, _state, _stamp, _sequence);
                }
            }
        }

        private static DecisionWork _decisionWork;
        internal static DecisionWork CurrentDecision => Volatile.Read(ref _decisionWork);

        // Caller holds PrivacyDecisionGate; close only reads the published work.
        internal static DecisionWork BeginDecision(out bool owner)
        {
            var work = CurrentDecision;
            owner = work == null;
            if (owner) Volatile.Write(ref _decisionWork, work = new DecisionWork());
            return work;
        }

        internal static void EndDecision(DecisionWork work, bool owner)
        {
            if (!owner) return;
            work.Complete();
            Volatile.Write(ref _decisionWork, null);
        }

        public static ShardPilotConsentState Load(IShardPilotKeyValueStore store)
        {
            bool readFailed;
            return Load(store, out readFailed);
        }

        /// <summary>
        /// Loads the persisted consent decision. An ABSENT record resolves
        /// to Unknown with <paramref name="readFailed"/> false (the player
        /// has simply not decided yet); a store READ FAILURE also resolves
        /// to Unknown but reports <paramref name="readFailed"/> true — the
        /// stored decision (possibly a denial) is unreadable, so the caller
        /// must fail closed: under consent-first an Unknown state never
        /// transmits, and the constructor additionally purges the offline
        /// spool for every non-granted resolution (see the strengthened
        /// spool rule in <see cref="ShardPilotClient"/>).
        /// </summary>
        public static ShardPilotConsentState Load(IShardPilotKeyValueStore store, out bool readFailed)
        {
            return Load(store, null, out readFailed);
        }

        // A client selects shadows by its own actor, even when the shared
        // durable record is absent, unreadable, or belongs to another actor.
        internal static ShardPilotConsentState Load(IShardPilotKeyValueStore store, string anonymousId,
            out bool readFailed)
        {
            if (HasForcedMinorRestriction(store, out readFailed))
            {
                return ShardPilotConsentState.DeniedForcedMinor;
            }
            if (readFailed) return ShardPilotConsentState.Unknown;

            // The raw durable truth — legacy bare values, the modern
            // pair-carrying shape, absent, unreadable, and corrupted all
            // resolve through LoadRecord with the same fail-closed
            // semantics the pre-pair switch had (a NON-EMPTY unrecognized
            // value is a corrupted record, not an absent one: the stored
            // decision may have been a denial, so it reports as a READ
            // FAILURE and the caller fails closed — spools purge and the
            // crash plane holds).
            // A healing write persists the denial before clearing its shadow.
            // Preserve shadows present before this read, so a captured stale
            // grant cannot win if that heal clears the shadow during the read.
            var capturedDenials = CaptureSessionDenials(store);
            var record = LoadRecord(store, out readFailed);

            // A denial recorded earlier in this process that storage never
            // took outranks whatever the store holds: it is the player's
            // latest decision, and only a successful durable persist (which
            // clears it) supersedes it. Reported as a clean DENIED read — the
            // in-process decision is known, not unreadable — preserving the
            // forced-minor flavor when that is what failed to persist.
            var shadowActor = anonymousId ?? (readFailed ? null : record.AnonymousId);
            var pendingDenial = SessionDenialFor(store, shadowActor);
            if (!IsDeniedState(pendingDenial))
            {
                pendingDenial = SessionDenialFor(store, shadowActor, capturedDenials);
            }
            if (IsDeniedState(pendingDenial))
            {
                readFailed = false;
                return pendingDenial;
            }

            return record.State;
        }

        /// <summary>
        /// Clears the persisted decision record back to the ABSENT state
        /// (loads as Unknown — fresh consent required). Used by the Mode-B
        /// identity-binding fail-closed path: an identity change that drops
        /// a PENDING grant receipt must not leave a persisted grant behind
        /// whose consent row can never reach the backend. Returns false when
        /// the durable clear failed — the caller still downgrades in memory,
        /// and this session stays gated regardless.
        /// </summary>
        internal static bool Clear(IShardPilotKeyValueStore store)
        {
            if (store == null)
            {
                return true;
            }

            try
            {
                store.Set(StorageKey, string.Empty);
            }
            catch
            {
                return false;
            }

            // VERIFY-AFTER-CLEAR: a dual-backend store can report an EMPTY
            // write as success while a fallback copy still wins the next
            // read — the PlayerPrefs store's empty-value clear of its
            // fallback file is deliberately best-effort (for the fail-closed
            // markers a surviving copy is the safe direction), but for THIS
            // key a surviving "granted" fallback is fail-OPEN: the shadow
            // would drop and the next load would resurrect the receipt-less
            // grant. The clear therefore counts only when a re-read through
            // the same store — exercising exactly the dual-source path a
            // future load takes — no longer serves a decided value; an
            // unverifiable re-read counts as NOT cleared (the caller's
            // shadow stays, and Load's own read-failure path fails closed).
            try
            {
                return string.IsNullOrEmpty(ShardPilotConfig.Trim(store.Get(StorageKey)));
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Fail-closed shadow for a FAILED identity-downgrade clear, keyed
        /// by backend identity: while a store's downgrade write is still
        /// owed, its stale persisted grant keeps loading as "granted" — and
        /// the registration refresh would happily re-apply it, resurrecting
        /// the receipt-less grant the downgrade exists to prevent.
        /// Consulted by the client's construction and refresh (both treat
        /// the stale grant as downgraded while the shadow is set); the
        /// durable clear retries at the consent dispatch points, and ANY
        /// successful explicit Save on the store supersedes the shadow (the
        /// fresh decision is the operative record).
        ///
        /// TWO layers, because the shadow must survive a PROCESS KILL: the
        /// in-process set below covers this session (and is the only layer
        /// hot paths consult), while a best-effort DURABLE marker written
        /// into the same store covers the kill window — without it, a kill
        /// between the failed clear and its successful retry would relaunch
        /// into the stale "granted" record with no shadow at all, and the
        /// receipt-less grant would rule that launch with no receipt ever
        /// able to exist for it (the identity drop already rewrote the
        /// outbox). Whichever of [the clear lands] / [the marker lands]
        /// happens first closes that window; when BOTH keep failing the
        /// store is refusing every write and the next launch's reads are
        /// overwhelmingly failing too (fail-closed via the read-failure
        /// path). Residual accepted in the SAFE direction: a stale marker
        /// (its own clear failed after a successful explicit Save) at worst
        /// downgrades a persisted grant at the next launch and consent is
        /// asked again — fail-closed, never fail-open.
        /// </summary>
        private static readonly object DowngradeGate = new object();
        private static readonly HashSet<string> PendingDowngradeStores =
            new HashSet<string>(StringComparer.Ordinal);

        /// <summary>Durable marker key backing the shadow across a process kill.</summary>
        public const string DowngradePendingStorageKey = "shardpilot.consent.downgrade_pending";
        private const string DowngradePendingValue = "1";

        /// <summary>
        /// Key prefix (completed by the deciding scope's hash) of the
        /// durable WITHHELD-GRANT obligation marker: written when a grant's
        /// observable state persist is WITHHELD by the receipt-first
        /// ordering, so the obligation survives a process exit. Without it
        /// the obligation lived only in the deciding client's memory — a
        /// receipt made durable by an ordinary owed-write retry and then
        /// delivered by the NEXT launch would prune with no one left to
        /// complete the grant persist: local Unknown against a
        /// backend-recorded grant, permanently. The marker stores the
        /// receipt's idempotency key, its original decided_at, and whether
        /// the receipt had become DURABLE in the record (the phase that
        /// disambiguates an absent-because-DELIVERED receipt — safe to
        /// complete on load — from one LOST to the kill before it was ever
        /// durable, which must fail closed: the backend never saw it).
        /// Superseded (cleared) by every newer explicit decision and by the
        /// completion itself; a stale marker against a DECIDED persisted
        /// record is cleared at construction. Only ever touched under the
        /// process-wide decision gate.
        /// </summary>
        public const string WithheldGrantStorageKeyPrefix = "shardpilot.consent.grant-owed.";

        internal static string WithheldGrantStorageKey(string scopeHash)
        {
            return WithheldGrantStorageKeyPrefix + scopeHash;
        }

        /// <summary>Best-effort durable write; returns whether it landed.</summary>
        internal static bool RecordWithheldGrant(
            IShardPilotKeyValueStore store,
            string scopeHash,
            string idempotencyKey,
            string decidedAt,
            long decisionSeq,
            bool receiptDurable,
            ShardPilotConsentNoticeSnapshot notice = null)
        {
            if (store == null)
            {
                return false;
            }

            var builder = new System.Text.StringBuilder(160);
            builder.Append("{\"idempotency_key\":");
            ShardPilotJson.AppendQuoted(builder, idempotencyKey);
            builder.Append(",\"decided_at\":");
            ShardPilotJson.AppendQuoted(builder, decidedAt);
            if (decisionSeq > 0)
            {
                builder.Append(",\"decision_seq\":");
                builder.Append(decisionSeq.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }

            builder.Append(",\"receipt_durable\":");
            builder.Append(receiptDurable ? "true" : "false");
            new ShardPilotConsentOutboxEntry(notice).AppendNotice(builder);
            builder.Append('}');
            try
            {
                store.Set(WithheldGrantStorageKey(scopeHash), builder.ToString());
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Loads the scope's withheld-grant obligation marker; null when
        /// absent, garbled, or unreadable — the two failure shapes both
        /// resolve to "no completable obligation", which is the fail-CLOSED
        /// direction here (nothing is ever wrongly persisted as granted;
        /// at worst the player is asked again).
        /// </summary>
        internal static ShardPilotWithheldGrantMarker LoadWithheldGrant(
            IShardPilotKeyValueStore store, string scopeHash)
        {
            if (store == null)
            {
                return null;
            }

            string raw;
            try
            {
                raw = store.Get(WithheldGrantStorageKey(scopeHash));
            }
            catch
            {
                return null;
            }

            if (string.IsNullOrEmpty(raw))
            {
                return null;
            }

            System.Collections.Generic.Dictionary<string, object> record;
            if (!ShardPilotJsonParser.TryParseObject(raw, out record))
            {
                return null;
            }

            object keyValue;
            object decidedValue;
            object durableValue;
            var idempotencyKey = record.TryGetValue("idempotency_key", out keyValue) ? keyValue as string : null;
            var decidedAt = record.TryGetValue("decided_at", out decidedValue) ? decidedValue as string : null;
            var receiptDurable = record.TryGetValue("receipt_durable", out durableValue) &&
                                 durableValue is bool && (bool)durableValue;
            if (string.IsNullOrEmpty(idempotencyKey) || string.IsNullOrEmpty(decidedAt))
            {
                return null;
            }

            // Same long-typed bounds discipline as every other stored seq:
            // integral-only, positive, ceiling clamp; anything else reads
            // as the seq-less legacy 0 (stamp-only ordering, fail-closed).
            long decisionSeq = 0;
            object seqValue;
            if (record.TryGetValue("decision_seq", out seqValue) && seqValue is long)
            {
                var seq = (long)seqValue;
                if (seq > 0)
                {
                    decisionSeq = seq > MaxConsentDecisionSeq ? MaxConsentDecisionSeq : seq;
                }
            }

            ShardPilotConsentNoticeSnapshot notice;
            if (!ShardPilotConsentOutbox.TryReadNotice(record, out notice)) return null;
            return new ShardPilotWithheldGrantMarker(idempotencyKey, decidedAt, decisionSeq, receiptDurable, notice);
        }

        /// <summary>
        /// Clears the scope's withheld-grant obligation marker; returns
        /// whether the durable clear LANDED. Callers must not drop their
        /// in-memory bookkeeping of the obligation on a failed clear —
        /// the marker would survive its discharge, and a later clean
        /// rewrite of the outbox record would mint the false
        /// [undecided + clean-empty + durable] delivery proof at the next
        /// launch. The client keeps a failed discharge OWED (retried at
        /// every consent dispatch point, deferring teardown and standing
        /// the completion/empty-proof paths down meanwhile) — see
        /// <c>ShardPilotClient.DischargeWithheldMarkerDurable</c>.
        /// </summary>
        internal static bool ClearWithheldGrant(IShardPilotKeyValueStore store, string scopeHash)
        {
            if (store == null)
            {
                // No durable layer: no marker can exist, nothing is owed.
                return true;
            }

            try
            {
                store.Set(WithheldGrantStorageKey(scopeHash), string.Empty);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Returns whether the DURABLE marker layer landed (the in-process
        /// shadow is always recorded). Most callers are best-effort and
        /// ignore the result; the outbox's healing re-read is not — it must
        /// refuse to durably save a healed record minus a dropped pending
        /// grant until the downgrade is durable FIRST, or a kill between
        /// the two writes would relaunch into [granted consent key + healed
        /// record with nothing left to re-detect] and publish receipt-less.
        /// </summary>
        internal static bool RecordPendingDowngrade(IShardPilotKeyValueStore store)
        {
            if (store == null)
            {
                return false;
            }

            lock (DowngradeGate)
            {
                PendingDowngradeStores.Add(ShardPilotConsentOutbox.StoreIdentityToken(store));
            }

            // Durable layer (see the class note): idempotent, and
            // re-attempted by the dispatch-point retry while the clear
            // stays owed.
            try
            {
                store.Set(DowngradePendingStorageKey, DowngradePendingValue);
                return true;
            }
            catch
            {
                // The in-process shadow still guards this session; the
                // marker write is retried alongside the owed clear.
                return false;
            }
        }

        internal static void ClearPendingDowngrade(IShardPilotKeyValueStore store)
        {
            if (store == null)
            {
                return;
            }

            lock (DowngradeGate)
            {
                PendingDowngradeStores.Remove(ShardPilotConsentOutbox.StoreIdentityToken(store));
            }

            try
            {
                store.Set(DowngradePendingStorageKey, string.Empty);
            }
            catch
            {
                // A surviving marker fails CLOSED at the next launch (the
                // grant is re-downgraded and consent asked again) — never
                // open.
            }
        }

        internal static bool HasPendingDowngrade(IShardPilotKeyValueStore store)
        {
            return HasPendingDowngrade(store, consultDurableMarker: false);
        }

        /// <summary>
        /// SCOPE-KEYED pending-downgrade marker: armed when a propagated/catch-up grant is refused by the
        /// consent_outbox_full cap gate — the GLOBAL granted record stands
        /// (other scopes' receipts back it), but THIS scope holds no
        /// receipt, so its next construction must not open its pipeline
        /// from that record. Unlike the global marker, the consult never
        /// clears the shared consent key: it holds only the owning scope's
        /// state Unknown. Discharged when a later own-scope grant receipt
        /// arm succeeds.
        /// </summary>
        internal static string ScopedDowngradePendingStorageKey(string scopeHash)
        {
            return DowngradePendingStorageKey + "." + scopeHash;
        }

        /// <summary>Returns whether the durable marker write landed.</summary>
        internal static bool RecordScopedPendingDowngrade(IShardPilotKeyValueStore store, string scopeHash)
        {
            if (store == null || string.IsNullOrEmpty(scopeHash))
            {
                return false;
            }

            try
            {
                store.Set(ScopedDowngradePendingStorageKey(scopeHash), DowngradePendingValue);
                return true;
            }
            catch
            {
                return false;
            }
        }

        internal static void ClearScopedPendingDowngrade(IShardPilotKeyValueStore store, string scopeHash)
        {
            if (store == null || string.IsNullOrEmpty(scopeHash))
            {
                return;
            }

            try
            {
                store.Set(ScopedDowngradePendingStorageKey(scopeHash), string.Empty);
            }
            catch
            {
                // A surviving marker fails CLOSED at the scope's next launch
                // (its state holds Unknown and consent is asked again) —
                // never open. The clear retries at the next successful arm.
            }
        }

        /// <summary>Unreadable counts as SET — fail-closed, like the global consult.</summary>
        internal static bool HasScopedPendingDowngrade(IShardPilotKeyValueStore store, string scopeHash)
        {
            if (store == null || string.IsNullOrEmpty(scopeHash))
            {
                return false;
            }

            try
            {
                return ShardPilotConfig.Trim(store.Get(ScopedDowngradePendingStorageKey(scopeHash))) ==
                       DowngradePendingValue;
            }
            catch
            {
                return true;
            }
        }

        /// <summary>
        /// <paramref name="consultDurableMarker"/> extends the check to the
        /// durable marker layer — used by the COLD paths where
        /// cross-process visibility matters (construction, the registration
        /// refresh); hot dispatch-point retries consult only the in-process
        /// set (no store read per dispatch). An unreadable marker counts as
        /// SET: the store that cannot answer may be hiding an owed
        /// downgrade, and under consent-first a false "pending" only gates
        /// until an explicit decision — fail-closed, matching Load's own
        /// read-failure posture.
        /// </summary>
        internal static bool HasPendingDowngrade(IShardPilotKeyValueStore store, bool consultDurableMarker)
        {
            if (store == null)
            {
                return false;
            }

            lock (DowngradeGate)
            {
                if (PendingDowngradeStores.Count > 0 &&
                    PendingDowngradeStores.Contains(ShardPilotConsentOutbox.StoreIdentityToken(store)))
                {
                    return true;
                }
            }

            if (!consultDurableMarker)
            {
                return false;
            }

            try
            {
                return ShardPilotConfig.Trim(store.Get(DowngradePendingStorageKey)) == DowngradePendingValue;
            }
            catch
            {
                return true;
            }
        }

        public static void Save(IShardPilotKeyValueStore store, ShardPilotConsentState state)
        {
            if (store == null || state == ShardPilotConsentState.Unknown)
            {
                return;
            }

            string storedValue;
            switch (state)
            {
                case ShardPilotConsentState.Granted:
                    storedValue = GrantedValue;
                    break;
                case ShardPilotConsentState.DeniedForcedMinor:
                    storedValue = DeniedForcedMinorValue;
                    break;
                default:
                    storedValue = DeniedValue;
                    break;
            }

            try
            {
                if (state == ShardPilotConsentState.DeniedForcedMinor)
                {
                    store.Set(ForcedMinorStorageKey, ForcedMinorReason);
                }
                store.Set(StorageKey, storedValue);

                // A durable explicit decision supersedes any pending failed
                // denial on this store — and any owed identity-downgrade
                // clear on it: the fresh decision is the operative record now.
                ClearSessionDenial(store, null, true);
                ClearPendingDowngrade(store);
            }
            catch
            {
                if (IsDeniedState(state))
                {
                    // The denial applies in memory for the whole process: a
                    // later same-process construction or registration refresh
                    // must resolve to it (see Load) instead of reloading the
                    // stale persisted grant.
                    ArmSessionDenial(store, null, state, null, 0);
                }

                throw;
            }
        }

        // ------------------------------------------------------------------
        // Decision pair + denial-marker witness
        // ------------------------------------------------------------------

        /// <summary>
        /// Mints the next process-wide decision sequence — strictly
        /// monotonic within the process, floored at boot by
        /// <see cref="RaiseDecisionSeqFloor"/>, and SATURATING at
        /// <see cref="MaxConsentDecisionSeq"/> (a saturated mint keeps
        /// re-issuing the ceiling: pair comparisons then fall back to the
        /// stamp, which stays honest).
        /// </summary>
        internal static long NextDecisionSeq()
        {
            while (true)
            {
                var current = Interlocked.Read(ref _decisionSeqCounter);
                var next = current >= MaxConsentDecisionSeq ? MaxConsentDecisionSeq : current + 1;
                if (Interlocked.CompareExchange(ref _decisionSeqCounter, next, current) == current)
                {
                    return next;
                }
            }
        }

        /// <summary>
        /// Raises the mint floor to at least <paramref name="floor"/> —
        /// called at boot with the max of the restored record's seq, the
        /// marker's seq (a FOREIGN marker's included: its actor may return
        /// on this device, and this process's decisions must outrank it),
        /// and every retained receipt's seq. Negative floors are ignored;
        /// values above the ceiling clamp.
        /// </summary>
        internal static void RaiseDecisionSeqFloor(long floor)
        {
            if (floor <= 0)
            {
                return;
            }

            if (floor > MaxConsentDecisionSeq)
            {
                floor = MaxConsentDecisionSeq;
            }

            while (true)
            {
                var current = Interlocked.Read(ref _decisionSeqCounter);
                if (current >= floor)
                {
                    return;
                }

                if (Interlocked.CompareExchange(ref _decisionSeqCounter, floor, current) == current)
                {
                    return;
                }
            }
        }

        /// <summary>
        /// The stamp PLAUSIBILITY gate: a stored decided_at
        /// participates in pair comparisons only when it has EXACTLY the
        /// shape <see cref="ShardPilotJson.FormatTimestamp"/> writes —
        /// <c>yyyy-MM-ddTHH:mm:ss[.fffffff]Z</c> — because
        /// <see cref="CompareDecisionStamps"/> orders stamps by that
        /// literal fixed-width layout. A merely PARSEABLE foreign shape
        /// (an offset timestamp, a different separator) would string-order
        /// incorrectly, so it demotes the whole pair to LEGACY exactly
        /// like garbage does (fail-closed handling at the call sites — a
        /// legacy pair never outranks a stamped one). Trailing fraction
        /// zeros are tolerated (chronological no-ops under the padded
        /// compare) even though the writer trims them.
        /// </summary>
        internal static bool IsPlausibleDecisionStamp(string stamp)
        {
            // Fixed prefix: yyyy-MM-ddTHH:mm:ss (19 chars), then an
            // optional .f{1,7} fraction, then the mandatory 'Z'.
            if (stamp == null || stamp.Length < 20 || stamp.Length > 28 ||
                stamp[stamp.Length - 1] != 'Z')
            {
                return false;
            }

            for (var i = 0; i < 19; i++)
            {
                var c = stamp[i];
                switch (i)
                {
                    case 4:
                    case 7:
                        if (c != '-')
                        {
                            return false;
                        }

                        break;
                    case 10:
                        if (c != 'T')
                        {
                            return false;
                        }

                        break;
                    case 13:
                    case 16:
                        if (c != ':')
                        {
                            return false;
                        }

                        break;
                    default:
                        if (c < '0' || c > '9')
                        {
                            return false;
                        }

                        break;
                }
            }

            if (stamp.Length > 20)
            {
                if (stamp[19] != '.' || stamp.Length == 21)
                {
                    return false;
                }

                for (var i = 20; i < stamp.Length - 1; i++)
                {
                    if (stamp[i] < '0' || stamp[i] > '9')
                    {
                        return false;
                    }
                }
            }
            else if (stamp[19] != 'Z')
            {
                return false;
            }

            // The field values must still denote a real instant (month 13
            // or second 61 string-orders "fine" but is not a timestamp
            // this SDK wrote).
            return ShardPilotConsentOutbox.ParseReceiptTimestamp(stamp) != null;
        }

        /// <summary>
        /// CHRONOLOGICAL stamp compare for this SDK's trimmed-fraction
        /// timestamp shape (<c>yyyy-MM-ddTHH:mm:ss[.fffffff]Z</c> with
        /// trailing fraction zeros trimmed). Plain ordinal ordering would
        /// incorrectly sort <c>...:05.5Z</c> before <c>...:05.25Z</c>, and
        /// <c>...:05Z</c> vs <c>...:05.25Z</c> compares 'Z' against '.'.
        /// Second-prefix compares ordinally (fixed width), then the
        /// fractions compare right-padded to seven digits. Callers
        /// pre-validate plausibility.
        /// </summary>
        internal static int CompareDecisionStamps(string a, string b)
        {
            var aCore = a.EndsWith("Z", StringComparison.Ordinal) ? a.Substring(0, a.Length - 1) : a;
            var bCore = b.EndsWith("Z", StringComparison.Ordinal) ? b.Substring(0, b.Length - 1) : b;
            var aDot = aCore.IndexOf('.');
            var bDot = bCore.IndexOf('.');
            var aSeconds = aDot < 0 ? aCore : aCore.Substring(0, aDot);
            var bSeconds = bDot < 0 ? bCore : bCore.Substring(0, bDot);
            var secondsCompare = string.CompareOrdinal(aSeconds, bSeconds);
            if (secondsCompare != 0)
            {
                return secondsCompare;
            }

            var aFraction = aDot < 0 ? string.Empty : aCore.Substring(aDot + 1);
            var bFraction = bDot < 0 ? string.Empty : bCore.Substring(bDot + 1);
            return string.CompareOrdinal(aFraction.PadRight(7, '0'), bFraction.PadRight(7, '0'));
        }

        /// <summary>
        /// TRUE when pair A is strictly newer than pair B — chronological
        /// stamp first, sequence as the tiebreaker. Callers handle LEGACY
        /// pairs before comparing (a legacy pair has no stamp and never
        /// outranks a stamped one; two legacy pairs compare as equal).
        /// </summary>
        internal static bool DecisionPairNewer(string aStamp, long aSeq, string bStamp, long bSeq)
        {
            var stampCompare = CompareDecisionStamps(aStamp, bStamp);
            if (stampCompare != 0)
            {
                return stampCompare > 0;
            }

            return aSeq > bSeq;
        }

        /// <summary>
        /// Writes the consent record WITH its decision pair and the
        /// decision-time anonymous id — the modern record shape
        /// <c>{"state":...,"anonymous_id":...,"decided_at":...,"decision_seq":N}</c>.
        /// Same session-shadow semantics as the legacy overload (which
        /// remains for test seeding and pairless legacy paths).
        /// </summary>
        public static void Save(
            IShardPilotKeyValueStore store,
            ShardPilotConsentState state,
            string anonymousId,
            string decidedAt,
            long decisionSeq)
        {
            if (store == null || state == ShardPilotConsentState.Unknown)
            {
                return;
            }

            try
            {
                if (state == ShardPilotConsentState.DeniedForcedMinor)
                {
                    store.Set(ForcedMinorStorageKey, ForcedMinorReason);
                }
                store.Set(StorageKey, SerializeDecision(state, anonymousId, decidedAt, decisionSeq));
                ClearSessionDenial(store, anonymousId, false);
                ClearPendingDowngrade(store);
            }
            catch
            {
                if (IsDeniedState(state))
                {
                    ArmSessionDenial(store, anonymousId, state, decidedAt, decisionSeq);
                }

                throw;
            }
        }

        /// <summary>
        /// The one on-disk shape a denial witness has, in the marker slot and
        /// in the outbox mirror's backup of it alike.
        /// </summary>
        internal static string SerializeDenialDecision(
            ShardPilotConsentState state, string anonymousId, string decidedAt, long decisionSeq)
        {
            return SerializeDecision(state, anonymousId, decidedAt, decisionSeq);
        }

        private static string SerializeDecision(
            ShardPilotConsentState state, string anonymousId, string decidedAt, long decisionSeq)
        {
            string stateValue;
            switch (state)
            {
                case ShardPilotConsentState.Granted:
                    stateValue = GrantedValue;
                    break;
                case ShardPilotConsentState.DeniedForcedMinor:
                    stateValue = DeniedForcedMinorValue;
                    break;
                default:
                    stateValue = DeniedValue;
                    break;
            }

            var builder = new System.Text.StringBuilder(160);
            builder.Append("{\"state\":");
            ShardPilotJson.AppendQuoted(builder, stateValue);
            if (!string.IsNullOrEmpty(anonymousId))
            {
                builder.Append(",\"anonymous_id\":");
                ShardPilotJson.AppendQuoted(builder, anonymousId);
            }

            builder.Append(",\"decided_at\":");
            ShardPilotJson.AppendQuoted(builder, decidedAt ?? string.Empty);
            builder.Append(",\"decision_seq\":");
            builder.Append(decisionSeq.ToString(System.Globalization.CultureInfo.InvariantCulture));
            builder.Append('}');
            return builder.ToString();
        }

        /// <summary>
        /// Loads the FULL consent record — state plus the restored decision
        /// pair and decision-time anonymous id. Never null: an absent
        /// record is an Unknown LEGACY record; a bare legacy value
        /// (pre-pair launches, and test seeds) is its state with a LEGACY
        /// pair; a garbled non-empty value reports
        /// <paramref name="readFailed"/> exactly like <see cref="Load"/>.
        /// A modern record whose stamp fails the plausibility gate, or
        /// whose seq is absent/non-numeric/out of the strict
        /// <c>[0, 2^63)</c> double bounds, keeps its STATE but demotes the
        /// pair to LEGACY. Does NOT
        /// consult the in-process denial shadow — that is
        /// <see cref="Load"/>'s session-level concern; this is the raw
        /// durable truth the boot imposition/belt judge against.
        /// </summary>
        internal static ShardPilotConsentRecord LoadRecord(
            IShardPilotKeyValueStore store, out bool readFailed)
        {
            readFailed = false;
            if (store == null)
            {
                return ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.Unknown);
            }

            string raw;
            try
            {
                raw = ShardPilotConfig.Trim(store.Get(StorageKey));
            }
            catch
            {
                readFailed = true;
                return ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.Unknown);
            }

            if (string.IsNullOrEmpty(raw))
            {
                return ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.Unknown);
            }

            switch (raw)
            {
                case GrantedValue:
                    return ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.Granted);
                case DeniedValue:
                    return ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.Denied);
                case DeniedForcedMinorValue:
                    return ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.DeniedForcedMinor);
            }

            System.Collections.Generic.Dictionary<string, object> record;
            if (!ShardPilotJsonParser.TryParseObject(raw, out record))
            {
                readFailed = true;
                return ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.Unknown);
            }

            object stateValue;
            var stateRaw = record.TryGetValue("state", out stateValue) ? stateValue as string : null;
            ShardPilotConsentState state;
            switch (stateRaw)
            {
                case GrantedValue:
                    state = ShardPilotConsentState.Granted;
                    break;
                case DeniedValue:
                    state = ShardPilotConsentState.Denied;
                    break;
                case DeniedForcedMinorValue:
                    state = ShardPilotConsentState.DeniedForcedMinor;
                    break;
                default:
                    readFailed = true;
                    return ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.Unknown);
            }

            object anonValue;
            var anonymousId = record.TryGetValue("anonymous_id", out anonValue) ? anonValue as string : null;
            string decidedAt;
            long decisionSeq;
            var stamped = TryReadDecisionPair(record, out decidedAt, out decisionSeq);
            return stamped
                ? new ShardPilotConsentRecord(state, anonymousId, decidedAt, decisionSeq, legacy: false)
                : new ShardPilotConsentRecord(state, anonymousId, null, 0, legacy: true);
        }

        /// <summary>
        /// Reads a (decided_at, decision_seq) pair from a parsed record,
        /// applying the plausibility gate and the seq bounds:
        /// <see cref="ShardPilotJsonParser"/> yields a
        /// <c>long</c> for integral JSON numbers — the only shape this
        /// SDK ever writes — and a <c>double</c> for fractional/oversized
        /// ones, which are implausible for a sequence and demote the pair
        /// to legacy (no double round-trip means no 2^63 overflow hazard
        /// to special-case). Negative values demote; in-range values
        /// clamp to the mint ceiling.
        /// </summary>
        private static bool TryReadDecisionPair(
            System.Collections.Generic.Dictionary<string, object> record,
            out string decidedAt,
            out long decisionSeq)
        {
            decidedAt = null;
            decisionSeq = 0;
            object stampValue;
            var stamp = record.TryGetValue("decided_at", out stampValue) ? stampValue as string : null;
            if (!IsPlausibleDecisionStamp(stamp))
            {
                return false;
            }

            object seqValue;
            if (!record.TryGetValue("decision_seq", out seqValue) || !(seqValue is long))
            {
                return false;
            }

            var seq = (long)seqValue;
            if (seq < 0)
            {
                return false;
            }

            decidedAt = stamp;
            decisionSeq = seq > MaxConsentDecisionSeq ? MaxConsentDecisionSeq : seq;
            return true;
        }

        /// <summary>
        /// Writes the denial-marker WITNESS (write-ahead of a denial's
        /// state-first persist, the belt's convergence-failure witness, the
        /// settle-guard and shutdown-ladder fallbacks). Returns whether the
        /// durable write landed. Only ever carries a DENIED flavor.
        /// </summary>
        internal static bool RecordDenialMarker(
            IShardPilotKeyValueStore store,
            ShardPilotConsentState state,
            string anonymousId,
            string decidedAt,
            long decisionSeq)
        {
            if (store == null || !IsDeniedState(state))
            {
                return false;
            }

            try
            {
                store.Set(DenialMarkerStorageKey, SerializeDecision(state, anonymousId, decidedAt, decisionSeq));
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Loads the denial-marker slot. Null = absent (no witness).
        /// <paramref name="unreadable"/> = the store REFUSED the read — the
        /// slot may hold a denial witness, so the caller fails closed
        /// TRANSIENTLY (over a granted restore only; no rewrite, no
        /// retirement). Wholly unjudgeable content — garbled JSON, or a
        /// state that is not a denied flavor — returns a MANGLED marker
        /// that takes the same transient arm (never ignored: mangling must
        /// not LAUNDER a denial witness away; never imposed either: it
        /// cannot name a flavor or an actor). A marker whose STATE parses
        /// but whose pair fails the plausibility gate demotes to a LEGACY
        /// pair and still imposes fail-closed.
        /// </summary>
        /// <summary>
        /// Records the OWED backup — the marker write this actor could not
        /// land. Same shape and same codec as the slot, so the two can never
        /// drift; the caller decides WHEN one is owed.
        /// </summary>
        internal static bool RecordOwedDenialWitness(
            IShardPilotKeyValueStore store,
            ShardPilotConsentState state,
            string anonymousId,
            string decidedAt,
            long decisionSeq)
        {
            if (store == null || !IsDeniedState(state))
            {
                return false;
            }

            try
            {
                store.Set(
                    OwedDenialWitnessStorageKeyFor(anonymousId),
                    SerializeDecision(state, anonymousId, decidedAt, decisionSeq));
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Loads the owed backup. A MANGLED or LEGACY one is discarded rather
        /// than replayed: this is a copy WE wrote in one known shape, so
        /// unreadable content is corruption, and replaying a fabricated pair
        /// would outrank real witnesses. <paramref name="unreadable"/> reports a
        /// refused READ, which is not the same answer as nothing owed.
        /// </summary>
        internal static ShardPilotDenialMarker LoadOwedDenialWitness(
            IShardPilotKeyValueStore store, string anonymousId, out bool unreadable)
        {
            unreadable = false;
            if (store == null)
            {
                return null;
            }

            string raw;
            try
            {
                raw = ShardPilotConfig.Trim(store.Get(OwedDenialWitnessStorageKeyFor(anonymousId)));
            }
            catch
            {
                unreadable = true;
                return null;
            }

            if (string.IsNullOrEmpty(raw))
            {
                return null;
            }

            System.Collections.Generic.Dictionary<string, object> record;
            if (!ShardPilotJsonParser.TryParseObject(raw, out record))
            {
                // NON-EMPTY but unparseable is not absence. We wrote this key in one known shape, so garbled
                // content is corruption that may still be hiding a denial —
                // reported as UNREADABLE so the caller fails closed, never as
                // "nothing owed".
                unreadable = true;
                return null;
            }

            var witness = ParseDenialWitnessRecord(record);
            if (witness == null || witness.Mangled || witness.Legacy)
            {
                unreadable = true;
                return null;
            }

            return witness;
        }

        /// <summary>
        /// Discharges the backup. Best effort, like every retire here: a
        /// SURVIVING backup is the fail-CLOSED direction — at worst a later
        /// launch re-imposes a denial the stale guard then retires.
        /// </summary>
        internal static bool RetireOwedDenialWitness(
            IShardPilotKeyValueStore store, string anonymousId)
        {
            if (store == null)
            {
                return true;
            }

            try
            {
                store.Set(OwedDenialWitnessStorageKeyFor(anonymousId), string.Empty);
                return true;
            }
            catch
            {
                // Reported, not thrown: a SURVIVING backup is the fail-CLOSED
                // direction, but the caller must not then remember it as gone.
                return false;
            }
        }

        internal static ShardPilotDenialMarker LoadDenialMarker(
            IShardPilotKeyValueStore store, out bool unreadable)
        {
            unreadable = false;
            if (store == null)
            {
                return null;
            }

            string raw;
            try
            {
                raw = ShardPilotConfig.Trim(store.Get(DenialMarkerStorageKey));
            }
            catch
            {
                unreadable = true;
                return null;
            }

            if (string.IsNullOrEmpty(raw))
            {
                return null;
            }

            System.Collections.Generic.Dictionary<string, object> record;
            if (!ShardPilotJsonParser.TryParseObject(raw, out record))
            {
                return ShardPilotDenialMarker.CreateMangled();
            }

            return ParseDenialWitnessRecord(record);
        }

        /// <summary>
        /// The marker slot's decoder, over an ALREADY-PARSED object. Shared so
        /// the slot and the outbox mirror's owed-witness backup
        /// cannot drift on the shape: the mirror stores exactly what
        /// <see cref="SerializeDenialDecision"/> writes into the slot, and
        /// reads it back through this. A null record decodes as MANGLED, the
        /// same fail-closed answer garbled slot content gets.
        /// </summary>
        internal static ShardPilotDenialMarker ParseDenialWitnessRecord(
            System.Collections.Generic.Dictionary<string, object> record)
        {
            if (record == null)
            {
                return ShardPilotDenialMarker.CreateMangled();
            }

            object stateValue;
            var stateRaw = record.TryGetValue("state", out stateValue) ? stateValue as string : null;
            ShardPilotConsentState state;
            switch (stateRaw)
            {
                case DeniedValue:
                    state = ShardPilotConsentState.Denied;
                    break;
                case DeniedForcedMinorValue:
                    state = ShardPilotConsentState.DeniedForcedMinor;
                    break;
                default:
                    return ShardPilotDenialMarker.CreateMangled();
            }

            object anonValue;
            var anonymousId = record.TryGetValue("anonymous_id", out anonValue) ? anonValue as string : null;
            string decidedAt;
            long decisionSeq;
            var stamped = TryReadDecisionPair(record, out decidedAt, out decisionSeq);
            return stamped
                ? new ShardPilotDenialMarker(state, anonymousId, decidedAt, decisionSeq, legacy: false)
                : new ShardPilotDenialMarker(state, anonymousId, null, 0, legacy: true);
        }

        /// <summary>
        /// Retires (clears) the denial-marker slot; returns whether the
        /// durable clear landed. Best-effort by design — a SURVIVING
        /// marker is the fail-CLOSED direction (at worst the next launch
        /// re-imposes a denial and the stale-guard retires it against the
        /// converged record).
        /// </summary>
        internal static bool RetireDenialMarker(IShardPilotKeyValueStore store)
        {
            if (store == null)
            {
                return true;
            }

            try
            {
                store.Set(DenialMarkerStorageKey, string.Empty);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// SCOPED retirement: retires an own-actor,
        /// actor-less, mangled, or unreadable occupant; KEEPS a valid
        /// FOREIGN actor's witness — its actor may return on this device,
        /// and a decision by a different actor supersedes only the current
        /// actor's. Used everywhere the slot's occupant is NOT provably
        /// this caller's own just-written marker (successfully persisted
        /// decisions, the belt's convergence, the settle guard and the
        /// shutdown ladder); the boot imposition paths, which just judged
        /// the occupant own, retire unconditionally.
        /// </summary>
        internal static void RetireDenialMarkerScoped(
            IShardPilotKeyValueStore store, string currentAnonymousId)
        {
            if (store == null)
            {
                return;
            }

            bool unreadable;
            var occupant = LoadDenialMarker(store, out unreadable);
            if (occupant == null && !unreadable)
            {
                return; // confirmed absent — nothing to retire
            }

            if (IsForeignDenialMarkerOccupant(occupant, currentAnonymousId))
            {
                return; // a different actor's witness: deliberately kept
            }

            RetireDenialMarker(store);
        }

        /// <summary>
        /// Whether the marker slot's <paramref name="occupant"/> is ANOTHER
        /// actor's witness — the one thing neither a retire nor a write may
        /// take, because the slot holds one and it is that actor's only
        /// memory of its own denial once the shared record is superseded.
        ///
        /// The exemption requires a VALID identifier:
        /// a corrupted actor string is unjudgeable evidence, not another
        /// actor's witness — treating it as foreign would launder an
        /// own-actor witness into something no decision could ever retire.
        ///
        /// Shared so the retire and the write cannot drift apart on what
        /// counts as foreign.
        /// </summary>
        internal static bool IsForeignDenialMarkerOccupant(
            ShardPilotDenialMarker occupant, string currentAnonymousId)
        {
            return occupant != null && !occupant.Mangled && occupant.AnonymousId != null &&
                   ShardPilotConsentOutbox.IsValidIdentifier(occupant.AnonymousId) &&
                   !string.Equals(occupant.AnonymousId, currentAnonymousId, StringComparison.Ordinal);
        }

        // The durable per-scope ShardPilotConsentOutbox is an append-only
        // decision trail. Its head-dispatch keeps claim-and-handoff atomicity
        // (the client's consent dispatch gate), and the wire body is built
        // from the stored entry so retries and post-relaunch re-sends carry
        // idempotency_key and decided_at verbatim. Later decisions never
        // withdraw already-appended receipts (grant→deny delivers BOTH, in
        // order); what a denial or consent-read failure still clears is the
        // session grant-distribution MARKER above, which only governs arming
        // NEW per-scope receipts.
    }
}
