using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ShardPilot
{
    /// <summary>
    /// One stored consent-receipt record: the durable form of an explicit
    /// consent decision, held until the ingest service acknowledges its
    /// <c>POST /v1/consent</c>. Stored entries preserve the decision:
    /// <c>decided_at</c> is stamped ONCE at decision time and re-sent
    /// VERBATIM on every retry and post-relaunch re-send — same idempotency
    /// key, same decision moment; <c>anonymous_id</c> is retention-only
    /// metadata (the Mode-B identity-changed check at load) and NEVER rides
    /// the wire.
    /// </summary>
    internal sealed class ShardPilotConsentOutboxEntry
    {
        /// <summary>Original optional tuple; never replaced by a later decision or load-time policy.</summary>
        public readonly ShardPilotConsentNoticeSnapshot Notice;

        internal ShardPilotConsentOutboxEntry() { }

        internal ShardPilotConsentOutboxEntry(ShardPilotConsentNoticeSnapshot notice)
        {
            Notice = notice;
        }

        public string IdempotencyKey;
        public string WorkspaceId;
        public string AppId;
        public string EnvironmentId;
        public string ActorIdentifier;

        /// <summary>
        /// The actor's identity class, chosen by the canonical-actor
        /// rule at decision time: exactly
        /// <c>"anon"</c> or <c>"user_verified"</c> — this SDK NEVER produces
        /// <c>"user_unverified"</c> (a self-asserted identity is never
        /// presented as verified). Persisted with the receipt, re-sent
        /// verbatim, and the input to per-receipt dispatch-credential
        /// selection and parking. A PRE-AMENDMENT legacy entry (no kind)
        /// backfills <c>"anon"</c> at the sanitizer — see its rule.
        /// </summary>
        public string Kind;

        /// <summary>The <c>categories.analytics</c> boolean: true iff the decision is a grant.</summary>
        public bool AnalyticsGranted;

        /// <summary>ISO-8601 UTC decision moment, stamped once, never re-stamped.</summary>
        public string DecidedAt;

        /// <summary>Optional; the only value ever produced is <c>"denied_forced_minor"</c>.</summary>
        public string Reason;

        /// <summary>Optional decision-time anonymous-id snapshot; never on the wire.</summary>
        public string AnonymousId;

        /// <summary>
        /// Retention-only, never on the wire (like <see cref="AnonymousId"/>):
        /// true once a LATER explicit decision applied on this scope, i.e.
        /// this pending grant is no longer the receipt backing whatever
        /// consent state is persisted. The parked-pending-grant construction
        /// downgrade consults it: a persisted grant that
        /// a NEWER vouched decision wrote must not be wiped on every launch
        /// just because an older unvouchable grant receipt is still parked
        /// beside it. Stamped durably in the same decision that supersedes;
        /// delivery semantics are unaffected — a superseded receipt still
        /// dispatches (or stays parked) exactly as before.
        /// </summary>
        public bool Superseded;

        // A validated receipt recovered beside damaged content stays retained
        // even if a later valid rewrite would normally apply identity cleanup.
        // Its existing credential/parking rules still govern dispatch.
        public bool RetainedFromCorruption;

        /// <summary>
        /// Retention-only, never on the wire: the decision sequence minted
        /// with this receipt's decision (the (decided_at, decision_seq)
        /// pair). Feeds the boot seq floor and the
        /// belt's newer-than-restored-record judgment; 0 = a legacy
        /// (pre-pair) receipt, whose pair never outranks a stamped one.
        /// </summary>
        public long DecisionSeq;

        /// <summary>
        /// In-memory only (never serialized): the per-process
        /// ConsentRecorded stat already counted this receipt's 2xx. A
        /// receipt RETAINED past its acknowledgment as a denial's only
        /// durable witness (the settle guard) re-acknowledges on every
        /// paced re-dispatch — the server de-duplicates those on
        /// idempotency_key, and this flag de-duplicates the stat.
        /// </summary>
        public bool RecordedCounted;

        /// <summary>
        /// In-memory only (never serialized): this receipt's SERVER outcome is
        /// settled — it was acknowledged, or terminally refused — and it is
        /// retained solely as the standing denial's durable witness.
        ///
        /// That combination is what makes a local prune safe: nothing further
        /// is owed to the server, so once the witness gap closes the receipt
        /// can simply leave the trail. It replaces a client-global deferral
        /// that paced re-DISPATCH of a receipt the server had already settled —
        /// a deadline that borrowed its length from FlushInterval, outlived the
        /// reason it was taken for, and could not say which decision it
        /// belonged to. State about one receipt
        /// belongs on that receipt.
        ///
        /// Not serialized because it is only ever true between a settle and the
        /// witness heal, both within one process: a relaunch re-derives the
        /// retention from the restored record and marker.
        /// </summary>
        public bool WitnessHeldAfterSettle;

        /// <summary>
        /// Builds the wire body: the stored entry MINUS the
        /// <c>anonymous_id</c> retention metadata. The stored
        /// <c>kind</c> joins the body when <paramref name="emitKind"/> —
        /// <see cref="ShardPilotConfig.ConsentKindEmissionEnabled"/>, default
        /// ON; the empty-kind guard is defense in depth (every sanitized
        /// entry carries one, and an empty <c>"kind":""</c> would 400 a
        /// strict pre-amendment decoder exactly like an unknown field).
        /// </summary>
        public string BuildWireBody(bool emitKind)
        {
            var builder = new StringBuilder(256);
            builder.Append("{\"workspace_key\":");
            ShardPilotJson.AppendQuoted(builder, WorkspaceId);
            builder.Append(",\"app_key\":");
            ShardPilotJson.AppendQuoted(builder, AppId);
            builder.Append(",\"environment_key\":");
            ShardPilotJson.AppendQuoted(builder, EnvironmentId);
            builder.Append(",\"actor_identifier\":");
            ShardPilotJson.AppendQuoted(builder, ActorIdentifier);
            if (emitKind && !string.IsNullOrEmpty(Kind))
            {
                builder.Append(",\"kind\":");
                ShardPilotJson.AppendQuoted(builder, Kind);
            }

            builder.Append(",\"categories\":{\"analytics\":");
            builder.Append(AnalyticsGranted ? "true" : "false");
            builder.Append("},\"decided_at\":");
            ShardPilotJson.AppendQuoted(builder, DecidedAt);
            builder.Append(",\"idempotency_key\":");
            ShardPilotJson.AppendQuoted(builder, IdempotencyKey);
            if (!string.IsNullOrEmpty(Reason))
            {
                builder.Append(",\"reason\":");
                ShardPilotJson.AppendQuoted(builder, Reason);
            }

            AppendNotice(builder);
            builder.Append('}');
            return builder.ToString();
        }

        internal void AppendNotice(StringBuilder builder)
        {
            if (Notice == null) return;
            builder.Append(",\"notice_version\":");
            ShardPilotJson.AppendQuoted(builder, Notice.NoticeVersion);
            builder.Append(",\"notice_locale\":");
            ShardPilotJson.AppendQuoted(builder, Notice.NoticeLocale);
            builder.Append(",\"policy_version\":");
            ShardPilotJson.AppendQuoted(builder, Notice.PolicyVersion);
        }
    }

    /// <summary>
    /// The CURRENT Mode-B identity a retained receipt is compared against
    /// by the identity-changed drop: the resolved UserId-then-Anon actor
    /// identifier AND the anonymous id. Immutable, so the outbox's
    /// volatile filter reference can never expose a torn half-identity to
    /// a concurrent recovery re-read.
    /// </summary>
    internal sealed class ShardPilotModeBIdentity
    {
        internal readonly string ActorIdentifier;
        internal readonly string AnonymousId;

        internal ShardPilotModeBIdentity(string actorIdentifier, string anonymousId)
        {
            ActorIdentifier = actorIdentifier;
            AnonymousId = anonymousId;
        }
    }

    /// <summary>
    /// Internal control-flow signal for the grant-receipt dispatch gate: thrown by the
    /// publish path when the event-batch leg must refuse with
    /// <see cref="ShardPilotConsentOutbox.ReceiptPendingError"/> because an
    /// analytics-GRANT receipt is retained undispatched. Caught by the
    /// publish wrappers and turned into a retryable flush refusal WITHOUT
    /// failed-batch accounting — nothing was attempted, so nothing failed.
    /// </summary>
    internal sealed class ShardPilotConsentReceiptPendingException : Exception
    {
        public ShardPilotConsentReceiptPendingException()
            : base(ShardPilotConsentOutbox.ReceiptPendingError)
        {
        }
    }

    /// <summary>
    /// Durable consent-receipt outbox: one record
    /// per client scope, retaining every explicit decision's receipt —
    /// oldest first, an append-only decision trail — until each is
    /// acknowledged (or terminally rejected) by the consent endpoint.
    ///
    /// Contract highlights:
    /// - 32-entry cap, FIFO eviction OLDEST first, applied on save — a
    ///   FAILED durable write NEVER evicts and never partially succeeds:
    ///   the in-memory mirror keeps ruling the process and the write is
    ///   marked owed (dirty) and retried at every consent dispatch point.
    /// - The same sanitizer runs on load AND save: an entry survives only
    ///   with all required non-empty string fields, a boolean analytics
    ///   category, and identifiers within the 512-BYTE clamp;
    ///   invalid individual entries never send. Whole or partial corruption
    ///   holds the original durable bytes until a fresh explicit decision;
    ///   intact receipts remain deliverable under their credential rules.
    /// - Deliberately NO TTL: an undelivered receipt retries until acked.
    /// - Consent-plane only: never event envelopes, never crash payloads,
    ///   never consent-purged — receipts deliver while analytics consent is
    ///   denied or unknown, because a receipt documents the decision itself.
    /// - Per-scope isolation: the record key embeds the same
    ///   workspace/app/environment scope hash the event spool uses, so two
    ///   apps or scopes never share an outbox.
    ///
    /// Thread-safety: every member is safe under a private LEAF lock — the
    /// client calls in while holding its own gate (mirror mutations) or
    /// outside it (durable writes); the outbox never calls back into the
    /// client and does no IO beyond the injected key-value store.
    /// </summary>
    internal sealed class ShardPilotConsentOutbox
    {
        /// <summary>
        /// Process-wide record ownership, keyed by storage key: the outbox
        /// most recently CONSTRUCTED for a scope owns that scope's durable
        /// record, mirroring the event spool's replacement/disown handoff. A
        /// same-scope client replacement (ShardPilotSdk re-initialization,
        /// or any successor construction) would otherwise race the old
        /// client's LATE consent-ack settle: the old instance's stale
        /// mirror, pruned of the acked entry but missing everything the new
        /// client appended since, would rewrite the shared key over the
        /// successor's durable receipts. A displaced (disowned) instance
        /// therefore settles bookkeeping in its own mirror but never writes
        /// the record again, never counts as pending for ITS client's
        /// shutdown (its retained entries are the successor's to deliver —
        /// the successor loaded the same record), never gates its client's
        /// remaining drains, and stops dispatching (the successor delivers;
        /// a double-send would only be de-duplicated by idempotency key
        /// anyway, but the wasted traffic is avoidable). Guarded by
        /// <see cref="OwnershipGate"/>.
        /// </summary>
        private static readonly object OwnershipGate = new object();
        private static readonly Dictionary<string, ShardPilotConsentOutbox> Owners =
            new Dictionary<string, ShardPilotConsentOutbox>(StringComparer.Ordinal);

        /// <summary>
        /// Ownership is keyed by BACKEND identity + scope, never scope
        /// alone: two clients with the same scope but different injected
        /// stores (isolated backends — multi-profile setups, test
        /// isolation) must not displace or merge into each other. Stores
        /// that address a shared backend advertise a stable identity token
        /// (<see cref="IShardPilotStoreIdentity"/> — the PlayerPrefs-backed
        /// store shares one token across its instances, the file store
        /// tokens on its canonical directory, mirroring how the spool
        /// distinguishes backends by canonical file path); a store without
        /// one is its own private backend and gets a per-instance token.
        /// </summary>
        private static readonly ConditionalWeakTable<IShardPilotKeyValueStore, string> InstanceTokens =
            new ConditionalWeakTable<IShardPilotKeyValueStore, string>();

        internal static string StoreIdentityToken(IShardPilotKeyValueStore store)
        {
            var identity = store as IShardPilotStoreIdentity;
            if (identity != null)
            {
                string token = null;
                try
                {
                    token = identity.StoreIdentityToken;
                }
                catch
                {
                    // Fall through to the per-instance token.
                }

                if (!string.IsNullOrEmpty(token))
                {
                    return token;
                }
            }

            return InstanceTokens.GetValue(
                store, _ => "instance|" + Guid.NewGuid().ToString("N"));
        }

        /// <summary>Fixed retention cap: the newest decisions are the operative ones.</summary>
        internal const int MaxEntries = 32;

        /// <summary>
        /// The two receipt actor kinds this SDK produces:
        /// <c>anon</c> for the anonymous actor, and
        /// <c>user_verified</c> for a Mode-B-backed configured
        /// <see cref="ShardPilotConfig.UserId"/>. <c>user_unverified</c>
        /// exists on the wire contract but is NEVER emitted — a self-asserted
        /// identity is never presented as verified, and presenting it as
        /// unverified would be rejected under the server's enforce mode
        /// anyway.
        /// </summary>
        internal const string KindAnon = "anon";
        internal const string KindUserVerified = "user_verified";

        /// <summary>
        /// Identifier clamp: host-supplied identifiers (user id,
        /// anonymous id — and therefore every receipt's actor/anon fields,
        /// which copy them) are limited to 512 BYTES of UTF-8, not chars.
        /// REJECT, never truncate: truncation could collide distinct
        /// identities and mis-attribute events or consent decisions. The
        /// numeric bound is cross-SDK contract. Defined here, next to the
        /// outbox's other persistence budgets, and bound by the client's
        /// acceptance gates.
        /// </summary>
        internal const int MaxIdentifierBytes = 512;

        internal const string StorageKeyPrefix = "shardpilot.consent-outbox.";

        /// <summary>
        /// The distinct refusal the event-batch leg returns while an
        /// analytics-GRANT receipt is retained undispatched: events
        /// dispatched meanwhile would overtake the grant on the wire and be
        /// terminally suppressed per-event on a strict-enforce workspace.
        /// </summary>
        internal const string ReceiptPendingError = "consent_receipt_pending";

        private readonly IShardPilotKeyValueStore _store;
        private readonly string _storageKey;
        private readonly string _workspaceId;
        private readonly string _appId;
        private readonly string _environmentId;

        // Scope-local state and dispatch readers use this view. Persistence,
        // caps and global actor witnesses deliberately retain the whole trail.
        internal readonly ScopeReceiptView OwnScope;

        internal sealed class ScopeReceiptView
        {
            private readonly ShardPilotConsentOutbox _owner;

            internal ScopeReceiptView(ShardPilotConsentOutbox owner) { _owner = owner; }

            internal bool Matches(ShardPilotConsentOutboxEntry entry)
            {
                return MatchesTuple(entry, _owner._workspaceId, _owner._appId, _owner._environmentId);
            }

            internal List<ShardPilotConsentOutboxEntry> SnapshotEntries()
            {
                lock (_owner._lock)
                {
                    var entries = new List<ShardPilotConsentOutboxEntry>();
                    foreach (var entry in _owner._entries)
                        if (Matches(entry)) entries.Add(entry);
                    return entries;
                }
            }

            internal int Count { get { return SnapshotEntries().Count; } }
            internal string TailDeniedDecidedAt { get { return _owner.TailDeniedDecidedAt; } }
            internal bool Contains(string key) { return FindByKey(key) != null; }

            internal ShardPilotConsentOutboxEntry FindByKey(string key)
            {
                lock (_owner._lock)
                {
                    foreach (var entry in SnapshotEntries())
                        if (string.Equals(entry.IdempotencyKey, key, StringComparison.Ordinal)) return entry;
                    return null;
                }
            }

            internal void RemoveFromMirror(string key)
            {
                lock (_owner._lock) { _owner.RemoveFromMirror(FindByKey(key)); }
            }

            internal ShardPilotConsentOutboxEntry FirstDispatchableEntry(Func<ShardPilotConsentOutboxEntry, bool> parked)
            {
                return _owner.FirstDispatchableEntry(parked);
            }

            internal bool HasUndispatchedAnalyticsGrant(string inFlightKey)
            {
                return _owner.HasUndispatchedAnalyticsGrant(inFlightKey);
            }

            internal void MarkRetainedAnalyticsGrantsSuperseded(string exceptKey)
            {
                _owner.MarkRetainedAnalyticsGrantsSuperseded(exceptKey);
            }

            internal void MarkParkedAnalyticsGrantsSuperseded()
            {
                _owner.MarkParkedAnalyticsGrantsSuperseded();
            }
        }

        /// <summary>Backend-identity + scope key the ownership registry uses.</summary>
        private readonly string _ownershipKey;
        private readonly object _lock = new object();
        private List<ShardPilotConsentOutboxEntry> _entries = new List<ShardPilotConsentOutboxEntry>();
        private bool _dirty;

        /// <summary>
        /// True while the construction-time record READ failed (the store
        /// threw — NOT a successful read of an absent or garbled record):
        /// the record may hold valid undelivered receipts, so the outbox
        /// fails CLOSED — pending/dirty (a close is held on it),
        /// the grant-receipt gate holds the batch leg, and durable writes are refused —
        /// until a dispatch-point re-read heals it. See Load/
        /// TryRecoverFromLoadFailureLocked.
        /// </summary>
        private bool _loadFailed;
        private bool _recoveryDroppedPendingGrant;
        private bool _recoveryMergedParkedPendingGrant;

        /// <summary>
        /// True when the construction-time (or recovery) read SUCCEEDED but
        /// non-empty raw content was discarded by the receipt sanitize-on-load
        /// (garbled JSON, missing receipts array, or dropped entries) —
        /// held until a fresh explicit decision authorizes replacement.
        /// Maintenance cannot erase the original evidence, nor can a clean
        /// sanitized mirror prove that the missing entries were delivered.
        /// </summary>
        private bool _loadRecordMalformed;
        private volatile bool _explicitDecisionAuthorizesReplacement;

        /// <summary>
        /// The Mode-B current identity (resolved UserId-then-Anon actor +
        /// anonymous id) the construction-time identity drop ran with,
        /// re-applied to entries recovered by a late re-read after a load
        /// failure (the drop could not see them). Null in Mode A —
        /// historic-identity receipts re-send unchanged there.
        /// </summary>
        internal volatile ShardPilotModeBIdentity ModeBIdentityFilter;

        /// <summary>
        /// The idempotency key of this outbox's receipt currently handed to
        /// the HTTP sender, written through by the owning client alongside
        /// its own in-flight bookkeeping. Exists so the grant-receipt gate can be
        /// answered ACROSS clients: a displaced client's drain delegates to
        /// the current owner's gate, and the owner's in-flight head must
        /// release the gate only for itself there too.
        /// </summary>
        internal volatile string InFlightKeyForGate;

        /// <summary>
        /// The owning client's PARKED predicate: TRUE for a receipt the current
        /// session cannot vouch a dispatch credential for — a
        /// verified-keyed receipt with no Mode-B provider or a different
        /// configured user, or an anon-keyed receipt in a Mode-B-only
        /// session that vouches a configured user. Parked receipts are
        /// RETAINED records: persisted, counted toward the cap and
        /// pending-ness, skipped by dispatch selection AND by the grant-receipt gate
        /// (a parked grant must never wedge a signed-out session's batches
        /// or teardown), delivered verbatim when a later launch vouches
        /// their actor again. Written through by the owning client at
        /// construction (its vouching inputs — config user id, provider
        /// presence — are immutable per client, so the capture is stable);
        /// the cross-client gate delegation judges with the OWNER's
        /// predicate, exactly like the in-flight key. Null = nothing parks
        /// (Mode A).
        /// </summary>
        internal volatile Func<ShardPilotConsentOutboxEntry, bool> ParkedPredicateForGate;

        /// <summary>
        /// The FIRST retained entry the given predicate does NOT park — the
        /// serial dispatch head with vouched parking in effect (parked
        /// receipts are skipped, preserving per-actor FIFO: parked-ness is
        /// uniform per kind class within a session). Null when every
        /// retained entry is parked or none is retained.
        /// </summary>
        internal ShardPilotConsentOutboxEntry FirstDispatchableEntry(
            Func<ShardPilotConsentOutboxEntry, bool> parked)
        {
            lock (_lock)
            {
                for (var i = 0; i < _entries.Count; i++)
                {
                    if (!OwnScope.Matches(_entries[i])) continue;
                    // A receipt whose SERVER outcome is settled has nothing left
                    // to send: it is retained only as the standing denial's
                    // durable witness, and re-dispatching it re-asks a question
                    // the server has already answered. Skipped here rather than
                    // paced by a deadline — the deadline was the old shape, and
                    // it cost a client-global (DateTime, bool, seq, decided_at)
                    // that went stale four different ways.
                    //
                    // Dispatch selection only: every caller of this method is on
                    // the dispatch path. The grant-receipt gate reads the trail through
                    // ParkedPredicateForGate and is deliberately untouched — a
                    // held receipt still gates, exactly as before.
                    if (_entries[i].WitnessHeldAfterSettle)
                    {
                        continue;
                    }

                    if (parked == null || !parked(_entries[i]))
                    {
                        return _entries[i];
                    }
                }

                return null;
            }
        }

        internal ShardPilotConsentOutbox(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            _store = store;
            _workspaceId = config.WorkspaceKey;
            _appId = config.AppKey;
            _environmentId = config.EnvironmentKey;
            OwnScope = new ScopeReceiptView(this);
            _storageKey = StorageKeyPrefix + ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey);
            _ownershipKey = store != null ? StoreIdentityToken(store) + "|" + _storageKey : _storageKey;

            // Claim the scope's record FIRST, then load: the claim waits out
            // any predecessor write in flight (the write holds this same
            // registry gate — see TryPersist), and once claimed no displaced
            // instance can write again, so the load below reads the SETTLED
            // record. Loading before claiming would let an in-flight
            // predecessor write land after our read — its entries durable
            // but invisible to this session's mirror.
            ShardPilotConsentOutbox displaced = null;
            if (_store != null)
            {
                lock (OwnershipGate)
                {
                    Owners.TryGetValue(_ownershipKey, out displaced);
                    Owners[_ownershipKey] = this;
                }
            }

            Load();

            if (displaced != null && !ReferenceEquals(displaced, this) &&
                displaced.ConsumeRecoveryDroppedPendingGrant())
            {
                // RECOVERY-DOWNGRADE HANDOFF: the displaced instance's healing
                // re-read dropped a Mode-B identity-mismatched PENDING GRANT,
                // and its client had not yet consumed the flag at a dispatch
                // point. This successor loaded the HEALED record — the
                // mismatched entry is already gone, so its own construction-
                // time identity drop finds nothing and would never learn the
                // persisted grant lost its backing receipt. The pending
                // downgrade therefore transfers with the record: the
                // successor's client consumes it at a dispatch point and
                // judges it against ITS OWN state and decision generation
                // (the displaced instance stands down — a displaced client
                // must not clear the shared persisted record it no longer
                // governs).
                lock (_lock)
                {
                    _recoveryDroppedPendingGrant = true;
                }
            }

            if (displaced != null && !ReferenceEquals(displaced, this) && displaced.Dirty)
            {
                _explicitDecisionAuthorizesReplacement = displaced._explicitDecisionAuthorizesReplacement;
                // FAILED-WRITE-NEVER-EVICTS survives REPLACEMENT too: when
                // the predecessor's owed write was still failing at handoff
                // time (the pre-replacement persist retry could not land),
                // the record loaded above is missing its dirty entries — and
                // the ownership swap just cut the only instance that still
                // held them off from ever writing again. Mirror the spool's
                // in-memory handoff (SetSpoolSuccessor/AcceptHandoffEntries):
                // adopt the displaced instance's retained entries that the
                // record does not already carry, in decision order (the
                // record's settled/older entries first, then the displaced
                // mirror's unpersisted tail), de-duplicated by idempotency
                // key, and mark the write owed so this owner's dispatch
                // points persist them. The merge fires only while the
                // predecessor is DIRTY — a clean predecessor's mirror is
                // already contained in the record this instance loaded, so
                // there is no delta to carry (and no reason to resurrect
                // entries a clean settle already pruned). A racing settle on
                // the displaced instance at worst re-adds an entry it was
                // about to prune — a duplicate re-send the server
                // de-duplicates by idempotency key.
                var pending = displaced.SnapshotEntries();
                lock (_lock)
                {
                    var merged = 0;
                    for (var i = 0; i < pending.Count; i++)
                    {
                        var entry = Sanitize(pending[i]);
                        if (entry == null)
                        {
                            continue;
                        }

                        var alreadyPresent = false;
                        for (var j = 0; j < _entries.Count; j++)
                        {
                            if (SameReceiptIdentity(_entries[j], entry))
                            {
                                alreadyPresent = true;
                                break;
                            }
                        }

                        if (!alreadyPresent)
                        {
                            _entries.Add(entry);
                            merged++;
                        }
                    }

                    if (merged > 0)
                    {
                        _dirty = true;
                    }
                }
            }

        }

        /// <summary>
        /// True while this instance is the scope's record writer: it has no
        /// durable backend (nothing to contend for), or no later same-scope
        /// instance has been constructed.
        /// </summary>
        internal bool IsCurrentOwner
        {
            get
            {
                if (_store == null)
                {
                    return true;
                }

                lock (OwnershipGate)
                {
                    ShardPilotConsentOutbox owner;
                    return Owners.TryGetValue(_ownershipKey, out owner) && ReferenceEquals(owner, this);
                }
            }
        }

        /// <summary>Displaced by a later same-scope construction — see <see cref="OwnershipGate"/>.</summary>
        internal bool IsDisowned
        {
            get { return !IsCurrentOwner; }
        }

        /// <summary>
        /// True when a durable backend exists at all. Without one the mirror
        /// still rules the process (retained receipts still deliver), but a
        /// close is held while receipts are retained and answers
        /// <c>spool_persist_failed</c> if they still are at its deadline —
        /// successfully persisted receipts survive process restarts.
        /// </summary>
        internal bool HasDurableBackend
        {
            get { return _store != null; }
        }

        /// <summary>
        /// Mirror non-empty OR a durable write owed. The dirty-with-empty-
        /// mirror case (a failed post-ack prune) counts as pending: it keeps
        /// shutdown durability unsatisfied until the rewrite lands.
        /// </summary>
        internal bool Pending
        {
            get
            {
                // A DISOWNED instance is never pending: its retained entries
                // live in the record the successor loaded and now owns, so
                // they are the successor's to deliver and to keep durable —
                // the displaced client's shutdown must not gate on them (and
                // its dispatch points must stop re-sending them).
                if (IsDisowned)
                {
                    return false;
                }

                lock (_lock)
                {
                    // The owed recovery downgrades count as pending: shutdown durability
                    // teardown must not complete while the fail-closed
                    // response to a heal-dropped (or heal-merged parked)
                    // grant receipt is still owed — a clean shutdown right
                    // after the final drain's own heal would otherwise
                    // relaunch into the stale persisted grant with the
                    // healed record carrying nothing left to re-trigger the
                    // downgrade (the close retry's next dispatch point
                    // consumes it, so this defers, never wedges). The
                    // merged-parked flavor re-detects at the NEXT launch's
                    // construction scan regardless (the receipt is
                    // retained), so its hold here is belt only.
                    return _entries.Count > 0 || _dirty || _loadFailed ||
                           _recoveryDroppedPendingGrant || _recoveryMergedParkedPendingGrant;
                }
            }
        }

        /// <summary>
        /// True while the construction-time read failure is unhealed — the
        /// dispatch chokepoint stands down on it (a NEW decision's mirror
        /// head must not overtake receipts trapped in the unreadable record:
        /// serial FIFO is per-scope decision order), exactly like TryPersist
        /// refuses writes on it.
        /// </summary>
        internal bool LoadFailed
        {
            get
            {
                lock (_lock)
                {
                    return _loadFailed;
                }
            }
        }

        internal bool LoadFailedForTesting
        {
            get { return LoadFailed; }
        }

        /// <summary>See <see cref="_loadRecordMalformed"/>.</summary>
        internal bool LoadedRecordMalformed
        {
            get
            {
                lock (_lock)
                {
                    return _loadRecordMalformed && !_explicitDecisionAuthorizesReplacement;
                }
            }
        }

        internal void AuthorizeExplicitDecisionReplacement()
        {
            lock (_lock)
            {
                _explicitDecisionAuthorizesReplacement = true;
                if (_loadRecordMalformed && _store != null) _dirty = true;
            }
        }

        private static bool MatchesTuple(ShardPilotConsentOutboxEntry entry,
            string workspaceId, string appId, string environmentId)
        {
            return entry != null && string.Equals(entry.WorkspaceId, workspaceId, StringComparison.Ordinal) &&
                   string.Equals(entry.AppId, appId, StringComparison.Ordinal) &&
                   string.Equals(entry.EnvironmentId, environmentId, StringComparison.Ordinal);
        }

        private static bool SameReceiptIdentity(ShardPilotConsentOutboxEntry left, ShardPilotConsentOutboxEntry right)
        {
            return right != null && MatchesTuple(left, right.WorkspaceId, right.AppId, right.EnvironmentId) &&
                   string.Equals(left.IdempotencyKey, right.IdempotencyKey, StringComparison.Ordinal);
        }

        /// <summary>
        /// True when a recovery re-read dropped a Mode-B identity-mismatched
        /// PENDING ANALYTICS GRANT — the construction could not see it (the
        /// record was unreadable then), so the constructor's identity
        /// downgrade never ran: the client consumes this at a dispatch point
        /// and runs the same downgrade-to-Unknown machinery.
        /// </summary>
        internal bool RecoveryDroppedPendingGrant
        {
            get
            {
                lock (_lock)
                {
                    return _recoveryDroppedPendingGrant;
                }
            }
        }

        internal bool ConsumeRecoveryDroppedPendingGrant()
        {
            lock (_lock)
            {
                var value = _recoveryDroppedPendingGrant;
                _recoveryDroppedPendingGrant = false;
                return value;
            }
        }

        /// <summary>
        /// True when a recovery re-read MERGED a retained, unsuperseded,
        /// PARKED analytics-GRANT entry the construction could not see (the
        /// record was unreadable then, so the constructor's
        /// parked-pending-grant downgrade never ran). The client consumes this at a dispatch point and runs the
        /// same fail-closed downgrade; until it does, the grant-receipt gate holds the
        /// pipeline anyway (an UNSUPERSEDED parked grant is never skipped).
        /// Parked-ness is judged with the owner's published predicate at
        /// merge time; a merge with no predicate published counts as parked
        /// for the flag — fail-closed.
        /// </summary>
        internal bool RecoveryMergedParkedPendingGrant
        {
            get
            {
                lock (_lock)
                {
                    return _recoveryMergedParkedPendingGrant;
                }
            }
        }

        internal bool ConsumeRecoveryMergedParkedPendingGrant()
        {
            lock (_lock)
            {
                var value = _recoveryMergedParkedPendingGrant;
                _recoveryMergedParkedPendingGrant = false;
                return value;
            }
        }

        internal bool Dirty
        {
            get
            {
                lock (_lock)
                {
                    return _dirty;
                }
            }
        }

        internal int Count
        {
            get
            {
                lock (_lock)
                {
                    return _entries.Count;
                }
            }
        }

        /// <summary>The oldest retained entry — the only one ever in flight (serial FIFO delivery).</summary>
        internal ShardPilotConsentOutboxEntry Head
        {
            get
            {
                lock (_lock)
                {
                    return _entries.Count > 0 ? _entries[0] : null;
                }
            }
        }

        /// <summary>
        /// The <c>decided_at</c> of the NEWEST retained OWN-SCOPE receipt
        /// when it is a DENIAL; null when that scope's trail is empty or
        /// ends with a grant. Foreign evidence never documents this scope's
        /// backend decision. Consulted by the registration catch-up of a live
        /// in-session denial: the scope's own denial receipt is minted
        /// whenever the trail does NOT already end with THIS decision (or a
        /// newer one) — an empty trail mints, because empty is
        /// indistinguishable from delivered-and-pruned (a scope whose grant
        /// receipt already delivered has an empty outbox while the
        /// backend's last word is still granted), and a tail denial OLDER
        /// than the live decision mints too: the trail is append-only and
        /// the backend resolves receipts last-write-wins by decided_at, so
        /// suppressing the newer denial would freeze the backend on the
        /// older decision's moment and reason (a later forced-minor denial
        /// would never be documented). A possibly-superfluous denial
        /// receipt is harmless; a suppressed one is a compliance gap.
        /// </summary>
        internal string TailDeniedDecidedAt
        {
            get
            {
                lock (_lock)
                {
                    for (var i = _entries.Count - 1; i >= 0; i--)
                    {
                        var tail = _entries[i];
                        if (OwnScope.Matches(tail)) return tail.AnalyticsGranted ? null : tail.DecidedAt;
                    }

                    return null;
                }
            }
        }

        /// <summary>Snapshot copy of the retained entries, oldest first.</summary>
        internal List<ShardPilotConsentOutboxEntry> SnapshotEntries()
        {
            lock (_lock)
            {
                return new List<ShardPilotConsentOutboxEntry>(_entries);
            }
        }

        internal List<ShardPilotConsentOutboxEntry> SnapshotForTesting()
        {
            return SnapshotEntries();
        }

        /// <summary>
        /// Appends a decision's receipt to the in-memory mirror ONLY (the
        /// caller runs this in the same critical section that flips the
        /// consent state; the durable write follows outside it via
        /// <see cref="TryPersist"/>). Invalid entries are refused fail-safe —
        /// nothing unsendable may enter the trail.
        /// </summary>
        internal bool AppendToMirror(ShardPilotConsentOutboxEntry entry)
        {
            if (Sanitize(entry) == null)
            {
                return false;
            }

            lock (_lock)
            {
                _entries.Add(entry);

                // Cap discipline without a durable write (no backend): the
                // mirror itself is the record, so adoption happens here —
                // denial-preferring, like the save-side cap.
                // With a backend the cap is applied by the SAVE (a failed
                // save must never evict), so the mirror is left intact.
                if (_store == null && _entries.Count > MaxEntries)
                {
                    EvictOnePreferringGrants(_entries);
                }

                // The new entry is not on disk yet; until TryPersist lands
                // the write is owed.
                _dirty = _store != null;
            }

            return true;
        }

        /// <summary>
        /// Stamps every retained analytics-GRANT entry
        /// <see cref="ShardPilotConsentOutboxEntry.Superseded"/> except the
        /// one named by <paramref name="exceptIdempotencyKey"/> (the NEW
        /// decision's own receipt — the state's backer, which must stay
        /// unstamped; null exempts nothing). Called ONLY AFTER the newer
        /// decision's consent state has DURABLY persisted (a stamp that lands while the new state's persist
        /// failed would let a stale persisted Granted skip the
        /// parked-pending-grant downgrade at the next launch — the
        /// fail-open direction). Retention and delivery are untouched; only
        /// the parked-pending-grant construction downgrade (and the grant-receipt
        /// gate's parked-skip) consult the stamp. Stamping marks the write
        /// owed so the stamp lands with the caller's follow-up persist (or
        /// the standard owed-write retries); a kill before it lands merely
        /// re-downgrades once at the next launch — the fail-CLOSED
        /// residual.
        /// </summary>
        internal void MarkRetainedAnalyticsGrantsSuperseded(string exceptIdempotencyKey)
        {
            lock (_lock)
            {
                var changed = false;
                for (var i = 0; i < _entries.Count; i++)
                {
                    var entry = _entries[i];
                    if (OwnScope.Matches(entry) && entry.AnalyticsGranted && !entry.Superseded &&
                        !string.Equals(entry.IdempotencyKey, exceptIdempotencyKey, StringComparison.Ordinal))
                    {
                        entry.Superseded = true;
                        changed = true;
                    }
                }

                if (changed && _store != null)
                {
                    _dirty = true;
                }
            }
        }

        /// <summary>
        /// Stamps only the PARKED unsuperseded analytics grants (judged
        /// with the owner's published predicate) — the recovery-heal
        /// flavor of the supersede stamp: a
        /// heal-merged parked grant that arrived AFTER a fresh, durably
        /// established grant's own stamp pass must be stamped by the
        /// fresh-decision recovery branch, or the grant-receipt gate would hold every
        /// batch on it until relaunch. Non-parked retained grants are
        /// untouched — the fresh decision's own (dispatchable) receipt
        /// among them.
        /// </summary>
        internal void MarkParkedAnalyticsGrantsSuperseded()
        {
            lock (_lock)
            {
                var parked = ParkedPredicateForGate;
                if (parked == null)
                {
                    return;
                }

                var changed = false;
                for (var i = 0; i < _entries.Count; i++)
                {
                    var entry = _entries[i];
                    if (OwnScope.Matches(entry) && entry.AnalyticsGranted && !entry.Superseded && parked(entry))
                    {
                        entry.Superseded = true;
                        changed = true;
                    }
                }

                if (changed && _store != null)
                {
                    _dirty = true;
                }
            }
        }

        /// <summary>
        /// Rewrites the durable record from the current mirror: sanitize,
        /// cap (evicting OLDEST first), write. On SUCCESS the mirror ADOPTS
        /// what storage actually kept (post-sanitize, post-cap) and the owed
        /// flag clears; <paramref name="evicted"/> reports how many entries
        /// the cap displaced. On FAILURE nothing is evicted, the mirror
        /// keeps every entry, and the write stays owed — retried at every
        /// consent dispatch point, in the pause/quit persist, and at
        /// shutdown. Never throws.
        /// </summary>
        internal bool TryPersist(out int evicted)
        {
            evicted = 0;
            if (_store == null)
            {
                return false;
            }

            List<ShardPilotConsentOutboxEntry> kept;
            lock (_lock)
            {
                // A load FAILURE must heal (successful re-read + merge)
                // before any durable write: rewriting from the post-failure
                // mirror would clobber an unreadable-but-maybe-valid record
                // and lose the receipts it held. While the store still
                // refuses to read, the write stays owed and fail-closed.
                if (_loadFailed && !TryRecoverFromLoadFailureLocked())
                {
                    evicted = 0;
                    _dirty = true;
                    return false;
                }

                if (_loadRecordMalformed && !_explicitDecisionAuthorizesReplacement)
                {
                    // Acknowledgment, cleanup, pause and shutdown may alter the
                    // mirror, but none authorizes erasing damaged evidence. This
                    // maintenance rewrite is not a debt that can hold teardown.
                    // New catch-up receipts, however, remain a dirty delta for
                    // ownership handoff until a fresh decision permits saving.
                    _dirty = false;
                    foreach (var entry in _entries)
                    {
                        if (!entry.RetainedFromCorruption) _dirty = true;
                    }
                    return false;
                }

                kept = SanitizeList(_entries);
                while (kept.Count > MaxEntries)
                {
                    // Denial-preferring (see EvictOnePreferringGrants): the
                    // oldest PURE GRANT leaves first; the oldest denial only
                    // in an all-denials overflow.
                    EvictOnePreferringGrants(kept);
                    evicted++;
                }

                var serialized = Serialize(kept);

                // The ownership check is ATOMIC with the durable write: a
                // pre-checked-then-written flow would leave a TOCTOU window
                // where a successor claims the record (and appends) between
                // this instance's check and its Set — the stale writer would
                // then overwrite the successor's receipts anyway. Holding
                // the registry gate across the Set closes it: a successor's
                // construction-claim blocks until this write lands and its
                // LOAD then reads the settled record, or this writer sees
                // itself displaced and refuses. Lock order is instance lock
                // -> ownership gate, and no path nests them in the reverse
                // direction; the write itself is the cheap synchronous
                // key-value contract.
                lock (OwnershipGate)
                {
                    ShardPilotConsentOutbox owner;
                    if (Owners.TryGetValue(_ownershipKey, out owner) && !ReferenceEquals(owner, this))
                    {
                        // NEVER rewrite a record a successor owns: this
                        // instance's mirror is stale by definition (it
                        // predates everything the successor appended). The
                        // refusal is silent-by-contract — the acked-entry
                        // prune this write wanted is redundant (the
                        // successor's mirror holds the same entry and will
                        // re-send it; the server de-duplicates on
                        // idempotency key), and no write is owed by a
                        // non-owner.
                        evicted = 0;
                        return false;
                    }

                    try
                    {
                        _store.Set(_storageKey, serialized);
                        // Consume authority with the successful write, before
                        // a successor can claim ownership. Failed writes keep
                        // their outstanding decision-time authority for handoff.
                        _explicitDecisionAuthorizesReplacement = false;
                    }
                    catch
                    {
                        // A failed durable write NEVER evicts and never
                        // partially succeeds: the mirror keeps ruling the
                        // process and the write is owed.
                        evicted = 0;
                        _dirty = true;
                        return false;
                    }
                }

                _entries = kept;
                _loadRecordMalformed = false;
                _dirty = false;
                return true;
            }
        }

        /// <summary>
        /// Removes one settled entry (acknowledged, or terminally dropped)
        /// by idempotency key from the MIRROR — the caller rewrites the
        /// record afterwards. The mirror drops first: a failed rewrite never
        /// blocks the rest of the trail — the owed flag stays set, the
        /// rewrite retries at later dispatch points, and if the process dies
        /// first the next launch re-sends the stale entry and the server
        /// de-duplicates on the idempotency key.
        /// </summary>
        internal void RemoveFromMirror(string idempotencyKey)
        {
            lock (_lock) { RemoveFromMirror(FindByKey(idempotencyKey)); }
        }

        internal void RemoveFromMirror(ShardPilotConsentOutboxEntry receipt)
        {
            lock (_lock)
            {
                for (var i = 0; i < _entries.Count; i++)
                {
                    if (SameReceiptIdentity(_entries[i], receipt))
                    {
                        _entries.RemoveAt(i);
                        _dirty = _store != null;
                        return;
                    }
                }
            }
        }

        /// <summary>True while an entry with this idempotency key is retained in the mirror.</summary>
        internal bool Contains(string idempotencyKey)
        {
            lock (_lock)
            {
                for (var i = 0; i < _entries.Count; i++)
                {
                    if (string.Equals(_entries[i].IdempotencyKey, idempotencyKey, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        /// <summary>
        /// The LIVE retained entry with this idempotency key, or null. The
        /// settle path re-finds its receipt by key rather than trusting the
        /// dispatched object's identity: a displacement dirty-merge or a
        /// late recovery re-read can rebuild the mirror with fresh objects,
        /// and a key miss means the receipt already left the trail.
        /// </summary>
        internal ShardPilotConsentOutboxEntry FindByKey(string idempotencyKey)
        {
            lock (_lock)
            {
                for (var i = 0; i < _entries.Count; i++)
                {
                    if (string.Equals(_entries[i].IdempotencyKey, idempotencyKey, StringComparison.Ordinal))
                    {
                        return _entries[i];
                    }
                }

                return null;
            }
        }

        /// <summary>
        /// Read-only DURABLE-record probe (the denial-witness ladder's
        /// dirty-mirror arm): parses the store's CURRENT record — never the
        /// mirror, which a dirty state proves differs from disk — and
        /// reports whether any entry that would SURVIVE the next launch's
        /// construction (the standing Mode-B identity drop applied)
        /// satisfies the predicate. Mutates nothing: not the mirror, not
        /// the dirty flag, not the store. A read failure reports false —
        /// nothing durable can be proven from a store that refuses to
        /// answer.
        /// </summary>
        internal bool AnyDurableEntry(Func<ShardPilotConsentOutboxEntry, bool> predicate)
        {
            if (_store == null)
            {
                return false;
            }

            string raw;
            try
            {
                raw = _store.Get(_storageKey);
            }
            catch
            {
                return false;
            }

            bool sanitizedFromMalformed;
            var durable = ParseRecord(raw, out sanitizedFromMalformed);
            var filter = ModeBIdentityFilter;
            for (var i = 0; i < durable.Count; i++)
            {
                if (filter != null && !sanitizedFromMalformed && OwnScope.Matches(durable[i]) &&
                    IsIdentityMismatched(durable[i], filter.ActorIdentifier, filter.AnonymousId))
                {
                    continue;
                }

                if (predicate(durable[i]))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// The grant-receipt gate predicate: TRUE iff any retained receipt with
        /// <c>categories.analytics == true</c> has not been handed to the
        /// transport — parked behind a Retry-After/backoff window, queued
        /// behind another receipt, or simply not yet dispatched (e.g. a
        /// reloaded outbox right after relaunch). The in-flight HEAD releases
        /// the gate only for itself: with grant→deny→grant queued, the later
        /// grant still gates until its own dispatch.
        /// </summary>
        internal bool HasUndispatchedAnalyticsGrant(string inFlightIdempotencyKey)
        {
            // A DISPLACED client answers the gate from the CURRENT OWNER's
            // outbox: the scope's live receipt trail moved there, and an old
            // client's fire-and-forget close drain must not push its batch
            // onto events:batch while the successor still retains an
            // undispatched grant (e.g. queued behind an in-flight deny
            // head) — that batch would overtake the grant on the wire
            // exactly like any other grant-receipt gate violation. The owner's own
            // in-flight head keeps releasing the gate only for itself, via
            // the write-through key. (The displaced instance's OWN retained
            // mirror stops mattering: those entries are the successor's
            // record now — merged when they were a dirty delta.)
            if (IsDisowned)
            {
                ShardPilotConsentOutbox owner;
                lock (OwnershipGate)
                {
                    Owners.TryGetValue(_ownershipKey, out owner);
                }

                return owner != null &&
                       !ReferenceEquals(owner, this) &&
                       owner.OwnScope.HasUndispatchedAnalyticsGrant(owner.InFlightKeyForGate);
            }

            lock (_lock)
            {
                // An unreadable record cannot PROVE there is no undelivered
                // grant in it — the gate fails closed until a re-read heals
                // (the empty pipeline is still never gated: this predicate
                // is only consulted when there are events to hold).
                if (_loadFailed)
                {
                    return true;
                }

                // An OWED recovery downgrade holds the gate too: the heal
                // just dropped the identity-mismatched receipt that backed
                // the loaded grant, and the client's downgrade-to-Unknown is
                // pending at its next dispatch point — a batch shipped in
                // that gap would ride the unbacked grant receipt-less. The
                // window closes at the consuming dispatch point (the refusal
                // is retryable), so this never wedges.
                if (_recoveryDroppedPendingGrant)
                {
                    return true;
                }

                var parked = ParkedPredicateForGate;
                for (var i = 0; i < _entries.Count; i++)
                {
                    if (!OwnScope.Matches(_entries[i]) || !_entries[i].AnalyticsGranted)
                    {
                        continue;
                    }

                    // SUPERSEDED parked grants never arm the gate:
                    // a parked receipt is not dispatchable
                    // on this launch at all, and holding the batch leg for
                    // it would wedge every flush (and teardown's final
                    // drains) for as long as the credential context stays
                    // absent. Per-actor order is safe — the server keys the
                    // verified actor's events on the verified record, which
                    // this parked receipt (not any batch) carries. The skip
                    // requires the SUPERSEDE stamp: whenever the pipeline is legitimately open
                    // (Granted), every parked grant is either superseded
                    // (stamped by the newer decision that opened it) or the
                    // construction downgrade would have closed the state —
                    // so an UNSUPERSEDED parked grant here means the state
                    // and the trail disagree (e.g. a recovery merge the
                    // construction never saw), and the gate fails closed
                    // until the dispatch-point downgrade converges it.
                    if (_entries[i].Superseded && parked != null && parked(_entries[i]))
                    {
                        continue;
                    }

                    // An UNSUPERSEDED parked grant: not dispatchable AND not
                    // proven superseded — hold the gate (see above).
                    if (parked != null && parked(_entries[i]))
                    {
                        return true;
                    }

                    // The in-flight head releases the gate ONLY for itself,
                    // matched by exact idempotency key — never by position:
                    // with parked receipts retained ahead of the dispatch
                    // head, index 0 is not necessarily the claimed receipt.
                    var isInFlightHead = inFlightIdempotencyKey != null &&
                                         string.Equals(
                                             _entries[i].IdempotencyKey,
                                             inFlightIdempotencyKey,
                                             StringComparison.Ordinal);
                    if (!isInFlightHead)
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        /// <summary>
        /// Mode-B-ONLY direct-actor identity guard, run once at construction:
        /// tokens are minted
        /// bound to the CURRENT identity, so an ANON-kinded retained receipt
        /// whose actor is not the current anonymous id can never be
        /// delivered — re-sending it would pair the old actor with a token
        /// bound to the new identity and 401 forever, wedging the trail
        /// head (see <see cref="IsIdentityMismatched"/>). VERIFIED-kinded
        /// receipts never drop — they PARK under the client's vouching
        /// predicate instead. Such anon entries are dropped from the mirror
        /// (the caller rewrites the record); the dropped list is returned
        /// for diagnostics. Callers must not invoke this in Mode A or dual
        /// mode: the publishable key has no identity binding, so the
        /// historic actor is the correct subject and historic-identity
        /// receipts re-send unchanged there.
        ///
        /// This SDK exposes NO runtime identity-rotation
        /// API (the anonymous id is fixed at construction and only ever
        /// read into the construction-time mirror above), so that
        /// rotation-refusal rule has no call site. A future rotation API
        /// must refuse while <see cref="Pending"/> is true, including
        /// owed-prune work, so it cannot strand the trail mid-session.
        /// </summary>
        internal List<ShardPilotConsentOutboxEntry> DropIdentityMismatched(
            string currentActorIdentifier, string currentAnonymousId)
        {
            var dropped = new List<ShardPilotConsentOutboxEntry>();
            lock (_lock)
            {
                if (_loadRecordMalformed) return dropped;
                for (var i = _entries.Count - 1; i >= 0; i--)
                {
                    if (OwnScope.Matches(_entries[i]) &&
                        IsIdentityMismatched(_entries[i], currentActorIdentifier, currentAnonymousId))
                    {
                        dropped.Add(_entries[i]);
                        _entries.RemoveAt(i);
                        _dirty = _store != null;
                    }
                }
            }

            return dropped;
        }

        /// <summary>
        /// True when a retained receipt could NEVER be sent by any launch of
        /// this configuration: in a Mode-B-ONLY session
        /// the minted JWT vouches solely for the current anonymous subject
        /// (or a verified receipt's configured user), so an ANON-kinded
        /// receipt whose <c>actor_identifier</c> is not the current
        /// anonymous id has no credential that could ever lawfully carry it
        /// — re-sending it would 401/actor-mismatch forever and wedge the
        /// serial trail head. VERIFIED-kinded receipts are NEVER dropped
        /// here: a pending verified receipt at a signed-out launch may be
        /// the only record of that actor's consent change (worst of all an
        /// undelivered withdrawal) — it PARKS instead (the client's parked
        /// predicate) until a session vouches its actor again. The
        /// decision-time anonymous-id SNAPSHOT no longer votes in this
        /// judgment: the wire actor answers deliverability directly, and
        /// the snapshot stays retention metadata for the witness machinery.
        /// Callers gate on Mode-B-ONLY (a configured publishable PublishableKey
        /// gives anon receipts a binding-free credential — nothing drops in
        /// Mode A or dual mode).
        /// </summary>
        private static bool IsIdentityMismatched(
            ShardPilotConsentOutboxEntry entry,
            string currentActorIdentifier,
            string currentAnonymousId)
        {
            if (entry.RetainedFromCorruption) return false;

            // ONLY anon-kinded receipts are droppable here: verified
            // receipts park instead (they may be the only record of that
            // actor's decision), and an UNKNOWN kind (a future/foreign
            // format retained inert by the sanitizer) cannot have its actor
            // semantics judged by this build — it is kept inert, never
            // dropped on an actor compare it might not even mean.
            return string.Equals(entry.Kind, KindAnon, StringComparison.Ordinal) &&
                   !string.IsNullOrEmpty(entry.ActorIdentifier) &&
                   !string.Equals(entry.ActorIdentifier, currentAnonymousId, StringComparison.Ordinal);
        }

        /// <summary>Non-empty and within the 512-byte UTF-8 clamp.</summary>
        internal static bool IsValidIdentifier(string value)
        {
            return !string.IsNullOrEmpty(value) && Encoding.UTF8.GetByteCount(value) <= MaxIdentifierBytes;
        }

        /// <summary>
        /// A receipt is a PURE GRANT for cap-eviction purposes when its
        /// category map carries NO false value — with the single analytics
        /// category today that is exactly <see
        /// cref="ShardPilotConsentOutboxEntry.AnalyticsGranted"/>. Any false
        /// value makes it denial-carrying and eviction-protected: a recorded
        /// denial is the compliance-critical write (a lost denial fail-opens
        /// the actor server-side under fail-open-on-missing), while a lost
        /// grant only delays pipeline opening and is re-writable.
        /// </summary>
        internal static bool IsPureGrant(ShardPilotConsentOutboxEntry entry)
        {
            return entry.AnalyticsGranted;
        }

        /// <summary>
        /// Removes one over-cap entry using a denial-preferring,
        /// backer-protecting ranking:
        /// rank 1 the oldest SUPERSEDED pure grant, rank 2 the oldest
        /// denial that precedes a retained newer grant (that later grant
        /// superseded the withdrawal), rank 3 the oldest UNSUPERSEDED
        /// NON-PARKED pure grant (a pathological pile-up only; PARKED
        /// unsuperseded grants — the flavor that can back a stale Granted
        /// across a restart — outlast even denials, which in every
        /// reachable rank-3b shape positionally precede them), rank 4 the
        /// all-parked last resort. A GRANT APPEND whose overflow the SAFE
        /// ranks (1–2) cannot absorb is REFUSED fail-closed at the cap
        /// gate BEFORE it ever reaches an eviction pass — see
        /// <see cref="GrantAppendBlockedByCap"/> — so a fresh grant is
        /// never silently dropped, a recorded operative denial is never
        /// traded for one, and the durable Granted state's unsuperseded
        /// backing receipt is never displaced by a denial append.
        /// </summary>
        private void EvictOnePreferringGrants(List<ShardPilotConsentOutboxEntry> entries)
        {
            var parkedJudge = ParkedPredicateForGate;

            // Rank 1 — the oldest SUPERSEDED pure grant: a newer explicit
            // decision durably replaced it as state-backer, so it is the
            // first eligible evictee.
            for (var i = 0; i < entries.Count; i++)
            {
                if (IsPureGrant(entries[i]) && entries[i].Superseded)
                {
                    entries.RemoveAt(i);
                    return;
                }
            }

            // Rank 2 — the oldest denial that PRECEDES a retained newer
            // analytics grant (the user's own later grant superseded that
            // withdrawal; its delivery value is historical record, not the
            // operative protection). The NEWEST entry never counts as the
            // superseding witness: for the grant-append gate's simulation
            // that would let the appended grant justify evicting the very
            // denials the consent_outbox_full contract protects, and at a
            // load-time overflow the newest entry's own backing status is
            // still unjudged. Without this rank
            // a denial append at the cap evicted the UNSUPERSEDED grant
            // still backing the durable Granted state — after a restart
            // nothing was left to hold the grant-receipt gate or trigger the
            // parked-pending-grant downgrade, and the scope reopened under
            // the stale grant.
            var newestSupersedingGrant = -1;
            for (var j = 0; j < entries.Count - 1; j++)
            {
                if (IsPureGrant(entries[j]))
                {
                    newestSupersedingGrant = j;
                }
            }

            if (newestSupersedingGrant >= 0)
            {
                for (var i = 0; i < newestSupersedingGrant; i++)
                {
                    if (!IsPureGrant(entries[i]))
                    {
                        entries.RemoveAt(i);
                        return;
                    }
                }
            }

            // Rank 3 — the oldest UNSUPERSEDED pure grant, only when no
            // superseded backer and no superseded denial exists (a trail of
            // unsuperseded grants and operative tail denials — reaching
            // here requires the pathological pile-up of failed persists).
            // PARKED unsuperseded grants are skipped when the owner's
            // predicate is published: a parked
            // grant is the one flavor that can silently back a stale
            // persisted Granted across a restart — a heal that recovered
            // it must not have its own save evict it before the
            // downgrade consumes the recovery flag; a dispatchable
            // unsuperseded grant resolves through delivery instead.
            for (var i = 0; i < entries.Count; i++)
            {
                if (IsPureGrant(entries[i]) &&
                    (parkedJudge == null || !parkedJudge(entries[i])))
                {
                    entries.RemoveAt(i);
                    return;
                }
            }

            // Rank 3b — every remaining grant is PARKED-unsuperseded: the
            // oldest denial leaves before any of them. In every reachable
            // shape here the retained denials positionally PRECEDE the
            // parked grant (a tail denial newer than every grant implies a
            // persisted non-granted state, which the parked grant then
            // cannot be backing), so their protection value is historical
            // while the parked grant may be the stale Granted's only
            // remaining witness across a restart.
            for (var i = 0; i < entries.Count; i++)
            {
                if (!IsPureGrant(entries[i]))
                {
                    entries.RemoveAt(i);
                    return;
                }
            }

            // Rank 4 — everything retained is a parked grant: the oldest
            // entry is the absolute last resort.
            entries.RemoveAt(0);
        }

        /// <summary>
        /// The <c>consent_outbox_full</c> fail-closed grant gate:
        /// TRUE when appending ONE MORE pure-grant
        /// receipt would overflow the cap by more than the SAFELY evictable
        /// pool — superseded pure grants plus denials a retained newer
        /// grant superseded (the cap ranking's ranks 1–2) — i.e. the cap
        /// pass, simulated over the SANITIZED mirror, would have to evict
        /// the fresh grant itself, trade an operative recorded denial for
        /// it, or displace an UNSUPERSEDED grant that may still back the
        /// durable Granted state. Judged over the FULL post-append
        /// overflow, not any-one-entry: a failed durable write never
        /// evicts, so the mirror can sit OVER the cap, where a single safe
        /// evictee cannot absorb an overflow of two. Callers refuse the
        /// grant BEFORE any state flips; denial appends are never gated.
        /// </summary>
        internal bool GrantAppendBlockedByCap()
        {
            lock (_lock)
            {
                var sanitized = SanitizeList(_entries);
                var overflowAfterAppend = sanitized.Count + 1 - MaxEntries;
                if (overflowAfterAppend <= 0)
                {
                    return false;
                }

                // The evictable pool mirrors the cap pass's SAFE ranks: SUPERSEDED pure grants
                // (rank 1) and denials preceding a retained newer grant
                // (rank 2). The HYPOTHETICAL appended grant never counts
                // as the superseding witness — it is simply not in the
                // list, which keeps a fresh grant from justifying denial
                // evictions and gutting the consent_outbox_full contract —
                // but every CURRENTLY RETAINED grant does, the tail
                // included (the real post-append
                // pass excludes only the appended newest, so an existing
                // tail grant legitimately witnesses evicting an older
                // denial; excluding it here refused grants the pass would
                // have retained). An UNSUPERSEDED old grant is NOT
                // evictable for a new grant: it may still back the durable
                // Granted state, and displacing it would strand a restart
                // with no receipt to hold the grant-receipt gate or trigger the
                // parked-pending-grant downgrade.
                var evictable = 0;
                var newestSupersedingGrant = -1;
                for (var j = 0; j < sanitized.Count; j++)
                {
                    if (IsPureGrant(sanitized[j]))
                    {
                        newestSupersedingGrant = j;
                    }
                }

                for (var i = 0; i < sanitized.Count; i++)
                {
                    if (IsPureGrant(sanitized[i]) && sanitized[i].Superseded)
                    {
                        evictable++;
                    }
                    else if (!IsPureGrant(sanitized[i]) && i < newestSupersedingGrant)
                    {
                        evictable++;
                    }
                }

                return overflowAfterAppend > evictable;
            }
        }

        /// <summary>
        /// The same scope-hash convention the event spool's file naming
        /// uses (16 hex chars of SHA-256 over the newline-joined scope), so
        /// outbox records get the identical per-scope isolation.
        /// </summary>
        internal static string ScopeHash(string workspaceId, string appId, string environmentId)
        {
            using (var sha = SHA256.Create())
            {
                var digest = sha.ComputeHash(Encoding.UTF8.GetBytes(workspaceId + "\n" + appId + "\n" + environmentId));
                var builder = new StringBuilder(16);
                for (var i = 0; i < 8; i++)
                {
                    builder.Append(digest[i].ToString("x2", CultureInfo.InvariantCulture));
                }

                return builder.ToString();
            }
        }

        internal string StorageKeyForTesting
        {
            get { return _storageKey; }
        }

        private void Load()
        {
            if (_store == null)
            {
                return;
            }

            string raw;
            try
            {
                raw = _store.Get(_storageKey);
            }
            catch
            {
                // A READ FAILURE is NOT a successful read of garbage: the
                // record may hold perfectly valid undelivered receipts —
                // including a grant whose delivery the grant-receipt gate exists to
                // sequence — so loading "empty" here would silently release
                // the gate and let the first post-restart batch ship
                // receipt-less. Fail CLOSED instead: the outbox reports
                // pending/dirty (a close is held on it; the batch
                // leg stays gated), durable writes are refused so a stale
                // mirror can never clobber the unreadable-but-maybe-valid
                // record, and every dispatch point's owed-write retry
                // attempts a RE-READ until the store answers. A valid reread
                // releases this transient refusal; a malformed reread instead
                // holds the evidence until a fresh explicit decision.
                lock (_lock)
                {
                    _loadFailed = true;
                    _dirty = true;
                }

                return;
            }

            bool sanitizedFromMalformed;
            var loaded = ParseRecord(raw, out sanitizedFromMalformed);
            lock (_lock)
            {
                _entries = loaded;
                _loadRecordMalformed = sanitizedFromMalformed;
            }
        }

        /// <summary>
        /// Parses individually valid receipts without rewriting the source.
        /// Absence is valid empty evidence. Any discarded non-empty content
        /// (whole JSON, missing receipt array, or damaged entry) imposes the
        /// corruption hold; an empty/partial mirror never proves delivery.
        /// Intact receipts recovered beside damage retain that provenance on
        /// a later authorized rewrite, preserving their credential parking.
        /// </summary>
        private static List<ShardPilotConsentOutboxEntry> ParseRecord(string raw, out bool sanitizedFromMalformed)
        {
            sanitizedFromMalformed = false;
            var loaded = new List<ShardPilotConsentOutboxEntry>();
            if (string.IsNullOrEmpty(raw))
            {
                return loaded;
            }

            Dictionary<string, object> record;
            if (!ShardPilotJsonParser.TryParseObject(raw, out record))
            {
                // A wholly garbled record loads as empty.
                sanitizedFromMalformed = true;
                return loaded;
            }

            object receiptsValue;
            var receipts = record.TryGetValue("receipts", out receiptsValue)
                ? receiptsValue as List<object>
                : null;
            if (receipts == null)
            {
                sanitizedFromMalformed = true;
                return loaded;
            }

            for (var i = 0; i < receipts.Count; i++)
            {
                var entry = Sanitize(ParseEntry(receipts[i] as Dictionary<string, object>));
                if (entry != null)
                {
                    loaded.Add(entry);
                }
                else
                {
                    sanitizedFromMalformed = true;
                }
            }

            if (sanitizedFromMalformed)
            {
                foreach (var entry in loaded) entry.RetainedFromCorruption = true;
            }

            return loaded;
        }

        /// <summary>
        /// Attempts to heal a load FAILURE with a fresh read. Caller must
        /// hold <c>_lock</c>. On a successful re-read the record's entries
        /// (the older ones) are merged AHEAD of any post-failure mirror
        /// appends, de-duplicated by idempotency key, the Mode-B identity
        /// filter the construction could not apply is applied late, and the
        /// write stays owed only when the merged mirror differs from the
        /// record. Returns false while the store still refuses to read.
        /// </summary>
        private bool TryRecoverFromLoadFailureLocked()
        {
            if (!_loadFailed)
            {
                return true;
            }

            string raw;
            try
            {
                raw = _store.Get(_storageKey);
            }
            catch
            {
                return false;
            }

            bool sanitizedFromMalformed;
            var recovered = ParseRecord(raw, out sanitizedFromMalformed);
            if (sanitizedFromMalformed)
            {
                // The recovered read lost content to the sanitizer: the
                // withheld-marker completion decision must know (caller
                // holds _lock).
                _loadRecordMalformed = true;
            }

            // The construction-time Mode-B identity drop ran against the
            // unreadable record: apply it to the late-recovered entries so a
            // stale-identity receipt cannot wedge the trail head behind
            // 401s — the same BOTH-component comparison (anon snapshot AND
            // stored actor vs the current UserId-then-Anon actor).
            var filter = ModeBIdentityFilter;
            var droppedByFilter = 0;
            if (filter != null && !_loadRecordMalformed)
            {
                // DURABILITY ORDERING for a dropped pending GRANT: once this
                // heal completes, the caller durably saves the healed record
                // WITHOUT the grant receipt — after which NOTHING on disk
                // can re-detect the downgrade this drop owes (the ctor drop
                // of the next launch finds no mismatch; the in-memory flag
                // below dies with a kill). The downgrade must therefore be
                // durable FIRST: the same fail-closed marker layer the
                // constructor's downgrade uses is written BEFORE the drop is
                // allowed to proceed, so a kill at any point leaves either
                // [record still holds the mismatched grant -> next launch
                // re-detects and re-runs the downgrade] or [marker durable
                // -> next launch's construction consults it and fails the
                // stale grant closed]. When the marker cannot be written the
                // heal itself is refused (stays load-failed, retried) — the
                // store is refusing writes anyway, so the record rewrite
                // could not land either.
                var dropsPendingGrant = false;
                for (var i = 0; i < recovered.Count; i++)
                {
                    if (OwnScope.Matches(recovered[i]) && recovered[i].AnalyticsGranted &&
                        IsIdentityMismatched(recovered[i], filter.ActorIdentifier, filter.AnonymousId))
                    {
                        dropsPendingGrant = true;
                        break;
                    }
                }

                if (dropsPendingGrant && !ShardPilotConsent.RecordPendingDowngrade(_store))
                {
                    return false;
                }

                for (var i = recovered.Count - 1; i >= 0; i--)
                {
                    if (OwnScope.Matches(recovered[i]) &&
                        IsIdentityMismatched(recovered[i], filter.ActorIdentifier, filter.AnonymousId))
                    {
                        if (recovered[i].AnalyticsGranted)
                        {
                            // The construction's identity downgrade could not
                            // see this pending GRANT (the record was
                            // unreadable then): flag it so the client runs
                            // the same downgrade-to-Unknown at a dispatch
                            // point — otherwise the heal would leave the new
                            // identity publishing under a grant whose receipt
                            // can never reach the backend. (Made durable by
                            // the marker written above; the dispatch-point
                            // downgrade clears both when it completes.)
                            _recoveryDroppedPendingGrant = true;
                        }

                        recovered.RemoveAt(i);
                        droppedByFilter++;
                    }
                }
            }

            var postFailureAppends = _entries;
            var merged = new List<ShardPilotConsentOutboxEntry>(recovered);
            for (var i = 0; i < postFailureAppends.Count; i++)
            {
                var alreadyPresent = false;
                for (var j = 0; j < merged.Count; j++)
                {
                    if (SameReceiptIdentity(merged[j], postFailureAppends[i]))
                    {
                        alreadyPresent = true;
                        break;
                    }
                }

                if (!alreadyPresent)
                {
                    merged.Add(postFailureAppends[i]);
                }
            }

            _entries = merged;
            _loadFailed = false;
            _dirty = _store != null && (postFailureAppends.Count > 0 || droppedByFilter > 0);

            // A recovered UNSUPERSEDED PARKED analytics grant is one the
            // construction's parked-pending-grant downgrade could not see
            // (the record was unreadable then): flag it so the client runs
            // the same fail-closed downgrade at a dispatch point. Judged with the owner's published
            // parking predicate; a missing predicate counts as parked —
            // fail-closed, and the grant-receipt gate holds the pipeline on the same
            // condition until the downgrade converges.
            var parkedJudge = ParkedPredicateForGate;
            for (var i = 0; i < merged.Count; i++)
            {
                if (OwnScope.Matches(merged[i]) && merged[i].AnalyticsGranted && !merged[i].Superseded &&
                    (parkedJudge == null || parkedJudge(merged[i])))
                {
                    _recoveryMergedParkedPendingGrant = true;
                    break;
                }
            }

            return true;
        }

        private static ShardPilotConsentOutboxEntry ParseEntry(Dictionary<string, object> value)
        {
            if (value == null)
            {
                return null;
            }

            // Kind reads STRICTLY: an ABSENT key
            // is the legacy shape (the sanitizer backfills "anon"), but a
            // PRESENT NON-STRING value (boolean/object/number — a corrupt
            // record or an incompatible writer) is structural corruption,
            // not a legacy record: backfilling it as anon would hand a
            // malformed receipt the anon credential/parking rules. Dropped
            // fail-safe, exactly like a non-boolean analytics flag.
            object rawKind;
            if (value.TryGetValue("kind", out rawKind) && !(rawKind is string))
            {
                return null;
            }

            ShardPilotConsentNoticeSnapshot notice;
            if (!TryReadNotice(value, out notice)) return null;
            var entry = new ShardPilotConsentOutboxEntry(notice)
            {
                IdempotencyKey = ReadString(value, "idempotency_key"),
                WorkspaceId = ReadString(value, "workspace_id"),
                AppId = ReadString(value, "app_id"),
                EnvironmentId = ReadString(value, "environment_id"),
                ActorIdentifier = ReadString(value, "actor_identifier"),
                Kind = rawKind as string,
                DecidedAt = ReadString(value, "decided_at"),
                Reason = ReadString(value, "reason"),
                AnonymousId = ReadString(value, "anonymous_id")
            };

            object analytics;
            if (!value.TryGetValue("analytics", out analytics) || !(analytics is bool))
            {
                // The analytics category must be a real boolean — anything
                // else is a malformed entry (dropped by the sanitizer).
                return null;
            }

            entry.AnalyticsGranted = (bool)analytics;
            object retained;
            entry.RetainedFromCorruption = value.TryGetValue("retained_from_corruption", out retained) &&
                                           retained is bool && (bool)retained;

            // Retention-only supersede stamp (never on the wire): lenient —
            // absent or non-boolean reads as false, the fail-CLOSED
            // direction (an unstamped parked pending grant still triggers
            // the construction downgrade).
            object superseded;
            entry.Superseded = value.TryGetValue("superseded", out superseded) &&
                               superseded is bool && (bool)superseded;

            // Retention-only decision seq (never on the wire): lenient with
            // the strict double bounds — absent/non-numeric/out-of-range
            // reads as 0 (the legacy pair, which never outranks a stamped
            // one — fail-closed for the belt, floor-neutral).
            object seqRaw;
            if (value.TryGetValue("decision_seq", out seqRaw) && seqRaw is long)
            {
                // The parser yields a long for integral JSON numbers — the
                // only shape Serialize writes; fractional/oversized doubles
                // are implausible for a sequence and read as the legacy 0
                // (a legacy pair never outranks a stamped one). Negative
                // values read legacy too; in-range clamps to the ceiling.
                var seq = (long)seqRaw;
                if (seq > 0)
                {
                    entry.DecisionSeq = seq > ShardPilotConsent.MaxConsentDecisionSeq
                        ? ShardPilotConsent.MaxConsentDecisionSeq
                        : seq;
                }
            }

            return entry;
        }

        private static string ReadString(Dictionary<string, object> value, string key)
        {
            object raw;
            return value.TryGetValue(key, out raw) ? raw as string : null;
        }

        internal static bool TryReadNotice(Dictionary<string, object> value, out ShardPilotConsentNoticeSnapshot notice)
        {
            notice = null;
            object version, locale, policy;
            var hasVersion = value.TryGetValue("notice_version", out version);
            var hasLocale = value.TryGetValue("notice_locale", out locale);
            var hasPolicy = value.TryGetValue("policy_version", out policy);
            if (!hasVersion && !hasLocale && !hasPolicy) return true;
            // Missing/non-string members are structural record damage, using
            // the existing corruption hold. Complete stored strings are copied
            // verbatim, never revalidated, upgraded or canonicalized at reload.
            if (!(version is string) || !(locale is string) || !(policy is string)) return false;
            notice = new ShardPilotConsentNoticeSnapshot((string)version, (string)locale, (string)policy);
            return true;
        }

        /// <summary>
        /// Parses a stored receipt timestamp (the ISO-8601 UTC shape
        /// <see cref="ShardPilotJson.FormatTimestamp"/> writes, full tick
        /// precision preserved) back to a UTC moment; null when unparseable
        /// — callers fail closed. Shared by the receipt sanitizer's timestamp
        /// rule, the withheld-marker validity gate, the settle-time
        /// distribution, and the denial catch-up's tail comparison.
        /// </summary>
        internal static DateTime? ParseReceiptTimestamp(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return null;
            }

            DateTime parsed;
            if (DateTime.TryParse(
                    value,
                    CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.AssumeUniversal |
                    System.Globalization.DateTimeStyles.AdjustToUniversal,
                    out parsed))
            {
                return DateTime.SpecifyKind(parsed, DateTimeKind.Utc);
            }

            return null;
        }

        /// <summary>
        /// The receipt sanitizer, shared by load and save: returns the entry
        /// when it satisfies every validity rule, null otherwise. Oversized
        /// identifiers are excluded from the dispatch mirror like any
        /// malformed field. Loading one holds the original record; only a
        /// fresh explicit decision can authorize a sanitized replacement.
        /// </summary>
        private static ShardPilotConsentOutboxEntry Sanitize(ShardPilotConsentOutboxEntry entry)
        {
            if (entry == null ||
                string.IsNullOrEmpty(entry.IdempotencyKey) ||
                string.IsNullOrEmpty(entry.WorkspaceId) ||
                string.IsNullOrEmpty(entry.AppId) ||
                string.IsNullOrEmpty(entry.EnvironmentId) ||
                string.IsNullOrEmpty(entry.DecidedAt) ||
                !IsValidIdentifier(entry.ActorIdentifier))
            {
                return null;
            }

            // The decision moment must PARSE, not merely exist (mirrors the
            // reason/state validation): every receipt this SDK mints stamps
            // ShardPilotJson.FormatTimestamp, so a non-ISO decided_at is a
            // corrupted record — and forwarding it would let the endpoint's
            // terminal 4xx REJECTION masquerade as a settle (the withheld
            // completion treats terminal drops as delivery outcomes for
            // policy rejections of well-formed decisions, never for bodies
            // the record corrupted). Dropped fail-safe; a dropped MARKED
            // grant then routes through the partially-sanitized fail-close
            // (the marker discharges instead of re-arming).
            if (ParseReceiptTimestamp(entry.DecidedAt) == null)
            {
                return null;
            }

            // The ONLY reason value the contract ever produces is the
            // forced-minor marker, and forced-minor is a DENIAL flavor.
            // An absent reason is valid on any decision, but a
            // reason on an analytics GRANT, or any reason string other than
            // the exact forced-minor marker, is a malformed record — dropped
            // fail-safe like every other sanitizer violation rather than
            // forwarded to the consent endpoint as a fabricated reason.
            if (entry.Reason != null &&
                (entry.AnalyticsGranted ||
                 !string.Equals(entry.Reason, ShardPilotConsent.ForcedMinorReason, StringComparison.Ordinal)))
            {
                return null;
            }

            if (entry.AnonymousId != null &&
                (entry.AnonymousId.Length == 0 || !IsValidIdentifier(entry.AnonymousId)))
            {
                return null;
            }

            // Actor kind: entries this SDK writes always carry one. An
            // ABSENT/EMPTY kind is the PRE-AMENDMENT legacy record shape and
            // backfills "anon" UNCONDITIONALLY
            // — deliberately NOT inferred as user_verified from a user-shaped
            // actor: a pre-kind snapshot cannot retroactively PROVE
            // verified-ness (the retired user-first snapshot took
            // self-asserted user ids even under a publishable key), so a
            // user_verified backfill would mint exactly the spoofable
            // verified claim the amendment forbids AND would park the
            // receipt behind the vouching predicate — a delivery regression
            // for legacy denials. The stored actor_identifier is never
            // rewritten. The backfill mutates the entry in place
            // (idempotent), so the mirror object and the sanitized copy
            // agree.
            //
            // A PRESENT value outside {anon, user_verified} —
            // "user_unverified" included, which this SDK never writes — is
            // most plausibly a FUTURE/foreign format this build cannot
            // interpret. It is RETAINED-INERT, not dropped: the receipt may be the only witness of a decision — worst
            // of all an undelivered withdrawal — so it stays in the trail
            // (cap-counted, never dispatched: the parking predicate parks
            // every non-{anon,user_verified} kind, and a retained-inert
            // PENDING GRANT therefore routes through the parked-pending-
            // grant fail-closed downgrade instead of silently unlatching
            // the pipeline). A kind that fails even the identifier bound
            // (oversized/empty-after-trim) is garbage, not a format, and
            // still drops fail-safe.
            if (string.IsNullOrEmpty(entry.Kind))
            {
                entry.Kind = KindAnon;
            }
            else if (!string.Equals(entry.Kind, KindAnon, StringComparison.Ordinal) &&
                     !string.Equals(entry.Kind, KindUserVerified, StringComparison.Ordinal) &&
                     !IsValidIdentifier(entry.Kind))
            {
                return null;
            }

            return entry;
        }

        private static List<ShardPilotConsentOutboxEntry> SanitizeList(List<ShardPilotConsentOutboxEntry> entries)
        {
            var kept = new List<ShardPilotConsentOutboxEntry>(entries.Count);
            for (var i = 0; i < entries.Count; i++)
            {
                if (Sanitize(entries[i]) != null)
                {
                    kept.Add(entries[i]);
                }
            }

            return kept;
        }

        private static string Serialize(List<ShardPilotConsentOutboxEntry> entries)
        {
            var builder = new StringBuilder(256);
            builder.Append("{\"version\":1,\"receipts\":[");
            for (var i = 0; i < entries.Count; i++)
            {
                if (i > 0)
                {
                    builder.Append(',');
                }

                var entry = entries[i];
                builder.Append("{\"idempotency_key\":");
                ShardPilotJson.AppendQuoted(builder, entry.IdempotencyKey);
                builder.Append(",\"workspace_id\":");
                ShardPilotJson.AppendQuoted(builder, entry.WorkspaceId);
                builder.Append(",\"app_id\":");
                ShardPilotJson.AppendQuoted(builder, entry.AppId);
                builder.Append(",\"environment_id\":");
                ShardPilotJson.AppendQuoted(builder, entry.EnvironmentId);
                builder.Append(",\"actor_identifier\":");
                ShardPilotJson.AppendQuoted(builder, entry.ActorIdentifier);
                if (!string.IsNullOrEmpty(entry.Kind))
                {
                    // Every sanitized entry carries a kind (the backfill runs
                    // before every save); the guard is defense in depth for a
                    // caller-constructed list.
                    builder.Append(",\"kind\":");
                    ShardPilotJson.AppendQuoted(builder, entry.Kind);
                }

                builder.Append(",\"analytics\":");
                builder.Append(entry.AnalyticsGranted ? "true" : "false");
                builder.Append(",\"decided_at\":");
                ShardPilotJson.AppendQuoted(builder, entry.DecidedAt);
                if (!string.IsNullOrEmpty(entry.Reason))
                {
                    builder.Append(",\"reason\":");
                    ShardPilotJson.AppendQuoted(builder, entry.Reason);
                }

                if (!string.IsNullOrEmpty(entry.AnonymousId))
                {
                    builder.Append(",\"anonymous_id\":");
                    ShardPilotJson.AppendQuoted(builder, entry.AnonymousId);
                }

                if (entry.RetainedFromCorruption)
                {
                    builder.Append(",\"retained_from_corruption\":true");
                }

                if (entry.Superseded)
                {
                    // Retention-only (like anonymous_id): the supersede
                    // stamp survives relaunches but never rides the wire.
                    builder.Append(",\"superseded\":true");
                }

                if (entry.DecisionSeq > 0)
                {
                    // Retention-only decision seq — the receipt's half of
                    // the decision pair; never on the wire.
                    builder.Append(",\"decision_seq\":");
                    builder.Append(entry.DecisionSeq.ToString(CultureInfo.InvariantCulture));
                }

                entry.AppendNotice(builder);
                builder.Append('}');
            }

            builder.Append("]}");
            return builder.ToString();
        }
    }
}
