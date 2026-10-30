// Input maps accept both existing non-null-value and nullable-value dictionaries.
// The invariant value argument stays oblivious; the optional map is annotated.
#nullable disable annotations
using EventProperties = System.Collections.Generic.IDictionary<string, object>;
#nullable enable annotations
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using WithdrawalFrame = ShardPilot.ShardPilotWithdrawalJournal.Frame;
using WithdrawalKind = ShardPilot.ShardPilotWithdrawalForm.Kind;

namespace ShardPilot
{
    /// <summary>
    /// The DURABILITY of one crash capture at the moment
    /// <see cref="ShardPilotClient.CaptureCrash(ShardPilotCrashEvent, Action{ShardPilotResult}, out ShardPilotCrashCaptureDurability)"/>
    /// returns — an additive internal seam for callers (the AEI next-launch
    /// pass) that must settle their OWN bookkeeping only once the report is
    /// safe. The public capture API and the JSON lane's semantics are
    /// unchanged: the boolean still means "dispatched (or durably parked)".
    /// </summary>
    internal enum ShardPilotCrashCaptureDurability
    {
        /// <summary>
        /// Nothing durable remains of this capture: it was refused before
        /// any persist, or an abort (raced denial/opt-out/close/ownership
        /// move) removed the write-ahead record again.
        /// </summary>
        None = 0,

        /// <summary>
        /// The write-ahead spool append landed and survived every recheck:
        /// the crash spool re-sends the report on a later launch even if
        /// the live send never completes.
        /// </summary>
        Spooled = 1,

        /// <summary>
        /// No crash spool exists in this configuration (SpoolEnabled or
        /// CrashSpoolEnabled off, no resolvable directory) — dispatch is
        /// the documented best-effort. Also reported for a successor
        /// handoff: the durable state then lives down the ownership chain
        /// and is not knowable here (the AEI pass runs on a freshly
        /// initialized client, where no successor exists).
        /// </summary>
        NoSpool = 2,

        /// <summary>
        /// A crash spool exists but the write-ahead append FAILED
        /// (oversized payload, write failure, disowned with no successor):
        /// the live send was still dispatched, but nothing durable backs
        /// it — a caller with replay bookkeeping must wait for the ack.
        /// </summary>
        SpoolAppendFailed = 3
    }

    public sealed partial class ShardPilotClient
    {
        private readonly ShardPilotConfig _config;
        private readonly bool _consentConfiguredActorInvalid;
        private bool _consentEventPurgeOwed;
        private bool _consentCrashPurgeOwed;
        private bool ConsentPurgeOwed => _consentEventPurgeOwed || _consentCrashPurgeOwed;
        private readonly IShardPilotTransport _transport;
        private readonly ShardPilotStatsCollector _stats = new ShardPilotStatsCollector();
        private readonly ShardPilotBreadcrumbRing _breadcrumbs = new ShardPilotBreadcrumbRing();
        private readonly Queue<ShardPilotEvent> _queue;
        private readonly List<FlushRequest> _flushRequests = new List<FlushRequest>();

        // Public flushes the worker is holding through a retryable failure
        // (or an unadmitted experiment application), and when it retries
        // them. Worker only.
        private readonly List<FlushRequest> _heldFlushes = new List<FlushRequest>();
        private DateTime _heldFlushRetryAtUtc = DateTime.MinValue;
        private static readonly TimeSpan HeldFlushApplicationRecheck = TimeSpan.FromMilliseconds(25);

        // The first terminal publish failure of the current flush pass. A
        // later retryable batch in the same drain must not let a held flush
        // outlive it and report success. Worker only.
        private ShardPilotResult _passTerminal;

        // The lowest intake sequence in the batch that met _passTerminal: the
        // rejection is a flush's answer only if it reaches back to the flush's
        // own events. Worker only.
        private long _passTerminalOldest;

        // One close operation, including its application pre-drain. Publish
        // the task before starting work; retain it after every completion.
        // The owning request and terminal result are guarded by _gate.
        private TaskCompletionSource<ShardPilotResult> _closeCompletion;
        private FlushRequest _owningCloseRequest;
        private ShardPilotResult _closeTerminal;

        // Last intake sequence handed out. Guarded by _gate.
        private long _intakeSequence;
        private readonly object _gate = new object();

        /// <summary>
        /// Durable offline spool for undeliverable events, or null when the
        /// spool is disabled, no directory is resolvable, or the platform is
        /// WebGL (documented no-op — no real synchronous filesystem). The
        /// spool lock is a LEAF lock: spool calls are made outside <c>_gate</c>
        /// and the spool never calls back into the client.
        /// </summary>
        private readonly ShardPilotSpool _spool;

        /// <summary>
        /// Events restored from the spool at construction, drained by the
        /// worker AHEAD of the live queue so the oldest events ship first.
        /// Kept separate from <c>_queue</c> so a large restored backlog never
        /// consumes <see cref="ShardPilotConfig.BufferSize"/> capacity and
        /// drops fresh events at enqueue. Guarded by <c>_gate</c>.
        /// </summary>
        private readonly Queue<ShardPilotEvent> _restoredPending = new Queue<ShardPilotEvent>();

        /// <summary>Event ids of experiment facts a real-subjects sentinel
        /// withdrew AFTER they were accepted into this pipeline. The purge
        /// removes queue/restored/spool copies immediately; ids linger here
        /// so the WORKER evicts its held batch's copies under the gate at
        /// its next cycle (an in-flight publish reads that list outside the
        /// lock, so the purge thread never mutates it), and so persist
        /// snapshots skip them. Bounded defensively.</summary>
        private readonly HashSet<string> _condemnedFactIds = new HashSet<string>(StringComparer.Ordinal);

        private const int MaxCondemnedFactIds = 2048;

        /// <summary>Overflow of the bounded index above for ids VERIFIED to
        /// sit in the worker's held batch when a single withdrawal exceeded
        /// the cap: the queue/restored copies were already extracted
        /// synchronously against the full id list, and these must still be
        /// evicted (and kept out of persist snapshots) at the worker's next
        /// cycle. Bounded by the batch the ids were found in; drained as
        /// the eviction confirms them.</summary>
        private readonly HashSet<string> _condemnedBatchOverflow = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>Durable pending-purge record for withdrawn experiment
        /// facts: newline-joined event ids in the settings store, written
        /// BEFORE any removal and cleared only once THIS client's spool
        /// CONFIRMS their copies are gone. A crash — or a failing spool
        /// rewrite — between the sentinel and the removal must not leave a
        /// withdrawn fact on disk for the next launch to resend: the load
        /// path filters restored events against this record and re-attempts
        /// the removal until it lands. The stored key is SCOPED PER SPOOL
        /// (base key + a fingerprint of the spool file path): the record is
        /// a debt against one specific spool file, and only the client that
        /// owns that spool may confirm — and clear — it; a sibling client
        /// over a different (or no) spool never touches a foreign debt. A
        /// spool-less client records no debt at all: it has no disk copies
        /// a crash could resend.</summary>
        internal const string CondemnedFactsStorageKey = "shardpilot.experiments.condemned_facts";

        /// <summary>Serializes read-modify-write cycles on the pending
        /// condemned-facts records across clients sharing one settings
        /// store.</summary>
        private static readonly object CondemnedFactsStoreGate = new object();

        /// <summary>Ids whose durable debt MARKER could not be saved yet
        /// (the settings store write failed): the spool removal is withheld
        /// until the marker lands — removing copies with no durable record
        /// would leave a crash window with nothing for the next launch to
        /// honor. Retried at every persist barrier. Guarded by
        /// <see cref="CondemnedFactsStoreGate"/>.</summary>
        private readonly HashSet<string> _condemnedPurgeDebt = new HashSet<string>(StringComparer.Ordinal);

        private ShardPilotExperiments _withdrawalDebts;
        private volatile bool _withdrawalScanReady, _withdrawalFramesUnknown;
        private ShardPilotWithdrawalWork BorrowWithdrawalWork(bool create, out bool absent)
        { return ShardPilotWithdrawalWork.Open(_config, ResolveExperimentsCachePath(), create, out absent); }
        private WithdrawalFrame[] ReadWithdrawalFrames(out bool known)
        {
            bool absent; WithdrawalFrame[] frames = null;
            using (var work = BorrowWithdrawalWork(false, out absent))
            {
                known = work == null ? absent : work.TryRead(out frames);
                _withdrawalFramesUnknown = !known;
                if (work == null || !known) return new WithdrawalFrame[0];
                var owed = new List<WithdrawalFrame>(frames);
                foreach (var frame in work.Unfinished())
                    if (!owed.Exists(held => held.Scope == frame.Scope && held.Id == frame.Id)) owed.Add(frame);
                return owed.ToArray();
            }
        }
        private WithdrawalFrame ConvergeWithdrawal(WithdrawalFrame proposed)
        {
            bool absent;
            using (var work = BorrowWithdrawalWork(true, out absent))
            {
                if (work == null) return null;
                return ConvergeWithdrawal(work, proposed, ResolveExperimentsCachePath(), true);
            }
        }
        private WithdrawalFrame ConvergeWithdrawal(ShardPilotWithdrawalWork work, WithdrawalFrame proposed, string path, bool scan)
        {
                WithdrawalFrame current;
                if (!work.TryGet(proposed.Scope, proposed.Id, out current)) return null;
                if (current == null && !work.TryPersist(null, proposed, out current)) return null;
                WithdrawalKind workKind; ShardPilotWithdrawalForm.TryKind(current, out workKind);
                if ((int)workKind < 2 && !current.Complete && _withdrawalScanReady && current.Store == ShardPilotWithdrawalWork.StoreFor(_config))
                {
                    var ids = new List<string>(current.Pending);
                    lock (_gate)
                    {
                        CollectExperimentKeyFactIds(_queue, current.Experiment, current.Keys, current.Spared.Contains, ids);
                        CollectExperimentKeyFactIds(_restoredPending, current.Experiment, current.Keys, current.Spared.Contains, ids);
                        CollectExperimentKeyFactIds(_workerBatch, current.Experiment, current.Keys, current.Spared.Contains, ids);
                        ids.AddRange(_ageWithdrawnAtHandoff);
                    }
                    var next = new WithdrawalFrame(current.Store, current.Scope, current.Id, current.Experiment, current.Revision + 1,
                        current.BoundaryTicks, current.Fixed, false, false, true, current.Scans, current.Keys, current.Spared, ids);
                    if (!work.TryPersist(current, next, out current)) return null;
                }
                var accepted = work.Converge(current,
                    frame => ShardPilotExperiments.ObserveWithdrawalAssignment(path, frame, _experiments?.RecordReadFailureForTesting),
                    frame => _experiments?.RecordReadFailureForTesting == null && ShardPilotExperiments.ApplyWithdrawalAssignment(path, frame,
                        _experiments?.WriteFailureForTesting ?? ShardPilotExperiments.StaticWriteFailureForTesting),
                    frame =>
                    {
                        WithdrawalKind kind; if (!ShardPilotWithdrawalForm.TryKind(frame, out kind)) return false;
                        if (kind == WithdrawalKind.Drop) return true;
                        if (frame.Store != ShardPilotWithdrawalWork.StoreFor(_config)) return false;
                        if (!scan) return false;
                        if (!_withdrawalScanReady) return false;
                        var quiet = ShardPilotExperiments.WithdrawalEmissionsQuiet(path, frame);
                        if (!quiet) return false;
                        var removed = kind == WithdrawalKind.Purge ? PurgeCondemnedExperimentFacts(new List<string>(frame.Pending), null) :
                            WithdrawExperimentKeyFacts(frame.Experiment, frame.Keys, frame.Spared.Contains, null);
                        if ((int)kind < 2 && frame.Pending.Count > 0)
                            removed &= PurgeCondemnedExperimentFacts(new List<string>(frame.Pending), null);
                        return quiet && removed && ShardPilotExperiments.WithdrawalEmissionsQuiet(path, frame);
                    });
                if (accepted != null && !accepted.Fixed && (int)workKind < 2 && scan && _withdrawalScanReady &&
                    accepted.Store == ShardPilotWithdrawalWork.StoreFor(_config))
                {
                    WithdrawExperimentKeyFacts(accepted.Experiment, accepted.Keys, accepted.Spared.Contains, null);
                    if (accepted.Pending.Count > 0) PurgeCondemnedExperimentFacts(new List<string>(accepted.Pending), null);
                }
                ShardPilotExperiments.ObserveWithdrawalProgress(path, accepted);
                return accepted;
        }
        private bool JournalFactPurge(List<string> ids)
        {
            if (ids == null || ids.Count == 0) return true;
            if (ids.Count > 512)
            {
                var complete = true;
                for (var i = 0; i < ids.Count; i += 512) complete &= JournalFactPurge(ids.GetRange(i, Math.Min(512, ids.Count - i)));
                return complete;
            }
            if (_spool == null) return PurgeCondemnedExperimentFacts(ids, null);
            PurgeCondemnedExperimentFacts(ids, null, true);
            lock (_gate) _condemnedPurgeDebt.UnionWith(ids);
            bool known; WithdrawalFrame proposed = null;
            foreach (var frame in ReadWithdrawalFrames(out known))
            {
                WithdrawalKind kind; ShardPilotWithdrawalForm.TryKind(frame, out kind);
                if (kind != WithdrawalKind.Purge) continue;
                var covers = true; foreach (var id in ids) covers &= frame.Pending.Contains(id);
                if (covers) { proposed = frame; break; }
            }
            proposed = proposed ?? new WithdrawalFrame(ShardPilotWithdrawalWork.StoreFor(_config),
                ShardPilotExperimentsLogic.BuildScope(_config.WorkspaceKey, _config.AppKey, _config.EnvironmentKey,
                    null, ShardPilotExperimentsLogic.CredentialFingerprint(_config.PublishableKey), _config.AppUrl),
                ShardPilotWithdrawalForm.NewId(WithdrawalKind.Purge), "purge", 1, 0, false, false, false, false,
                0, new string[0], new string[0], ids);
            var accepted = ConvergeWithdrawal(proposed);
            if (accepted == null) { PurgeCondemnedExperimentFacts(ids, null); return false; }
            if (accepted != null) lock (_gate) _condemnedPurgeDebt.ExceptWith(ids);
            return accepted != null && accepted.Complete;
        }
        private void RetryWithdrawalFrames()
        {
            _experiments?.ResumeWithdrawals();
            _withdrawalDebts?.ResumeWithdrawals();
            bool known;
            foreach (var frame in ReadWithdrawalFrames(out known)) ConvergeWithdrawal(frame);
            List<string> pending; lock (_gate) pending = new List<string>(_condemnedPurgeDebt);
            if (pending.Count > 0) JournalFactPurge(pending);
        }

        /// <summary>The per-spool pending-purge record key, or null for a
        /// spool-less client (no disk copies, no debt). The path is folded
        /// to LOWER-INVARIANT before fingerprinting — the same canonical
        /// casing the lifecycle registry's ownership comparisons use
        /// (case-insensitive on every platform): two clients whose
        /// SpoolDir spellings differ only by case share one physical
        /// spool on case-insensitive filesystems, and they must read/write
        /// ONE marker key, or a relaunch under the other casing would miss
        /// a still-owed purge and restore withdrawn facts. Clean-cut (no
        /// legacy-key probe): this file-routed key family is new in this
        /// change set — no released build ever derived the case-preserving
        /// form.</summary>
        private string CondemnedFactsKey
        {
            get
            {
                var spool = _spool;
                if (spool == null)
                {
                    return null;
                }

                return CondemnedFactsStorageKey + "." +
                       ShardPilotExperimentsLogic.CredentialFingerprint(
                           SpoolFilePath.ToLowerInvariant());
            }
        }

        /// <summary>
        /// The worker's held batch. A field (not a worker local) so
        /// <see cref="PersistPendingEvents"/> can snapshot it under
        /// <c>_gate</c> on application pause/quit; every MUTATION of the list
        /// happens under <c>_gate</c> for that reason, while the worker's own
        /// reads stay lock-free (only the worker mutates it).
        /// </summary>
        private readonly List<ShardPilotEvent> _workerBatch;

        /// <summary>
        /// The denial epoch the worker last reconciled its held batch against,
        /// shared with <see cref="PersistPendingEvents"/> so a pause snapshot
        /// never revives a held batch that a consent denial has already
        /// condemned (the denial also wiped the spool).
        /// </summary>
        private readonly ConsentEpoch _workerConsentEpoch = new ConsentEpoch();

        /// <summary>
        /// Monotonic count of held-batch settlements — a batch delivered
        /// (2xx), permanently rejected, or discarded by a consent denial.
        /// Bumped under <c>_gate</c> in the same lock hold that clears the
        /// held batch (see <see cref="RecordSettlementLocked"/>), BEFORE the
        /// settlement's own unlocked spool removal runs. Guarded by
        /// <c>_gate</c>.
        /// </summary>
        private long _batchSettlements;

        /// <summary>
        /// Serializes <see cref="PersistPendingEvents"/> calls (an
        /// application pause can land during a close, and both persist). The
        /// settled-during-persist record below is a single shared slot, so
        /// concurrent persists must not clear each other's record mid-flight.
        /// Ordering: taken before <c>_gate</c>/the spool lock, never inside
        /// either.
        /// </summary>
        private readonly object _persistGate = new object();

        /// <summary>
        /// True while a <see cref="PersistPendingEvents"/> append is running
        /// outside <c>_gate</c>. While set, every settlement records its
        /// events into <see cref="_settledDuringPersist"/> (same lock hold as
        /// the settlement itself), because the settlement's own spool removal
        /// may run before the in-flight append has put those entries on disk.
        /// Guarded by <c>_gate</c>.
        /// </summary>
        private bool _persistInFlight;

        /// <summary>
        /// The events settled while the current persist append was in flight;
        /// null when none. The persist's post-append reconciliation removes
        /// exactly (its snapshot ∩ this list) from the spool, then clears it.
        /// Guarded by <c>_gate</c>.
        /// </summary>
        private List<ShardPilotEvent> _settledDuringPersist;

        /// <summary>
        /// Count of spool appends currently in flight whose snapshots were
        /// taken under <c>_gate</c> (the persist append, the worker's
        /// transient-failure insurance append, and the displaced-client
        /// handoff): incremented in the SAME hold as the snapshot,
        /// decremented when the append lands. A sentinel purge consults it
        /// BEFORE its spool removal — a counted append may have snapshotted
        /// a fact before the purge's in-memory eviction indexed it, and its
        /// unlocked append can write that condemned fact back to disk AFTER
        /// the removal ran. While any such append is in flight the durable
        /// pending-purge marker's retirement is DEFERRED (the marker must
        /// outlive every append that could resurrect a condemned id); the
        /// completing append re-runs the marker-driven removal and only a
        /// confirmed post-append removal retires it. Guarded by
        /// <c>_gate</c>.
        /// </summary>
        private int _spoolAppendsInFlight;

        /// <summary>
        /// Ids of DISK-ONLY overflow events accepted this session and not yet
        /// settled. Queued, held, and restored events are all counted by the
        /// existing paths when a consent denial drops them; overflow events
        /// live only in the spool file, so the denial's wipe would otherwise
        /// erase them without any <c>Dropped</c> accounting — this set is
        /// folded into the dropped counter and cleared when a denial lands.
        /// BOUNDED accounting: ids whose entries leave the file (FIFO
        /// eviction reported back by the spool's appends) are pruned, and as
        /// a belt-and-braces clamp the set never exceeds
        /// <see cref="ShardPilotConfig.SpoolMaxEvents"/> (oldest dropped
        /// first, tracked by the companion insertion-order queue with lazy
        /// deletion) — an accounting-only approximation, so a long offline
        /// session cannot grow it without bound. Guarded by <c>_gate</c>.
        /// </summary>
        private readonly HashSet<string> _spooledOverflowIds = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>
        /// Insertion order companion of <see cref="_spooledOverflowIds"/>,
        /// used to clamp oldest-first. May momentarily hold ids already
        /// pruned from the set (lazy deletion); stale heads are drained on
        /// every add and the whole queue is compacted when it outgrows twice
        /// the cap. Guarded by <c>_gate</c>.
        /// </summary>
        private readonly Queue<string> _spooledOverflowOrder = new Queue<string>();
        private readonly string _batchEndpoint;
        private readonly string _consentEndpoint;
        private readonly IShardPilotKeyValueStore _settingsStore;
        private readonly IShardPilotHttpRequestSender _httpSender;
        private readonly ShardPilotDispatcher _crashCallbackHost;

        /// <summary>The experiment-assignment consumer, non-null exactly
        /// when <see cref="ShardPilotConfig.ExperimentsEnabled"/> — dark by
        /// default.</summary>
        private readonly ShardPilotExperiments _experiments;

        /// <summary>The persisted `spcid_` experiment subject id, or null
        /// while none has been provisioned. Read lock-free by the consumer's
        /// subject-id delegate (a reference read is atomic); written only
        /// through the consumer's mint path.</summary>
        private string _experimentsSubjectId;

        /// <summary>Thread-scoped close-drain window: the close path sets
        /// _closed at SUBMISSION, before its final drain runs — but the
        /// drain's owed-exposure sweep exists precisely to land facts after
        /// that point. The worker opens this window tightly around the
        /// close-time housekeeping sweep (which is synchronous — no yields
        /// inside it on either profile), so the internal sweep can enqueue
        /// while every public tracking path stays refused with
        /// "shutdown".</summary>
        [ThreadStatic]
        private static bool _experimentFactDrainWindow;
        private readonly string _crashEndpoint;
        private readonly string _tombstoneEndpoint;
        private long _nonFatalCrashCounter;

        /// <summary>
        /// Durable spool for crash reports, or null when disabled (the
        /// <see cref="ShardPilotConfig.SpoolEnabled"/> master switch or
        /// <see cref="ShardPilotConfig.CrashSpoolEnabled"/> is off, no
        /// <see cref="ShardPilotConfig.CrashKey"/> is configured, no
        /// directory is resolvable, or the platform is WebGL). A SEPARATE
        /// file from the analytics event spool: crash reports ship
        /// one-per-request to the crash endpoint in their own body shape.
        /// Every report is persisted at capture time — already scrubbed, the
        /// exact bytes that go to the wire — and removed when the crash
        /// ingest service acknowledges it. Like the event spool, its lock is
        /// a LEAF lock: calls are made outside <c>_gate</c> and the spool
        /// never calls back into the client.
        /// </summary>
        private readonly ShardPilotCrashSpool _crashSpool;

        /// <summary>
        /// True while a crash re-send pass is walking the spooled reports
        /// (serially, one request at a time). One pass at a time; a pass
        /// stopped by a retryable failure re-arms after
        /// <see cref="_crashResendNotBefore"/>. Guarded by <c>_gate</c>.
        /// </summary>
        private bool _crashResendActive;

        /// <summary>
        /// The earliest UTC time the next crash re-send pass may start:
        /// seeded from the PERSISTED Retry-After deadline at construction
        /// (server backpressure must survive a relaunch) and advanced by
        /// retryable re-send failures during the session. Guarded by
        /// <c>_gate</c>.
        /// </summary>
        private DateTime _crashResendNotBefore = DateTime.MinValue;

        /// <summary>
        /// Mode-B JWT cache + refresh state machine. Null in Mode A
        /// (publishable <c>sp_</c> key), where the Bearer is the static
        /// <see cref="ShardPilotConfig.PublishableKey"/>.
        /// </summary>
        private readonly ShardPilotTokenManager _tokenManager;
        private readonly ShardPilotDispatcher _fetchCallbackHost;
        private volatile bool _fetchCallbacksRetired;
        private bool FetchOwnerCurrent => !HostRetiring && !_fetchCallbacksRetired;
#if !(UNITY_WEBGL && !UNITY_EDITOR)
        private readonly AutoResetEvent _wake = new AutoResetEvent(false);

        // Not readonly: on the public construction path the worker is armed
        // AFTER the lifecycle registration (StartDispatchAfterRegistration),
        // outside the constructor.
        private Thread _worker;
#endif

        private bool _closed;
        private bool _closeRequested;
        // Set by a close's deadline: the worker tears down after any publish
        // in flight. Written under _gate, read with Volatile anywhere.
        private bool _closeDeadlineReached;
        private ShardPilotConsent.DecisionWork _closingDecision;
        private bool ClosingDecisionPending => Volatile.Read(ref _closingDecision) is { Completed: false };
        private int _consentUnpersistedReported;
        // Non-null while a close queues the session end it claimed: closes
        // arriving meanwhile wait here, so none can drain and stop the worker
        // before that end enters. Guarded by _gate.
        private List<FlushRequest> _closesAfterEnd;
        private bool _workerStopped;
        // All session state travels together. Each advance creates a new value,
        // so a provisional overflow operation can restore its complete predecessor
        // only while it still owns the current reference.
        private SessionState _sessionState = new SessionState(string.Empty, 0, false);
        // An owed-start operation owns this claim through its append verdict.
        // Guarded by _gate; the admission fast path reads it with Volatile.Read.
        private int _owedSessionStartThreadId;

        // Stable lifecycle identity is separate from per-event sequence revisions.
        // A start's verdict belongs to this identity even if another transition
        // supersedes it while its append is unlocked. Verdict and timestamp fields use _gate;
        // only StartAccepted is also read by the admission fast path.
        private sealed class SessionLifecycle
        {
            public readonly bool ReplacesOwedStart;
            public int PendingStartThreadId;
            public bool StartAccepted;
            public DateTime LatestAcceptedTimestampUtc;
            public DateTime AutomaticEndUtc;

            public SessionLifecycle(bool replacesOwedStart = false, int pendingStartThreadId = 0, bool startAccepted = false)
            {
                ReplacesOwedStart = replacesOwedStart;
                PendingStartThreadId = pendingStartThreadId;
                StartAccepted = startAccepted;
            }
        }

        private sealed class SessionState
        {
            public readonly string Id;
            public readonly long Sequence;
            public readonly SessionLifecycle Lifecycle;
            public bool OwesStart => OwedStart ||
                (Lifecycle.ReplacesOwedStart && !Volatile.Read(ref Lifecycle.StartAccepted));
            // An end arms this debt; the next ordinary enqueue emits a real
            // start before its event. Initial lazy creation remains unannounced.
            public readonly bool OwedStart;

            public SessionState(string id, long sequence, bool owedStart, SessionLifecycle lifecycle = null)
            {
                Id = id;
                Sequence = sequence;
                OwedStart = owedStart;
                Lifecycle = lifecycle ?? new SessionLifecycle();
            }

            public SessionState WithSequence(long sequence)
            {
                return new SessionState(Id, sequence, OwedStart, Lifecycle);
            }
        }

        /// <summary>
        /// When the app last went to background, or <c>default</c> when it is
        /// foreground. Resume and current activity evaluate the boundary
        /// against this stamp instead of a timer armed while backgrounded: a
        /// suspended mobile process runs no timers, so a timer-based end would
        /// never fire on the platforms it matters for. Guarded by
        /// <c>_gate</c>.
        /// </summary>
        private DateTime _backgroundedAtUtc;

        // A deadline belongs to one lifecycle and travels as a whole value.
        // The engine's background stamp remains separate: accepting a replacement
        // while backgrounded arms its own deadline without fabricating a resume.
        private sealed class SessionPause
        {
            public readonly SessionLifecycle Lifecycle;
            public readonly DateTime SinceUtc;
            public readonly long SinceMonotonicTicks;

            // Set under _gate by a resume that could not claim this pause
            // because a provisional replacement of its session was in flight:
            // the end that resume would have sent, or default when the pause
            // had not expired. A refused replacement's rollback settles it.
            public DateTime RacedResumeEndUtc;

            public SessionPause(SessionLifecycle lifecycle, DateTime sinceUtc, long sinceMonotonicTicks)
            {
                Lifecycle = lifecycle;
                SinceUtc = sinceUtc;
                SinceMonotonicTicks = sinceMonotonicTicks;
            }
        }

        private SessionPause _sessionPause;

        // Advanced by every resume, under _gate. An unlocked overflow append
        // compares it to tell whether a resume ran while it was in flight.
        private long _resumeGeneration;

        /// <summary>
        /// The lifecycle-transition count, carried as
        /// <c>props.lifecycle_transitions</c> on BOTH <c>app.session_started</c>
        /// and <c>app.session_ended</c>. A session's transitions are
        /// <c>end − start</c>, computed by the reader over the raw table; the
        /// SDK holds no per-session value. Guarded by <c>_gate</c>.
        /// </summary>
        /// <remarks>
        /// Never reset this counter at a session boundary. A cumulative value
        /// avoids resets in explicit start, lazy creation, owed-start discharge,
        /// overflow rollback and timeout handling. Readers derive the session's
        /// count from the cumulative values on its start and end events.
        ///
        /// The value counts every pause and every resume observed WHILE
        /// ANALYTICS CONSENT IS GRANTED, and it restarts at zero after any
        /// consent loss: <see cref="_lifecycleTransitionsEpoch"/> is the
        /// consent-denial epoch it accumulated under, and a stale stamp reads
        /// as zero. Every consent-loss site advances that epoch, so nothing
        /// that reaches the wire encodes a transition observed under a loss or
        /// before the most recent one — the rule the crash plane's breadcrumbs
        /// already keep ("denial leaves no trace of prior activity").
        ///
        /// <see cref="_lifecycleRun"/> names the value's LIFETIME: a random id
        /// minted at construction and re-minted whenever the value restarts. A
        /// reader pairs a start and an end only within one run. Without it a
        /// consent loss inside a session could read as a plausible
        /// non-negative delta once the post-loss transitions reach the start's
        /// value. It is random on purpose: a counter of losses would itself
        /// encode the history the restart erases.
        ///
        /// What the reader's query sees, stated as the limit it is. A session
        /// with an end and no start — the lazily created first session of an
        /// integration that never calls <c>SessionStart</c> — is unbracketed
        /// and drops out of the sample once per run; count them as ends whose
        /// <c>session_id</c> has no start. This is a declared limit of the
        /// count, accepted on 2026-09-01. A session with no end is
        /// invisible, as it always was. The resume that closes a session by
        /// timeout is inside the session it closes, because the increment
        /// precedes the boundary decision.
        /// </remarks>
        private long _lifecycleTransitions;

        /// <summary>
        /// The consent-denial epoch <see cref="_lifecycleTransitions"/>
        /// accumulated under. Guarded by <c>_gate</c>.
        /// </summary>
        private long _lifecycleTransitionsEpoch;

        /// <summary>
        /// The identity of <see cref="_lifecycleTransitions"/>'s current
        /// lifetime, carried as <c>props.lifecycle_run</c>. Guarded by
        /// <c>_gate</c>.
        /// </summary>
        private string _lifecycleRun = NewLifecycleRun();

        /// <summary>
        /// The monotonic source, injectable so the boundary's clock behaviour is
        /// testable without waiting on a real one.
        /// </summary>
        private Func<long> _monotonicTicks = System.Diagnostics.Stopwatch.GetTimestamp;

        /// <summary>The wall clock, injectable for the same reason.</summary>
        private Func<DateTime> _utcNow = () => DateTime.UtcNow;

        internal void SetMonotonicSourceForTesting(Func<long> source)
        {
            _monotonicTicks = source ?? System.Diagnostics.Stopwatch.GetTimestamp;
        }

        internal void SetWallClockForTesting(Func<DateTime> source)
        {
            _utcNow = source ?? (() => DateTime.UtcNow);
        }

        private ShardPilotConsentState _consentState = ShardPilotConsentState.Unknown;

        // Every reconstruction and transition passes this boundary before any
        // producer can observe Granted. A fallback identity must never inherit
        // consent for an explicitly invalid configured actor; denials keep
        // their original restriction. The shared persisted record is untouched.
        private void ApplyConsentState(ShardPilotConsentState state)
        {
            if (_consentState == ShardPilotConsentState.DeniedForcedMinor) return;
            _consentState = _consentConfiguredActorInvalid && state == ShardPilotConsentState.Granted
                ? ShardPilotConsentState.Unknown : state;
        }
        private long _consentDenialEpoch;

        /// <summary>Serializes the experiments consumer's final pre-send
        /// consent-epoch re-read + transport handoff against denial-epoch
        /// advances: every <c>_consentDenialEpoch</c> increment goes through
        /// <see cref="AdvanceConsentDenialEpochLocked"/>, which takes this
        /// gate INSIDE <c>_gate</c> (ordering: gate → egress gate; the
        /// consumer holds it bare, never nested under anything). Once a
        /// denial's advance is observable, zero assignment requests can
        /// start — the assignment plane's granted-only contract holds at
        /// the wire.</summary>
        private readonly object _experimentEgressGate = new object();

        /// <summary>
        /// Counts the EXPLICIT in-session consent decisions applied to
        /// THIS client — its own SetConsent, and live propagated / session
        /// -marker caught-up grant and denial applications. States merely
        /// INHERITED from a persisted read (construction load, refresh
        /// re-apply of a relaunch-loaded record) never count. Guarded by
        /// <c>_gate</c>. The identity-grant downgrade machinery consults
        /// it to stand down: a downgrade invalidates the LOADED grant that
        /// its dropped receipt (or cleared record) documented, and must
        /// never wipe a decision the player made after that record was
        /// read — e.g. the recovery-dropped-grant flag is consumed at a
        /// dispatch point that can run arbitrarily long after the heal, by
        /// which time a fresh explicit grant may have superseded the
        /// dropped lineage entirely.
        /// </summary>
        private long _consentDecisionGeneration;

        /// <summary>
        /// Decision-moment ticks of the live in-session denial this client's
        /// registration catch-up last minted its own-scope receipt for
        /// (0 = none). Keyed on the session denial marker's decided_at so
        /// repeated catch-ups — e.g. registration refreshes after the minted
        /// receipt delivered and pruned, leaving the trail empty again —
        /// never re-mint the SAME decision, while a NEWER denial (fresh
        /// moment) mints normally. Touched only under the process-wide
        /// decision gate, which serializes refreshes with decisions.
        /// </summary>
        private long _catchUpDenialMintedForTicks;

        /// <summary>
        /// Idempotency key of a GRANT receipt whose OBSERVABLE grant-state
        /// persist was WITHHELD by the receipt-first ordering (SetConsent
        /// returned consent_outbox_persist_failed with the receipt neither
        /// durable nor settled) and is still owed — guarded by
        /// <c>_gate</c>. When that receipt SETTLES (async 2xx, or the
        /// terminal drop of a decision the player made regardless), the
        /// settle path completes the ordered pair by persisting the grant
        /// under the process-wide decision gate — otherwise a
        /// backend-accepted grant would leave the NEXT launch failing
        /// closed against it. Cleared by every newer explicit decision and
        /// by a propagated denial (supersession: the late settle of an old
        /// grant must never overwrite a newer persisted state), and by the
        /// completion itself. <see cref="_withheldGrantStatePersistOwed"/>
        /// carries a completion whose own persist failed, retried at the
        /// consent dispatch points.
        /// </summary>
        private string _withheldGrantStateKey;
        private bool _withheldGrantStatePersistOwed;

        /// <summary>
        /// Durable-marker bookkeeping for the withheld obligation (see
        /// <see cref="ShardPilotConsent.WithheldGrantStorageKeyPrefix"/>):
        /// the entry's original decided_at and minted decision seq (stored
        /// verbatim in the marker — the seq makes the
        /// standing-denial-vs-obligation ordering a FULL pair comparison), whether a durable marker currently
        /// exists for the key, and the receipt-durability PHASE the stored
        /// marker carries. Guarded by <c>_gate</c>; the marker itself is
        /// only ever written/cleared under the process-wide decision gate,
        /// so marker mutations can never interleave with a decision's
        /// supersession-clear.
        /// </summary>
        private string _withheldGrantDecidedAt;
        private ShardPilotConsentNoticeSnapshot _withheldGrantNotice;
        private long _withheldGrantDecisionSeq;
        private bool _withheldGrantMarkerArmed;
        private bool _withheldGrantMarkerReceiptDurable;

        /// <summary>
        /// True while a reloaded withheld-grant marker's resolution is
        /// DEFERRED behind an unreadable outbox record (construction could
        /// not judge retained/superseded/delivered). Guarded by
        /// <c>_gate</c>. While set, the consent HEAD DISPATCH stands down —
        /// the recovered marked receipt must not dispatch (and prune on
        /// ack) before the obligation is re-armed — and the dispatch
        /// points re-run the resolution the moment the heal lands.
        /// </summary>
        private bool _withheldMarkerResolutionPending;

        /// <summary>
        /// True while the construction-time boot denial belt is DEFERRED
        /// behind an unreadable consent-outbox record: the belt's scan needs the retained trail, so
        /// construction could not judge whether a retained denial receipt
        /// outranks the granted restore. Guarded by <c>_gate</c>. While
        /// set, the consent HEAD DISPATCH stands down — a recovered denial
        /// receipt must not deliver (and prune on ack) with the stale
        /// granted state still live — and the dispatch points re-run the
        /// belt the moment the heal lands, before the deferred
        /// withheld-marker resolution (the construction order).
        /// </summary>
        private bool _bootBeltResolutionPending;

        /// <summary>
        /// The idempotency key of the own-scope grant receipt the most
        /// recent propagated/catch-up arm appended to THIS client's trail —
        /// consumed by the deciding flow's post-persist supersede-stamp
        /// pass (<see cref="StampOwnScopeSupersededAfterGlobalPersist"/>)
        /// so the fresh receipt stays unstamped. Guarded by <c>_gate</c>;
        /// arming and consumption are both serialized under the
        /// process-wide decision gate.
        /// </summary>
        private string _lastPropagatedGrantReceiptKey;

        /// <summary>
        /// The consent record as RESTORED at construction — the durable
        /// pair every boot witness (denial marker, belt receipt) is judged
        /// against. Construction-time constant thereafter (in-session
        /// decisions supersede through their own machinery, never by
        /// rewriting this snapshot).
        /// </summary>
        private ShardPilotConsentRecord _restoredConsentRecord =
            ShardPilotConsentRecord.LegacyRecord(ShardPilotConsentState.Unknown);

        /// <summary>
        /// Armed while the LATEST decision is a denial whose respective
        /// durable write is still owed (the record's or write-ahead
        /// marker's), guarded by <c>_gate</c>. The shutdown ladder retries
        /// both. These obligations hold close while the latest denial has
        /// no durable witness (record, marker or qualifying retained receipt),
        /// until the close deadline. Close reports a denial still unwitnessed
        /// at that deadline and keeps it in force for this process. The settle
        /// guard consults these obligations before consuming a retained
        /// denial receipt. Any newer decision recomputes them — a grant
        /// (explicit, propagated, or a completed withheld persist) computes
        /// both false: the witness rule guards the latest decision only.
        /// <see cref="_consentDenialPendingDecidedAt"/> /
        /// <see cref="_consentDenialPendingSeq"/> snapshot that standing
        /// denial's pair for the qualifying-receipt judgment.
        /// </summary>
        private bool _consentDenialRecordPending;
        private bool _consentDenialMarkerPending;
        private string _consentDenialPendingDecidedAt;

        // Fast path for the sync's hot case. The backup key is file-routed, so
        // every dispatch point was paying a File.ReadAllText just to discover
        // there was nothing to retire — for the whole life of every client that
        // never denies. This remembers "absent for THIS actor" from the boot
        // read and from our own retires.
        //
        // A stale TRUE can only make us skip a retire we could have done: the
        // backup then outlives its usefulness until the next launch, which is
        // the fail-CLOSED direction this plane already takes everywhere. It can
        // never skip a WRITE, because an armed obligation leaves the fast path.
        // The BACKUP's own refusal, kept apart from _denialMarkerSlotUnjudgeable. Both hold a granted restore closed, but the
        // slot flag has other consumers — it also gates withheld-grant
        // completions on the SLOT's terms — so one flag was answering two
        // different questions.
        private bool _owedBackupUnjudgeable;

        private bool _owedBackupKnownAbsent;

        // What we last WROTE to the key, so a long-lived owed obligation stops
        // re-reading it at every dispatch point to discover it is unchanged.
        // Same fail-closed asymmetry as the absent flag: a stale belief can only
        // make us skip a rewrite of a value we ourselves put there.
        private string _owedBackupWrittenDecidedAt;
        private long _owedBackupWrittenSeq;
        private long _consentDenialPendingSeq;

        /// <summary>
        /// Armed at construction when the denial-marker slot held
        /// UNJUDGEABLE content (store read refused, or mangled bytes) —
        /// evidence that may witness a denial but can prove neither actor
        /// nor order. While set, a
        /// WITHHELD-GRANT completion is refused (kept owed, never
        /// discharged): the deferred grant persist would reopen the
        /// pipeline under a slot that may hold a newer denial. Cleared by
        /// the boot belt's flip (concrete receipt evidence ends the
        /// transient hold) and by any FRESH explicit decision this
        /// session — a new decision supersedes whatever the slot held, and
        /// its own withheld completion must not wedge behind the hold; otherwise a readable later launch
        /// re-judges the real slot. Guarded by <c>_gate</c>.
        /// </summary>
        private bool _denialMarkerSlotUnjudgeable;

        /// <summary>
        /// True while a withheld-marker DISCHARGE is owed: a decision to
        /// clear the durable marker (identity drop, supersession, an
        /// invalid/unprovable obligation, a completed pair) whose store
        /// write FAILED. Guarded by <c>_gate</c>. The in-memory bookkeeping
        /// is dropped at discharge time regardless — the obligation is
        /// judged dead — but the durable marker survived, and a later
        /// clean rewrite of the outbox record would combine with it into
        /// the false [undecided + clean-empty + durable] delivery proof at
        /// the next launch. While owed: the clear retries at every consent
        /// dispatch point, the outbox record REFUSES durable rewrites (the
        /// empty-proof half must not be minted while the marker half is
        /// condemned-but-standing; marker and record are file-routed to
        /// the same backend, so a store failing one is failing both), the
        /// deferred marker resolution stands down (the on-disk marker is
        /// condemned — re-loading it could complete a discharged
        /// obligation), and teardown defers like any owed write. A fresh
        /// arm (RecordWithheldGrant overwriting the scope marker in place)
        /// supersedes the owed discharge. Residual after a KILL with the
        /// discharge still owed: the record still holds the receipts the
        /// refused rewrite kept, so the next launch re-judges fail-closed
        /// (re-detected identity drop / supersession / malformed) — never
        /// the clean-empty proof.
        /// </summary>
        private bool _withheldMarkerDischargeOwed;

        /// <summary>This client's scope hash (outbox record + withheld-marker key suffix).</summary>
        private readonly string _consentScopeHash;

        /// <summary>
        /// The durable per-scope consent-receipt outbox: an append-only
        /// decision trail retaining every
        /// explicit decision's receipt until the consent endpoint
        /// acknowledges it (or rejects it terminally). Head dispatch keeps
        /// claim-and-handoff atomicity under <see cref="_consentDispatchGate"/>.
        /// ORDERING CONTRACT: on a
        /// strict-enforce workspace an event batch that reaches the server
        /// before the grant's <c>POST /v1/consent</c> receipt row exists is
        /// terminally suppressed per-event (<c>suppressed_no_consent</c>),
        /// so the batch publish path dispatches the pending head receipt to
        /// the HTTP sender BEFORE handing the batch to the transport within
        /// the same flush cycle — sequencing the HANDOFFS only, never the
        /// acknowledgment (ack-gating was deliberately rejected). On top of
        /// that, the grant-receipt dispatch gate holds the event-batch leg (distinct
        /// refusal <c>consent_receipt_pending</c>) while any analytics-GRANT
        /// receipt is retained UNDISPATCHED — parked behind a Retry-After or
        /// backoff window, queued behind another receipt (delivery is
        /// strictly serial FIFO, decision order), or reloaded from disk
        /// after a relaunch. The outbox is PER CLIENT SCOPE — the decider,
        /// every propagated sibling, and any late registrant while the
        /// session grant marker is set records a receipt for its OWN scope.
        /// Never constructed null.
        /// </summary>
        private ShardPilotConsentOutbox _consentOutbox;

        /// <summary>
        /// Serializes the [claim head -> hand receipt to the sender] step so
        /// "claimed but not yet handed off" stays unobservable: a batch
        /// publish (or a chained dispatch) that finds this gate held by a
        /// decision-time dispatch waits that HANDOFF out — a local enqueue
        /// wait, never a network wait — before reading the gate predicate.
        /// Ordering: taken BEFORE <c>_gate</c>, never inside it; re-entrant
        /// on a synchronous sender's ack-chain.
        /// </summary>
        private readonly object _consentDispatchGate = new object();

        /// <summary>
        /// The idempotency key of the receipt currently handed to the HTTP
        /// sender, or null when none is in flight. AT MOST ONE receipt is in
        /// flight (strictly serial delivery in decision order, so a
        /// grant-then-deny can never settle deny-then-grant). Guarded by
        /// <c>_gate</c>.
        /// </summary>
        private string _consentInFlightKey;

        /// <summary>
        /// The consent plane's own deferral deadline — server Retry-After
        /// (honored on 429 AND 5xx) or the jittered backoff below —
        /// INDEPENDENT of the events plane's publish backoff: a denial
        /// clears the publish deferral while receipt retries keep backing
        /// off. Guarded by <c>_gate</c>.
        /// </summary>
        private DateTime _consentRetryNotBefore = DateTime.MinValue;

        // WHEN THE WORKER SHOULD LOOK AGAIN because a retained receipt had no
        // bearer. Deliberately NOT _consentRetryNotBefore: that field also GATES
        // dispatch, so arming it would refuse the next explicit Flush() — a caller
        // asking for progress right now. This timer only shortens the worker's
        // sleep. It never refuses anybody, so it cannot strand a caller, and it
        // cannot spin either: the pacing IS the sleep, and each failed retry arms
        // a longer one through the same ladder.
        private DateTime _consentTokenRetryWakeAt = DateTime.MinValue;

        // WHY the deferral above stands. One paced for an OWED denial witness is
        // cleared the moment the witness stops being owed; a server-driven one
        // (the backoff ladder, a Retry-After) is not — the server is still
        // refusing and only it decides when that ends. Without the distinction
        // the clear would either miss the witness pacing or trample a live
        // server deferral.

        // WHICH denial armed that pacing. The deferral is one field on the
        // client; the retention it paces belongs to a decision. Without this a
        // newer denial whose writes also fail inherits the older one's deadline.

        // The OTHER half of that identity. NextDecisionSeq saturates at
        // MaxConsentDecisionSeq and keeps returning it, so at the ceiling the
        // seq alone stops distinguishing decisions and the timestamp carries the
        // ordering — which is why DenialProvablyOlderThanObligation compares the
        // pair. Comparing the seq alone let a newer denial minted at the ceiling
        // inherit the older one's deadline.

        /// <summary>
        /// Consecutive retryable consent dispatch failures without a server
        /// hint; attempt 1 retries free at the next dispatch point, attempt
        /// n≥2 takes full jitter in [1s, min(2^(n-2), 60)s]. Reset by an
        /// acknowledgment. Guarded by <c>_gate</c>.
        /// </summary>
        private int _consentBackoffAttempt;

        // Uniform [0, 1) jitter source for the consent backoff; drawn under
        // _gate.
        private readonly Random _consentBackoffJitter = new Random();

        /// <summary>
        /// Generation counter for consent-plane Mode-B token invalidations:
        /// advanced when a consent 401 invalidates the cached JWT. A bearer
        /// RESOLVED before the invalidation must never dispatch after it —
        /// without this, a kick whose token fetch raced the 401 settle could
        /// re-send the very token the server just rejected. Dispatch paths
        /// snapshot the epoch before resolving and the head dispatch stands
        /// down on a mismatch (a later dispatch point re-mints fresh).
        /// Guarded by <c>_gate</c>.
        /// </summary>
        private long _consentAuthEpoch;

        // Test-only seam replacing the consent backoff jitter draw. Never
        // assigned in production.
        internal Func<double> ConsentBackoffJitterForTesting;

        /// <summary>
        /// The crash plane's own opt-out — INDEPENDENT of analytics consent
        /// (separate persisted record, separate gate). Crash diagnostics are
        /// a legitimate-interest plane: ON by default with no first-run
        /// decision, opt-out via <see cref="SetCrashReportingEnabled"/>.
        /// While disabled, collection stops too, not just sending: no report
        /// is prepared, nothing is written to the crash spool, breadcrumbs
        /// are neither recorded nor retained, and the spooled backlog is not
        /// re-sent (it ages out under the spool's retention window, which is
        /// still enforced at every construction). Guarded by <c>_gate</c>.
        /// </summary>
        private bool _crashReportingEnabled = true;

        /// <summary>
        /// Why crash reporting is disabled: null while enabled,
        /// <see cref="ShardPilotCrashSettings.OptOutReason"/> for an explicit
        /// opt-out, <see cref="ShardPilotCrashSettings.ReadFailedReason"/>
        /// when the persisted setting could not be read or was malformed
        /// (fail closed). Guarded by <c>_gate</c>.
        /// </summary>
        private string _crashReportingDisabledReason;

        /// <summary>
        /// A SEPARATE crash-plane hold raised when the analytics-consent
        /// record was UNREADABLE at construction: the record may have been a
        /// persisted denial (which suppresses crash capture), so the crash
        /// plane stays closed regardless of the crash setting — including
        /// through a later <see cref="SetCrashReportingEnabled"/>(true),
        /// which only governs the opt-out. Cleared by the next EXPLICIT
        /// consent decision (<see cref="SetConsent"/>, or one propagated
        /// from a live sibling), which persists a fresh readable record and
        /// supersedes whatever the unreadable one held. Guarded by
        /// <c>_gate</c>.
        /// </summary>
        private bool _consentReadFailureCrashHold;

        /// <summary>
        /// Generation counter for crash-plane BLOCKED transitions, the
        /// crash-plane analogue of <c>_consentDenialEpoch</c>: advanced under
        /// <c>_gate</c> every time collection becomes blocked (a local or
        /// propagated opt-out, a refresh-applied disable, or a raised
        /// consent-read-failure hold). A capture prepared outside the locks
        /// snapshots it at entry and compares at every later recheck, so a
        /// disable followed by a quick re-enable still aborts the spanning
        /// capture — the <see cref="CrashCollectionAllowedLocked"/> boolean
        /// alone would read allowed again, letting a report carrying
        /// pre-opt-out breadcrumbs persist and ship after the disable call
        /// already returned. Guarded by <c>_gate</c>.
        /// </summary>
        private long _crashDisableEpoch;

        /// <summary>
        /// The effective crash-plane gate: the opt-out setting AND the
        /// consent-read-failure hold. Every collection/egress path gates on
        /// this, not on the raw flag. Caller must hold <c>_gate</c>.
        /// </summary>
        private bool CrashCollectionAllowedLocked
        {
            get { return _crashReportingEnabled && !_consentReadFailureCrashHold; }
        }

        /// <summary>
        /// Serializes the [final opt-out recheck -> hand the crash request to
        /// the sender] step against <see cref="SetCrashReportingEnabled"/>.
        /// The recheck runs under <c>_gate</c> but the sender call cannot
        /// (no IO under the state lock), which leaves a gap a disable could
        /// land in; disabling therefore acquires THIS gate after flipping
        /// the flag, so by the time the opt-out call returns, any
        /// concurrently dispatching send has already been handed to the
        /// transport and no new dispatch can pass the recheck. Ordering:
        /// taken BEFORE <c>_gate</c> (never inside it); spool locks stay
        /// leaves. Reentrant on the resend pass's synchronous-callback
        /// chain.
        /// </summary>
        private readonly object _crashDispatchGate = new object();

        /// <summary>
        /// The crash dispatch gate, exposed to the AEI reporter so the
        /// dump-class READ rides the same synchronization as every
        /// crash-plane egress: the pass holds this across [current-state
        /// recheck -> tombstone stream read], and a revocation
        /// (<see cref="SetConsent(bool, ShardPilotConsentNotice?)"/> denial or
        /// <see cref="SetCrashReportingEnabled"/>(false)) acquires it after
        /// flipping state — so once the revocation call RETURNS, no
        /// tombstone read can start afterwards; a read already inside the
        /// hold finishes before the revocation returns (its bytes are then
        /// refused at the upload gates). Same-thread reentrancy is the
        /// documented residual, exactly as on the send handoff.
        /// </summary>
        internal object CrashDispatchGateForAeiPass
        {
            get { return _crashDispatchGate; }
        }

        // Number of crash sends handed to the transport but not yet completed.
        // flush/close await this returning to zero (bounded) so a crash captured
        // just before shutdown is not lost while its send is still in flight,
        // mirroring how the analytics batch is drained before flush completes.
        private long _pendingCrashSends;

        // Consecutive retryable event-batch publish failures that carried no
        // server Retry-After hint; drives the full-jitter exponential-backoff
        // fallback in RetryWait. Mutated only on the publishing context (the
        // threaded worker, or the main thread on the WebGL coroutine profile):
        // advanced by RecordPublishFailure, reset by a successful publish and
        // by a consent denial that discards the held batch.
        private int _publishBackoffAttempts;

        // Uniform [0, 1) jitter source for the publish backoff fallback; only
        // the publishing context draws from it.
        private readonly Random _publishBackoffJitter = new Random();

        // Test-only seam replacing the publish backoff jitter draw so schedule
        // tests are deterministic. Never assigned in production.
        internal Func<double> PublishBackoffJitterForTesting;

        // Test-only read of the hint-less failure streak so tests can assert
        // the advance/reset transitions without wall-clock waits.
        internal int PublishBackoffAttemptsForTesting
        {
            get { return _publishBackoffAttempts; }
        }

        // Test-only seam invoked AFTER a crash is serialized but BEFORE the
        // pre-send consent recheck, so a test can deterministically land a
        // SetConsent(false) inside that exact race window. Never assigned in
        // production.
        internal Action CrashPreSendHookForTesting;

        // Test-only seam invoked immediately BEFORE the capture-time crash
        // persist, so a test can deterministically land a SetConsent(false)
        // inside the entry-check-to-persist race window (the persist's own
        // post-append epoch recheck must then remove the just-written
        // record). Never assigned in production.
        internal Action CrashPrePersistHookForTesting;

        // Test-only seam invoked between the backoff re-arm's consent guard
        // and its persisted-marker write, so a test can deterministically
        // land a SetConsent(false) inside that window (the post-write
        // recheck must then undo the marker). Never assigned in production.
        internal Action CrashReArmPreMarkerHookForTesting;

        // Test-only seam invoked after the in-flight count is taken but
        // BEFORE the final home-consent recheck of a handed-off capture, so
        // a test can deterministically land a successor SetConsent(false)
        // inside the snapshot-to-dispatch window. Never assigned in
        // production.
        internal Action CrashPreDispatchHookForTesting;

        // Test-only seam invoked AFTER a spool append's consent/settlement
        // snapshot but BEFORE the unlocked append itself (both in the
        // failed-publish append and in PersistPendingEvents), so a test can
        // deterministically land a SetConsent(false) or a publish settlement
        // inside that exact race window. Never assigned in production.
        internal Action SpoolPreAppendHookForTesting;

        // Runs between SessionEnd's claim (which tears the session down) and the
        // commit that stamps and enqueues app.session_ended, so a test can land
        // a consent loss and a re-grant inside that exact window. Never
        // assigned in production.
        internal Action SessionEndPostClaimHookForTesting;

        // Pauses an owed-start caller before it claims the transition.
        // Never assigned in production.
        internal Action OwedSessionStartBeforeClaimHookForTesting;
        internal Action OwedSessionStartPostClaimHookForTesting;

        /// <summary>
        /// Test-only: invoked at the top of the threaded worker's loop, BEFORE
        /// it observes whether the held batch is empty and before the drain
        /// that would refill it. Lets a test place an event in that window —
        /// the only one in which a fresh partial batch can inherit the
        /// previous batch's deadline. Never assigned in production.
        /// </summary>
        internal Action WorkerPreDrainHookForTesting;

        // Test-only preemption after terminal eligibility, before publication.
        // Runs in their shared gate hold; never assigned in production.
        internal Action CloseTerminalBeforeRecordHookForTesting;

        // Test-only seam invoked by the publish path AFTER it has read the
        // pending consent receipt but BEFORE the receipt's dispatch attempt,
        // so a test can deterministically land a SetConsent(false) inside
        // that window and prove the cancelled grant receipt never leaves.
        // Never assigned in production.
        internal Action ConsentReceiptPreDispatchHookForTesting;

        // Test-only seam invoked by the worker's close path AFTER the shutdown
        // durability check passed but BEFORE the teardown decision, so a
        // test can deterministically land a propagated denial inside that
        // window and prove the atomic teardown re-check defers the close.
        // Never assigned in production.
        internal Action CloseTeardownPreStopHookForTesting;

        public ShardPilotClient(ShardPilotConfig config)
            : this(CaptureHostConstruction(config, true)) { }

        private ShardPilotClient(HostConstruction construction)
            : this(construction, construction.Config,
                new ShardPilotUnityWebRequestTransport(ShardPilotDispatcher.Instance,
                    construction.Config == null || construction.Config.RequestCompressionEnabled),
                true, new ShardPilotUnityKeyValueStore(), new ShardPilotUnityHttpRequestSender(), true)
        {
            CompleteHostConstruction(true);
        }

        /// <summary>
        /// Runs BEFORE the chained constructor above (it is evaluated as its
        /// first argument): resolves the spool file this config maps to and
        /// hands its ownership over — a registered client owning that file
        /// persists its pending remnant and is disowned — so the construction
        /// below LOADS a file that already contains the displaced remnant
        /// instead of one the remnant lands in afterwards (where this
        /// client's next rewrite would drop it). The config-derived
        /// EXPERIMENT CACHE scope (path + tenant identity) is resolved and
        /// handed over even when BOTH spools are disabled: a same-scope
        /// cache-sharing predecessor's automatic experiments lane must stop
        /// before this construction loads the shared assignment cache — a
        /// held revalidation released inside the construction window could
        /// otherwise persist an old assignment this client (or the next
        /// launch) then restores, because the registration-time disown runs
        /// only AFTER the load. Best-effort and side-effect free for the
        /// config, which is returned unchanged; an invalid config skips the
        /// handoff and is rejected by the constructor's own validation as
        /// usual. Internal so the suite can drive the public constructor's
        /// exact pre-construction sequence against directly constructed
        /// clients.
        /// </summary>
        internal static ShardPilotConfig PrepareSpoolHandoff(ShardPilotConfig config, Func<bool> constructionLive = null)
        {
            ConstructionBoundary("entry");
            if (config == null)
            {
                // The chained constructor throws uniformly for null configs.
                return null;
            }

            try
            {
                // Normalize a COPY first: the spool scope hash must be
                // computed from the same trimmed values the construction
                // below will use.
                var normalized = config.CloneWithDefaults();
                string spoolFilePath = null;
                string crashSpoolFilePath = null;
                if (normalized.SpoolEnabled)
                {
                    spoolFilePath = ShardPilotSpool.ResolveFilePath(normalized);

                    // Same enablement condition as CreateCrashSpool: the
                    // handoff must cover exactly the files the construction
                    // below will claim.
                    if (normalized.CrashSpoolEnabled && !string.IsNullOrEmpty(normalized.CrashKey))
                    {
                        crashSpoolFilePath = ShardPilotCrashSpool.ResolveFilePath(normalized);
                    }
                }

                // Spool-independent and flag-independent, exactly like the
                // registration compare: a spool-less (even flag-off)
                // successor still displaces an enabled predecessor over the
                // same cache scope. The FULL identity set rides along —
                // full, sans-base-URL, sans-credentials, plus the dark /
                // token-only flags — so the pre-construction handoff
                // applies the SAME relaxed dark-successor matching the
                // registration compare does: a dark (or token-only dark)
                // direct replacement matched only at registration would
                // leave the predecessor's automatic lane live for the
                // whole construction window.
                var experimentsCachePath = ResolveExperimentsCachePath(normalized);
                if (!string.IsNullOrEmpty(spoolFilePath) || !string.IsNullOrEmpty(crashSpoolFilePath) ||
                    !string.IsNullOrEmpty(experimentsCachePath))
                {
                    var experimentsDark = !normalized.ExperimentsEnabled;
                    ConstructionBoundary("handoff");
                    ShardPilotDispatcher.PrepareOwnershipHandoff(
                        spoolFilePath,
                        crashSpoolFilePath,
                        experimentsCachePath,
                        ExperimentsScopeIdentityFor(normalized),
                        ExperimentsScopeIdentitySansBaseUrlFor(normalized),
                        ExperimentsScopeIdentitySansCredentialsFor(normalized),
                        experimentsDark,
                        experimentsDark && ExperimentsConfigCredentiallessFor(normalized), constructionLive,
                        ShardPilotWithdrawalWork.StoreFor(normalized));
                }
            }
            catch
            {
                // Best-effort: an invalid config is rejected by the
                // constructor itself; a failed handoff only skips the
                // pre-load snapshot.
            }

            return config;
        }

        internal ShardPilotClient(ShardPilotConfig config, IShardPilotTransport transport, bool startWorker)
            : this(config, transport, startWorker, null, null)
        {
        }

        internal ShardPilotClient(
            ShardPilotConfig config,
            IShardPilotTransport transport,
            bool startWorker,
            IShardPilotKeyValueStore settingsStore,
            IShardPilotHttpRequestSender httpSender,
            bool deferDispatchForRegistration = false)
            : this(CaptureHostConstruction(config), config, transport, startWorker, settingsStore, httpSender, deferDispatchForRegistration) { }

        private ShardPilotClient(HostConstruction construction, ShardPilotConfig config,
            IShardPilotTransport transport, bool startWorker, IShardPilotKeyValueStore settingsStore,
            IShardPilotHttpRequestSender httpSender, bool deferDispatchForRegistration)
        {
            _hostConstruction = construction;
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            _consentConfiguredActorInvalid =
                (config.AnonymousId != null && !ShardPilotConsentOutbox.IsValidIdentifier(config.AnonymousId.Trim())) ||
                (!string.IsNullOrWhiteSpace(config.UserId) && !ShardPilotConsentOutbox.IsValidIdentifier(config.UserId.Trim()));
            ConstructionBoundary("config");
            _config = config.CloneWithDefaults();
            _stats.DiagnosticRedactor = text => ShardPilotDiagnostics.Redact(_config, text);
            _transport = transport ?? throw new ArgumentNullException("transport");
            _settingsStore = settingsStore;
            _httpSender = httpSender;
            _crashCallbackHost = ShardPilotDispatcher.Instance;
            ConstructionBoundary("credentials");
            _fetchCallbackHost = ShardPilotDispatcher.Instance;
            if (_config.UsesTokenProvider)
            {
                _tokenManager = new ShardPilotTokenManager(_config.TokenProvider, _config.TokenRefreshLead,
                    ShardPilotDispatcher.Instance, () => !HostRetiring);
            }
            // Identifier acceptance gate: host-supplied identities
            // are limited to 512 UTF-8 BYTES — REJECT, never truncate
            // (truncation could collide distinct identities and
            // mis-attribute events or consent decisions). An out-of-bounds
            // config identity falls back like any other invalid config
            // identity: the user id drops to empty (the actor falls back to
            // the anonymous id), an invalid anonymous id is replaced by the
            // persisted/fresh one below. Receipts persist identifiers
            // verbatim and a failed outbox write never evicts, so an
            // unclamped identifier could wedge the durable write — and with
            // it shutdown — forever.
            if (!string.IsNullOrEmpty(_config.UserId) &&
                !ShardPilotConsentOutbox.IsValidIdentifier(_config.UserId))
            {
                _config.UserId = string.Empty;
                SafeLog("shardpilot config user id rejected: exceeds {0} bytes (invalid_user_id)",
                    ShardPilotConsentOutbox.MaxIdentifierBytes);
            }

            if (!string.IsNullOrEmpty(_config.AnonymousId) &&
                !ShardPilotConsentOutbox.IsValidIdentifier(_config.AnonymousId))
            {
                _config.AnonymousId = string.Empty;
                SafeLog("shardpilot config anonymous id rejected: exceeds {0} bytes (invalid_anonymous_id)",
                    ShardPilotConsentOutbox.MaxIdentifierBytes);
            }

            // Report only an explicitly configured unknown platform, once during
            // construction. Detection defaults stay quiet; fresh wire writers
            // fold the configured value without repeating raw unknown input.
            if (ShardPilotEnvelopePlatform.IsSetButUnmapped(config.Platform))
            {
                SafeLog("shardpilot config platform_unmapped: analytics and new crash reports use other.");
            }

            ConstructionBoundary("identity");
            var consentReadFailed = false;

            // TRUE while the denial-marker slot's occupant held the granted
            // restore fail-closed WITHOUT imposing a definite flavor (store
            // read refused, or mangled content): the belt below still runs
            // against the RESTORED pair — a retained denial receipt is
            // concrete evidence that CAN name an actor, and its flip ends
            // the transient hold.
            var consentDenialSlotFailClosed = false;

            // The backup went UNREADABLE under the decision gate after reading
            // cleanly before it. Distinct from "no candidate": nothing is imposed
            // either way, but blind is not the same as empty, and the belt is told
            // so.
            var backupUnjudgeableUnderGate = false;
            var legacyAnonymousIdRejected = false;
            if (string.IsNullOrEmpty(_config.AnonymousId))
            {
                // The anonymous id exists for EVERY session — with no
                // durable backend it is minted in-memory (LoadOrCreate
                // handles the null store; persistence is best-effort).
                // Under canonical-actor keying the anon id is the receipt
                // actor for every non-vouched decision, so it can never be
                // conditional on durability.
                _config.AnonymousId = ShardPilotAnonymousId.LoadOrCreate(
                    _settingsStore, out legacyAnonymousIdRejected);
            }

            ConstructionBoundary("consent");
            if (_settingsStore != null)
            {

                // An OWED identity-downgrade clear on this backend (a failed
                // Mode-B fail-closed downgrade earlier this process)
                // outranks the stale persisted grant it could not remove.
                // The shadow is SNAPSHOTTED BEFORE the load: read in the
                // other order, a concurrent dispatch-point retry could clear
                // store+shadow between our stale-granted read and the shadow
                // check, and the stale grant would be kept. Snapshot-first
                // is race-free without a gate: [shadow set → treated
                // downgraded regardless of what the retry does next];
                // [shadow clear → either no downgrade was ever owed, or the
                // retry completed and the load below reads the CLEARED
                // record].
                var pendingIdentityDowngrade =
                    ShardPilotConsent.HasPendingDowngrade(_settingsStore, consultDurableMarker: true);
                // The configured actor owns this inherited obligation even if
                // another actor occupies the shared record. Capture it before
                // a healing write can clear the shadow during the durable read.
                ShardPilotConsentState inheritedDenialState;
                string inheritedDenialDecidedAt;
                long inheritedDenialSeq;
                var inheritedDenial = ShardPilotConsent.TryGetSessionDenial(
                    _settingsStore, _config.AnonymousId, out inheritedDenialState,
                    out inheritedDenialDecidedAt, out inheritedDenialSeq);
                var restoredState = ShardPilotConsent.Load(_settingsStore, _config.AnonymousId, out consentReadFailed);
                ApplyConsentState(restoredState == ShardPilotConsentState.DeniedForcedMinor
                    ? restoredState
                    : inheritedDenial ? inheritedDenialState : restoredState);

                // The restored RECORD pair — the durable ordinal every boot
                // witness (marker, belt receipt) is judged against — and the
                // first leg of the seq floor.
                bool restoredRecordReadFailed;
                _restoredConsentRecord = ShardPilotConsent.LoadRecord(
                    _settingsStore, out restoredRecordReadFailed);
                if (!_restoredConsentRecord.Legacy)
                {
                    ShardPilotConsent.RaiseDecisionSeqFloor(_restoredConsentRecord.DecisionSeq);
                }

                // An earlier client in this process tore down with this
                // actor's denial still unwitnessed on this store: Load above
                // already resolved to it; take over its owed record and marker
                // writes so this client's own heal retries carry them.
                if (inheritedDenial)
                {
                    _consentDenialRecordPending = true;
                    _consentDenialMarkerPending = true;
                    _consentDenialPendingDecidedAt = inheritedDenialDecidedAt;
                    _consentDenialPendingSeq = inheritedDenialSeq;
                }

                // DENIAL-MARKER BOOT IMPOSITION. A
                // kill between a denial decision and its record write left
                // the write-ahead marker as the only witness — impose it.
                bool denialMarkerUnreadable;
                var denialMarker = ShardPilotConsent.LoadDenialMarker(
                    _settingsStore, out denialMarkerUnreadable);
                // THE BACKUP IS A CANDIDATE, NOT A SUBSTITUTE FOR AN EMPTY
                // SLOT. Treating it as one lost the
                // denial three ways: a foreign occupant winning a re-read, an
                // occupied slot never consulting it at all, and a stale own
                // marker being judged in place of the newer owed pair. So both
                // lanes are read, both raise the floor, and the AUTHORITATIVE
                // one is chosen by actor and pair before the imposition runs.
                bool owedUnreadable;
                var owed = ShardPilotConsent.LoadOwedDenialWitness(
                    _settingsStore, _config.AnonymousId, out owedUnreadable);

                // Seed the sync's fast path from the read boot already does, so
                // an ordinary client that never denies never touches this key
                // again.
                _owedBackupKnownAbsent = owed == null && !owedUnreadable;

                // The floor rises from ANY valid durable witness — foreign and
                // never-replayed included — exactly as the slot's own occupant
                // does below, or a fresh decision minted after a relaunch could
                // fail to outrank a stored pair.
                if (owed != null && !owed.Legacy)
                {
                    ShardPilotConsent.RaiseDecisionSeqFloor(owed.DecisionSeq);
                }

                // An unreadable BACKUP takes the same transient fail-closed arm
                // the unreadable slot takes. This was refused while the backup
                // lived in the outbox mirror — an unreadable outbox owns that
                // failure through its own recovery, and routing it through the
                // consent hold broke twelve recovery tests. On its own key that
                // objection is gone: nothing else recovers or replays it, so
                // ignoring the refusal would boot GRANTED over what may be the
                // only surviving denial witness.

                // Ours, and better than what the slot holds: no occupant, a
                // FOREIGN occupant (which we may neither judge nor take), or an
                // own occupant the backup's pair supersedes.
                var denialMarkerFromSlot = denialMarker;
                var witnessFromMirror = false;
                if (owed != null &&
                    !ShardPilotConsent.IsForeignDenialMarkerOccupant(owed, _config.AnonymousId) &&
                    (denialMarker == null ||
                     // MANGLED content names neither actor nor pair, so it can
                     // be compared with nothing and would otherwise win by
                     // simply being non-null. A valid own backup is strictly
                     // better evidence; the slot's own
                     // transient hold still applies alongside it.
                     denialMarker.Mangled ||
                     ShardPilotConsent.IsForeignDenialMarkerOccupant(
                         denialMarker, _config.AnonymousId) ||
                     (!denialMarker.Legacy && !owed.Legacy &&
                      ShardPilotConsent.DecisionPairNewer(
                          owed.DecidedAt, owed.DecisionSeq,
                          denialMarker.DecidedAt, denialMarker.DecisionSeq))))
                {
                    // The occupant's seq joins the floor BEFORE it is replaced: the unconditional raise below
                    // only ever sees the winner, and a foreign occupant's
                    // durable pair must still outrank a fresh decision minted
                    // after this relaunch.
                    if (denialMarker != null && !denialMarker.Legacy)
                    {
                        ShardPilotConsent.RaiseDecisionSeqFloor(denialMarker.DecisionSeq);
                    }

                    denialMarker = owed;
                    witnessFromMirror = true;
                }

                // `owed` was read BEFORE the decision gate below, and the
                // imposition acts on it inside. A live same-actor sibling can
                // write a newer backup in between, so the snapshot is
                // re-validated there — for the IMPOSE path as well as the
                // retire.

                if (denialMarker != null && !denialMarker.Mangled)
                {
                    // The marker's seq joins the floor UNCONDITIONALLY — a
                    // FOREIGN marker's included: its actor may return on
                    // this device and this process's decisions must
                    // outrank it.
                    if (!denialMarker.Legacy)
                    {
                        ShardPilotConsent.RaiseDecisionSeqFloor(denialMarker.DecisionSeq);
                    }

                    // Actor scope: the FOREIGN exemption (inert AND kept —
                    // its actor may return; the slot is never laundered by
                    // another actor's session) requires a VALID identifier
                    // for a DIFFERENT actor. A marker with NO actor, or
                    // one whose actor field corrupted into an invalid
                    // identifier, is UNJUDGEABLE and treated as the
                    // current actor's — the fail-closed direction (reading corruption as "foreign"
                    // would launder an own witness into permanent
                    // inertness).
                    var markerIsOwn = denialMarker.AnonymousId == null ||
                                      !ShardPilotConsentOutbox.IsValidIdentifier(
                                          denialMarker.AnonymousId) ||
                                      string.Equals(
                                          denialMarker.AnonymousId, _config.AnonymousId,
                                          StringComparison.Ordinal);
                    if (markerIsOwn)
                    {
                        // Serialized under the DECISION GATE with an
                        // in-gate record RE-READ:
                        // a concurrent live sibling's decision persists
                        // under the same gate, so judging the marker
                        // against a pre-gate snapshot could let a stale
                        // denial convergence clobber a grant that landed
                        // between our load and our write.
                        lock (ShardPilotDispatcher.PrivacyDecisionGate)
                        {
                            bool regateReadFailed;
                            _restoredConsentRecord = ShardPilotConsent.LoadRecord(
                                _settingsStore, out regateReadFailed);
                            if (!_restoredConsentRecord.Legacy)
                            {
                                ShardPilotConsent.RaiseDecisionSeqFloor(
                                    _restoredConsentRecord.DecisionSeq);
                            }

                            if (!regateReadFailed &&
                                _restoredConsentRecord.State != ShardPilotConsentState.Unknown)
                            {
                                // Adopt the re-read state: it is the same
                                // record the pre-gate Load consulted unless
                                // a concurrent decision just landed — then
                                // the newer decision is the truer restore.
                                ApplyConsentState(ShardPilotConsent.Load(
                                    _settingsStore, _config.AnonymousId, out consentReadFailed));
                            }

                            // STALE GUARD (pair compare): a marker whose
                            // pair the record STRICTLY supersedes is
                            // retired, never imposed — the record proves a
                            // newer decision (a later grant included)
                            // landed durably while this marker's
                            // best-effort retirement failed. SAME-ACTOR
                            // only: the record's
                            // decision supersedes this marker only when
                            // its decision-time actor IS the marker's
                            // actor — i.e. the current one, since only an
                            // own marker is judged here. A returning actor
                            // whose foreign-kept denial marker meets a
                            // DIFFERENT actor's newer grant record must
                            // impose, not retire — B's grant never
                            // launders A's denial. An EQUAL pair keeps
                            // imposing — idempotent for the marker's own
                            // persisted denial, fail-closed for ties — and
                            // a legacy pair on either side (or a record
                            // actor that is absent/invalid) keeps the
                            // unconditional fail-closed imposition
                            // (neither side can prove order/scope).
                            var staleAgainstRecord =
                                !denialMarker.Legacy && !_restoredConsentRecord.Legacy &&
                                _restoredConsentRecord.AnonymousId != null &&
                                ShardPilotConsentOutbox.IsValidIdentifier(
                                    _restoredConsentRecord.AnonymousId) &&
                                string.Equals(
                                    _restoredConsentRecord.AnonymousId, _config.AnonymousId,
                                    StringComparison.Ordinal) &&
                                ShardPilotConsent.DecisionPairNewer(
                                    _restoredConsentRecord.DecidedAt,
                                    _restoredConsentRecord.DecisionSeq,
                                    denialMarker.DecidedAt, denialMarker.DecisionSeq);
                            if (staleAgainstRecord)
                            {
                                if (witnessFromMirror)
                                {
                                    // Retire the BACKUP, not the slot: the
                                    // witness came from the key and the slot may
                                    // be holding another actor's. Leaving it
                                    // stored would re-impose this superseded
                                    // denial on every launch, and once a foreign
                                    // grant takes the shared record its own
                                    // staleness can no longer be proved.
                                    //
                                    // RE-READ first: `owed` was taken before
                                    // this gate, so another client may have
                                    // written a newer backup since, and retiring
                                    // on the stale snapshot would delete it.
                                    bool recheckUnreadable;
                                    var current = ShardPilotConsent.LoadOwedDenialWitness(
                                        _settingsStore, _config.AnonymousId, out recheckUnreadable);
                                    if (recheckUnreadable)
                                    {
                                        // Cannot prove the backup stale, so do
                                        // not act as if it were: hold, exactly
                                        // as an unreadable backup does anywhere
                                        // else.
                                        if (ApplyBackupEvidenceLocked(BackupEvidence.Unreadable, null, freshDecision: false))
                                        {
                                            // The belt's own guard was computed
                                            // from the pre-demotion state, so it
                                            // has to be told, or a held granted
                                            // restore silently skips the belt.
                                            consentDenialSlotFailClosed = true;
                                        }
                                    }

                                    if (!recheckUnreadable && current != null)
                                    {
                                        var sameAsJudged =
                                            string.Equals(
                                                current.AnonymousId, owed.AnonymousId,
                                                StringComparison.Ordinal) &&
                                            string.Equals(
                                                current.DecidedAt, owed.DecidedAt,
                                                StringComparison.Ordinal) &&
                                            current.DecisionSeq == owed.DecisionSeq;

                                        if (sameAsJudged)
                                        {
                                            ShardPilotConsent.RetireOwedDenialWitness(
                                                _settingsStore, _config.AnonymousId);
                                        }
                                        else
                                        {
                                            // A sibling replaced it with a
                                            // DIFFERENT denial while we judged.
                                            // Neither retiring nor imposing it
                                            // left the client on the restored
                                            // grant with a durable newer denial
                                            // on disk.
                                            if (!current.Legacy)
                                            {
                                                ShardPilotConsent.RaiseDecisionSeqFloor(
                                                    current.DecisionSeq);
                                            }

                                            ApplyConsentState(current.State);
                                            consentReadFailed = false;
                                            _consentDenialMarkerPending = true;
                                            _consentDenialPendingDecidedAt = current.DecidedAt;
                                            _consentDenialPendingSeq = current.DecisionSeq;
                                        }
                                    }
                                }
                                else
                                {
                                    ShardPilotConsent.RetireDenialMarker(_settingsStore);
                                }
                            }
                            else
                            {
                                // IMPOSE the witnessed denial (flavor
                                // preserved) and CONVERGE: the record write
                                // that carries the marker's own pair (legacy
                                // markers converge to the legacy record shape
                                // — a fabricated pair would outrank real
                                // witnesses). The marker retires only against
                                // durable convergence.
                                if (!witnessFromMirror && owed == null)
                                {
                                    // The pre-gate read found nothing, but a
                                    // same-actor sibling can have recorded one
                                    // between then and this gate — and an
                                    // unregistered constructing client gets no
                                    // propagation.
                                    bool lateUnreadable;
                                    var late = ShardPilotConsent.LoadOwedDenialWitness(
                                        _settingsStore, _config.AnonymousId, out lateUnreadable);
                                    if (!lateUnreadable && late != null)
                                    {
                                        denialMarker = late;
                                        witnessFromMirror = true;
                                    }
                                }

                                if (witnessFromMirror)
                                {
                                    // The snapshot was taken before this gate;
                                    // a live sibling may have written a newer
                                    // backup since. Impose what is THERE.
                                    bool freshUnreadable;
                                    var fresh = ShardPilotConsent.LoadOwedDenialWitness(
                                        _settingsStore, _config.AnonymousId, out freshUnreadable);
                                    if (freshUnreadable)
                                    {
                                        if (ApplyBackupEvidenceLocked(BackupEvidence.Unreadable, null, freshDecision: false))
                                        {
                                            // The belt's own guard was computed
                                            // from the pre-demotion state, so it
                                            // has to be told, or a held granted
                                            // restore silently skips the belt.
                                            consentDenialSlotFailClosed = true;
                                        }

                                        // AND NOTHING IS IMPOSED. The pre-gate
                                        // snapshot is not evidence any more: the
                                        // key was readable then and is refused
                                        // NOW, so it may have been replaced by a
                                        // newer decision or removed outright, and
                                        // imposing the old pair would persist it
                                        // over that and run the destructive wipes
                                        // on unjudgeable evidence — the one thing
                                        // this verdict exists to prevent. The hold armed above
                                        // stands, and a launch that can read the
                                        // key judges it properly.
                                        backupUnjudgeableUnderGate = true;
                                        denialMarker = null;
                                        witnessFromMirror = false;
                                    }

                                    if (!freshUnreadable && fresh == null)
                                    {
                                        // A sibling retired it — after a newer
                                        // GRANT, typically. Imposing the stale
                                        // snapshot here would undo that grant.
                                        denialMarker = null;
                                        witnessFromMirror = false;
                                    }

                                    if (!freshUnreadable && fresh != null)
                                    {
                                        // The in-gate value joins the floor too:
                                        // a sibling can have raised the pair
                                        // since the pre-gate read, and a fresh
                                        // decision must outrank what is actually
                                        // stored.
                                        if (!fresh.Legacy)
                                        {
                                            ShardPilotConsent.RaiseDecisionSeqFloor(fresh.DecisionSeq);
                                        }

                                        denialMarker = fresh;
                                    }
                                }

                                if (denialMarker == null)
                                {
                                    // The candidate vanished under the gate, or the key stopped being
                                    // readable there. Nothing left to
                                    // impose either way, and everything below
                                    // assumes there is — so this launch simply has
                                    // no witness to judge.
                                    //
                                    // The belt signal is cleared only on PROVEN
                                    // absence. Blind is not empty: clearing it
                                    // after an unreadable re-read would hand the
                                    // belt a launch that looks witness-free when
                                    // its granted restore was in fact held.
                                    if (!backupUnjudgeableUnderGate)
                                    {
                                        consentDenialSlotFailClosed = false;
                                    }
                                }
                                else
                                {
                                ApplyConsentState(denialMarker.State);
                                consentReadFailed = false;
                                var converged = denialMarker.Legacy
                                    ? PersistConsentState(denialMarker.State)
                                    : PersistConsentState(
                                        denialMarker.State, denialMarker.DecidedAt,
                                        denialMarker.DecisionSeq);
                                if (converged && !witnessFromMirror)
                                {
                                    ShardPilotConsent.RetireDenialMarker(_settingsStore);

                                    // The older backup this pair supersedes is
                                    // NOT retired here. Doing so needs the same
                                    // in-gate re-validation the other retire
                                    // paths have, and adding a third copy of
                                    // that rule to save one dispatch point is
                                    // the wrong trade — the dispatch-point sync
                                    // already retires it, re-reading first.
                                }
                                else if (witnessFromMirror)
                                {
                                    if (!converged)
                                    {
                                        // The recovery write failed too. Fail-
                                        // closed either way, but the metric must
                                        // still see it or an ongoing storage
                                        // outage reads as quiet.
                                        _stats.RecordConsentPersistFailed();
                                    }

                                    // The marker LANE is what the backup exists
                                    // to rebuild, so arm the obligation instead
                                    // of retiring anything: the dispatch-point
                                    // heal writes it through the same rule that
                                    // spares a foreign occupant, and the mirror
                                    // keeps the backup until it lands.
                                    _consentDenialMarkerPending = true;
                                    _consentDenialPendingDecidedAt = denialMarker.DecidedAt;
                                    _consentDenialPendingSeq = denialMarker.DecisionSeq;
                                }
                                else
                                {
                                    _stats.RecordConsentPersistFailed();
                                }

                                SafeLog(
                                    "shardpilot consent: a durable denial-marker witness imposed the " +
                                    "denial at load{0}",
                                    converged
                                        ? " and converged to the record"
                                        : "; convergence stays owed");
                                }
                            }
                        }
                    }
                }
                else if (denialMarkerUnreadable || denialMarker != null)
                {
                    // The slot REFUSED the read, or holds MANGLED content
                    // that cannot even name a denied state: either may
                    // still witness a denial. The session-long flag
                    // refuses WITHHELD-GRANT completions regardless of the
                    // restored state (a deferred grant persist must not
                    // reopen the pipeline under a slot that may hold a
                    // newer denial); a GRANTED restore additionally fails
                    // closed TRANSIENTLY — in memory only, no record
                    // rewrite, no retirement: a readable (or healed) later
                    // launch judges the real slot. Non-granted restores
                    // are already fail-closed and stand. Unknown rather
                    // than a denied flavor: unity's denied boot branch
                    // runs the DESTRUCTIVE denial wipes (spool, crash
                    // spool, era snapshots), which unjudgeable evidence
                    // must not trigger — Unknown closes egress identically
                    // under consent-first while preserving the data for
                    // the launch that can actually judge the slot.
                    _denialMarkerSlotUnjudgeable = true;
                    if (_consentState == ShardPilotConsentState.Granted)
                    {
                        consentDenialSlotFailClosed = true;
                        ApplyConsentState(ShardPilotConsentState.Unknown);
                        SafeLog(
                            denialMarkerUnreadable
                                ? "shardpilot consent: the denial-marker slot is unreadable; the granted " +
                                  "restore is held fail-closed (Unknown) for this session"
                                : "shardpilot consent: the denial-marker slot holds mangled content; the " +
                                  "granted restore is held fail-closed (Unknown) for this session");
                    }
                }

                // The BACKUP's own read refusal, applied INDEPENDENTLY of the
                // slot. Folding it into
                // denialMarkerUnreadable above only worked while the slot was
                // empty or mangled: any valid occupant takes the branch before
                // this one, so a foreign marker plus a granted record plus an
                // unreadable backup booted GRANTED over what may be this
                // actor's only denial witness. Same transient hold, same
                // reasoning, its own condition.
                // The SLOT's refusal counts here too when the backup won the
                // selection: the branch that normally applies it is the one the
                // occupant takes, and choosing `owed` skips it. An unreadable
                // slot may hold a denial NEWER than the backup, and without the
                // hold a withheld grant older than that denial could pass the
                // ordering check and reopen analytics over it.
                // MANGLED slot content counts with an unreadable one here: both
                // are unjudgeable, and choosing the backup over either skips the
                // branch that would otherwise apply the slot's own transient
                // hold (the same gap an earlier fix closed
                // for the unreadable case).
                var slotUnjudgeableUnderSelection =
                    denialMarkerUnreadable ||
                    (denialMarkerFromSlot != null && denialMarkerFromSlot.Mangled);

                if (owedUnreadable || (slotUnjudgeableUnderSelection && witnessFromMirror))
                {
                    if (owedUnreadable)
                    {
                        _owedBackupUnjudgeable = true;

                        // The CRASH plane is held on the same evidence. It reads
                        // its own setting, not the analytics state, so holding
                        // consent alone left crash bytes leaving the device over
                        // a backup that may be this actor's only denial witness.
                        _consentReadFailureCrashHold = true;
                    }

                    if (slotUnjudgeableUnderSelection)
                    {
                        // The SAME condition the outer branch entered on: an
                        // earlier fix widened that one to include mangled content and
                        // left this one testing only the refused read, so a
                        // mangled slot entered the block and armed nothing.
                        _denialMarkerSlotUnjudgeable = true;
                    }

                    if (_consentState == ShardPilotConsentState.Granted)
                    {
                        consentDenialSlotFailClosed = true;
                        ApplyConsentState(ShardPilotConsentState.Unknown);
                        SafeLog(
                            "shardpilot consent: the owed denial-witness backup is unreadable; the " +
                            "granted restore is held fail-closed (Unknown) for this session");
                    }
                }

                if (pendingIdentityDowngrade && _consentState == ShardPilotConsentState.Granted)
                {
                    ApplyConsentState(ShardPilotConsentState.Unknown);

                    // The shadow may be DURABLE-marker-only (a PREVIOUS
                    // process recorded the downgrade but was killed before
                    // its clear retry landed — the marker is what survives
                    // that kill): re-record it so the in-process set backs
                    // it for this session's gated re-checks and
                    // dispatch-point retries.
                    ShardPilotConsent.RecordPendingDowngrade(_settingsStore);

                    // The clear retry serializes under the decision gate
                    // with an in-gate re-check: without it, this clear could
                    // land right after a concurrent explicit decision's Save
                    // (which supersedes the shadow) and wipe the FRESH
                    // record.
                    lock (ShardPilotDispatcher.PrivacyDecisionGate)
                    {
                        if (ShardPilotConsent.HasPendingDowngrade(_settingsStore) &&
                            ShardPilotConsent.Clear(_settingsStore))
                        {
                            ShardPilotConsent.ClearPendingDowngrade(_settingsStore);
                        }
                    }
                }
            }

            // The crash plane's own persisted opt-out, resolved independently
            // of analytics consent: absent = the legitimate-interest default
            // (enabled); an explicit opt-out, a read failure, or a malformed
            // record disables the plane — the latter two fail CLOSED.
            ConstructionBoundary("crash-state");
            _crashReportingEnabled = ShardPilotCrashSettings.Load(_settingsStore, out _crashReportingDisabledReason);

            if (!_crashReportingEnabled &&
                _crashReportingDisabledReason == ShardPilotCrashSettings.OptOutReason)
            {
                // PERSISTED-OPT-OUT REPLAY, mirroring the persisted-denial
                // construction branch below: a process killed after the
                // opt-out's TrySave(false) durably landed but before the
                // live path's era wipe + suppression-floor stamp relaunches
                // HERE with the revocation durable and its cleanup never
                // run — and the ANR lane may stay dark for the whole
                // opted-out relaunch, so no AEI pass exists to floor the
                // cursor before a later re-enable lets opt-out-window
                // records resurface (possibly attributing through the stale
                // era file). Replay both. The setting is GLOBAL (one
                // persisted key, ShardPilotCrashSettings.StorageKey), so
                // the wipe is the same all-scope sweep the live opt-out
                // runs — the sweep stamps each swept scope's floor — plus
                // this scope's own floor for the era-file-less case. A
                // read-failure or not-configured disable deliberately does
                // NOT replay: nothing may be destroyed while the stored
                // decision is unknowable, matching the AEI pass's hold. A
                // re-run on every opted-out launch is idempotent and merely
                // extends the disclaimed window the standing opt-out
                // already implies.
                ShardPilotAnrEraStore.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message));
                ShardPilotAeiCursor.RecordSuppressionFloor(
                    _settingsStore, _config, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            }

            _queue = new Queue<ShardPilotEvent>(_config.BufferSize);
            _workerBatch = new List<ShardPilotEvent>(_config.BatchSize);
            _batchEndpoint = _config.IngestUrl.TrimEnd('/') + "/v1/events:batch";
            _consentEndpoint = _config.IngestUrl.TrimEnd('/') + "/v1/consent";
            // A SEPARATE crash ingest endpoint from the analytics
            // batch/consent endpoints, on the crash base URL with the
            // crash:write key. The crash URL never falls back to the ingest URL.
            _crashEndpoint = _config.CrashUrl.TrimEnd('/') + "/api/v1/crashes/ingest";
            // The tombstone lane's multipart sibling,
            // same crash base + crash:write Bearer.
            _tombstoneEndpoint = _config.CrashUrl.TrimEnd('/') + ShardPilotTombstoneUpload.IngestPath;

            // Durable offline spool: undeliverable events are persisted to
            // disk and re-sent through the normal publish path on the next
            // launch. The worker has not started yet, so no lock is needed.
            ConstructionBoundary("storage");
            _spool = CreateSpool();

            // Durable crash spool: reports persisted by an earlier launch
            // (captured in a dying process, or their send failed) are loaded
            // here and re-sent through the crash re-send pass below.
            _crashSpool = CreateCrashSpool();

            // Experiment-assignment consumer — dark
            // unless ExperimentsEnabled. Constructed only when the flag is
            // on: while off there is no subject-id read or mint, no fetch,
            // no revalidation, no exposure and no new persistence keys. The
            // subject id is SDK-managed (loaded from its own persisted key,
            // never from config — no host override path exists) and minted
            // lazily by the consumer at first need. Experiment facts ride
            // the normal analytics pipeline through EnqueueSealedExperimentFact
            // with the facts contract's envelope rules (no user_id; the
            // standard anonymous_id — never the experiments subject id).
            // The subject-id and consent delegates are lock-free field
            // reads by the consumer's threading contract; the store/emit
            // delegates take this client's locks and are never called under
            // the consumer's own lock.
            _consentScopeHash = ShardPilotConsentOutbox.ScopeHash(
                _config.WorkspaceKey, _config.AppKey, _config.EnvironmentKey);

            ConstructionBoundary("experiments");
            if (_config.ExperimentsEnabled)
            {
                _experimentsSubjectId = ShardPilotSubjectId.Load(_settingsStore);
                _experiments = new ShardPilotExperiments(
                    _config,
                    ResolveExperimentsCachePath(),
                    _httpSender,
                    () => _experimentsSubjectId,
                    value =>
                    {
                        _experimentsSubjectId = value;
                        return ShardPilotSubjectId.Store(_settingsStore, value);
                    },
                    () =>
                    {
                        // Mint-decision convergence: adopt a subject a
                        // sibling client persisted since construction.
                        var persisted = ShardPilotSubjectId.Load(_settingsStore);
                        if (persisted != null)
                        {
                            _experimentsSubjectId = persisted;
                        }

                        return persisted;
                    },
                    () => _consentState,
                    CurrentExperimentSessionId,
                    () => _config.AnonymousId,
                    code => SafeLog("shardpilot experiments: {0}", code),
                    PurgeCondemnedExperimentFacts,

                    // Lock-free like the consent-state read above: the
                    // consumer captures this at dispatch intent and
                    // re-compares before mint/dispatch and at settle, so a
                    // denial (even one a re-grant papered over) resolves
                    // in-flight fetches closed.
                    () => Interlocked.Read(ref _consentDenialEpoch),

                    // The shared egress gate: the consumer's final pre-send
                    // epoch re-read + transport handoff and this client's
                    // denial-epoch advances serialize on it, so a denial
                    // can never land between the consumer's last check and
                    // the bytes leaving the device.
                    _experimentEgressGate,
                    ResolveSubjectIdStoreIdentity(),
                    AdmitExperimentApplicationSession,
                    WithdrawExperimentKeyFacts,
                    EnqueueSealedExperimentFact, () => FetchOwnerCurrent, ConvergeWithdrawal);
            }

            // Durable consent-receipt outbox: receipts an earlier launch
            // could not deliver load here (sanitize-on-load, fail-safe) and
            // re-send at the dispatch points — the outbox is consent-plane
            // only and NEVER consent-purged: delivery is permitted, and
            // required, while analytics consent is denied or unknown,
            // because a receipt documents the decision itself. A fresh
            // consent-first install stays fully dark: an empty outbox
            // dispatches nothing and no token is minted for it. (The scope
            // hash itself is computed earlier, before the experiments
            // consumer.)
            ConstructionBoundary("outbox");
            _consentOutbox = new ShardPilotConsentOutbox(_settingsStore, _config);

            // The grant-receipt gate's cross-client delegation judges parked-ness with
            // the OWNER's predicate (vouching inputs are construction-time
            // constants per client), exactly like the in-flight key
            // write-through.
            _consentOutbox.ParkedPredicateForGate = IsConsentReceiptParked;

            // Monotonic mint floor, RECEIPT leg (the
            // record and marker legs raised theirs above): a fresh decision
            // must outrank every retained (stamp, seq) pair even when the
            // record write failed — the retained receipt then carries the
            // highest seq this install has minted. Runs BEFORE the Mode-B
            // identity drop below: a dropped receipt's pair was still
            // minted on this install, and a floor kept high is always the
            // safe direction (a floor kept low risks pair collisions).
            foreach (var retained in _consentOutbox.SnapshotEntries())
            {
                ShardPilotConsent.RaiseDecisionSeqFloor(retained.DecisionSeq);
            }

            // BELT against a stale granted record: the
            // write-ahead marker above is the primary witness for a denial
            // whose record write failed, but a RETAINED (undelivered)
            // denial receipt is durable evidence too — scoped, stamped,
            // and already loaded. A denial-carrying receipt for THIS actor
            // whose (decided_at, decision_seq) pair is STRICTLY newer than
            // the restored record's decision proves the granted restore
            // stale (the denial's record write never landed and its marker
            // is gone or was never written): boot the receipt's denial
            // instead and converge the record best-effort; the receipt
            // itself stays retained and delivers under its own rules. The
            // strictness keeps a genuinely newer grant in charge — a
            // same-pair or older denial receipt (a deny-then-grant whose
            // denial receipt is still undelivered) never flips it. A
            // LEGACY granted record with no stamp cannot prove it
            // postdates the receipt, so it fails closed to the denial;
            // receipts without a decision-time anon snapshot or a
            // plausible stamp are skipped — they can prove neither actor
            // scope nor order.
            //
            // The belt also runs UNDER the transient fail-closed hold of
            // an unreadable/mangled marker slot whose RESTORED state was
            // granted: the transient arm imposes on
            // evidence that cannot name an actor, but a retained denial
            // receipt is CONCRETE evidence that can — skipping the belt
            // there would let the receipt deliver and prune with the
            // record never converged and no witness written, so a healed
            // slot at a later launch would resurrect the stale grant. The
            // comparison base is the RESTORED record pair either way, and
            // a flip ENDS the transient hold: the belt's denial is the
            // real state to persist from here on.
            var beltGuardsGrantedRestore =
                _consentState == ShardPilotConsentState.Granted ||
                (consentDenialSlotFailClosed &&
                 _restoredConsentRecord.State == ShardPilotConsentState.Granted);
            if (beltGuardsGrantedRestore && _consentOutbox.LoadFailed)
            {
                // DEFERRED belt: the trail is
                // UNREADABLE, so the scan below would judge an EMPTY
                // snapshot — a retained denial receipt that outranks the
                // granted restore would go unseen, and once the heal merges
                // it back the dispatch could deliver and prune the denial's
                // only durable evidence with the state still Granted and
                // the record never converged. Arm the deferral instead: the
                // head dispatch stands down while it is pending (exactly
                // like the deferred withheld-marker resolution), and the
                // dispatch-point retry re-runs the belt the moment the heal
                // lands — before the recovered head may dispatch.
                lock (_gate)
                {
                    _bootBeltResolutionPending = true;
                }
            }
            else if (beltGuardsGrantedRestore)
            {
                // Serialized under the DECISION GATE with an in-gate
                // record RE-READ, exactly like the marker imposition: the scan-to-converge window
                // must not let a concurrent sibling decision land between
                // judging the receipts and writing the belt's denial.
                lock (ShardPilotDispatcher.PrivacyDecisionGate)
                {
                    bool beltReadFailed;
                    var beltBase = ShardPilotConsent.LoadRecord(_settingsStore, out beltReadFailed);
                    if (!beltBase.Legacy)
                    {
                        ShardPilotConsent.RaiseDecisionSeqFloor(beltBase.DecisionSeq);
                    }

                    // A record that changed under the gate (a concurrent
                    // decision landed) or refuses to read no longer
                    // matches the restore this belt was armed for. Under
                    // the transient hold the base stays the RESTORED pair
                    // — but only while the durable record still IS that
                    // restored record: a concurrent decision landing in
                    // the load-to-gate window postdates every retained
                    // receipt, so the belt stands down rather than judge
                    // stale candidates against a stale base. Outside the hold the freshest
                    // record is the base and a non-granted one stands the
                    // belt down.
                    if (consentDenialSlotFailClosed)
                    {
                        var baseChanged = beltReadFailed ||
                                          beltBase.State != _restoredConsentRecord.State ||
                                          beltBase.Legacy != _restoredConsentRecord.Legacy ||
                                          !string.Equals(
                                              beltBase.DecidedAt, _restoredConsentRecord.DecidedAt,
                                              StringComparison.Ordinal) ||
                                          beltBase.DecisionSeq != _restoredConsentRecord.DecisionSeq;
                        if (baseChanged)
                        {
                            beltGuardsGrantedRestore = false;
                        }
                    }
                    else
                    {
                        _restoredConsentRecord = beltBase;
                        if (beltReadFailed ||
                            beltBase.State != ShardPilotConsentState.Granted)
                        {
                            var beltReload = ShardPilotConsent.Load(
                                _settingsStore, _config.AnonymousId, out consentReadFailed);
                            if (!ConsentEvidenceUnjudgeableLocked() ||
                                ShardPilotConsent.IsDeniedState(beltReload))
                            {
                                ApplyConsentState(beltReload);
                            }

                            beltGuardsGrantedRestore = false;
                        }
                    }

                    ShardPilotConsentOutboxEntry staleDenial = null;
                    if (beltGuardsGrantedRestore)
                    {
                        staleDenial = FindBeltStaleDenialAgainstRestoredRecord();
                    }

                    if (staleDenial != null)
                    {
                        ApplyConsentState(string.Equals(
                            staleDenial.Reason, ShardPilotConsent.ForcedMinorReason,
                            StringComparison.Ordinal)
                            ? ShardPilotConsentState.DeniedForcedMinor
                            : ShardPilotConsentState.Denied);

                        // Concrete receipt evidence supersedes the transient
                        // unreadable/mangled hold: persists from here carry the
                        // belt's denial.
                        consentDenialSlotFailClosed = false;
                        _denialMarkerSlotUnjudgeable = false;

                        // The BACKUP hold is NOT cleared here. The belt's receipt proves ordering against
                        // the restored RECORD, not against a backup nobody could
                        // read — which may hold a denial newer than the receipt.
                        // Clearing it would let a grant ordered after the receipt
                        // but before that hidden denial reopen egress. Only this
                        // actor's own fresh decision lifts it.
                        var converged = PersistConsentState(
                            _consentState, staleDenial.DecidedAt, staleDenial.DecisionSeq);
                        var beltWitnessDurable = false;
                        if (converged)
                        {
                            // The record durably holds the belt's denial:
                            // retire the marker slot on the same scoped rule as
                            // a persisted decision (an own-actor, actor-less,
                            // mangled, or unreadable occupant goes — it either
                            // witnessed an older own decision or can name no
                            // actor at all against a now-durable one; a valid
                            // FOREIGN witness stays).
                            ShardPilotConsent.RetireDenialMarkerScoped(
                                _settingsStore, _config.AnonymousId);
                        }
                        else
                        {
                            _stats.RecordConsentPersistFailed();

                            // WITNESS PRESERVATION: this
                            // retained receipt is the denial's ONLY durable
                            // proof, and it is about to dispatch — a successful
                            // delivery removes it from the outbox, and with the
                            // convergence write failed the next launch would
                            // restore the stale grant with no witness left
                            // anywhere. Write the write-ahead marker as the
                            // receipt-independent witness (actor, flavor, and
                            // the receipt's own decision pair), and arm the
                            // same pending flags a live denial's failed writes
                            // arm: the shutdown ladder refuses to finalize
                            // while the denial has no durable witness, and the
                            // settle guard retains the receipt over a still
                            // open gap.
                            beltWitnessDurable = ShardPilotConsent.RecordDenialMarker(
                                _settingsStore, _consentState, _config.AnonymousId,
                                staleDenial.DecidedAt, staleDenial.DecisionSeq);
                            lock (_gate)
                            {
                                _consentDenialRecordPending = true;
                                _consentDenialMarkerPending = !beltWitnessDurable;
                                _consentDenialPendingDecidedAt = staleDenial.DecidedAt;
                                _consentDenialPendingSeq = staleDenial.DecisionSeq;
                            }
                        }

                        SafeLog(
                            "shardpilot consent: a retained denial receipt outranks the restored " +
                            "granted record; booting denied{0}",
                            converged
                                ? " (record converged)"
                                : beltWitnessDurable
                                    ? " (convergence failed; the marker slot holds the witness)"
                                    : " (convergence and marker write both failed; witness writes retry " +
                                      "at the settle guard and the shutdown ladder)");
                    }
                }
            }

            List<ShardPilotConsentOutboxEntry> identityDropped = null;
            var parkedPendingGrant = false;
            var downgradeTriggeringGrant = false;
            if (_config.UsesTokenProvider && string.IsNullOrEmpty(_config.PublishableKey))
            {
                // Mode-B-ONLY direct-actor identity rule: tokens are
                // minted bound to the CURRENT identity, so an ANON-kinded
                // retained receipt whose actor is not the current anonymous
                // id has no credential that could ever lawfully carry it —
                // it drops fail-safe at load (identity_changed).
                // VERIFIED-kinded receipts never drop: they PARK under
                // IsConsentReceiptParked until a session vouches their
                // actor again. With a publishable PublishableKey configured (Mode A
                // or dual mode) NOTHING drops — anon receipts ride the key,
                // which has no identity binding, so the historic actor is
                // the correct subject of the historic decision. Remembered
                // by the outbox so a record recovered by a LATE re-read
                // (after a construction-time load failure) gets the same
                // identity drop this construction applies below.
                _consentOutbox.ModeBIdentityFilter =
                    new ShardPilotModeBIdentity(
                        FirstNonEmpty(_config.UserId, _config.AnonymousId), _config.AnonymousId);

                identityDropped =
                    _consentOutbox.DropIdentityMismatched(
                        FirstNonEmpty(_config.UserId, _config.AnonymousId), _config.AnonymousId);
                var droppedPendingGrant = false;
                for (var i = 0; i < identityDropped.Count; i++)
                {
                    droppedPendingGrant |= identityDropped[i].AnalyticsGranted;
                    SafeLog(
                        "shardpilot consent receipt dropped (scope=consent, code=identity_changed): " +
                        "the stored anon-kinded actor no longer matches the current anonymous id " +
                        "and no publishable key exists to carry it");
                }

                downgradeTriggeringGrant = droppedPendingGrant;
            }

            // A PARKED pending grant downgrades the persisted grant too:
            // parking retains the receipt durably (it may
            // be the only record of that actor's decision and delivers when
            // a later launch vouches the actor again), but THIS session
            // cannot deliver it, so a persisted grant backed only by it
            // would ship every batch receipt-less exactly like the dropped
            // case. Receipt retention and state downgrade compose: the
            // receipt survives for the actor who made the decision; the
            // session's operative state fails closed to fresh-consent.
            // Mode-independent: a verified receipt parks in Mode A (no
            // provider) and in a different-user Mode-B/dual session alike.
            foreach (var retained in _consentOutbox.OwnScope.SnapshotEntries())
            {
                // A SUPERSEDED parked grant is not the persisted state's
                // backer — a newer explicit decision stamped it when it
                // applied (see MarkRetainedAnalyticsGrantsSuperseded) — so
                // it must not downgrade the state that newer decision
                // persisted (without the stamp, a
                // parked historical grant re-downgraded a valid fresh
                // grant on every launch).
                if (retained.AnalyticsGranted && !retained.Superseded &&
                    IsConsentReceiptParked(retained))
                {
                    parkedPendingGrant = true;
                    break;
                }
            }

            {
                // Identity BINDING, failed closed: when the identity change
                // DROPPED a pending grant receipt (Mode-B-only anon
                // mismatch) or the construction PARKED one (a verified
                // grant this session cannot vouch), the persisted grant it
                // documented cannot reach the backend from THIS session —
                // leaving it in force would ship every batch receipt-less
                // (strict workspaces suppress them; the audit trail has no
                // grant row for this actor). The persisted grant is
                // therefore DOWNGRADED to the absent/Unknown state:
                // HOST-VISIBLE CONSEQUENCE — after an identity change,
                // consent must be asked and recorded again (consent-first
                // keeps the pipeline closed until then), which is exactly
                // the fresh-consent posture an identity change warrants.
                // Dropped/parked DENY receipts downgrade nothing: a
                // persisted denial is already maximally fail-closed. Runs
                // BEFORE the consent-first spool branch below, so the
                // downgrade also routes this construction through the
                // standard non-granted spool purge.
                if ((downgradeTriggeringGrant || parkedPendingGrant) &&
                    _consentState == ShardPilotConsentState.Granted)
                {
                    // Serialized under the decision gate: the flip, the
                    // durable clear, and the LIVE-SIBLING propagation form
                    // one downgrade that no concurrent explicit decision or
                    // registration refresh may interleave.
                    lock (ShardPilotDispatcher.PrivacyDecisionGate)
                    {
                        ApplyConsentState(ShardPilotConsentState.Unknown);
                        if (!ShardPilotConsent.Clear(_settingsStore))
                        {
                            // The durable clear failed: without a shadow the
                            // stale "granted" record would be re-read by the
                            // registration refresh (or a successor
                            // construction) and re-applied, resurrecting the
                            // receipt-less grant this downgrade exists to
                            // prevent. The backend-keyed in-process shadow
                            // keeps every consult of this store fail-closed
                            // until the clear lands (retried at the consent
                            // dispatch points) or a fresh explicit decision
                            // supersedes it.
                            ShardPilotConsent.RecordPendingDowngrade(_settingsStore);
                            SafeLog(
                                "shardpilot consent: persisted grant downgrade could not be written; " +
                                "the in-process shadow keeps this backend gated and the clear retries " +
                                "at the dispatch points (fresh consent required)");
                        }

                        // Live registered siblings loaded the grant earlier
                        // and the refresh applies nothing for a persisted
                        // Unknown — without propagation they would keep
                        // publishing under the unbacked grant. Mirrors the
                        // denial propagation's shape and fail-closed queue
                        // handling, but mints NO receipts: a downgrade to
                        // Unknown is not a player decision.
                        ShardPilotDispatcher.PropagateIdentityGrantDowngrade(this);
                    }

                    if (_experiments != null)
                    {
                        // The downgrade is a purge point for THIS client
                        // too, exactly as ApplyIdentityGrantDowngrade makes
                        // it for live siblings: the consumer was constructed
                        // — and its durable owed exposures RESTORED — above,
                        // BEFORE this downgrade, so restored captures still
                        // carry the OLD identity's arm-time anonymous id.
                        // Left alone they would sit out the non-granted
                        // window and drain after the host records a fresh
                        // grant for the NEW identity, publishing
                        // pre-downgrade facts under the new token/consent
                        // lineage. The purge kills them (in memory and on
                        // disk, with the scrub-debt fail-safe) while live
                        // entries re-arm fresh — their re-emits capture the
                        // post-downgrade identity.
                        _experiments.OnAnalyticsPurge();
                    }

                    SafeLog(
                        "shardpilot consent: the identity change {0} a pending grant receipt; " +
                        "the persisted grant is downgraded — a fresh explicit SetConsent is required " +
                        "for the new identity",
                        downgradeTriggeringGrant ? "dropped" : "parked");
                }
            }

            // A SCOPE-KEYED pending downgrade from a refused or held
            // propagated/catch-up grant: the GLOBAL granted record stands, but
            // this scope could not safely accept it, so opening this
            // scope's pipeline from the shared record would publish
            // receipt-less. Hold this scope's OWN state Unknown — the
            // shared key is untouched (other scopes' receipts back it) and
            // the marker stands until a later successful own-scope receipt
            // arm discharges it. Runs BEFORE the consent-first spool
            // branches so the non-granted purge posture applies.
            if (_consentState == ShardPilotConsentState.Granted &&
                ShardPilotConsent.HasScopedPendingDowngrade(_settingsStore, _consentScopeHash))
            {
                ApplyConsentState(ShardPilotConsentState.Unknown);
                SafeLog(
                    "shardpilot consent: this scope could not safely accept an earlier shared " +
                    "grant; the scope stays gated " +
                    "(Unknown) until a fresh decision retains its receipt — the shared granted " +
                    "record is untouched");
            }

            // A previous launch's WITHHELD-GRANT obligation (the observable
            // grant persist deferred behind receipt-first ordering, and the
            // process ended before the settle completed it) reloads here —
            // BEFORE the consent-first spool branches below, so a completed
            // grant takes the granted path. Runs after the Mode-B identity
            // drop: an identity-dropped receipt voids the obligation.
            ConstructionBoundary("privacy-markers");
            ProcessWithheldGrantMarkerAtConstruction(identityDropped, consentReadFailed);

            if (ShardPilotConsent.IsDeniedState(_consentState))
            {
                if (_spool != null)
                {
                    // The persisted consent decision is a denial: destroy any
                    // spooled events without sending them.
                    _spool.Wipe();
                }

                if (_crashSpool != null)
                {
                    // Crash reports carry the same consent subject
                    // (anonymous_id, breadcrumbs): the denial destroys them too.
                    _crashSpool.Wipe();
                }

                // The consent decision is GLOBAL while spool files are
                // per-scope: sweep every scope's file in the active directory
                // (idempotent for this scope's own file), fail-closed per
                // file. Files under a DIFFERENT custom SpoolDir from an
                // earlier run are out of reach — keep SpoolDir stable.
                // The crash spool sweep mirrors the event sweep with its own
                // markers; the deniable Retry-After marker goes with it. The
                // ANR era sweep re-runs here for the same kill-recovery
                // reason the spool sweeps do: a process that died between
                // the live denial's persist and ITS era sweep relaunches
                // into this branch, and the sibling scopes' subject-bearing
                // snapshots must still be destroyed.
                QuarantineSpoolIfSweepOwed(ShardPilotSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));
                QuarantineCrashSpoolIfSweepOwed(ShardPilotCrashSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));
                ShardPilotAnrEraStore.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message));

                // The AEI suppression floor re-stamps here for the same
                // kill-recovery reason the sweeps re-run: a process killed
                // between the live denial's PersistConsentState and ITS
                // RecordSuppressionFloor relaunches into this branch with
                // the denial durable but the floor never stamped — a later
                // re-grant plus a flag-on launch would then report ANR
                // records Android retained from the denied window. Same
                // marker semantics as the live path (forward-only max); a
                // re-stamp on every denied launch merely extends the
                // disclaimed window the standing denial already implies.
                ShardPilotAeiCursor.RecordSuppressionFloor(
                    _settingsStore, _config, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                ShardPilotCrashSpool.ClearRetryAfterDeadline(_settingsStore, _config);
            }
            else
            {
                if (_consentState != ShardPilotConsentState.Granted)
                {
                    // CONSENT-FIRST spool rule: only a construction that
                    // starts from a PERSISTED GRANT may load the offline
                    // event spool. Any non-granted start — unknown (absent
                    // record) or an unreadable consent record — PURGES the
                    // spool instead of holding it: a spool without an
                    // affirmative grant behind it cannot be proven to have
                    // been written under one (a pre-consent-first install
                    // spooled while "unknown" was open; an unreadable record
                    // may have carried a denial whose wipe was still owed).
                    // Wipe() fails closed: when the file cannot be removed, a
                    // persisted "wipe owed" marker keeps the spool disabled —
                    // nothing loaded, nothing appended — until a later launch
                    // removes the file for good, so unproven events can never
                    // ride a later grant.
                    if (_spool != null)
                    {
                        _spool.Wipe();
                    }

                    // The consent key is GLOBAL while spool files are
                    // per-scope, so the purge must reach EVERY scope's file
                    // in the active directory (idempotent for this scope's
                    // own file, fail-closed per file via the sweep-owed
                    // marker) — otherwise a sibling scope's pre-grant spool
                    // would survive this non-granted launch, and a later
                    // launch of THAT scope under the (globally persisted)
                    // grant would load and send unproven events. Same sweep
                    // the persisted-denial branch runs.
                    QuarantineSpoolIfSweepOwed(ShardPilotSpool.WipeAllScopesAtResolvedDirectory(
                        _config, _settingsStore, message => SafeLog("{0}", message)));
                    SafeLog(
                        consentReadFailed
                            ? "shardpilot consent state could not be read; failing closed: offline spools purged, analytics stays gated until an explicit grant"
                            : "shardpilot spool: no persisted consent grant at construction; purged the offline spools (consent-first)");
                }
                else if (legacyAnonymousIdRejected)
                {
                    // Fail-safe for the RESTORED spool: the stored
                    // anonymous id was just REJECTED by identity validation
                    // and replaced with a fresh UUID —
                    // but spooled envelopes were serialized VERBATIM under
                    // that identity, so restoring them would put the
                    // rejected identifier right back on
                    // events:batch. Mirror the sanitizer's posture for
                    // oversized outbox entries (drop, never truncate, never
                    // send) and defold's identity-mismatch drop-at-load
                    // shape: the whole record — written under the rejected
                    // identity, and the id is global — is purged, across
                    // every scope, without sending. Wipe() fails closed via
                    // the wipe-owed marker exactly like the non-granted
                    // purge.
                    if (_spool != null)
                    {
                        _spool.Wipe();
                    }

                    QuarantineSpoolIfSweepOwed(ShardPilotSpool.WipeAllScopesAtResolvedDirectory(
                        _config, _settingsStore, message => SafeLog("{0}", message)));
                    SafeLog(
                        "shardpilot spool: persisted anonymous id failed identity validation; " +
                        "purged the offline spools written under the rejected identifier");
                }

                if (legacyAnonymousIdRejected)
                {
                    // The same fail-safe for the CRASH spool — and
                    // deliberately OUTSIDE the consent-state chain above:
                    // crash reports ride the legitimate-interest plane
                    // regardless of analytics consent, and reports spooled
                    // BEFORE validation replaced the rejected id carry that
                    // contract-illegal anonymous_id verbatim; re-sending
                    // them would put it on crashes/ingest exactly like a
                    // restored event envelope would on events:batch. Purge
                    // the whole crash record (fail-closed wipe-owed marker),
                    // sweep sibling scopes' crash files (the id is global),
                    // and drop the persisted crash Retry-After — the
                    // backpressure belonged to the condemned backlog. (The
                    // persisted-denial path already runs this identical
                    // purge before reaching here.)
                    if (_crashSpool != null)
                    {
                        _crashSpool.Wipe();
                    }

                    QuarantineCrashSpoolIfSweepOwed(ShardPilotCrashSpool.WipeAllScopesAtResolvedDirectory(
                        _config, _settingsStore, message => SafeLog("{0}", message)));
                    ShardPilotCrashSpool.ClearRetryAfterDeadline(_settingsStore, _config);
                    SafeLog(
                        "shardpilot crash spool: purged reports written under the rejected " +
                        "persisted anonymous id instead of replaying the rejected identifier");
                }

                if (consentReadFailed)
                {
                    // The UNREADABLE consent record may have been a DENIAL —
                    // and a persisted denial suppresses crash capture and
                    // wipes the crash spool (the anonymous_id is the
                    // consent/erasure subject). Fail the crash plane closed
                    // the same way: collection is suppressed for this session
                    // (reason settings_read_failed — an explicit
                    // SetCrashReportingEnabled(true) records a fresh, durable
                    // decision) and the crash spool + its persisted
                    // Retry-After are purged, because the wipe a denial would
                    // have owed cannot be ruled out. A merely ABSENT consent
                    // record (fresh install) takes the normal
                    // legitimate-interest path below instead.
                    // Only the HOLD is raised — the independent crash
                    // setting loaded above stays intact (flag and reason
                    // alike), so lifting the hold with a fresh SetConsent
                    // restores exactly the state the player's own opt-out
                    // decision dictates, and an explicit opt-out keeps
                    // reporting opt_out once the hold is gone.
                    _consentReadFailureCrashHold = true;
                    if (_crashSpool != null)
                    {
                        _crashSpool.Wipe();
                    }

                    // The maybe-denial condemns sibling scopes' crash spools
                    // too — a persisted denial's sweep would have reached
                    // them all, so the fail-closed stand-in must as well.
                    QuarantineCrashSpoolIfSweepOwed(ShardPilotCrashSpool.WipeAllScopesAtResolvedDirectory(
                        _config, _settingsStore, message => SafeLog("{0}", message)));
                    ShardPilotCrashSpool.ClearRetryAfterDeadline(_settingsStore, _config);
                }
                else if (_crashSpool != null)
                {
                    // A Retry-After the crash ingest service issued before the
                    // previous process died gates the re-send pass on THIS
                    // launch too: server backpressure survives a relaunch.
                    // The crash plane is legitimate-interest and independent
                    // of analytics consent, so an unknown (absent) consent
                    // state does not purge the crash spool — its own opt-out
                    // gate and retention window govern it.
                    _crashResendNotBefore = ShardPilotCrashSpool.ReadRetryAfterDeadline(_settingsStore, _config);
                }
            }

            ConstructionBoundary("restore");
            if (_spool != null && _consentState == ShardPilotConsentState.Granted && !legacyAnonymousIdRejected)
            {
                // Restored events keep their original envelope verbatim and
                // stay in the spool file until the ingest service
                // acknowledges them (ack-based removal), so an app killed
                // mid-resend loses nothing. Only a persisted GRANT reaches
                // this load — every non-granted construction purged above,
                // and a rejected legacy anonymous id purged too (its
                // envelopes carry the contract-illegal identifier verbatim).
                var restored = _spool.CreateRestoredEvents();
                FilterAnalyticsAge(restored);
                for (var i = 0; i < restored.Count; i++)
                {
                    _restoredPending.Enqueue(restored[i]);
                    _stats.RecordEnqueued();
                }
            }

            // A durable pending purge from a previous run (a sentinel whose
            // spool rewrite failed, or a crash between the condemnation and
            // the removal) is honored BEFORE anything can flush: withdrawn
            // facts restored above are dropped, and the removal re-attempts.
            ConstructionBoundary("restore-filters");
            FilterCondemnedRestoredFactsAtLoad();

            // The restored experiment facts that SURVIVED the filter enter
            // the condemnation plane: they are previous-process emissions
            // the consumer's emit-time pre-registration never saw, and a
            // real-subjects sentinel landing before the restored batch
            // flushes must be able to withdraw them. Each is described WITH
            // its envelope's assignment coordinates so the registration can
            // recover the fact's true freshness from the matching serving
            // entry — a post-re-enable fact must not purge as pre-sentinel
            // under a stale condemnation marker just because it spooled
            // across the relaunch.
            if (_experiments != null)
            {
                List<ShardPilotRestoredFact> restoredExperimentFacts = null;
                lock (_gate)
                {
                    foreach (var restoredEvent in _restoredPending)
                    {
                        ShardPilotRestoredFact restoredFact;
                        if (TryDescribeRestoredExperimentFact(restoredEvent, out restoredFact))
                        {
                            if (restoredExperimentFacts == null)
                            {
                                restoredExperimentFacts = new List<ShardPilotRestoredFact>();
                            }

                            restoredExperimentFacts.Add(restoredFact);
                        }
                    }
                }

                if (restoredExperimentFacts != null)
                {
                    _experiments.RegisterRestoredFacts(restoredExperimentFacts);
                }

                // Every fact the spool held is in the backlog now: the
                // fact-key history may be pruned from here on, starting with
                // the keys a previous launch retired whose facts are gone.
                _experiments.ArmFactKeyPrune();
                PruneExperimentFactKeys();
            }

            _withdrawalScanReady = true;
            RetryWithdrawalFrames();

            // The PUBLIC construction path defers ALL egress arming until
            // after the lifecycle registration and its privacy-state
            // catch-up (see StartDispatchAfterRegistration): nothing may
            // dispatch under state that a concurrent decision — propagated
            // only to registered clients — has already superseded. Direct
            // internal constructions (tests, multi-client setups) keep the
            // construction-time arming.
            ConstructionBoundary("construction-complete");
            _startWorkerOnDispatch = startWorker;
#if SHARDPILOT_SDK_TEST_SUPPORT
            lock (ConstructedForTesting)
            {
                ConstructedForTesting.Add(new WeakReference<ShardPilotClient>(this));
            }
#endif
            if (!deferDispatchForRegistration) CompleteHostConstruction(false);
        }

        /// <summary>
        /// Set at construction; consumed by
        /// <see cref="StartDispatchAfterRegistration"/> so the public path
        /// can arm the worker AFTER the lifecycle registration's privacy
        /// catch-up instead of inside the constructor.
        /// </summary>
        private readonly bool _startWorkerOnDispatch;

        /// <summary>
        /// Arms this client's egress: starts the analytics worker (when
        /// configured) and kicks the initial crash re-send pass. On the
        /// public construction path this runs AFTER the lifecycle
        /// registration and its persisted-state refresh, so the first
        /// dispatch can never ride consent/opt-out state that a concurrent
        /// decision already superseded; internal constructions call it from
        /// the constructor (no registration is involved for them). Crash
        /// reports left by an earlier launch go out FIRST — before the
        /// analytics worker's first flush interval elapses — because a
        /// crash is the most valuable signal if this process dies again
        /// soon; a persisted Retry-After deadline defers the kick and the
        /// worker re-arms it once the deadline elapses.
        /// </summary>
        private void StartDispatchAfterRegistration()
        {
            // ⚠ app.opened IS DELIBERATELY NOT EMITTED HERE, and the test suite is
            // why. Emitting it at this construction dispatch point makes the SDK
            // enqueue an event on init, which changed behaviour for every consumer:
            // the lazily created session came to belong to app.opened rather than to
            // the game's first tracked event, and fifteen existing scenes asserting
            // "nothing queued yet" failed correctly rather than brittlely
            // (FlagOffMeansZeroExperimentCodePaths: "zero queued facts, but was 1").
            //
            // Foreground/background transitions measure the switch rate.
            // app.opened contributes only one event per launch, so it is
            // not emitted by this transition observer.
            // The automatic session boundary. The dispatcher host behaviour is
            // the SDK's one observer of OnApplicationPause and already fans its
            // transitions out here for the ANR watchdog, so this is a second
            // subscriber to an existing seam rather than new engine plumbing.
            ConstructionBoundary("callbacks");
            try
            {
                ShardPilotDispatcher.PauseStateChanged += OnPauseStateChangedForSession;
            }
            catch
            {
                // A missing dispatcher must never fail construction: without it
                // the automatic boundary is simply absent and SessionEnd stays
                // available to the host.
            }

            ConstructionBoundary("worker");
            if (_startWorkerOnDispatch)
            {
#if UNITY_WEBGL && !UNITY_EDITOR
                ShardPilotDispatcher.Instance.Run(RunCoroutineWorker());
#else
                _worker = new Thread(RunWorker)
                {
                    IsBackground = true,
                    Name = "ShardPilot SDK Worker"
                };
                _worker.Start();
#endif
            }

            ConstructionBoundary("crash-resend");
            MaybeStartCrashResendPass();

            // Client construction is a consent dispatch point: a
            // receipt retained by an earlier launch starts re-sending now —
            // ahead of the first post-restart batch, which the grant-receipt gate holds
            // until the retained grant's own handoff regardless.
            ConstructionBoundary("consent-resend");
            MaybeKickConsentOutbox(workerCadence: false);
        }

        /// <summary>
        /// Creates the offline spool for this client, or returns null when it
        /// is disabled or unavailable. On WebGL the spool is a documented
        /// no-op: browser persistence is not a real synchronous filesystem,
        /// so the in-memory behavior is unchanged there.
        /// </summary>
        private ShardPilotSpool CreateSpool()
        {
#if UNITY_WEBGL && !UNITY_EDITOR
            return null;
#else
            if (!_config.SpoolEnabled)
            {
                return null;
            }

            // The settings store (identity/consent) also carries the spool's
            // per-scope "wipe owed" marker, so a failed consent wipe fails
            // closed across launches.
            return ShardPilotSpool.TryCreate(_config, _settingsStore, message => SafeLog("{0}", message));
#endif
        }

        /// <summary>
        /// Creates the durable crash spool for this client, or returns null
        /// when it is disabled or unavailable. Gated on the SpoolEnabled
        /// master switch AND the crash spool's own flag AND a configured
        /// crash:write key — without the key no report could ever be
        /// re-sent, so persisting them would only grow a file that can never
        /// drain. On WebGL the crash spool is a documented no-op, exactly
        /// like the event spool.
        /// </summary>
        private ShardPilotCrashSpool CreateCrashSpool()
        {
#if UNITY_WEBGL && !UNITY_EDITOR
            return null;
#else
            if (!_config.SpoolEnabled || !_config.CrashSpoolEnabled || string.IsNullOrEmpty(_config.CrashKey))
            {
                return null;
            }

            return ShardPilotCrashSpool.TryCreate(_config, _settingsStore, message => SafeLog("{0}", message));
#endif
        }

        public bool Track(string eventName, EventProperties? props = null)
        {
            try
            {
                return Enqueue(new ShardPilotEvent
                {
                    Name = eventName,
                    // PrepareEvent snapshots accepted properties after name admission.
                    Props = props
                });
            }
            catch (Exception ex)
            {
                _stats.RecordFailure(ex.Message);
                SafeLog("shardpilot event rejected: {0}", ex.Message);
                return false;
            }
        }

        public ShardPilotResult Track(string eventName, EventProperties? props,
            ShardPilotTrackOptions? options)
        {
            var accepted = Enqueue(new ShardPilotEvent
            {
                Name = eventName,
                Props = props,
                SchemaVersion = options?.SchemaVersion
            }, out var error);
            return new ShardPilotResult(accepted, error, false);
        }

        /// <summary>
        /// Starts a new session, emitting <c>app.session_started</c>. A session
        /// that is still open, including one opened lazily by an earlier event,
        /// ends first: <c>app.session_ended</c> with reason
        /// <c>session_start</c>, or with <c>idle_timeout</c> at its deadline
        /// when its background pause has already expired.
        /// </summary>
        /// <remarks>
        /// The end is claimed and enqueued in the same gate hold that commits
        /// the start, ahead of it, and on the overflow path it is appended to
        /// the spool together with the start: a refused start therefore leaves
        /// the replaced session, its pause and any staged experiments renewal
        /// exactly as they were, and no end is emitted for it. Two things that
        /// happen while that append is in flight change this. A resume that
        /// finds the session's pause expired cannot claim it, so the refusal
        /// ends the session at its deadline and announces a new one, as the
        /// resume would have. An event accepted into the new session keeps the
        /// new session, so the replaced one still ends. A session whose
        /// own start is still awaiting its append verdict on another call is
        /// not ended: that start may yet be refused.
        /// </remarks>
        public bool SessionStart(string? sessionId = null, EventProperties? props = null)
        {
            return StartSession(sessionId, props, null, true);
        }

        private bool StartSession(string sessionId, IDictionary<string, object> props, SessionState owedSession,
            bool replaceOpenSession = false)
        {
            var normalizedSessionId = ShardPilotConfig.Trim(sessionId);
            if (string.IsNullOrEmpty(normalizedSessionId))
            {
                normalizedSessionId = ShardPilotIds.NewUuidV7();
            }

            ShardPilotEvent prepared;
            ShardPilotEvent replacementEnd = null;
            try
            {
                prepared = PrepareEvent(new ShardPilotEvent
                {
                    Name = "app.session_started",
                    SessionId = normalizedSessionId,
                    SessionSequence = 1,
                    Props = ShardPilotEvent.CloneMap(props)
                });
                if (replaceOpenSession)
                {
                    // Session id, sequence, time and reason are filled in the
                    // commit hold, from the session that is open at that moment.
                    replacementEnd = PrepareEvent(new ShardPilotEvent { Name = "app.session_ended" },
                        stampTimestamp: false);
                }
            }
            catch (Exception ex)
            {
                _stats.RecordFailure(ex.Message);
                SafeLog("shardpilot event rejected: {0}", ex.Message);
                return false;
            }

            SessionLifecycle lifecycle = null;
            var accepted = false;
            try
            {
                accepted = EnqueuePrepared(prepared, true, provisional =>
                {
                    var renewalStake = 0L;
                    // The whole session-start transition — the experiments
                    // renewal INCLUDED — runs in the commit's own gate hold,
                    // BEFORE the worker is woken and BEFORE _sessionState.Id names
                    // the new session. Deferring the renewal past the enqueue
                    // let a threaded worker sweep a pre-session owed exposure
                    // into the just-created session under the OLD marker, and
                    // the late rotation then re-armed the same assignment: two
                    // first-session facts with different deterministic ids. The
                    // renewal runs first (rotating the marker, claiming a
                    // renewal's un-adopted snapshots for the prior lazy
                    // session, and adopting a first session's pre-session
                    // snapshots onto the id passed in), so no sweep can ever
                    // observe the new session with the old marker. The
                    // exposure contract is once per (experiment, version,
                    // subject) per SESSION: the renewal re-arms it, and a
                    // still-applied assignment emits one exposure into the new
                    // session on its next sweep. The consumer's lock nests
                    // inside _gate here; the consumer never calls back into
                    // client code under its lock, so the order cannot invert.
                    //
                    // On the overflow-to-spool path this commit is PROVISIONAL:
                    // acceptance is decided only after the unlocked disk
                    // append, and a refused append rolls the session mutation
                    // back. The renewal is therefore only STAGED there — the
                    // consumer latches its sweep lane and keeps serving the
                    // pre-start session id (closing the same
                    // new-session/old-marker window by suppression instead of
                    // ordering) and commits or abandons with the append's
                    // verdict, so a refused SessionStart leaves the consumer
                    // bit-identical to before the call and SweepOwed can never
                    // emit an exposure for a session-start the SDK refused. The
                    // returned STAKE binds the stage to THIS overflow
                    // operation: only this operation's verdict settles it — a
                    // concurrent overflow event's rollback abandons its own
                    // stage (or nothing), never this one.
                    if (_experiments != null)
                    {
                        var previousSessionId = CurrentExperimentSessionId();
                        if (provisional)
                        {
                            renewalStake = _experiments.BeginProvisionalSessionRenewal(
                                !string.IsNullOrEmpty(previousSessionId), previousSessionId, normalizedSessionId);
                        }
                        else
                        {
                            _experiments.OnSessionRenewed(
                                !string.IsNullOrEmpty(previousSessionId), previousSessionId, normalizedSessionId);
                        }
                    }

                    lifecycle = new SessionLifecycle(_sessionState.OwesStart,
                        provisional ? Thread.CurrentThread.ManagedThreadId : 0, !provisional);
                    _sessionState = new SessionState(
                        normalizedSessionId, prepared.SessionSequence, false, lifecycle);

                    // Lifecycle metadata was stamped and size-checked in this
                    // same gate hold before committing the start's effects.
                    return renewalStake;
                }, owedSession, replacementEnd: replacementEnd);
            }
            finally
            {
                lock (_gate)
                {
                    if (lifecycle != null && lifecycle.PendingStartThreadId != 0)
                    {
                        Volatile.Write(ref lifecycle.StartAccepted, accepted);
                        lifecycle.PendingStartThreadId = 0;
                        Monitor.PulseAll(_gate);
                    }
                }
            }

            return accepted;
        }

        /// <summary>
        /// Ends the active session, emitting <c>app.session_ended</c> for it.
        /// Paired with the <c>app.session_started</c> this SDK already
        /// emits, it is what makes session length and sessions-per-DAU
        /// derivable: the schema requires <c>session_id</c>, so a consumer
        /// differences the two <c>event_ts</c> on the shared id and needs no
        /// duration property.
        /// </summary>
        /// <param name="reason">
        /// Recorded as <c>props.reason</c>; a missing or empty reason records
        /// <c>session_end</c>. <c>idle_timeout</c> is what the automatic
        /// background boundary passes.
        /// </param>
        /// <returns>
        /// Whether the WIRE event was accepted, or <c>true</c> when no session
        /// was open: ending nothing succeeds and sends nothing. The local
        /// session always ends — see below — so a <c>false</c> here means the
        /// event was refused (consent not granted, queue full with no spool) or
        /// the client is closed, never that the session is still open.
        /// </returns>
        /// <remarks>
        /// <para>
        /// ⚠ NO ACTIVE SESSION IS A NO-OP, and the EARLY RETURN below is what
        /// enforces it. <c>AttachSessionLocked</c> LAZILY OPENS a session for any
        /// client-source event carrying none, so reaching the enqueue with no
        /// session would have this method create one purely in order to announce
        /// that it had ended — inventing a session nobody played, which then
        /// reports zero length.
        /// </para>
        /// <para>
        /// The <c>attachCurrentSession: false</c> argument says the same thing a
        /// second way, and is deliberately kept — but it is NOT what makes this
        /// safe today, and the comment claimed otherwise until a mutant proved
        /// it. Flipping that argument to <c>true</c> changes no observable
        /// behaviour, because the event always carries an explicit
        /// <c>SessionId</c> and the lazy-create branch is guarded on that field
        /// being empty. It earns its place only if a later edit stops attaching
        /// the id explicitly. The scene that fails when the early return is
        /// removed is
        /// <c>SessionEndWithNoActiveSessionDoesNotOpenOneToCloseIt</c>.
        /// </para>
        /// <para>
        /// ⚠ LOCAL TEARDOWN ALWAYS COMPLETES, even when the wire event is
        /// refused. A session ending is a fact about the app, not about delivery,
        /// so a consent-blocked or
        /// dropped event must not leave session state stuck active — the next
        /// event would otherwise inherit a session the SDK believes never ended.
        /// </para>
        /// <para>
        /// ⚠ The clear is a COMPARE-and-clear. Between reading the active id and
        /// committing, a concurrent <c>SessionStart</c> may have rotated the
        /// session; clearing unconditionally would kill the NEW session instead
        /// of the one that ended.
        /// </para>
        /// </remarks>
        public bool SessionEnd(string? reason = null)
        {
            // An expired background pause keeps its earlier boundary. Otherwise
            // the caller decides when this manual session ends, so use "now".
            return SessionEnd(reason, default(DateTime));
        }

        /// <summary>
        /// <see cref="SessionEnd(string)"/> with an explicit manual end instant.
        /// An already-expired background pause still takes precedence.
        /// </summary>
        internal bool SessionEnd(string reason, DateTime endedAtUtc)
        {
            SessionEndClaim claim;
            bool closed;
            lock (_gate)
            {
                if (_hostExiting || _hostLifetime.Exiting) return false;
                closed = _closed;
                claim = ClaimExpiredSessionLocked() ??
                    ClaimSessionEndLocked(string.IsNullOrEmpty(reason) ? "session_end" : reason,
                        endedAtUtc, null);
            }
            // With no open session there is nothing to end: that succeeds and
            // sends nothing. A closed client refuses.
            if (claim == null) return !closed;
            return EnqueueSessionEnd(claim);
        }

        // The engine's quit callback cannot await a network drain. Claim the
        // active session once, then persist its end with the pending tail on
        // this thread. An already expired pause retains its earlier boundary;
        // quitting never discharges the replacement session's start debt.
        internal void OnApplicationQuitForSession()
        {
            try
            {
                SessionEndClaim claim;
                lock (_gate)
                {
                    claim = ClaimExpiredSessionLocked() ??
                        ClaimSessionEndLocked("app_final", DateTime.UtcNow, _sessionState.Lifecycle);
                }
                EnqueueSessionEnd(claim, closing: _hostExiting || _hostLifetime.Exiting);
            }
            finally
            {
                // Even a refused end must not skip consent retries or leave
                // previously accepted events without their persistence attempt.
                PersistPendingEvents();
            }
        }

        private sealed class SessionEndClaim
        {
            public ShardPilotEvent Event;
            public long Transitions;
            public string Run;
            public long Epoch;
        }

        // Claim and local teardown are one gate hold. Delivery happens afterwards,
        // so refusal cannot leave a locally ended session active, and a concurrent
        // start cannot be cleared or ended by this claim.
        private SessionEndClaim ClaimSessionEndLocked(string reason, DateTime endedAtUtc,
            SessionLifecycle expectedLifecycle)
        {
            if (_closed || string.IsNullOrEmpty(_sessionState.Id) ||
                (expectedLifecycle != null && !ReferenceEquals(_sessionState.Lifecycle, expectedLifecycle)))
            {
                return null;
            }

            if (expectedLifecycle != null)
            {
                // Wall-clock corrections and caller-provided event timestamps must
                // not put the automatic end before accepted activity in this session.
                if (endedAtUtc < expectedLifecycle.LatestAcceptedTimestampUtc)
                {
                    endedAtUtc = expectedLifecycle.LatestAcceptedTimestampUtc;
                }
                expectedLifecycle.AutomaticEndUtc = endedAtUtc;
            }

            RefreshLifecycleTransitionsLocked();
            var claim = new SessionEndClaim
            {
                Event = new ShardPilotEvent
                {
                    Name = "app.session_ended",
                    SessionId = _sessionState.Id,
                    SessionSequence = _sessionState.Sequence + 1,
                    TimestampUtc = endedAtUtc,
                    Props = new Dictionary<string, object>
                    {
                        { "reason", reason }
                    }
                },
                Transitions = _lifecycleTransitions,
                Run = _lifecycleRun,
                Epoch = _lifecycleTransitionsEpoch
            };
            _sessionState = new SessionState(null, 0, true);
            return claim;
        }

        /// <param name="closing">An end a close owes still enters a closed
        /// client: it lifts only the closed-client refusal.</param>
        private bool EnqueueSessionEnd(SessionEndClaim claim, bool closing = false)
        {
            if (claim == null) return false;
            SessionEndPostClaimHookForTesting?.Invoke();
            ShardPilotEvent prepared;
            try
            {
                prepared = PrepareEvent(claim.Event);
            }
            catch (Exception ex)
            {
                _stats.RecordFailure(ex.Message);
                SafeLog("shardpilot event rejected: {0}", ex.Message);
                return false;
            }

            StampLifecycleTransitionsLocked(prepared, claim.Transitions, claim.Run);
            return EnqueuePrepared(prepared, false, null, expectedConsentEpoch: claim.Epoch, closing: closing);
        }

        // The same admission precedes ordinary events and host experiment facts.
        // Historical experiment sweeps deliberately do not call this method.
        private bool AdmitCurrentSession()
        {
            SessionEndClaim claim;
            lock (_gate)
            {
                claim = ClaimExpiredSessionLocked();
            }
            EnqueueSessionEnd(claim);
            return DischargeOwedSessionStart();
        }

        /// <summary>
        /// The end an expired pause owes, or <c>default</c> while the pause is
        /// still within the session timeout. Stopwatch can stop during mobile
        /// deep sleep and wall time can move backwards, so the larger elapsed
        /// value decides. The end is the deadline, never later than now.
        /// Caller must hold <c>_gate</c>.
        /// </summary>
        private DateTime ExpiredPauseEndLocked(SessionPause pause)
        {
            var ticks = _monotonicTicks() - pause.SinceMonotonicTicks;
            var monotonic = ticks <= 0 ? TimeSpan.Zero
                : TimeSpan.FromSeconds(ticks / (double)System.Diagnostics.Stopwatch.Frequency);
            var now = _utcNow();
            var wall = now - pause.SinceUtc;
            if ((wall > monotonic ? wall : monotonic) < _config.SessionTimeout) return default(DateTime);
            var endedAt = pause.SinceUtc + _config.SessionTimeout;
            return endedAt > now ? now : endedAt;
        }

        private SessionEndClaim ClaimExpiredSessionLocked()
        {
            var pause = _sessionPause;
            if (_backgroundedAtUtc == default(DateTime) || pause == null) return null;
            var lifecycle = pause.Lifecycle;
            if (!ReferenceEquals(_sessionState.Lifecycle, lifecycle))
            {
                // A provisional replacement can still roll back to the paused
                // lifecycle. Observing a different identity does not consume it.
                return null;
            }

            // A forward wall-clock correction can still split a short session;
            // the suspension-inclusive platform-clock limitation remains #89.
            var endedAt = ExpiredPauseEndLocked(pause);
            if (endedAt == default(DateTime)) return null;

            // Consume only the boundary identity. The app remains backgrounded
            // until resume, so lifecycle-transition counting still sees that edge.
            _sessionPause = null;
            // ClaimSessionEndLocked applies the accepted-event lower bound last:
            // it wins even if a backward clock puts that event after now.
            return ClaimSessionEndLocked("idle_timeout", endedAt, lifecycle);
        }

        /// <summary>
        /// Performs the real session start a previous END left owed, so the event
        /// about to be enqueued lands in a session that has one.
        /// </summary>
        /// <remarks>
        /// The fast path reads both debt and the in-flight claim: a provisional
        /// start has already cleared the debt, but is not yet accepted. Competing
        /// callers wait without holding _gate across the append. Same-thread
        /// re-entry refuses instead of waiting on itself or using provisional state.
        ///
        /// Called from Enqueue rather than from AttachSessionLocked, which runs UNDER
        /// _gate inside the enqueue it would have to recurse into. SessionEnd and
        /// SessionStart both reach EnqueuePrepared directly, so neither re-enters
        /// here: an end cannot discharge the start it just armed.
        /// </remarks>
        private bool DischargeOwedSessionStart()
        {
            if (!Volatile.Read(ref _sessionState).OwesStart &&
                Volatile.Read(ref _owedSessionStartThreadId) == 0)
            {
                return true;
            }

            OwedSessionStartBeforeClaimHookForTesting?.Invoke();
            var threadId = Thread.CurrentThread.ManagedThreadId;
            SessionState owedSession;
            lock (_gate)
            {
                // Recheck both owners after every wake: while an explicit append
                // releases the gate, another waiter can acquire the owed claim.
                while (_owedSessionStartThreadId != 0 || _sessionState.Lifecycle.PendingStartThreadId != 0)
                {
                    if (_owedSessionStartThreadId == threadId ||
                        _sessionState.Lifecycle.PendingStartThreadId == threadId)
                    {
                        _stats.RecordRefusal("session_start_pending");
                        return false;
                    }

                    Monitor.Wait(_gate);
                }

                if (IntakeClosed)
                {
                    return false;
                }

                if (!_sessionState.OwesStart)
                {
                    return true;
                }

                owedSession = _sessionState;
                Volatile.Write(ref _owedSessionStartThreadId, threadId);
            }

            try
            {
                OwedSessionStartPostClaimHookForTesting?.Invoke();
                // Refusal leaves the debt armed (including a whole-value overflow
                // rollback) and refuses this caller's dependent event with it.
                return StartSession(null, null, owedSession);
            }
            finally
            {
                lock (_gate)
                {
                    Volatile.Write(ref _owedSessionStartThreadId, 0);
                    Monitor.PulseAll(_gate);
                }
            }
        }

        // Called under _gate; Monitor.Wait releases it so the append can finish.
        // Always re-read the current lifecycle: rollback or a later transition
        // may replace the identity while this caller waits.
        private bool WaitForSessionStartLocked()
        {
            while (_sessionState.Lifecycle.PendingStartThreadId != 0)
            {
                if (_sessionState.Lifecycle.PendingStartThreadId == Thread.CurrentThread.ManagedThreadId)
                {
                    _stats.RecordRefusal("session_start_pending");
                    return false;
                }
                Monitor.Wait(_gate);
            }
            return true;
        }

        /// <summary>
        /// Restarts the lifecycle-transition count when a consent loss has
        /// advanced the denial epoch since it last accumulated, and re-mints
        /// its run id with it. Caller must hold <c>_gate</c>. Runs before
        /// every increment and every read, so a stale value can neither grow
        /// nor reach the wire; the enumeration of loss sites is the epoch's
        /// own, not a list kept here.
        /// </summary>
        private void RefreshLifecycleTransitionsLocked()
        {
            var epoch = Interlocked.Read(ref _consentDenialEpoch);
            if (_lifecycleTransitionsEpoch != epoch)
            {
                _lifecycleTransitions = 0;
                _lifecycleTransitionsEpoch = epoch;
                _lifecycleRun = NewLifecycleRun();
            }
        }

        /// <summary>
        /// Stamps the current value and run onto a session start at its commit.
        /// Runs inside the enqueue's own gate, past its consent refusal. Caller must hold
        /// <c>_gate</c>.
        /// </summary>
        private void CarryLifecycleTransitionsLocked(ShardPilotEvent ev)
        {
            RefreshLifecycleTransitionsLocked();
            StampLifecycleTransitionsLocked(ev, _lifecycleTransitions, _lifecycleRun);
        }

        /// <summary>
        /// A caller-supplied prop of either name is overwritten: the SDK owns
        /// this measurement.
        /// </summary>
        private static void StampLifecycleTransitionsLocked(ShardPilotEvent ev, long transitions, string run)
        {
            if (ev.Props == null)
            {
                ev.Props = new Dictionary<string, object>(2, StringComparer.Ordinal);
            }

            ev.Props["lifecycle_transitions"] = transitions;
            ev.Props["lifecycle_run"] = run;
        }

        /// <summary>
        /// A random id with no timestamp in it: a UUIDv7 would carry the mint
        /// moment of a post-loss restart onto the wire.
        /// </summary>
        private static string NewLifecycleRun()
        {
            return Guid.NewGuid().ToString("N");
        }

        /// <summary>
        /// Records engine pause/resume transitions and evaluates the automatic
        /// session boundary. Current activity also evaluates it before admission;
        /// no background timer or running suspended process is required.
        /// </summary>
        internal void OnPauseStateChangedForSession(bool paused)
        {
            try
            {
                SessionEndClaim claim = null;
                lock (_gate)
                {
                    var flipsBackgroundState = paused == (_backgroundedAtUtc == default(DateTime));
                    if (IntakeClosed || !flipsBackgroundState) return;
                    if (_consentState == ShardPilotConsentState.Granted)
                    {
                        RefreshLifecycleTransitionsLocked();
                        _lifecycleTransitions++;
                    }
                    if (paused)
                    {
                        _backgroundedAtUtc = _utcNow();
                        ArmBackgroundSessionLocked(_sessionState.Lifecycle);
                    }
                    else
                    {
                        // Count the closing resume before taking the session's
                        // upper edge. Claim and clear the pause in this same hold.
                        claim = ClaimExpiredSessionLocked();
                        if (claim == null && _sessionPause != null &&
                            !ReferenceEquals(_sessionPause.Lifecycle, _sessionState.Lifecycle))
                        {
                            // A provisional replacement of the paused session is in
                            // flight. Its rollback settles what this resume decides.
                            _sessionPause.RacedResumeEndUtc = ExpiredPauseEndLocked(_sessionPause);
                        }
                        _sessionPause = null;
                        _backgroundedAtUtc = default(DateTime);
                        _resumeGeneration++;
                    }
                }

                if (claim != null)
                {
                    EnqueueSessionEnd(claim);
                }
                if (!paused && ConsentState == ShardPilotConsentState.Granted)
                {
                    // Resume also discharges a fresh start owed after consent
                    // loss, whose old pause and session were already discarded.
                    DischargeOwedSessionStart();
                }
            }
            catch (Exception ex)
            {
                SafeLog("shardpilot session boundary skipped: {0}", ex.Message);
            }
        }

        public bool Enqueue(ShardPilotEvent ev)
        {
            return Enqueue(ev, out _);
        }

        private bool Enqueue(ShardPilotEvent ev, out string error)
        {
            error = null;
            var name = NormalizeCustomEventName(ev == null ? null : ev.Name);
            lock (_gate)
            {
                if (IntakeClosed) { error = ShardPilotFlushCodes.Shutdown; return false; }
                if (ev != null && name.Length == 0)
                {
                    error = "event_name_required";
                    _stats.RecordRefusal(error);
                    return false;
                }
                if (_consentState != ShardPilotConsentState.Granted)
                {
                    error = ShardPilotConsent.IsDeniedState(_consentState)
                        ? ShardPilotConsent.DeniedError : ShardPilotConsent.UnknownError;
                    _stats.RecordRefusal(error);
                    return false;
                }
                if (IsReservedCustomEventName(name))
                {
                    error = "reserved_event_name";
                    _stats.RecordRefusal(error);
                    return false;
                }
                if (ev?.SchemaVersion is int version && (version < 1 || version > 65535))
                {
                    error = "schema_version_invalid";
                    _stats.RecordRefusal(error);
                    return false;
                }
            }
            return EnqueueEvent(ev, name, out error);
        }

        private static string NormalizeCustomEventName(string name)
        {
            if (name == null) return string.Empty;
            const string whitespace = " \t\r\n\v\f";
            var first = 0;
            var last = name.Length;
            while (first < last && whitespace.IndexOf(name[first]) >= 0) first++;
            while (last > first && whitespace.IndexOf(name[last - 1]) >= 0) last--;
            return name.Substring(first, last - first);
        }

        private static bool IsReservedCustomEventName(string name)
        {
            var canonical = name.StartsWith("app.", StringComparison.Ordinal) ? name.Substring(4) : name;
            switch (canonical)
            {
                case "experiment_exposure":
                case "experiment_outcome":
                case "governed_action_result":
                case "app_runtime_ping":
                case "monitor_evaluation_recorded":
                case "llm_usage":
                case "support_assistant_feedback":
                case "ad_impression_revenue":
                case "session_started":
                case "session_ended":
                    return true;
                default:
                    return false;
            }
        }

        // Typed producers supply their own names; the public entry point above
        // owns custom-name admission before any session transition or queue work.
        private bool EnqueueEvent(ShardPilotEvent ev, string normalizedCustomName = null, Action<string> refusal = null)
        {
            var accepted = EnqueueEvent(ev, normalizedCustomName, out var error);
            if (!accepted && error != null) refusal?.Invoke(error);
            return accepted;
        }

        private bool EnqueueEvent(ShardPilotEvent ev, string normalizedCustomName, out string error)
        {
            error = null;
            ShardPilotEvent prepared;
            try
            {
                prepared = PrepareEvent(ev, false, normalizedCustomName);
                prepared.CapturedProps = ShardPilotJson.CaptureMap(prepared.Props);
                prepared.CapturedContext = ShardPilotJson.CaptureMap(prepared.Context);
            }
            catch (Exception ex)
            {
                error = ex.Message;
                _stats.RecordFailure(ex.Message);
                SafeLog("shardpilot event rejected: {0}", ex.Message);
                return false;
            }

            // An explicit session ID is already-attributed history. It must not
            // spend current-session debt or advance the current timestamp record,
            // even if a caller has reused that same ID for another lifecycle.
            var currentActivity = string.IsNullOrEmpty(prepared.SessionId);
            string sizeError = null;
            Action<string> refusal = code => sizeError = code;
            if (currentActivity)
            {
                lock (_gate)
                {
                    if (IntakeClosed) { error = ShardPilotFlushCodes.Shutdown; return false; }
                    if (_consentState == ShardPilotConsentState.Granted)
                    {
                        // Before owed-start/expired-end work: preview metadata without
                        // spending a session or sequence. The final admission rechecks
                        // and stamps the actual construction under this same gate.
                        var preview = prepared.Clone();
                        if (preview.TimestampUtc == default(DateTime)) preview.TimestampUtc = DateTime.UtcNow;
                        var session = _sessionState.OwesStart || (_backgroundedAtUtc != default(DateTime) &&
                            _sessionPause != null && ReferenceEquals(_sessionPause.Lifecycle, _sessionState.Lifecycle) &&
                            ExpiredPauseEndLocked(_sessionPause) != default(DateTime))
                            ? new SessionState(ShardPilotIds.NewUuidV7(), 1, false) : _sessionState;
                        PreviewAttachedSession(preview, true, session);
                        if (!AdmitEventSize(preview, refusal, retain: false)) { error = sizeError; return false; }
                    }
                }
            }
            if (currentActivity && !AdmitCurrentSession()) { error = "event_rejected"; return false; }
            if (prepared.TimestampUtc == default(DateTime)) prepared.TimestampUtc = DateTime.UtcNow;
            var accepted = EnqueuePrepared(prepared, currentActivity, null, refusal: refusal);
            if (!accepted) error = sizeError ?? "event_rejected";
            return accepted;
        }

        private bool AdmitEventSize(ShardPilotEvent ev, Action<string> refusal = null, bool retain = true,
            bool experiment = false)
        {
            int bytes;
            var envelope = ShardPilotJson.SerializeEnvelope(_config, ev);
            if (!ShardPilotJson.EventExceedsSizeLimit(envelope, out bytes))
            {
                // Batching and live fact handoff use SerializedEnvelope to identify
                // restored entries. Retain checked bytes without that replay marker.
                if (retain || experiment) ev.AdmittedEnvelope = envelope;
                return true;
            }
            var code = ShardPilotExperimentApply.EnvelopeTooLarge;
            var message = code + ": " + bytes.ToString(System.Globalization.CultureInfo.InvariantCulture) + " bytes exceeds 2048";
            if (experiment) _stats.RecordExperimentDrop(ev.Name, code, message);
            else _stats.RecordRefusal(message);
            refusal?.Invoke(code);
            return false;
        }

        // ── experiments (dark unless ExperimentsEnabled) ──

        /// <summary>
        /// Fetches the server-evaluated assignment for one experiment.
        /// Assignment delivery requires granted consent (forced-minor is
        /// fully off); remote-config delivery remains available without
        /// identifiers when consent is not granted. The assignment getters
        /// serve null rather than failing, so game code can ship one code
        /// path with the control experience as its default. The callback
        /// runs once on the captured host thread while this owner is live.
        /// Returns false when no request starts; live refusals still notify.
        /// </summary>
        public bool FetchExperimentAssignment(
            string experimentKey,
            IDictionary<string, string>? attributes,
            Action<ShardPilotExperimentResult>? callback)
        {
            if (!_fetchCallbackHost.IsLive || !FetchOwnerCurrent) return false;
            callback = _fetchCallbackHost.WrapCallback(callback, () => FetchOwnerCurrent);
            lock (_gate)
            {
                if (IntakeClosed)
                {
                    InvokeExperimentCallback(callback, ShardPilotExperimentResult.Failure("shutdown"));
                    return false;
                }
            }

            if (_experiments == null)
            {
                InvokeExperimentCallback(callback, ShardPilotExperimentResult.Failure("experiments_not_configured"));
                return false;
            }

            return _experiments.FetchExperimentAssignment(experimentKey, attributes, callback);
        }

        public bool FetchExperimentAssignment(string experimentKey, Action<ShardPilotExperimentResult>? callback)
        {
            return FetchExperimentAssignment(experimentKey, null, callback);
        }

        private static void InvokeExperimentCallback(
            Action<ShardPilotExperimentResult> callback,
            ShardPilotExperimentResult result)
        {
            if (callback == null)
            {
                return;
            }

            try
            {
                callback(result);
            }
            catch
            {
                // The callback is game code; never let it break the SDK.
            }
        }

        internal void RemoteConfigRefused(Action<ShardPilotRemoteConfigResult> callback)
        {
            _fetchCallbackHost.WrapCallback(callback, () => FetchOwnerCurrent)?.Invoke(
                ShardPilotRemoteConfigResult.Failure("remote config is not configured"));
        }

        /// <summary>The cached variant key for the experiment, or null when
        /// none is served (not fetched, not assigned, consent not granted,
        /// experiments not configured, or the client is closed). Never
        /// touches the network. A CLOSED client serves nothing: callers
        /// holding a stale reference after CloseAsync must stop applying
        /// treatments, the same refusal the fetch/track paths give.</summary>
        public string? GetExperimentVariant(string experimentKey)
        {
            lock (_gate)
            {
                if (IntakeClosed || _workerStopped)
                {
                    return null;
                }
            }

            return _experiments != null ? _experiments.GetExperimentVariant(experimentKey) : null;
        }

        /// <summary>The cached variant payload (a defensive copy), or null.
        /// Same posture as <see cref="GetExperimentVariant"/> — including
        /// the closed-client refusal.</summary>
        public IReadOnlyDictionary<string, object?>? GetExperimentPayload(string experimentKey)
        {
            lock (_gate)
            {
                if (IntakeClosed || _workerStopped)
                {
                    return null;
                }
            }

            return _experiments != null ? _experiments.GetExperimentPayload(experimentKey) : null;
        }

        /// <summary>Records an application and returns its variant, or null when refused.</summary>
        public string? ApplyExperimentVariant(string experimentKey)
        {
            lock (_gate) { if (IntakeClosed || _workerStopped) return null; }
            var result = _experiments == null ? null : _experiments.ApplyExperimentVariant(experimentKey);
            WakeWorker();
            return result;
        }

        /// <summary>Records the same application slot as ApplyExperimentVariant and returns a defensive payload copy.</summary>
        public IReadOnlyDictionary<string, object?>? ApplyExperimentPayload(string experimentKey)
        {
            lock (_gate) { if (IntakeClosed || _workerStopped) return null; }
            var result = _experiments == null ? null : _experiments.ApplyExperimentPayload(experimentKey);
            WakeWorker();
            return result;
        }

        public bool TrackExperimentExposure(string experimentKey)
        {
            string ignored;
            return TrackExperimentExposure(experimentKey, out ignored);
        }

        /// <summary>Record an additional application of the cached assignment.
        /// Returns after local capture; CP sealing and analytics delivery are asynchronous.</summary>
        public bool TrackExperimentExposure(string experimentKey, out string? error)
        {
            error = null;
            lock (_gate)
            {
                if (IntakeClosed)
                {
                    error = "shutdown";
                    return false;
                }
            }

            if (_experiments == null)
            {
                error = "experiments_not_configured";
                return false;
            }

            var recorded = _experiments.TrackExposure(experimentKey, out error);
            if (recorded) WakeWorker();
            return recorded;
        }

        public bool TrackExperimentOutcome(string experimentKey, string outcomeKey, double outcomeValue)
        {
            string ignored;
            return TrackExperimentOutcome(experimentKey, outcomeKey, outcomeValue, out ignored);
        }

        /// <summary>Record a safe-integer outcome for the last explicit application
        /// in the current session. CP sealing and analytics delivery are asynchronous.</summary>
        public bool TrackExperimentOutcome(string experimentKey, string outcomeKey, double outcomeValue, out string? error)
        {
            error = null;
            lock (_gate)
            {
                if (IntakeClosed)
                {
                    error = "shutdown";
                    return false;
                }
            }

            if (_experiments == null)
            {
                error = "experiments_not_configured";
                return false;
            }

            var recorded = _experiments.TrackOutcome(experimentKey, outcomeKey, outcomeValue, out error);
            if (recorded) WakeWorker();
            return recorded;
        }

        internal ShardPilotExperiments ExperimentsForTesting
        {
            get { return _experiments; }
        }

        /// <summary>Called by the facade when this client is REPLACED: the
        /// experiments automatic lane (revalidation fetches) stops
        /// immediately, so the detached client cannot rewrite the shared
        /// assignment cache with a decision the successor already
        /// superseded. Owed housekeeping keeps running for the background
        /// close's final drain.</summary>
        internal void DisownExperimentsForHandoff()
        {
            _fetchCallbacksRetired = true;
            if (_experiments != null)
            {
                _experiments.DisownAutomaticLane();
            }
        }

        /// <summary>Logs the replaced-client warning a disown owes, once
        /// (#159). Called outside the handoff's gate, after the successor has
        /// adopted this client's parked debts.</summary>
        internal void EmitReplacedNoticeIfOwed()
        {
            if (_experiments != null)
            {
                _experiments.EmitReplacedNoticeIfOwed();
            }
        }

        /// <summary>Hands a displaced predecessor's parked DESTRUCTIVE
        /// experiment cache debts (authoritative drops, the owed
        /// whole-record clear) to THIS client's consumer, whose own retry
        /// machinery re-arms and converges them — the facade (and the
        /// direct-construction registration handoff) calls this right after
        /// constructing the successor. Constructive write debts were
        /// cancelled at disown (the ownership rule); destructive debts are
        /// server verdicts the kill rule still owes the disk. A FLAG-OFF
        /// successor constructs no consumer to adopt them, so the parked
        /// drops persist as durable per-key tombstones instead (and the
        /// condemnation marker is re-written for a parked clear whose
        /// original marker write failed): a future enabled construction
        /// re-arms and converges them — otherwise re-enabling experiments
        /// later would restore and serve a stale assignment whose
        /// 404/not-assigned/permanent-400 drop was owed.</summary>
        internal void AdoptExperimentDebtsFromPredecessor(ShardPilotClient previous)
        {
            var debts = previous?._experiments ?? previous?._withdrawalDebts;
            if (debts == null)
            {
                return;
            }

            if (_experiments == null)
            {
                if (_withdrawalDebts == null) _withdrawalDebts = debts;
                else if (!ReferenceEquals(_withdrawalDebts, debts)) _withdrawalDebts.AdoptDestructiveDebtsFrom(debts);
                _withdrawalDebts.WithdrawalDriver = ConvergeWithdrawal;
                _withdrawalDebts.ParkDestructiveDebtsDurably();
                return;
            }

            _experiments.AdoptDestructiveDebtsFrom(debts);
        }

        /// <summary>
        /// The experiments consumer's sentinel purge: a real-subjects
        /// condemnation withdrew a scope's subject-fact keys, and facts
        /// carrying them that were already ACCEPTED into this pipeline must
        /// not ship on the next flush. Queue and restored copies are removed
        /// here — settled under the gate (so a racing persist cannot
        /// re-spool them) and counted dropped; the worker-held batch is
        /// evicted by the WORKER — at its next cycle, and again under the
        /// gate at the publish's own payload build, so a sentinel landing
        /// mid-publish still keeps the withdrawn fact out of the serialized
        /// batch (the batch list is only ever mutated on the worker's own
        /// flow, never from this thread); spool copies are removed by event
        /// id, which also covers copies of events still held in the batch.
        /// </summary>
        internal bool PurgeCondemnedExperimentFacts(List<string> eventIds)
        {
            return JournalFactPurge(eventIds);
        }

        /// <summary>The purge above; <paramref name="discoverLocked"/>, when
        /// given, supplies the ids under the SAME gate hold that condemns
        /// them, so no worker cycle can hand a discovered fact to the
        /// transport in between. Returns true when the withdrawn ids'
        /// pending-purge marker is durable, or nothing needed one (no id, no
        /// spool).</summary>
        private bool PurgeCondemnedExperimentFacts(List<string> eventIds, Func<List<string>> discoverLocked, bool memoryOnly = false)
        {
            if (discoverLocked == null && (eventIds == null || eventIds.Count == 0))
            {
                return true;
            }

            List<ShardPilotEvent> removed;
            List<ShardPilotEvent> stubs = null;
            lock (_gate)
            {
                if (discoverLocked != null)
                {
                    eventIds = discoverLocked();
                    if (eventIds.Count == 0)
                    {
                        return true;
                    }
                }

                // The synchronous queue/restored extraction runs against
                // the FULL withdrawal — a transient set, never the bounded
                // index — so a single sentinel withdrawing more ids than
                // the index cap still removes EVERY in-memory copy; the cap
                // must never silently spare the excess.
                var withdrawal = new HashSet<string>(StringComparer.Ordinal);
                for (var i = 0; i < eventIds.Count; i++)
                {
                    if (!string.IsNullOrEmpty(eventIds[i]))
                    {
                        withdrawal.Add(eventIds[i]);
                    }
                }

                removed = ExtractCondemnedLocked(_queue, withdrawal);
                var restoredRemoved = ExtractCondemnedLocked(_restoredPending, withdrawal);
                if (restoredRemoved != null)
                {
                    if (removed == null)
                    {
                        removed = restoredRemoved;
                    }
                    else
                    {
                        removed.AddRange(restoredRemoved);
                    }
                }

                if (removed != null && removed.Count > 0)
                {
                    _stats.RecordDropped(removed.Count);
                    RecordSettlementLocked(removed);
                    for (var i = 0; i < removed.Count; i++)
                    {
                        if (!string.IsNullOrEmpty(removed[i].Id))
                        {
                            withdrawal.Remove(removed[i].Id);
                        }
                    }
                }

                // What remains was NOT queue-resident: either it is held in
                // the worker's batch — evicted at the worker's next cycle,
                // and kept out of persist snapshots, via the index — or it
                // has no in-memory copy at all (delivered, ambiguous, or
                // never enqueued; the spool stubs below still cover disk).
                // The bounded index takes the remainder up to its cap; past
                // the cap, ids VERIFIED batch-resident go to the overflow
                // set the worker eviction and snapshot guard also consult,
                // so no condemned in-memory fact outlives the cap.
                HashSet<string> batchResident = null;
                foreach (var id in withdrawal)
                {
                    if (_condemnedFactIds.Count < MaxCondemnedFactIds)
                    {
                        _condemnedFactIds.Add(id);
                        continue;
                    }

                    if (batchResident == null)
                    {
                        batchResident = new HashSet<string>(StringComparer.Ordinal);
                        for (var i = 0; i < _workerBatch.Count; i++)
                        {
                            var held = _workerBatch[i];
                            if (held != null && !string.IsNullOrEmpty(held.Id))
                            {
                                batchResident.Add(held.Id);
                            }
                        }
                    }

                    if (batchResident.Contains(id))
                    {
                        _condemnedBatchOverflow.Add(id);
                    }
                }


            }

            if (memoryOnly) return false;

            if (discoverLocked != null)
            {
                ExperimentKeyWithdrawalPostScanHookForTesting?.Invoke();
            }

            FactPurgePostGateHookForTesting?.Invoke();

            // The journal receipt requires the actual removal and a quiescent append boundary.
            var spool = _spool;
            if (spool == null)
            {
                // No spool = no disk copies a crash could resend, and no
                // debt to record: the in-memory withdrawal above is the
                // whole job.
                return true;
            }

            stubs = PurgeStubs(eventIds);

            // Read BEFORE the removal below: a counted append may have
            // snapshotted these events before this sentinel's in-memory
            // eviction indexed them, and its unlocked spool append can land
            // AFTER the removal — writing a condemned fact back to disk. If
            // the marker retired now, a crash before the worker's batch
            // eviction would leave the next launch nothing to filter the
            // restored fact with, and the withdrawn subject-fact key would
            // resend. So while any counted append is in flight, the removal
            // still runs (idempotent) but the retirement is DEFERRED — the
            // completing append re-runs the marker-driven removal and only
            // that confirmed post-append pass retires the marker. Reading
            // the counter FIRST keeps the zero case safe too: an append
            // that completed before this read has its write on disk, so the
            // removal below covers it, and an append that STARTS after it
            // snapshots post-eviction state the index already filters.
            bool deferRetirement;
            lock (_gate)
            {
                deferRetirement = _spoolAppendsInFlight > 0;
            }

            var confirmed = SpoolRemove(stubs) && !deferRetirement;
            if (confirmed)
                lock (_gate)
                    foreach (var id in eventIds)
                        if (!_workerBatch.Exists(ev => ev.Id == id))
                        { _condemnedFactIds.Remove(id); _condemnedBatchOverflow.Remove(id); }
            return confirmed;
        }

        /// <summary>
        /// The experiments consumer's per-experiment withdrawal (an
        /// <c>age_ineligible</c> refusal): the refused subject's
        /// already-ACCEPTED facts for ONE experiment must not ship. Facts
        /// match on the exposure/outcome event name, the
        /// <c>experiment_key</c> prop and an <c>assignment_key</c> prop among
        /// the given fact keys — no known key matches nothing, since facts
        /// name their subject only by that key. Facts a later application
        /// emitted (<paramref name="emittedAfterWithdrawal"/>) are spared.
        /// Every in-memory copy is located — the live queue, the restored
        /// backlog, the worker's held batch (experiment facts never overflow
        /// to a disk-only spool copy) — and condemned through the sentinel
        /// purge above within the SAME gate hold: queue and restored copies
        /// are removed now, held-batch copies at the worker's next cycle (a
        /// batch already on the wire is wire-ambiguous: it is not re-sent
        /// after a failure, and nothing claims it was withdrawn), and spool
        /// copies by id under the durable pending-purge record, so a
        /// relaunch cannot restore them.
        /// </summary>
        internal bool WithdrawExperimentKeyFacts(
            string experimentKey, IList<string> subjectFactKeys, Func<string, bool> emittedAfterWithdrawal,
            Action<IList<string>> selectedForPurge)
        {
            if (string.IsNullOrEmpty(experimentKey) || subjectFactKeys == null)
            {
                return true;
            }

            var durable = PurgeCondemnedExperimentFacts(null, () =>
            {
                var ids = new List<string>();
                CollectExperimentKeyFactIds(_queue, experimentKey, subjectFactKeys, emittedAfterWithdrawal, ids);
                CollectExperimentKeyFactIds(_restoredPending, experimentKey, subjectFactKeys, emittedAfterWithdrawal, ids);
                CollectExperimentKeyFactIds(_workerBatch, experimentKey, subjectFactKeys, emittedAfterWithdrawal, ids);
                ids.AddRange(_ageWithdrawnAtHandoff);
                _ageWithdrawnAtHandoff.Clear();
                selectedForPurge?.Invoke(ids);
                return ids;
            });

            // The withdrawn facts no longer hold their keys; a fact on the
            // wire still does until its batch settles.
            PruneExperimentFactKeys();
            return durable;
        }

        /// <summary>
        /// The liveness prune of the experiments consumer's fact-key history
        /// and per-key withdrawal generations: collects the (experiment key,
        /// fact key) of every experiment fact the pipeline holds — the live
        /// queue, the restored backlog, and the worker's held or in-flight
        /// batch — and hands them to the consumer within the SAME gate hold
        /// (gate → consumer lock, the existing order), so no fact can be
        /// enqueued between the collection and the consumer's pass. The
        /// offline spool needs no scan of its own: experiment facts never
        /// overflow to a disk-only copy, a launch restores the whole spool
        /// into the backlog before the first prune, and every spooled copy
        /// is a copy of a fact those holders still have until it settles.
        /// The durable write of a shrunk history runs after the gate is
        /// released. A closing client leaves the shared record alone.
        /// </summary>
        internal void PruneExperimentFactKeys()
        {
            var experiments = _experiments;
            if (experiments == null || !experiments.HasFactKeyStateToPrune)
            {
                return;
            }

            lock (_gate)
            {
                if (_closed || _closeRequested)
                {
                    return;
                }

                var live = new HashSet<string>(StringComparer.Ordinal);
                CollectExperimentFactKeyTokens(_queue, live);
                CollectExperimentFactKeyTokens(_restoredPending, live);
                CollectExperimentFactKeyTokens(_workerBatch, live);
                experiments.PruneFactKeyState(live);
            }

            experiments.FlushPrunedFactKeyHistory();
        }

        private static void CollectExperimentFactKeyTokens(IEnumerable<ShardPilotEvent> source, HashSet<string> live)
        {
            foreach (var ev in source)
            {
                ShardPilotRestoredFact fact;
                if (TryDescribeRestoredExperimentFact(ev, out fact) &&
                    fact.ExperimentKey != null && fact.AssignmentKey != null)
                {
                    live.Add(ShardPilotExperiments.FactKeyToken(fact.ExperimentKey, fact.AssignmentKey));
                }
            }
        }

        /// <summary>Test seam: runs after a per-experiment withdrawal's
        /// gate hold (discovery and in-memory condemnation), before its
        /// spool phase. Never assigned in production.</summary>
        internal Action ExperimentKeyWithdrawalPostScanHookForTesting;

        /// <summary>Test seam: runs after every fact purge's gate hold,
        /// before its spool removal. Never assigned in production.</summary>
        internal Action FactPurgePostGateHookForTesting;

        /// <summary>Ids the transport-handoff check evicted: facts no longer
        /// in memory whose spool copies the next per-experiment withdrawal
        /// scan still has to remove. Guarded by <c>_gate</c>.</summary>
        private readonly HashSet<string> _ageWithdrawnAtHandoff = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>The handoff half of an age withdrawal: between the
        /// consumer recording the withdrawal and its pipeline scan, the
        /// worker could otherwise hand a fact the withdrawal covers to the
        /// transport. A fact whose source predates a recorded withdrawal
        /// matching its experiment and subject is evicted here instead —
        /// counted dropped once, like the scan would — and its id is kept
        /// for that scan's spool removal. Caller must hold <c>_gate</c>
        /// (gate → consumer lock, the existing order).</summary>
        private void EvictAgeWithdrawnFactsLocked(List<ShardPilotEvent> batch)
        {
            var experiments = _experiments;
            if (experiments == null || batch.Count == 0 || !experiments.HasAgeWithdrawals)
            {
                return;
            }

            List<ShardPilotEvent> evicted = null;
            for (var i = batch.Count - 1; i >= 0; i--)
            {
                ShardPilotRestoredFact fact;
                if (TryDescribeRestoredExperimentFact(batch[i], out fact) &&
                    experiments.IsAgeWithdrawnFact(fact.EventId, fact.ExperimentKey, fact.AssignmentKey))
                {
                    if (evicted == null)
                    {
                        evicted = new List<ShardPilotEvent>();
                    }

                    evicted.Add(batch[i]);
                    _ageWithdrawnAtHandoff.Add(fact.EventId);
                    batch.RemoveAt(i);
                }
            }

            if (evicted != null)
            {
                _stats.RecordDropped(evicted.Count);
                RecordSettlementLocked(evicted);
                if (batch.Count == 0)
                {
                    _publishBackoffAttempts = 0;
                }
            }
        }

        private static List<ShardPilotEvent> PurgeStubs(List<string> eventIds)
        {
            var stubs = new List<ShardPilotEvent>(eventIds.Count);
            for (var i = 0; i < eventIds.Count; i++)
            {
                if (!string.IsNullOrEmpty(eventIds[i]))
                {
                    stubs.Add(new ShardPilotEvent { Id = eventIds[i] });
                }
            }

            return stubs;
        }

        /// <summary>Records the ids in the durable pending-purge marker;
        /// false leaves every id owed in memory for the persist-barrier
        /// retry (see the purge above).</summary>
        private bool MarkPendingCondemnedFactIds(List<ShardPilotEvent> stubs)
        {
            bool marked;
            lock (CondemnedFactsStoreGate)
            {
                // An UNREADABLE marker fails closed exactly like an
                // unwritable one: overwriting debt this process cannot see
                // would erase a previous run's still-owed removals, so the
                // new ids go to the in-memory debt and the removal is
                // withheld until the store heals.
                HashSet<string> pending;
                marked = TryLoadPendingCondemnedFactIds(out pending);
                if (marked)
                {
                    for (var i = 0; i < stubs.Count; i++)
                    {
                        pending.Add(stubs[i].Id);
                    }

                    marked = SavePendingCondemnedFactIds(pending);
                }

                if (!marked)
                {
                    // EVERY id stays owed — the debt is bounded by what the
                    // pipeline actually held, and capping it would let the
                    // healed retry persist and remove only a subset while
                    // the excess withdrawn facts sit in the spool with no
                    // durable marker for the next launch to honor.
                    for (var i = 0; i < stubs.Count; i++)
                    {
                        _condemnedPurgeDebt.Add(stubs[i].Id);
                    }
                }
            }

            return marked;
        }

        /// <summary>Appends the ids of the events in
        /// <paramref name="source"/> that are facts of the given experiment
        /// and of one of the given subjects, live events and restored
        /// envelopes alike, except those a later application emitted.</summary>
        private static void CollectExperimentKeyFactIds(
            IEnumerable<ShardPilotEvent> source,
            string experimentKey,
            IList<string> subjectFactKeys,
            Func<string, bool> emittedAfterWithdrawal,
            List<string> ids)
        {
            foreach (var ev in source)
            {
                ShardPilotRestoredFact fact;
                if (!TryDescribeRestoredExperimentFact(ev, out fact) ||
                    !string.Equals(fact.ExperimentKey, experimentKey, StringComparison.Ordinal))
                {
                    continue;
                }

                if (fact.AssignmentKey == null || !subjectFactKeys.Contains(fact.AssignmentKey))
                {
                    continue;
                }

                if (emittedAfterWithdrawal != null && emittedAfterWithdrawal(fact.EventId))
                {
                    continue;
                }

                ids.Add(fact.EventId);
            }
        }

        /// <summary>Persist-barrier and append-completion retry for the
        /// sentinel purge debt: saves a marker that could not land at purge
        /// time, then re-attempts the spool removal for everything known,
        /// clearing only what THIS spool confirms — and, while any counted
        /// spool append is still in flight, deferring the marker retirement
        /// exactly like the live purge does. The removal is
        /// MARKER-INDEPENDENT here too (removal-first, the live purge's
        /// rule): a marker load or save that STILL fails keeps the debt
        /// owed for the marker but never withholds the idempotent removal —
        /// the counted-append completion runs through this method, and a
        /// withheld removal there would leave the copy the append just
        /// wrote back on disk with no durable record: a quit before the
        /// store heals would hand the withdrawn fact to the next launch to
        /// restore and publish. What leaves the disk needs no marker; the
        /// marker exists for the copies that REMAIN, and the retained debt
        /// keeps retrying it. Only the RETIREMENT stays marker-gated.</summary>
        private void RetryCondemnedFactPurgeDebt()
        { RetryWithdrawalFrames(); }

        /// <summary>Reads the durable pending-purge marker. Returns FALSE
        /// when the settings store cannot be read — and unreadable is NOT
        /// empty: the marker is the only crash-safe record of which
        /// withdrawn ids must be filtered at relaunch, so every caller
        /// fails CLOSED on false (the load-time filter conservatively
        /// drops restored experiment facts, writers withhold rather than
        /// overwrite debt they cannot see).</summary>
        private bool TryLoadPendingCondemnedFactIds(out HashSet<string> pending)
        {
            pending = new HashSet<string>(StringComparer.Ordinal);
            var key = CondemnedFactsKey;
            if (key == null)
            {
                return true;
            }

            try
            {
                var stored = _settingsStore.Get(key);
                if (!string.IsNullOrEmpty(stored))
                {
                    var parts = stored.Split('\n');
                    for (var i = 0; i < parts.Length; i++)
                    {
                        if (!string.IsNullOrEmpty(parts[i]))
                        {
                            pending.Add(parts[i]);
                        }
                    }
                }
            }
            catch
            {
                return false;
            }

            return true;
        }

        /// <summary>Returns true only when the record is durably down —
        /// callers FAIL CLOSED on false (the removal is withheld and
        /// retried).</summary>
        private bool SavePendingCondemnedFactIds(HashSet<string> pending)
        {
            var key = CondemnedFactsKey;
            if (key == null)
            {
                return true;
            }

            try
            {
                if (pending == null || pending.Count == 0)
                {
                    _settingsStore.Set(key, string.Empty);
                    return true;
                }

                // Every id persists — the marker is bounded by what the
                // pipeline actually held (queue + batch + spool), and a
                // truncated marker would quietly leave the excess ids'
                // spool copies restorable at the next launch.
                var ordered = new List<string>(pending);
                ordered.Sort(StringComparer.Ordinal);
                _settingsStore.Set(key, string.Join("\n", ordered.ToArray()));
                return true;
            }
            catch
            {
                return false;
            }
        }

        private void ClearConfirmedCondemnedFactIds(List<ShardPilotEvent> confirmed)
        {
            lock (CondemnedFactsStoreGate)
            {
                HashSet<string> pending;
                if (!TryLoadPendingCondemnedFactIds(out pending))
                {
                    // Unreadable fails closed: rewriting a record this
                    // process cannot see could erase debt beyond the
                    // confirmed ids. The next readable pass settles it.
                    return;
                }

                var changed = false;
                for (var i = 0; i < confirmed.Count; i++)
                {
                    changed |= pending.Remove(confirmed[i].Id);
                }

                if (changed)
                {
                    SavePendingCondemnedFactIds(pending);
                }
            }
        }

        /// <summary>Honors the durable pending-purge record at spool load: a
        /// previous run's sentinel purge may have died before the spool
        /// rewrite removed the withdrawn facts, and restored copies must not
        /// ride this launch's flushes. Dropped here, and the removal is
        /// re-attempted until the spool confirms.</summary>
        private void FilterCondemnedRestoredFactsAtLoad()
        {
            bool known; var frames = ReadWithdrawalFrames(out known);
            if (!known) { DropRestoredExperimentFactsForUnreadableMarker(); return; }
            var ids = new HashSet<string>(StringComparer.Ordinal);
            lock (_gate)
                foreach (var ev in _restoredPending)
                {
                    ShardPilotRestoredFact fact; if (!TryDescribeRestoredExperimentFact(ev, out fact)) continue;
                    foreach (var frame in frames)
                    {
                        WithdrawalKind kind; ShardPilotWithdrawalForm.TryKind(frame, out kind);
                        if (frame.Pending.Contains(ev.Id) || ((int)kind < 2 && frame.Experiment == fact.ExperimentKey &&
                            frame.Keys.Contains(fact.AssignmentKey) && !frame.Spared.Contains(ev.Id))) ids.Add(ev.Id);
                    }
                }
            if (ids.Count > 0) PurgeCondemnedExperimentFacts(new List<string>(ids), null);
        }

        /// <summary>The fail-closed path for an UNREADABLE pending-purge
        /// marker at load: the marker is the only crash-safe record of
        /// which restored ids carry withdrawn subject-fact keys, so with it
        /// unreadable ANY restored experiment fact may be one whose spool
        /// removal was owed. Publishing a withdrawn fact is the integrity
        /// violation; conservatively dropping a live one costs a fact —
        /// so every restored experiment fact is dropped, their spool
        /// copies removed best-effort, and the marker itself is left
        /// UNTOUCHED: the debt it may record stays owed (never overwritten
        /// blind) until the store reads again or a healthy later purge
        /// rewrites it.</summary>
        private void DropRestoredExperimentFactsForUnreadableMarker()
        {
            List<ShardPilotEvent> dropped = null;
            lock (_gate)
            {
                if (_restoredPending.Count == 0)
                {
                    return;
                }

                var keep = new List<ShardPilotEvent>(_restoredPending.Count);
                while (_restoredPending.Count > 0)
                {
                    var ev = _restoredPending.Dequeue();
                    if (IsExperimentFactEvent(ev))
                    {
                        if (dropped == null)
                        {
                            dropped = new List<ShardPilotEvent>();
                        }

                        dropped.Add(ev);
                    }
                    else
                    {
                        keep.Add(ev);
                    }
                }

                for (var i = 0; i < keep.Count; i++)
                {
                    _restoredPending.Enqueue(keep[i]);
                }

                if (dropped != null)
                {
                    _stats.RecordDropped(dropped.Count);
                }
            }

            if (dropped == null)
            {
                return;
            }

            SafeLog(
                "shardpilot experiment facts dropped fail-closed: pending-purge marker unreadable ({0})",
                dropped.Count);
            var spool = _spool;
            if (spool != null)
            {
                var stubs = new List<ShardPilotEvent>(dropped.Count);
                for (var i = 0; i < dropped.Count; i++)
                {
                    if (!string.IsNullOrEmpty(dropped[i].Id))
                    {
                        stubs.Add(new ShardPilotEvent { Id = dropped[i].Id });
                    }
                }

                if (stubs.Count > 0)
                {
                    SpoolRemove(stubs);
                }
            }
        }

        /// <summary>Removes the listed condemned facts from one pending
        /// queue, preserving the order of everything kept. The id set is
        /// the caller's — the purge passes the FULL withdrawal (never the
        /// bounded index, whose cap must not spare an excess id), the
        /// load-time filter passes the whole persisted marker. Caller must
        /// hold <c>_gate</c>.</summary>
        private List<ShardPilotEvent> ExtractCondemnedLocked(Queue<ShardPilotEvent> queue, HashSet<string> condemned)
        {
            if (condemned.Count == 0 || queue.Count == 0)
            {
                return null;
            }

            List<ShardPilotEvent> removed = null;
            var keep = new List<ShardPilotEvent>(queue.Count);
            while (queue.Count > 0)
            {
                var ev = queue.Dequeue();
                if (ev != null && !string.IsNullOrEmpty(ev.Id) && condemned.Contains(ev.Id))
                {
                    if (removed == null)
                    {
                        removed = new List<ShardPilotEvent>();
                    }

                    removed.Add(ev);
                }
                else
                {
                    keep.Add(ev);
                }
            }

            for (var i = 0; i < keep.Count; i++)
            {
                queue.Enqueue(keep[i]);
            }

            return removed;
        }

        /// <summary>Worker-side half of the sentinel purge: evicts condemned
        /// facts from the HELD batch, before the next publish — a retained
        /// batch behind backoff is scrubbed at its next wake whether or not
        /// it tops up. Caller must hold <c>_gate</c>.</summary>
        private void EvictCondemnedFactsLocked(List<ShardPilotEvent> batch)
        {
            if ((_condemnedFactIds.Count == 0 && _condemnedBatchOverflow.Count == 0) || batch.Count == 0)
            {
                return;
            }

            List<ShardPilotEvent> evicted = null;
            for (var i = batch.Count - 1; i >= 0; i--)
            {
                var ev = batch[i];
                if (ev != null && !string.IsNullOrEmpty(ev.Id) &&
                    (_condemnedFactIds.Contains(ev.Id) || _condemnedBatchOverflow.Contains(ev.Id)))
                {
                    if (evicted == null)
                    {
                        evicted = new List<ShardPilotEvent>();
                    }

                    evicted.Add(ev);
                    batch.RemoveAt(i);
                }
            }

            if (evicted != null)
            {
                _stats.RecordDropped(evicted.Count);
                RecordSettlementLocked(evicted);
                for (var i = 0; i < evicted.Count; i++)
                {
                    if (!string.IsNullOrEmpty(evicted[i].Id))
                    {
                        _condemnedFactIds.Remove(evicted[i].Id);
                        _condemnedBatchOverflow.Remove(evicted[i].Id);
                    }
                }

                if (batch.Count == 0)
                {
                    // The failure streak belonged to the withdrawn batch: a
                    // fresh post-purge batch must start at the flush
                    // cadence, not deep in a schedule the condemned facts
                    // earned (same rationale as the consent-denial discard).
                    _publishBackoffAttempts = 0;
                }
            }
        }

        /// <summary>Copies events into a snapshot list, skipping condemned
        /// facts: a spool insurance append or a persist snapshot must not
        /// put a withdrawn fact (back) on disk. Caller must hold
        /// <c>_gate</c>.</summary>
        private void AddUncondemnedLocked(List<ShardPilotEvent> target, IEnumerable<ShardPilotEvent> source)
        {
            foreach (var ev in source)
            {
                if (ev != null && !string.IsNullOrEmpty(ev.Id) &&
                    ((_condemnedFactIds.Count > 0 && _condemnedFactIds.Contains(ev.Id)) ||
                     (_condemnedBatchOverflow.Count > 0 && _condemnedBatchOverflow.Contains(ev.Id))))
                {
                    continue;
                }

                target.Add(ev);
            }
        }

        internal string ExperimentsSubjectIdForTesting
        {
            get { return _experimentsSubjectId; }
        }

        /// <summary>The identity of the settings-store backend where the
        /// experiment SUBJECT-ID key physically lives, for the consumer's
        /// process-wide mint gate: the spcid_ key is GLOBAL to the store
        /// while cache paths are per-config, so two live clients sharing
        /// one store must share ONE mint gate even when their configs
        /// resolve DIFFERENT cache paths (a direct replacement changing
        /// SpoolDir) — different gates would let both reload an
        /// empty store and mint different subjects. For the Unity store
        /// that is its FILE side (the subject key is file-routed past the
        /// PlayerPrefs duality), whose canonical lower-invariant directory
        /// token also unifies with a raw file store over the same
        /// directory; any other identity-bearing store contributes its own
        /// token. Null for an identity-less or absent store — no shared
        /// backend is provable there, and the consumer falls back to its
        /// per-cache-path gate.</summary>
        private string ResolveSubjectIdStoreIdentity()
        {
            var unityStore = _settingsStore as ShardPilotUnityKeyValueStore;
            if (unityStore != null)
            {
                return unityStore.FileRoutedStoreIdentityToken;
            }

            var identity = _settingsStore as IShardPilotStoreIdentity;
            return identity == null ? null : identity.StoreIdentityToken;
        }

        /// <summary>The experiments durable-cache path: the spool's
        /// directory resolution (explicit SpoolDir, else the Unity
        /// persistent data path), with a dedicated file. Null when no
        /// directory is resolvable — the consumer then runs memory-only,
        /// exactly like a remote-config client with no cache path.</summary>
        private string ResolveExperimentsCachePath()
        {
            return ResolveExperimentsCachePath(_config);
        }

        /// <summary>Static flavor of <see cref="ResolveExperimentsCachePath()"/>
        /// for the PRE-CONSTRUCTION handoff, where no instance exists yet:
        /// resolves the cache path a (normalized) config maps to, so the
        /// handoff covers exactly the file the construction will load.</summary>
        private static string ResolveExperimentsCachePath(ShardPilotConfig config)
        {
            var directory = config.SpoolDir;
            if (string.IsNullOrEmpty(directory))
            {
                try
                {
                    directory = UnityEngine.Application.persistentDataPath;
                }
                catch
                {
                    return null;
                }
            }

            if (string.IsNullOrEmpty(directory))
            {
                return null;
            }

            try
            {
                // Canonicalize exactly like the spool's owner path: two
                // spellings of one directory (a "." segment, a relative
                // form) share ONE physical record file, and the static
                // fence/condemnation keys embed this path — differing
                // spellings would give sibling clients different keys over
                // the same file, so one client's sentinel could not stop
                // the other from serving the withdrawn scope.
                string canonical;
                try
                {
                    canonical = System.IO.Path.GetFullPath(directory);
                }
                catch
                {
                    // An uncanonicalizable directory still names the
                    // location (the spool helper's posture).
                    canonical = directory;
                }

                return System.IO.Path.Combine(canonical, "shardpilot.experiments.json");
            }
            catch
            {
                return null;
            }
        }

        // A refused owed start can retain an id if historical activity advanced
        // its state. Neither fact capture nor renewal may adopt that id as an
        // active session. Stay lock-free: the consumer reads this under its lock.
        private bool AdmitExperimentApplicationSession()
        {
            if (!AdmitCurrentSession()) return false;
            lock (_gate)
            {
                if (IntakeClosed || _consentState != ShardPilotConsentState.Granted ||
                    _config.Source != ShardPilotSource.Client || _sessionState.OwesStart || _owedSessionStartThreadId != 0)
                    return false;
                // Applying a treatment is current-session activity even before
                // the first ordinary event. Reserve its real session now.
                if (string.IsNullOrEmpty(_sessionState.Id))
                    _sessionState = new SessionState(ShardPilotIds.NewUuidV7(), 0, _sessionState.OwedStart);
                return true;
            }
        }

        private string CurrentExperimentSessionId()
        {
            var session = Volatile.Read(ref _sessionState);
            return session.OwesStart ? string.Empty : session.Id;
        }

        /// <summary>
        /// The shared enqueue path behind the experiment fact producers: the
        /// consent-first gate, the identity requirement, the lazy session,
        /// and the queue cap apply to facts exactly as to ordinary events.
        /// The two deviations the facts contract needs: a pre-derived
        /// deterministic event id (so at-least-once retries and same-session
        /// double emissions collapse server-side) and NO user_id on the
        /// envelope (the server 400s any value; the identity is the standard
        /// anonymous_id alone — never the experiments subject id). A full
        /// queue REFUSES the fact with a distinct retryable code instead of
        /// spilling to the overflow spool: the consumer's owed-exposure
        /// machinery owns the retry, and a spooled copy re-sent next launch
        /// would race it.
        /// </summary>
        private bool EnqueueSealedExperimentFact(ShardPilotSealedExperimentFact fact, string sessionId,
            string anonymousId, string sessionMarker, long consentEpoch, out string code)
        {
            code = null;
            ShardPilotEvent prepared;
            try
            {
                // The fact enqueues under the analytics identity it BELONGS
                // TO (captured when its application was armed): an owed
                // exposure draining after a session renewal or an identity
                // rotation keeps its own actor's envelope identity. Empty =
                // the pipeline attaches the current values below.
                prepared = PrepareEvent(new ShardPilotEvent
                {
                    Id = fact.EventId,
                    Name = fact.EventName,
                    TimestampUtc = DateTime.Parse(fact.EventTimestamp, System.Globalization.CultureInfo.InvariantCulture,
                        System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal),
                    SealedFact = fact,
                    Props = fact.Props,
                    SessionId = ShardPilotConfig.Trim(sessionId),
                    AnonymousId = ShardPilotConfig.Trim(anonymousId)
                });
            }
            catch (Exception ex)
            {
                _stats.RecordFailure(ex.Message);
                code = "invalid_event";
                return false;
            }

            prepared.UserId = string.Empty;
            if (string.IsNullOrEmpty(prepared.AnonymousId))
            {
                // The facts contract requires anonymous_id (erasure
                // reachability); without one the fact cannot ride at all.
                code = "identity_required";
                return false;
            }

            lock (_gate)
            {
                if (_closed && !_experimentFactDrainWindow)
                {
                    code = "shutdown";
                    return false;
                }

                if (_consentState != ShardPilotConsentState.Granted)
                {
                    code = ShardPilotConsent.IsDeniedState(_consentState)
                        ? ShardPilotConsent.DeniedError
                        : ShardPilotConsent.UnknownError;
                    _stats.RecordRefusal(code);
                    return false;
                }

                if (_consentDenialEpoch != consentEpoch)
                {
                    code = "consent_changed";
                    return false;
                }

                // An end can land after a host fact's admission and before its
                // session capture. Refuse that empty capture retryably: starting
                // here would be too late for its exposure marker/arm. An active
                // claim also reserves attachment before staging begins. Historical
                // facts keep their captured session and lifecycle identity.
                // A session-start renewal is STAGED (an overflow commit
                // awaiting its provisional append verdict) and this fact
                // resolved NO envelope session id: on a FIRST session the
                // consumer's latch has no pre-start id to serve, and the
                // lazy attach below would bind the fact to the PENDING
                // _sessionState.Id — a session the SDK may yet refuse and roll
                // back. Refuse retryably instead (checked HERE, under the
                // same gate the staging and its verdict run under, so the
                // consumer-side resolution cannot race the latch); a
                // renewal-window fact carries the latched pre-start id and
                // passes through, and owed sweeps are already latched.
                if (string.IsNullOrEmpty(prepared.SessionId) &&
                    (_sessionState.OwesStart || _owedSessionStartThreadId != 0 ||
                     (_experiments != null && _experiments.HasStagedSessionRenewal())))
                {
                    code = "session_start_pending";
                    return false;
                }

                var attached = PreviewAttachedSession(prepared, true, _sessionState);
                if (!AdmitEventSize(prepared, retain: false, experiment: true))
                {
                    code = ShardPilotExperimentApply.EnvelopeTooLarge;
                    return false;
                }
                if (_queue.Count >= _config.BufferSize)
                {
                    // Pressure retains a valid fact; an impossible size is terminal
                    // even while full, so it cannot block the owed FIFO forever.
                    code = "queue_full";
                    return false;
                }
                _sessionState = attached;
                StampAnalyticsIntake(prepared);
                prepared.CloseOwner = _closeCompletion;
                prepared.IntakeSequence = ++_intakeSequence;
                _queue.Enqueue(prepared);
                if (_experiments != null && _experiments.IsCurrentSessionMarker(sessionMarker))
                {
                    RecordSessionAcceptanceLocked(prepared, CurrentEventLifecycleLocked(prepared));
                }
                _stats.RecordEnqueued();
                if (CrashCollectionAllowedLocked)
                {
                    _breadcrumbs.Record(prepared.Name);
                }
            }

            WakeWorker();
            return true;
        }

        /// <summary><paramref name="onEnqueued"/> commits the caller's
        /// session transition; its argument says whether the commit is
        /// PROVISIONAL (the overflow-to-spool path — acceptance still
        /// pending, a refused append rolls the mutation back) or final (the
        /// queued path — acceptance certain). Its return value is the
        /// experiments-renewal STAKE the provisional commit staged (0 =
        /// none): the overflow context carries it so the append verdict
        /// settles exactly the stage this operation owns.</summary>
        private bool EnqueuePrepared(ShardPilotEvent prepared, bool attachCurrentSession, Func<bool, long> onEnqueued,
            SessionState owedSession = null, long? expectedConsentEpoch = null, ShardPilotEvent replacementEnd = null,
            bool closing = false, Action<string> refusal = null)
        {
            OverflowContext overflow = null;
            ShardPilotEvent replacedEnd = null;
            lock (_gate)
            {
                if ((owedSession != null && !WaitForSessionStartLocked()) || (IntakeClosed && !closing))
                {
                    return false;
                }

                // An end claimed before consent loss belongs to the withdrawn
                // session even if a new grant arrived while it was preparing.
                if (expectedConsentEpoch.HasValue && expectedConsentEpoch.Value != _consentDenialEpoch)
                {
                    _stats.RecordRefusal(ShardPilotConsent.DeniedError);
                    return false;
                }

                // CONSENT-FIRST: only an explicit grant opens the pipeline.
                // Unknown (no decision yet, or an unreadable decision) and
                // Denied both refuse the event right here — it is DROPPED at
                // enqueue, never queued, never spooled, and no analytics
                // bytes leave the device. The refusal reason is surfaced as
                // a distinct code through ShardPilotStats.LastError.
                if (_consentState != ShardPilotConsentState.Granted)
                {
                    // Both denied flavors (chosen and forced-minor) refuse
                    // with the SAME consent_denied reason.
                    _stats.RecordRefusal(
                        ShardPilotConsent.IsDeniedState(_consentState)
                            ? ShardPilotConsent.DeniedError
                            : ShardPilotConsent.UnknownError);
                    return false;
                }

                // Sequence-only attachment preserves lifecycle identity. Only a
                // completed, accepted start can supersede the owed claim with a
                // usable replacement; an id alone may be lazy or provisional.
                if (owedSession != null && !ReferenceEquals(_sessionState.Lifecycle, owedSession.Lifecycle))
                {
                    return _sessionState.Lifecycle.StartAccepted && !_sessionState.OwesStart &&
                        !string.IsNullOrEmpty(_sessionState.Id);
                }

                var attached = PreviewAttachedSession(prepared, attachCurrentSession, _sessionState);
                if (prepared.Name == "app.session_started") CarryLifecycleTransitionsLocked(prepared);
                if (!AdmitEventSize(prepared, refusal)) return false;
                ClaimReplacedSessionEndLocked(replacementEnd, prepared.TimestampUtc, null,
                    out var endRefused, refusal, preview: true);
                if (endRefused) return false;

                // Every admission while close prepares/drains belongs to that
                // operation: host exit fences fresh intake before _closed,
                // while owned facts and internal closing paths still drain.
                prepared.CloseOwner = _closeCompletion;

                if (_queue.Count >= _config.BufferSize)
                {
                    if (_spool == null)
                    {
                        // No spool (disabled/unavailable/WebGL): a full queue
                        // still drops, exactly as before.
                        _stats.RecordDropped(1);
                        return false;
                    }

                    // Queue-overflow spills to the offline spool instead of
                    // dropping (a sustained outage fills the queue — losing
                    // precisely the events the spool exists to protect). The
                    // event is DISK-ONLY: it is not queued, and it ships on
                    // the NEXT LAUNCH through the normal restore path. The
                    // session is attached AND its transition committed (the
                    // onEnqueued session-start effects) here, in this same
                    // lock hold — matching the queued path's ordering. A
                    // concurrent event during the unlocked disk write below
                    // could otherwise advance the session first and then be
                    // clobbered by a late commit carrying older values. The
                    // pre/post session state is snapshotted so a FAILED or
                    // raced append can roll the whole mutation back — a
                    // dropped event must not leave a lazily created session,
                    // an advanced sequence, or a committed session-start
                    // behind (the invariant the queued path keeps by
                    // attaching only once acceptance is certain), unless
                    // another event has advanced the session meanwhile.
                    overflow = new OverflowContext
                    {
                        Closing = closing,
                        EpochBefore = _consentDenialEpoch,
                        CrashEpochBefore = _crashDisableEpoch,
                        SessionBefore = _sessionState,
                        PauseBefore = _sessionPause,
                        ResumeGenerationBefore = _resumeGeneration
                    };
                    // The replaced session's end is staged with this provisional
                    // start: appended with it, and undone with it on refusal.
                    replacedEnd = ClaimReplacedSessionEndLocked(replacementEnd, prepared.TimestampUtc, overflow, out endRefused, refusal);
                    if (endRefused) return false;
                    if (replacedEnd != null)
                    {
                        StampAnalyticsIntake(replacedEnd);
                        replacedEnd.CloseOwner = prepared.CloseOwner;
                        overflow.ReplacedEnd = replacedEnd;
                    }
                    StampAnalyticsIntake(prepared);
                    _sessionState = attached;
                    if (onEnqueued != null)
                    {
                        overflow.RenewalStake = onEnqueued(true);
                    }

                    overflow.SessionAfter = _sessionState;
                    overflow.EventLifecycle = attachCurrentSession ? CurrentEventLifecycleLocked(prepared) : null;
                }
                else
                {
                    // Attach the session ONLY now that the event is certain to be
                    // enqueued. Doing this in PrepareEvent (before these checks) let a
                    // denied/closed/over-buffer event lazily create _sessionState.Id and
                    // advance _sessionState.Sequence, which a later accepted event — or a
                    // crash — would then inherit, binding it to session state created
                    // during a denied period.
                    //
                    // A start that replaces an open session ends it first, in this
                    // same hold and ahead of the start. It rides with the start's
                    // admission, so it can take the queue one event past its size.
                    replacedEnd = ClaimReplacedSessionEndLocked(replacementEnd, prepared.TimestampUtc, null, out endRefused, refusal);
                    if (endRefused) return false;
                    if (replacedEnd != null)
                    {
                        StampAnalyticsIntake(replacedEnd);
                        replacedEnd.CloseOwner = prepared.CloseOwner;
                        replacedEnd.IntakeSequence = ++_intakeSequence;
                        _queue.Enqueue(replacedEnd);
                        _stats.RecordEnqueued();
                        if (CrashCollectionAllowedLocked)
                        {
                            _breadcrumbs.Record(replacedEnd.Name);
                        }
                    }

                    _sessionState = attached;

                    StampAnalyticsIntake(prepared);
                    prepared.IntakeSequence = ++_intakeSequence;
                    _queue.Enqueue(prepared);
                    if (onEnqueued != null)
                    {
                        onEnqueued(false);
                    }
                    // A claimed end carries a historical ID. Even if a replacement
                    // reuses that ID, the end cannot advance its timestamp history.
                    if (attachCurrentSession)
                    {
                        RecordSessionAcceptanceLocked(prepared, CurrentEventLifecycleLocked(prepared));
                    }
                    _stats.RecordEnqueued();

                    // Recorded under _gate, the same lock the consent check above
                    // and SetConsent(false) use, so the breadcrumb trail can never
                    // disagree with consent: a concurrent denial either ran first
                    // (this event is rejected above and leaves no trace) or runs
                    // after (and clears this breadcrumb along with the queue).
                    // Breadcrumbs exist only for crash reports, so the crash
                    // plane's own opt-out gates the COLLECTION too: while crash
                    // reporting is disabled, nothing is recorded — a report
                    // emitted after a re-enable can never carry opt-out-period
                    // breadcrumbs (disabling also cleared the ring).
                    if (CrashCollectionAllowedLocked)
                    {
                        _breadcrumbs.Record(prepared.Name);
                    }
                }
            }

            if (overflow != null)
            {
                return SpoolOverflowEvent(prepared, overflow);
            }

            WakeWorker();
            return true;
        }

        /// <summary>
        /// Builds the end of the session a start is about to replace, from the
        /// session open in this gate hold, or returns null when none is open.
        /// An already expired background pause ends with <c>idle_timeout</c> at
        /// its deadline and is consumed; otherwise the reason is
        /// <c>session_start</c> at the start's time. The session state itself
        /// is left to the start's commit, which replaces it in this same hold.
        /// On the overflow path <paramref name="overflow"/> records what a
        /// refused append must restore. Caller must hold <c>_gate</c>.
        /// </summary>
        private ShardPilotEvent ClaimReplacedSessionEndLocked(ShardPilotEvent template, DateTime startedAtUtc,
            OverflowContext overflow, out bool refused, Action<string> refusal = null, bool preview = false)
        {
            refused = false;
            if (template == null || string.IsNullOrEmpty(_sessionState.Id))
            {
                return null;
            }

            // A session whose own start is still awaiting its append verdict
            // may yet be refused; ending it could send an end for a session
            // that never started.
            if (_sessionState.Lifecycle.PendingStartThreadId != 0)
            {
                return null;
            }

            var reason = "session_start";
            var endedAt = startedAtUtc;
            var pause = _sessionPause;
            var lifecycle = _sessionState.Lifecycle;
            var closesPause = false;
            if (_backgroundedAtUtc != default(DateTime) && pause != null &&
                ReferenceEquals(pause.Lifecycle, lifecycle))
            {
                var expiredEnd = ExpiredPauseEndLocked(pause);
                if (expiredEnd != default(DateTime))
                {
                    reason = "idle_timeout";
                    endedAt = expiredEnd;
                    if (endedAt < lifecycle.LatestAcceptedTimestampUtc)
                    {
                        endedAt = lifecycle.LatestAcceptedTimestampUtc;
                    }

                    closesPause = true;
                }
            }

            // Like an automatic end, the replacing end never precedes the
            // replaced session's last accepted event.
            if (endedAt < lifecycle.LatestAcceptedTimestampUtc)
            {
                endedAt = lifecycle.LatestAcceptedTimestampUtc;
            }

            RefreshLifecycleTransitionsLocked();
            var ended = template.Clone();
            // This is a new construction: none of the template's encoded bytes
            // can describe the session metadata and props chosen below.
            ended.SerializedEnvelope = ended.AdmittedEnvelope = ended.CapturedProps = null;
            ended.SessionId = _sessionState.Id;
            ended.SessionSequence = _sessionState.Sequence + 1;
            ended.TimestampUtc = endedAt;
            ended.Props = new Dictionary<string, object> { { "reason", reason } };
            StampLifecycleTransitionsLocked(ended, _lifecycleTransitions, _lifecycleRun);
            if (!AdmitEventSize(ended, refusal))
            {
                refused = true;
                return null;
            }
            // The clock was sampled for THIS end. Commit only after measuring
            // its exact bytes; do not re-evaluate the boundary after validation.
            if (!preview && closesPause)
            {
                if (overflow != null)
                {
                    overflow.AutomaticEndLifecycle = lifecycle;
                    overflow.AutomaticEndBefore = lifecycle.AutomaticEndUtc;
                }
                _sessionPause = null;
                lifecycle.AutomaticEndUtc = endedAt;
            }
            return ended;
        }

        /// <summary>
        /// Persists an event that arrived while the in-memory queue was full:
        /// instead of being dropped, it goes straight to the offline spool
        /// (disk-only — never queued) and is delivered on the NEXT LAUNCH
        /// through the normal restore path; the spool caps (FIFO) still bound
        /// the total. The file write runs on the calling thread, outside
        /// <c>_gate</c>. Acceptance is recorded only after the event is
        /// durably on disk AND neither a consent denial nor a close raced
        /// the append:
        /// - A denial's spool wipe may have run before the append landed, so
        ///   the same post-append epoch recheck as every other spool append
        ///   undoes it.
        /// - A close that won <c>_gate</c> during the file IO has already run
        ///   its final drain WITHOUT this event; accepting it afterwards would
        ///   break the close barrier, so it is dropped and its spool entry
        ///   removed — a post-close Track never delivers, the same contract
        ///   as the queued path.
        /// The session transition was already committed in the hold that
        /// attached it (see EnqueuePrepared), so acceptance here only records
        /// the counters and the breadcrumb. A rejected event (failed write,
        /// oversized, denial, close) rolls back the session mutation its
        /// attach and commit produced, unless another event advanced the
        /// session in the meantime; a close-raced append is undone WITH
        /// restoration of the durable entries its cap eviction displaced (an
        /// undone provisional append must never cost unrelated backlog),
        /// while a denial-raced append is plainly removed — the denial
        /// condemns the evicted entries too. Returns false (counted dropped)
        /// in all rejection cases.
        /// </summary>
        private bool SpoolOverflowEvent(ShardPilotEvent prepared, OverflowContext context)
        {
            // Narrow the race window before any disk IO: a close or denial
            // that has ALREADY landed means nothing should be written at all.
            // The post-write recheck below remains the backstop for races
            // landing during the write itself.
            var rejectedBeforeWrite = false;
            lock (_gate)
            {
                if ((IntakeClosed && !context.Closing) || _consentDenialEpoch != context.EpochBefore)
                {
                    RollbackOverflowSessionLocked(context);
                    rejectedBeforeWrite = true;
                }
            }

            if (rejectedBeforeWrite)
            {
                _stats.RecordDropped(1);
                SettleRefusedReplacement(context);
                return false;
            }

            // Test seam: lets a test land a SetConsent(false) or a close
            // inside the recheck-to-append window. No-op in production.
            var preAppendHook = SpoolPreAppendHookForTesting;
            if (preAppendHook != null)
            {
                preAppendHook();
            }

            var single = new List<ShardPilotEvent>(2);
            if (context.ReplacedEnd != null)
            {
                single.Add(context.ReplacedEnd);
            }
            single.Add(prepared);
            ShardPilotSpool.AppendUndoToken undoToken;

            // PROVISIONAL and non-evicting: at the caps this append is
            // rejected — the event is dropped like a full in-memory queue
            // would drop it — rather than displacing durable backlog a raced
            // undo might then have to restore. Oldest backlog is preserved.
            var appended = AppendToActiveEventSpool(single, true, out undoToken);

            bool accepted;
            bool denialLanded;
            lock (_gate)
            {
                denialLanded = _consentDenialEpoch != context.EpochBefore;
                var closedMeanwhile = IntakeClosed && !context.Closing;
                var endedBeforeAcceptance = context.EventLifecycle != null &&
                    context.EventLifecycle.AutomaticEndUtc != default(DateTime) &&
                    prepared.TimestampUtc > context.EventLifecycle.AutomaticEndUtc;
                accepted = appended && !denialLanded && !closedMeanwhile && !endedBeforeAcceptance;
                if (accepted)
                {
                    // Record the acceptance under _gate, mirroring the queued
                    // path: counters and the breadcrumb only for an event that
                    // actually became durable — and never after a denial. The
                    // breadcrumb is gated on the crash plane's opt-out exactly
                    // like the queued path's.
                    //
                    // The append confirmed: THIS session-start's STAGED
                    // experiments renewal (see SessionStart's provisional
                    // commit) becomes real now, in this same gate hold —
                    // settled by its OWN stake, so an ordinary overflow
                    // event (stake 0) racing a session-start's pending
                    // append no longer lands the neighbor's stage before
                    // its verdict.
                    if (_experiments != null)
                    {
                        _experiments.CommitProvisionalSessionRenewal(context.RenewalStake);
                    }

                    if (context.ReplacedEnd != null)
                    {
                        _stats.RecordEnqueued();
                        _stats.RecordSpooledOverflow();
                        AddOverflowAccountingLocked(context.ReplacedEnd.Id);
                    }

                    _stats.RecordEnqueued();
                    RecordSessionAcceptanceLocked(prepared, context.EventLifecycle);
                    _stats.RecordSpooledOverflow();
                    AddOverflowAccountingLocked(prepared.Id);

                    // Epoch as well as boolean: the append above ran outside
                    // _gate, so an opt-out (which cleared the ring) followed
                    // by a quick re-enable could otherwise let this record a
                    // pre-opt-out event name AFTER the completed disable —
                    // the exact leak the ring clear exists to prevent.
                    if (CrashCollectionAllowedLocked && _crashDisableEpoch == context.CrashEpochBefore)
                    {
                        if (context.ReplacedEnd != null)
                        {
                            _breadcrumbs.Record(context.ReplacedEnd.Name);
                        }

                        _breadcrumbs.Record(prepared.Name);
                    }
                }
                else
                {
                    RollbackOverflowSessionLocked(context);
                }
            }

            if (accepted)
            {
                return true;
            }

            if (appended || undoToken != null)
            {
                if (denialLanded)
                {
                    // The denial's wipe condemns the append AND whatever its
                    // cap eviction displaced: plain removal, no restore —
                    // failing closed if the removal cannot be confirmed.
                    RemoveDeniedOrFailClosed(single);
                }
                else
                {
                    // Close race: the final drain completed without this
                    // event, so the provisional append is undone — touching
                    // ONLY what that append staged (a null token means it
                    // staged nothing: a duplicate of an already-spooled id,
                    // whose pre-existing entry must survive) and restoring
                    // the durable entries its eviction displaced.
                    if (undoToken != null && !WithActiveEventSpool(owner => owner._spool.UndoAppend(undoToken)))
                    {
                        // The undo could not be confirmed on disk. Do NOT
                        // escalate to a wipe — one stuck event must not
                        // destroy legitimate backlog. Record the id on the
                        // persisted per-scope suppression list instead: the
                        // next launch skips it at load and removes it from
                        // the file, so a Track that returned false can never
                        // deliver.
                        // This marker belongs to the scope's settings store,
                        // so it remains writable during an ownership gap.
                        _spool.SuppressOnNextLaunch(undoToken.StagedIds);
                    }
                }
            }

            _stats.RecordDropped(1);
            SettleRefusedReplacement(context);
            return false;
        }

        // The end of the replaced session, as it was staged, at endedAtUtc.
        // Caller must hold _gate.
        private SessionEndClaim SettledEndClaimLocked(ShardPilotEvent replacedEnd, DateTime endedAtUtc)
        {
            object reason;
            replacedEnd.Props.TryGetValue("reason", out reason);
            RefreshLifecycleTransitionsLocked();
            return new SessionEndClaim
            {
                Event = new ShardPilotEvent
                {
                    Name = "app.session_ended",
                    SessionId = replacedEnd.SessionId,
                    SessionSequence = replacedEnd.SessionSequence,
                    TimestampUtc = endedAtUtc,
                    Props = new Dictionary<string, object> { { "reason", reason } }
                },
                Transitions = _lifecycleTransitions,
                Run = _lifecycleRun,
                Epoch = _lifecycleTransitionsEpoch
            };
        }

        // A resume that ran during the refused append found nothing to claim:
        // end the restored session with idle_timeout at endedAtUtc and owe the
        // next start, as that resume would have. Caller must hold _gate.
        private void SettleRestoredSessionEndLocked(OverflowContext context, DateTime endedAtUtc)
        {
            RefreshLifecycleTransitionsLocked();
            context.SettledEnd = new SessionEndClaim
            {
                Event = new ShardPilotEvent
                {
                    Name = "app.session_ended",
                    SessionId = context.SessionBefore.Id,
                    SessionSequence = context.SessionBefore.Sequence + 1,
                    TimestampUtc = endedAtUtc,
                    Props = new Dictionary<string, object> { { "reason", "idle_timeout" } }
                },
                Transitions = _lifecycleTransitions,
                Run = _lifecycleRun,
                Epoch = _lifecycleTransitionsEpoch
            };
            context.SettledEndOwesStart = true;
            _sessionState = new SessionState(null, 0, true);
        }

        // Outside _gate: the end a refused replacement settles, and, when it
        // ended the restored session for a raced resume, the owed start.
        private void SettleRefusedReplacement(OverflowContext context)
        {
            if (context.SettledEnd == null) return;
            // A close that won the append still carries this end; it can land
            // after the close drain's last pass, so it is also persisted.
            if (EnqueueSessionEnd(context.SettledEnd, true) && Volatile.Read(ref _closed))
            {
                PersistPendingEvents();
            }
            if (context.SettledEndOwesStart && ConsentState == ShardPilotConsentState.Granted)
            {
                DischargeOwedSessionStart();
            }
        }

        /// <summary>
        /// Undoes the session mutation a rejected overflow event produced —
        /// its attach (a lazily created session id and/or an advanced
        /// sequence) AND its committed session-start transition — restoring
        /// the pre-attach snapshot, but ONLY while the session state is still
        /// exactly what that event left behind. If another event advanced the
        /// session in the meantime, its state is left untouched (rolling back
        /// underneath it would hand out out-of-order sequence numbers).
        /// Caller must hold <c>_gate</c>.
        /// </summary>
        private void RollbackOverflowSessionLocked(OverflowContext context)
        {
            if (_consentDenialEpoch != context.EpochBefore)
            {
                // Consent loss replaced the session, not a later accepted
                // event. Its withdrawn provisional renewal must be abandoned,
                // never committed onto the fresh post-loss lifecycle.
                _experiments?.AbandonProvisionalSessionRenewal(context.RenewalStake);
                return;
            }

            if (ReferenceEquals(_sessionState, context.SessionAfter))
            {
                _sessionState = context.SessionBefore;

                // A refused replacement also never ended the replaced session:
                // an expired pause it consumed is restored with the session.
                // A resume that ran during the append found no pause to claim,
                // so the refusal settles what that resume would have: the
                // replaced session ends at its deadline and a start is owed.
                if (context.AutomaticEndLifecycle != null)
                {
                    if (_resumeGeneration == context.ResumeGenerationBefore)
                    {
                        context.AutomaticEndLifecycle.AutomaticEndUtc = context.AutomaticEndBefore;
                        if (_sessionPause == null)
                        {
                            _sessionPause = context.PauseBefore;
                        }
                    }
                    else
                    {
                        SettleRestoredSessionEndLocked(context, context.ReplacedEnd.TimestampUtc);
                    }
                }
                else if (context.PauseBefore != null &&
                    context.PauseBefore.RacedResumeEndUtc != default(DateTime) &&
                    ReferenceEquals(context.PauseBefore.Lifecycle, context.SessionBefore.Lifecycle))
                {
                    // The pause had not expired when this start claimed the
                    // session, but it had by the time a resume ran during the
                    // append. That resume could not claim it; settle its end.
                    var endedAt = context.PauseBefore.RacedResumeEndUtc;
                    var lifecycle = context.SessionBefore.Lifecycle;
                    if (endedAt < lifecycle.LatestAcceptedTimestampUtc)
                    {
                        endedAt = lifecycle.LatestAcceptedTimestampUtc;
                    }
                    lifecycle.AutomaticEndUtc = endedAt;
                    SettleRestoredSessionEndLocked(context, endedAt);
                }

                // The session mutation rolled back, so THIS operation's
                // STAGED experiments renewal is abandoned with it: the
                // renewal never ran, leaving the consumer bit-identical to
                // before the refused call (no marker rotation, no adopted
                // or re-armed exposure under the refused session id — the
                // sweep latch kept the window quiet). The abandon is
                // OWNER-BOUND (this context's stake; 0 for ordinary
                // overflow events, which abandon nothing): a rejected
                // ordinary event racing a session-start's pending append
                // used to reach an unconditional abandon here and kill the
                // OTHER operation's stage — its append then succeeded, its
                // commit no-opped, and the accepted session never re-armed
                // or adopted the cached assignments.
                if (_experiments != null)
                {
                    _experiments.AbandonProvisionalSessionRenewal(context.RenewalStake);
                }
            }
            else
            {
                // Another event advanced the session during the unlocked
                // append: the session state is left standing (rolling back
                // underneath it would hand out out-of-order sequence
                // numbers), so the new session is real de facto — events
                // are bound to it. THIS operation's staged renewal commits
                // with it (owner-bound; a concurrent operation's pending
                // stage stays for its own verdict); only the
                // session_started envelope itself was refused.
                if (_experiments != null)
                {
                    _experiments.CommitProvisionalSessionRenewal(context.RenewalStake);
                }

                // The replaced session ended when it was replaced, de facto as
                // well: its staged end left with the refused pair, so it is
                // sent on its own.
                if (context.ReplacedEnd != null)
                {
                    context.SettledEnd = SettledEndClaimLocked(context.ReplacedEnd,
                        context.ReplacedEnd.TimestampUtc);
                }
            }
        }

        /// <summary>
        /// Snapshots taken under <c>_gate</c> when a full-queue event heads
        /// for the overflow-to-spool path: the consent denial epoch (for the
        /// post-append recheck) and the session state around the attach (for
        /// the failed-append rollback).
        /// </summary>
        private sealed class OverflowContext
        {
            /// <summary>An end a close owes: a closed client still spools it.</summary>
            public bool Closing;
            public long EpochBefore;
            public long CrashEpochBefore;
            public SessionState SessionBefore;
            public SessionState SessionAfter;
            public SessionLifecycle EventLifecycle;

            /// <summary>The end of the session a provisional start replaces,
            /// appended with the start and undone with it; null when none.</summary>
            public ShardPilotEvent ReplacedEnd;
            public SessionPause PauseBefore;
            public long ResumeGenerationBefore;
            public SessionLifecycle AutomaticEndLifecycle;
            public DateTime AutomaticEndBefore;

            /// <summary>An end a refused replacement still owes: the restored
            /// session's, when a resume ran during the append, or the replaced
            /// session's, when an intervening event kept the replacement.
            /// Sent once the gate is released.</summary>
            public SessionEndClaim SettledEnd;
            public bool SettledEndOwesStart;

            /// <summary>The experiments-renewal stage THIS overflow
            /// operation created (0 = none): its append verdict commits or
            /// abandons exactly this stage — never a concurrent
            /// operation's.</summary>
            public long RenewalStake;
        }

        // Accepted timestamps belong to lifecycle identity, not sequence revisions.
        // A refused provisional append never advances this high-water mark.
        private SessionLifecycle CurrentEventLifecycleLocked(ShardPilotEvent ev)
        {
            return !string.IsNullOrEmpty(ev.SessionId) && ev.SessionId == _sessionState.Id
                ? _sessionState.Lifecycle : null;
        }

        private void RecordSessionAcceptanceLocked(ShardPilotEvent ev, SessionLifecycle lifecycle)
        {
            if (lifecycle != null && ev.TimestampUtc > lifecycle.LatestAcceptedTimestampUtc)
            {
                lifecycle.LatestAcceptedTimestampUtc = ev.TimestampUtc;
            }
            if (lifecycle != null && ReferenceEquals(lifecycle, _sessionState.Lifecycle))
            {
                ArmBackgroundSessionLocked(lifecycle);
            }
        }

        private void ArmBackgroundSessionLocked(SessionLifecycle lifecycle)
        {
            if (_backgroundedAtUtc != default(DateTime) && !string.IsNullOrEmpty(_sessionState.Id) &&
                (_sessionPause == null || !ReferenceEquals(_sessionPause.Lifecycle, lifecycle)))
            {
                _sessionPause = new SessionPause(lifecycle, _utcNow(), _monotonicTicks());
            }
        }

        // Mutates only the private event; callers commit the returned state after
        // the full envelope fits. A refused event cannot spend a lazy session.
        private SessionState PreviewAttachedSession(ShardPilotEvent ev, bool allowLazyCreate, SessionState session)
        {
            if (allowLazyCreate && string.IsNullOrEmpty(ev.SessionId))
            {
                // The server REQUIRES session_id whenever source is not "backend"
                // (taxonomy.go: sessionIDRequiredForEvent). An event tracked before
                // an explicit SessionStart() would otherwise ship with no
                // session_id and get the WHOLE batch 400-rejected. Lazily open a
                // session (resetting the sequence so the first event gets 1) for
                // any client/server source; backend sources never synthesize one.
                if (string.IsNullOrEmpty(session.Id) &&
                    _config.Source != ShardPilotSource.Backend)
                {
                    session = new SessionState(
                        ShardPilotIds.NewUuidV7(), 0, session.OwedStart);
                }

                if (!string.IsNullOrEmpty(session.Id))
                {
                    ev.SessionId = session.Id;
                }
            }

            if (!string.IsNullOrEmpty(ev.SessionId) && ev.SessionSequence <= 0)
            {
                // Under _gate, so a plain increment is safe (no Interlocked).
                session = session.WithSequence(session.Sequence + 1);
                ev.SessionSequence = session.Sequence;
            }
            return session;
        }

        /// <summary>
        /// Captures a crash and ships it to the dedicated crash
        /// ingest endpoint: <c>POST {crashBase}/api/v1/crashes/ingest</c>
        /// with the <c>crash:write</c> Bearer and the crash report JSON
        /// wire shape — NOT wrapped as a <c>mobile_crash</c> analytics event on
        /// <c>/v1/events:batch</c>. The crash carries the configured component
        /// <c>source</c> slug, the sanitized exception/frames/breadcrumbs, and
        /// the hashed-at-ingest anonymous/session ids.
        ///
        /// Returns <c>false</c> synchronously (no task) when crash capture is
        /// disabled (no <see cref="ShardPilotConfig.CrashKey"/>), the client
        /// is closed, the event is invalid, or a NON-FATAL crash is sampled
        /// away. FATAL crashes are NEVER sampled — a terminating crash is always
        /// sent, bypassing the sampler, consistent across the ShardPilot SDKs. When the
        /// send is dispatched, returns <c>true</c> and surfaces the in-flight
        /// transport result through <paramref name="onSent"/> (best-effort).
        /// </summary>
        public bool CaptureCrash(ShardPilotCrashEvent crashEvent, Action<ShardPilotResult>? onSent = null)
        {
            ShardPilotCrashCaptureDurability ignoredDurability;
            return CaptureCrash(crashEvent, WrapCrashCallback(onSent), out ignoredDurability);
        }

        // Only the public notification is host-owned. The overload below also
        // serves AEI durability and fatal-send signals, which must settle even
        // when no host frame can run.
        private Action<ShardPilotResult>? WrapCrashCallback(Action<ShardPilotResult>? callback)
        {
            if (callback == null) return null;
            return result =>
            {
                var notify = Interlocked.Exchange(ref callback, null);
                if (notify == null) return;
                _ = _crashCallbackHost.InvokeOnHost(() =>
                {
                    try { notify(result); } catch { /* Game code cannot break SDK settlement. */ }
                    return Task.FromResult(true);
                }, CancellationToken.None, () => !HostRetiring);
            };
        }

        /// <summary>
        /// The internal capture overload that additionally reports the
        /// report's DURABILITY at return (see
        /// <see cref="ShardPilotCrashCaptureDurability"/>) — consumed by the
        /// AEI next-launch pass, whose cursor may only settle a record once
        /// the report is durably spooled or acked. Behavior is otherwise
        /// identical to the public overload.
        /// </summary>
        internal bool CaptureCrash(
            ShardPilotCrashEvent crashEvent,
            Action<ShardPilotResult> onSent,
            out ShardPilotCrashCaptureDurability durability)
        {
            durability = ShardPilotCrashCaptureDurability.None;
            if (crashEvent == null)
            {
                return false;
            }

            // Crash capture requires the crash:write key; without it there is no
            // credential for the crash endpoint and nothing is sent (safe no-op).
            if (string.IsNullOrEmpty(_config.CrashKey))
            {
                SafeLog("shardpilot crash capture skipped: no crash:write api key configured");
                return false;
            }

            long capturedDenialEpoch;
            long capturedCrashEpoch;
            lock (_gate)
            {
                if (IntakeClosed)
                {
                    return false;
                }

                // A denied analytics-consent posture also suppresses crash
                // diagnostics: the anonymous_id is the consent/erasure subject.
                if (ShardPilotConsent.IsDeniedState(_consentState))
                {
                    _stats.RecordDropped(1);
                    return false;
                }

                // The crash plane's own opt-out (independent of analytics
                // consent) gates the capture BEFORE any work happens: no
                // sanitization, no serialization, no write-ahead persist —
                // while disabled, collection stops, not just sending. The
                // same gate applies when the persisted setting could not be
                // read or was malformed (fail closed).
                if (!CrashCollectionAllowedLocked)
                {
                    _stats.RecordRefusal(ShardPilotCrashSettings.DisabledError);
                    return false;
                }

                // Snapshot the denial epoch under the same lock the consent
                // check and SetConsent(false) use. Sanitization and
                // serialization below run UNLOCKED, so a SetConsent(false) can
                // land while this crash is being prepared; the epoch is
                // rechecked under the lock immediately before the send so the
                // anonymous_id/breadcrumbs of a just-denied subject never reach
                // the wire. Mirrors the analytics publish-path epoch recheck.
                capturedDenialEpoch = _consentDenialEpoch;

                // The crash-plane analogue: an opt-out landing while this
                // report is prepared UNLOCKED must abort it at the rechecks
                // below even when a quick re-enable restores the boolean —
                // this capture's breadcrumbs predate the opt-out, and the
                // disable call that already returned promised no later
                // collection of them.
                capturedCrashEpoch = _crashDisableEpoch;
            }

            // FATAL crashes bypass sampling entirely (consistent across the
            // ShardPilot SDKs): a terminating crash is always sent. Only NON-FATAL reports are
            // rate-sampled to bound volume from a noisy handled error.
            var fatal = crashEvent.Fatal;
            var nonFatalSampleOneIn = _config.CrashSampleEvery;
            if (!fatal && !ShouldEmitNonFatal(nonFatalSampleOneIn))
            {
                _stats.RecordDropped(1);
                return false;
            }

            ShardPilotCrashEvent prepared;
            try
            {
                prepared = ShardPilotCrashSanitizer.Prepare(crashEvent, _breadcrumbs.Snapshot());
                // Freeze the classification and rate that admitted this report,
                // before serialization and write-ahead persistence. Caller maps
                // and a later client's config cannot supply these wire fields.
                prepared.Fatal = fatal;
                prepared.NonFatalSampleOneIn = fatal ? 0 : nonFatalSampleOneIn;
                // Resolve the component source against the configured default,
                // distinguishing a bad caller override (omit, bare app) from an
                // absent one (fall back to the default). Done here, not in
                // Prepare, because Prepare preserves the raw caller value so this
                // distinction survives.
                prepared.Source = ShardPilotCrashSanitizer.ResolveCrashSource(crashEvent.Source, _config.CrashComponentValue);
                if (!prepared.UseEraAttribution)
                {
                    prepared.AppVersion = ShardPilotCrashSanitizer.SanitizeOptionalString(FirstNonEmpty(prepared.AppVersion, _config.AppVersion));
                    prepared.BuildId = ShardPilotCrashSanitizer.SanitizeOptionalString(FirstNonEmpty(prepared.BuildId, _config.AppBuild));
                }
                else
                {
                    // An era-attributed (next-launch AEI) report describes a
                    // PREVIOUS process: its app version/build come from the
                    // attributing era's snapshot, already on the event —
                    // possibly EMPTY (no era, or a pre-upgrade era line),
                    // which must ship as unknown. Backfilling the CURRENT
                    // launch's config here would attribute the historical
                    // ANR to the build doing the reporting, exactly the
                    // borrow the frozen-attribution rule forbids for the
                    // anonymous id. Sanitized by Prepare above like every
                    // other event field.
                    prepared.AppVersion = ShardPilotCrashSanitizer.SanitizeOptionalString(prepared.AppVersion);
                    prepared.BuildId = ShardPilotCrashSanitizer.SanitizeOptionalString(prepared.BuildId);
                }

                prepared.Platform = ShardPilotCrashSanitizer.SanitizeOptionalString(FirstNonEmpty(prepared.Platform, _config.Platform));
            }
            catch (Exception ex)
            {
                _stats.RecordFailure(ex.Message);
                SafeLog("shardpilot crash rejected: {0}", ex.Message);
                return false;
            }

            lock (_gate)
            {
                if (string.IsNullOrEmpty(prepared.SessionId))
                {
                    prepared.SessionId = ShardPilotCrashSanitizer.SanitizeSessionId(_sessionState.Id);
                }
            }

            string payload;
            try
            {
                payload = ShardPilotJson.SerializeCrashIngest(_config, prepared);
            }
            catch (Exception ex)
            {
                _stats.RecordFailure(ex.Message);
                SafeLog("shardpilot crash serialize failed: {0}", ex.Message);
                return false;
            }

            // ONE continuous dispatch-gate hold spans the write-ahead append
            // through the sender handoff (see _crashDispatchGate): disabling
            // acquires this same gate after flipping the flag, so once
            // SetCrashReportingEnabled(false) returns, every in-gate capture
            // has already been rechecked (and its append undone), and any
            // later capture refuses at the gate-held check below BEFORE
            // anything reaches the disk — a fatal crash mid-append can no
            // longer strand an opted-out record behind a completed opt-out
            // call (if the process dies inside this hold, the blocked
            // disable call never returned to its caller).
            lock (_crashDispatchGate)
            {
                // Fresh gate-held recheck BEFORE the write-ahead append: a
                // disable, denial, or close landing between the entry check
                // and this gate refuses here, with nothing on disk yet.
                lock (_gate)
                {
                    if (_closed || capturedDenialEpoch != _consentDenialEpoch ||
                        capturedCrashEpoch != _crashDisableEpoch || !CrashCollectionAllowedLocked)
                    {
                        _stats.RecordDropped(1);
                        return false;
                    }
                }

                // Test seam: lets a test land a SetConsent(false) precisely inside
                // the entry-check-to-persist race window. No-op in production.
                var prePersistHook = CrashPrePersistHookForTesting;
                if (prePersistHook != null)
                {
                    prePersistHook();
                }

                // Durable write-ahead: persist the report BEFORE the send attempt,
                // synchronously on the capturing thread — for a fatal crash this is
                // the moment the process may die, so "later" does not exist. What
                // goes to disk is the exact scrubbed wire payload serialized above,
                // never a rawer form. The record is removed when the ingest
                // service acknowledges (or permanently rejects) the send; until
                // then it survives the process and is re-sent on the next launch.
                // False means a consent denial raced the persist: the record was
                // removed again fail-closed and the capture must be dropped.
                // The undo token tells the close-race abort below what the
                // write-ahead append staged and what it evicted (null when the
                // record went to a successor or nowhere).
                ShardPilotCrashSpool.AppendUndoToken persistUndoToken;
                if (!PersistCrashReportAtCapture(prepared.CrashId, payload, capturedDenialEpoch, capturedCrashEpoch, out persistUndoToken, out durability))
                {
                    durability = ShardPilotCrashCaptureDurability.None;
                    _stats.RecordDropped(1);
                    return false;
                }

                if (_httpSender == null)
                {
                    // No transport wired (e.g. a test client constructed without a
                    // sender). The crash cannot ship NOW — but the durable record
                    // above still delivers it on a later launch that has one.
                    SafeLog("shardpilot crash capture skipped: no http sender");
                    return false;
                }

                // Test seam: let a test land a SetConsent(false) precisely inside
                // the serialize-then-recheck race window. No-op in production.
                var preSendHook = CrashPreSendHookForTesting;
                if (preSendHook != null)
                {
                    preSendHook();
                }

                // A record that was handed to a successor lives under THAT
                // client's consent authority, and this displaced client is off
                // the lifecycle registry — a denial recorded there between the
                // handoff and this point never propagates back here. Ask the
                // durable home directly: if it is denied NOW, its wipe already
                // condemned the copy, and the live send below must not ship the
                // denied subject's report either. Checked BEFORE the in-flight
                // count below is taken, so this abort never leaves a pending
                // crash send that flush/close would wait out.
                // Routing is derived from the CURRENT spool state, not from how
                // the persist above ran: a replacement can land between an
                // own-spool append and this point (registration disowns the
                // spool and forwards its backlog — this record included), and
                // from then on the durable copy lives down the chain even though
                // the append produced an undo token.
                var handedToSuccessor = CrashReportRoutesToSuccessor();

                // The settlement anchor: WHICH client's consent world governed
                // this send, and that world's denial epoch AS OF the dispatch.
                // Epoch counters are per-client, so a settlement may only ever
                // compare an epoch against the same instance it was read from.
                var settleHome = this;
                var settleEpoch = capturedDenialEpoch;
                var settleCrashEpoch = capturedCrashEpoch;
                if (handedToSuccessor)
                {
                    var home = ResolveCrashDurableHome();
                    if (!ReferenceEquals(home, this))
                    {
                        // State and epochs are read under ONE lock hold: read
                        // separately, a denial landing between them would leave
                        // a Granted verdict paired with a post-denial epoch —
                        // the settlement guard would then treat the condemned
                        // world as current.
                        long homeEpoch;
                        long homeCrashEpoch;
                        if (!home.TrySnapshotConsentForDispatch(out homeEpoch, out homeCrashEpoch))
                        {
                            // The home refused (denied, crash plane disabled,
                            // or a no-key home that could only lend an empty
                            // credential): a denial's wipe already condemned
                            // the handed-off durable copy (removal no-ops),
                            // but a crash OPT-OUT deliberately keeps backlog —
                            // and must not keep the copy of a capture that is
                            // being rejected here, or it would re-send after a
                            // re-enable despite this CaptureCrash reporting
                            // false. Remove it wherever it lives now —
                            // and from this client's own orphaned file when
                            // a no-spool chain refused the forward.
                            home.RemoveCrashRecordForRejectedCapture(prepared.CrashId);
                            RemoveRejectedCaptureFromOrphanedSpoolFile(prepared.CrashId);
                            durability = ShardPilotCrashCaptureDurability.None;
                            _stats.RecordDropped(1);
                            return false;
                        }

                        settleHome = home;
                        settleEpoch = homeEpoch;
                        settleCrashEpoch = homeCrashEpoch;
                    }
                }

                // Built ABOVE the consent recheck, so the recheck-to-dispatch
                // window below is as small as it can be without doing IO under
                // the lock. A handed-off record ships with the durable HOME's
                // endpoint and crash key: after a re-initialization that rotated
                // credentials, the stale client's old key could 401 — a
                // permanent classification that would settle away the
                // successor's copy of a report the NEW credentials would have
                // delivered.
                var request = new ShardPilotHttpRequest
                {
                    Method = "POST",
                    Url = settleHome._crashEndpoint,
                    Bearer = settleHome._config.CrashKey,
                    ContentType = "application/json",
                    Body = payload,
                    Timeout = settleHome._config.HttpTimeout
                };

                // Recheck consent under _gate IMMEDIATELY before handing the bytes
                // to the transport. The serialization above ran unlocked; if a
                // SetConsent(false) advanced the denial epoch in the meantime, drop
                // the crash here with no trace rather than shipping a just-denied
                // subject's anonymous_id/breadcrumbs. Same epoch mechanism the
                // analytics publish path uses before its transport send; like
                // there, a denial landing after this hold while the request is
                // already on its way is enforced server-side (the ingest service
                // suppresses storage for a denied subject). This recheck-to-
                // dispatch step continues under the SAME dispatch-gate hold
                // that covered the write-ahead append above.
                var abortSend = false;
                var denialLandedBeforeSend = false;
                var disableLandedBeforeSend = false;
                lock (_gate)
                {
                    // The crash opt-out is rechecked alongside close/denial: a
                    // SetCrashReportingEnabled(false) landing while this report
                    // was being prepared must stop the send AND remove the
                    // write-ahead record (below) — fail-closed for the raced
                    // record, while the enabled-period backlog stays durable
                    // for a possible re-enable within the retention window.
                    if (_closed || capturedDenialEpoch != _consentDenialEpoch ||
                        capturedCrashEpoch != _crashDisableEpoch || !CrashCollectionAllowedLocked)
                    {
                        abortSend = true;
                        denialLandedBeforeSend = capturedDenialEpoch != _consentDenialEpoch;
                        disableLandedBeforeSend = capturedCrashEpoch != _crashDisableEpoch || !CrashCollectionAllowedLocked;
                    }
                    else
                    {
                        // Count this send as in flight while still holding the lock, so
                        // a concurrent flush/close either observes it as pending (and
                        // waits) or runs entirely after it — never races past it.
                        Interlocked.Increment(ref _pendingCrashSends);
                    }
                }

                if (!abortSend)
                {
                    var preDispatchHook = CrashPreDispatchHookForTesting;
                    if (preDispatchHook != null)
                    {
                        preDispatchHook();
                    }

                    // Ownership can MOVE between the anchor above and this
                    // dispatch: an SDK re-initialization registering a
                    // successor disowns this client's spool and forwards its
                    // backlog — this record included — mid-capture. Re-resolve
                    // the durable home NOW; if it is no longer the anchored
                    // one, abort the live send: a stale-anchored dispatch
                    // could ship a report the new owner has just refused, or
                    // settle the new owner's durable copy against a rotated
                    // (now-401ing) old credential. The capture reports false,
                    // so the forwarded copy is removed from the new home too —
                    // a false capture must never deliver later.
                    var homeNow = CrashReportRoutesToSuccessor() ? ResolveCrashDurableHome() : this;
                    if (!ReferenceEquals(homeNow, settleHome))
                    {
                        homeNow.RemoveCrashRecordForRejectedCapture(prepared.CrashId);
                        RemoveRejectedCaptureFromOrphanedSpoolFile(prepared.CrashId);
                        Interlocked.Decrement(ref _pendingCrashSends);
                        WakeWorker();
                        durability = ShardPilotCrashCaptureDurability.None;
                        _stats.RecordDropped(1);
                        return false;
                    }
                }

                // The final recheck and the sender handoff run under the
                // DURABLE HOME's dispatch gate (a reentrant no-op when the
                // home is this client): the home's SetCrashReportingEnabled
                // (false) synchronizes on ITS gate, so without this hold a
                // successor's opt-out could complete between the snapshot
                // below and the Send — letting a stale predecessor start a
                // crash request after the disable call already returned.
                // Ordering is strictly predecessor -> successor (the same
                // direction every handoff call takes), so the nested hold
                // cannot deadlock.
                lock (settleHome._crashDispatchGate)
                {
                    // For a handed-off record, re-verify the HOME's consent world
                    // one final time now that the in-flight count is taken: the
                    // stale predecessor's own epoch (checked above) cannot move when
                    // the successor denies — it is off the lifecycle registry — so
                    // the last-line dispatch gate must anchor on the home itself.
                    // Runs under the home's dispatch gate (held above), so a home
                    // opt-out either completed before this snapshot (refused here)
                    // or blocks until the Send below has been handed to the
                    // transport — matching the own-path guarantee. Checked outside
                    // this client's _gate; the released count mirrors the
                    // sync-throw path.
                    if (!abortSend && handedToSuccessor && !ReferenceEquals(settleHome, this))
                    {
                        long homeEpochNow;
                        long homeCrashEpochNow;
                        if (!settleHome.TrySnapshotConsentForDispatch(out homeEpochNow, out homeCrashEpochNow) ||
                            homeEpochNow != settleEpoch || homeCrashEpochNow != settleCrashEpoch)
                        {
                            // Denied, crash reporting disabled, or the world moved
                            // (even a deny+re-grant): the capture is rejected, so its
                            // durable copy must not outlive it. A denial's wipe
                            // already removed it (this removal no-ops), but an
                            // OPT-OUT keeps backlog by design — explicitly remove
                            // the rejected capture's record wherever it lives now.
                            settleHome.RemoveCrashRecordForRejectedCapture(prepared.CrashId);
                            RemoveRejectedCaptureFromOrphanedSpoolFile(prepared.CrashId);
                            Interlocked.Decrement(ref _pendingCrashSends);
                            WakeWorker();
                            durability = ShardPilotCrashCaptureDurability.None;
                            _stats.RecordDropped(1);
                            return false;
                        }
                    }

                    if (abortSend)
                    {
                        // The write-ahead record must not outlive the abort (spool ops
                        // outside _gate — leaf lock):
                        // - A denial condemns the report entirely — remove it, failing
                        //   closed to the wipe when the removal cannot be confirmed
                        //   (the denial condemns whatever the append evicted too, so
                        //   no restore).
                        // - A close means this CaptureCrash reports false, and a false
                        //   capture must not deliver on a later launch (the same
                        //   contract as the event spool's close-raced appends) — NOR
                        //   cost unrelated reports their durability, so the undo also
                        //   restores what the append's cap enforcement evicted. A
                        //   record that was (or has since been) handed to a successor
                        //   is removed THERE: the contract binds the CAPTURE, not the
                        //   spool it landed in. (When the append ran here but a
                        //   replacement forwarded the backlog mid-capture, an
                        //   eviction that append made cannot be restored anymore —
                        //   the forwarded backlog already lacked those records; a
                        //   triple race of capture, replacement, and close resolves
                        //   toward the fewest resurrections.) A failed undo merely
                        //   leaves a real crash report the ingest service
                        //   de-duplicates.
                        if (denialLandedBeforeSend)
                        {
                            RemoveDeniedCrashOrFailClosed(prepared.CrashId);
                        }
                        else if (CrashReportRoutesToSuccessor())
                        {
                            // Ownership is re-resolved FRESH here, not from the
                            // earlier snapshot: a replacement can register after
                            // that snapshot and before this abort, disowning the
                            // spool and forwarding the record — an undo on the
                            // disowned spool would no-op and leave the forwarded
                            // copy deliverable despite this capture reporting
                            // false.
                            var home = ResolveCrashDurableHome();
                            if (!ReferenceEquals(home, this))
                            {
                                home.RemoveCrashRecordForRejectedCapture(prepared.CrashId);
                                RemoveRejectedCaptureFromOrphanedSpoolFile(prepared.CrashId);
                            }
                        }
                        else if (disableLandedBeforeSend)
                        {
                            // An opt-out-raced record fails CLOSED: it must not
                            // survive to a re-enable within the retention window,
                            // so a failed undo escalates instead of being left
                            // as a benign leftover.
                            RemoveOptOutRacedCrashOrFailClosed(prepared.CrashId, persistUndoToken);
                        }
                        else if (_crashSpool != null)
                        {
                            _crashSpool.UndoAppend(persistUndoToken);
                        }

                        // Whatever the persist staged was removed/undone
                        // above: nothing durable survives an aborted capture.
                        durability = ShardPilotCrashCaptureDurability.None;
                        _stats.RecordDropped(1);
                        return false;
                    }

                    try
                    {
                        _httpSender.Send(request, response =>
                        {
                            try
                            {
                                var result = ClassifyCrashResponse(response);
                                if (result.Success)
                                {
                                    _stats.RecordCrashSent();
                                }
                                else
                                {
                                    _stats.RecordFailure(result.Error);
                                    SafeLog("shardpilot crash ingest failed: {0}", result.Error);
                                }

                                // Settle the durable copy written before the send:
                                // acknowledged or permanently rejected reports are
                                // removed; a retryable failure keeps the record for a
                                // later attempt and persists a server Retry-After so
                                // the backpressure survives a relaunch. Anchored on
                                // the home + epoch observed at dispatch.
                                SettleCrashReport(settleHome, settleEpoch, prepared.CrashId, result);

                                if (onSent != null)
                                {
                                    try
                                    {
                                        onSent(result);
                                    }
                                    catch
                                    {
                                        // A caller callback must never throw back into the
                                        // dispatcher / crash-capture path.
                                    }
                                }
                            }
                            finally
                            {
                                // Mark the send complete and wake the worker so a flush/
                                // close that is waiting on the in-flight count can return.
                                Interlocked.Decrement(ref _pendingCrashSends);
                                WakeWorker();
                            }
                        });
                    }
                    catch (Exception ex)
                    {
                        // The send threw synchronously before the callback could run; it
                        // will never complete, so release the in-flight count here.
                        Interlocked.Decrement(ref _pendingCrashSends);
                        _stats.RecordFailure(ex.Message);
                        SafeLog("shardpilot crash send failed: {0}", ex.Message);
                        return false;
                    }
                }
            }

            return true;
        }

        /// <summary>
        /// Ships one RAW Android tombstone to the multipart
        /// <c>ingest-tombstone</c> lane for the AEI
        /// native-crash pass: the metadata envelope built from
        /// <paramref name="crashEvent"/> (sanitized through the same
        /// pipeline as every crash body) plus <paramref name="tombstoneBytes"/>
        /// VERBATIM — the server owns parsing/projection/privacy, so the
        /// bytes are never decoded, stripped, or truncated here.
        ///
        /// Consent is the DUMP-CLASS posture, evaluated on CURRENT state and
        /// fail-closed exactly like <c>CaptureCrash</c>'s entry gates: a
        /// denied analytics consent, a crash-plane opt-out, an unreadable
        /// settings state, or a missing crash key all refuse (return false —
        /// nothing leaves the device). There is NO durable write-ahead for
        /// tombstone bytes (raw dump-class bytes never touch the spool): the
        /// caller settles its cursor only on a completed send —
        /// <paramref name="onCompleted"/> receives the crash lane's standard
        /// classification (2xx delivered, incl. the server's 202 dedup; 4xx
        /// permanent; transport/429/5xx retryable), and an unacked record
        /// simply retries on a later launch, collapsing server-side. A
        /// retryable failure's Retry-After re-arms the crash plane's shared
        /// persisted backpressure marker (the crash re-send path's own
        /// mechanism), which the next launch's AEI pass honors before
        /// reopening a trace stream.
        ///
        /// Returns true when the request was handed to the transport;
        /// false when the upload was refused or failed to dispatch (the
        /// callback then never runs).
        /// </summary>
        internal bool TryUploadAeiTombstone(
            ShardPilotCrashEvent crashEvent,
            byte[] tombstoneBytes,
            Action<ShardPilotResult> onCompleted)
        {
            if (crashEvent == null || tombstoneBytes == null || tombstoneBytes.Length == 0)
            {
                return false;
            }

            // The hard cap is re-asserted at the last altitude before the
            // wire: an over-cap tombstone is refused whole (the server would
            // 413 it), NEVER truncated — a truncated proto is unparseable.
            if (tombstoneBytes.Length > ShardPilotTombstoneUpload.MaxTombstoneBytes)
            {
                SafeLog("shardpilot tombstone upload refused: {0} bytes exceeds the {1}-byte lane cap",
                    tombstoneBytes.Length, ShardPilotTombstoneUpload.MaxTombstoneBytes);
                return false;
            }

            if (string.IsNullOrEmpty(_config.CrashKey))
            {
                SafeLog("shardpilot tombstone upload skipped: no crash:write api key configured");
                return false;
            }

            if (_httpSender == null)
            {
                // No transport wired: with no durable copy for raw dump-class
                // bytes, the record must stay above the caller's cursor and
                // retry on a launch that has a sender.
                SafeLog("shardpilot tombstone upload skipped: no http sender");
                return false;
            }

            long capturedDenialEpoch;
            long capturedCrashEpoch;
            lock (_gate)
            {
                if (_closed)
                {
                    return false;
                }

                // CURRENT-consent upload gate, fail-closed (the dump-class posture, same entry gates as CaptureCrash): a
                // denied subject or a disabled/unreadable crash plane ships
                // nothing.
                if (ShardPilotConsent.IsDeniedState(_consentState))
                {
                    _stats.RecordDropped(1);
                    return false;
                }

                if (!CrashCollectionAllowedLocked)
                {
                    _stats.RecordRefusal(ShardPilotCrashSettings.DisabledError);
                    return false;
                }

                capturedDenialEpoch = _consentDenialEpoch;
                capturedCrashEpoch = _crashDisableEpoch;
            }

            string metadataJson;
            try
            {
                // Same sanitize pipeline as the JSON lane. No breadcrumb
                // fallback: an AEI report describes a PREVIOUS era and the
                // builder sets OmitBreadcrumbs.
                var prepared = ShardPilotCrashSanitizer.Prepare(crashEvent, null);
                prepared.Source = ShardPilotCrashSanitizer.ResolveCrashSource(crashEvent.Source, _config.CrashComponentValue);

                // The sanitizer's closed thread-state set defaults to "main",
                // but the tombstone lane's crashed thread is the SERVER's to
                // derive from the proto (the sidecar ordinal) — the
                // envelope must not ship a competing client claim.
                prepared.ThreadState = string.Empty;

                // Derive the tombstone crash identity as UUIDv5 in the
                // fixed exit-record namespace. Keep the original AEI key
                // separate; cursor loss and retries reproduce both IDs
                // and the exact metadata bytes for content deduplication.
                if (ShardPilotAnrIds.IsRecordKeyShape(prepared.AeiRecordKey))
                {
                    prepared.CrashId = ShardPilotIds.FromExitRecordKey(prepared.AeiRecordKey);
                }

                metadataJson = ShardPilotJson.SerializeTombstoneMetadata(_config, prepared);
            }
            catch (Exception ex)
            {
                _stats.RecordFailure(ex.Message);
                SafeLog("shardpilot tombstone metadata serialize failed: {0}", ex.Message);
                return false;
            }

            var boundary = ShardPilotTombstoneUpload.NewBoundary();
            byte[] body;
            try
            {
                body = ShardPilotTombstoneUpload.BuildMultipartBody(metadataJson, tombstoneBytes, boundary);
            }
            catch (Exception ex)
            {
                _stats.RecordFailure(ex.Message);
                SafeLog("shardpilot tombstone multipart build failed: {0}", ex.Message);
                return false;
            }

            // The recheck-then-dispatch runs under the crash dispatch gate,
            // like every crash-plane send: once SetCrashReportingEnabled(false)
            // returns, no tombstone request can start afterwards.
            lock (_crashDispatchGate)
            {
                lock (_gate)
                {
                    if (_closed || capturedDenialEpoch != _consentDenialEpoch ||
                        capturedCrashEpoch != _crashDisableEpoch || !CrashCollectionAllowedLocked)
                    {
                        _stats.RecordDropped(1);
                        return false;
                    }

                    // Counted in flight so a concurrent flush/close waits for
                    // the completion callback, mirroring the JSON crash sends.
                    Interlocked.Increment(ref _pendingCrashSends);
                }

                var request = new ShardPilotHttpRequest
                {
                    Method = "POST",
                    Url = _tombstoneEndpoint,
                    Bearer = _config.CrashKey,
                    ContentType = ShardPilotTombstoneUpload.ContentTypeFor(boundary),
                    BodyBytes = body,
                    Timeout = _config.HttpTimeout,

                    // The production sender only ENQUEUES its coroutine here
                    // and returns; the network request is created when the
                    // dispatcher pumps it — after this gate hold released. A
                    // denial or crash opt-out completing in that window must
                    // still stop the dump-class bytes, so the sender
                    // re-evaluates this closure at the last moment before
                    // the request object exists: a moved consent world
                    // refuses (synthetic retryable completion; nothing
                    // leaves), and the record retries next launch, where the
                    // standing decision settles it.
                    BeforeSend = () => TombstoneDispatchStillCurrent(capturedDenialEpoch, capturedCrashEpoch)
                };

                try
                {
                    _httpSender.Send(request, response =>
                    {
                        try
                        {
                            // The crash lane's standard classification: a 2xx
                            // (the server's 202 queued AND its 202
                            // deduplicated) is delivered; only an exact
                            // 400/404 kind_not_enabled is retryable among
                            // terminal 4xx responses. Bare 404 and the typed
                            // 400 tombstone_unparseable remain permanent;
                            // transport failures, 408, 429, and 5xx (e.g. the
                            // typed 503 stackwalk_worker_disabled) are
                            // retryable.
                            var result = ClassifyCrashResponse(response);
                            if (result.Success)
                            {
                                _stats.RecordCrashSent();
                            }
                            else
                            {
                                _stats.RecordFailure(result.Error);
                                SafeLog("shardpilot tombstone ingest failed: {0}", result.Error);
                                if (result.Retryable)
                                {
                                    // A tombstone record has NO spooled copy
                                    // — its retry IS the next launch's AEI
                                    // pass — so a retryable Retry-After must
                                    // ride the SAME persist-and-honor
                                    // mechanism the crash re-send path uses
                                    // (the shared scope marker the pass
                                    // checks before reopening a trace
                                    // stream), not fall on the ledger's
                                    // retryable path, which deliberately
                                    // leaves the record unsettled and knows
                                    // nothing of pacing. Consent-world guard
                                    // on BOTH epochs: a denial since this
                                    // dispatch condemned the record — and so
                                    // did a crash OPT-OUT, whose suppression
                                    // floor (stamped at opt-out, raised at
                                    // re-enable) settles it without a report
                                    // at the next pass. Either way the
                                    // record's backpressure dies with it: a
                                    // late 429 must not park the re-enabled
                                    // lane's FRESH records behind a deadline
                                    // earned by a condemned one.
                                    ReArmCrashResendBackoffIfCurrent(
                                        capturedDenialEpoch, capturedCrashEpoch, result.RetryAfter);
                                }
                            }

                            if (onCompleted != null)
                            {
                                try
                                {
                                    onCompleted(result);
                                }
                                catch
                                {
                                    // The caller's settle must never throw
                                    // back into the dispatcher.
                                }
                            }
                        }
                        finally
                        {
                            Interlocked.Decrement(ref _pendingCrashSends);
                            WakeWorker();
                        }
                    });
                }
                catch (Exception ex)
                {
                    // The send threw synchronously; its callback will never
                    // run, so release the in-flight count here.
                    Interlocked.Decrement(ref _pendingCrashSends);
                    _stats.RecordFailure(ex.Message);
                    SafeLog("shardpilot tombstone send failed: {0}", ex.Message);
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// The tombstone request's last-moment dispatch predicate (see
        /// <see cref="ShardPilotHttpRequest.BeforeSend"/>): true only while
        /// the consent world the upload was dispatched under still stands —
        /// not closed, same denial epoch, same crash-disable epoch, crash
        /// collection still allowed. Evaluated by the sender right before
        /// the network request object is created (on the dispatcher, after
        /// the synchronized handoff released the dispatch gate), so a
        /// revocation that completed in the enqueue-to-coroutine-start
        /// window still stops the dump-class bytes from leaving the device.
        /// </summary>
        private bool TombstoneDispatchStillCurrent(long denialEpochAtDispatch, long crashDisableEpochAtDispatch)
        {
            lock (_gate)
            {
                return !_closed &&
                       _consentDenialEpoch == denialEpochAtDispatch &&
                       _crashDisableEpoch == crashDisableEpochAtDispatch &&
                       CrashCollectionAllowedLocked;
            }
        }

        /// <summary>
        /// 1-in-N sampler for NON-FATAL crashes only. Returns true on every
        /// N-th call (N = <see cref="ShardPilotConfig.CrashSampleEvery"/>,
        /// clamped to >= 1 at config time). Fatal crashes never consult this.
        /// </summary>
        private bool ShouldEmitNonFatal(int every)
        {
            if (every == 0) return false;
            if (every <= 1)
            {
                return true;
            }

            var n = Interlocked.Increment(ref _nonFatalCrashCounter);
            return n % every == 0;
        }

        /// <summary>
        /// Write-ahead persistence for one captured crash report: appends the
        /// exact serialized wire payload to the durable crash spool on the
        /// capturing thread, so the report survives even if the process dies
        /// before (or during) the send attempt. When this client's crash
        /// spool was disowned by a successor (the SDK was re-initialized and
        /// a caller still holds this reference), the report is handed to the
        /// successor instead, under ITS consent gates — including when the
        /// disown lands mid-call and fails the own-spool append. Returns
        /// false ONLY on a consent refusal: a denial raced the own-spool
        /// persist (the just-written record is then removed again
        /// fail-closed, escalating to the wipe when the removal cannot be
        /// confirmed), or the successor holding the durable home is DENIED —
        /// either way the caller must drop the capture, live send included.
        /// A disabled spool or a failed write returns true: durability is
        /// best-effort and must never block the live send.
        /// <paramref name="undoToken"/> carries the own-spool append's undo
        /// handle for the close-race abort (null when the record went to a
        /// successor — that copy belongs to the successor's still-open
        /// client — or nowhere). <paramref name="durability"/> reports what
        /// this persist achieved (see
        /// <see cref="ShardPilotCrashCaptureDurability"/>); meaningful only
        /// when this method returns true — a false return means the capture
        /// is dropped and nothing durable remains.
        /// </summary>
        private bool PersistCrashReportAtCapture(
            string crashId,
            string payload,
            long epochBefore,
            long crashEpochBefore,
            out ShardPilotCrashSpool.AppendUndoToken undoToken,
            out ShardPilotCrashCaptureDurability durability)
        {
            undoToken = null;
            durability = ShardPilotCrashCaptureDurability.None;

            // A replaced client hands the report to its successor — a
            // refusal is the successor chain's DENIED consent, crash
            // opt-out, or missing crash key (a replaced client is off the
            // lifecycle registry and never receives propagated decisions
            // itself): the capture must abort its live send too, not just
            // lose the durable copy. This includes a replaced client that
            // itself ran WITHOUT a crash spool — its successor may have
            // one, and is the live consent authority either way.
            if (CrashReportRoutesToSuccessor())
            {
                // The durable state lives down the ownership chain and is
                // not knowable here: reported as NoSpool (best-effort) — see
                // the enum. The AEI pass runs on a freshly initialized
                // client, where no successor exists.
                durability = ShardPilotCrashCaptureDurability.NoSpool;
                return _spoolSuccessor.AcceptCrashReportHandoff(crashId, payload);
            }

            if (_crashSpool == null)
            {
                durability = ShardPilotCrashCaptureDurability.NoSpool;
                return true;
            }

            int evicted;
            var appended = _crashSpool.Append(crashId, payload, out evicted, out undoToken);
            _stats.RecordCrashSpoolDropped(evicted);
            if (appended)
            {
                _stats.RecordCrashSpooled();
                durability = ShardPilotCrashCaptureDurability.Spooled;
            }
            else
            {
                // The append can fail because the ownership handoff disowned
                // the spool between the check above and the append itself:
                // with the successor link now set, the report is handed over
                // instead of losing its durability.
                var lateSuccessor = _spoolSuccessor;
                if (lateSuccessor != null && _crashSpool.IsDisowned)
                {
                    durability = ShardPilotCrashCaptureDurability.NoSpool;
                    return lateSuccessor.AcceptCrashReportHandoff(crashId, payload);
                }

                durability = ShardPilotCrashCaptureDurability.SpoolAppendFailed;

                // Rejected for real (oversized, write failure, or disowned
                // with no successor): the capture keeps its live send, but it
                // will not survive to a next-launch re-send.
                _stats.RecordCrashSpoolDropped(1);
            }

            // Post-append recheck, the same epoch discipline as every
            // event-spool append: a SetConsent(false) may have wiped the file
            // BEFORE this append landed, which would leave a just-denied
            // subject's report on disk. The crash OPT-OUT is rechecked here
            // too — not only at the later pre-send abort — because a FATAL
            // crash may kill the process right after this write-ahead
            // persist: an opt-out-raced record left on disk would re-send
            // after a re-enable even though collection was already off.
            bool denialLanded;
            bool optOutLanded;
            lock (_gate)
            {
                denialLanded = _consentDenialEpoch != epochBefore;

                // Epoch, not boolean: a disable that raced this unlocked
                // append and was already re-enabled must still condemn the
                // record — a fatal crash may die right after this persist,
                // and the raced record would otherwise re-send later.
                optOutLanded = _crashDisableEpoch != crashEpochBefore || !CrashCollectionAllowedLocked;
            }

            if (!denialLanded && !optOutLanded)
            {
                return true;
            }

            if (appended)
            {
                if (denialLanded)
                {
                    RemoveDeniedCrashOrFailClosed(crashId);
                }
                else
                {
                    // Opt-out race: undo exactly what this append staged,
                    // restoring whatever its cap enforcement evicted — the
                    // enabled-period backlog stays durable, unlike a
                    // denial's wipe. FAIL-CLOSED: an undo that cannot be
                    // confirmed escalates (removal, then wipe) — the raced
                    // record must not survive to a later re-enable.
                    RemoveOptOutRacedCrashOrFailClosed(crashId, undoToken);
                }

                undoToken = null;
            }

            return false;
        }

        /// <summary>
        /// Hands the crash spool's in-memory backlog over to the successor
        /// after this client's spools were disowned at the successor's
        /// registration. The successor loaded the crash file BEFORE this
        /// client's construction-gap captures could land in it, so its
        /// in-memory list may be missing those late records — and its next
        /// rewrite (a settlement of the construction-time re-send, say)
        /// would drop them from the file. Forwarding the (now frozen)
        /// backlog through the successor's id-deduplicated append makes its
        /// memory a superset again: already-loaded ids no-op, gap records
        /// append. Called AFTER the disown, so no concurrent capture can
        /// slip between the snapshot and the freeze — a capture that lost
        /// the append race to the disown hands itself over (see
        /// PersistCrashReportAtCapture). Never throws.
        /// </summary>
        internal void ForwardCrashSpoolBacklogToSuccessor()
        {
            try
            {
                var successor = _spoolSuccessor;
                if (successor == null || _crashSpool == null || !_crashSpool.IsDisowned)
                {
                    return;
                }

                var records = _crashSpool.PendingRecords();
                for (var i = 0; i < records.Count; i++)
                {
                    if (!successor.AcceptCrashReportHandoff(records[i].Id, records[i].Payload))
                    {
                        // The successor chain REFUSED (denied consent — its
                        // wipe already condemned this scope's backlog —, a
                        // crash opt-out, or a no-crash-key successor): stop
                        // forwarding rather than re-offering refused records.
                        return;
                    }
                }
            }
            catch
            {
                // Best-effort: an unforwarded record is re-sent on a later
                // launch (if the successor's rewrites keep it) or lost with
                // its live-send attempt — never an exception into the
                // lifecycle registry.
            }
        }

        /// <summary>
        /// Accepts a displaced predecessor's crash report into THIS client's
        /// crash spool (id-deduplicated, with this client's own consent
        /// gates), so a report captured through a stale reference after a
        /// re-initialization stays durable. Forwards down the (acyclic)
        /// succession chain when this client was itself displaced meanwhile.
        /// Returns false when the handoff must be REFUSED: this client's
        /// consent posture is DENIED (at entry, or a denial raced the
        /// append), its crash plane is opted out, or it was initialized
        /// without a <see cref="ShardPilotConfig.CrashKey"/>. The consent
        /// subject is the same scope the predecessor serves, but a replaced
        /// client is no longer in the lifecycle registry and never receives
        /// propagated decisions — so the refusal must flow back for the
        /// predecessor to abort its own live send too, not just lose the
        /// durable copy. Every other no-store outcome (spool disabled with a
        /// key present, duplicate, failed write) returns true: durability is
        /// best-effort and must not suppress the live send.
        /// </summary>
        internal bool AcceptCrashReportHandoff(string crashId, string payload)
        {
            if (string.IsNullOrEmpty(crashId) || string.IsNullOrEmpty(payload))
            {
                return true;
            }

            // A replaced client routes the handoff onward — even one that
            // ran without a crash spool: its consent state is stale (it is
            // off the lifecycle registry), so it must neither keep nor judge
            // the report.
            if (CrashReportRoutesToSuccessor())
            {
                return _spoolSuccessor.AcceptCrashReportHandoff(crashId, payload);
            }

            // The whole accept — gate check, write-ahead append, post-append
            // recheck — runs under THIS client's dispatch gate, mirroring the
            // own-spool capture path: once this client's opt-out call returns,
            // no handoff append can land afterwards, and a fatal crash mid-
            // append cannot strand a record behind a completed opt-out (the
            // blocked disable never returned).
            lock (_crashDispatchGate)
            {
                // The consent gate comes BEFORE the no-spool early-out: a
                // successor that runs without a crash spool (disabled, or no
                // crash key) but has DENIED consent must still refuse the
                // handoff — the refusal is what aborts the stale predecessor's
                // live send, and that predecessor never receives propagated
                // denials itself. The successor's crash opt-out refuses for the
                // same reason: it is the live authority for the crash plane, so
                // a disabled successor must stop the predecessor's capture (live
                // send included), not just decline the durable copy.
                long epochBefore;
                long crashEpochBefore;
                lock (_gate)
                {
                    if (ShardPilotConsent.IsDeniedState(_consentState) || !CrashCollectionAllowedLocked)
                    {
                        return false;
                    }

                    epochBefore = _consentDenialEpoch;
                    crashEpochBefore = _crashDisableEpoch;
                }

                // A successor initialized WITHOUT a crash key has the crash
                // plane disabled by configuration — and as the durable home it
                // would also anchor the stale predecessor's live send, whose
                // request is built from THIS client's endpoint and (empty)
                // crash key. Reinitializing without a key is documented as
                // turning crash capture off, so a crash caught through the old
                // reference during the handoff window must not put crash bytes
                // on the wire either: refuse like an opt-out instead of taking
                // the generic no-spool best-effort path below.
                if (string.IsNullOrEmpty(_config.CrashKey))
                {
                    return false;
                }

                if (_crashSpool == null)
                {
                    return true;
                }

                int evicted;
                ShardPilotCrashSpool.AppendUndoToken handoffUndoToken;
                var appended = _crashSpool.Append(crashId, payload, out evicted, out handoffUndoToken);
                if (appended)
                {
                    _stats.RecordCrashSpooled();
                }
                else
                {
                    // The append can fail because THIS client was itself replaced
                    // mid-handoff (its spool disowned between the entry check and
                    // the append): with the new successor link set, pass the
                    // report down the chain instead of losing its durability —
                    // the same late-disown fallback the capture path has.
                    var lateSuccessor = _spoolSuccessor;
                    if (lateSuccessor != null && _crashSpool.IsDisowned)
                    {
                        return lateSuccessor.AcceptCrashReportHandoff(crashId, payload);
                    }
                }

                _stats.RecordCrashSpoolDropped(evicted);

                // The same post-append recheck as the own-spool persist: a
                // denial OR a crash opt-out racing this unlocked append must not
                // leave the record behind — the predecessor's process may die
                // (fatal crash) before its later dispatch guard could remove it.
                bool denialLanded;
                bool optOutLanded;
                lock (_gate)
                {
                    denialLanded = _consentDenialEpoch != epochBefore;

                    // Epoch, not boolean: same disable+re-enable race as the
                    // own-spool persist recheck.
                    optOutLanded = _crashDisableEpoch != crashEpochBefore || !CrashCollectionAllowedLocked;
                }

                if (denialLanded || optOutLanded)
                {
                    if (appended)
                    {
                        if (denialLanded)
                        {
                            RemoveDeniedCrashOrFailClosed(crashId);
                        }
                        else
                        {
                            // Opt-out race: undo exactly this append, keeping
                            // the enabled-period backlog durable — escalating
                            // fail-closed when the undo cannot be confirmed.
                            RemoveOptOutRacedCrashOrFailClosed(crashId, handoffUndoToken);
                        }
                    }

                    return false;
                }

                return true;
            }
        }

        /// <summary>
        /// Fail-closed removal for a crash append raced by an OPT-OUT: the
        /// undo is attempted first (it restores whatever the append's cap
        /// enforcement evicted), then a direct removal, and when neither can
        /// be CONFIRMED on disk the spool is wiped — an opt-out-raced record
        /// must not survive to a re-enable within the retention window, and
        /// on the double-failure path (disk-level breakage) destroying the
        /// enabled-period backlog is the fail-closed price of that bound.
        /// A reinitialization can DISOWN this spool (forwarding its backlog,
        /// the raced record included) between the caller's route check and
        /// this cleanup: undo, removal, and wipe all no-op on the disowned
        /// instance, so the cleanup follows the CURRENT durable home instead
        /// — and purges the orphaned frozen file when the chain ends without
        /// a crash spool. Internal for the regression test.
        /// </summary>
        internal void RemoveOptOutRacedCrashOrFailClosed(string crashId, ShardPilotCrashSpool.AppendUndoToken undoToken)
        {
            if (_crashSpool == null)
            {
                return;
            }

            if (undoToken != null && _crashSpool.UndoAppend(undoToken))
            {
                return;
            }

            if (_crashSpool.Remove(crashId))
            {
                return;
            }

            if (_crashSpool.IsDisowned)
            {
                // The record was forwarded down the chain when the spool was
                // disowned — remove it where it lives NOW, or the successor
                // would keep (and later re-send) a capture that was rejected
                // because an opt-out raced it.
                var home = ResolveCrashDurableHome();
                if (!ReferenceEquals(home, this))
                {
                    home.RemoveCrashRecordForRejectedCapture(crashId);
                }

                RemoveRejectedCaptureFromOrphanedSpoolFile(crashId);
                return;
            }

            _crashSpool.Wipe();
        }

        /// <summary>
        /// Denial-recheck removal for the crash spool: undoes a crash-report
        /// persist that raced a consent denial and, when the removal cannot
        /// be CONFIRMED on disk, escalates to the fail-closed consent wipe
        /// (truthful delete; on failure the path-scoped "wipe owed" marker
        /// plus the instance tombstone) — so a just-denied subject's report
        /// can never outlive the denial on disk unnoticed.
        /// </summary>
        private void RemoveDeniedCrashOrFailClosed(string crashId)
        {
            if (_crashSpool == null)
            {
                return;
            }

            if (!_crashSpool.Remove(crashId))
            {
                _crashSpool.Wipe();
            }
        }

        /// <summary>
        /// Settles a crash report's durable copy against its send outcome:
        /// an acknowledged (2xx, including accepted-but-suppressed) or
        /// permanently rejected (non-retryable 4xx — it would fail forever)
        /// report is removed; a retryable failure keeps the record for a
        /// later attempt, and a server Retry-After is persisted so the
        /// backpressure gates the re-send pass even across a relaunch.
        /// Settles a crash report's durable copy against its send outcome,
        /// anchored on the consent world that governed the DISPATCH:
        /// <paramref name="homeAtSend"/> is the client whose spool held the
        /// record when the request left (the sender itself, or the durable
        /// home a handed-off record lived in), and
        /// <paramref name="homeEpochAtSend"/> is THAT client's denial epoch
        /// read at the same moment — epoch counters are per-client, so an
        /// epoch may only ever be compared against the instance it was read
        /// from.
        ///
        /// An acknowledged (2xx, including accepted-but-suppressed) or
        /// permanently rejected (non-retryable 4xx) report is removed
        /// wherever it lives NOW (chain walk from the anchor — a
        /// replacement may have forwarded it since); removal is right in
        /// every consent state (a no-op after a denial's wipe). A RETRYABLE
        /// failure may re-arm backoff only on the anchor itself, and only
        /// while the anchor's consent world is unchanged — a denial since
        /// the dispatch (even one followed by a re-grant) condemned the
        /// record this response answers, and its backpressure must not gate
        /// fresh post-grant reports; an anchor that was itself displaced
        /// since the send skips the re-arm (a later pass earns its own).
        /// </summary>
        private static void SettleCrashReport(
            ShardPilotClient homeAtSend, long homeEpochAtSend, string crashId, ShardPilotResult result)
        {
            if (homeAtSend == null || result == null)
            {
                return;
            }

            if (result.Success || !result.Retryable)
            {
                var home = homeAtSend.ResolveCrashDurableHome();
                if (home._crashSpool != null)
                {
                    // A failed removal is benign here: the committed memory
                    // filters the id out of every later successful rewrite,
                    // and a re-sent leftover is de-duplicated (or
                    // re-rejected) by the crash ingest service.
                    home._crashSpool.Remove(crashId);
                }

                if (!result.Success)
                {
                    homeAtSend.SafeLog(
                        "shardpilot crash report permanently rejected ({0}); it will not be re-sent", result.Error);
                }

                return;
            }

            homeAtSend.ReArmCrashResendBackoffIfCurrent(homeEpochAtSend, result.RetryAfter);
        }

        /// <summary>
        /// Re-arms the crash re-send backoff after a retryable failure —
        /// but ONLY while this client's consent world is exactly the one
        /// the failed request was dispatched under (same denial epoch, not
        /// denied) and this client's durable home was not handed to a
        /// successor. Anything else means the record the response answers
        /// was condemned or moved: stale backpressure must not gate fresh
        /// reports, and a later pass's own failure earns its own backoff.
        /// The TOMBSTONE caller additionally passes the crash-disable epoch
        /// captured at dispatch: a crash OPT-OUT condemns a tombstone record
        /// too (its suppression floor settles the record without a report at
        /// the next pass), so a moved crash epoch skips the re-arm exactly
        /// like a moved denial epoch — while the JSON callers stay
        /// unguarded there (<see cref="CrashDisableEpochUnguarded"/>),
        /// because their spooled backlog SURVIVES an opt-out and its
        /// backpressure legitimately keeps governing it.
        /// A spool-LESS configuration still re-arms: the persisted marker's
        /// consumers include the tombstone lane's next-launch AEI pass,
        /// which holds no spool by design (raw dump-class bytes never touch
        /// one) and reads the marker before reopening a trace stream. The
        /// persisted marker records only the (clamped) SERVER deadline; the
        /// flush-interval floor is in-session pacing, mirroring the
        /// analytics worker's retry wait.
        /// </summary>
        private void ReArmCrashResendBackoffIfCurrent(long epochAtSend, TimeSpan rawRetryAfter)
        {
            // The JSON-lane callers deliberately do NOT guard on the crash
            // disable epoch: an opt-out does not condemn the SPOOLED backlog
            // (it stays durable and legitimately re-sends if re-enabled
            // within retention), so server backpressure earned by one of its
            // sends keeps governing it across an opt-out/re-enable cycle.
            ReArmCrashResendBackoffIfCurrent(epochAtSend, CrashDisableEpochUnguarded, rawRetryAfter);
        }

        /// <summary>
        /// Sentinel for <see cref="ReArmCrashResendBackoffIfCurrent(long, long, TimeSpan)"/>:
        /// the caller's records survive a crash opt-out, so the re-arm must
        /// not be skipped on a moved crash-disable epoch. The real epoch
        /// counter starts at zero and only increments — it can never collide.
        /// </summary>
        private const long CrashDisableEpochUnguarded = -1L;

        private void ReArmCrashResendBackoffIfCurrent(long epochAtSend, long crashDisableEpochAtSend, TimeSpan rawRetryAfter)
        {
            lock (_gate)
            {
                if (ShardPilotConsent.IsDeniedState(_consentState) || _consentDenialEpoch != epochAtSend ||
                    (crashDisableEpochAtSend != CrashDisableEpochUnguarded &&
                     _crashDisableEpoch != crashDisableEpochAtSend))
                {
                    return;
                }
            }

            // A DISOWNED spool means a successor owns the backlog (and the
            // scope marker) now — its own pass earns its own backoff. A
            // NULL spool does NOT short-circuit: the spool-less tombstone
            // retry honors this marker too (see the summary above).
            if (_crashSpool != null && _crashSpool.IsDisowned)
            {
                return;
            }

            // Test seam: lets a test land a SetConsent(false) between the
            // guard above and the marker write below. No-op in production.
            var preMarkerHook = CrashReArmPreMarkerHookForTesting;
            if (preMarkerHook != null)
            {
                preMarkerHook();
            }

            // Clamped (one day max) BEFORE deriving any deadline: the
            // persisted marker clamps on write anyway, but the in-memory
            // deadline must not let an absurd header park re-send for the
            // whole session either.
            var retryAfter = ShardPilotCrashSpool.ClampRetryAfter(rawRetryAfter);
            if (retryAfter > TimeSpan.Zero)
            {
                ShardPilotCrashSpool.WriteRetryAfterDeadline(_settingsStore, _config, DateTime.UtcNow + retryAfter);
            }

            // The marker write above ran unlocked, so a denial can land
            // inside it — AFTER SetConsent(false) already cleared the marker
            // for the condemned backlog. Recheck under _gate: on a moved
            // world the just-written marker is cleared again (write-then-
            // recheck-then-undo, the same discipline as the spool appends)
            // and the in-memory deadline is left untouched; otherwise the
            // bump happens in this same lock hold, so it can never follow a
            // denial's reset.
            // Floored on the CRASH plane's own constant, not on the flush
            // interval. A crash re-send is the one retry that must not slow
            // down when an integrator lengthens the idle analytics cadence:
            // the incident being reported is exactly the moment the SDK
            // exists for. The value reproduces the floor the old default
            // actually delivered (1s), independent of the knob.
            var floor = TimeSpan.FromSeconds(CrashResendRetryFloorSeconds);
            var wait = retryAfter > floor ? retryAfter : floor;
            var rearmDeadline = DateTime.UtcNow + wait;
            var condemnedMeanwhile = false;
            lock (_gate)
            {
                if (ShardPilotConsent.IsDeniedState(_consentState) || _consentDenialEpoch != epochAtSend ||
                    (crashDisableEpochAtSend != CrashDisableEpochUnguarded &&
                     _crashDisableEpoch != crashDisableEpochAtSend))
                {
                    condemnedMeanwhile = true;
                }
                else if (rearmDeadline > _crashResendNotBefore)
                {
                    _crashResendNotBefore = rearmDeadline;
                }
            }

            if (condemnedMeanwhile && retryAfter > TimeSpan.Zero)
            {
                ShardPilotCrashSpool.ClearRetryAfterDeadline(_settingsStore, _config);
            }
        }

        /// <summary>
        /// Advances the consent-denial epoch INSIDE the experiment egress
        /// gate (caller holds <c>_gate</c>; ordering gate → egress gate is
        /// this pairing's only nesting). The experiments consumer holds the
        /// same gate across its final pre-send epoch re-read and the
        /// transport handoff, so the advance either completes first — and
        /// the consumer's gated re-read refuses with zero wire traffic — or
        /// waits for an in-progress handoff to reach the transport, making
        /// that request pre-decision traffic the settle path fails closed.
        /// Either way, once the advance is observable no assignment request
        /// egresses (the granted-only contract at the wire).
        /// </summary>
        private void AdvanceConsentDenialEpochLocked()
        {
            lock (_experimentEgressGate)
            {
                // Interlocked, not a plain increment: the experiments
                // consumer reads this epoch lock-free via Interlocked.Read,
                // and on 32-bit Unity/Mono targets a plain long write is not
                // atomic — a torn write could hide a deny (or a deny→grant
                // flip) from an in-flight epoch comparison. The interlocked
                // pair makes the advance indivisible for those readers;
                // in-gate readers are unaffected (every write still happens
                // inside this hold, under the caller's _gate).
                Interlocked.Increment(ref _consentDenialEpoch);
                // Every consent-loss path shares this boundary. Forget the
                // lifecycle and its pause without emitting an end; a later
                // permitted activity or resume must announce a fresh session.
                // Replacing the whole value also invalidates provisional start
                // rollback and owed-start claims from the withdrawn session.
                var needsFreshStart = !string.IsNullOrEmpty(_sessionState.Id) || _sessionState.OwesStart;
                _sessionState = new SessionState(string.Empty, 0, needsFreshStart);
                _sessionPause = null;
            }
        }

        /// <summary>
        /// Atomically snapshots this client's consent world for a CRASH
        /// dispatch decision: false when analytics consent is DENIED or the
        /// crash plane's own opt-out is in force (the dispatch must be
        /// dropped); true otherwise, with the denial epoch read in the SAME
        /// lock hold — so a verdict can never be paired with an epoch from
        /// a different consent world.
        /// </summary>
        private bool TrySnapshotConsentForDispatch(out long epoch, out long crashEpoch)
        {
            lock (_gate)
            {
                epoch = _consentDenialEpoch;
                crashEpoch = _crashDisableEpoch;

                // A home initialized WITHOUT a crash key refuses here too:
                // it can never lend a valid credential, and a dispatch
                // anchored on it would ship with an empty Bearer. This
                // covers the LOCAL-append ordering the handoff refusal
                // cannot: a no-key successor registering after the
                // predecessor's own-spool append but before this snapshot
                // becomes the durable home without any handoff call.
                return !ShardPilotConsent.IsDeniedState(_consentState)
                    && CrashCollectionAllowedLocked
                    && !string.IsNullOrEmpty(_config.CrashKey);
            }
        }

        /// <summary>
        /// True when crash-report duties (persistence, settlement, consent
        /// authority) belong to this client's successor rather than to this
        /// client itself: it was replaced (a successor link exists) and its
        /// own crash spool is disowned — or it never had one. A replaced
        /// client is off the lifecycle registry and its consent state goes
        /// stale, so a chain hop must NOT stop at it just because it ran
        /// without a crash spool.
        /// </summary>
        private bool CrashReportRoutesToSuccessor()
        {
            return _spoolSuccessor != null && (_crashSpool == null || _crashSpool.IsDisowned);
        }

        /// <summary>
        /// Walks the (acyclic) succession chain to the client that currently
        /// owns the crash-report durable home: the first client that does
        /// not route crash duties onward — including THROUGH replaced
        /// intermediates that ran without a crash spool. A forwarded record
        /// always lives — and is consent-governed — there.
        /// </summary>
        private ShardPilotClient ResolveCrashDurableHome()
        {
            var client = this;
            while (client.CrashReportRoutesToSuccessor())
            {
                client = client._spoolSuccessor;
            }

            return client;
        }

        /// <summary>
        /// Whether a captured crash report's write-ahead record is STILL
        /// pending in the durable home's crash spool — the AEI pass's
        /// end-of-pass settlement probe: a pass-appended
        /// record that a LATER append in the SAME pass evicted (and that no
        /// ingest ack settled) must not be cursor-settled, or the eviction
        /// would lose the ANR permanently once its live send fails.
        /// Chain-aware like the settlement paths: a mid-pass replacement may
        /// have forwarded the backlog to a successor's spool. Never throws.
        /// </summary>
        internal bool CrashReportRemainsDurablySpooled(string crashId)
        {
            try
            {
                if (string.IsNullOrEmpty(ShardPilotConfig.Trim(crashId)))
                {
                    return false;
                }

                var home = ResolveCrashDurableHome();
                var spool = home == null ? null : home._crashSpool;
                return spool != null && spool.ContainsPendingRecord(crashId);
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Removes a handed-off record whose originating CAPTURE was
        /// rejected (the predecessor's close won the race): a CaptureCrash
        /// that returned false must not deliver on a later launch, no matter
        /// which spool the write-ahead copy landed in. Walks to the CURRENT
        /// durable home itself — the home the caller resolved may have been
        /// replaced (and its backlog forwarded) in the meantime — and
        /// follows a disown that lands mid-removal. Best-effort — a failed
        /// removal leaves a real crash report the ingest service
        /// de-duplicates.
        /// </summary>
        /// <summary>
        /// Takes this client's live EVENT spool out of play when a consent
        /// sweep of its directory remains OWED: the next launch retries that
        /// sweep over the WHOLE directory, so anything appended to the live
        /// file after this point — post-grant events included — would be
        /// destroyed by the retry. Tombstoning the instance makes every
        /// later append/rewrite a no-op; offline durability is lost for the
        /// session, the fail-closed price of the owed sweep. No-op when the
        /// sweep completed.
        /// </summary>
        private void QuarantineSpoolIfSweepOwed(bool sweepCompleted)
        {
            lock (_gate) _consentEventPurgeOwed = !sweepCompleted;

            if (sweepCompleted || _spool == null)
            {
                return;
            }

            _spool.Disown();
            SafeLog("shardpilot spool disabled for this session: a consent sweep of its directory is still owed");
        }

        /// <summary>
        /// The crash-plane twin of <see cref="QuarantineSpoolIfSweepOwed"/>:
        /// a crash report persisted after this point would be destroyed by
        /// the owed sweep's retry on the next launch, so write-ahead
        /// durability is suspended for the session (live sends continue).
        /// </summary>
        private void QuarantineCrashSpoolIfSweepOwed(bool sweepCompleted)
        {
            lock (_gate) _consentCrashPurgeOwed = !sweepCompleted;

            if (sweepCompleted || _crashSpool == null)
            {
                return;
            }

            _crashSpool.Disown();
            SafeLog("shardpilot crash spool disabled for this session: a consent sweep of its directory is still owed");
        }

        /// <summary>
        /// Purges a rejected capture's write-ahead record from THIS client's
        /// now-disowned crash spool FILE when the succession chain ends in a
        /// client WITHOUT a crash spool (a no-crash-key reinitialization).
        /// In that topology the refusal aborted the capture, but the record
        /// predates the disown, the forward to the successor was refused,
        /// and no live spool will ever settle or prune the orphaned file —
        /// a later keyed initialization of this scope would load and send a
        /// report whose CaptureCrash returned false. Rewriting the frozen
        /// file is safe here precisely because that chain owns no crash
        /// spool: nothing else holds the path. A no-op in every other
        /// topology (a keyed successor owns the file and received the
        /// forwarded record — removal happens there via
        /// <see cref="RemoveCrashRecordForRejectedCapture"/>).
        /// </summary>
        private void RemoveRejectedCaptureFromOrphanedSpoolFile(string crashId)
        {
            var spool = _crashSpool;
            if (spool == null || !spool.IsDisowned)
            {
                return;
            }

            var home = ResolveCrashDurableHome();
            if (ReferenceEquals(home, this) || home._crashSpool != null)
            {
                return;
            }

            spool.RemoveRefusedCaptureRecord(crashId);
        }

        internal void RemoveCrashRecordForRejectedCapture(string crashId)
        {
            if (CrashReportRoutesToSuccessor())
            {
                _spoolSuccessor.RemoveCrashRecordForRejectedCapture(crashId);
                return;
            }

            if (_crashSpool == null)
            {
                return;
            }

            if (!_crashSpool.Remove(crashId) && CrashReportRoutesToSuccessor())
            {
                // The spool was disowned between the routing check and the
                // removal: the record (if it survived) was forwarded to the
                // successor — follow it down the (acyclic) chain.
                _spoolSuccessor.RemoveCrashRecordForRejectedCapture(crashId);
            }
        }

        /// <summary>
        /// Starts one serial re-send pass over the spooled crash reports when
        /// due: kicked at construction (crash reports go out before the
        /// analytics worker's first flush interval elapses — the most
        /// valuable signal gets the first shot at the network if this process
        /// dies again) and re-armed from the worker loop once a backoff or a
        /// persisted Retry-After deadline elapses. One pass at a time; each
        /// report is a single POST of the verbatim persisted payload, removed
        /// on acknowledgment as the pass advances, so a kill mid-pass loses
        /// nothing and a partial delivery resumes on the next launch. A
        /// report whose original live send is still in flight may be sent
        /// twice across a pass boundary — the stable crash_id de-duplicates
        /// it server-side. Never throws.
        /// </summary>
        private void MaybeStartCrashResendPass()
        {
            if (_crashSpool == null || _httpSender == null)
            {
                return;
            }

            // A DISOWNED spool means a successor owns the backlog now (the
            // registration forwarded it): the successor re-sends under ITS
            // consent gates, while this displaced client is off the
            // lifecycle registry and would never see a denial the successor
            // records later — so it must not keep re-sending pre-denial
            // reports from its stale snapshot.
            if (_crashSpool.IsDisowned)
            {
                return;
            }

            // The retention window holds IN-SESSION too, not just at load:
            // prune expired records before arming a pass, so reports that
            // aged past the window since the construction-time load (a long
            // crash opt-out followed by a re-enable, say) can never re-send.
            // Runs before the disabled check below, so even a disabled
            // client keeps enforcing retention while the process lives.
            // Cheap no-op when nothing expired; spool lock is a leaf lock.
            _crashSpool.PruneExpiredRecords();

            long epoch;
            lock (_gate)
            {
                // The crash opt-out gates the re-send pass exactly like a
                // denial does: while disabled, the pending backlog is not
                // re-sent (it stays durable and ages out under the spool's
                // retention window; entries captured while enabled re-send
                // only if re-enabled within it).
                if (_closed || _crashResendActive || ShardPilotConsent.IsDeniedState(_consentState) ||
                    !CrashCollectionAllowedLocked)
                {
                    return;
                }

                if (DateTime.UtcNow < _crashResendNotBefore)
                {
                    return;
                }

                if (!_crashSpool.HasPendingRecords)
                {
                    return;
                }

                _crashResendActive = true;
                epoch = _consentDenialEpoch;
            }

            var records = _crashSpool.PendingRecords();
            if (records.Count == 0)
            {
                lock (_gate)
                {
                    _crashResendActive = false;
                }

                return;
            }

            SafeLog("shardpilot crash spool: re-sending {0} undelivered crash report(s)", records.Count);
            SendCrashResendStep(records, 0, epoch);
        }

        internal void MaybeStartCrashResendPassForTesting()
        {
            MaybeStartCrashResendPass();
        }

        // Test seam for the crash re-send re-arm's PACING only. It drives the
        // same method the send path calls, at the current consent epoch, so a
        // test can observe the deadline the floor produces without staging a
        // whole crash upload.
        internal void ReArmCrashResendBackoffForTesting(TimeSpan rawRetryAfter)
        {
            ReArmCrashResendBackoffIfCurrent(_consentDenialEpoch, rawRetryAfter);
        }

        internal DateTime CrashResendNotBeforeForTesting
        {
            get
            {
                lock (_gate)
                {
                    return _crashResendNotBefore;
                }
            }
        }

        internal long PendingCrashSendsForTesting
        {
            get { return Interlocked.Read(ref _pendingCrashSends); }
        }

        /// <summary>
        /// Caps a worker wait so the worker also wakes when the crash
        /// re-send pass becomes due: with a persisted Retry-After deadline
        /// shorter than the flush interval (say 5 seconds against a 1-hour
        /// interval), a plain flush-interval sleep would leave the pass
        /// un-armed until the next unrelated wake. Only an ACTIVE deferral
        /// caps the wait — no pending reports, a running pass, a denied
        /// consent, or an elapsed deadline leave the wait unchanged, so this
        /// can never turn the worker into a busy loop. (The in-session
        /// backoff a failed pass sets is always at least a flush interval,
        /// so a sub-interval cap can only come from the one-time persisted
        /// deadline read at construction.)
        /// </summary>
        private TimeSpan CapWaitToCrashResendDeadline(TimeSpan wait)
        {
            if (_crashSpool == null || _httpSender == null || _crashSpool.IsDisowned)
            {
                return wait;
            }

            DateTime notBefore;
            lock (_gate)
            {
                if (_closed || _crashResendActive || ShardPilotConsent.IsDeniedState(_consentState) ||
                    !CrashCollectionAllowedLocked)
                {
                    return wait;
                }

                notBefore = _crashResendNotBefore;
            }

            if (notBefore == DateTime.MinValue)
            {
                return wait;
            }

            var untilDue = notBefore - DateTime.UtcNow;
            if (untilDue >= wait)
            {
                return wait;
            }

            if (!_crashSpool.HasPendingRecords)
            {
                return wait;
            }

            // A deadline that elapsed between the loop-top re-arm check and
            // this wait still gets one quick re-loop instead of a full sleep.
            var floor = TimeSpan.FromMilliseconds(10);
            return untilDue < floor ? floor : untilDue;
        }

        /// <summary>
        /// Sends the <paramref name="index"/>-th snapshot record and, from
        /// its completion callback, advances to the next: strictly one crash
        /// request in flight, so a 429/Retry-After stops the whole pass
        /// instead of racing N requests into backpressure. Consent and close
        /// are rechecked before every step; each send is counted in the
        /// in-flight crash-send counter so flush/close wait for it (bounded).
        /// </summary>
        private void SendCrashResendStep(List<ShardPilotCrashSpool.CrashSpoolRecord> records, int index, long epoch)
        {
            if (index >= records.Count)
            {
                lock (_gate)
                {
                    _crashResendActive = false;
                }

                return;
            }

            var record = records[index];

            // The pass's snapshot can outlive a retention prune that ran
            // while it was in flight (the prune only guards NEW passes): a
            // record that has expired by DISPATCH time is skipped, so the
            // retention bound holds per-step, not just per-arming — a long
            // or backpressured pass cannot POST a report the maintenance
            // prune just removed.
            if (ShardPilotCrashSpool.IsRecordExpired(record.Id, DateTime.UtcNow))
            {
                SendCrashResendStep(records, index + 1, epoch);
                return;
            }

            // A disown landing mid-pass hands the backlog to a successor:
            // stop before the next request — the successor re-sends under
            // its own consent gates (this displaced client would never see
            // a denial recorded there). Checked outside _gate; the one
            // request that may already be in flight settles through the
            // succession chain.
            if (_crashSpool.IsDisowned)
            {
                lock (_gate)
                {
                    _crashResendActive = false;
                }

                return;
            }

            string replayPayload;
            if (!ShardPilotJsonParser.TryMigrateCrashComponent(record.Payload, out replayPayload))
            {
                // Retain ambiguous or malformed records rather than dropping or
                // dispatching a payload whose component cannot be established.
                SendCrashResendStep(records, index + 1, epoch);
                return;
            }

            // Built BEFORE the consent/close recheck, so the recheck-to-
            // dispatch window is as small as it can be without IO under the
            // lock. Only the retired root source key is renamed; the
            // stable crash_id still de-duplicates a
            // report that was delivered but not observed as acknowledged.
            var request = new ShardPilotHttpRequest
            {
                Method = "POST",
                Url = _crashEndpoint,
                Bearer = _config.CrashKey,
                ContentType = "application/json",
                Body = replayPayload,
                Timeout = _config.HttpTimeout
            };

            // The recheck-to-dispatch step runs under the crash dispatch
            // gate (see _crashDispatchGate): a disable acquires it after
            // flipping the flag, so once SetCrashReportingEnabled(false)
            // returns, no further re-send dispatch can pass this recheck.
            lock (_crashDispatchGate)
            {
                // A denial landing after this hold while the request is already
                // on its way is enforced server-side (the ingest service
                // suppresses storage for a denied subject), the same residual
                // window the analytics publish path documents.
                lock (_gate)
                {
                    if (_closed || epoch != _consentDenialEpoch || !CrashCollectionAllowedLocked)
                    {
                        // A denial already wiped the spool; a close already ran
                        // its final drain; a crash opt-out landing mid-pass stops
                        // it BEFORE the next dispatch (the records stay durable
                        // for a possible re-enable within the retention window).
                        // Either way this pass is over.
                        _crashResendActive = false;
                        return;
                    }

                    Interlocked.Increment(ref _pendingCrashSends);
                }

                try
                {
                    _httpSender.Send(request, response =>
                    {
                        var advance = false;
                        try
                        {
                            var result = ClassifyCrashResponse(response);
                            if (result.Success)
                            {
                                _stats.RecordCrashSent();
                                // Chain-aware settlement (not a direct Remove):
                                // if a replacement disowned this spool while the
                                // request was in flight, the forwarded durable
                                // copy lives in the SUCCESSOR's spool and must be
                                // removed there.
                                SettleCrashReport(this, epoch, record.Id, result);
                                advance = true;
                            }
                            else if (!result.Retryable)
                            {
                                _stats.RecordFailure(result.Error);
                                SettleCrashReport(this, epoch, record.Id, result);
                                advance = true;
                            }
                            else
                            {
                                // Retryable: stop the pass, keep this and the
                                // remaining records durable. Back off at least a
                                // flush interval (mirroring the analytics worker's
                                // retry wait); a server Retry-After is persisted
                                // so the backpressure survives a relaunch. The
                                // worker re-arms the pass once the deadline
                                // elapses.
                                _stats.RecordFailure(result.Error);
                                SafeLog("shardpilot crash re-send failed ({0}); {1} report(s) kept for a later attempt",
                                    result.Error, records.Count - index);

                                // Re-arm through the shared guard: a denial that
                                // landed while this request was in flight (or a
                                // disown handing the backlog to a successor)
                                // condemned or moved the record — its
                                // backpressure must not be re-armed over
                                // post-re-grant or successor-owned reports.
                                ReArmCrashResendBackoffIfCurrent(epoch, result.RetryAfter);
                                lock (_gate)
                                {
                                    _crashResendActive = false;
                                }
                            }
                        }
                        finally
                        {
                            Interlocked.Decrement(ref _pendingCrashSends);
                            WakeWorker();
                        }

                        if (advance)
                        {
                            SendCrashResendStep(records, index + 1, epoch);
                        }
                    });
                }
                catch (Exception ex)
                {
                    // The send threw synchronously; its callback will never run.
                    Interlocked.Decrement(ref _pendingCrashSends);
                    _stats.RecordFailure(ex.Message);
                    SafeLog("shardpilot crash re-send failed: {0}", ex.Message);
                    lock (_gate)
                    {
                        // The crash plane's OWN floor, not the analytics flush
                        // interval. A synchronous throw is the same failure as
                        // a callback-reported one — the send did not land —
                        // and it must not be the one shape whose retry slows
                        // down because an integrator lengthened an unrelated
                        // batching cadence. Reading Config.FlushInterval here
                        // meant the new 15s default delayed it fifteen
                        // seconds, and an hour-long cadence delayed it an hour.
                        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(CrashResendRetryFloorSeconds);
                        if (deadline > _crashResendNotBefore)
                        {
                            _crashResendNotBefore = deadline;
                        }

                        _crashResendActive = false;
                    }
                }
            }
        }

        /// <summary>
        /// Maps a raw crash-ingest HTTP response onto a result. A 2xx is
        /// success; a transport failure, 429, or 5xx is retryable. An exact
        /// 400/404 kind_not_enabled refusal is retryable on this crash lane;
        /// other non-2xx refusals remain permanent.
        /// </summary>
        private static ShardPilotResult ClassifyCrashResponse(ShardPilotHttpResponse response)
        {
            if (response == null)
            {
                return new ShardPilotResult(false, "no response", true);
            }

            if (response.TransportError)
            {
                return new ShardPilotResult(false, "connection failed", true);
            }

            if (response.StatusCode >= 200 && response.StatusCode < 300)
            {
                return ParseAcceptedCrashResponse(response.Body);
            }

            var disabledKind = (response.StatusCode == 400 || response.StatusCode == 404) &&
                string.Equals(ShardPilotUnityWebRequestTransport.BuildStatusException(
                    (int)response.StatusCode, null, response.Body).ErrorCode,
                    "kind_not_enabled", StringComparison.Ordinal);
            var retryable = disabledKind || response.StatusCode == 408 || response.StatusCode == 429 || response.StatusCode >= 500;
            var retryAfter = response.RetryAfterSeconds > 0
                ? TimeSpan.FromSeconds(response.RetryAfterSeconds)
                : TimeSpan.Zero;
            return new ShardPilotResult(false, disabledKind ? "kind_not_enabled" : "status " + response.StatusCode,
                retryable, retryAfter);
        }

        /// <summary>
        /// Surfaces the server's per-crash response from an accepted (2xx) send: a
        /// <c>suppressed</c> crash was accepted but NOT stored (the actor withheld
        /// consent), and <c>warnings</c> are non-fatal processing notices. A 2xx with an
        /// empty or unparseable body is still a normal acceptance.
        /// </summary>
        private static ShardPilotResult ParseAcceptedCrashResponse(string body)
        {
            if (string.IsNullOrEmpty(body))
            {
                return ShardPilotResult.Ok;
            }

            Dictionary<string, object> parsed;
            if (!ShardPilotJsonParser.TryParseObject(body, out parsed))
            {
                return ShardPilotResult.Ok;
            }

            var suppressed = false;
            object suppressedValue;
            if (parsed.TryGetValue("suppressed", out suppressedValue) && suppressedValue is bool)
            {
                suppressed = (bool)suppressedValue;
            }

            var warnings = ExtractWarnings(parsed);
            if (!suppressed && warnings == null)
            {
                return ShardPilotResult.Ok;
            }

            return new ShardPilotResult(true, null, false, TimeSpan.Zero, suppressed, warnings);
        }

        private static IReadOnlyList<string> ExtractWarnings(Dictionary<string, object> parsed)
        {
            object value;
            if (!parsed.TryGetValue("warnings", out value))
            {
                return null;
            }

            var list = value as List<object>;
            if (list == null || list.Count == 0)
            {
                return null;
            }

            var warnings = new List<string>(list.Count);
            foreach (var item in list)
            {
                var text = item as string;
                if (!string.IsNullOrEmpty(text))
                {
                    warnings.Add(text);
                }
            }

            return warnings.Count > 0 ? warnings : null;
        }

        public Task<ShardPilotResult> FlushAsync(TimeSpan? timeout = null)
        {
            return EnqueueFlushRequest(false, timeout, true);
        }

        /// <summary>
        /// One drain attempt, answered with that attempt's own outcome: a
        /// retryable failure answers at once instead of holding. The close
        /// drain paces its own retries with it.
        /// </summary>
        internal Task<ShardPilotResult> DrainOnceAsync(TimeSpan? timeout = null)
        {
            return EnqueueFlushRequest(false, timeout);
        }

        public Task<ShardPilotResult> CloseAsync(TimeSpan? timeout = null)
        {
            TaskCompletionSource<ShardPilotResult> completion;
            RejectionWindow rejections;
            lock (_gate)
            {
                if (_closeCompletion != null && (_closeStarted || _closeCompletion.Task.IsCompleted)) return _closeCompletion.Task;
                completion = _closeCompletion ?? new TaskCompletionSource<ShardPilotResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                _closeCompletion = completion;
                _closingDecision = ShardPilotConsent.CurrentDecision;
                _closeStarted = true;
                rejections = new RejectionWindow();
                RegisterRejections(rejections, _intakeSequence, completion);
            }
            _ = CompleteCloseOperationAsync(completion, timeout, rejections);
            return completion.Task;
        }

        private async Task CompleteCloseOperationAsync(TaskCompletionSource<ShardPilotResult> completion, TimeSpan? timeout, RejectionWindow rejections)
        {
            try
            {
                ShardPilotResult result;
                result = await (_experiments != null ? CloseAfterApplicationsAsync(timeout ?? TimeSpan.FromSeconds(10))
                    : EnqueueFlushRequest(true, timeout)).ConfigureAwait(false);
                // The public close also owns verdicts from its application pre-drain.
                completion.TrySetResult(CompleteRejections(result, rejections));
            }
            catch (OperationCanceledException)
            {
                lock (_rejectionGate) _pendingRejections.Remove(rejections);
                completion.TrySetCanceled();
            }
            catch (Exception error)
            {
                lock (_rejectionGate) _pendingRejections.Remove(rejections);
                completion.TrySetException(error);
            }
        }

        private async Task<ShardPilotResult> CloseAfterApplicationsAsync(TimeSpan budget)
        {
            if (budget <= TimeSpan.Zero) budget = TimeSpan.FromSeconds(10);
            var elapsed = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                _experiments.BeginCloseDrain();
                while (_experiments.HasApplicationsToDrain)
                {
                    var remaining = budget - elapsed.Elapsed;
                    if (remaining <= TimeSpan.Zero) break;
                    // Closing refuses new work while retaining every owned
                    // application until admission or the caller's deadline.
                    _experiments.Tick();
                    await DrainOnceAsync(remaining).ConfigureAwait(false);
                    remaining = budget - elapsed.Elapsed;
                    if (_experiments.HasApplicationsToDrain && remaining > TimeSpan.Zero)
                        await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(10, remaining.TotalMilliseconds))).ConfigureAwait(false);
                }
                var closeBudget = budget - elapsed.Elapsed;
                // Non-positive flush timeouts mean the default ten seconds.
                // Preserve the spent budget instead of starting it again.
                return await EnqueueFlushRequest(true, closeBudget > TimeSpan.Zero
                    ? closeBudget : TimeSpan.FromMilliseconds(1)).ConfigureAwait(false);
            }
            finally
            {
                // Also covers exceptions and the already-stopped return.
                _experiments.Teardown();
            }
        }

        public ShardPilotStats Snapshot()
        {
            return _stats.Snapshot();
        }

        public string AnonymousId
        {
            get { return _config.AnonymousId; }
        }

        /// <summary>
        /// The normalized config, for the crash-plane capture lanes wired at
        /// hook-install time (the ANR lane reads its static
        /// flags, the crash scope identity, and the spool directory from
        /// here). Read-only by convention: capture lanes must never mutate
        /// it.
        /// </summary>
        internal ShardPilotConfig CrashPlaneConfig
        {
            get { return _config; }
        }

        public ShardPilotConsentState ConsentState
        {
            get
            {
                lock (_gate)
                {
                    return _consentState;
                }
            }
        }

        /// <summary>
        /// Whether crash reporting is currently enabled. Crash diagnostics
        /// are a legitimate-interest plane, ON by default with no first-run
        /// decision and INDEPENDENT of analytics consent; the player opts
        /// out via <see cref="SetCrashReportingEnabled"/>. While disabled,
        /// <paramref name="disabledReason"/> is
        /// <see cref="ShardPilotCrashSettings.NotConfiguredReason"/> when the
        /// client has no <see cref="ShardPilotConfig.CrashKey"/> (crash
        /// reporting is off by configuration — no capture is ever possible),
        /// <see cref="ShardPilotConsent.DeniedError"/> while analytics
        /// consent is explicitly DENIED (a denial suppresses crash capture
        /// too), <see cref="ShardPilotCrashSettings.OptOutReason"/> for an
        /// explicit opt-out, or
        /// <see cref="ShardPilotCrashSettings.ReadFailedReason"/> when the
        /// persisted setting could not be read or was malformed — all fail
        /// CLOSED (no capture, no send).
        /// </summary>
        public bool IsCrashReportingEnabled(out string? disabledReason)
        {
            // The configuration gate outranks every runtime state: without a
            // crash key, CaptureCrash refuses unconditionally and no send can
            // ever be built, so reporting "enabled" here would tell an
            // integration that capture is active in a no-key initialization
            // (or a reinitialization intended to turn crash reporting off).
            if (string.IsNullOrEmpty(_config.CrashKey))
            {
                disabledReason = ShardPilotCrashSettings.NotConfiguredReason;
                return false;
            }

            lock (_gate)
            {
                // A DENIED analytics consent suppresses crash capture too
                // (the anonymous_id is the consent/erasure subject): every
                // CaptureCrash is dropped in this state, so the status API
                // must not claim collection is active. Lifts with the next
                // explicit grant.
                if (ShardPilotConsent.IsDeniedState(_consentState))
                {
                    disabledReason = ShardPilotConsent.DeniedError;
                    return false;
                }

                if (_consentReadFailureCrashHold)
                {
                    // The unreadable-consent hold outranks the opt-out
                    // setting: it lifts only on the next explicit consent
                    // decision, not on SetCrashReportingEnabled(true).
                    disabledReason = ShardPilotCrashSettings.ReadFailedReason;
                    return false;
                }

                disabledReason = _crashReportingEnabled ? null : _crashReportingDisabledReason;
                return _crashReportingEnabled;
            }
        }

        /// <summary>
        /// Records the crash-reporting opt-in/opt-out decision — the crash
        /// plane's own setting, independent of analytics consent. Disabling
        /// stops COLLECTION as well as sending: every later capture is
        /// refused before any report is prepared or persisted, the spooled
        /// backlog stops re-sending (it stays durable and ages out under the
        /// crash spool's retention window; reports captured while enabled
        /// re-send only if re-enabled within it), the breadcrumb ring is
        /// cleared so a report emitted after a re-enable can never carry
        /// opt-out-period or pre-opt-out breadcrumbs, and the scope's ANR
        /// era snapshots (the era subject's anonymous id) are wiped in the
        /// same call — a re-enable starts a fresh era at the next SDK init.
        /// The decision is persisted immediately and applies in memory even
        /// when persistence fails. Returns true when the decision was
        /// durably persisted;
        /// false when the client is closed (no effect) or the persist failed
        /// (<see cref="ShardPilotStats.LastError"/> becomes
        /// <c>crash_persist_failed</c>; the in-memory decision still applies
        /// for this session — call again to retry persistence, and note a
        /// failed ENABLE is never readable by a later construction, which
        /// therefore stays fail-closed). Call from the Unity main thread so
        /// the decision can be persisted through PlayerPrefs.
        /// </summary>
        public bool SetCrashReportingEnabled(bool enabled)
        {
            // Same decision-gate discipline as SetConsent: the persist and
            // the propagation must not interleave with a registration-time
            // refresh that read the previous persisted value.
            lock (ShardPilotDispatcher.PrivacyDecisionGate)
            {
                return SetCrashReportingEnabledUnderDecisionGate(enabled);
            }
        }

        private bool SetCrashReportingEnabledUnderDecisionGate(bool enabled)
        {
            var wasEnabled = false;
            lock (_gate)
            {
                if (IntakeClosed)
                {
                    return false;
                }

                // The previous in-memory decision, captured for the
                // suppression-floor raise below: only a REAL
                // disabled→enabled transition ends a dark opted-out window.
                wasEnabled = _crashReportingEnabled;

                _crashReportingEnabled = enabled;
                _crashReportingDisabledReason = enabled ? null : ShardPilotCrashSettings.OptOutReason;
                if (!enabled)
                {
                    // Opt-out leaves no trail behind: without this, a
                    // disable -> re-enable -> CaptureCrash sequence would
                    // leak pre-opt-out event names into the new report's
                    // breadcrumbs. (Recording is also gated while disabled.)
                    _breadcrumbs.Clear();

                    // Any capture that began before this disable must abort
                    // at its next recheck, even if a re-enable lands before
                    // it gets there: the epoch outlives the boolean.
                    _crashDisableEpoch++;
                }
            }

            // Persist FIRST, before publishing the decision to other live
            // clients — the same discipline SetConsent uses: a client
            // constructed concurrently then reads the NEW persisted value
            // (or, when the persist fails, the process-wide pending opt-out
            // that TrySave records), while a client that registers after
            // the propagation below already loaded the right state. Persist
            // runs OUTSIDE _gate (the store may do file IO); the in-memory
            // decision above applies regardless of the outcome, and the
            // store contract guarantees a failed write is never readable
            // back as though persisted, so a failed ENABLE cannot reopen a
            // fail-closed client at the next construction.
            var persisted = ShardPilotCrashSettings.TrySave(_settingsStore, enabled);

            if (enabled)
            {
                // A re-enable puts the retained (unexpired) backlog back in
                // play NOW: kick the re-send pass directly and wake the
                // worker, instead of leaving the reports parked until the
                // next flush tick — with a long flush interval and no
                // unrelated wake, that could be arbitrarily later. Both are
                // cheap no-ops when nothing is pending, and this applies to
                // the in-memory decision, so it runs regardless of the
                // persistence outcome.
                MaybeStartCrashResendPass();
                WakeWorker();

                // A standing AEI suppression-floor marker means the dark
                // opted-out window ends HERE: raise it to now, so an ANR
                // Android retained from inside that window (not just the
                // backlog standing when the opt-out landed) dies at the
                // next enabled pass, while post-re-enable records stay
                // strictly above the floor and report. Gated on the REAL
                // disabled→enabled transition: an idempotent repeat call
                // while already enabled changes nothing about the window —
                // moving a still-standing marker (the lane dark, no pass
                // run yet) to the repeat's moment would silently settle
                // ANRs that happened AFTER the real opt-in, which are
                // legitimately reportable.
                if (!wasEnabled)
                {
                    var reEnableNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    ShardPilotAeiCursor.RaiseSuppressionFloorIfMarked(
                        _settingsStore, _config, reEnableNowMs);

                    // The opt-out sweep stamped EVERY enumerable scope's
                    // floor; the re-enable must close the window for all of
                    // them, not just this one, or an inactive sibling scope
                    // keeps a stale (revocation-time) floor. Same era-file
                    // enumeration the sweep uses; the per-scope armed flag is
                    // the backstop for scopes whose era file the sweep wiped.
                    ShardPilotAnrEraStore.RaiseSuppressionFloorForAllScopesAtResolvedDirectory(
                        _config, _settingsStore, reEnableNowMs, message => SafeLog("{0}", message));
                }
            }
            else
            {
                // Synchronize with any in-flight dispatch: the final
                // opt-out recheck and the sender handoff run under this
                // gate, so acquiring it here means that when this call
                // returns, a concurrent dispatch has either already been
                // handed to the transport or will observe the flipped flag
                // and refuse — a disabled client cannot start another send
                // after the opt-out call completes.
                lock (_crashDispatchGate)
                {
                }

                // The ANR era snapshots carry the opting-out subject's
                // anonymous id: destroy this scope's NOW, at the same
                // moment the breadcrumb ring cleared above — never parked
                // until a next-launch AEI pass (which never runs while the
                // ANR flags are dark) or the retention TTL. Crash-plane
                // machinery with its own fail-closed rule: a wipe that
                // cannot be confirmed persists the owed marker that keeps
                // the era store disabled until a later launch settles it.
                // Outside _gate — file IO, like the spool wipes.
                ShardPilotAnrEraStore.WipeScope(_config, _settingsStore, message => SafeLog("{0}", message));

                // And the AEI suppression floor: while the ANR lane is dark
                // no pass can floor the cursor, yet Android may retain an
                // ANR record from the opted-out period — stamp the moment
                // in plain KV (no AEI read, no JNI) so the next ENABLED
                // pass floors its cursor here and the opted-out-period
                // record can never report after a re-enable.
                ShardPilotAeiCursor.RecordSuppressionFloor(
                    _settingsStore, _config, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                // The opt-out SETTING is GLOBAL — one persisted key
                // (ShardPilotCrashSettings.StorageKey), read by every
                // scope's construction — while era files and floor markers
                // are per-scope, and only LIVE clients receive the
                // propagation below: an INACTIVE scope would keep its era
                // snapshot and get no floor, then re-initialize after a
                // later (equally global) re-enable and report Android-
                // retained ANRs from the opted-out window. Sweep every
                // scope's era file in this directory exactly like the
                // consent denial does — the sweep stamps each swept
                // scope's suppression floor, keyed by the scope hash in
                // the filename. Enumerability, stated honestly: era files
                // on disk plus this scope are what an opt-out can reach —
                // the KV store (IShardPilotKeyValueStore) is Get/Set only
                // (PlayerPrefs has no key listing), so a scope with a
                // cursor but NO era file in this directory cannot be
                // discovered here; its records still cannot attribute (no
                // era survives anywhere) and the standing opt-out gate
                // suppresses every pass until re-enable. Note the opt-out
                // SPOOL posture is unchanged and scope-local: no spool,
                // own or sibling, is destroyed — the spooled backlog
                // legitimately re-sends if re-enabled within retention.
                ShardPilotAnrEraStore.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message));
            }

            // The decision is GLOBAL (one persisted key) while clients are
            // per-scope: propagate it to every other live registered client
            // — mirroring the consent-denial propagation — so an opt-out
            // stops collection everywhere NOW, and an opt-in resumes it,
            // instead of each client waiting for its next construction.
            // Runs AFTER the persist, so a construction racing this call
            // lands on either the propagation snapshot or the new persisted
            // state — never on neither. The just-swept directory rides
            // along so a propagated client under a DIFFERENT custom
            // SpoolDir sweeps its own era files too (and one under
            // the SAME directory does not sweep it twice), mirroring the
            // denial propagation's directory handoff.
            ShardPilotDispatcher.PropagateCrashReportingEnabled(
                this, enabled, ShardPilotSpool.ResolveDirectory(_config));

            if (!persisted)
            {
                _stats.RecordFailure(ShardPilotCrashSettings.PersistFailedError);
                SafeLog("shardpilot crash settings persist failed; the decision applies in memory only");
                return false;
            }

            return true;
        }

        /// <summary>
        /// Records an ordinary grant or denial. Every overload returns whether the
        /// decision applied; unfinished durability is reported in Warnings.
        /// Call from the Unity main thread. An ordinary grant cannot reverse forced-minor denial.
        /// </summary>
        public ShardPilotResult SetConsent(bool analyticsGranted)
        {
            return SetConsent(analyticsGranted, (ShardPilotConsentNotice?)null);
        }

        /// <summary>Records a decision with optional per-call notice provenance.</summary>
        public ShardPilotResult SetConsent(bool analyticsGranted, ShardPilotConsentNotice? notice = null)
        {
            string ignored;
            return SetConsentCore(
                analyticsGranted ? ShardPilotConsentDecision.Granted : ShardPilotConsentDecision.Denied,
                notice, out ignored);
        }

        /// <summary>
        /// Records an explicit decision, with application success, refusal Error and
        /// non-fatal Warnings. Call from the Unity main thread.
        /// </summary>
        public ShardPilotResult SetConsent(ShardPilotConsentDecision decision)
        {
            return SetConsent(decision, (ShardPilotConsentNotice?)null);
        }

        /// <summary>Records a decision with optional per-call notice provenance.</summary>
        public ShardPilotResult SetConsent(ShardPilotConsentDecision decision, ShardPilotConsentNotice? notice = null)
        {
            string ignored;
            return SetConsentCore(decision, notice, out ignored);
        }

        /// <summary>
        /// Records an explicit analytics decision. Applied decisions succeed even
        /// when persistence, purge or receipt work remains owed; Warnings names that work.
        /// Refused decisions fail with Error and do not reopen analytics.
        /// Forced-minor denial remains sticky across ordinary grant and denial calls:
        /// the grant is refused and the denial succeeds without replacing its receipt.
        /// An invalid configured actor may deny locally but cannot mint a new receipt or grant.
        /// An ordinary-denied grant waits for any outstanding spool purge to complete.
        /// The additional diagnostic output names the refusal or durability notice;
        /// use the returned Error and Warnings to distinguish their meanings.
        /// Call from the Unity main thread so PlayerPrefs writes run on that thread.
        /// </summary>
        public ShardPilotResult SetConsent(ShardPilotConsentDecision decision, out string? error)
        {
            return SetConsentCore(decision, null, out error);
        }

        private ShardPilotResult SetConsentCore(ShardPilotConsentDecision decision, ShardPilotConsentNotice? notice, out string error)
        {
            // The whole [apply -> persist -> propagate] sequence runs under
            // the process-wide decision gate, mutually exclusive with the
            // registration-time refresh: otherwise a refresh that read the
            // OLD persisted value before this decision could apply it AFTER
            // this decision's propagation, replaying a superseded state.
            lock (ShardPilotDispatcher.PrivacyDecisionGate)
            {
                var work = ShardPilotConsent.BeginDecision(out var owner);
                try
                {
                    return SetConsentResultUnderDecisionGate(decision, notice, out error);
                }
                finally { ShardPilotConsent.EndDecision(work, owner); }
            }
        }

        private ShardPilotResult SetConsentResultUnderDecisionGate(ShardPilotConsentDecision decision,
            ShardPilotConsentNotice? notice, out string error)
        {
            var warnings = new List<string>();
            var durable = SetConsentUnderDecisionGate(decision, notice, warnings, out error);
            var appliedWithWarning = error == "consent_persist_failed" ||
                                     error == "consent_outbox_persist_failed";
            if (durable || appliedWithWarning)
            {
                if (appliedWithWarning && !warnings.Contains(error)) warnings.Add(error);
                lock (_gate)
                {
                    if (ConsentPurgeOwed) warnings.Add("spool_purge_failed");
                }
                if (_consentConfiguredActorInvalid) warnings.Add("consent_actor_invalid");
                error = warnings.Count == 0 ? null : warnings[0];
                return new ShardPilotResult(true, null, false, TimeSpan.Zero, false, warnings);
            }

            return new ShardPilotResult(false, error, false);
        }

        private bool RetryConsentPurge()
        {
            // Retry only unfinished sweeps. Denied describes consent, not debt;
            // repeating a completed sweep could delete newer granted activity.
            bool sweepEvents;
            bool sweepCrashes;
            lock (_gate)
            {
                sweepEvents = _consentEventPurgeOwed;
                sweepCrashes = _consentCrashPurgeOwed;
            }
            var eventsComplete = ShardPilotSpool.RetryOwedPurge(
                _config, _settingsStore, message => SafeLog("{0}", message), sweepEvents);
            var crashesComplete = ShardPilotCrashSpool.RetryOwedPurge(
                _config, _settingsStore, message => SafeLog("{0}", message), sweepCrashes);
            QuarantineSpoolIfSweepOwed(eventsComplete);
            QuarantineCrashSpoolIfSweepOwed(crashesComplete);
            return eventsComplete && crashesComplete;
        }

        private bool SetConsentUnderDecisionGate(ShardPilotConsentDecision decision, ShardPilotConsentNotice? notice,
            List<string> warnings, out string error)
        {
            error = null;
            ShardPilotConsentState state;
            switch (decision)
            {
                case ShardPilotConsentDecision.Granted:
                    state = ShardPilotConsentState.Granted;
                    break;
                case ShardPilotConsentDecision.Denied:
                    state = ShardPilotConsentState.Denied;
                    break;
                case ShardPilotConsentDecision.DeniedForcedMinor:
                    state = ShardPilotConsentState.DeniedForcedMinor;
                    break;
                default:
                    // The three accepted decisions are contract; everything
                    // else is refused without touching any state.
                    error = "invalid_consent";
                    return false;
            }

            lock (_gate)
            {
                if (_workerStopped || IntakeClosed)
                {
                    // Torn down — or closing: once CloseAsync has been
                    // submitted the client accepts no further decisions,
                    // exactly like it refuses new events. Without this, a
                    // decision racing the close could append its receipt
                    // AFTER the close path's shutdown-durability check
                    // and the resolved close would leave an
                    // undurable receipt behind with no worker left to
                    // retry the owed write. `_closed` stays set while a
                    // close is held for owed consent writes too: that
                    // hold exists only to land those writes, not to take
                    // new decisions.
                    error = "shutdown";
                    return false;
                }

                // An ordinary consent call carries no authority to remove
                // a forced-minor restriction. Preserve its original decision
                // and receipt: a reasonless replacement denial would erase
                // the restriction's provenance at the receiver.
                if (_consentState == ShardPilotConsentState.DeniedForcedMinor)
                {
                    if (state == ShardPilotConsentState.Granted)
                    {
                        error = "consent_forced_minor";
                        return false;
                    }

                    if (state == ShardPilotConsentState.Denied)
                    {
                        return true;
                    }
                }
            }

            // A replaced client no longer receives registry propagation. Its
            // stale local decision cannot remove this backend's restriction.
            bool restrictionUnreadable;
            var forcedMinor = ShardPilotConsent.HasForcedMinorRestriction(_settingsStore, out restrictionUnreadable);
            if (!forcedMinor && !restrictionUnreadable)
            {
                bool recordUnreadable;
                forcedMinor = ShardPilotConsent.Load(_settingsStore, _config.AnonymousId, out recordUnreadable) ==
                              ShardPilotConsentState.DeniedForcedMinor;
            }
            if (forcedMinor)
            {
                ApplyGlobalConsentDenial(null, ShardPilotConsentState.DeniedForcedMinor);
                if (state == ShardPilotConsentState.Granted)
                {
                    error = "consent_forced_minor";
                    return false;
                }
                if (state == ShardPilotConsentState.Denied) return true;
            }
            if (restrictionUnreadable && state == ShardPilotConsentState.Granted)
            {
                ApplyConsentReadFailureFailClosed();
                error = "consent_restriction_unreadable";
                return false;
            }

            if (state == ShardPilotConsentState.Granted && _consentConfiguredActorInvalid)
            {
                error = "consent_actor_invalid";
                return false;
            }

            if (state == ShardPilotConsentState.Granted && !RetryConsentPurge())
            {
                error = "spool_purge_failed";
                return false;
            }

            var analyticsGranted = state == ShardPilotConsentState.Granted;
            if (analyticsGranted && _httpSender != null && _consentOutbox.GrantAppendBlockedByCap())
            {
                error = "consent_outbox_full";
                return false;
            }
            ShardPilotConsentNoticeSnapshot noticeSnapshot;
            string noticeWarning;
            if (!ShardPilotConsentNoticeValidator.TrySnapshot(notice, analyticsGranted, out noticeSnapshot, out error, out noticeWarning))
                return false;
            if (noticeWarning != null) warnings.Add(noticeWarning);

            // The decision moment, captured ONCE: the decider's receipt and
            // every receipt later armed from the session marker (propagated
            // siblings, late registrants) stamp decided_at with this exact
            // time — the moment the player actually decided — never the
            // arming time; retries and post-relaunch re-sends carry it (and
            // the idempotency key) VERBATIM from the stored entry. The
            // decision PAIR (decided_at, decision_seq) is minted with it:
            // the seq disambiguates decisions
            // sharing a stamp, and every durable artifact of this decision
            // — record, receipt, denial marker — carries the same pair.
            var decidedAtUtc = DateTime.UtcNow;
            var decidedAtStamp = ShardPilotJson.FormatTimestamp(decidedAtUtc);
            var decisionSeq = ShardPilotConsent.NextDecisionSeq();
            var entry = CreateConsentReceiptEntry(
                analyticsGranted,
                decidedAtUtc,
                state == ShardPilotConsentState.DeniedForcedMinor ? ShardPilotConsent.ForcedMinorReason : null,
                decisionSeq, noticeSnapshot);

            var cleared = 0;
            var previousConsentState = ShardPilotConsentState.Unknown;
            lock (_gate)
            {
                if (_workerStopped || IntakeClosed)
                {
                    // Re-checked ATOMICALLY with the apply/append: the early
                    // guard above released the gate, so CloseAsync may have
                    // landed in between. Refusing here (nothing applied, no
                    // append) makes the race two-sided-safe: either this
                    // decision's append commits BEFORE the close submission
                    // — then the append-time owed-write mark keeps the close
                    // path's shutdown durability check holding teardown until
                    // the receipt is durably on disk — or the close wins and
                    // this decision is refused as shutdown.
                    error = "shutdown";
                    return false;
                }

                // consent_outbox_full FAIL-CLOSED GRANT GATE: a GRANT whose receipt the
                // denial-preferring cap pass would have to evict — the trail
                // is denial-full at the cap, so the appended grant would be
                // among the evictable pure grants and could get displaced,
                // persisting Granted with no grant receipt retained anywhere
                // — is REFUSED before any state flips. Judged by simulating
                // the FULL post-append cap pass over the sanitized mirror
                // (a failed durable write never evicts, so the mirror can
                // sit over the cap where any-one-old-grant probing would
                // mis-admit). Nothing is appended, nothing evicted, no
                // state moves; the recourse is delivering/settling retained
                // denials first (they drain at every dispatch point).
                // DENIALS are never gated: a withdrawal always takes local
                // effect, and the cap pass protects denial receipts by
                // construction.
                if (analyticsGranted && entry != null && _consentOutbox.GrantAppendBlockedByCap())
                {
                    error = "consent_outbox_full";
                    return false;
                }

                // The superseded state, captured for the suppression-floor
                // raise below: only a REAL transition TO granted ends a
                // dark denied window.
                previousConsentState = _consentState;
                _consentOutbox.AuthorizeExplicitDecisionReplacement();

                ApplyConsentState(state);
                ShardPilotConsent.CurrentDecision?.Accept(state, decidedAtStamp, decisionSeq);

                // An explicit in-session decision landed on this client: the
                // identity-downgrade machinery must never wipe it (it only
                // downgrades states inherited from a persisted read).
                // (The SUPERSEDE stamp on older retained pending grants is
                // deliberately NOT applied here: it lands only after this
                // decision's state persist durably succeeds — see the
                // post-persist stamp below —
                // a stamp that survived a FAILED state persist would let
                // the stale persisted grant skip its parked-pending-grant
                // downgrade at the next launch.)
                _consentDecisionGeneration++;

                // Every NEW explicit decision supersedes an older withheld
                // grant-state completion: the late settle of a PREVIOUS
                // grant's receipt must never write a state this decision has
                // replaced (this decision's own withhold, if any, re-arms
                // below with its own key).
                _withheldGrantStateKey = null;
                _withheldGrantStatePersistOwed = false;

                // Append the decision's receipt to the outbox MIRROR in the
                // SAME critical section that applies the consent state: any
                // post-grant batch was enqueued after this lock released, so
                // its publish observes the retained receipt (via the grant-receipt gate
                // and the receipts-first pump) and can never overtake it on
                // the wire. The DURABLE write completes outside this lock.
                // The trail is append-only (defold-canonical D1): an earlier
                // decision's still-retained receipt is NOT withdrawn —
                // grant→deny delivers both, in decision order, so after a
                // denial the deny receipt is always the server's last word.
                if (entry != null)
                {
                    if (analyticsGranted)
                    {
                        // PESSIMISTIC ARM of the withheld-grant obligation,
                        // in the SAME gate hold that makes the receipt
                        // visible: until the observable grant persists later
                        // in this call, the grant persist is factually owed —
                        // and any dispatch point (the worker's own cycle
                        // included) may make the receipt durable and send it
                        // the moment this lock releases. Arming only after
                        // the durability judgment would leave a window where
                        // an exit strands a durable (even delivered) receipt
                        // whose completion obligation died in a local
                        // variable: the next launch would re-send and prune
                        // it with nothing left to persist the grant. The
                        // in-memory key and the durable marker (phase
                        // pending) therefore exist BEFORE the append; the
                        // successful state persist below discharges them on
                        // the spot, so the common path books and clears the
                        // obligation inside one call. A marker write the
                        // store refuses is re-attempted at every dispatch
                        // point (the in-process key still guards this
                        // session); two DIFFERENT keys cannot be written
                        // atomically, so a store that selectively fails only
                        // the marker key while accepting the record remains
                        // the accepted residual — fail-closed at the next
                        // launch (re-ask), never fail-open.
                        _withheldGrantStateKey = entry.IdempotencyKey;
                        _withheldGrantDecidedAt = entry.DecidedAt;
                        _withheldGrantNotice = entry.Notice;
                        _withheldGrantDecisionSeq = entry.DecisionSeq;
                        _withheldGrantMarkerReceiptDurable = false;
                        _withheldGrantMarkerArmed = ShardPilotConsent.RecordWithheldGrant(
                            _settingsStore,
                            _consentScopeHash,
                            entry.IdempotencyKey,
                            entry.DecidedAt,
                            entry.DecisionSeq,
                            receiptDurable: false, notice: entry.Notice);
                        if (_withheldGrantMarkerArmed)
                        {
                            // The fresh arm overwrote the scope marker in
                            // place: any OWED discharge of the previous
                            // marker content is superseded by this write.
                            _withheldMarkerDischargeOwed = false;
                        }
                    }

                    _consentOutbox.AppendToMirror(entry);

                    // PESSIMISTIC witness-pending arm for DENIALS, in the
                    // SAME hold that makes the receipt dispatchable: the
                    // worker can dispatch and settle this receipt anywhere
                    // between this append and the exact recompute that
                    // follows the marker/record writes below — an unarmed
                    // settle in that window would consume what may be the
                    // denial's only witness with no guard consulted. Armed
                    // worst-case here; the recompute replaces it with the
                    // exact write outcomes (both false on a healthy
                    // store), and a guard write racing this decision
                    // carries this same pair — benign duplication.
                    if (!analyticsGranted)
                    {
                        _consentDenialRecordPending = _settingsStore != null;
                        _consentDenialMarkerPending = _settingsStore != null;
                        _consentDenialPendingDecidedAt = entry.DecidedAt;
                        _consentDenialPendingSeq = entry.DecisionSeq;
                    }

                    // (The scope's cap-refusal marker, if any, is NOT
                    // cleared here: the append is mirror-only, and a clear
                    // that outlives a failed persist would relaunch the
                    // scope receipt-less under the shared grant. It clears at
                    // the post-persist point
                    // once the decision's durable evidence exists.)
                }

                // Record the process-wide grant-decision marker with its
                // ORIGINAL decision time (cleared on a denial): a client
                // constructed/registered AFTER this decision — invisible to
                // the propagation below — consults it during its
                // registration catch-up (serialized with this whole decision
                // under the process-wide decision gate) and arms its OWN
                // scope's receipt carrying this same decided_at.
                ShardPilotConsent.RecordSessionGrantDecision(
                    analyticsGranted ? decidedAtUtc : (DateTime?)null, noticeSnapshot);

                // The mirror marker for DENIALS: a client registering after
                // this live denial's propagation snapshot consults it during
                // its registration catch-up (serialized under the same
                // decision gate) and appends its OWN scope's denial receipt
                // with this same decided_at when its trail would otherwise
                // end granted — the same-session decision arriving late, not
                // a relaunch-loaded mint without an explicit decision. A grant clears it: the newest
                // decision is the operative one.
                ShardPilotConsent.RecordSessionDenialDecision(
                    analyticsGranted ? (DateTime?)null : decidedAtUtc, noticeSnapshot);

                // An EXPLICIT consent decision supersedes whatever the
                // unreadable record held, so it lifts the crash plane's
                // consent-read-failure hold: a denial's own gates (and the
                // wipes below) take over, a grant leaves the crash setting
                // in charge again.
                _consentReadFailureCrashHold = false;
                if (!analyticsGranted)
                {
                    // Advancing the denial epoch invalidates every event the
                    // worker drained before this point: the worker discards
                    // its held batch when it sees the epoch move.
                    AdvanceConsentDenialEpochLocked();
                    cleared = _queue.Count + _restoredPending.Count + _spooledOverflowIds.Count;
                    _queue.Clear();
                    _restoredPending.Clear();

                    // Disk-only overflow events are erased by the spool wipe
                    // below; they are counted dropped here because no other
                    // path ever sees them again in this session.
                    _spooledOverflowIds.Clear();
                    _spooledOverflowOrder.Clear();

                    // Denial leaves no trace of prior activity: without this,
                    // a deny -> grant -> CaptureCrash sequence would leak the
                    // names of events accepted before the denial into the
                    // crash report's breadcrumbs.
                    _breadcrumbs.Clear();
                }
            }

            if (!analyticsGranted && _experiments != null)
            {
                // The purge above discarded queued-but-unpublished
                // experiment exposure facts. Live assignments can re-arm
                // after a new session is announced following re-grant;
                // captures must not retain the withdrawn session identity.
                // Outside _gate — the consumer takes its own lock.
                _experiments.OnAnalyticsPurge();
            }

            // The superseded obligation's DURABLE marker goes with the
            // in-memory key the apply block just cleared (still under the
            // decision gate): a relaunch must never complete a grant this
            // decision replaced. A GRANT with a mintable receipt already
            // OVERWROTE the scope's marker in place with its own
            // pessimistic arm above — clearing here would wipe the fresh
            // obligation, so only decisions that armed nothing clear.
            if (entry == null || !analyticsGranted)
            {
                ClearWithheldGrantMarkerBookkeeping();
            }

            if (cleared > 0)
            {
                _stats.RecordDropped(cleared);
            }

            // DURABILITY ORDERING — asymmetric by decision kind, so a kill
            // at any point between the two durable writes fails CLOSED:
            // - DENIAL: the decision record persists FIRST — before the
            //   local wipe, the directory sweep, and the live-client
            //   propagation below: a kill anywhere in that block must leave
            //   the DENIAL already durable, so the relaunch starts in the
            //   persisted-denial branch and re-runs the full sweep
            //   fail-closed. (A receipt lost to that window is an audit gap
            //   the next explicit decision can repair; an unenforced denial
            //   would be a privacy violation.)
            // - GRANT: the RECEIPT becomes durable FIRST — before the
            //   observable persisted grant (see below): a relaunch must
            //   never see a durable grant with an empty outbox, or the grant-receipt
            //   gate would hold nothing and post-grant events could ship
            //   before the grant ever reaches /v1/consent. The inverse
            //   window (receipt durable, state not) is safe: the relaunch
            //   loads the previous non-granted state (pipeline closed) and
            //   the retained receipt still delivers — receipts deliver
            //   under denied/unknown, and over-delivery is de-duplicated by
            //   idempotency key.
            var statePersisted = true;
            var grantStatePersistWithheld = false;
            var siblingReceiptsDurablySafe = true;
            if (!analyticsGranted)
            {
                // WRITE-AHEAD DENIAL MARKER: the witness slot is
                // written BEFORE the state-first persist, so a kill between
                // the decision and its record write still leaves a durable
                // denial witness for the next launch's boot imposition. The
                // marker retires below only against the durable record that
                // carries this same pair; a failed record persist leaves it
                // standing as THE witness (with the in-process shadow
                // covering this session). A failed marker WRITE is logged
                // and the flow continues — the record persist is the
                // primary, and the settle guard / shutdown ladder are the
                // remaining nets.
                var denialMarkerDurable = ShardPilotConsent.RecordDenialMarker(
                    _settingsStore, state, _config.AnonymousId, decidedAtStamp, decisionSeq);
                if (!denialMarkerDurable && _settingsStore != null)
                {
                    SafeLog(
                        "shardpilot consent: the denial's write-ahead marker could not be written; " +
                        "the record persist and the shutdown witness ladder remain the nets");
                }

                statePersisted = PersistConsentState(state, decidedAtStamp, decisionSeq);
                if (statePersisted)
                {
                    // The record now witnesses this denial (same pair):
                    // retire the write-ahead slot. SCOPED even here — when
                    // our own write-ahead above failed to land, the slot
                    // may still hold a valid FOREIGN actor's witness the
                    // single-slot supersession never actually displaced.
                    ShardPilotConsent.RetireDenialMarkerScoped(_settingsStore, _config.AnonymousId);
                }

                // Armed while THIS latest denial's respective durable
                // write is still owed: the shutdown ladder retries both
                // and refuses to finalize while no durable witness exists
                // anywhere, and the settle guard consults them before
                // consuming a retained denial receipt. With NO
                // durable backend the machinery stays unarmed by the same
                // rule — there is neither a record a relaunch could
                // mis-restore nor a marker slot.
                lock (_gate)
                {
                    _consentDenialRecordPending = _settingsStore != null && !statePersisted;
                    _consentDenialMarkerPending = _settingsStore != null && !denialMarkerDurable;
                    _consentDenialPendingDecidedAt = decidedAtStamp;
                    _consentDenialPendingSeq = decisionSeq;

                    // A fresh explicit decision supersedes whatever the
                    // unjudgeable slot held (the denial's own write-ahead
                    // just overwrote it): the session hold ends — its only
                    // job was blocking OLD withheld completions under
                    // unjudgeable evidence.
                    _denialMarkerSlotUnjudgeable = false;
                    _owedBackupUnjudgeable = false;

                    // The BACKUP is written HERE, the moment the obligation is
                    // armed, not later on an outbox save. A denial that could
                    // mint no receipt at all — no sender — skips the
                    // conditional outbox persist, returns through the
                    // persist-failure branch, and never reaches a dispatch
                    // point; a process exit there left no durable backup and a
                    // later foreign grant could restore GRANTED for this actor.
                    SyncOwedDenialWitnessLocked(freshDecision: true);
                }

                // Synchronize with any in-flight crash dispatch, exactly like
                // the crash opt-out: the final recheck and the sender handoff
                // run under this gate, so acquiring it here means that when
                // this denial returns, a concurrent capture has either
                // already handed its bytes to the transport or will observe
                // the advanced denial epoch and refuse — no crash bytes can
                // START leaving the device after the denial call completed.
                lock (_crashDispatchGate)
                {
                }

                // A denial also destroys the on-disk spool: previously
                // undeliverable events must never be re-sent on a later
                // launch. Done outside _gate — the spool lock is a leaf lock.
                // The consent-receipt OUTBOX is deliberately NOT purged: it
                // is a consent-plane store and the denial's own receipt must
                // still deliver.
                if (_spool != null)
                {
                    _spool.Wipe();
                }

                // The crash spool carries the same consent subject
                // (anonymous_id, breadcrumbs): the denial destroys it too,
                // with the same fail-closed owed-marker semantics, and the
                // persisted crash Retry-After deadline goes with it — the
                // in-memory re-send deadline included, so post-re-grant
                // reports are not held behind backpressure that belonged to
                // the condemned backlog.
                if (_crashSpool != null)
                {
                    _crashSpool.Wipe();
                }

                ShardPilotCrashSpool.ClearRetryAfterDeadline(_settingsStore, _config);
                lock (_gate)
                {
                    _crashResendNotBefore = DateTime.MinValue;
                }

                // The ANR era snapshots carry the same consent subject
                // (anonymous id at era start): the denial destroys this
                // scope's too, immediately — parity with the breadcrumb
                // clear above and the crash-spool wipe — under the era
                // store's own fail-closed owed-marker rule. Other live
                // scopes wipe their own in the propagation below.
                ShardPilotAnrEraStore.WipeScope(_config, _settingsStore, message => SafeLog("{0}", message));

                // And this scope's AEI suppression floor (plain KV, no AEI
                // read): a dark ANR lane cannot floor its cursor now, and a
                // denied-period ANR record must never report after a later
                // re-grant.
                ShardPilotAeiCursor.RecordSuppressionFloor(
                    _settingsStore, _config, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                // The consent decision is GLOBAL while spool files are
                // per-scope: the denial must also reach EVERY other scope's
                // file in the active directory (and any leftover when no live
                // spool exists this run), fail-closed per file. Files under a
                // DIFFERENT custom SpoolDir from an earlier run are out
                // of reach — keep SpoolDir stable. The crash spool's
                // sweep mirrors the event sweep with its own markers, and
                // the ANR era sweep mirrors both: an INACTIVE scope's era
                // snapshot left behind would attribute historical ANRs with
                // the pre-denial anonymous id after a later re-grant
                // initializes that scope.
                QuarantineSpoolIfSweepOwed(ShardPilotSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));
                QuarantineCrashSpoolIfSweepOwed(ShardPilotCrashSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));
                ShardPilotAnrEraStore.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message));

                // And to every LIVE registered client: their persisted state
                // is this same global key, but their in-memory pre-denial
                // events would otherwise stay deliverable until relaunch.
                // Each propagated client also sweeps its own directory,
                // skipping the one just swept above. The forced-minor flavor
                // propagates as itself so every sibling reports the same
                // persisted-state truth. The deciding writes' outcomes ride
                // along so each recipient can
                // judge its OWN witness exposure: the durable record
                // witnesses everyone, but the write-ahead marker names only
                // THIS decider's actor — a sibling under a different
                // anonymous id reads it as a valid FOREIGN marker at boot
                // (kept, inert), so for that sibling a marker-only outcome
                // is no witness at all. Each unwitnessed sibling arms its
                // witness-pending pair (from its own appended receipt, or
                // from this decision's pair when it cannot mint one) so its
                // settle guard retains the receipt over the open gap and
                // its retries heal the shared record — instead of pruning
                // the denial's only durable evidence.
                ShardPilotDispatcher.PropagateConsentDenial(
                    this, ShardPilotSpool.ResolveDirectory(_config), state, decidedAtUtc,
                    deciderRecordPersisted: statePersisted,
                    deciderMarkerDurable: denialMarkerDurable,
                    deciderMarkerActor: _config.AnonymousId,
                    deciderDecisionSeq: decisionSeq, notice: noticeSnapshot);

                // Wake the worker so the held batch is discarded immediately
                // instead of surviving until the next flush tick.
                WakeWorker();
            }
            else
            {
                // A re-grant ends any dark denied window for the AEI
                // suppression floor, exactly like a crash re-enable ends an
                // opted-out one: raise a standing marker to now (plain KV;
                // no ordering dependency on the receipt machinery below).
                // Gated on the REAL transition to granted, mirroring the
                // crash setter: a repeated grant while already granted must
                // not move a still-standing marker over ANRs that happened
                // after the real re-grant.
                if (previousConsentState != ShardPilotConsentState.Granted)
                {
                    var reGrantNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    ShardPilotAeiCursor.RaiseSuppressionFloorIfMarked(
                        _settingsStore, _config, reGrantNowMs);

                    // The denial's all-scope sweep floored every enumerable
                    // scope; the re-grant closes the window for all of them,
                    // like the crash re-enable path — the inactive sibling's
                    // stale floor otherwise stands.
                    ShardPilotAnrEraStore.RaiseSuppressionFloorForAllScopesAtResolvedDirectory(
                        _config, _settingsStore, reGrantNowMs, message => SafeLog("{0}", message));
                }

                // GRANT: make the appended receipt durable FIRST (a failed
                // write NEVER evicts — the mirror keeps ruling the process
                // and the write stays owed), including one in-call retry, so
                // the durability judgment below reads the CURRENT state.
                var receiptDurablyRetained = true;
                if (entry != null && _consentOutbox.HasDurableBackend)
                {
                    receiptDurablyRetained = PersistConsentOutbox() ||
                                             (_consentOutbox.Dirty && PersistConsentOutbox());
                    if (receiptDurablyRetained)
                    {
                        // The write that made the receipt durable upgrades
                        // the marker's phase in the SAME operation — before
                        // any dispatch point may send/ack/prune it. Left at
                        // pending, a state-persist failure below returns
                        // with the receipt durable but the marker still
                        // saying otherwise; dispatch+ack+prune can then
                        // rewrite the record EMPTY, and a kill relaunches
                        // [undecided + clean-empty + pending-phase] — a
                        // backend-recorded grant treated as lost.
                        UpgradeWithheldMarkerPhaseAfterDurableWrite(entry);
                    }
                }

                // This GRANT is now the latest decision: any older denial's
                // witness-pending pair stands down — the witness rule
                // guards the latest decision only (recomputed
                // there on every SetConsent). Deliberately unconditional
                // on this grant's own persist outcome: a failed grant
                // persist leaves the grant's OWN machinery (receipt-first
                // withholding, the belt, the withheld-marker completion)
                // in charge of convergence, not the superseded denial's.
                lock (_gate)
                {
                    _consentDenialRecordPending = false;
                    _consentDenialMarkerPending = false;
                    _consentDenialPendingDecidedAt = null;
                    _consentDenialPendingSeq = 0;


                    // A fresh explicit GRANT supersedes whatever the
                    // unjudgeable slot held too: without this clear, the
                    // grant's OWN withheld completion (its state persist
                    // deferred behind receipt-first ordering) would stay
                    // wedged behind the session hold even after storage
                    // heals.
                    _denialMarkerSlotUnjudgeable = false;
                    _owedBackupUnjudgeable = false;
                }

                // Only a durably retained (or backend-less, or already
                // settled) receipt may be followed by the OBSERVABLE
                // persisted grant: persisting the grant while its receipt is
                // only in memory would reopen the relaunch window above. The
                // in-memory grant still applies for this session either way
                // (and propagates below); the caller retries via SetConsent.
                if (receiptDurablyRetained || !_consentOutbox.HasDurableBackend)
                {
                    statePersisted = PersistConsentState(state, decidedAtStamp, decisionSeq);
                    if (statePersisted)
                    {
                        ConvergeDurableGrantPersist();
                    }
                }
                else
                {
                    grantStatePersistWithheld = true;
                }

                // CONSENT-FIRST makes the in-memory state load-bearing for
                // every live client's enqueue gate, and the persisted key is
                // GLOBAL: a grant recorded here must open the pipeline on
                // every other live registered client NOW (future events
                // only, exactly like this client's own grant) — not at each
                // client's next construction. Denials already propagate;
                // grants must mirror them or a sibling keeps refusing
                // events its persisted state allows. Each propagated
                // sibling appends a receipt for its OWN scope to its OWN
                // outbox (receipt scope is baked into the body), so its
                // first post-grant batch enforces the same receipt-first
                // ordering and its scope gets its own consent row.
                // The propagation REPORTS whether every sibling's required
                // receipt is durably safe — folded into this call's application
                // result below: the global grant is one decision, and a
                // sibling scope whose receipt exists only in memory would
                // reload Granted-but-receipt-less after a kill.
                siblingReceiptsDurablySafe = ShardPilotDispatcher.PropagateConsentGrant(this);

                // Recompute the worker's wait now that a receipt may be
                // parked/queued: the grant-receipt gate and the consent retry deadline
                // both feed its wait computation (the denial branch already
                // wakes above).
                WakeWorker();
            }

            // The denial's receipt write runs after its state persist (the
            // fail-closed direction for denials — see the ordering note
            // above); the grant's already ran. Then dispatch: an explicit
            // decision is a consent dispatch point.
            if (entry != null && !analyticsGranted)
            {
                PersistConsentOutbox();
            }

            MaybeKickConsentOutbox(workerCadence: false);

            // Result evaluation for denials (the decision
            // record's failure outranks the receipt's) and adjusted for the
            // grant ordering above (a withheld grant-state persist makes the
            // RECEIPT failure the actionable error).
            if (!analyticsGranted && !statePersisted)
            {
                // The denial applied in memory and its receipt still
                // delivers; only the decision RECORD's durability failed.
                // Call again to retry.
                _stats.RecordConsentPersistFailed();
                AddConsentDurabilityWarnings(warnings, entry, !statePersisted && !grantStatePersistWithheld, siblingReceiptsDurablySafe);
                error = "consent_persist_failed";
                return false;
            }

            if (entry != null)
            {
                // Receipt durability is judged on the CURRENT state, not the
                // first write attempt: a receipt already acknowledged and
                // pruned (the sync-sender fast path) reports success, and an
                // owed write healed by an in-call retry does too.
                if (!_consentOutbox.HasDurableBackend || _consentOutbox.IsDisowned)
                {
                    // No durable backend, or this client's outbox was
                    // displaced by a later same-scope construction (which
                    // never saw this entry): a receipt still retained here
                    // has no durable home this client can guarantee.
                    if (_consentOutbox.OwnScope.Contains(entry.IdempotencyKey))
                    {
                        // The receipt may already be IN FLIGHT with a real
                        // async sender: when its 2xx (or terminal drop)
                        // settles later, the settle path completes the
                        // withheld observable-grant persist — the obligation
                        // (in-memory key + durable marker) was armed
                        // PESSIMISTICALLY in the same gate hold that
                        // appended the receipt, so it already stands and
                        // survives a process exit in this window.
                        AddConsentDurabilityWarnings(warnings, entry, !statePersisted && !grantStatePersistWithheld, siblingReceiptsDurablySafe);
                        error = "consent_outbox_persist_failed";
                        return false;
                    }
                }
                else if (_consentOutbox.Dirty)
                {
                    if (PersistConsentOutbox())
                    {
                        // This in-call retry is the write that made the
                        // receipt durable: the marker's phase upgrades in
                        // the SAME operation (see the durability-ordering
                        // note on UpgradeWithheldMarkerPhaseAfterDurableWrite).
                        UpgradeWithheldMarkerPhaseAfterDurableWrite(entry);
                    }
                    else if (_consentOutbox.OwnScope.Contains(entry.IdempotencyKey))
                    {
                        // An owed write with THIS entry still retained: the
                        // receipt is genuinely not yet safe. The Contains
                        // check is what distinguishes an owed APPEND from an
                        // owed PRUNE — a receipt the decision-time kick
                        // already got ACKNOWLEDGED (and pruned from the
                        // mirror) is SETTLED server-side even when the
                        // pruned record's rewrite is still failing: that
                        // owed prune retries at every dispatch point as
                        // usual (and at worst the next launch re-sends a
                        // stale entry the server de-duplicates)
                        // and must neither fail this call nor withhold
                        // the grant-state persist below — a server-accepted
                        // consent row with the observable grant withheld
                        // would leave the NEXT launch consent-gated against
                        // a grant the backend already recorded.
                        // The receipt may already be IN FLIGHT with a real
                        // async sender: its later settle (2xx or terminal
                        // drop) completes the withheld observable-grant
                        // persist via the obligation (in-memory key +
                        // durable marker) armed PESSIMISTICALLY in the same
                        // gate hold that appended the receipt — see
                        // _withheldGrantStateKey.
                        AddConsentDurabilityWarnings(warnings, entry, !statePersisted && !grantStatePersistWithheld, siblingReceiptsDurablySafe);
                        error = "consent_outbox_persist_failed";
                        return false;
                    }
                }
            }

            if (grantStatePersistWithheld)
            {
                // The receipt is durably retained NOW (a dispatch-point
                // retry may have healed the owed write after the grant block
                // withheld the state persist): complete the ordered pair —
                // receipt first, observable grant second — inside this call
                // instead of reporting a failure the durability state no
                // longer justifies. The marker phase upgrades BEFORE the
                // state persist attempt: that dispatch-point heal's own
                // TryEnter-gated sync may have lost this very call's gate
                // hold, and a failed state persist below returns with the
                // receipt durable — the marker must already say so, or a
                // dispatch+ack+prune rewriting the record empty ahead of a
                // kill would relaunch [undecided + clean-empty +
                // pending-phase] and treat a backend-recorded grant as lost.
                UpgradeWithheldMarkerPhaseAfterDurableWrite(entry);
                statePersisted = PersistConsentState(state, decidedAtStamp, decisionSeq);
                if (statePersisted)
                {
                    // The SAME convergence as the un-withheld grant persist
                    // above, by the same call: the scoped stale-marker
                    // supersession and the fresh-decision backup sync. This
                    // route used to take only the first of the two.
                    ConvergeDurableGrantPersist();
                }
            }

            if (!statePersisted)
            {
                // Grant path: the receipt is durably safe (or the decision
                // has no receipt), but the decision RECORD's persist failed
                // — surfaced in the result; call again to retry. The
                // pessimistically armed obligation deliberately SURVIVES
                // this return: the persist is still owed, and the marker is
                // what lets a kill before the caller's retry (or the settle)
                // still complete the grant at the next launch. The
                // SUPERSEDE stamp on older pending grants is deliberately
                // NOT applied on this path: the
                // durable state still reads the OLD decision, and its
                // parked backer must keep its downgrade power at the next
                // launch.
                _stats.RecordConsentPersistFailed();
                AddConsentDurabilityWarnings(warnings, entry, !statePersisted && !grantStatePersistWithheld, siblingReceiptsDurablySafe);
                error = "consent_persist_failed";
                return false;
            }

            // The decision's state is DURABLE: every OLDER retained pending
            // grant is no longer the persisted state's backer — stamp them
            // superseded (this decision's own receipt exempted) and land
            // the stamp with an immediate rewrite; a kill before it lands
            // re-downgrades once at the next launch (fail-closed residual).
            // Live sibling scopes stamp their own trails through the
            // dispatcher pass — their older parked grants would otherwise
            // re-downgrade the state THIS persist just wrote.
            _consentOutbox.OwnScope.MarkRetainedAnalyticsGrantsSuperseded(
                entry != null ? entry.IdempotencyKey : null);
            PersistConsentOutbox();
            ShardPilotDispatcher.StampSupersededConsentGrants(this);

            if (entry != null)
            {
                // The scope's cap-refusal marker (if any) is discharged only
                // HERE — with the decision's state durably persisted and its
                // receipt past the durability judgment above (a
                // non-durable receipt returned consent_outbox_persist_failed
                // before reaching this point) — never at the mirror-only
                // append: a clear that outlives a failed persist would
                // relaunch the scope receipt-less under the shared grant. A receipt-less decision
                // (entry == null) leaves the marker standing: the debt is a
                // RECEIPT debt, and only a retained receipt repays it.
                ShardPilotConsent.ClearScopedPendingDowngrade(_settingsStore, _consentScopeHash);
            }

            if (analyticsGranted && entry != null)
            {
                // The observable grant persisted: the obligation armed in
                // the apply hold is discharged inside the same call that
                // booked it — the durable marker must not outlive the
                // persist it guarded. (A sync ack may have already
                // discharged it through the completion; both paths are
                // idempotent.)
                lock (_gate)
                {
                    if (string.Equals(
                            _withheldGrantStateKey, entry.IdempotencyKey, StringComparison.Ordinal))
                    {
                        _withheldGrantStateKey = null;
                    }
                }

                ClearWithheldGrantMarkerBookkeeping();
            }

            if (analyticsGranted && !siblingReceiptsDurablySafe)
            {
                // The durability judgment SPANS the propagated sibling
                // scopes: the deciding scope's own receipt is durable and
                // the state persisted, but some live sibling's REQUIRED
                // own-scope receipt exists only in memory (its store write
                // is failing). A kill before that sibling's owed-write
                // retry would reload its scope as Granted (the global key)
                // with an EMPTY outbox — receipt-less. Report the honest
                // failure; the decision is fully applied and every sibling's
                // owed write keeps retrying — calling again re-judges (and
                // at worst re-arms a duplicate receipt the endpoint accepts
                // under a fresh idempotency key).
                AddConsentDurabilityWarnings(warnings, entry, !statePersisted && !grantStatePersistWithheld, siblingReceiptsDurablySafe);
                error = "consent_outbox_persist_failed";
                return false;
            }

            return true;
        }

        private void AddConsentDurabilityWarnings(List<string> warnings, ShardPilotConsentOutboxEntry entry,
            bool stateFailed, bool siblingsSafe)
        {
            if (stateFailed && !warnings.Contains("consent_persist_failed")) warnings.Add("consent_persist_failed");
            var ownReceiptOwed = entry != null && _consentOutbox.OwnScope.Contains(entry.IdempotencyKey) &&
                (!_consentOutbox.HasDurableBackend || _consentOutbox.IsDisowned || _consentOutbox.Dirty);
            if ((!siblingsSafe || ownReceiptOwed) && !warnings.Contains("consent_outbox_persist_failed"))
                warnings.Add("consent_outbox_persist_failed");
        }

        /// <summary>
        /// Completes the withheld observable-grant persist when the receipt
        /// it belonged to SETTLES (async acknowledgment, or the terminal
        /// drop of a decision the player made regardless): the receipt-first
        /// ordered pair [receipt safe -> observable grant] finishes here
        /// when SetConsent's in-call window closed with the persist
        /// withheld. Serialized under the process-wide decision gate and
        /// re-validated against the CURRENT state so a late settle can never
        /// overwrite a newer decision (whose apply already cleared the
        /// withheld key anyway). A completion whose own persist fails is
        /// marked owed and retried at the consent dispatch points.
        /// </summary>
        private void CompleteWithheldGrantStatePersist()
        {
            // NEVER BLOCK on the decision gate here: this runs inside the
            // synchronous ack chain, which can hold the dispatch handoff
            // gate — while a concurrent SetConsent holds the decision gate
            // and its own kick wants that handoff gate (a classic ABBA).
            // TryEnter keeps the order acyclic: when the gate is busy the
            // persist is marked owed and the next dispatch point (with the
            // gate free) completes it. Re-entrant for the in-call chains
            // that already hold the decision gate.
            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                lock (_gate)
                {
                    _withheldGrantStatePersistOwed = true;
                }

                return;
            }

            try
            {
                bool standingDenied;
                string obligationDecidedAt;
                long obligationDecisionSeq;
                lock (_gate)
                {
                    // UNJUDGEABLE denial-marker slot: the slot may witness a denial that can prove
                    // neither actor nor order — a deferred grant persist
                    // must not reopen the pipeline under it, and unlike a
                    // fresh explicit decision it supersedes nothing. Kept
                    // OWED, never discharged: a readable later launch
                    // judges the real slot and re-runs the completion. (A
                    // fresh explicit decision THIS session clears the hold
                    // — it supersedes whatever the slot held — so its own
                    // withheld completion is never wedged.)
                    if (_consentOutbox.LoadedRecordMalformed)
                    {
                        _withheldGrantStatePersistOwed = false;
                        return;
                    }

                    if (ConsentEvidenceUnjudgeableLocked())
                    {
                        _withheldGrantStatePersistOwed = true;
                        return;
                    }

                    standingDenied = ShardPilotConsent.IsDeniedState(_consentState);
                    obligationDecidedAt = _withheldGrantDecidedAt;
                    obligationDecisionSeq = _withheldGrantDecisionSeq;
                }

                if (standingDenied)
                {
                    // Superseded by a newer denial: its own persist
                    // ordering governs now — but ONLY when the denial
                    // actually postdates (or cannot be ordered against)
                    // the obligation: a marker/belt imposition of a
                    // PROVABLY OLDER denial must not eat the newer grant
                    // the backend already recorded; the completion proceeds and overwrites it. An
                    // UNKNOWN state proceeds — that is precisely the
                    // RELAUNCH-armed shape (the withheld grant is the
                    // state whose persist is owed; the reloaded client
                    // sits Unknown until this completion lands) — and
                    // every in-session path that leaves Granted for a
                    // reason other than the withhold itself clears the
                    // withheld key, so the key-matched callers cannot
                    // reach here stale; the persisted-record validation
                    // below re-checks the cross-client truth regardless.
                    if (StandingDenialOutranksObligation(
                            obligationDecidedAt, obligationDecisionSeq))
                    {
                        lock (_gate)
                        {
                            _withheldGrantStatePersistOwed = false;
                        }

                        return;
                    }
                }

                // A DISPLACED client stands down: a same-scope successor owns
                // the record now, and — crucially — this client left the
                // propagation set at the replacement, so a denial decided on
                // the successor never reached this instance's in-memory
                // state. Validating only against OUR OWN _consentState would
                // let this late settle write Granted straight over the
                // successor's persisted denial. The successor's own
                // decisions (and its receipts) govern the scope.
                if (_consentOutbox.IsDisowned)
                {
                    lock (_gate)
                    {
                        _withheldGrantStatePersistOwed = false;
                    }

                    return;
                }

                // And even for the current owner, validate against the
                // PERSISTED record under the decision gate — the cross-client
                // truth for this backend: a denial recorded by a path that
                // never propagated here (or whose propagation raced this
                // settle) must win — unless it is PROVABLY OLDER than the
                // obligation (a stale imposed witness the completion
                // overwrites). An ABSENT record is
                // the expected precondition (that is exactly what this
                // completion exists to fill in); an unreadable record
                // fails CLOSED — the write stays owed and a later dispatch
                // point revalidates.
                bool persistedReadFailed;
                var persisted = ShardPilotConsent.Load(_settingsStore, _config.AnonymousId, out persistedReadFailed);
                if (persistedReadFailed ||
                    (ShardPilotConsent.IsDeniedState(persisted) &&
                     StandingDenialOutranksObligation(
                         obligationDecidedAt, obligationDecisionSeq)))
                {
                    lock (_gate)
                    {
                        _withheldGrantStatePersistOwed = persistedReadFailed;
                    }

                    return;
                }

                // The completed grant persists with the OBLIGATION's
                // original decision pair when the marker carried the seq
                // (the honest original end to end); a seq-less obligation falls back to a fresh mint,
                // with the original stamp still the pair's primary
                // ordinal.
                string completionDecidedAt;
                long completionDecisionSeq;
                lock (_gate)
                {
                    completionDecidedAt = _withheldGrantDecidedAt;
                    completionDecisionSeq = _withheldGrantDecisionSeq;
                }

                if (PersistConsentState(
                        ShardPilotConsentState.Granted,
                        completionDecidedAt,
                        completionDecisionSeq > 0
                            ? completionDecisionSeq
                            : ShardPilotConsent.NextDecisionSeq()))
                {
                    // The persisted GRANT is the latest durable decision: it
                    // stands down any older denial's witness-pending pair,
                    // exactly like the explicit grant persist.
                    lock (_gate)
                    {
                        _consentDenialRecordPending = false;
                        _consentDenialMarkerPending = false;
                        _consentDenialPendingDecidedAt = null;
                        _consentDenialPendingSeq = 0;
                    }

                    // And it owes the SAME convergence as either explicit route
                    // to a durable grant — the scoped stale-marker supersession
                    // and the fresh-decision backup sync. This asynchronous route
                    // used to take only the first of the two, so a corrupt backup
                    // survived the completed grant and demoted it at the next
                    // launch. Deliberately AFTER the stand-down above, so the sync
                    // judges the key against this grant rather than against the
                    // denial it just superseded.
                    ConvergeDurableGrantPersist();

                    // The completing decision's ORIGINAL moment, snapshotted
                    // BEFORE the bookkeeping clear wipes it: the settle-time
                    // distribution below stamps sibling receipts with it. In
                    // a RESTARTED process the session grant marker is empty
                    // — relying on it would leave late-registered siblings
                    // gated against the just-persisted grant — so the
                    // obligation's own decided_at (carried by the durable
                    // marker across the exit) is the source of truth.
                    string decidedAtForDistribution;
                    string completedReceiptKey;
                    ShardPilotConsentNoticeSnapshot completedNotice;
                    lock (_gate)
                    {
                        _withheldGrantStatePersistOwed = false;
                        decidedAtForDistribution = _withheldGrantDecidedAt;
                        completedReceiptKey = _withheldGrantStateKey;
                        completedNotice = _withheldGrantNotice;
                    }

                    // The completed pair discharges the DURABLE obligation
                    // marker (still under the decision gate), and the
                    // RELAUNCH-armed flavor converges this client's own
                    // in-memory state too: the reloaded client sat Unknown
                    // while the persist was owed — the in-session flavor is
                    // already Granted and this is a no-op flip that mints
                    // nothing (armOwnScopeReceipt false). The completion
                    // carries EXACTLY the durable own-scope evidence a
                    // scoped cap-refusal hold demands — its receipt settled
                    // and its state just persisted — so it discharges any
                    // standing scoped marker and its convergence bypasses
                    // the receipt-less-apply hold (a stranded marker would re-downgrade the scope at the
                    // next launch despite the delivered receipt).
                    ClearWithheldGrantMarkerBookkeeping();
                    ShardPilotConsent.ClearScopedPendingDowngrade(_settingsStore, _consentScopeHash);
                    ApplyGlobalConsentGrant(
                        armOwnScopeReceipt: false, withheldCompletionConvergence: true);

                    // The withheld GRANT is now the durably persisted truth:
                    // stamp the OLDER retained pending grants superseded
                    // (the completed receipt itself — the state's backer —
                    // exempted; it may already be pruned, which exempts
                    // nothing, harmlessly), land the stamp, and reach live
                    // sibling scopes through the same dispatcher pass the
                    // explicit-decision path uses. Runs BEFORE the
                    // settle-time distribution so late siblings' fresh arms
                    // append after it and stay unstamped.
                    _consentOutbox.OwnScope.MarkRetainedAnalyticsGrantsSuperseded(completedReceiptKey);
                    PersistConsentOutbox();
                    ShardPilotDispatcher.StampSupersededConsentGrants(this);

                    // SETTLE-TIME DISTRIBUTION: while the observable grant
                    // was withheld, a sibling could construct and register
                    // against the still-ABSENT persisted record — the
                    // decision-time propagation never saw it (not registered
                    // then) and its registration refresh read Unknown, so it
                    // sat consent-gated against a decision the player made
                    // and the backend has now accepted. Completing the
                    // ordered pair therefore also completes the DISTRIBUTION:
                    // late-constructed live siblings converge to the grant
                    // and arm their OWN scope's receipt (original decided_at
                    // from the still-live session marker) — while siblings
                    // the decision-time propagation already reached are
                    // skipped, so no scope double-mints for one decision.
                    // Lock-order safe: decision gate -> registry snapshot is
                    // the established propagation order, and any nested
                    // re-entry into this method on the same thread TryEnters
                    // the (re-entrant) decision gate it already holds. The
                    // obligation's ORIGINAL decided_at rides along so a
                    // post-restart completion (dead session marker) still
                    // arms sibling receipts; an unparseable moment falls
                    // back to the marker-less refusal (fail closed, the
                    // sibling converges via its refresh instead).
                    ShardPilotDispatcher.PropagateWithheldGrantCompletion(
                        this, ShardPilotConsentOutbox.ParseReceiptTimestamp(decidedAtForDistribution), completedNotice);
                }
                else
                {
                    lock (_gate)
                    {
                        _withheldGrantStatePersistOwed = true;
                    }

                    SafeLog(
                        "shardpilot consent: the settled grant receipt's withheld state persist " +
                        "failed; the write stays owed and retries at the dispatch points");
                }
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// Keeps the durable withheld-obligation marker in sync with the
        /// in-memory obligation: (re)writes an owed marker, and UPGRADES its
        /// receipt-durability phase once the receipt has landed in the
        /// durable record — the phase is what lets a relaunch distinguish a
        /// receipt that is absent-because-DELIVERED (safe to complete on
        /// load) from one LOST to a kill before it was ever durable (fail
        /// closed: the backend never saw it). Runs at every consent
        /// dispatch point via <see cref="RetryOwedConsentWriteIfDirty"/>;
        /// cheap no-op without an armed obligation. TryEnter on the
        /// decision gate — marker mutations are gate-serialized so this can
        /// never interleave with a decision's supersession-clear and
        /// resurrect a cleared marker.
        /// </summary>
        private void MaybeSyncWithheldGrantMarker()
        {
            string key;
            string decidedAt;
            ShardPilotConsentNoticeSnapshot notice;
            long decisionSeq;
            bool markerArmed;
            bool markerDurablePhase;
            lock (_gate)
            {
                key = _withheldGrantStateKey;
                decidedAt = _withheldGrantDecidedAt;
                notice = _withheldGrantNotice;
                decisionSeq = _withheldGrantDecisionSeq;
                markerArmed = _withheldGrantMarkerArmed;
                markerDurablePhase = _withheldGrantMarkerReceiptDurable;
            }

            if (key == null || decidedAt == null)
            {
                return;
            }

            var receiptNowDurable = markerDurablePhase ||
                                    (!_consentOutbox.Dirty && _consentOutbox.OwnScope.Contains(key));
            if (markerArmed && (markerDurablePhase || !receiptNowDurable))
            {
                // Marker durable and its phase current — nothing owed.
                return;
            }

            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                return;
            }

            try
            {
                lock (_gate)
                {
                    // Re-validated inside the gate: a decision's
                    // supersession-clear cannot interleave from here on.
                    if (!string.Equals(_withheldGrantStateKey, key, StringComparison.Ordinal))
                    {
                        return;
                    }
                }

                if (ShardPilotConsent.RecordWithheldGrant(
                        _settingsStore, _consentScopeHash, key, decidedAt, decisionSeq,
                        receiptNowDurable, notice))
                {
                    lock (_gate)
                    {
                        if (string.Equals(_withheldGrantStateKey, key, StringComparison.Ordinal))
                        {
                            _withheldGrantMarkerArmed = true;
                            _withheldGrantMarkerReceiptDurable = receiptNowDurable;
                        }

                        // The landed (re)write owns the scope marker: any
                        // owed discharge of previous content is superseded.
                        _withheldMarkerDischargeOwed = false;
                    }
                }
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// Upgrades the durable marker's receipt-durability phase in the
        /// SAME operation that made the receipt durable — the in-call
        /// decision-path writes (the grant's receipt-first persist, the
        /// durability-judgment retry, the withheld-heal completion). The
        /// dispatch points' <see cref="MaybeSyncWithheldGrantMarker"/> is
        /// TryEnter-gated and can lose the decision gate to this very
        /// call, so relying on it leaves a window where the receipt is
        /// durable, the state persist fails, and a dispatch+ack+prune
        /// rewrites the record empty before any sync runs: a kill then
        /// relaunches [undecided + clean-empty + pending-phase], treating
        /// a backend-recorded grant as LOST (fail-closed but wrong — the
        /// player's decision re-asked though the backend has it). Callers
        /// hold the decision gate, so the marker write cannot interleave
        /// with a supersession-clear. No-op unless THIS entry's obligation
        /// is armed with the phase still pending and the receipt is
        /// durably retained (not owed, not pruned — a pruned receipt's
        /// completion belongs to the settle path).
        /// </summary>
        private void UpgradeWithheldMarkerPhaseAfterDurableWrite(ShardPilotConsentOutboxEntry entry)
        {
            if (entry == null || !entry.AnalyticsGranted)
            {
                return;
            }

            string decidedAt;
            long decisionSeq;
            lock (_gate)
            {
                if (!string.Equals(_withheldGrantStateKey, entry.IdempotencyKey, StringComparison.Ordinal) ||
                    _withheldGrantMarkerReceiptDurable)
                {
                    return;
                }

                decidedAt = _withheldGrantDecidedAt;
                decisionSeq = _withheldGrantDecisionSeq;
            }

            if (decidedAt == null ||
                _consentOutbox.Dirty ||
                !_consentOutbox.OwnScope.Contains(entry.IdempotencyKey))
            {
                return;
            }

            if (ShardPilotConsent.RecordWithheldGrant(
                    _settingsStore, _consentScopeHash, entry.IdempotencyKey, decidedAt, decisionSeq,
                    receiptDurable: true, notice: entry.Notice))
            {
                lock (_gate)
                {
                    if (string.Equals(_withheldGrantStateKey, entry.IdempotencyKey, StringComparison.Ordinal))
                    {
                        _withheldGrantMarkerArmed = true;
                        _withheldGrantMarkerReceiptDurable = true;
                    }

                    // The landed write owns the scope marker: any owed
                    // discharge of previous content is superseded.
                    _withheldMarkerDischargeOwed = false;
                }
            }
        }

        /// <summary>
        /// Clears the durable withheld-obligation marker and its
        /// bookkeeping — a completed pair or a superseding decision
        /// discharges the obligation. Callers hold the decision gate.
        /// </summary>
        private void ClearWithheldGrantMarkerBookkeeping()
        {
            bool armed;
            lock (_gate)
            {
                armed = _withheldGrantMarkerArmed;
                _withheldGrantMarkerArmed = false;
                _withheldGrantMarkerReceiptDurable = false;
                _withheldGrantDecidedAt = null;
                _withheldGrantNotice = null;
                _withheldGrantDecisionSeq = 0;
            }

            if (armed)
            {
                DischargeWithheldMarkerDurable();
            }
        }

        /// <summary>
        /// Discharges the scope's DURABLE withheld-grant marker and keeps a
        /// failed clear OWED instead of swallowing it: the callers drop
        /// their in-memory bookkeeping at discharge time by design (the
        /// obligation is judged dead), so a silently-failed clear would
        /// leave the marker to survive its own discharge — and a later
        /// clean rewrite of the outbox record would mint the false
        /// [undecided + clean-empty + durable] delivery proof at the next
        /// launch. See <see cref="_withheldMarkerDischargeOwed"/> for the
        /// owed discipline (dispatch-point retries, record-rewrite refusal,
        /// resolution stand-down, teardown deferral).
        /// </summary>
        private void DischargeWithheldMarkerDurable()
        {
            var cleared = ShardPilotConsent.ClearWithheldGrant(_settingsStore, _consentScopeHash);
            lock (_gate)
            {
                _withheldMarkerDischargeOwed = !cleared;
            }

            if (!cleared)
            {
                SafeLog(
                    "shardpilot consent: the withheld-grant marker's discharge could not be " +
                    "written; the clear stays owed and retries at the dispatch points (durable " +
                    "record rewrites are refused meanwhile)");
            }
        }

        /// <summary>
        /// Retries an OWED withheld-marker discharge. Runs at every consent
        /// dispatch point (ahead of the owed record write — the discharge
        /// must land before any rewrite may produce the clean-empty half of
        /// the false proof). TryEnter on the decision gate keeps marker
        /// mutations serialized against arms and supersession-clears
        /// (re-entrant for in-call chains already holding it); a busy gate
        /// retries at the next dispatch point. Stands down while an
        /// obligation is ARMED in-memory — a fresh arm overwrote the scope
        /// marker in place and owns it now (the arm sites also clear the
        /// owed flag under the gate; this guard is the belt).
        /// </summary>
        private void RetryOwedWithheldMarkerDischargeIfAny()
        {
            lock (_gate)
            {
                if (!_withheldMarkerDischargeOwed)
                {
                    return;
                }
            }

            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                return;
            }

            try
            {
                lock (_gate)
                {
                    if (!_withheldMarkerDischargeOwed || _withheldGrantStateKey != null)
                    {
                        return;
                    }
                }

                if (ShardPilotConsent.ClearWithheldGrant(_settingsStore, _consentScopeHash))
                {
                    lock (_gate)
                    {
                        _withheldMarkerDischargeOwed = false;
                    }
                }
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// Reloads a previous launch's WITHHELD-GRANT obligation at
        /// construction and resolves it through the relaunch/settle matrix
        /// (marker phase + persisted record + trail order, LWW):
        /// - persisted DECIDED (granted or denied): the marker is stale —
        ///   the completion landed before the exit, or a newer decision
        ///   superseded it (denials persist state-FIRST) — cleared;
        /// - the marked receipt was identity-dropped at this construction:
        ///   the grant can never be documented for the new identity — the
        ///   identity downgrade governs; cleared FIRST, even when the
        ///   consent key is unreadable (clearing writes no consent state,
        ///   and the drop has already dirtied the outbox toward an empty
        ///   save the surviving marker would falsely complete against);
        /// - outbox unreadable: nothing provable — the marker stays for a
        ///   later launch (fail closed, nothing persisted);
        /// - receipt STILL RETAINED and the TRAIL TAIL (no later decision
        ///   behind it): the obligation is live — re-armed, and the normal
        ///   settle machinery completes it when the re-sent receipt acks
        ///   (or terminally drops); a receipt behind LATER entries is
        ///   superseded (trail order is decision order) — cleared;
        /// - receipt ABSENT with the marker's receipt-durability phase SET:
        ///   provably delivered-then-pruned (a durable receipt leaves the
        ///   record only by settle-prune; cap eviction is impossible while
        ///   the marker lives, because any newer decision would have
        ///   cleared it) — the grant persist completes ON LOAD, exactly
        ///   what the backend already recorded;
        /// - receipt ABSENT with the phase UNSET: the receipt died with the
        ///   kill before ever becoming durable — the backend never saw the
        ///   grant, and completing would open a receipt-less pipeline; the
        ///   marker clears and the player is asked again (fail closed).
        /// </summary>
        private void ProcessWithheldGrantMarkerAtConstruction(
            List<ShardPilotConsentOutboxEntry> identityDropped, bool consentReadFailed)
        {
            var marker = ShardPilotConsent.LoadWithheldGrant(_settingsStore, _consentScopeHash);
            if (marker == null)
            {
                return;
            }

            // The identity-dropped discharge runs FIRST — before the
            // consent-read-failure bailout: discharging the marker never
            // WRITES consent state, so an unreadable consent key changes
            // nothing about its safety, while bailing would leave the
            // marker alive after the drop already left the outbox dirty.
            // The later empty save would then combine with the surviving
            // receipt_durable marker into a false [absent + clean-empty +
            // durable] completion for the NEW identity at the next launch —
            // a grant whose receipt was explicitly dropped as
            // undeliverable.
            if (identityDropped != null)
            {
                for (var i = 0; i < identityDropped.Count; i++)
                {
                    if (string.Equals(
                            identityDropped[i].IdempotencyKey, marker.IdempotencyKey, StringComparison.Ordinal))
                    {
                        DischargeWithheldMarkerDurable();
                        SafeLog(
                            "shardpilot consent: the identity change discharged the scope's " +
                            "withheld-grant marker along with its dropped receipt — the grant can " +
                            "never be documented for the new identity (fresh consent required)");
                        return;
                    }
                }
            }

            if (consentReadFailed)
            {
                // The record may hold a NEWER decision this marker must not
                // overwrite; leave everything else for a readable launch.
                return;
            }

            if (_consentState != ShardPilotConsentState.Unknown)
            {
                // A GRANTED state means the record already holds this
                // grant or a newer one — the obligation is settled. A
                // DENIED state discharges only when the denial actually
                // POSTDATES (or cannot be ordered against) the
                // obligation: a marker/belt imposition of a PROVABLY
                // OLDER denial must not eat the newer grant the backend
                // already recorded — the matrix below re-arms or
                // completes it instead, and the completion overwrites the
                // stale denial and retires its witness.
                if (!ShardPilotConsent.IsDeniedState(_consentState) ||
                    StandingDenialOutranksObligation(marker.DecidedAt, marker.DecisionSeq))
                {
                    DischargeWithheldMarkerDurable();
                    return;
                }
            }

            if (_consentOutbox.LoadFailed)
            {
                // The record is UNREADABLE: nothing about the marked
                // receipt (retained? superseded? delivered?) can be judged
                // yet. The resolution is DEFERRED — not skipped: the flag
                // makes the dispatch points re-run this matrix as soon as a
                // heal recovers the record, and the consent head dispatch
                // STANDS DOWN until then — otherwise the recovered marked
                // receipt could dispatch and prune before the obligation is
                // re-armed, losing the completion exactly like the
                // arm-order hole this layer exists to close.
                lock (_gate)
                {
                    _withheldMarkerResolutionPending = true;
                }

                return;
            }

            ResolveWithheldGrantMarkerCore(marker);
        }

        /// <summary>
        /// The shared retained/absent half of the withheld-marker matrix —
        /// runs at construction once the outbox is readable, and again from
        /// <see cref="ResolveWithheldGrantMarkerAfterOutboxRecovery"/> when
        /// the construction-time read failure heals later. Two validity
        /// gates precede the matrix, both discharging fail-closed: a marker
        /// whose decided_at cannot parse (the obligation can never
        /// faithfully mint follow-ups), and a PARTIALLY-SANITIZED record
        /// (the surviving trail — empty or partial — cannot prove tail
        /// order or delivery).
        /// </summary>
        private void ResolveWithheldGrantMarkerCore(ShardPilotWithheldGrantMarker marker)
        {
            if (ShardPilotConsentOutbox.ParseReceiptTimestamp(marker.DecidedAt) == null)
            {
                // A marker whose decided_at does not PARSE is an invalid
                // obligation, judged here at the completion decision itself:
                // completing (or re-arming toward a settle-time completion)
                // would flip states while the distribution — which stamps
                // sibling receipts with the ORIGINAL decision moment — can
                // never faithfully mint a follow-up from it. Fail closed:
                // discharged, nothing persisted, fresh consent required.
                DischargeWithheldMarkerDurable();
                SafeLog(
                    "shardpilot consent: a withheld-grant marker's decided_at is unparseable; the " +
                    "obligation is invalid — failing closed, fresh consent required");
                return;
            }

            if (_consentOutbox.LoadedRecordMalformed)
            {
                // Content was LOST to the receipt sanitizer on the way in
                // (wholly garbled, a missing receipts array, or individual
                // dropped entries): the surviving trail — EMPTY or PARTIAL —
                // is not the trail the decisions wrote, so NEITHER half of
                // the matrix below can be trusted: the retained-marker tail
                // check may be looking at a tail whose dropped successor was
                // a newer superseding denial, and an empty produced by
                // discarding content is not the clean delivered-and-pruned
                // rewrite shape. The completion decision fails
                // CLOSED like a read failure — and because the proof is
                // permanently unrecoverable (the next clean rewrite would
                // launder the record into a trustworthy-looking shape), the
                // marker is discharged NOW rather than left to falsely
                // complete a later launch. Surviving entries still deliver
                // as ordinary trail receipts; only the marker's
                // completion/re-arm is refused. Fresh consent required;
                // nothing is ever persisted as granted from a torn record.
                DischargeWithheldMarkerDurable();
                SafeLog(
                    "shardpilot consent: a withheld-grant marker met a partially-sanitized outbox " +
                    "record; the trail cannot prove the obligation — failing closed, fresh consent " +
                    "required");
                return;
            }

            if (_consentOutbox.OwnScope.Contains(marker.IdempotencyKey))
            {
                var trail = _consentOutbox.OwnScope.SnapshotEntries();
                var isTail = trail.Count > 0 &&
                             string.Equals(
                                 trail[trail.Count - 1].IdempotencyKey,
                                 marker.IdempotencyKey,
                                 StringComparison.Ordinal);
                if (!isTail)
                {
                    DischargeWithheldMarkerDurable();
                    return;
                }

                lock (_gate)
                {
                    _withheldGrantStateKey = marker.IdempotencyKey;
                    _withheldGrantDecidedAt = marker.DecidedAt;
                    _withheldGrantNotice = trail[trail.Count - 1].Notice;
                    _withheldGrantDecisionSeq = marker.DecisionSeq;
                    _withheldGrantMarkerArmed = true;

                    // Presence in the LOADED record proves durability; the
                    // stored phase may lag a kill that raced the upgrade.
                    _withheldGrantMarkerReceiptDurable = true;
                }

                SafeLog(
                    "shardpilot consent: reloaded a withheld-grant obligation; the retained receipt " +
                    "re-sends and its settle completes the observable grant persist");
                return;
            }

            if (marker.ReceiptDurable)
            {
                // ANY remaining trail entry supersedes the absent marked
                // grant: serial FIFO delivery means the grant can only have
                // been pruned after everything BEFORE it delivered, so
                // whatever the trail still holds is strictly NEWER than the
                // marked decision — and marker clears are best-effort, so a
                // newer decision whose own persist AND marker-clear both
                // died with the process leaves exactly this shape (persisted
                // undecided + a retained newer receipt + the stale grant
                // marker). Completing the OLD grant here would open
                // analytics before the newer decision even delivers. Only
                // [absent receipt + EMPTY trail + durable phase] completes;
                // everything else fails closed. (A superseding decision
                // that itself ALSO delivered and pruned while its state
                // persist and marker-clear both failed leaves no durable
                // trace this launch could consult — the accepted residual
                // of best-effort clears; the backend's last-write-wins row
                // still postdates the completed grant, so the authoritative
                // side stays correct.)
                if (_consentOutbox.OwnScope.Count > 0)
                {
                    DischargeWithheldMarkerDurable();
                    SafeLog(
                        "shardpilot consent: a stale withheld-grant marker is superseded by newer " +
                        "retained receipts; failing closed — the trail is the operative truth");
                    return;
                }

                lock (ShardPilotDispatcher.PrivacyDecisionGate)
                {
                    lock (_gate)
                    {
                        _withheldGrantMarkerArmed = true;
                        _withheldGrantDecidedAt = marker.DecidedAt;
                        _withheldGrantNotice = marker.Notice;
                        _withheldGrantDecisionSeq = marker.DecisionSeq;
                    }

                    SafeLog(
                        "shardpilot consent: reloaded a withheld-grant obligation whose receipt was " +
                        "durably delivered before the exit; completing the observable grant persist");
                    CompleteWithheldGrantStatePersist();
                }

                return;
            }

            DischargeWithheldMarkerDurable();
            SafeLog(
                "shardpilot consent: a withheld-grant obligation's receipt was lost before it became " +
                "durable; failing closed — the backend never recorded the grant, fresh consent required");
        }

        /// <summary>
        /// Re-runs the deferred withheld-marker resolution once the
        /// construction-time outbox read failure has HEALED — before the
        /// recovered head may dispatch (this runs inside
        /// <see cref="RetryOwedConsentWriteIfDirty"/>, which every dispatch
        /// path calls ahead of the head handoff, and the head dispatch
        /// additionally stands down while the resolution is pending).
        /// Defers further while the heal's own identity-recovery drop is
        /// unconsumed — <see cref="MaybeApplyRecoveryIdentityDowngrade"/>
        /// must judge (and discharge a marker whose receipt was dropped)
        /// first, or this resolution could complete a grant whose receipt
        /// the recovery just dropped as undeliverable. TryEnter on the
        /// decision gate; a busy gate retries at the next dispatch point.
        /// </summary>
        private void ResolveWithheldGrantMarkerAfterOutboxRecovery()
        {
            lock (_gate)
            {
                if (!_withheldMarkerResolutionPending)
                {
                    return;
                }

                if (_withheldMarkerDischargeOwed)
                {
                    // The on-disk marker is CONDEMNED (its discharge is
                    // owed): re-loading it here could complete or re-arm an
                    // obligation a previous judgment already discharged.
                    // Stand down; the dispatch-point retry clears the
                    // marker, after which this resolution finds nothing.
                    return;
                }
            }

            if (_consentOutbox.LoadFailed || _consentOutbox.RecoveryDroppedPendingGrant ||
                _consentOutbox.RecoveryMergedParkedPendingGrant)
            {
                return;
            }

            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                return;
            }

            try
            {
                lock (_gate)
                {
                    if (!_withheldMarkerResolutionPending)
                    {
                        return;
                    }

                    _withheldMarkerResolutionPending = false;
                    if (_withheldGrantStateKey != null)
                    {
                        // An in-session obligation owns the marker now (a
                        // fresh explicit grant re-armed it); the deferred
                        // resolution is superseded.
                        return;
                    }
                }

                var marker = ShardPilotConsent.LoadWithheldGrant(_settingsStore, _consentScopeHash);
                if (marker == null)
                {
                    return;
                }

                bool undecided;
                lock (_gate)
                {
                    undecided = _consentState == ShardPilotConsentState.Unknown;
                }

                if (!undecided)
                {
                    // A decision (or a propagated/refreshed state) landed
                    // while the resolution was deferred: the marker's grant
                    // is superseded — with the SAME pair-aware exception as
                    // the construction matrix:
                    // a DENIED state whose witness is PROVABLY OLDER than
                    // the obligation (an imposed marker/belt witness of a
                    // decision this withheld grant itself superseded) must
                    // not eat the newer grant — the matrix re-arms or
                    // completes it instead.
                    if (!ShardPilotConsent.IsDeniedState(_consentState) ||
                        StandingDenialOutranksObligation(marker.DecidedAt, marker.DecisionSeq))
                    {
                        DischargeWithheldMarkerDurable();
                        return;
                    }
                }

                ResolveWithheldGrantMarkerCore(marker);
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// The boot belt's candidate scan (shared by the construction belt
        /// and its deferred post-recovery re-run): the NEWEST retained
        /// same-actor plausibly-stamped DENIAL receipt that outranks the
        /// restored record — a modern record by a full strictly-newer pair
        /// (a receipt with a plausible stamp but NO minted seq is a LEGACY
        /// pair under the parser contract: it can fail a legacy record
        /// closed, but never outranks a STAMPED record by timestamp alone), a LEGACY record fail-closed. Against
        /// a LEGACY record the retained trail itself is the only order
        /// there is, so the scan honors it END TO END: when the newest qualifying GRANT receipt strictly
        /// outranks the newest qualifying denial — an upgrade-window
        /// deny→grant whose bare granted record already matches the
        /// trail's last word — the belt stands down instead of imposing
        /// the older denial. A pairless grant never outranks a
        /// seq-carrying denial (the same demotion receipts get against
        /// stamped records): ordering evidence that thin fails closed to
        /// the denial, and an exact tie stays with the denial too.
        /// </summary>
        private ShardPilotConsentOutboxEntry FindBeltStaleDenialAgainstRestoredRecord()
        {
            ShardPilotConsentOutboxEntry staleDenial = null;
            ShardPilotConsentOutboxEntry newestGrant = null;
            foreach (var retained in _consentOutbox.SnapshotEntries())
            {
                if (!string.Equals(
                        retained.AnonymousId, _config.AnonymousId, StringComparison.Ordinal) ||
                    !ShardPilotConsent.IsPlausibleDecisionStamp(retained.DecidedAt))
                {
                    // Receipts without a decision-time anon snapshot for
                    // THIS actor or a plausible stamp can prove neither
                    // actor scope nor order.
                    continue;
                }

                if (!retained.AnalyticsGranted)
                {
                    if ((_restoredConsentRecord.Legacy ||
                         (retained.DecisionSeq > 0 &&
                          ShardPilotConsent.DecisionPairNewer(
                              retained.DecidedAt, retained.DecisionSeq,
                              _restoredConsentRecord.DecidedAt,
                              _restoredConsentRecord.DecisionSeq))) &&
                        (staleDenial == null ||
                         ShardPilotConsent.DecisionPairNewer(
                             retained.DecidedAt, retained.DecisionSeq,
                             staleDenial.DecidedAt, staleDenial.DecisionSeq)))
                    {
                        staleDenial = retained;
                    }
                }
                else if (_restoredConsentRecord.Legacy &&
                         (newestGrant == null ||
                          ShardPilotConsent.DecisionPairNewer(
                              retained.DecidedAt, retained.DecisionSeq,
                              newestGrant.DecidedAt, newestGrant.DecisionSeq)))
                {
                    newestGrant = retained;
                }
            }

            if (staleDenial != null && newestGrant != null &&
                ShardPilotConsent.DecisionPairNewer(
                    newestGrant.DecidedAt, newestGrant.DecisionSeq,
                    staleDenial.DecidedAt, staleDenial.DecisionSeq) &&
                (staleDenial.DecisionSeq <= 0 || newestGrant.DecisionSeq > 0))
            {
                SafeLog(
                    "shardpilot consent: the legacy granted record matches the retained trail's " +
                    "newest decision (a grant newer than every retained denial); the belt stands " +
                    "down");
                return null;
            }

            return staleDenial;
        }

        /// <summary>
        /// Re-runs the boot denial belt once the construction-time outbox
        /// read failure has HEALED — before the recovered head may
        /// dispatch (this runs inside
        /// <see cref="RetryOwedConsentWriteIfDirty"/> ahead of the
        /// deferred withheld-marker resolution, mirroring the construction
        /// order belt → withheld matrix, and the head dispatch stands down
        /// while the re-run is pending). The deferral clears WITHOUT
        /// imposing when a LIVE decision landed on this client meanwhile
        /// (the decision generation moved — any explicit, propagated, or
        /// caught-up decision postdates every retained receipt), when the
        /// durable record no longer matches the construction restore (a
        /// decision landed elsewhere; the freshest record governs — the
        /// hold-arm rule applied to both arms of the re-run), or
        /// when the current state no longer holds a granted restore to
        /// guard. A flip applies the denial's full mid-session
        /// consequences — the same destructive set the deciding client's
        /// own SetConsent(denied) applies — plus the convergence write and
        /// the witness tail the construction belt runs. TryEnter on the
        /// decision gate; a busy gate retries at the next dispatch point.
        /// </summary>
        private void RunBootDenialBeltAfterOutboxRecovery()
        {
            lock (_gate)
            {
                if (!_bootBeltResolutionPending)
                {
                    return;
                }
            }

            if (_consentOutbox.LoadFailed)
            {
                return;
            }

            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                return;
            }

            try
            {
                ShardPilotConsentOutboxEntry staleDenial;
                lock (_gate)
                {
                    if (!_bootBeltResolutionPending || _consentOutbox.LoadFailed)
                    {
                        return;
                    }

                    _bootBeltResolutionPending = false;

                    // Monotonic mint floor, RECEIPT leg: the construction
                    // pass ran over the unreadable (empty) snapshot, so the
                    // recovered entries' pairs never raised it — a fresh
                    // decision minted from here must still outrank every
                    // retained pair.
                    foreach (var retained in _consentOutbox.SnapshotEntries())
                    {
                        ShardPilotConsent.RaiseDecisionSeqFloor(retained.DecisionSeq);
                    }

                    if (_consentDecisionGeneration != 0)
                    {
                        // A live decision landed while the belt was
                        // deferred: it postdates every retained receipt,
                        // and its own machinery owns convergence.
                        return;
                    }

                    var holdsGrantedRestore =
                        _consentState == ShardPilotConsentState.Granted ||
                        (ConsentEvidenceUnjudgeableLocked() &&
                         _consentState == ShardPilotConsentState.Unknown &&
                         _restoredConsentRecord.State == ShardPilotConsentState.Granted);
                    if (!holdsGrantedRestore)
                    {
                        return;
                    }

                    bool beltReadFailed;
                    var beltBase = ShardPilotConsent.LoadRecord(_settingsStore, out beltReadFailed);
                    if (!beltBase.Legacy)
                    {
                        ShardPilotConsent.RaiseDecisionSeqFloor(beltBase.DecisionSeq);
                    }

                    if (beltReadFailed ||
                        beltBase.State != _restoredConsentRecord.State ||
                        beltBase.Legacy != _restoredConsentRecord.Legacy ||
                        !string.Equals(
                            beltBase.DecidedAt, _restoredConsentRecord.DecidedAt,
                            StringComparison.Ordinal) ||
                        beltBase.DecisionSeq != _restoredConsentRecord.DecisionSeq)
                    {
                        // The durable record is no longer the construction
                        // restore this belt was armed for: a decision
                        // landed (here or in another process) that
                        // postdates every retained receipt.
                        return;
                    }

                    staleDenial = FindBeltStaleDenialAgainstRestoredRecord();
                    if (staleDenial == null)
                    {
                        return;
                    }

                    // FLIP, in the same _gate hold that judged the scan:
                    // state, holds, the denial epoch, and the queue clears
                    // move together so no publish serialized between
                    // separate holds can put a pre-denial batch on the wire
                    // against the old epoch — the same discipline as a
                    // propagated denial.
                    ApplyConsentState(string.Equals(
                        staleDenial.Reason, ShardPilotConsent.ForcedMinorReason,
                        StringComparison.Ordinal)
                        ? ShardPilotConsentState.DeniedForcedMinor
                        : ShardPilotConsentState.Denied);
                    _denialMarkerSlotUnjudgeable = false;

                    // Same rule as the boot belt: this receipt orders
                    // against the RECORD, not against a backup nobody could
                    // read, so it does not lift that hold.
                    _consentReadFailureCrashHold = false;
                    AdvanceConsentDenialEpochLocked();
                    _queue.Clear();
                    _restoredPending.Clear();
                    _spooledOverflowIds.Clear();
                    _spooledOverflowOrder.Clear();
                    _breadcrumbs.Clear();
                }

                // The recognized denial's destructive consequences — the
                // same set the deciding client's own SetConsent(denied)
                // applies (and the denied-boot branch re-runs): in-flight
                // crash dispatch fenced, both spools and every sibling
                // scope's files destroyed, the ANR eras and the deniable
                // Retry-After marker cleared, the AEI suppression floor
                // stamped. The deferred withheld-marker resolution, which
                // runs right after this in the same dispatch-point retry,
                // judges its obligation against this imposed state with
                // the pair-aware discharge gates — a newer withheld grant
                // still completes over this older recognized denial.
                lock (_crashDispatchGate)
                {
                }

                if (_spool != null)
                {
                    _spool.Wipe();
                }

                if (_crashSpool != null)
                {
                    _crashSpool.Wipe();
                }

                ShardPilotCrashSpool.ClearRetryAfterDeadline(_settingsStore, _config);
                lock (_gate)
                {
                    _crashResendNotBefore = DateTime.MinValue;
                }

                ShardPilotAnrEraStore.WipeScope(_config, _settingsStore, message => SafeLog("{0}", message));
                ShardPilotAeiCursor.RecordSuppressionFloor(
                    _settingsStore, _config, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                QuarantineSpoolIfSweepOwed(ShardPilotSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));
                QuarantineCrashSpoolIfSweepOwed(ShardPilotCrashSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));
                ShardPilotAnrEraStore.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message));

                if (_experiments != null)
                {
                    _experiments.OnAnalyticsPurge();
                }

                // Convergence + the witness tail, exactly like the
                // construction belt: a converged record retires the marker
                // slot on the scoped rule; a refused convergence writes the
                // write-ahead marker as the receipt-independent witness and
                // arms the pending pair so the settle guard retains the
                // receipt over the open gap and the ladders retry.
                ShardPilotConsentState beltState;
                lock (_gate)
                {
                    beltState = _consentState;
                }

                var converged = PersistConsentState(
                    beltState, staleDenial.DecidedAt, staleDenial.DecisionSeq);
                var beltWitnessDurable = false;
                if (converged)
                {
                    ShardPilotConsent.RetireDenialMarkerScoped(
                        _settingsStore, _config.AnonymousId);
                }
                else
                {
                    _stats.RecordConsentPersistFailed();
                    beltWitnessDurable = ShardPilotConsent.RecordDenialMarker(
                        _settingsStore, beltState, _config.AnonymousId,
                        staleDenial.DecidedAt, staleDenial.DecisionSeq);
                    lock (_gate)
                    {
                        _consentDenialRecordPending = true;
                        _consentDenialMarkerPending = !beltWitnessDurable;
                        _consentDenialPendingDecidedAt = staleDenial.DecidedAt;
                        _consentDenialPendingSeq = staleDenial.DecisionSeq;
                    }
                }

                SafeLog(
                    "shardpilot consent: a denial receipt recovered from the healed outbox " +
                    "outranks the restored granted record; imposing the denial{0}",
                    converged
                        ? " (record converged)"
                        : beltWitnessDurable
                            ? " (convergence failed; the marker slot holds the witness)"
                            : " (convergence and marker write both failed; witness writes retry " +
                              "at the settle guard and the shutdown ladder)");
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// Fail-closes the scope's withheld marker when an IDENTITY-CHANGED
        /// recovery drop is consumed and the marker's receipt is no longer
        /// retained: the drop removed a receipt that could never deliver
        /// for the new identity, and the marker documenting its obligation
        /// must go with it — even when no state downgrade was needed —
        /// or a later launch would read [absent receipt + durable phase +
        /// healed-empty trail] and complete a grant whose receipt was
        /// explicitly dropped as undeliverable. A marker whose receipt IS
        /// still retained survived the identity filter (identity-matched)
        /// and stays live. Runs under the decision gate (the consume paths
        /// hold it).
        /// </summary>
        private void DischargeWithheldMarkerAfterIdentityRecovery()
        {
            var marker = ShardPilotConsent.LoadWithheldGrant(_settingsStore, _consentScopeHash);
            if (marker == null)
            {
                return;
            }

            if (_consentOutbox.OwnScope.Contains(marker.IdempotencyKey))
            {
                return;
            }

            lock (_gate)
            {
                if (string.Equals(_withheldGrantStateKey, marker.IdempotencyKey, StringComparison.Ordinal))
                {
                    _withheldGrantStateKey = null;
                    _withheldGrantStatePersistOwed = false;
                }

                _withheldGrantMarkerArmed = false;
                _withheldGrantMarkerReceiptDurable = false;
                _withheldGrantDecidedAt = null;
                _withheldGrantNotice = null;
                _withheldGrantDecisionSeq = 0;
                _withheldMarkerResolutionPending = false;
            }

            DischargeWithheldMarkerDurable();
            SafeLog(
                "shardpilot consent: the identity-changed recovery drop discharged the scope's " +
                "withheld-grant marker — the dropped receipt's grant can never be documented for " +
                "the new identity (fresh consent required)");
        }

        /// <summary>
        /// Retries an owed Mode-B identity-downgrade clear — SERIALIZED
        /// under the decision gate (TryEnter, never blocking: dispatch
        /// points can sit inside the synchronous ack chain, which holds the
        /// dispatch handoff gate) with the shadow RE-CHECKED inside the
        /// gate. Without the gate the retry could interleave between a
        /// registration refresh's stale-granted read and its shadow check
        /// (resurrecting the receipt-less grant), or between a concurrent
        /// explicit decision's shadow-supersession and its Save (wiping the
        /// FRESH record with the stale clear).
        /// </summary>
        private void RetryOwedIdentityDowngradeIfAny()
        {
            if (!ShardPilotConsent.HasPendingDowngrade(_settingsStore))
            {
                return;
            }

            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                // The next dispatch point retries with the gate free.
                return;
            }

            try
            {
                if (ShardPilotConsent.HasPendingDowngrade(_settingsStore))
                {
                    if (ShardPilotConsent.Clear(_settingsStore))
                    {
                        ShardPilotConsent.ClearPendingDowngrade(_settingsStore);
                    }
                    else
                    {
                        // Re-attempt the DURABLE shadow marker while the
                        // clear stays owed: whichever of the two lands first
                        // closes the process-kill window (a kill with
                        // neither durable would relaunch into the stale
                        // "granted" record with no shadow at all).
                        ShardPilotConsent.RecordPendingDowngrade(_settingsStore);
                    }
                }
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// Consumes the outbox's recovery-dropped-pending-grant flag (a
        /// healing re-read dropped a Mode-B identity-mismatched GRANT the
        /// construction could not see because the record was unreadable
        /// then) and runs the SAME identity downgrade the constructor runs:
        /// state to Unknown, persisted grant cleared (shadowed on failure),
        /// propagated to live siblings. Under the decision gate via
        /// TryEnter; the flag stays set while the gate is busy so the next
        /// dispatch point retries.
        /// </summary>
        private void MaybeApplyRecoveryIdentityDowngrade()
        {
            if (!_consentOutbox.RecoveryDroppedPendingGrant &&
                !_consentOutbox.RecoveryMergedParkedPendingGrant)
            {
                return;
            }

            // A DISPLACED instance stands down entirely — flag left alone: a
            // same-scope successor owns the record now, runs its own load
            // (and its own construction-time identity drop / downgrade), and
            // a displaced instance clearing the SHARED persisted consent
            // record or propagating a downgrade would act on state it no
            // longer governs.
            if (_consentOutbox.IsDisowned)
            {
                return;
            }

            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                return;
            }

            try
            {
                bool granted;
                bool freshDecisionLanded;
                lock (_gate)
                {
                    granted = _consentState == ShardPilotConsentState.Granted;
                    freshDecisionLanded = _consentDecisionGeneration > 0;
                }

                if (!granted)
                {
                    // Nothing to downgrade (a denial or a fresh Unknown is
                    // already fail-closed); consume the flags so they do not
                    // re-fire. The scope's withheld marker is still judged:
                    // a marker whose receipt the recovery just dropped must
                    // go with it — EVEN with no state downgrade needed — or
                    // a later launch would complete a grant whose receipt
                    // was explicitly dropped as undeliverable. (A merged
                    // PARKED receipt keeps its marker: the receipt is
                    // retained, not dropped.)
                    if (_consentOutbox.ConsumeRecoveryDroppedPendingGrant())
                    {
                        DischargeWithheldMarkerAfterIdentityRecovery();
                    }

                    _consentOutbox.ConsumeRecoveryMergedParkedPendingGrant();
                    return;
                }

                if (freshDecisionLanded)
                {
                    // The dropped/merged record documented the grant LOADED
                    // at construction — but an EXPLICIT in-session decision
                    // has landed since (the outbox load failure began
                    // strictly before it, at construction), and the heal
                    // commonly runs inside that very decision's own persist.
                    // The newest decision governs: its own receipt and its
                    // own persist ordering already cover this scope, so the
                    // stale lineage must not wipe it. Consume the flags —
                    // the superseded lineage never re-fires. (A merged
                    // parked grant the fresh decision's stamp pass could not
                    // reach — it merged after the stamp — re-detects at the
                    // NEXT construction scan as an unsuperseded parked
                    // grant: one extra fail-closed downgrade, never a
                    // fail-open.) The marker judgment still runs for drops:
                    // the fresh decision's OWN obligation (if any) points at
                    // a RETAINED identity-matched receipt and survives; a
                    // stale marker pointing at the dropped receipt goes.
                    if (_consentOutbox.ConsumeRecoveryDroppedPendingGrant())
                    {
                        DischargeWithheldMarkerAfterIdentityRecovery();
                    }

                    if (_consentOutbox.ConsumeRecoveryMergedParkedPendingGrant())
                    {
                        // The merged parked grant arrived AFTER the fresh
                        // decision's own stamp pass, so it is unsuperseded —
                        // and the grant-receipt gate holds every batch on an
                        // unsuperseded parked grant (without this stamp the pipeline would wedge on
                        // consent_receipt_pending until relaunch). Stamp the
                        // merged PARKED grants now, but only with the
                        // durable evidence in hand: a CLEAN durable read of
                        // Granted here — with the in-memory state granted
                        // and a fresh in-session decision — proves the
                        // fresh grant durably established (a failed grant
                        // persist leaves the durable at its non-granted old
                        // value, and a fresh DENIAL takes the !granted
                        // branch above). Anything else stamps nothing: the
                        // fresh decision's own persist retry runs its
                        // post-persist stamp pass when it lands, and the
                        // held gate is the correct fail-closed posture
                        // until then.
                        bool durableReadFailed;
                        if (ShardPilotConsent.Load(_settingsStore, _config.AnonymousId, out durableReadFailed) ==
                            ShardPilotConsentState.Granted && !durableReadFailed)
                        {
                            _consentOutbox.OwnScope.MarkParkedAnalyticsGrantsSuperseded();
                            PersistConsentOutbox();
                        }
                    }

                    SafeLog(
                        "shardpilot consent: the healed outbox record carried a stale pending grant " +
                        "receipt, but a fresh explicit decision superseded that lineage — the " +
                        "current state stands");
                    return;
                }

                var recoveryDropped = _consentOutbox.ConsumeRecoveryDroppedPendingGrant();
                var recoveryMergedParked = _consentOutbox.ConsumeRecoveryMergedParkedPendingGrant();
                if (!recoveryDropped && !recoveryMergedParked)
                {
                    return;
                }

                if (recoveryDropped)
                {
                    DischargeWithheldMarkerAfterIdentityRecovery();
                }

                SafeLog(
                    recoveryDropped
                        ? "shardpilot consent: the healed outbox record dropped a pending grant receipt " +
                          "(identity_changed); the unbacked grant is downgraded — a fresh explicit " +
                          "SetConsent is required for the new identity"
                        : "shardpilot consent: the healed outbox record merged a PARKED pending grant " +
                          "receipt this session cannot vouch; the unbacked grant is downgraded " +
                          "fail-closed (the receipt stays retained for a later vouched launch)");
                ApplyIdentityGrantDowngrade();
                if (!ShardPilotConsent.Clear(_settingsStore))
                {
                    ShardPilotConsent.RecordPendingDowngrade(_settingsStore);
                }
                else
                {
                    // The HEAL armed the durable downgrade marker BEFORE it
                    // saved the healed record (so a kill in between could
                    // never resurrect the receipt-less grant); the completed
                    // clear discharges it.
                    ShardPilotConsent.ClearPendingDowngrade(_settingsStore);
                }

                // The downgrade-to-Unknown routes through the same
                // consent-first spool posture the CONSTRUCTOR's identity
                // downgrade takes (it falls through to the non-granted
                // all-scope purge): the consent key is GLOBAL while spool
                // files are per-scope, so beyond this client's own wipe
                // (inside ApplyIdentityGrantDowngrade) every sibling scope's
                // file in the active directory is swept too — fail-closed
                // per file via the sweep-owed marker — or a later launch of
                // a sibling scope would load pre-downgrade events under a
                // grant this downgrade just revoked.
                QuarantineSpoolIfSweepOwed(ShardPilotSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));

                ShardPilotDispatcher.PropagateIdentityGrantDowngrade(this);
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// Applies the Mode-B identity-grant DOWNGRADE to this client: a
        /// granted state whose pending receipt was identity-dropped falls
        /// back to Unknown — fresh consent required. NOT a decision: no
        /// receipt is minted, no session markers move, and it never
        /// persists a denial — but the queue handling mirrors a denial's
        /// fail-closed clears (epoch advance + queue/restored/overflow
        /// drops + breadcrumb clear + own-spool wipe), so events accepted
        /// under the now-unbacked grant can never publish and their names
        /// cannot leak through a later crash report. Idempotent; a denied
        /// or unknown client is untouched. Reaches CLOSING clients exactly
        /// like the denial propagation does (a retryable-close window's
        /// remaining drain must not ship under the unbacked grant); only a
        /// fully-STOPPED worker is skipped. A client whose grant is a LIVE
        /// in-session explicit decision (its own SetConsent, or a live
        /// propagated/caught-up grant — decision generation above zero) is
        /// NEVER downgraded: the downgrade invalidates the LOADED grant the
        /// dropped receipt documented, not decisions made after it.
        /// Returns whether the downgrade APPLIED (false when it stood
        /// down), so callers can log/act accurately.
        /// </summary>
        internal bool ApplyIdentityGrantDowngrade()
        {
            var cleared = 0;
            lock (_gate)
            {
                if (_consentDecisionGeneration > 0 ||
                    _consentState != ShardPilotConsentState.Granted)
                {
                    return false;
                }

                ApplyConsentState(ShardPilotConsentState.Unknown);

                // The epoch advance condemns batches the worker drained
                // under the unbacked grant, exactly as a denial's does.
                AdvanceConsentDenialEpochLocked();
                cleared = _queue.Count + _restoredPending.Count + _spooledOverflowIds.Count;
                _queue.Clear();
                _restoredPending.Clear();
                _spooledOverflowIds.Clear();
                _spooledOverflowOrder.Clear();

                // Same leak the denial clears: breadcrumbs recorded under
                // the unbacked grant must not ride a later crash report.
                _breadcrumbs.Clear();

                // A pending withheld-grant completion belonged to the
                // downgraded grant lineage: a late settle must not resurrect
                // it (the completion's own state re-check would stand down
                // anyway — this keeps the bookkeeping honest).
                _withheldGrantStateKey = null;
                _withheldGrantStatePersistOwed = false;
            }

            if (cleared > 0)
            {
                _stats.RecordDropped(cleared);
            }

            if (_experiments != null)
            {
                // The downgrade purged queued-but-unpublished analytics
                // facts exactly like a denial: the experiments purge hook
                // re-arms undelivered exposures so a later real grant can
                // report them (every purge entry point invokes it).
                _experiments.OnAnalyticsPurge();
            }

            // Consent-first non-granted rule: the spool's events were
            // accepted under the unbacked grant and must not survive to
            // ride a later one.
            if (_spool != null)
            {
                _spool.Wipe();
            }

            WakeWorker();
            return true;
        }

        /// <summary>
        /// Retries an owed withheld-grant state persist (a settle-time
        /// completion whose own durable write failed). Cheap no-op normally;
        /// runs at the consent dispatch points alongside the owed outbox
        /// write retry.
        /// </summary>
        private void RetryOwedWithheldGrantStateIfAny()
        {
            bool owed;
            lock (_gate)
            {
                owed = _withheldGrantStatePersistOwed;
            }

            if (owed)
            {
                CompleteWithheldGrantStatePersist();
            }
        }

        /// <summary>
        /// Persists the consent decision record; returns false (surfaced by
        /// the caller as <c>consent_persist_failed</c>) when the durable
        /// write failed — the in-memory decision applies regardless.
        /// </summary>
        private bool PersistConsentState(ShardPilotConsentState state)
        {
            // Pairless flavor — the legacy record shape. Kept for the
            // convergence of LEGACY witnesses (a legacy marker/receipt has
            // no pair to write) and for callers with no decision pair in
            // scope; explicit decisions persist through the pair overload.
            return PersistConsentState(state, null, 0);
        }

        private bool PersistConsentState(ShardPilotConsentState state, string decidedAt, long decisionSeq)
        {
            if (_settingsStore == null)
            {
                return true;
            }

#if SHARDPILOT_SDK_TEST_SUPPORT
            if (_quiescedForTesting)
            {
                // The test that built this client is over. Every durable
                // consent write from here is refused BEFORE it reaches the
                // store, because a refused one is exactly what arms the
                // process-wide pending denial a later test would then read.
                return false;
            }
#endif

            try
            {
                if (ShardPilotConsent.IsPlausibleDecisionStamp(decidedAt))
                {
                    // The modern pair-carrying record: state + decision-time
                    // anonymous id + (decided_at, decision_seq).
                    ShardPilotConsent.Save(
                        _settingsStore, state, _config.AnonymousId, decidedAt, decisionSeq);
                }
                else
                {
                    // Pairless/legacy convergence keeps the legacy shape —
                    // a fabricated pair would outrank real stamped
                    // witnesses.
                    ShardPilotConsent.Save(_settingsStore, state);
                }

                return true;
            }
            catch (Exception ex)
            {
                SafeLog("shardpilot consent persist failed: {0}", ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Builds the durable outbox entry for an explicit decision, or null
        /// when no receipt can ever be posted (no HTTP sender, or no valid
        /// actor identifier to attribute the decision to — the
        /// acceptance gate applies here too: an identifier over 512 UTF-8
        /// bytes is rejected, never truncated).
        /// <paramref name="decidedAtUtc"/> is the ORIGINAL decision moment —
        /// receipts armed later (propagation, late registration) must not
        /// restamp it with their arming time.
        /// </summary>
        private ShardPilotConsentOutboxEntry CreateConsentReceiptEntry(
            bool analyticsGranted,
            DateTime decidedAtUtc,
            string reason,
            long decisionSeq = 0,
            ShardPilotConsentNoticeSnapshot notice = null)
        {
            if (decisionSeq == 0)
            {
                // Arm flavors without the deciding call's pair in scope
                // (propagated/catch-up/settle receipts) mint fresh: the
                // boot floor guarantees a fresh mint outranks every
                // restored seq, and the stamp stays the original decision
                // moment either way.
                decisionSeq = ShardPilotConsent.NextDecisionSeq();
            }

            if (_httpSender == null || _consentConfiguredActorInvalid)
            {
                return null;
            }

            // CANONICAL-ACTOR KEYING (the
            // defold-canonical receipt_actor rule, retiring the v0.9.1
            // user-first snapshot): the receipt keys to the configured
            // verified user ONLY when Mode B can vouch it — a TokenProvider
            // is present AND UserId is a valid identifier (the construction
            // clamp already cleared an invalid one). Everywhere else —
            // Mode A included, even with a UserId configured — the actor is
            // the ANONYMOUS id with kind "anon": a self-asserted identity
            // is never presented as verified, and a publishable-key write
            // is server-bound to the caller's anon scope anyway. This SDK
            // has no per-call user declaration (config-time identity only),
            // so the born-parked grant refusal
            // (consent_receipt_unvouchable) has NO reachable mint here: a
            // verified mint requires the provider + the configured user
            // (vouched by construction), and an anon mint happens only with
            // NO configured user (nothing to park it). Mirrors the
            // equivalent N/A notes in unreal/defold for their absent
            // surfaces.
            var verifiedActor = _config.UsesTokenProvider &&
                                ShardPilotConsentOutbox.IsValidIdentifier(_config.UserId);
            var actorIdentifier = verifiedActor ? _config.UserId : _config.AnonymousId;
            if (!ShardPilotConsentOutbox.IsValidIdentifier(actorIdentifier))
            {
                return null;
            }

            return new ShardPilotConsentOutboxEntry(notice)
            {
                IdempotencyKey = ShardPilotIds.NewUuidV7(),
                WorkspaceId = _config.WorkspaceKey,
                AppId = _config.AppKey,
                EnvironmentId = _config.EnvironmentKey,
                ActorIdentifier = actorIdentifier,
                Kind = verifiedActor
                    ? ShardPilotConsentOutbox.KindUserVerified
                    : ShardPilotConsentOutbox.KindAnon,
                AnalyticsGranted = analyticsGranted,
                DecidedAt = ShardPilotJson.FormatTimestamp(decidedAtUtc),
                Reason = reason,

                // Decision-time anonymous-id snapshot, retention-only (the
                // Mode-B identity-changed check at the next load); NEVER on
                // the wire.
                AnonymousId = ShardPilotConsentOutbox.IsValidIdentifier(_config.AnonymousId)
                    ? _config.AnonymousId
                    : null,
                DecisionSeq = decisionSeq
            };
        }

        /// <summary>
        /// The VOUCHED-PARKING predicate: TRUE for a retained
        /// receipt the current session cannot vouch a dispatch credential
        /// for. A VERIFIED-keyed receipt is dispatchable only while a
        /// Mode-B provider exists AND its actor IS the configured user —
        /// otherwise it parks (retained, persisted, counted toward the cap
        /// and pending-ness, skipped by dispatch and by the grant-receipt gate; never
        /// dropped — it may be the only record of that actor's consent
        /// change, worst of all an undelivered withdrawal, and dispatching
        /// it under another identity's JWT would take a terminal 403
        /// actor_mismatch). An ANON-keyed receipt parks only in the
        /// Mode-B-ONLY + configured-user session: the mint binds the
        /// verified subject and no publishable key exists to carry the anon
        /// actor — a later no-user launch delivers it. The comparison uses
        /// the host-configured identity (the same UserId that keyed the
        /// receipt), never JWT introspection; the server's actor_mismatch
        /// remains the backstop arbiter. Vouching inputs are per-client
        /// construction-time constants, so parked-ness is stable for the
        /// client's lifetime.
        /// </summary>
        private bool IsConsentReceiptParked(ShardPilotConsentOutboxEntry entry)
        {
            if (!_consentOutbox.OwnScope.Matches(entry))
            {
                return true;
            }

            if (string.Equals(entry.Kind, ShardPilotConsentOutbox.KindUserVerified, StringComparison.Ordinal))
            {
                return !_config.UsesTokenProvider ||
                       !string.Equals(entry.ActorIdentifier, _config.UserId, StringComparison.Ordinal);
            }

            if (!string.Equals(entry.Kind, ShardPilotConsentOutbox.KindAnon, StringComparison.Ordinal))
            {
                // Neither anon nor verified: a future/foreign kind the
                // sanitizer retained INERT. No credential rule of this
                // build vouches it, so it parks unconditionally — retained,
                // cap-counted, skipped by dispatch and the grant-receipt gate — and a
                // pending GRANT of this shape routes through the
                // parked-pending-grant fail-closed downgrade.
                return true;
            }

            return _config.UsesTokenProvider &&
                   string.IsNullOrEmpty(_config.PublishableKey) &&
                   (!string.IsNullOrEmpty(_config.UserId) ||
                    !string.Equals(entry.ActorIdentifier, _config.AnonymousId, StringComparison.Ordinal));
        }

        /// <summary>
        /// MOST-VOUCHING per-receipt dispatch credential: TRUE when the receipt dispatches under the
        /// static publishable PublishableKey, FALSE when it rides the Mode-B JWT.
        /// Anon-keyed receipts ride the publishable key where one is
        /// configured — the key has no identity binding, so the historic
        /// actor is the correct subject — EXCEPT a CURRENT-anon GRANT in a
        /// no-configured-user Mode-B session, which rides the JWT (a
        /// publishable-key grant is denials-only server-side, and with no
        /// declared user the mint binds the current anon, so the token
        /// vouches for it). Verified-keyed receipts ride the JWT ONLY —
        /// never a publishable fallback, which would misrecord the decision
        /// under the caller's anon scope (parking guarantees the provider
        /// exists by dispatch time). 401 handling follows the CREDENTIAL
        /// used, never the configured mode — see the settle.
        /// </summary>
        private bool ConsentReceiptUsesPublishableKey(ShardPilotConsentOutboxEntry entry)
        {
            if (!string.Equals(entry.Kind, ShardPilotConsentOutbox.KindAnon, StringComparison.Ordinal))
            {
                // Verified rides the JWT only; an unknown (retained-inert)
                // kind is parked and never reaches dispatch — the FALSE here
                // is defense in depth, not a live credential choice.
                return false;
            }

            if (string.IsNullOrEmpty(_config.PublishableKey))
            {
                return false;
            }

            var currentAnonGrantMostVouchedByToken =
                _config.UsesTokenProvider &&
                entry.AnalyticsGranted &&
                string.IsNullOrEmpty(_config.UserId) &&
                string.Equals(entry.ActorIdentifier, _config.AnonymousId, StringComparison.Ordinal);
            return !currentAnonGrantMostVouchedByToken;
        }

        /// <summary>
        /// Rewrites the durable outbox record from the mirror and does the
        /// client-side accounting: cap evictions are counted and diagnosed
        /// (<c>outbox_overflow</c>), a failed write counts
        /// <c>consent_outbox_persist_failed</c> and leaves the write owed
        /// (dirty) — retried at every consent dispatch point, in the
        /// pause/quit persist, and at shutdown.
        /// </summary>
        private bool PersistConsentOutbox()
        {
            if (!_consentOutbox.HasDurableBackend)
            {
                return false;
            }

            if (_consentOutbox.IsDisowned)
            {
                // A later same-scope construction owns the record now; this
                // client's writes are refused by contract (see the outbox's
                // ownership note) — silently: nothing is owed by a
                // non-owner, so no failure accounting either.
                return false;
            }

            bool dischargeOwed;
            lock (_gate)
            {
                // Every save carries the CURRENT obligation, so the mirror can
                // never disagree with the flags it is backing up.
                SyncOwedDenialWitnessLocked();
                dischargeOwed = _withheldMarkerDischargeOwed;
            }

            if (dischargeOwed)
            {
                // A condemned marker's discharge is still OWED: retry it
                // first, and while it keeps failing REFUSE the record
                // rewrite — a rewrite without the condemned marker's
                // receipt would mint the clean-empty half of the false
                // [undecided + clean-empty + durable] proof, and marker and
                // record are file-routed to the same backend, so a store
                // failing the marker clear is failing this write anyway.
                // The write stays owed (dirty) exactly like any failed
                // persist and retries at every dispatch point.
                RetryOwedWithheldMarkerDischargeIfAny();
                lock (_gate)
                {
                    dischargeOwed = _withheldMarkerDischargeOwed;
                }

                if (dischargeOwed)
                {
                    SafeLog(
                        "shardpilot consent outbox rewrite refused: a withheld-marker discharge " +
                        "is still owed; the record write stays owed and retries with it");
                    return false;
                }
            }

            int evicted;
            var persisted = _consentOutbox.TryPersist(out evicted);
            if (evicted > 0)
            {
                _stats.RecordConsentOutboxEvicted(evicted);
                SafeLog(
                    "shardpilot consent outbox overflow (scope=consent, code=outbox_overflow): " +
                    "{0} oldest receipt(s) evicted at the {1}-entry cap",
                    evicted,
                    ShardPilotConsentOutbox.MaxEntries);
            }

            if (!persisted)
            {
                _stats.RecordConsentOutboxPersistFailed();
                SafeLog("shardpilot consent outbox persist failed; the write stays owed and retries");
            }

            return persisted;
        }

        /// <summary>
        /// Retries an owed outbox write. Runs at every consent dispatch
        /// point, in <see cref="PersistPendingEvents"/> (even with the event
        /// spool disabled — the outbox is independent of event spooling),
        /// and at shutdown.
        /// </summary>
        private void RetryOwedConsentWriteIfDirty()
        {
            // An OWED marker discharge retries FIRST — before the record
            // write below may rewrite the record without the condemned
            // marker's receipt (the clean-empty half of the false proof
            // must never land while the marker half is still standing; the
            // persist itself also refuses while the discharge stays owed).
            RetryOwedWithheldMarkerDischargeIfAny();

            if (_consentOutbox.HasDurableBackend && !_consentOutbox.IsDisowned && _consentOutbox.Dirty)
            {
                PersistConsentOutbox();
            }

            // The boot denial belt deferred behind an unreadable record
            // re-runs the moment the heal above lands — BEFORE the
            // deferred withheld-marker resolution below (the construction
            // order: belt first, then the withheld matrix judges against
            // the belt's imposed state through the pair-aware discharge
            // gates) and before the head dispatch this same call precedes,
            // so a recovered denial receipt cannot deliver and prune with
            // the stale granted state still live.
            RunBootDenialBeltAfterOutboxRecovery();

            // A withheld-marker resolution deferred behind an unreadable
            // record re-runs the moment the heal above lands — BEFORE the
            // head dispatch this same call precedes, so the recovered
            // marked receipt cannot dispatch (and prune on ack) with the
            // obligation still unarmed.
            ResolveWithheldGrantMarkerAfterOutboxRecovery();

            // The withheld obligation's durable marker rides the same
            // cadence: an owed marker write re-attempts, and the marker's
            // receipt-durability phase upgrades once the receipt has landed
            // in the record (cheap no-op without an armed obligation).
            MaybeSyncWithheldGrantMarker();
        }

        /// <summary>
        /// Consent dispatch point: retries the owed durable
        /// write, then — when a head receipt is retained, nothing is in
        /// flight, and no deferral window is open — resolves a Bearer and
        /// hands the head to the HTTP sender. Cheap no-op when the outbox is
        /// empty; a fresh consent-first install therefore stays fully dark
        /// (no token is ever minted for an empty outbox). Called at client
        /// construction, on every explicit decision, on every worker cycle
        /// and publish, at shutdown's final drain, and by the ack-chain — so
        /// one healthy dispatch point drains the whole trail.
        /// </summary>
        /// <param name="workerCadence">
        /// TRUE only for the worker loop's periodic tick. The token-retry pace
        /// applies to THAT and nothing else: a caller who flushes is asking for
        /// progress now and may know the token is back, so pacing them would
        /// make a background cadence refuse an explicit Flush().
        /// Passed explicitly, with no default, so a
        /// dispatch point added later has to say which it is.
        /// </param>
        private void MaybeKickConsentOutbox(bool workerCadence)
        {
            if (_httpSender == null)
            {
                return;
            }

            // A settle-time withheld-grant completion whose own persist
            // failed retries here, alongside the owed outbox write below.
            RetryOwedWithheldGrantStateIfAny();

            // An owed Mode-B identity-downgrade clear retries here too, and
            // a healing re-read that dropped a pending grant the
            // construction could not see triggers the same downgrade.
            RetryOwedIdentityDowngradeIfAny();
            MaybeApplyRecoveryIdentityDowngrade();

            // The standing denial's owed witness writes (record → marker)
            // retry at every consent dispatch point too: the close paths
            // refuse to finalize while the denial is unwitnessed, and this
            // is the stay-alive worker's convergence cadence. A cheap
            // no-op unless armed.
            RetryOwedDenialWitnessWrites();

            bool retryWrite;
            bool dispatchable;
            lock (_gate)
            {
                // A DEFERRED marker resolution or an OWED marker discharge
                // keeps this dispatch point live even when the outbox
                // itself reports nothing pending: a heal can leave the
                // record clean and EMPTY (nothing retained, no write owed)
                // with the resolution still un-run because its TryEnter
                // lost the decision gate on the healing pass — without this
                // the completion would strand until an UNRELATED path
                // (a decision, a refresh, a close) happened to fire.
                var markerWorkOwed = _withheldMarkerResolutionPending || _withheldMarkerDischargeOwed ||
                                     _bootBeltResolutionPending;
                if (_workerStopped || (!_consentOutbox.Pending && !markerWorkOwed))
                {
                    // Torn down (a late ack settles bookkeeping but chains
                    // nothing), or nothing retained, no write owed, and no
                    // marker work owed.
                    return;
                }

                retryWrite = _consentOutbox.Dirty || markerWorkOwed;

                // The dispatch head is the first NON-PARKED retained receipt:
                // an outbox holding only parked
                // receipts is never dispatch-due — parked receipts wait for
                // a credential context, not a worker pass.
                dispatchable = _consentInFlightKey == null &&
                               _consentOutbox.OwnScope.FirstDispatchableEntry(IsConsentReceiptParked) != null &&
                               DateTime.UtcNow >= _consentRetryNotBefore &&
                               (!workerCadence || DateTime.UtcNow >= _consentTokenRetryWakeAt);
            }

            if (retryWrite)
            {
                RetryOwedConsentWriteIfDirty();
            }

            if (!dispatchable)
            {
                return;
            }

            long authEpochAtResolve;
            bool resolveForPublishableKey;
            lock (_gate)
            {
                authEpochAtResolve = _consentAuthEpoch;

                // MOST-VOUCHING per-receipt credential: the head's recorded
                // kind (and the grant/anon most-vouching exception) selects
                // WHICH credential this dispatch resolves — the claim below
                // re-judges the selection against the then-current head and
                // stands down on a mismatch, so a head change between this
                // peek and the claim (a settle pruning the old head, a cap
                // eviction) can never pair a receipt with the other class's
                // credential.
                var peekedHead = _consentOutbox.OwnScope.FirstDispatchableEntry(IsConsentReceiptParked);
                if (peekedHead == null)
                {
                    return;
                }

                resolveForPublishableKey = ConsentReceiptUsesPublishableKey(peekedHead);
            }

            ResolveBearerForConsent(resolveForPublishableKey, bearer =>
            {
                if (string.IsNullOrEmpty(bearer))
                {
                    // No usable bearer (Mode-B token unavailable): the receipt
                    // stays retained and delivers at a later dispatch point
                    // WITHOUT another SetConsent call.
                    //
                    // THAT LATER DISPATCH POINT IS NOW ONE THIS CLIENT CREATES.
                    // It used to be whatever happened to come along — and with a
                    // long FlushInterval the answer was "in an hour", because
                    // nothing here armed a retry. The cost is not one receipt: the
                    // receipts-first grant-receipt gate refuses event batches while a receipt
                    // is undispatched, so a transient token failure withheld ALL
                    // analytics until an unrelated event woke the worker.
                    //
                    // It is armed through the SAME ladder a retryable server
                    // response uses, deliberately rather than with a fresh timer:
                    // attempt 1 is the free retry at the next dispatch point (the
                    // wake below makes that point exist), and attempt n>=2 backs
                    // off, so a token backend that stays down cannot be hammered.
                    // A successful dispatch resets the streak like any other.
                    _stats.RecordConsentFailed("token_unavailable");
                    SafeLog("shardpilot consent post deferred: no auth token available; receipt retained");
                    lock (_gate)
                    {
                        var wait = NextConsentBackoffWaitLocked();
                        if (wait > MaxConsentDeferral)
                        {
                            wait = MaxConsentDeferral;
                        }

                        _consentTokenRetryWakeAt = DateTime.UtcNow + wait;
                    }

                    WakeWorker();
                    return;
                }

                // A bearer exists, so any token-backend deferral this client is
                // still carrying is obsolete — clear it rather than make the
                // worker sit out a wake armed for a failure that has already
                // passed. A caller-driven dispatch is
                // exactly how that happens: it bypasses the pace by design, and
                // if its request then comes back retryable (a Mode-B 401 is
                // never deferred) the re-mint must not queue behind the stale
                // deadline. The backoff STREAK is deliberately not reset here;
                // only a successful dispatch earns that.
                lock (_gate)
                {
                    _consentTokenRetryWakeAt = DateTime.MinValue;
                }

                DispatchConsentOutboxHead(bearer, authEpochAtResolve, resolveForPublishableKey);
            });
        }

        /// <summary>
        /// Atomically [claim the head -> hand its receipt to the sender]
        /// under the consent dispatch gate, so "claimed but not yet handed
        /// off" is unobservable: a batch publish that runs concurrently
        /// waits this handoff out (a local enqueue wait, never a network
        /// wait) before reading the grant-receipt gate predicate. At most ONE receipt
        /// is in flight; delivery is strictly serial in decision order.
        /// Mode-agnostic (defold-canonical D2): a Mode-A grant dispatches
        /// under the publishable sp_ key and takes the server's terminal 403
        /// (denials-only for publishable keys) as a terminal drop.
        /// </summary>
        private void DispatchConsentOutboxHead(string bearer, long authEpochAtResolve, bool bearerIsPublishableKey)
        {
            if (_httpSender == null || string.IsNullOrEmpty(bearer))
            {
                return;
            }

            lock (_consentDispatchGate)
            {
                // The owed-write retry rides every dispatch attempt.
                RetryOwedConsentWriteIfDirty();

                ShardPilotConsentOutboxEntry head;
                lock (_gate)
                {
                    if (_workerStopped ||
                        _consentOutbox.IsDisowned ||
                        _consentOutbox.LoadFailed ||
                        _withheldMarkerResolutionPending ||
                        _bootBeltResolutionPending ||
                        _consentInFlightKey != null ||
                        DateTime.UtcNow < _consentRetryNotBefore)
                    {
                        // Torn down, or DISPLACED by a same-scope successor:
                        // a displaced client's mirror is stale — its head may
                        // predate decisions the successor has since appended
                        // (a retained grant after the successor's denial), so
                        // handing it off would violate the trail's decision
                        // order. The displaced drain relies on the grant-receipt gate
                        // delegation instead and dispatches nothing itself,
                        // exactly like the kick and persistence paths.
                        // LoadFailed stands down the same way: while the
                        // record is unreadable, a NEW decision's mirror head
                        // could overtake receipts trapped in it — serial
                        // FIFO is per-scope DECISION order, and the trapped
                        // entries are older by construction. The owed-write
                        // retry above keeps attempting the healing re-read;
                        // dispatch resumes with the merged record-first
                        // order once it lands. A PENDING withheld-marker
                        // resolution stands down too: the recovered marked
                        // receipt must not dispatch (its ack would prune
                        // WITHOUT completing the grant persist) before the
                        // resolution re-arms the obligation — the retry
                        // above just ran it, so this holds only while the
                        // heal is still owed or the decision gate was busy,
                        // and the next dispatch point proceeds. A PENDING
                        // boot-belt re-run holds the head back the same
                        // way: a recovered denial receipt must not deliver
                        // (and prune) before the belt judges whether it
                        // outranks the stale granted restore.
                        return;
                    }

                    if (authEpochAtResolve != _consentAuthEpoch)
                    {
                        // This bearer was resolved BEFORE a consent 401
                        // invalidated the cached token: dispatching it would
                        // re-send the very credential the server just
                        // rejected. Stand down — a later dispatch point
                        // re-mints fresh.
                        return;
                    }

                    // The serial trail head is the first DISPATCHABLE
                    // (non-parked) receipt — parked verified-keyed entries
                    // are skipped, preserving per-actor decision order.
                    head = _consentOutbox.OwnScope.FirstDispatchableEntry(IsConsentReceiptParked);
                    if (head == null)
                    {
                        return;
                    }

                    // CREDENTIAL-CLASS RECHECK at the claim, using this SDK's
                    // immutable vouching inputs: the bearer
                    // was resolved for the head PEEKED at the dispatch
                    // point, and the head can have CHANGED since (a settle
                    // pruned it, a cap eviction displaced it). Pairing the
                    // new head with the other class's credential would
                    // either 403 a verified receipt under the publishable
                    // key's anon binding or misroute an anon receipt onto a
                    // JWT bound to another subject — both terminal drops of
                    // a deliverable receipt. Stand down instead; the next
                    // dispatch point re-resolves for the current head.
                    if (ConsentReceiptUsesPublishableKey(head) != bearerIsPublishableKey)
                    {
                        return;
                    }

                    _consentInFlightKey = head.IdempotencyKey;

                    // Write-through for the grant-receipt gate's cross-client
                    // delegation: a DISPLACED client's drain consults the
                    // owner outbox's gate, which needs the owner's in-flight
                    // head to release only itself.
                    _consentOutbox.InFlightKeyForGate = head.IdempotencyKey;
                }

                SendConsentReceipt(bearer, head, bearerIsPublishableKey);
            }
        }

        /// <summary>
        /// Hands one retained receipt to the HTTP sender. The batch publish
        /// never waits on the response — release of the grant-receipt gate is the
        /// HANDOFF, never the acknowledgment; the response callback below
        /// settles the outbox (ack -> prune + chain; retryable -> retained
        /// at the head with Retry-After/backoff pacing; terminal -> dropped
        /// + chain).
        /// </summary>
        private void SendConsentReceipt(string bearer, ShardPilotConsentOutboxEntry entry, bool usedPublishableKey)
        {
            try
            {
                var request = new ShardPilotHttpRequest
                {
                    Method = "POST",
                    Url = _consentEndpoint,
                    Bearer = bearer,
                    ContentType = "application/json",
                    Body = entry.BuildWireBody(_config.ConsentKindEmissionEnabled),
                    Timeout = _config.HttpTimeout
                };

                _httpSender.Send(request, response => OnConsentReceiptResponse(entry, response, usedPublishableKey, bearer));
            }
            catch (Exception ex)
            {
                // The sender enqueue itself failed: a local, retryable
                // condition — the receipt stays retained at the head.
                lock (_gate)
                {
                    ClearConsentInFlightLocked(entry);
                }

                _stats.RecordConsentFailed(ShardPilotDiagnostics.Redact(_config, "send_failed", bearer));
                SafeLogRequest(bearer, "shardpilot consent post failed: {0}", ex.Message);
            }
        }

        /// <summary>Caller must hold <c>_gate</c>.</summary>
        private void ClearConsentInFlightLocked(ShardPilotConsentOutboxEntry entry)
        {
            if (string.Equals(_consentInFlightKey, entry.IdempotencyKey, StringComparison.Ordinal))
            {
                _consentInFlightKey = null;
                _consentOutbox.InFlightKeyForGate = null;
            }
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. The belt predicate a retained
        /// receipt must meet to witness the CURRENT standing denial at the
        /// next boot: denial-carrying, scoped to this actor by its
        /// decision-time anon snapshot, plausibly stamped, and at-or-after
        /// the standing denial's pair (the pending snapshot armed with the
        /// witness flags). An IN-FLIGHT receipt is excluded even when it
        /// matches: its durability is about to be CONSUMED by its own
        /// settle — the settle prune removes it from the durable outbox,
        /// possibly after a close that finalized on its evidence, leaving
        /// the stale pre-denial record as the only thing the next launch
        /// reads. The settle-side witness-gap guard covers the receipt
        /// once it settles; until then it proves nothing here.
        /// </summary>
        private bool ConsentReceiptQualifiesAsDenialWitnessLocked(ShardPilotConsentOutboxEntry retained)
        {
            if (_consentInFlightKey != null && _consentOutbox.OwnScope.Matches(retained) &&
                string.Equals(retained.IdempotencyKey, _consentInFlightKey, StringComparison.Ordinal))
            {
                return false;
            }

            return ConsentReceiptPairQualifiesAsDenialWitnessLocked(retained);
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. The qualification WITHOUT the
        /// in-flight exclusion: the settle guard judges its own SETTLING
        /// receipt with this — that receipt's in-flight claim refers to
        /// the very settle running the judgment, not to a consumption
        /// pending elsewhere, so excluding it there would bypass the guard
        /// for exactly the receipt it exists to retain.
        /// </summary>
        private bool ConsentReceiptPairQualifiesAsDenialWitnessLocked(ShardPilotConsentOutboxEntry retained)
        {
            if (retained.AnalyticsGranted ||
                !string.Equals(retained.AnonymousId, _config.AnonymousId, StringComparison.Ordinal) ||
                !ShardPilotConsent.IsPlausibleDecisionStamp(retained.DecidedAt))
            {
                return false;
            }

            if (string.IsNullOrEmpty(_consentDenialPendingDecidedAt))
            {
                return true;
            }

            // Against a SEQ-CARRYING pending pair, a pairless receipt
            // (plausible stamp, no minted seq) is a legacy half-pair that
            // cannot prove order — the same contract the boot belt applies
            // against stamped records: a close
            // finalizing on it would rest on evidence the next launch's
            // belt ignores. A pending pair the belt armed from a LEGACY
            // record shape (seq 0) stays stamp-ordered — its own arming
            // receipt must keep witnessing it, exactly as the belt's
            // legacy arm honors it at the next boot.
            if (_consentDenialPendingSeq > 0 && retained.DecisionSeq <= 0)
            {
                return false;
            }

            return ShardPilotConsent.DecisionPairNewer(
                       retained.DecidedAt, retained.DecisionSeq,
                       _consentDenialPendingDecidedAt, _consentDenialPendingSeq) ||
                   (string.Equals(
                        retained.DecidedAt, _consentDenialPendingDecidedAt,
                        StringComparison.Ordinal) &&
                    retained.DecisionSeq == _consentDenialPendingSeq);
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. TRUE when a receipt qualifying as
        /// the standing denial's witness is DURABLY retained — judged on
        /// the mirror when it is clean (mirror = disk), and on the actual
        /// DISK contents when dirty (what a relaunch would read; the
        /// standing Mode-B identity drop applied). No durable backend
        /// proves nothing.
        /// </summary>
        private bool DenialReceiptRetainedDurablyLocked()
        {
            if (!_consentOutbox.HasDurableBackend)
            {
                return false;
            }

            if (!_consentOutbox.Dirty)
            {
                // Clean mirror = the durable record's exact contents.
                var retained = _consentOutbox.SnapshotEntries();
                for (var i = 0; i < retained.Count; i++)
                {
                    if (ConsentReceiptQualifiesAsDenialWitnessLocked(retained[i]))
                    {
                        return true;
                    }
                }

                return false;
            }

            return _consentOutbox.AnyDurableEntry(ConsentReceiptQualifiesAsDenialWitnessLocked);
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. WITNESS-GAP SETTLE GUARD:
        /// Before consuming a denial receipt on acknowledgment or terminal
        /// refusal, retry witness persistence if a durable backend exists
        /// and the standing denial's marker write remains owed. A landed
        /// record alone cannot release the receipt: another actor can replace
        /// that shared record. If healing does not discharge the marker debt,
        /// retain a still-present receipt whose decision pair qualifies as
        /// this denial's witness. It stays retained without re-dispatch after
        /// settlement; dispatch-point pruning releases it once the marker
        /// debt clears or the decision no longer qualifies. Returns TRUE
        /// when the settle must not consume the receipt.
        /// </summary>
        /// <summary>
        /// Caller must hold <c>_gate</c>. The condition under which a denial
        /// receipt can still be the standing denial's ONLY durable witness:
        /// the MARKER write owed, on a live denial.
        ///
        /// The marker alone decides it because it is the only lane that cannot
        /// be taken away. A landed RECORD witnesses
        /// the denial right now, but the record is one shared lane and any
        /// actor's newer decision supersedes it — releasing the receipt on the
        /// record's strength leaves a denial whose marker is still refused with
        /// nothing at all behind it the moment a foreign grant lands. A landed
        /// MARKER survives exactly that, so it frees the receipt.
        ///
        /// Named because two places have to agree on it: the retention guard
        /// below, and the release of the deferral that retention takes. They
        /// were written separately once, and the narrower of the two left a
        /// PARTIAL heal — marker landed, record still failing — paced for a
        /// whole FlushInterval with no receipt left to retain. Keying on the
        /// marker keeps the earlier answer: a landed marker still releases
        /// immediately, with the record write owed or not.
        /// </summary>
        /// <summary>
        /// Caller must hold <c>_gate</c>. THE ONE QUESTION every decision that
        /// would OPEN egress has to ask: is this actor's own denial evidence
        /// currently unjudgeable? Either lane can make it so — an unreadable or
        /// mangled marker SLOT, or a refused or corrupt owed BACKUP — and both
        /// mean the same thing here: a denial may exist that this launch cannot
        /// see, so nothing may re-open on a grant until one of them is judgeable
        /// again or this actor makes a fresh decision.
        ///
        /// Registration refresh, boot and post-recovery checks, propagated and
        /// direct grants, and withheld-grant persistence use this shared check.
        /// ShardPilotConsentEvidenceHoldTests verifies the call sites and
        /// prevents direct reads of the two evidence flags.
        /// </summary>
        /// <summary>
        /// Arms the BACKUP half of the hold, and demotes a granted state with
        /// it. Arming alone only refuses FUTURE opens; a grant already applied
        /// stays applied, and admission reads the state. Unknown rather than a denied flavor, for the same reason the
        /// slot's own hold chooses it: unjudgeable evidence must not trigger the
        /// destructive denial wipes, and Unknown closes egress identically.
        /// </summary>
        private bool ArmBackupEvidenceHoldLocked()
        {
            _owedBackupUnjudgeable = true;

            // The CRASH gate closes with it, symmetrically with the lift that
            // opens it: crash admission reads its own setting, so arming only
            // the analytics half left crash bytes leaving the device over a
            // backup this launch could not read.
            _consentReadFailureCrashHold = true;
            if (_consentState != ShardPilotConsentState.Granted)
            {
                return false;
            }

            ApplyConsentState(ShardPilotConsentState.Unknown);

            // Reported so the caller can tell whatever it computed from the
            // PRE-demotion state — the boot belt's granted-restore guard is one.
            return true;
        }

        /// <summary>
        /// The mirror of <see cref="ArmBackupEvidenceHoldLocked"/>: a key this
        /// client just READ is judgeable, so the hold its refusal armed is over.
        /// The demoted state is NOT restored — only a decision does that — but
        /// nothing stays refused on evidence that has since become readable.
        /// </summary>
        private void LiftBackupEvidenceHoldLocked()
        {
            if (!_owedBackupUnjudgeable)
            {
                return;
            }

            _owedBackupUnjudgeable = false;

            // The CRASH gate opens on the same proof as the grant below, not
            // before it: crash admission reads its own
            // setting, so clearing it against a record that is unreadable — or
            // that another process replaced with a denial — reopened crash
            // egress on evidence nobody had checked.
            //
            // And the grant the hold demoted is restored, but only on proof:
            // nothing else holds, nothing is owed, and the durable record still
            // reads GRANTED for this actor. Without this the client sat Unknown
            // for the rest of the session over a read failure that healed
            // seconds later. A denial in the record,
            // an unreadable record, or an owed obligation all leave it closed.
            if (_consentState != ShardPilotConsentState.Unknown ||
                _denialMarkerSlotUnjudgeable ||
                _consentDenialRecordPending || _consentDenialMarkerPending)
            {
                return;
            }

            bool recordUnreadable;
            var record = ShardPilotConsent.LoadRecord(_settingsStore, out recordUnreadable);

            // "GRANTED FOR THIS ACTOR" — and the actor is now CHECKED, not just
            // claimed by the comment above it. The record is GLOBAL to the
            // settings store while the grant being restored is this client's, so
            // an unscoped read restored a grant a DIFFERENT actor gave: an earlier fix
            // found it reopening crash egress for an actor whose own denial was
            // sitting in the marker slot at that moment. A legacy record carries
            // no decision-time actor and can prove nothing, so it cannot restore.
            if (!recordUnreadable && record.State == ShardPilotConsentState.Granted &&
                !record.Legacy && record.AnonymousId != null &&
                ShardPilotConsentOutbox.IsValidIdentifier(record.AnonymousId) &&
                string.Equals(
                    record.AnonymousId, _config.AnonymousId, StringComparison.Ordinal))
            {
                ApplyConsentState(ShardPilotConsentState.Granted);
                _consentReadFailureCrashHold = false;
            }
        }

        /// <summary>
        /// What this launch knows about the actor's backup key. Reading and
        /// judging the key must produce a verdict before any action can lift
        /// or arm its hold. Each verdict keeps the candidate and its required
        /// action together.
        /// </summary>
        private enum BackupEvidence
        {
            /// <summary>Refused or corrupt: nothing may be concluded from it.</summary>
            Unreadable,

            /// <summary>Read, and nothing is stored.</summary>
            Absent,

            /// <summary>
            /// Read, and it still carries an obligation nobody discharged: the
            /// denial STANDS and no other witness carries it. Egress SHUTS —
            /// leaving it merely judged is what let a launch go on publishing
            /// under a denial already durable on disk. The denial's destructive
            /// half belongs to the next launch's boot imposition; this key is a
            /// restart mechanism, not a live propagation channel.
            /// </summary>
            Live,

            /// <summary>
            /// Read, and its pair REACHED THE MARKER SLOT. The witness changed
            /// lanes; the denial itself is untouched and still stands. Split
            /// out of <see cref="Superseded"/> because sharing one
            /// verdict made them share one ACTION, and lifting on a discharge
            /// restored a grant against a denial the slot was witnessing. Shuts
            /// egress exactly like <see cref="Live"/>.
            /// </summary>
            Discharged,

            /// <summary>
            /// Read, and a NEWER decision by the SAME actor replaced it: the
            /// denial is obsolete. The ONE readable verdict that may lift.
            /// </summary>
            Superseded,
        }

        /// <summary>
        /// Caller must hold <c>_gate</c> AND the decision gate. READS the key
        /// and JUDGES it in one step — the two are never separable, which is
        /// the whole point.
        /// </summary>
        private BackupEvidence JudgeBackupEvidenceLocked(out ShardPilotDenialMarker stored)
        {
            bool unreadable;
            stored = ShardPilotConsent.LoadOwedDenialWitness(
                _settingsStore, _config.AnonymousId, out unreadable);
            if (unreadable)
            {
                return BackupEvidence.Unreadable;
            }

            if (stored == null)
            {
                return BackupEvidence.Absent;
            }

            // SUPERSESSION IS ASKED FIRST, and the order is load-bearing: both
            // can hold at once (the pair reached the slot, and the actor then
            // decided again), and then the denial is OBSOLETE — answering
            // "discharged" would impose a denial its own actor already replaced.
            // Discharge only ever means "some other witness carries it now".
            if (OwedBackupIsSupersededLocked(stored))
            {
                return BackupEvidence.Superseded;
            }

            return OwedBackupReachedTheMarkerSlotLocked(stored)
                ? BackupEvidence.Discharged
                : BackupEvidence.Live;
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. THE ONLY caller of the arm and lift
        /// pair AND the only place a recovered denial becomes owed — all three
        /// pinned by ShardPilotConsentEvidenceHoldTests, so a future reader
        /// cannot arm, lift or impose without a verdict in hand. Returns whether
        /// arming demoted a grant, which the boot belt needs to know.
        ///
        /// <paramref name="stored"/> is the evidence the verdict was read FROM,
        /// passed alongside it rather than re-read here: a verdict and the pair
        /// it was formed from are one fact, and re-reading would let the two
        /// disagree. It is null exactly when nothing could be read — which is
        /// what <see cref="BackupEvidence.Unreadable"/> means.
        /// </summary>
        private bool ApplyBackupEvidenceLocked(
            BackupEvidence evidence, ShardPilotDenialMarker stored, bool freshDecision)
        {
            if (freshDecision)
            {
                // A FRESH decision by THIS actor is authoritative over this
                // actor's OWN key, and everything the key could say is older than
                // it. So no verdict read from it may act: an unreadable key must
                // not re-arm the hold the decision just cleared, and a recovered denial must not be imposed over the
                // decision that replaced it — a grant landing on a slot that
                // still held its own superseded denial pair reads DISCHARGED, and
                // imposing that would undo the grant the player just gave.
                //
                // The caller used to make the first of these judgments itself,
                // for Unreadable alone. Both belong here, with the verdict.
                return false;
            }

            switch (evidence)
            {
                case BackupEvidence.Unreadable:
                    return ArmBackupEvidenceHoldLocked();

                case BackupEvidence.Absent:
                case BackupEvidence.Superseded:
                    // NOTHING IS OWED. Absent: there was never an obligation.
                    // Superseded: its own actor replaced it. These are the only
                    // two verdicts that may open anything back up.
                    LiftBackupEvidenceHoldLocked();
                    return false;

                default:
                    // Live and discharged evidence closes this actor's egress.
                    // Spool wipes and epoch changes belong to boot recovery,
                    // which reads the same key and applies the stored denial.
                    // This key provides restart recovery, not live cross-process
                    // propagation.
                    //
                    // Unknown closes egress identically to Denied — the same
                    // reason it was chosen for unreadable evidence — so the
                    // denial takes effect here at once, and takes hold at the next
                    // launch.
                    //
                    // Safe to demote a grant precisely because the verdict is
                    // FINAL: a newer decision of this actor's, withheld grant
                    // included, reads Superseded and never reaches this branch.
                    if (!stored.Legacy)
                    {
                        // The floor every recovered witness raises: a mint after
                        // this must outrank what is actually stored.
                        ShardPilotConsent.RaiseDecisionSeqFloor(stored.DecisionSeq);
                    }

                    if (ShardPilotConsent.IsDeniedState(_consentState))
                    {
                        // ALREADY DENIED: the client's state already reflects the
                        // key, which is the whole meaning of this hold, so it
                        // ends. Without this, a hold armed while the key was
                        // unreadable outlives the launch that armed it — once the
                        // key reads as this actor's own standing denial there is
                        // no verdict left that would ever produce Absent, and the
                        // scope goes on refusing a SIBLING's later grant for the
                        // actor.
                        LiftBackupEvidenceHoldLocked();
                        return false;
                    }

                    ArmBackupEvidenceHoldLocked();
                    return false;
            }
        }

        /// <summary>
        /// THE convergence a durably persisted GRANT owes, wherever that persist
        /// landed. Caller must NOT hold <c>_gate</c>.
        ///
        /// A grant reaches its durable record by three routes — the straight
        /// persist, the in-call retry after the receipt-first ordering withheld
        /// it, and the withheld-grant completion that runs asynchronously — and
        /// each of them owes the same two things. They were written one at a time
        /// and only the first ever grew the backup sync, so an unreadable or
        /// corrupt old backup outlived a grant that arrived by either of the other
        /// two and demoted the durable grant to Unknown at the next launch. Centralized rather than fixed twice: a FOURTH route
        /// to a durable grant now inherits both by calling one method.
        ///
        /// Order: the sync FIRST, while the marker slot is still as this grant
        /// found it. It proves supersession from the RECORD — which carries the
        /// grant only once the persist above has landed — so it is
        /// indifferent to the retire, while running it second would judge a slot
        /// this method had already emptied.
        /// </summary>
        private void ConvergeDurableGrantPersist()
        {
            lock (_gate)
            {
                SyncOwedDenialWitnessLocked(freshDecision: true);
            }

            // A newer successfully-persisted decision supersedes any stale
            // write-ahead denial marker an earlier failed retirement left behind:
            // retire it, or the next boot would weigh an outdated denial against
            // this fresh grant record. (Best-effort — the stale-guard pair-compare
            // at construction is the belt for a failed clear here. Scoped: a valid
            // FOREIGN actor's marker is kept — this grant supersedes only the
            // current actor's decisions.)
            ShardPilotConsent.RetireDenialMarkerScoped(
                _settingsStore, _config.AnonymousId);
        }

        private bool ConsentEvidenceUnjudgeableLocked()
        {
            return _denialMarkerSlotUnjudgeable || _owedBackupUnjudgeable;
        }

        private bool DenialWitnessStillOwedLocked()
        {
            return _consentDenialMarkerPending &&
                   ShardPilotConsent.IsDeniedState(_consentState);
        }

        private bool DenialWitnessBlocksConsumptionLocked(ShardPilotConsentOutboxEntry settling)
        {
            if (settling.AnalyticsGranted || !_consentOutbox.HasDurableBackend ||
                !DenialWitnessStillOwedLocked())
            {
                return false;
            }

            if (TryHealDenialWitnessLocked())
            {
                // A witness write landed, or a newer decision superseded
                // the obligation: the settle may consume the receipt.
                return false;
            }

            // Position is not identity: re-find the settling receipt by
            // key — a concurrent decision burst can have cap-evicted it
            // meanwhile, and a key miss means it already left the trail
            // (nothing left to retain). Judged WITHOUT the in-flight
            // exclusion: the settling receipt's own in-flight claim is
            // this very settle, not a pending consumption elsewhere.
            var live = _consentOutbox.OwnScope.FindByKey(settling.IdempotencyKey);
            return live != null && ConsentReceiptPairQualifiesAsDenialWitnessLocked(live);
        }

        /// <summary>
        /// Caller must hold <c>_gate</c> AND the decision gate. The shared
        /// consent RECORD now carries this actor's denial pair, which witnesses
        /// the denial right now — but that is NOT a reason to retire the denial
        /// MARKER, and this is the distinction the retire used to miss.
        ///
        /// The record is a single lane ANY actor can supersede with a newer
        /// decision; the marker slot is this actor's own memory of its own
        /// denial, which every foreign retire deliberately keeps. Retiring it
        /// because the record momentarily agrees traded a durable per-actor
        /// witness for a lane a foreign grant can take a moment later — and
        /// then the denial had NO witness at all, in either lane, so the actor
        /// returned to a grant it never gave.
        ///
        /// The marker's retirement belongs to this actor's OWN newer decision
        /// or to its own boot imposition, both of which already retire it; so
        /// the write here is best effort and a refused one stays owed.
        ///
        /// Symmetrically, this write never CLOBBERS another actor's witness to
        /// take the slot: the slot holds one, and it
        /// is exactly the memory for them that this write is for us. Their
        /// occupancy simply leaves this denial owed, and the retained receipt
        /// is what backs it meanwhile — see
        /// <see cref="DenialWitnessStillOwedLocked"/>.
        /// </summary>
        /// <summary>
        /// Caller must hold <c>_gate</c>. Mirrors the OWED marker obligation
        /// into this scope's outbox mirror, which is the only per-actor storage
        /// this client owns. Without it the obligation would live in
        /// memory alone: a relaunch re-read a denied record, did not re-arm,
        /// and could consume the receipt that was standing in for the refused
        /// marker — after which a foreign grant took the record and the denial
        /// had nothing left anywhere.
        ///
        /// Derived, never assigned alongside the flags: there are a dozen sites
        /// that move them, and a copy updated at each would be the same drift
        /// this plane has been bitten by before. This runs at the outbox save
        /// chokepoint and at every dispatch point, and is a no-op unless the
        /// value actually changed.
        /// </summary>
        private void SyncOwedDenialWitnessLocked(bool freshDecision = false)
        {
            // ONE rule for every write this client makes to a denial-witness
            // lane, the marker slot's and the backup's alike: another actor's
            // occupant is not ours to take. The identity check bounds
            // BOTH directions here — we neither overwrite their obligation with
            // ours nor clear it when ours ends. The mirror is scoped to the
            // workspace/app/env, not to the actor, so an actor change on the
            // device meets theirs.
            // A torn-down or DISOWNED client stands down on the RETRY paths,
            // exactly as the witness heal does: a same-scope successor disowns
            // its predecessor without clearing that predecessor's pending fields
            // or stopping its worker, and carries the same anonymous id — so a
            // stale retry could overwrite the successor's backup with an older
            // pair.
            //
            // A FRESH decision is not a retry. A displaced client stays
            // callable, and a SetConsent on it is the newest thing anyone knows
            // — refusing to record its backup would drop exactly the obligation
            // this lane exists to keep.
            if (!freshDecision && (_workerStopped || _consentOutbox.IsDisowned))
            {
                return;
            }

            var owesWitness = _consentDenialMarkerPending &&
                              ShardPilotConsent.IsDeniedState(_consentState) &&
                              ShardPilotConsent.IsPlausibleDecisionStamp(
                                  _consentDenialPendingDecidedAt);

            // A FRESH decision never trusts the cache. Two clients can share an
            // anonymous id, so this client's belief about the key can be stale
            // the moment a sibling writes one — and a decision is exactly when
            // acting on a stale belief costs something: a grant would skip
            // retiring the sibling's backup.
            // Decisions are rare; the read is free at that rate.
            if (!freshDecision && !owesWitness && _owedBackupKnownAbsent)
            {
                // Nothing owed and nothing stored: the whole body below would
                // read the key, find it empty and return. Skip the I/O.
                return;
            }

            if (!freshDecision && owesWitness &&
                _owedBackupWrittenDecidedAt != null &&
                string.Equals(
                    _owedBackupWrittenDecidedAt, _consentDenialPendingDecidedAt,
                    StringComparison.Ordinal) &&
                _owedBackupWrittenSeq == _consentDenialPendingSeq)
            {
                // We put exactly this there and nothing since has changed our
                // mind. The read below would only confirm it.
                return;
            }

            // NO BLIND WRITE ON A CACHED ABSENCE. There used to be a fast path
            // here that wrote the pending pair straight through when this client
            // believed the key empty, on the reasoning that a read could only
            // confirm the emptiness. It cannot: the cache is this client's belief,
            // the key is shared with every same-actor sibling, and a sibling that
            // wrote a NEWER denial since had its only durable witness overwritten
            // by our older pair. The full path below
            // already declines exactly that, by comparing pairs under the gate —
            // this path existed only to skip the read that makes the comparison
            // possible.
            //
            // The cache still earns its keep where it was actually measured: the
            // stand-down above, for the many clients that owe nothing at all. What
            // is paid back is one read per dispatch point on a client that is
            // ACTIVELY owing a witness, which is rare and already doing I/O.

            // ONE key, MANY clients: the backup is global while each client
            // holds only its private _gate, so an unguarded read-then-write lets
            // two actors' workers both see it empty and the loser's only restart
            // witness disappear. The decision gate is
            // the store-wide section that already serialises decisions.
            //
            // TryEnter, not Enter: taking that gate under _gate is the ONE
            // deliberate lock inversion this plane allows, and only because it
            // never blocks. A busy gate means a decision is landing right now,
            // and this runs again at the next dispatch point.
            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                return;
            }

            try
            {
                SyncOwedDenialWitnessUnderGateLocked(freshDecision);
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// Caller must hold <c>_gate</c> AND the decision gate.
        /// </summary>
        private void SyncOwedDenialWitnessUnderGateLocked(bool freshDecision)
        {
            var owesWitness = _consentDenialMarkerPending &&
                              ShardPilotConsent.IsDeniedState(_consentState) &&
                              ShardPilotConsent.IsPlausibleDecisionStamp(
                                  _consentDenialPendingDecidedAt);

            ShardPilotDenialMarker stored;
            var evidence = JudgeBackupEvidenceLocked(out stored);

            // The verdict is applied unconditionally: the applier itself knows
            // that a fresh decision by this actor outranks anything its own key
            // can say. The verdict is still used below.
            ApplyBackupEvidenceLocked(evidence, stored, freshDecision);

            if (evidence == BackupEvidence.Unreadable && !freshDecision)
            {
                // Corrupt or refused: it may still be OUR denial, so it is not
                // ours to overwrite or clear on a guess. The hold is armed by
                // the apply above; nothing else happens here.
                return;
            }

            if (evidence == BackupEvidence.Unreadable)
            {
                // A FRESH decision is authoritative over this actor's OWN key,
                // corrupt or not: the actor just decided, and leaving unreadable
                // content there would keep every later launch failing closed on
                // it forever. Write the new pair, or
                // clear the key when nothing is owed.
                if (owesWitness)
                {
                    if (ShardPilotConsent.RecordOwedDenialWitness(
                            _settingsStore, _consentState, _config.AnonymousId,
                            _consentDenialPendingDecidedAt, _consentDenialPendingSeq))
                    {
                        _owedBackupKnownAbsent = false;
                        _owedBackupWrittenDecidedAt = _consentDenialPendingDecidedAt;
                        _owedBackupWrittenSeq = _consentDenialPendingSeq;
                    }

                    return;
                }

                if (ShardPilotConsent.RetireOwedDenialWitness(
                        _settingsStore, _config.AnonymousId))
                {
                    _owedBackupKnownAbsent = true;
                    _owedBackupWrittenDecidedAt = null;
                }

                return;
            }

            if (_consentDenialMarkerPending &&
                ShardPilotConsent.IsDeniedState(_consentState) &&
                ShardPilotConsent.IsPlausibleDecisionStamp(_consentDenialPendingDecidedAt))
            {
                if (stored != null && !stored.Legacy &&
                    ShardPilotConsent.DecisionPairNewer(
                        stored.DecidedAt, stored.DecisionSeq,
                        _consentDenialPendingDecidedAt, _consentDenialPendingSeq))
                {
                    // What is stored is NEWER than what we owe — a sibling's
                    // later denial. Overwriting it with our older pair would
                    // downgrade the durable witness.
                    return;
                }

                if (stored != null && !stored.Legacy &&
                    stored.State == _consentState &&
                    string.Equals(
                        stored.AnonymousId, _config.AnonymousId, StringComparison.Ordinal) &&
                    string.Equals(
                        stored.DecidedAt, _consentDenialPendingDecidedAt, StringComparison.Ordinal) &&
                    stored.DecisionSeq == _consentDenialPendingSeq)
                {
                    // Already exactly this. Rewriting it every dispatch point
                    // would put a File.WriteAllText on the flush cadence for as
                    // long as the marker write stays refused.
                    return;
                }

                if (ShardPilotConsent.RecordOwedDenialWitness(
                        _settingsStore, _consentState, _config.AnonymousId,
                        _consentDenialPendingDecidedAt, _consentDenialPendingSeq))
                {
                    _owedBackupKnownAbsent = false;
                    _owedBackupWrittenDecidedAt = _consentDenialPendingDecidedAt;
                    _owedBackupWrittenSeq = _consentDenialPendingSeq;
                }

                return;
            }

            // RETIRE ONLY ON GLOBAL EVIDENCE. The backup key is shared while
            // _consentDenialMarkerPending is this client's alone, so "I owe
            // nothing" says nothing about a SIBLING that still does — and a
            // propagated denial leaves siblings with clear flags by design.
            // The marker slot carrying the stored pair is proof the obligation
            // was discharged; a newer own decision is proof it was superseded.
            if (evidence == BackupEvidence.Absent)
            {
                _owedBackupKnownAbsent = true;
                _owedBackupWrittenDecidedAt = null;
                // Nothing stored: the common case for every client that never
                // owes a witness. Retiring anyway would put a File.WriteAllText
                // on the flush cadence for its whole life, since the key is
                // file-routed and a retire is a Set.
                return;
            }

            // The verdict already judged it, and the apply already lifted the
            // hold if it earned lifting. All that is left is the retire.
            //
            // A DISCHARGED backup is redundant exactly like a superseded one — the
            // slot carries its pair — but only once the denial it carries is
            // actually in force HERE. Retiring it while this client is still open
            // would make the next verdict ABSENT, and Absent is the verdict that
            // LIFTS: the hold would end over a denial nothing had applied yet.
            //
            // A LIVE backup is the only witness there is, and is never retired.
            if (evidence == BackupEvidence.Superseded ||
                (evidence == BackupEvidence.Discharged &&
                 ShardPilotConsent.IsDeniedState(_consentState)))
            {
                if (ShardPilotConsent.RetireOwedDenialWitness(
                        _settingsStore, _config.AnonymousId))
                {
                    _owedBackupKnownAbsent = true;
                    _owedBackupWrittenDecidedAt = null;
                }
            }
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. Whether the stored backup's pair
        /// REACHED THE MARKER SLOT — the obligation it was backing up is
        /// discharged and the key is redundant.
        ///
        /// This does not determine whether the denial still stands: the slot
        /// carrying the pair witnesses the denial rather than withdrawing it.
        /// Discharge must not lift a hold or restore a grant.
        /// </summary>
        private bool OwedBackupReachedTheMarkerSlotLocked(ShardPilotDenialMarker stored)
        {
            bool slotUnreadable;
            var slot = ShardPilotConsent.LoadDenialMarker(_settingsStore, out slotUnreadable);
            return !slotUnreadable && slot != null && !slot.Mangled && !slot.Legacy &&
                   string.Equals(slot.AnonymousId, stored.AnonymousId, StringComparison.Ordinal) &&
                   string.Equals(slot.DecidedAt, stored.DecidedAt, StringComparison.Ordinal) &&
                   slot.DecisionSeq == stored.DecisionSeq &&
                   // FLAVOR TOO. Same actor and same pair but a different State is
                   // not the same decision written twice — it is two records that
                   // disagree, and one of them is wrong. Discharging on the pair
                   // alone let the caller retire a DeniedForcedMinor backup while
                   // the slot held a plain Denied, so the next launch restored the
                   // wrong flavor. A disagreement falls
                   // through to LIVE, which shuts egress and keeps both.
                   //
                   // NOT covered by a test, and the reason is written down rather
                   // than papered over: the retire this protects sits past the
                   // sync's write branch, which returns early whenever a marker
                   // obligation is armed — the state a boot imposition leaves — so
                   // every fixture reaching this shape short-circuits before the
                   // retire. A test was written, found to pass with the comparison
                   // DELETED, and removed. The tightening stands on its own terms:
                   // an equality over part of a record is not an equality.
                   slot.State == stored.State;
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. Whether a NEWER decision by the SAME
        /// actor replaced the stored denial — the one thing that makes it
        /// obsolete rather than merely re-witnessed. Anything else — including
        /// "this client has no pending flag" — leaves it, because the backup
        /// outlives the client that armed it.
        /// </summary>
        private bool OwedBackupIsSupersededLocked(ShardPilotDenialMarker stored)
        {
            // A newer WITHHELD GRANT is a newer decision by this actor too — it
            // simply has not reached the record yet, because the receipt-first
            // ordering is holding its persist back. Asking here rather than at the
            // moment of imposition is what makes the verdict complete: a verdict
            // that said "impose" and a separate stand-down that said "actually,
            // don't" were two answers to one question, and keeping them apart is
            // what let the imposition be queued without shutting egress — nothing
            // could safely close on a decision that a later step might reverse. Folded in, the verdict is final:
            // Superseded means nothing will be imposed, and Live/Discharged means
            // something WILL be, so egress can shut the moment one is judged.
            //
            // Same actor by construction, twice over: the backup key is read with
            // THIS client's own anonymous id, and an identity change drops this
            // scope's withheld marker with the receipt it belonged to.
            //
            // The stamp is validated before it is compared, as
            // CompareDecisionStamps requires and as an earlier fix had to learn on the
            // other side of this same comparison: an implausible stamp is not a
            // newer decision, it is an unusable one, and treating it as newer
            // would retire a real denial's last witness.
            if (!stored.Legacy && ShardPilotConsent.IsPlausibleDecisionStamp(stored.DecidedAt))
            {
                var withheldGrant = ShardPilotConsent.LoadWithheldGrant(
                    _settingsStore, _consentScopeHash);
                if (withheldGrant != null &&
                    ShardPilotConsent.IsPlausibleDecisionStamp(withheldGrant.DecidedAt) &&
                    DenialProvablyOlderThanObligation(
                        stored.DecidedAt, stored.DecisionSeq,
                        withheldGrant.DecidedAt, withheldGrant.DecisionSeq))
                {
                    return true;
                }
            }

            // Superseded: the durable RECORD carries a newer decision by the
            // SAME actor — the identical rule the boot imposition's stale guard
            // applies to the slot, so the two cannot drift. A different actor's
            // newer record supersedes only their own denial, never this one.
            bool recordReadFailed;
            var record = ShardPilotConsent.LoadRecord(_settingsStore, out recordReadFailed);
            if (recordReadFailed || record.Legacy || stored.Legacy ||
                record.AnonymousId == null ||
                !ShardPilotConsentOutbox.IsValidIdentifier(record.AnonymousId) ||
                !string.Equals(record.AnonymousId, stored.AnonymousId, StringComparison.Ordinal) ||
                !ShardPilotConsent.DecisionPairNewer(
                    record.DecidedAt, record.DecisionSeq,
                    stored.DecidedAt, stored.DecisionSeq))
            {
                return false;
            }

            // A superseding DENIAL must have a witness of its own before the one
            // it supersedes is deleted. Otherwise a
            // record-only D2 retires D1, a foreign grant takes the record, and
            // the actor has neither. A superseding GRANT needs no denial
            // witness — it is not a denial.
            if (!ShardPilotConsent.IsDeniedState(record.State))
            {
                return true;
            }

            bool slotUnjudgeable;
            var witness = ShardPilotConsent.LoadDenialMarker(_settingsStore, out slotUnjudgeable);
            return !slotUnjudgeable && witness != null && !witness.Mangled && !witness.Legacy &&
                   string.Equals(
                       witness.AnonymousId, record.AnonymousId, StringComparison.Ordinal) &&
                   string.Equals(witness.DecidedAt, record.DecidedAt, StringComparison.Ordinal) &&
                   witness.DecisionSeq == record.DecisionSeq;
        }

        private void KeepOwnDenialMarkerLocked()
        {
            if (!_consentDenialMarkerPending)
            {
                return;
            }

            bool occupantUnreadable;
            var occupant = ShardPilotConsent.LoadDenialMarker(_settingsStore, out occupantUnreadable);
            if (occupantUnreadable ||
                ShardPilotConsent.IsForeignDenialMarkerOccupant(occupant, _config.AnonymousId))
            {
                // Unreadable is the same answer: the slot MAY hold a foreign
                // witness, and taking it on a failed read would launder one
                // away. Stay owed and retry.
                return;
            }

            if (ShardPilotConsent.RecordDenialMarker(
                    _settingsStore, _consentState, _config.AnonymousId,
                    _consentDenialPendingDecidedAt, _consentDenialPendingSeq))
            {
                _consentDenialMarkerPending = false;
            }
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. The SHARED witness-heal core the
        /// settle guard and the dispatch-point/close-ladder retry both run
        /// (one judgment, no divergence):
        /// TryEnters the decision gate (the one deliberate inversion of
        /// the PrivacyDecisionGate→_gate order — non-blocking, so it
        /// cannot deadlock; a busy gate means a decision is deciding right
        /// now and the caller stays fail-closed), stands down for
        /// torn-down/disowned clients, and re-validates the durable record
        /// before writing:
        /// - a newer SAME-ACTOR record supersedes the obligation
        ///   (pendings clear; a record already carrying the pending
        ///   pair discharges the RECORD obligation only — the write
        ///   landed elsewhere — and leaves the marker to
        ///   KeepOwnDenialMarkerLocked);
        /// - a newer FOREIGN-actor record is never clobbered AND never
        ///   stands the own witness down — the MARKER is the actor-scoped
        ///   witness lane (kept by every foreign retire), so only the
        ///   marker write is attempted and the record write stays owed
        ///   for the actor's return;
        /// - otherwise the record write retries first, then the marker.
        /// Returns TRUE when the standing denial ends the call WITNESSED
        /// or SUPERSEDED (nothing left for receipt evidence to back);
        /// FALSE when the obligation is still unwitnessed — the caller
        /// stays fail-closed on the receipt. WITNESSED means the MARKER
        /// landed, in every branch alike: a
        /// landed record discharges its own obligation but is a lane a
        /// foreign decision can take, so reporting it as witnessed let
        /// the settle consume the very receipt that was covering the
        /// still-refused marker.
        /// </summary>
        private bool TryHealDenialWitnessLocked()
        {
            // A torn-down client's obligation died with its close (the
            // ladder judged a durable witness existed); a DISOWNED
            // client's record belongs to its same-scope successor. Either
            // way this instance must not write the shared record — and it
            // cannot declare the denial witnessed either.
            if (_workerStopped || _consentOutbox.IsDisowned)
            {
                return false;
            }

            if (!Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                return false;
            }

            try
            {
                bool retryReadFailed;
                var current = ShardPilotConsent.LoadRecord(_settingsStore, out retryReadFailed);
                if (!retryReadFailed && !current.Legacy &&
                    ShardPilotConsent.DecisionPairNewer(
                        current.DecidedAt, current.DecisionSeq,
                        _consentDenialPendingDecidedAt, _consentDenialPendingSeq))
                {
                    var recordActorIsOwn =
                        current.AnonymousId != null &&
                        ShardPilotConsentOutbox.IsValidIdentifier(current.AnonymousId) &&
                        string.Equals(
                            current.AnonymousId, _config.AnonymousId, StringComparison.Ordinal);
                    if (recordActorIsOwn)
                    {
                        // A newer decision by THIS actor supersedes the
                        // standing denial: the obligation stands down.
                        _consentDenialRecordPending = false;
                        _consentDenialMarkerPending = false;
                        return true;
                    }

                    // A FOREIGN actor's newer record: neither clobber it nor stand the own
                    // witness down — write the actor-scoped MARKER only;
                    // the record stays theirs and this actor's denial
                    // re-converges from the marker when they return.
                    //
                    // Through the SAME writer as every other marker write, so
                    // the "never take another actor's occupant" rule holds here
                    // too. Writing directly here meant a foreign record and a
                    // foreign SLOT occupant were judged by different rules, and
                    // this branch quietly took the slot.
                    KeepOwnDenialMarkerLocked();

                    return !_consentDenialMarkerPending;
                }

                if (!retryReadFailed && !current.Legacy &&
                    ShardPilotConsent.IsDeniedState(current.State) &&
                    string.Equals(
                        current.DecidedAt, _consentDenialPendingDecidedAt,
                        StringComparison.Ordinal) &&
                    current.DecisionSeq == _consentDenialPendingSeq)
                {
                    // The record already carries the pending pair — the
                    // write landed elsewhere. The RECORD obligation is
                    // discharged; the marker's is NOT retired on the
                    // record's strength (see KeepOwnDenialMarkerLocked).
                    _consentDenialRecordPending = false;
                    KeepOwnDenialMarkerLocked();
                    return !_consentDenialMarkerPending;
                }

                if (PersistConsentState(
                        _consentState, _consentDenialPendingDecidedAt, _consentDenialPendingSeq))
                {
                    _consentDenialRecordPending = false;
                    KeepOwnDenialMarkerLocked();
                    return !_consentDenialMarkerPending;
                }

                if (_consentDenialMarkerPending &&
                    ShardPilotConsent.RecordDenialMarker(
                        _settingsStore, _consentState, _config.AnonymousId,
                        _consentDenialPendingDecidedAt, _consentDenialPendingSeq))
                {
                    // The marker carries the decision's full pair — a
                    // durable receipt-independent witness; the record
                    // write stays owed and re-converges at the next
                    // launch's imposition.
                    _consentDenialMarkerPending = false;
                }

                return !_consentDenialMarkerPending;
            }
            finally
            {
                Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate);
            }
        }

        /// <summary>
        /// The shutdown ladder's WRITE RETRIES (run before each close
        /// path's durability judgment and at the consent dispatch points):
        /// delegates to the shared witness-heal core
        /// (<see cref="TryHealDenialWitnessLocked"/> — one judgment for
        /// this retry and the settle guard alike: decision-gate TryEnter,
        /// stale-client stand-down, actor-scoped record re-validation,
        /// record→marker write order). A no-op once a newer decision
        /// superseded the denial or nothing is owed.
        /// </summary>
        private void RetryOwedDenialWitnessWrites()
        {
            bool pruned;
            bool completeWithheldGrant;
            lock (_gate)
            {
                if (_consentDenialRecordPending || _consentDenialMarkerPending)
                {
                    // Only heal while something is actually owed; the release
                    // below runs either way, because a heal is not the only way
                    // the witness stops being owed — a newer decision
                    // superseding the denial does it too.
                    if (ShardPilotConsent.IsDeniedState(_consentState))
                    {
                        TryHealDenialWitnessLocked();
                    }
                }

                // AFTER the heal too, and for the same reason: the heal is what
                // discharges the obligation, and the mirror must not keep
                // claiming one that just landed.
                SyncOwedDenialWitnessLocked();

                // AFTER the heal, deliberately: the heal is precisely what can
                // land a write and stop the witness being owed, and on the
                // automatic worker path there is no second dispatch point until
                // the flush interval elapses.
                pruned = ReleaseSettledWitnessReceiptsLocked(out completeWithheldGrant);
            }

            // Outside the lock, like every other durable write on this path: a
            // failed rewrite never blocks the trail — it stays owed and retries,
            // and a process death re-sends the stale entry, de-duplicated by
            // idempotency key.
            if (pruned)
            {
                PersistConsentOutbox();
            }

            if (completeWithheldGrant)
            {
                CompleteWithheldGrantStatePersist();
            }
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. Releases every receipt held ONLY as
        /// the standing denial's durable witness once it stops being one, and
        /// reports whether anything left the trail so the caller can persist
        /// outside the lock.
        ///
        /// A held receipt's SERVER outcome is already settled — retention only
        /// ever happens after a 2xx or a terminal refusal — so nothing is owed
        /// to the server and the release is a local prune, never a re-dispatch.
        /// DenialWitnessBlocksConsumptionLocked supplies the same predicate
        /// used by the settle guard, keeping retention and release consistent.
        /// </summary>
        private bool ReleaseSettledWitnessReceiptsLocked(out bool completeWithheldGrant)
        {
            completeWithheldGrant = false;
            var pruned = false;

            foreach (var held in _consentOutbox.SnapshotEntries())
            {
                if (!held.WitnessHeldAfterSettle ||
                    string.Equals(_consentInFlightKey, held.IdempotencyKey, StringComparison.Ordinal))
                {
                    continue;
                }

                // The predicate's tail WITHOUT its TryHeal: this pass already
                // healed once above, and re-healing per retained entry turns one
                // write ordering into N — which is observable, and which the
                // trail's ordering tests assert on.
                var stillWitness = !held.AnalyticsGranted && _consentOutbox.HasDurableBackend &&
                                   DenialWitnessStillOwedLocked() &&
                                   ConsentReceiptPairQualifiesAsDenialWitnessLocked(held);
                if (stillWitness)
                {
                    continue;
                }

                _consentOutbox.RemoveFromMirror(held);
                pruned = true;

                // Same obligation the consume branches carry: a withheld grant
                // whose receipt is leaving still completes its observable
                // persist — the hold was a DELIVERY outcome, not a decision
                // change.
                if (held.AnalyticsGranted &&
                    string.Equals(_withheldGrantStateKey, held.IdempotencyKey, StringComparison.Ordinal))
                {
                    _withheldGrantStateKey = null;
                    completeWithheldGrant = true;
                }
            }

            return pruned;
        }

        /// <summary>
        /// TRUE when the STANDING denial outranks — or cannot be ordered
        /// against — a withheld-grant obligation decided at
        /// <paramref name="obligationDecidedAt"/>: the fail-closed
        /// default, preserving the "a newer denial supersedes the
        /// obligation" discharge semantics. FALSE only when the denial is
        /// PROVABLY OLDER than the obligation: then it is a STALE witness
        /// — a marker or belt imposition of a decision the withheld grant
        /// itself superseded — and the completion must win, or a denial
        /// whose record write failed would permanently eat the newer
        /// grant the backend already recorded.
        /// Ordered on the live witness-pending pair first (a live
        /// denial's exact pair), then the durable record's, then the
        /// marker slot's (the witness an unconverged imposition rode).
        /// FULL-pair comparisons when the obligation carries its minted
        /// seq (a same-stamp deny→grant is
        /// decided by the seq, real on coarse UtcNow timers); a seq-less
        /// obligation falls back to stamp-only with ties to the denial.
        /// </summary>
        private bool StandingDenialOutranksObligation(
            string obligationDecidedAt, long obligationDecisionSeq)
        {
            if (!ShardPilotConsent.IsPlausibleDecisionStamp(obligationDecidedAt))
            {
                return true;
            }

            bool pendingArmed;
            string pendingStamp;
            long pendingSeq;
            lock (_gate)
            {
                pendingArmed = _consentDenialRecordPending || _consentDenialMarkerPending;
                pendingStamp = _consentDenialPendingDecidedAt;
                pendingSeq = _consentDenialPendingSeq;
            }

            if (pendingArmed)
            {
                return !ShardPilotConsent.IsPlausibleDecisionStamp(pendingStamp) ||
                       !DenialProvablyOlderThanObligation(
                           pendingStamp, pendingSeq, obligationDecidedAt, obligationDecisionSeq);
            }

            bool recordReadFailed;
            var record = ShardPilotConsent.LoadRecord(_settingsStore, out recordReadFailed);
            if (recordReadFailed)
            {
                return true;
            }

            if (ShardPilotConsent.IsDeniedState(record.State))
            {
                return record.Legacy ||
                       !DenialProvablyOlderThanObligation(
                           record.DecidedAt, record.DecisionSeq,
                           obligationDecidedAt, obligationDecisionSeq);
            }

            // The record does not hold the denial (an imposition whose
            // convergence is still owed): the marker slot carries the
            // witness pair.
            bool markerUnreadable;
            var marker = ShardPilotConsent.LoadDenialMarker(_settingsStore, out markerUnreadable);
            if (markerUnreadable ||
                (marker != null && (marker.Mangled || marker.Legacy)))
            {
                return true;
            }

            if (marker != null)
            {
                return !DenialProvablyOlderThanObligation(
                    marker.DecidedAt, marker.DecisionSeq,
                    obligationDecidedAt, obligationDecisionSeq);
            }

            // No durable ordering witness left (e.g. the session shadow of
            // a denial whose every write failed): fail closed.
            return true;
        }

        /// <summary>
        /// TRUE only when the denial's pair is STRICTLY OLDER than the
        /// obligation's — the one shape that lets a withheld-grant
        /// completion win. With the obligation's seq carried (&gt; 0) this
        /// is the standard full-pair ordering; a seq-less obligation can
        /// only prove it postdates the denial by STRICT stamp order (a
        /// same-stamp tie is unprovable and goes to the denial,
        /// fail-closed).
        /// </summary>
        private static bool DenialProvablyOlderThanObligation(
            string denialStamp, long denialSeq, string obligationStamp, long obligationSeq)
        {
            if (obligationSeq > 0)
            {
                return ShardPilotConsent.DecisionPairNewer(
                    obligationStamp, obligationSeq, denialStamp, denialSeq);
            }

            return ShardPilotConsent.CompareDecisionStamps(obligationStamp, denialStamp) > 0;
        }

        /// <summary>
        /// Caller must hold <c>_gate</c>. DENIAL-WITNESS refusal residual:
        /// TRUE while the standing
        /// denial has NO durable witness anywhere — record and marker
        /// writes both still owed AND no qualifying receipt durably
        /// retained. A close finalizing in this shape would let the next
        /// launch read the stale pre-denial record and boot GRANTED
        /// against an explicit revocation; the close paths hold the close on
        /// it (the worker keeps retrying the writes) until its deadline, and
        /// a close still owing it there answers spool_persist_failed and
        /// keeps the denial in force in memory for the rest of the process.
        /// Residual, documented: a process KILL before any durable write
        /// lands loses the denial without trace — no client-side ordering
        /// can close that; the hold covers every exit the host controls.
        /// </summary>
        private bool DenialWitnessUnbackedLocked()
        {
            return _consentDenialRecordPending && _consentDenialMarkerPending &&
                   ShardPilotConsent.IsDeniedState(_consentState) &&
                   !DenialReceiptRetainedDurablyLocked();
        }

        /// <summary>
        /// Settles one receipt's response per the consent plane's shared
        /// retryability rules:
        /// - 2xx acknowledges: prune, clear the deferral and backoff, chain
        ///   the next retained receipt (one healthy dispatch point drains
        ///   the whole trail).
        /// - Retryable — transport unreachable (0), 408, 429, any 5xx, and 401
        ///   ONLY in Mode B (a fresh JWT can be minted; the cached one is
        ///   invalidated): the receipt stays AT THE HEAD. A server
        ///   Retry-After (whole seconds) defers the CONSENT plane on 429 AND
        ///   5xx; without one, jittered exponential backoff (first retry
        ///   free, then full jitter in [1s, min(2^(n-2), 60)s]); every
        ///   deferral clamps at 24h. A 401 is an auth problem and is never
        ///   deferred. This deferral state is INDEPENDENT of the events
        ///   plane's publish backoff.
        /// - Terminal — every other outcome, including any other 4xx and a
        ///   Mode-A 401/403 (the static key can never change; a Mode-A
        ///   grant's 403 consent_grant_requires_verified_credential is the
        ///   server's documented answer): the receipt is DROPPED (pruned +
        ///   diagnosed) so receipts queued behind it still deliver, and the
        ///   next is chained. Never pin a 2xx for Mode-A grants against the
        ///   real server (spec D4).
        /// </summary>
        private void OnConsentReceiptResponse(
            ShardPilotConsentOutboxEntry entry, ShardPilotHttpResponse response, bool usedPublishableKey, string requestBearer)
        {
            var transportError = response == null || response.TransportError;
            var status = transportError ? 0 : response.StatusCode;
            if (status >= 200 && status < 300)
            {
                bool completeWithheldGrant;
                bool retainedAsDenialWitness;
                lock (_gate)
                {
                    // Recorded ONCE per receipt: a receipt retained past
                    // its acknowledgment as a denial witness (below)
                    // re-acknowledges on every paced re-dispatch — the
                    // server de-duplicates those on idempotency_key, and
                    // the RecordedCounted flag de-duplicates the stat. A
                    // key miss still counts once: the receipt left the
                    // trail mid-flight (cap eviction) and can never
                    // re-acknowledge.
                    var acked = _consentOutbox.OwnScope.FindByKey(entry.IdempotencyKey);
                    if (acked == null || !acked.RecordedCounted)
                    {
                        _stats.RecordConsentRecorded();
                    }

                    if (acked != null)
                    {
                        acked.RecordedCounted = true;
                    }

                    // WITNESS-GAP guard: an acknowledged DENIAL receipt
                    // that is the standing denial's only durable witness
                    // stays RETAINED (see DenialWitnessBlocksConsumptionLocked
                    // — it retries the record and marker writes first).
                    retainedAsDenialWitness = DenialWitnessBlocksConsumptionLocked(entry);
                    if (retainedAsDenialWitness)
                    {
                        ClearConsentInFlightLocked(entry);
                        _consentBackoffAttempt = 0;
                        // Held, not paced: the server is done with this receipt
                        // and only the witness gap keeps it. The dispatch-point
                        // prune releases it the moment that gap closes, so there
                        // is no deadline to go stale.
                        if (acked != null)
                        {
                            acked.WitnessHeldAfterSettle = true;
                        }
                        completeWithheldGrant = false;
                    }
                    else
                    {
                        // Prune and release ATOMICALLY: the settled head leaves
                        // the mirror in the same lock hold that clears the
                        // in-flight claim — a dispatch point interleaving
                        // between a clear and a later prune could otherwise
                        // re-send the just-acknowledged head (a duplicate whose
                        // eventual failure would park the trail behind a stale
                        // deadline). The durable rewrite still runs outside the
                        // lock: a failed rewrite never blocks the rest of the
                        // trail — the write stays owed and retries, and a
                        // process death re-sends the stale entry, de-duplicated
                        // by idempotency key.
                        _consentOutbox.OwnScope.RemoveFromMirror(entry.IdempotencyKey);
                        ClearConsentInFlightLocked(entry);
                        _consentRetryNotBefore = DateTime.MinValue;
                        _consentBackoffAttempt = 0;
                        _consentTokenRetryWakeAt = DateTime.MinValue;

                        // The async 2xx of a grant whose OBSERVABLE state
                        // persist was withheld (SetConsent's in-call window
                        // closed first): claim the completion in this same hold.
                        completeWithheldGrant = entry.AnalyticsGranted &&
                                                string.Equals(
                                                    _withheldGrantStateKey,
                                                    entry.IdempotencyKey,
                                                    StringComparison.Ordinal);
                        if (completeWithheldGrant)
                        {
                            _withheldGrantStateKey = null;
                        }
                    }
                }

                if (retainedAsDenialWitness)
                {
                    // The acknowledged receipt stays retained as the
                    // denial's only durable witness; the pacing window
                    // keeps the re-dispatch cadence bounded. The close
                    // ladder owns the final judgment (it completes on this
                    // durably retained receipt, or on whichever witness
                    // write lands first).
                    SafeLogRequest(requestBearer,
                        "shardpilot consent receipt retained as the denial's only durable witness " +
                        "after acknowledgment; witness writes retry at the dispatch points");
                    WakeWorker();
                    return;
                }

                PersistConsentOutbox();
                if (completeWithheldGrant)
                {
                    // The backend just RECORDED this grant: finish the
                    // receipt-first ordered pair so the next launch loads
                    // the grant instead of failing closed against a consent
                    // row the backend already accepted.
                    CompleteWithheldGrantStatePersist();
                }

                // The grant-receipt gate may have released: give the worker a prompt
                // publish opportunity, then chain the next retained receipt.
                WakeWorker();
                MaybeKickConsentOutbox(workerCadence: false);
                return;
            }

            var code = "http_" + status.ToString(System.Globalization.CultureInfo.InvariantCulture);

            // 401 handling follows the CREDENTIAL the dispatch actually used,
            // never the configured mode: a
            // publishable-key 401 is a static credential's permanent verdict
            // — terminal even while a TokenProvider is configured for other
            // receipts — and says nothing about the cached JWT, so it must
            // neither retry nor invalidate the token another receipt is
            // about to ride.
            var healable401 = status == 401 && _tokenManager != null && !usedPublishableKey;
            var retryable = status == 0 || status == 408 || status == 429 || (status >= 500 && status <= 599) || healable401;
            _stats.RecordConsentFailed(ShardPilotDiagnostics.Redact(_config, code, requestBearer));
            if (healable401)
            {
                // A Mode-B JWT rejected by the consent endpoint is stale:
                // invalidate it so the next dispatch point re-mints, exactly
                // as the publish path does — and advance the auth epoch so a
                // bearer resolved BEFORE this invalidation can never carry a
                // later dispatch (see _consentAuthEpoch).
                _tokenManager.Invalidate();
                lock (_gate)
                {
                    _consentAuthEpoch++;
                }
            }

            if (retryable)
            {
                lock (_gate)
                {
                    ClearConsentInFlightLocked(entry);
                    if (!healable401)
                    {
                        var retryAfterSeconds = response != null ? response.RetryAfterSeconds : 0;
                        var wait = retryAfterSeconds > 0
                            ? TimeSpan.FromSeconds(retryAfterSeconds)
                            : NextConsentBackoffWaitLocked();
                        if (wait > MaxConsentDeferral)
                        {
                            wait = MaxConsentDeferral;
                        }

                        _consentRetryNotBefore = DateTime.UtcNow + wait;
                    }
                }

                SafeLogRequest(requestBearer, "shardpilot consent post failed ({0}); receipt retained for retry", code);

                // Recompute the worker's wait against the fresh deferral: a
                // short server Retry-After must cap a long flush-interval
                // sleep that started before this window existed.
                WakeWorker();
                return;
            }

            // Terminal outcome: drop this receipt so the ones queued behind
            // it still deliver, then chain the next. Prune and release
            // atomically (same rationale as the acknowledgment path), and
            // reset the deferral/backoff streak — it belonged to THIS
            // receipt's failures; the next queued receipt starts fresh
            // instead of inheriting a window that would needlessly prolong
            // the grant-receipt gate.
            bool completeWithheldGrantOnDrop;
            bool retainedAsDenialWitnessOnDrop;
            lock (_gate)
            {
                // WITNESS-GAP guard, terminal flavor: the drop consumes
                // this receipt's durability exactly like the 2xx prune —
                // and a terminally rejected denial receipt may still be
                // the ONLY durable evidence of a denial whose record and
                // marker writes are both owed (the server refusing the
                // RECEIPT does not un-decide the local state the next
                // launch must fail closed to). Over a still-open gap the
                // receipt is RETAINED (held at the head, paced one flush
                // cadence — the server keeps refusing the bounded
                // re-sends) until a witness write lands or a newer
                // decision supersedes the denial. The wedge this holds on
                // the trail is deliberate fail-closed: it exists only
                // while the record and marker stores are both broken.
                retainedAsDenialWitnessOnDrop = DenialWitnessBlocksConsumptionLocked(entry);
                if (retainedAsDenialWitnessOnDrop)
                {
                    ClearConsentInFlightLocked(entry);
                    // The backoff streak belonged to THIS receipt's failures and
                    // dies with its settlement, exactly as on the 2xx retention
                    // branch above. It used to be reset later, when the receipt
                    // eventually re-dispatched into a consume branch — a path
                    // the local prune no longer takes, so leaving it here would
                    // hand the next queued receipt this one's retry count and
                    // delay its FIRST retryable response by a ladder it never
                    // climbed.
                    _consentBackoffAttempt = 0;

                    // Held, not paced. The server terminally refused this
                    // receipt, so nothing more is owed to it and only the
                    // witness gap keeps the row; the dispatch-point prune
                    // releases it the moment that gap closes.
                    var heldOnDrop = _consentOutbox.OwnScope.FindByKey(entry.IdempotencyKey);
                    if (heldOnDrop != null)
                    {
                        heldOnDrop.WitnessHeldAfterSettle = true;
                    }

                    completeWithheldGrantOnDrop = false;
                }
                else
                {
                    _consentOutbox.OwnScope.RemoveFromMirror(entry.IdempotencyKey);
                    ClearConsentInFlightLocked(entry);
                    _consentRetryNotBefore = DateTime.MinValue;
                    _consentBackoffAttempt = 0;
                    _consentTokenRetryWakeAt = DateTime.MinValue;

                    // A withheld grant whose receipt takes a TERMINAL drop
                    // (e.g. the Mode-A 403) still completes the observable
                    // grant persist: the drop is a DELIVERY outcome, not a
                    // decision change — the un-withheld path persists the grant
                    // before the dispatch outcome is even known, and this path
                    // must converge to the same state.
                    completeWithheldGrantOnDrop = entry.AnalyticsGranted &&
                                                  string.Equals(
                                                      _withheldGrantStateKey,
                                                      entry.IdempotencyKey,
                                                      StringComparison.Ordinal);
                    if (completeWithheldGrantOnDrop)
                    {
                        _withheldGrantStateKey = null;
                    }
                }
            }

            if (retainedAsDenialWitnessOnDrop)
            {
                SafeLogRequest(requestBearer,
                    "shardpilot consent receipt retained as the denial's only durable witness " +
                    "after a terminal settle ({0}); witness writes retry at the dispatch points",
                    code);
                WakeWorker();
                return;
            }

            PersistConsentOutbox();
            if (completeWithheldGrantOnDrop)
            {
                CompleteWithheldGrantStatePersist();
            }

            SafeLogRequest(requestBearer, "shardpilot consent receipt dropped (scope=consent, status=dropped, code={0})", code);
            WakeWorker();
            MaybeKickConsentOutbox(workerCadence: false);
        }

        /// <summary>
        /// Every consent-plane deferral — server Retry-After or local
        /// backoff — clamps here (the defold-canonical 24h
        /// max_publish_defer_seconds applied to the consent plane).
        /// </summary>
        private static readonly TimeSpan MaxConsentDeferral = TimeSpan.FromHours(24);

        /// <summary>Caller must hold <c>_gate</c>.</summary>
        private TimeSpan NextConsentBackoffWaitLocked()
        {
            _consentBackoffAttempt++;
            var jitterSource = ConsentBackoffJitterForTesting;
            var jitter = jitterSource != null ? jitterSource() : _consentBackoffJitter.NextDouble();

            // Same contract shape as the publish backoff: attempt 1 retries
            // free at the next dispatch point; attempt n>=2 takes full jitter
            // in [1s, min(2^(n-2), 60)s].
            return PublishBackoffWait(_consentBackoffAttempt, jitter);
        }

        /// <summary>
        /// The publish deferral a grant-receipt gate refusal takes: never a spin — the
        /// worker sleeps until the earliest receipt-progress signal (every
        /// settle path wakes it) or the consent plane's own retry deadline;
        /// with no open window (e.g. a Mode-B chain still resolving its
        /// bearer) a short floor keeps the re-check cheap while wakes drive
        /// the real progress. This deadline is what prevents the full-batch
        /// publish branch (and the WebGL coroutine, which would otherwise
        /// re-enter the gated publish every iteration without yielding) from
        /// busy-looping against a parked receipt.
        /// </summary>
        private DateTime ConsentGateBackoffUntil()
        {
            DateTime deadline;
            lock (_gate)
            {
                deadline = _consentRetryNotBefore;
            }

            var now = DateTime.UtcNow;
            return deadline > now ? deadline : now + TimeSpan.FromSeconds(1);
        }

        /// <summary>
        /// Caps a worker wait to the consent plane's retry deadline, so a
        /// short server Retry-After (the strict-consent lane answers 503 +
        /// Retry-After: 5) cannot sit out a long flush interval before the
        /// parked head re-dispatches: the worker wakes when the window
        /// elapses and its top-of-loop dispatch point re-attempts. Waits are
        /// only capped while a retained head is actually waiting on the
        /// window (an in-flight head settles via its own callback wake; an
        /// empty outbox needs no cadence).
        /// </summary>
        private TimeSpan CapWaitToConsentRetryDeadline(TimeSpan wait)
        {
            if (_httpSender == null)
            {
                return wait;
            }

            DateTime deadline;
            bool headWaiting;
            lock (_gate)
            {
                // Parked receipts wait for a credential context, not a
                // worker cadence: an all-parked outbox caps nothing.
                headWaiting = _consentInFlightKey == null &&
                              _consentOutbox.OwnScope.FirstDispatchableEntry(IsConsentReceiptParked) != null;
                deadline = _consentRetryNotBefore;
                // The LATER of the two, because the worker's dispatch predicate
                // requires BOTH to have elapsed. Waking
                // at the earlier one only to stand down again would spin the
                // 250ms floor four times a second until the later one came due —
                // a retryable HTTP deferral followed by token_unavailable, or the
                // reverse, is enough to reach it. A deadline already in the past
                // is dominated by a future one under max, so a stale one cannot
                // drag the wait down either.
                if (_consentTokenRetryWakeAt != DateTime.MinValue &&
                    (deadline == DateTime.MinValue || _consentTokenRetryWakeAt > deadline))
                {
                    deadline = _consentTokenRetryWakeAt;
                }
            }

            if (!headWaiting || deadline == DateTime.MinValue)
            {
                return wait;
            }

            var untilDue = deadline - DateTime.UtcNow;
            if (untilDue <= TimeSpan.Zero)
            {
                // The window elapsed BETWEEN this iteration's dispatch point
                // (which saw it still open and stood down) and this wait
                // computation: sleeping the full interval now would strand
                // the due head until an unrelated wake. A short floor lets
                // the next cycle's dispatch point pick it up, without the
                // tight loop a zero wait could produce while a bearer is
                // unavailable.
                var floor = TimeSpan.FromMilliseconds(250);
                return floor < wait ? floor : wait;
            }

            return untilDue < wait ? untilDue : wait;
        }

        /// <summary>
        /// Resolves the Bearer for a consent receipt and hands it to
        /// <paramref name="onResolved"/> — PER-RECEIPT (most-vouching):
        /// <paramref name="usePublishableKey"/> selects the static sp_ key
        /// (invoked inline) or the Mode-B JWT (async token fetch; null on
        /// failure — the receipt stays retained). The fetch uses the
        /// configured HTTP timeout so it can never hang the receipt. A JWT
        /// request with no provider resolves null (defensive — parking
        /// keeps verified heads undispatchable in that configuration, so
        /// the selection never lawfully asks for it): a verified receipt
        /// must NEVER fall back to the publishable key, which would
        /// misrecord the decision under the caller's anon scope.
        /// </summary>
        private void ResolveBearerForConsent(bool usePublishableKey, Action<string> onResolved)
        {
            if (usePublishableKey)
            {
                onResolved(_config.PublishableKey);
                return;
            }

            if (_tokenManager == null)
            {
                onResolved(null);
                return;
            }

            var cts = new CancellationTokenSource(_config.HttpTimeout);
            _tokenManager.GetTokenAsync(cts.Token).ContinueWith(task =>
            {
                cts.Dispose();
                if (task.IsFaulted || task.IsCanceled)
                {
                    // Observe the fault so a failed/timed-out token fetch does not
                    // later surface via TaskScheduler.UnobservedTaskException (the
                    // token manager observes its background-refresh faults the same
                    // way). Harmless for a canceled task — Exception is null there.
                    _ = task.Exception;
                    onResolved(null);
                    return;
                }

                onResolved(task.Result);
            }, TaskScheduler.Default);
        }

        internal ShardPilotConsentOutbox ConsentOutboxForTesting
        {
            get { return _consentOutbox; }
        }

        internal string ConsentInFlightKeyForTesting
        {
            get
            {
                lock (_gate)
                {
                    return _consentInFlightKey;
                }
            }
        }

        internal DateTime ConsentRetryNotBeforeForTesting
        {
            get
            {
                lock (_gate)
                {
                    return _consentRetryNotBefore;
                }
            }
        }

        internal ShardPilotRemoteConfig CreateRemoteConfig(string cachePath, IShardPilotHttpRequestSender senderOverride)
        {
            if (IntakeClosed || !_config.RemoteConfigEnabled || string.IsNullOrEmpty(_config.AppUrl))
            {
                return null;
            }

            var sender = senderOverride ?? _httpSender;
            if (sender == null)
            {
                return null;
            }

            // Identifiers and optional attributes require a current grant.
            // The epoch also catches a denial followed by a new grant while
            // the request is queued on the main-thread dispatcher.
            return new ShardPilotRemoteConfig(
                _config,
                cachePath,
                sender,
                () => ConsentState,
                () => Interlocked.Read(ref _consentDenialEpoch), intakeClosed: () => IntakeClosed, hostCurrent: () => FetchOwnerCurrent);
        }

        public IReadOnlyList<ShardPilotBreadcrumb> Breadcrumbs()
        {
            return _breadcrumbs.Snapshot();
        }

        /// <summary>
        /// Persists the pending in-memory remnant — the queued events plus the
        /// worker-held batch — to the durable offline spool, so it survives
        /// the app being killed and is re-sent on the next launch. Invoked
        /// automatically when the application pauses or quits and by the final
        /// close drain; also safe to call manually from any thread. The
        /// in-memory queue is NOT cleared: normal delivery continues if the
        /// app resumes, and the eventual server acknowledgment removes the
        /// spooled copies (entries are de-duplicated by event id). A no-op
        /// when the spool is disabled/unavailable or consent is denied. Never
        /// throws.
        /// </summary>
        public void PersistPendingEvents()
        {
            PersistPendingEventsCore();
        }

        /// <summary>What a persist barrier left of the undelivered remnant.</summary>
        private enum PersistOutcome
        {
            /// <summary>Nothing was left to write.</summary>
            Nothing,

            /// <summary>The remnant is on disk for the next send.</summary>
            Durable,

            /// <summary>Events are left that storage did not take: the write
            /// failed, or there is no spool (disabled, or WebGL).</summary>
            Failed
        }

        private PersistOutcome PersistPendingEventsCore()
        {
            RetryEventSpoolDebts();
            // The consent outbox's owed-write retry rides every persist
            // barrier — INDEPENDENT of event spooling: it runs even when the
            // spool is disabled or the platform has none (the outbox is a
            // consent-plane store, not an analytics buffer).
            RetryOwedConsentWriteIfDirty();

            // The standing denial's owed witness writes ride the same
            // barriers (pause, quit, close snapshot): these are exactly the
            // moments before a potential process exit, which is when an
            // unwitnessed denial is most exposed.
            RetryOwedDenialWitnessWrites();

            // Purge debt survives the denial result too. Retry at the same
            // persistence barriers after storage recovers, without granting
            // consent or replacing the original denial decision.
            bool purgeOwed;
            lock (_gate) purgeOwed = ConsentPurgeOwed;
            if (purgeOwed && Monitor.TryEnter(ShardPilotDispatcher.PrivacyDecisionGate))
            {
                try { RetryConsentPurge(); }
                finally { Monitor.Exit(ShardPilotDispatcher.PrivacyDecisionGate); }
            }


            if (_experiments != null)
            {
                // Housekeeping may admit an already sealed fact before this
                // snapshot, but never sends apply HTTP or journals raw applications.
                // Pending applications are reported as unpersisted below.
                try
                {
                    _experimentFactDrainWindow = true;
                    _experiments.ShutdownHousekeeping();
                }
                finally
                {
                    _experimentFactDrainWindow = false;
                }
                if (_experiments.HasUnadmittedApplications) _stats.RecordFailure("experiment_applications_not_persisted");
            }

            if (_spool == null)
            {
                return SpoolUnavailableOutcome();
            }

            var successor = _spoolSuccessor;
            if (successor != null && _spool.IsDisowned)
            {
                // Displaced client: the file belongs to the successor now,
                // so a local append would be a no-op — hand the would-be
                // snapshot over instead, keeping late-accepted events (a
                // caller holding this old reference) durable. The successor
                // appends them through ITS spool, with its own consent gates
                // and the usual id de-duplication.
                List<ShardPilotEvent> handoff = null;
                lock (_gate)
                {
                    // Consent-first: only a granted client may hand events
                    // toward durable storage (non-granted states hold no
                    // events anyway — structural insurance).
                    if (_consentState != ShardPilotConsentState.Granted)
                    {
                        return PersistOutcome.Nothing;
                    }

                    var includeHeld = _workerConsentEpoch.Value == _consentDenialEpoch && _workerBatch.Count > 0;
                    var total = (includeHeld ? _workerBatch.Count : 0) + _queue.Count;
                    if (total == 0)
                    {
                        return PersistOutcome.Nothing;
                    }

                    handoff = new List<ShardPilotEvent>(total);
                    if (includeHeld)
                    {
                        AddUncondemnedLocked(handoff, _workerBatch);
                    }

                    AddUncondemnedLocked(handoff, _queue);

                    // Counted like a local append: the successor writes this
                    // snapshot to the SHARED spool file, so a sentinel purge
                    // firing in the snapshot-to-append window must defer its
                    // marker retirement past it too.
                    _spoolAppendsInFlight++;

                    // And counted on the SUCCESSOR — the client that owns
                    // the spool and whose purge/marker-debt machinery
                    // decides retirement in the reinit window: its counter
                    // rises before this snapshot gate releases (nested
                    // predecessor-gate → successor-gate, the sanctioned
                    // handoff direction), so no successor-side pass can
                    // read zero between this snapshot and the append.
                    successor.BeginHandoffSpoolAppend();
                }

                try
                {
                    return successor.PersistHandoffEntries(handoff);
                }
                finally
                {
                    // The successor's reconcile runs FIRST: the append
                    // landed in ITS spool, and its confirmed removal is the
                    // one that may retire a deferred marker.
                    successor.EndHandoffSpoolAppend();
                    lock (_gate)
                    {
                        _spoolAppendsInFlight--;
                    }

                    RetryCondemnedFactPurgeDebt();
                }
            }

            // The sentinel purge debt (a marker save or spool removal that
            // could not land at purge time) retries at every persist
            // barrier, BEFORE the snapshot below can be assembled.
            RetryCondemnedFactPurgeDebt();

            // One persist at a time: the settled-during-persist record is a
            // single shared slot on the client, so concurrent calls (an
            // application pause landing during a close, say) serialize here
            // instead of clearing each other's record mid-flight.
            lock (_persistGate)
            {
                List<ShardPilotEvent> pending;
                long epochBefore;
                lock (_gate)
                {
                    // Consent-first: nothing is ever persisted to the offline
                    // spool outside an explicit grant. (Non-granted states
                    // hold no pending events — the enqueue gate refuses them
                    // — so this is structural insurance.)
                    if (_consentState != ShardPilotConsentState.Granted)
                    {
                        return PersistOutcome.Nothing;
                    }

                    epochBefore = _consentDenialEpoch;

                    // The held batch is only included while the worker's observed
                    // denial epoch is current: after a denial the held batch is
                    // condemned (the worker discards it on its next cycle) and
                    // must not be revived through the spool — the denial already
                    // wiped it. Events still in _restoredPending are skipped:
                    // they are already in the spool file.
                    var includeHeld = _workerConsentEpoch.Value == _consentDenialEpoch && _workerBatch.Count > 0;
                    var total = (includeHeld ? _workerBatch.Count : 0) + _queue.Count;
                    if (total == 0)
                    {
                        return PersistOutcome.Nothing;
                    }

                    pending = new List<ShardPilotEvent>(total);
                    if (includeHeld)
                    {
                        // Held-batch events were drained first: keep them ahead of
                        // the queue so spool FIFO eviction preserves arrival order.
                        AddUncondemnedLocked(pending, _workerBatch);
                    }

                    AddUncondemnedLocked(pending, _queue);

                    // Arm the settlement recorder for the duration of the
                    // unlocked append below: any part of this snapshot — the
                    // held batch, or queued events the worker drains and
                    // delivers meanwhile — can settle while the append is in
                    // flight, and that settlement's own spool removal may run
                    // BEFORE the append has put the entries on disk. The
                    // appends-in-flight count rises in the SAME hold as the
                    // snapshot, so a sentinel purge landing anywhere in the
                    // snapshot-to-append window sees it and defers its
                    // marker retirement.
                    _persistInFlight = true;
                    _settledDuringPersist = null;
                    _spoolAppendsInFlight++;
                }

                bool denialLanded;
                var written = false;
                List<ShardPilotEvent> settledMeanwhile;
                try
                {
                    // Test seam: lets a test land a SetConsent(false) or a
                    // publish settlement inside the snapshot-to-append window
                    // below. No-op in production.
                    var preAppendHook = SpoolPreAppendHookForTesting;
                    if (preAppendHook != null)
                    {
                        preAppendHook();
                    }

                    // Serialize and write OUTSIDE _gate so file I/O never stalls a
                    // Track caller. Events are immutable once enqueued, and Append
                    // de-duplicates by event id (an event may already be spooled by an
                    // earlier failed-publish append).
                    ShardPilotSpool.AppendUndoToken persistToken;
                    written = AppendToActiveEventSpool(pending, false, out persistToken);
                    PruneOverflowAccountingForEvictions(persistToken);
                }
                finally
                {
                    lock (_gate)
                    {
                        denialLanded = _consentDenialEpoch != epochBefore;
                        settledMeanwhile = _settledDuringPersist;
                        _settledDuringPersist = null;
                        _persistInFlight = false;
                        _spoolAppendsInFlight--;
                    }

                    // The landed append re-runs the marker-driven removal:
                    // a sentinel purge that fired inside the append window
                    // deferred its marker retirement to this pass, whose
                    // removal now covers the entries this append just wrote
                    // — only a confirmed post-append removal retires the
                    // marker. A clean marker makes this a no-op.
                    RetryCondemnedFactPurgeDebt();
                }

                // The append ran unlocked, so reconcile what raced it (removal
                // is idempotent and keyed by event id, so it precisely undoes
                // this append and nothing else):
                if (denialLanded)
                {
                    // A consent denial: its wipe may have run BEFORE the
                    // append landed, which would leave just-denied events
                    // back on disk. Remove everything this call appended,
                    // failing closed if the removal cannot be confirmed.
                    RemoveDeniedOrFailClosed(pending);
                    return PersistOutcome.Nothing;
                }

                if (settledMeanwhile != null)
                {
                    // A batch settled during the append (2xx delivery or a
                    // permanent rejection — of the held batch, or of queued
                    // snapshot events the worker drained and published in the
                    // meantime): its own removal may have no-oped because the
                    // entries were not on disk yet. Remove exactly
                    // (snapshot ∩ settled-during-append) — a blanket removal
                    // would also delete the insurance copies of the STILL
                    // PENDING snapshot events.
                    var snapshotIds = new HashSet<string>(StringComparer.Ordinal);
                    for (var i = 0; i < pending.Count; i++)
                    {
                        if (!string.IsNullOrEmpty(pending[i].Id))
                        {
                            snapshotIds.Add(pending[i].Id);
                        }
                    }

                    List<ShardPilotEvent> settledFromSnapshot = null;
                    for (var i = 0; i < settledMeanwhile.Count; i++)
                    {
                        var settled = settledMeanwhile[i];
                        if (settled != null && !string.IsNullOrEmpty(settled.Id) && snapshotIds.Contains(settled.Id))
                        {
                            if (settledFromSnapshot == null)
                            {
                                settledFromSnapshot = new List<ShardPilotEvent>();
                            }

                            settledFromSnapshot.Add(settled);
                        }
                    }

                    if (settledFromSnapshot != null)
                    {
                        SpoolRemove(settledFromSnapshot);
                    }
                }

                return written ? PersistOutcome.Durable : PersistOutcome.Failed;
            }
        }

        /// <summary>
        /// The outcome when there is no spool to write to: a granted client
        /// with events left loses them at exit. Caller does not hold _gate.
        /// </summary>
        private PersistOutcome SpoolUnavailableOutcome()
        {
            lock (_gate)
            {
                var heldCounts = _workerConsentEpoch.Value == _consentDenialEpoch && _workerBatch.Count > 0;
                return _consentState == ShardPilotConsentState.Granted && (_queue.Count > 0 || heldCounts)
                    ? PersistOutcome.Failed
                    : PersistOutcome.Nothing;
            }
        }

        private bool AppendToActiveEventSpool(List<ShardPilotEvent> batch, bool provisional,
            out ShardPilotSpool.AppendUndoToken token, bool handoff = false)
        {
            return AppendToActiveEventSpoolOutcome(batch, provisional, out token, handoff) == PersistOutcome.Durable;
        }

        private PersistOutcome AppendToActiveEventSpoolOutcome(List<ShardPilotEvent> batch, bool provisional,
            out ShardPilotSpool.AppendUndoToken token, bool handoff = false)
        {
            ShardPilotSpool.AppendUndoToken staged = null;
            var result = WithActiveEventSpool(owner =>
            {
                ShardPilotSpool.AppendUndoToken next;
                PersistOutcome outcome;
                if (handoff || owner != this)
                {
                    if (owner != this) owner.BeginHandoffSpoolAppend();
                    try { outcome = owner.AppendHandoffEntriesOwned(batch, provisional, out next); }
                    finally { if (owner != this) owner.EndHandoffSpoolAppend(); }
                }
                else
                {
                    var appended = provisional ? owner._spool.AppendProvisional(owner._config, batch, out next)
                        : owner._spool.Append(owner._config, batch, out next);
                    if (!provisional && !owner._spool.FlushPendingWritesDurably()) appended = false;
                    outcome = appended ? PersistOutcome.Durable : PersistOutcome.Failed;
                }
                // A transfer may reload the first append before a repeated
                // append sees it. Preserve that original staged-ID receipt
                // even when the successor correctly reports a duplicate.
                if (next != null)
                {
                    if (staged == null) staged = next;
                    else
                    {
                        if (next.StagedIds != null)
                        {
                            if (staged.StagedIds == null) staged.StagedIds = new List<string>();
                            staged.StagedIds.AddRange(next.StagedIds);
                        }
                        if (next.EvictedEntries != null)
                        {
                            if (staged.EvictedEntries == null) staged.EvictedEntries = new List<ShardPilotSpool.SpoolEntry>();
                            staged.EvictedEntries.AddRange(next.EvictedEntries);
                        }
                    }
                }
                return outcome;
            }, PersistOutcome.Failed, PersistOutcome.Failed);
            token = staged;
            return result;
        }

        private void SpoolAppend(List<ShardPilotEvent> batch)
        {
            if (_spool != null && batch.Count > 0)
            {
                ShardPilotSpool.AppendUndoToken token;
                AppendToActiveEventSpool(batch, false, out token);
                PruneOverflowAccountingForEvictions(token);
            }
        }

        /// <summary>
        /// Records an accepted disk-only overflow event id and keeps the
        /// accounting bounded: stale queue heads (ids already pruned) are
        /// drained, the set is clamped to the spool cap oldest-first, and the
        /// order queue is compacted when lazy deletion lets it outgrow twice
        /// the cap. Caller must hold <c>_gate</c>.
        /// </summary>
        private void AddOverflowAccountingLocked(string id)
        {
            if (string.IsNullOrEmpty(id) || !_spooledOverflowIds.Add(id))
            {
                return;
            }

            _spooledOverflowOrder.Enqueue(id);

            // Drain heads that were pruned by eviction/settlement (lazy
            // deletion keeps prunes O(1)).
            while (_spooledOverflowOrder.Count > 0 && !_spooledOverflowIds.Contains(_spooledOverflowOrder.Peek()))
            {
                _spooledOverflowOrder.Dequeue();
            }

            // Belt-and-braces clamp (accounting-only approximation): the set
            // never exceeds the spool's own cap; the file cannot hold more
            // entries than that either.
            while (_spooledOverflowIds.Count > _config.SpoolMaxEvents && _spooledOverflowOrder.Count > 0)
            {
                _spooledOverflowIds.Remove(_spooledOverflowOrder.Dequeue());
            }

            // Compact a queue that lazy deletion let outgrow the cap.
            if (_spooledOverflowOrder.Count > (_config.SpoolMaxEvents * 2) + 64)
            {
                var live = new List<string>(_spooledOverflowIds.Count);
                foreach (var queued in _spooledOverflowOrder)
                {
                    if (_spooledOverflowIds.Contains(queued))
                    {
                        live.Add(queued);
                    }
                }

                _spooledOverflowOrder.Clear();
                for (var i = 0; i < live.Count; i++)
                {
                    _spooledOverflowOrder.Enqueue(live[i]);
                }
            }
        }

        /// <summary>
        /// Prunes overflow-accounting ids whose entries a spool append just
        /// FIFO-evicted from the file (reported through the append's undo
        /// token) — those events are gone from disk, so a later denial must
        /// not count them dropped again. A restore of evicted entries by an
        /// undone provisional append is not re-added: accounting-only
        /// approximation, always erring toward the smaller count.
        /// </summary>
        private void PruneOverflowAccountingForEvictions(ShardPilotSpool.AppendUndoToken token)
        {
            var evicted = token == null ? null : token.EvictedEntries;
            if (evicted == null || evicted.Count == 0)
            {
                return;
            }

            lock (_gate)
            {
                if (_spooledOverflowIds.Count == 0)
                {
                    return;
                }

                for (var i = 0; i < evicted.Count; i++)
                {
                    if (evicted[i] != null)
                    {
                        _spooledOverflowIds.Remove(evicted[i].Id);
                    }
                }
            }
        }

        private bool SpoolRemove(List<ShardPilotEvent> batch)
        {
            if (_spool == null || batch.Count == 0) return true;
            lock (_gate)
                foreach (var ev in batch)
                {
                    _eventSpoolRemovalDebt[ev.Id] = ev;
                    _analyticsRollbackDebt.Remove(ev.Id);
                }
            var removed = WithActiveEventSpool(owner => owner._spool.Remove(batch));
            var successor = _spoolSuccessor;
            if (!removed && successor != null) removed = successor.SpoolRemove(batch);
            if (removed)
                lock (_gate)
                    foreach (var ev in batch)
                    {
                        ShardPilotEvent pending;
                        if (_eventSpoolRemovalDebt.TryGetValue(ev.Id, out pending) && pending == ev)
                            _eventSpoolRemovalDebt.Remove(ev.Id);
                    }
            return removed;
        }

        /// <summary>
        /// Denial-recheck removal: undoes a spool append that raced a
        /// consent denial and, when the removal cannot be CONFIRMED on disk,
        /// escalates to the fail-closed consent wipe — truthful delete, and
        /// on failure the path-scoped "wipe owed" marker plus the instance
        /// tombstone — so denied bytes can never outlive the denial on disk
        /// unnoticed. The escalation may also destroy entries appended after
        /// a rapid re-grant inside the same race window; a consent race
        /// resolves toward deletion by design.
        /// </summary>
        private void RemoveDeniedOrFailClosed(List<ShardPilotEvent> events)
        {
            if (_spool == null || events.Count == 0)
            {
                return;
            }

            WithActiveEventSpool(owner =>
            {
                if (!owner._spool.Remove(events)) owner._spool.Wipe();
                return true;
            });
        }

        internal string CondemnedFactsKeyForTesting
        {
            get { return CondemnedFactsKey; }
        }
        internal string WithdrawalJournalPathForTesting
        { get { return ResolveExperimentsCachePath() + ".withdrawals." + ShardPilotExperimentsLogic.CredentialFingerprint(ShardPilotWithdrawalWork.StoreFor(_config)); } }
        internal WithdrawalFrame[] WithdrawalFramesForTesting
        { get { bool known; var frames = ReadWithdrawalFrames(out known); return known ? frames : null; } }
        internal void CopyWithdrawalJournalForTesting(string target)
        {
            bool absent;
            using (var work = BorrowWithdrawalWork(false, out absent))
            { if (work != null) System.IO.File.WriteAllBytes(target, work.CopyBytesForTesting()); }
        }

        internal string CurrentSessionIdForTesting
        {
            get
            {
                lock (_gate)
                {
                    return _sessionState.Id;
                }
            }
        }

        internal int PendingQueueCountForTesting
        {
            get
            {
                lock (_gate)
                {
                    return _queue.Count;
                }
            }
        }


        /// <summary>
        /// The absolute path of this client's spool file, or null when the
        /// spool is disabled/unavailable. The lifecycle registry uses it to
        /// enforce a single registered owner per spool scope; tests use it to
        /// inspect the file.
        /// </summary>
        internal string SpoolFilePath
        {
            get { return _spool == null ? null : _spool.FilePath; }
        }

        /// <summary>
        /// The absolute path of this client's crash spool file, or null when
        /// the crash spool is disabled/unavailable. The lifecycle registry
        /// matches it (alongside the event spool path) to enforce a single
        /// registered owner per spool scope even when only one of the two
        /// spools is enabled.
        /// </summary>
        internal string CrashSpoolFilePath
        {
            get { return _crashSpool == null ? null : _crashSpool.FilePath; }
        }

        /// <summary>
        /// The experiments consumer's durable-cache path, or null when
        /// experiments are dark or the consumer runs memory-only. The
        /// lifecycle registry matches it alongside the spool files: a
        /// replacement client with experiments enabled but BOTH spools
        /// disabled/unavailable shares only this file with its predecessor
        /// — without the match, the displaced client's automatic lane
        /// would stay live over the cache the successor owns and its
        /// parked destructive debts would die unadopted.
        /// </summary>
        internal string ExperimentsCachePath
        {
            get
            {
                var experiments = _experiments;
                return experiments == null ? null : experiments.CachePathForHandoff;
            }
        }

        /// <summary>
        /// The experiment cache path this client's CONFIG resolves to,
        /// independent of the experiments flag — null only when no
        /// directory is resolvable. MATCH-ONLY: the lifecycle registry
        /// compares a REGISTERING client's config-resolved path against a
        /// registered predecessor's in-use cache path, so a flag-off (or
        /// spool-less) successor over the same cache still displaces the
        /// enabled predecessor — whose automatic lane must stop and whose
        /// parked destructive debts must tombstone — while a dark client
        /// itself still constructs no consumer and touches no cache.
        /// </summary>
        internal string ExperimentsCacheScopePath
        {
            get { return ResolveExperimentsCachePath(); }
        }

        internal string ExperimentsCacheOwnerKey { get { return _experiments?.CacheOwnerKey; } }
        internal string ExperimentsCacheStore { get { return ShardPilotWithdrawalWork.StoreFor(_config); } }
        internal void BindExperimentsCacheIdentity() { _experiments?.BindCacheIdentity(); }

        /// <summary>
        /// The experiment TENANT identity this client's CONFIG resolves to:
        /// the cache-scope components minus the subject — workspace, app,
        /// environment, credential fingerprint, base URL — exactly the
        /// components <see cref="ShardPilotExperimentsLogic.BuildScope"/>
        /// composes. CONFIG-derived, so it exists for dark
        /// (experiments-off) clients too. The lifecycle registry requires
        /// it to MATCH alongside the cache path: the cache file name is
        /// fixed per directory, so two unrelated tenants in one
        /// persistentDataPath share the path string while owning disjoint
        /// experiment state — a path-only match would let an unrelated
        /// (possibly dark) registration disown an enabled client's
        /// experiments lane and park debts no successor adopts.
        /// </summary>
        internal string ExperimentsScopeIdentity
        {
            get { return ExperimentsScopeIdentityFor(_config); }
        }

        /// <summary>Static flavor of <see cref="ExperimentsScopeIdentity"/>
        /// for the PRE-CONSTRUCTION handoff, where no instance exists yet:
        /// derives the tenant identity from a (normalized) config so the
        /// cache-scope match sees the same values the construction will
        /// use.</summary>
        private static string ExperimentsScopeIdentityFor(ShardPilotConfig config)
        {
            return ShardPilotExperimentsLogic.BuildScope(
                config.WorkspaceKey,
                config.AppKey,
                config.EnvironmentKey,
                null,
                ShardPilotExperimentsLogic.CredentialFingerprint(config.PublishableKey),
                config.AppUrl);
        }

        /// <summary>
        /// The tenant identity MINUS the remote-config base URL, for the
        /// registration compare's DARK-successor leg: a direct replacement
        /// that disables experiments and also clears AppUrl
        /// still owns the same tenant and cache path, but its full identity
        /// carries an EMPTY URL where the enabled predecessor's carries the
        /// old server URL — a full-identity compare would never match,
        /// no handoff path would run, and the displaced consumer would keep
        /// revalidating and rewriting the cache. A dark successor has no
        /// experiments config to contradict the predecessor's, so same
        /// workspace/app/environment/credential (plus the cache-path match)
        /// suffices; an ENABLED successor keeps the full-identity match,
        /// where a different URL is genuinely a different server
        /// (and an enabled config cannot clear the URL — validation
        /// requires it).
        /// </summary>
        internal string ExperimentsScopeIdentitySansBaseUrl
        {
            get { return ExperimentsScopeIdentitySansBaseUrlFor(_config); }
        }

        /// <summary>Static flavor of
        /// <see cref="ExperimentsScopeIdentitySansBaseUrl"/> for the
        /// PRE-CONSTRUCTION handoff: the relaxed dark-successor legs must
        /// apply at BOTH match sites — the registration compare AND the
        /// pre-construction handoff — or a dark direct replacement sharing
        /// only the cache is matched at registration but not before it,
        /// reopening the construction window a held old-lane revalidation
        /// can write into.</summary>
        private static string ExperimentsScopeIdentitySansBaseUrlFor(ShardPilotConfig config)
        {
            return ShardPilotExperimentsLogic.BuildScope(
                config.WorkspaceKey,
                config.AppKey,
                config.EnvironmentKey,
                null,
                ShardPilotExperimentsLogic.CredentialFingerprint(config.PublishableKey),
                null);
        }

        /// <summary>
        /// The tenant identity MINUS the base URL AND the credential
        /// fingerprint — the bare workspace/app/environment triple — for
        /// the registration compare's TOKEN-ONLY dark-successor leg: a
        /// Mode-B replacement that disables experiments/remote-config and
        /// also removes the publishable PublishableKey (valid — TokenProvider
        /// satisfies ingest auth, and only an ENABLED experiments config
        /// requires the key and URL) has an EMPTY fingerprint where the
        /// enabled predecessor's carries the old sp_ key's, so even the
        /// URL-less identity never matches and no handoff path runs. Such
        /// a successor's experiments-plane credentials are WHOLLY absent —
        /// it carries no experiments identity to contradict ANY
        /// predecessor's — so the same tenant triple plus the cache-path
        /// match suffices to displace the lane. The narrowing is safe
        /// because the credential fingerprint exists to separate TENANTS
        /// under an in-place key swap: a successor with no key at all
        /// asserts no tenant-credential claim, while the unrelated-tenant
        /// protection still holds through workspace/app/environment. Dark
        /// successors that STILL carry a key keep the credential-checked
        /// URL-less compare above; enabled successors keep the full
        /// identity.
        /// </summary>
        internal string ExperimentsScopeIdentitySansCredentials
        {
            get { return ExperimentsScopeIdentitySansCredentialsFor(_config); }
        }

        /// <summary>Static flavor of
        /// <see cref="ExperimentsScopeIdentitySansCredentials"/> for the
        /// PRE-CONSTRUCTION handoff (see the sans-base-URL static flavor's
        /// note: both match sites carry the same relaxed legs).</summary>
        private static string ExperimentsScopeIdentitySansCredentialsFor(ShardPilotConfig config)
        {
            return ShardPilotExperimentsLogic.BuildScope(
                config.WorkspaceKey,
                config.AppKey,
                config.EnvironmentKey,
                null,
                null,
                null);
        }

        /// <summary>Whether this client's CONFIG enables experiments: the
        /// lifecycle registration needs the successor's flag to decide
        /// whether the relaxed (URL-less) tenant match above applies —
        /// only a DARK successor may ignore the base-URL component.</summary>
        internal bool ExperimentsConfigEnabled
        {
            get { return _config.ExperimentsEnabled; }
        }

        /// <summary>Whether this client's CONFIG carries NO experiments-
        /// plane credentials at all — no publishable PublishableKey and no
        /// AppUrl (a token-only Mode-B config): the
        /// registration compare relaxes the dark-successor match to the
        /// bare tenant triple ONLY for such a successor.</summary>
        internal bool ExperimentsConfigCredentialless
        {
            get { return ExperimentsConfigCredentiallessFor(_config); }
        }

        /// <summary>Static flavor of
        /// <see cref="ExperimentsConfigCredentialless"/> for the
        /// PRE-CONSTRUCTION handoff.</summary>
        private static bool ExperimentsConfigCredentiallessFor(ShardPilotConfig config)
        {
            return string.IsNullOrEmpty(config.PublishableKey) &&
                   string.IsNullOrEmpty(config.AppUrl);
        }

        /// <summary>
        /// Releases this client's claim on its spool files (the event spool
        /// AND the crash spool): every later spool write becomes a no-op.
        /// Called by the lifecycle registry when a NEWER client for the same
        /// (workspace, app, environment) scope registers — the newest client
        /// owns the files, and this client (typically about to be closed and
        /// replaced) must not rewrite them from a stale in-memory view.
        /// In-memory delivery is unaffected; a crash captured through this
        /// client afterwards is handed to the successor for persistence.
        /// Callers must have installed the successor link FIRST (see
        /// <see cref="SetSpoolSuccessor"/>): a disowned crash spool with no
        /// successor would silently drop the durability of a report captured
        /// in that window — crash reports, unlike events, have no in-memory
        /// remnant a later handoff could recover.
        /// </summary>
        internal void DisownSpool()
        {
            if (_spool != null)
            {
                _spool.Disown();
            }

            if (_crashSpool != null)
            {
                _crashSpool.Disown();
            }
        }

        /// <summary>
        /// Releases ONLY the event spool's claim, for the pre-construction
        /// ownership handoff — which runs BEFORE the successor client exists,
        /// so no successor link can be installed yet. The event spool must be
        /// frozen at that point (its remnant was just persisted, and a stale
        /// rewrite would clobber the file the successor is about to load),
        /// but the CRASH spool deliberately stays owned until the successor
        /// registers: a crash captured through this client inside the
        /// construction gap must still reach disk — a crash report has no
        /// in-memory remnant, so durability dropped here would be gone for
        /// good. The successor's construction-time load may miss such a
        /// late-landing record; it is still delivered by this client's live
        /// send, or by a later launch's load, de-duplicated by crash_id.
        /// </summary>
        internal void DisownEventSpoolForHandoff()
        {
            if (_spool != null)
            {
                _spool.Disown();
            }
        }

        /// <summary>
        /// The client that took over this client's spool file (set by the
        /// lifecycle registry at replacement). Once the own spool is
        /// disowned, the persist paths hand their would-be snapshot to the
        /// successor instead, so an event accepted through a still-held old
        /// reference stays durable. Volatile: set on the registering thread,
        /// read from persist paths on any thread.
        /// </summary>
        private volatile ShardPilotClient _spoolSuccessor;

        internal void SetSpoolSuccessor(ShardPilotClient successor)
        {
            _spoolSuccessor = successor;
            RetryEventSpoolDebts();
        }

        /// <summary>Counts a displaced predecessor's handoff append on THIS
        /// client — the one that owns the spool and appends the entries.
        /// The predecessor calls this INSIDE its own snapshot gate hold
        /// (nested predecessor-gate → successor-gate, the same strictly
        /// predecessor→successor direction every handoff call takes), so a
        /// sentinel purge or marker-debt retry running on this client
        /// anywhere in the snapshot-to-append window sees the in-flight
        /// append and defers the condemned-facts marker's retirement — the
        /// predecessor's own counter cannot protect a purge that consults
        /// this client's.</summary>
        internal void BeginHandoffSpoolAppend()
        {
            lock (_gate)
            {
                _spoolAppendsInFlight++;
            }
        }

        /// <summary>The handoff append landed (or failed): releases the
        /// count and runs THIS client's marker-driven reconcile, so a
        /// retirement deferred to this append retires only after a
        /// confirmed post-append removal. A clean marker makes the
        /// reconcile a no-op.</summary>
        internal void EndHandoffSpoolAppend()
        {
            lock (_gate)
            {
                _spoolAppendsInFlight--;
            }

            RetryCondemnedFactPurgeDebt();
        }

        /// <summary>
        /// Accepts a displaced predecessor's pending-envelope snapshot into
        /// THIS client's spool (id-deduplicated as usual, with this client's
        /// own consent gates). Disk-only: the entries deliver on the next
        /// launch through the normal restore path, or earlier via the
        /// predecessor's own close drain (whose duplicates the ingest service
        /// drops). Forwards down the chain when this client was itself
        /// displaced meanwhile.
        /// </summary>
        internal bool AcceptHandoffEntries(List<ShardPilotEvent> entries)
        {
            return PersistHandoffEntries(entries) == PersistOutcome.Durable;
        }

        private PersistOutcome PersistHandoffEntries(List<ShardPilotEvent> entries)
        {
            ShardPilotSpool.AppendUndoToken token;
            var outcome = AppendToActiveEventSpoolOutcome(entries, false, out token, true);
            PruneOverflowAccountingForEvictions(token);
            return outcome;
        }

        private PersistOutcome AppendHandoffEntriesOwned(List<ShardPilotEvent> entries, bool provisional,
            out ShardPilotSpool.AppendUndoToken token)
        {
            token = null;
            if (entries == null || entries.Count == 0)
            {
                return PersistOutcome.Failed;
            }

            if (_spool == null)
            {
                return PersistOutcome.Failed;
            }

            long epochBefore;
            lock (_gate)
            {
                // Consent-first: a handoff may only land in this spool while
                // THIS client holds an explicit grant — an unknown-state
                // successor (say, its consent read failed) purged its spool
                // at construction and must not re-accumulate unproven events.
                if (_consentState != ShardPilotConsentState.Granted)
                {
                    return PersistOutcome.Nothing;
                }

                epochBefore = _consentDenialEpoch;
            }

            // The entries are a displaced PREDECESSOR's emissions: their
            // experiment-fact ids were pre-registered on the predecessor's
            // condemnation plane, never this client's — a real-subjects
            // sentinel landing HERE between the predecessor's snapshot and
            // this append would find no id to withdraw, this client's
            // purge marker would record nothing, and the append below
            // would write withdrawn facts the post-append reconcile cannot
            // remove (a relaunch would then resend a fact with a withdrawn
            // subject-fact key). Register them on THIS consumer's plane
            // first — the restore path's exact treatment of
            // previous-process emissions, assignment coordinates and all:
            // a condemnation already standing purges them right here, and
            // a sentinel landing anywhere in the append window collects
            // them like any pipeline-resident fact (the deferred marker
            // retirement then owns the removal past the append).
            if (_experiments != null)
            {
                List<ShardPilotRestoredFact> handoffFacts = null;
                for (var i = 0; i < entries.Count; i++)
                {
                    ShardPilotRestoredFact handoffFact;
                    if (TryDescribeRestoredExperimentFact(entries[i], out handoffFact))
                    {
                        if (handoffFacts == null)
                        {
                            handoffFacts = new List<ShardPilotRestoredFact>();
                        }

                        handoffFacts.Add(handoffFact);
                    }
                }

                if (handoffFacts != null)
                {
                    _experiments.RegisterRestoredFacts(handoffFacts);
                }
            }

            // Test seam, same as every other spool append site: lets a test
            // land a sentinel (or a consent flip) inside the
            // snapshot-to-append window. No-op in production.
            var handoffPreAppendHook = SpoolPreAppendHookForTesting;
            if (handoffPreAppendHook != null)
            {
                handoffPreAppendHook();
            }

            // Filtered against THIS client's condemnation state at the
            // last moment before the disk write: an id a sentinel withdrew
            // anywhere in the snapshot-to-append window (discoverable here
            // through the registration above) never reaches the spool at
            // all, and whatever a purge condemned past the bounded
            // in-memory index is still covered by the marker-driven
            // post-append reconcile.
            List<ShardPilotEvent> accepted;
            lock (_gate)
            {
                accepted = new List<ShardPilotEvent>(entries.Count);
                AddUncondemnedLocked(accepted, entries);
            }

            if (accepted.Count == 0)
            {
                return PersistOutcome.Nothing;
            }

            var appended = provisional ? _spool.AppendProvisional(_config, accepted, out token)
                : _spool.Append(_config, accepted, out token);
#if !(UNITY_WEBGL && !UNITY_EDITOR)
            // The publish path, not the game thread at overflow: the
            // bound constrains Track, and a caller here is already on a
            // worker. So this append is made durable before it returns,
            // keeping every promise the synchronous spool made on exactly
            // the paths that were making them.
            if (!provisional && !_spool.FlushPendingWritesDurably()) appended = false;
#endif
            PruneOverflowAccountingForEvictions(token);

            // Same post-append denial recheck as every other spool append.
            bool denialLanded;
            lock (_gate)
            {
                denialLanded = _consentDenialEpoch != epochBefore;
            }

            if (denialLanded)
            {
                RemoveDeniedOrFailClosed(accepted);
                return PersistOutcome.Nothing;
            }
            return appended ? PersistOutcome.Durable : PersistOutcome.Failed;
        }

        /// <summary>
        /// Applies a GLOBALLY decided consent denial to this client (invoked
        /// by the lifecycle registry when another client records the denial):
        /// the same in-memory and on-disk consequences as a local
        /// <see cref="SetConsent"/>(false) — queue, held batch, restored
        /// backlog and overflow accounting dropped; spool wiped — but without
        /// re-persisting the (already written) global decision, re-posting
        /// the receipt, or re-propagating. Idempotent: a no-op when this
        /// client is already denied.
        /// </summary>
        internal void ApplyGlobalConsentDenial(
            string alreadySweptDirectory,
            ShardPilotConsentState deniedState = ShardPilotConsentState.Denied,
            DateTime? decidedAtUtc = null,
            bool deciderRecordPersisted = true,
            bool deciderMarkerDurable = false,
            string deciderMarkerActor = null,
            long deciderDecisionSeq = 0,
            bool freshExplicitDecision = false,
            ShardPilotConsentNoticeSnapshot notice = null)
        {
            lock (_gate)
            {
                if (_consentState == ShardPilotConsentState.DeniedForcedMinor &&
                    deniedState != ShardPilotConsentState.DeniedForcedMinor)
                {
                    return;
                }
            }

            // THIS recipient's witness exposure, judged per actor: the shared durable RECORD witnesses every
            // scope, but the decider's write-ahead MARKER names only the
            // decider's own anonymous id — a recipient under a DIFFERENT
            // id reads that slot at its next boot as a valid FOREIGN
            // marker (kept, inert, floors only), so for that recipient a
            // marker-only outcome is no witness at all and its own receipt
            // (or, failing that, its own heal writes) must carry the
            // denial. Only a live in-session denial (decidedAtUtc carried)
            // judges this — a registration-refresh re-apply of the
            // persisted record IS the record's own evidence.
            var witnessUnbacked = decidedAtUtc.HasValue &&
                                  !deciderRecordPersisted &&
                                  !(deciderMarkerDurable &&
                                    ShardPilotConsentOutbox.IsValidIdentifier(deciderMarkerActor) &&
                                    string.Equals(
                                        deciderMarkerActor, _config.AnonymousId,
                                        StringComparison.Ordinal));
            // A LIVE in-session denial (decidedAtUtc carried from the
            // deciding client) appends this scope's OWN denial receipt, in
            // decision order behind whatever this scope's trail already
            // holds: per-scope receipts mean the decider's deny row cannot
            // end THIS scope's trail, and without one a sibling whose own
            // grant receipt was armed earlier would leave the backend's
            // last word for this scope GRANTED under the new denied state.
            // A denial merely re-applied from the persisted record at a
            // registration refresh passes no decision moment and mints
            // nothing — receipts exist only for explicit in-session
            // decisions. The receipt stamps the DECIDER's
            // original decision moment and the forced-minor reason when
            // that is the propagated flavor.
            var entry = decidedAtUtc.HasValue
                ? CreateConsentReceiptEntry(
                    false,
                    decidedAtUtc.Value,
                    deniedState == ShardPilotConsentState.DeniedForcedMinor
                        ? ShardPilotConsent.ForcedMinorReason
                        : null, notice: notice)
                : null;

            var cleared = 0;
            var alreadyDenied = false;
            lock (_gate)
            {
                alreadyDenied = ShardPilotConsent.IsDeniedState(_consentState);
                ApplyConsentState(deniedState);
                if (decidedAtUtc.HasValue)
                {
                    // A LIVE in-session denial applied to this client (a
                    // propagation or a session-marker catch-up — never a
                    // refresh re-apply of a relaunch-loaded record, which
                    // passes no decision moment): counts toward the decision
                    // generation like the decider's own SetConsent.
                    _consentDecisionGeneration++;
                    if (freshExplicitDecision) _consentOutbox.AuthorizeExplicitDecisionReplacement();
                }

                if (entry != null)
                {
                    // Appended under _gate with the state flip, exactly like
                    // the deciding client's own append.
                    _consentOutbox.AppendToMirror(entry);
                }

                // Trail semantics (defold-canonical D1): the denial does NOT
                // withdraw receipts already appended to this client's outbox
                // — later decisions never cancel earlier receipts; this
                // scope's deny receipt (appended above on a live
                // propagation) follows them in FIFO order, so a stale grant
                // is never the server's last word for this scope. What the
                // denial still clears is the process-wide grant-distribution
                // MARKER, which only governs arming NEW per-scope receipts:
                // a client registering after this denial arms nothing.
                ShardPilotConsent.RecordSessionGrantDecision(null);

                // A propagated denial supersedes an older withheld
                // grant-state completion exactly like a local decision does:
                // the old grant receipt's late settle must not write a
                // Granted state over this denial.
                _withheldGrantStateKey = null;
                _withheldGrantStatePersistOwed = false;

                // And it supersedes any OLDER denial's witness-pending pair
                // on this client: the deciding client (or the persisted
                // record a catch-up re-applied) owns the newer decision's
                // durable witnesses — the witness rule guards the latest
                // decision only. EXCEPT when the decider's durable writes
                // left THIS recipient unwitnessed: then this sibling's own appended receipt may be
                // the denial's only durable evidence — arm the
                // witness-pending pair with that receipt's own pair, so
                // the settle guard retains it over the open gap and the
                // dispatch-point retries heal the SHARED record/marker
                // from this client. When no receipt could be minted at
                // all (no sender, or no valid actor to key one to), the
                // pendings arm from the DECIDER's own decision pair
                // instead of clearing: the decider reported no
                // durable witness exists for this scope, so the close
                // ladders must refuse to finalize and this client's heal
                // writes must carry the denial — an unarmed guard here
                // would let close finalize with the next launch restoring
                // the stale grant.
                if (witnessUnbacked && _settingsStore != null)
                {
                    _consentDenialRecordPending = true;
                    _consentDenialMarkerPending = true;
                    if (entry != null)
                    {
                        _consentDenialPendingDecidedAt = entry.DecidedAt;
                        _consentDenialPendingSeq = entry.DecisionSeq;
                    }
                    else
                    {
                        _consentDenialPendingDecidedAt =
                            ShardPilotJson.FormatTimestamp(decidedAtUtc.Value);
                        _consentDenialPendingSeq = deciderDecisionSeq;
                    }

                    // A PROPAGATED denial arms this sibling's obligation just
                    // like a local one, and a receipt-less sibling reaches no
                    // outbox persist to carry it — so the backup is written
                    // here, where the arm happens.
                    SyncOwedDenialWitnessLocked(freshDecision: true);
                }
                else
                {
                    _consentDenialRecordPending = false;
                    _consentDenialMarkerPending = false;
                    _consentDenialPendingDecidedAt = null;
                    _consentDenialPendingSeq = 0;
                }
                if (!alreadyDenied)
                {
                    // State, epoch, and queue clears flip in THIS SAME hold:
                    // a publish serialized between separate holds could
                    // otherwise still put a pre-denial batch on the wire
                    // against the old epoch.
                    _consentReadFailureCrashHold = false;
                    AdvanceConsentDenialEpochLocked();
                    cleared = _queue.Count + _restoredPending.Count + _spooledOverflowIds.Count;
                    _queue.Clear();
                    _restoredPending.Clear();
                    _spooledOverflowIds.Clear();
                    _spooledOverflowOrder.Clear();
                    _breadcrumbs.Clear();
                }
            }

            // The superseded obligation's DURABLE marker goes with the
            // in-memory key (still under the decision gate the propagation
            // runs beneath): a relaunch must never complete a grant this
            // denial replaced.
            ClearWithheldGrantMarkerBookkeeping();

            if (!alreadyDenied && _experiments != null)
            {
                // A PROPAGATED denial purges this sibling's analytics state
                // like a local SetConsent(false): the experiments purge
                // hook must run here too, or the sibling's exposure de-dupe
                // keeps claiming enqueued facts a later grant can never
                // re-send.
                _experiments.OnAnalyticsPurge();
            }

            if (entry != null)
            {
                PersistConsentOutbox();
                MaybeKickConsentOutbox(workerCadence: false);

                // Durability judgment for this CALLERLESS append, mirroring
                // the grant-arming path: when the appended deny receipt
                // remains retained without a durable home, surface it —
                // never silently best-effort. This matters most for the
                // POST-TEARDOWN window (a denial propagated to a client
                // whose worker already stopped does exactly one synchronous
                // persist right here on the deciding thread): the append is
                // deliberately KEPT even then, because a SUCCESSFUL persist
                // delivers the receipt at the scope's next launch — strictly
                // better than refusing (which would forfeit that case) —
                // while a FAILED one is surfaced here and covered by the
                // nets: the ownership registry still holds this outbox
                // DIRTY, so a same-session successor's construction merges
                // the retained entry (handoff), and the registration
                // catch-up re-mints the denial for any successor while the
                // session marker lives.
                var retainedWithoutDurableHome =
                    (!_consentOutbox.HasDurableBackend || _consentOutbox.IsDisowned)
                        ? _consentOutbox.OwnScope.Contains(entry.IdempotencyKey)
                        : (_consentOutbox.Dirty && !PersistConsentOutbox() &&
                           _consentOutbox.OwnScope.Contains(entry.IdempotencyKey));
                if (retainedWithoutDurableHome)
                {
                    _stats.RecordLastConsentError("consent_outbox_persist_failed");
                    SafeLog(
                        "shardpilot own-scope denial receipt appended but not durably persisted " +
                        "(consent_outbox_persist_failed): the write stays owed; a live worker " +
                        "retries it, and a torn-down scope's receipt is covered by the successor " +
                        "merge and the registration catch-up");
                }

                WakeWorker();
            }

            if (alreadyDenied)
            {
                // Already denied — maximally fail-closed, wipes done; the
                // state converged on the propagated flavor and the new
                // explicit decision's receipt (if any) was appended and is
                // delivering.
                return;
            }

            if (cleared > 0)
            {
                _stats.RecordDropped(cleared);
            }

            // Same dispatch-gate synchronization as the initiating denial: a
            // propagated denial must not return to the deciding caller while
            // this client can still start a crash send under the condemned
            // epoch.
            lock (_crashDispatchGate)
            {
            }

            if (_spool != null)
            {
                _spool.Wipe();
            }

            if (_crashSpool != null)
            {
                _crashSpool.Wipe();
            }

            // The crash Retry-After marker is scope-keyed, so each propagated
            // client clears its own — the in-memory re-send deadline too,
            // matching the initiator's denial path.
            ShardPilotCrashSpool.ClearRetryAfterDeadline(_settingsStore, _config);
            lock (_gate)
            {
                _crashResendNotBefore = DateTime.MinValue;
            }

            // The ANR era snapshots are scope-keyed like the Retry-After
            // marker: each propagated client wipes its own scope's,
            // matching the initiating denial path (fail-closed owed marker
            // when the file cannot be confirmed gone) — and stamps its own
            // scope's AEI suppression floor for the same reason the
            // initiator does.
            ShardPilotAnrEraStore.WipeScope(_config, _settingsStore, message => SafeLog("{0}", message));
            ShardPilotAeiCursor.RecordSuppressionFloor(
                _settingsStore, _config, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

            // The initiator swept ITS directory, but this client's resolved
            // directory can differ (a custom SpoolDir) and may hold
            // stale sibling-scope files: sweep it too, de-duplicated by
            // canonical directory — case-insensitive, matching the ownership
            // comparisons — so a shared directory is not swept twice in one
            // denial. The initiator runs BOTH sweeps (event + crash) over its
            // directory, so the skip covers both here.
            var ownDirectory = ShardPilotSpool.ResolveDirectory(_config);
            if (!string.IsNullOrEmpty(ownDirectory) &&
                !string.Equals(ownDirectory, alreadySweptDirectory, StringComparison.OrdinalIgnoreCase))
            {
                QuarantineSpoolIfSweepOwed(ShardPilotSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));
                QuarantineCrashSpoolIfSweepOwed(ShardPilotCrashSpool.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message)));

                // The initiator's era sweep (which it runs alongside ITS
                // spool sweeps) never reached this different directory —
                // sweep the sibling-scope era files here too, or an
                // inactive scope's snapshot would survive the denial.
                ShardPilotAnrEraStore.WipeAllScopesAtResolvedDirectory(
                    _config, _settingsStore, message => SafeLog("{0}", message));
            }
            else if (!string.IsNullOrEmpty(ownDirectory))
            {
                // The initiator swept this SAME directory: honor its outcome
                // via the owed markers — if that sweep failed, this client's
                // live spools must go out of play too, or post-re-grant data
                // appended here would be destroyed by the retry. (The
                // initiator's ERA sweep covered this directory too; era
                // enforcement is purely marker-based — no long-lived era
                // store instance rides in a client — so there is nothing to
                // quarantine here.)
                QuarantineSpoolIfSweepOwed(
                    !ShardPilotSpool.IsSweepOwedAtResolvedDirectory(_config, _settingsStore));
                QuarantineCrashSpoolIfSweepOwed(
                    !ShardPilotCrashSpool.IsSweepOwedAtResolvedDirectory(_config, _settingsStore));
            }

            WakeWorker();
        }

        /// <summary>
        /// Applies a crash-reporting opt-in/opt-out decided GLOBALLY on
        /// another client (invoked by the lifecycle registry): the same
        /// in-memory consequences as a local
        /// <see cref="SetCrashReportingEnabled"/> — flag + reason, breadcrumb
        /// ring cleared on disable, re-send pass kicked on enable — but
        /// without re-persisting the (already written) global decision or
        /// re-propagating. Idempotent: a no-op when this client already
        /// holds the propagated state. <paramref name="alreadySweptDirectory"/>
        /// is the era directory the deciding client's opt-out sweep just
        /// covered (null when unknown), mirroring
        /// <see cref="ApplyGlobalConsentDenial"/>'s handoff: a client whose
        /// own resolved directory DIFFERS sweeps its sibling-scope era
        /// files itself, one under the SAME directory skips the redundant
        /// sweep.
        /// </summary>
        internal void ApplyGlobalCrashReportingEnabled(bool enabled, string alreadySweptDirectory = null)
        {
            var becameEnabled = false;
            lock (_gate)
            {
                if (IntakeClosed && enabled)
                {
                    return;
                }

                becameEnabled = enabled && !_crashReportingEnabled;
                _crashReportingEnabled = enabled;

                // The reason is refreshed even when the flag already
                // matched: a client disabled by an unreadable/malformed
                // persisted setting must report the EXPLICIT opt-out another
                // live client just recorded (opt_out), not its stale
                // settings_read_failed — the process-wide state changed even
                // though this client's boolean did not.
                _crashReportingDisabledReason = enabled ? null : ShardPilotCrashSettings.OptOutReason;
                if (!enabled)
                {
                    _breadcrumbs.Clear();
                    _crashDisableEpoch++;
                }
            }

            if (becameEnabled)
            {
                MaybeStartCrashResendPass();
                WakeWorker();

                // Same dark-window close-out as the local setter: this scope's
                // standing AEI suppression-floor marker AND every enumerable
                // sibling scope's are raised to the re-enable moment (see
                // SetCrashReportingEnabledUnderDecisionGate).
                var reEnableNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                ShardPilotAeiCursor.RaiseSuppressionFloorIfMarked(
                    _settingsStore, _config, reEnableNowMs);
                ShardPilotAnrEraStore.RaiseSuppressionFloorForAllScopesAtResolvedDirectory(
                    _config, _settingsStore, reEnableNowMs, message => SafeLog("{0}", message));
            }
            else if (!enabled)
            {
                // Same dispatch-gate synchronization as the local setter: a
                // propagated opt-out must not return to the deciding caller
                // while this client can still start another send.
                lock (_crashDispatchGate)
                {
                }

                // And the same immediate era wipe as the local setter: the
                // opt-out reaches every live scope NOW, so each client
                // destroys its own scope's ANR era snapshots alongside its
                // breadcrumb ring (fail-closed owed marker on an
                // unconfirmed wipe). Outside _gate — file IO.
                ShardPilotAnrEraStore.WipeScope(_config, _settingsStore, message => SafeLog("{0}", message));

                // And the same AEI suppression floor as the local setter
                // (plain KV; safe on any platform, lane dark or not).
                ShardPilotAeiCursor.RecordSuppressionFloor(
                    _settingsStore, _config, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

                // The setting is GLOBAL while era files are per-scope: when
                // this client's resolved directory differs from the one the
                // deciding client just swept (a custom SpoolDir), its
                // sibling-scope era files saw neither that sweep nor any
                // propagation — sweep them here (each swept scope's floor
                // stamped by the sweep), exactly like the propagated
                // denial's directory handling. Same-directory clients skip:
                // the initiator's sweep already covered it.
                var ownEraDirectory = ShardPilotSpool.ResolveDirectory(_config);
                if (!string.IsNullOrEmpty(ownEraDirectory) &&
                    !string.Equals(ownEraDirectory, alreadySweptDirectory, StringComparison.OrdinalIgnoreCase))
                {
                    ShardPilotAnrEraStore.WipeAllScopesAtResolvedDirectory(
                        _config, _settingsStore, message => SafeLog("{0}", message));
                }
            }
        }

        /// <summary>
        /// Applies a GLOBALLY decided consent grant to this client (invoked
        /// by the lifecycle registry when another client records the grant):
        /// the pipeline opens for FUTURE events only, exactly like a local
        /// <see cref="SetConsent"/>(true) — nothing is restored or loaded,
        /// pre-grant events are gone by design — without re-persisting the
        /// (already written) global decision, re-posting the deciding
        /// scope's receipt, or re-propagating. Idempotent for the consent
        /// STATE (a no-op when already granted) — but when
        /// <paramref name="armOwnScopeReceipt"/> is true this client arms a
        /// pending receipt for ITS OWN scope regardless: receipt scope
        /// (workspace_id/app_id/environment_id) is baked into the body, so
        /// the deciding client's receipt cannot record this scope's row, and
        /// this client's next batch publish must sequence behind its own
        /// scope's receipt. Passed true when a live in-session
        /// grant decision reaches this client (propagation, or the
        /// registration catch-up while the session's grant-receipt marker
        /// is set); false when merely re-applying an already-persisted
        /// grant, which must not mint receipts.
        ///
        /// RETURNS whether this scope's REQUIRED receipt is durably safe (or
        /// settled, or not required at all): the DECIDING SetConsent's
        /// durability judgment spans every live sibling scope — a required
        /// sibling receipt that exists only in memory makes the deciding
        /// call report consent_outbox_persist_failed, because a process kill
        /// before the sibling's owed-write retry would reload that scope as
        /// Granted (the global key) with an EMPTY outbox: receipt-less. A
        /// closed client reports true — its scope's receipt is the
        /// successor/catch-up nets' concern, not the deciding caller's.
        ///
        /// <paramref name="skipAlreadyGranted"/> is the SETTLE-TIME
        /// distribution flavor (the withheld-grant completion): the
        /// DECISION-TIME propagation already armed a receipt for every
        /// sibling that was registered then, so the completion's late
        /// distribution must reach only the clients that MISSED the
        /// decision (constructed later, refreshed against the still-absent
        /// withheld record) — re-arming an already-granted sibling would
        /// mint a duplicate receipt for the same decision.
        /// </summary>
        internal bool ApplyGlobalConsentGrant(
            bool armOwnScopeReceipt,
            bool skipAlreadyGranted = false,
            DateTime? settledDecidedAtUtc = null,
            bool deferSupersededStampToDecider = false,
            bool withheldCompletionConvergence = false,
            ShardPilotConsentNoticeSnapshot settledNotice = null)
        {
            var durablySafe = false;
            try
            {
                durablySafe = ApplyGlobalConsentGrantCore(armOwnScopeReceipt, skipAlreadyGranted,
                    settledDecidedAtUtc, deferSupersededStampToDecider, withheldCompletionConvergence, settledNotice);
                return durablySafe;
            }
            finally
            {
                // A shared grant can precede any refusal, including a true-returning
                // hold or an exception. Keep the scope gated across process exit
                // until its own receipt is durable; closed clients leave ownership
                // to their successor and must not manufacture refusal debt.
                bool refusalOwed;
                lock (_gate)
                {
                    refusalOwed = !IntakeClosed &&
                        (!durablySafe || _consentState != ShardPilotConsentState.Granted);
                }
                if (refusalOwed &&
                    !ShardPilotConsent.RecordScopedPendingDowngrade(_settingsStore, _consentScopeHash))
                {
                    SafeLog("shardpilot consent: the refused grant's scope marker could not be written; " +
                        "the refusal has no durable scope marker for restart");
                }
            }
        }

        private bool ApplyGlobalConsentGrantCore(
            bool armOwnScopeReceipt, bool skipAlreadyGranted, DateTime? settledDecidedAtUtc,
            bool deferSupersededStampToDecider, bool withheldCompletionConvergence, ShardPilotConsentNoticeSnapshot settledNotice)
        {
            // THE SESSION HOLD REFUSES A PROPAGATED GRANT TOO. It is armed when
            // this actor's own evidence could not be judged — an unjudgeable
            // marker slot, or a backup whose read was refused — and admission
            // reads the STATE, not the flag, so letting another actor's grant
            // set Granted here reopened egress over what may be this actor's
            // only denial witness. This actor's own
            // fresh decision still clears the hold; nobody else's does.
            var withheldCompletionFlavor =
                !armOwnScopeReceipt || skipAlreadyGranted || settledDecidedAtUtc.HasValue;

            var closedAlready = false;
            var evidenceHoldRefuses = false;
            lock (_gate)
            {
                closedAlready = IntakeClosed;
                evidenceHoldRefuses = ConsentEvidenceUnjudgeableLocked() &&
                                      _consentState != ShardPilotConsentState.Granted;
            }

            // A CLOSED client short-circuits ahead of the refusal. It can still be in the propagation's lifecycle snapshot
            // between entering CloseAsync and unregistering, and its documented
            // contract is to report TRUE and leave the receipt to its successor
            // and the catch-up. Refusing first reported a durable grant as
            // consent_outbox_persist_failed, and wrote a scope downgrade debt for
            // a scope that is going away, purely because a torn-down client
            // happened to still be listed. The later _closed check stays where it
            // is: this one short-circuits, that one guards the mutation.
            if (closedAlready)
            {
                return true;
            }

            bool grantRestricted;
            lock (_gate)
            {
                grantRestricted = _consentState == ShardPilotConsentState.DeniedForcedMinor ||
                                  _consentConfiguredActorInvalid;
            }

            if (grantRestricted || !RetryConsentPurge())
            {
                return false;
            }

            if (evidenceHoldRefuses)
            {
                return false;
            }

            if (withheldCompletionFlavor && _consentOutbox.LoadedRecordMalformed)
            {
                return false;
            }

            // The settle-time distribution flavor carries the completing
            // obligation's ORIGINAL decided_at: a POST-RESTART completion
            // has no session grant marker (process state died with the
            // exit), so the sibling's receipt is minted from the moment the
            // durable marker preserved instead — same actor/scope
            // resolution, same original decision time, never the arming
            // time.
            var entry = armOwnScopeReceipt
                ? (settledDecidedAtUtc.HasValue
                    ? CreateConsentReceiptEntry(true, settledDecidedAtUtc.Value, null, notice: settledNotice)
                    : CreateOwnScopeGrantEntry())
                : null;
            if (skipAlreadyGranted && entry == null)
            {
                // Settle-time distribution ONLY converges a missed sibling
                // TOGETHER WITH its own-scope receipt: when no receipt can
                // be minted here (session grant marker cleared by a
                // read-failure fail-close, no HTTP sender, no valid actor),
                // flipping the sibling Granted would open its pipeline
                // receipt-less. It stays gated and converges through the
                // ordinary refresh against the now-persisted record instead
                // — the standard inherited-grant posture.
                return true;
            }

            if (entry == null && !withheldCompletionConvergence &&
                ShardPilotConsent.HasScopedPendingDowngrade(_settingsStore, _consentScopeHash))
            {
                // A RECEIPT-LESS (re-)apply — the registration refresh's
                // armOwnScopeReceipt:false flavor foremost — must not
                // reopen a scope whose grant-receipt debt is durably on
                // record: the cap-refusal marker
                // says this scope holds no receipt for the shared grant,
                // and only an ARM (entry != null) can repay that. The
                // scope stays gated; nothing is owed by this call. The
                // construction consult applies the same hold at the next
                // launch; unreadable markers count as SET — fail-closed.
                // (The WITHHELD COMPLETION's own convergence bypasses this
                // hold — its receipt just SETTLED
                // and its state just persisted, exactly the durable
                // own-scope evidence the marker demands; the completion
                // discharges the marker itself.)
                return true;
            }

            var wasGranted = true;
            var capBlocked = false;
            lock (_gate)
            {
                if (IntakeClosed)
                {
                    return true;
                }

                if (skipAlreadyGranted && _consentState == ShardPilotConsentState.Granted)
                {
                    // Settle-time distribution: this sibling already carries
                    // the grant (decision-time propagation, or its own
                    // decision) and with it its own scope's receipt — nothing
                    // to converge, nothing to mint.
                    return true;
                }

                // Whether this scope was ALREADY granted, captured for the
                // suppression-floor raise below: like the deciding client's
                // own grant path, only a REAL transition to granted ends a
                // dark denied window — an idempotent re-apply must not move
                // a still-standing marker over post-re-grant ANRs.
                wasGranted = _consentState == ShardPilotConsentState.Granted;

                // consent_outbox_full FAIL-CLOSED GRANT GATE for the
                // PROPAGATED/CATCH-UP legs, mirroring
                // the deciding SetConsent's gate: with this scope's trail
                // denial-full at the cap, the appended grant receipt would
                // itself be the only evictable pure grant — the cap pass
                // would displace it, the eviction would then read as
                // acked-and-pruned in the durability judgment below, and
                // this scope would relaunch Granted (the global key)
                // receipt-less. REFUSE instead: no append, no local flip —
                // the scope stays gated (the standard inherited-grant
                // posture) and converges when a later decision re-arms it
                // after retained denials drain. The deciding caller sees
                // the sibling-not-safe judgment through the same result
                // channel as any other non-durable sibling receipt.
                if (entry != null && _consentOutbox.GrantAppendBlockedByCap())
                {
                    capBlocked = true;
                }

                // Append this scope's receipt BEFORE the already-granted
                // idempotency return: a sibling that was already granted
                // (say, from the persisted state at construction) still
                // publishes post-decision batches that must sequence behind
                // the new decision's receipt. The trail is append-only (D1):
                // an earlier retained receipt is never superseded away — an
                // extra receipt for a scope that already has one carries the
                // identical decision under a fresh idempotency key, which
                // the endpoint accepts harmlessly.
                if (!capBlocked && !withheldCompletionFlavor && deferSupersededStampToDecider)
                {
                    _consentOutbox.AuthorizeExplicitDecisionReplacement();
                }

                if (entry != null && !capBlocked)
                {
                    // (No supersede stamp here: the deciding flow stamps
                    // this scope through the dispatcher pass only AFTER the
                    // decision's state persist durably lands — and the
                    // catch-up/settle flavors
                    // stamp below once the durable state is confirmed
                    // granted. The fresh receipt's key is remembered so
                    // that later stamp can exempt it. The scope's
                    // cap-refusal marker, if any, is NOT cleared here —
                    // the append is mirror-only; the clear waits for the
                    // durability judgment below.)
                    _consentOutbox.AppendToMirror(entry);
                    _lastPropagatedGrantReceiptKey = entry.IdempotencyKey;
                }

                // The GLOBAL grant decision reaching this client is newer
                // than any denial whose witness-pending pair this client
                // still carries: stand the pair down in BOTH arms. Even on
                // the cap-refused path (where the grant does not apply to
                // this scope) the stand-down is the safe direction — left
                // armed, the shutdown ladder's record retry would rewrite
                // the OLD denial over the newer globally persisted grant
                // record.
                _consentDenialRecordPending = false;
                _consentDenialMarkerPending = false;
                _consentDenialPendingDecidedAt = null;
                _consentDenialPendingSeq = 0;

                if (capBlocked)
                {
                    // Nothing applied for this scope: no receipt, no state
                    // flip, no generation bump — the scope keeps its prior
                    // (gated) posture. The refusal surfaces outside the
                    // lock.
                }
                else
                {
                    if (armOwnScopeReceipt)
                    {
                        // A LIVE in-session grant decision reached this client
                        // (propagation, session-marker catch-up, or settle-time
                        // completion — the arming flag is exactly that signal;
                        // a merely re-applied persisted grant passes false):
                        // counts toward the decision generation so the
                        // identity-downgrade machinery never wipes it.
                        _consentDecisionGeneration++;
                    }

                    // A fresh (globally persisted) decision supersedes an
                    // unreadable record: the crash plane's hold lifts here too.
                    _consentReadFailureCrashHold = false;
                    if (_consentState != ShardPilotConsentState.Granted &&
                        !ConsentEvidenceUnjudgeableLocked())
                    {
                        ApplyConsentState(ShardPilotConsentState.Granted);
                    }
                }
            }

            if (capBlocked)
            {
                // The callerless flavors (sibling propagation, registration
                // catch-up, settle-time distribution) surface the refusal on
                // the stats error surface, mirroring the durability
                // judgment's posture below; the deciding caller additionally
                // sees FALSE through the sibling durability judgment and reports the
                // decision's receipts as not durably safe.
                _stats.RecordLastConsentError("consent_outbox_full");
                SafeLog(
                    "shardpilot propagated grant refused (scope=consent, code=consent_outbox_full): " +
                    "this scope's trail is denial-full at the {0}-entry cap, so its grant receipt " +
                    "cannot be retained without displacing itself; the scope stays gated until " +
                    "retained denials drain and a later decision re-arms it",
                    ShardPilotConsentOutbox.MaxEntries);

                // A scope that INHERITED Granted from the persisted read
                // (a late construction refreshing against the already
                // persisted shared grant) converges to Unknown NOW: the refusal means this scope holds no
                // receipt for that grant, and leaving the inherited state
                // open would publish receipt-less for the rest of the
                // session — the durable marker alone only protects the
                // NEXT launch. ApplyIdentityGrantDowngrade is the standard
                // inherited-state downgrade: it wipes accepted-but-unsent
                // work and stands down for a state backed by this scope's
                // own live in-session decision (decision generation).
                if (ApplyIdentityGrantDowngrade())
                {
                    SafeLog(
                        "shardpilot consent: the cap-refused scope's inherited grant is converged " +
                        "to Unknown in-session — no receipt backs it on this scope");
                }

                return false;
            }

            // The propagated re-grant ends any dark denied window for this
            // scope's AEI suppression floor, like the deciding client's own
            // grant path (plain KV, outside _gate) — and transition-gated
            // like it too (see wasGranted above).
            if (!wasGranted)
            {
                var reGrantNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                ShardPilotAeiCursor.RaiseSuppressionFloorIfMarked(
                    _settingsStore, _config, reGrantNowMs);
                ShardPilotAnrEraStore.RaiseSuppressionFloorForAllScopesAtResolvedDirectory(
                    _config, _settingsStore, reGrantNowMs, message => SafeLog("{0}", message));
            }

            if (entry != null)
            {
                // CATCH-UP/SETTLE supersede stamp, conditioned on the
                // DURABLE state: these flavors
                // run outside a live deciding call, so no post-persist
                // dispatcher pass will reach this scope — the stamp is safe
                // exactly when the durable record already reads Granted
                // (the decision this arm follows has durably persisted; any
                // standing Granted has no unsuperseded parked backer, so
                // stamping older grants under it can never disarm a needed
                // downgrade). A read failure or a non-granted read stamps
                // nothing — fail-closed; the decision-time propagation
                // flavor is covered by the deciding flow's own
                // post-persist pass instead.
                bool durableStateReadFailed;
                if (ShardPilotConsent.Load(_settingsStore, _config.AnonymousId, out durableStateReadFailed) ==
                    ShardPilotConsentState.Granted && !durableStateReadFailed)
                {
                    _consentOutbox.OwnScope.MarkRetainedAnalyticsGrantsSuperseded(entry.IdempotencyKey);
                }

                if (!deferSupersededStampToDecider)
                {
                    // CATCH-UP/SETTLE flavors have no deciding flow behind
                    // them — no post-persist dispatcher pass will ever
                    // consume the exemption key remembered at the append,
                    // and a STALE key would exempt the wrong receipt from a
                    // LATER decision's stamp pass. Consume it now; only the decision-time
                    // propagation (deferSupersededStampToDecider) leaves it
                    // for the deciding flow's imminent pass, which is
                    // itself one-shot.
                    lock (_gate)
                    {
                        _lastPropagatedGrantReceiptKey = null;
                    }
                }

                PersistConsentOutbox();

                // Arming a receipt is a consent dispatch point for this
                // client: deliver it now when a bearer is available instead
                // of parking it until this client's next publish — and wake
                // the worker so its wait computation sees the armed receipt.
                MaybeKickConsentOutbox(workerCadence: false);

                // Durability judgment for the CALLERLESS arming path,
                // mirroring the decision-path durability judgment on the CURRENT
                // state (a receipt the kick just synchronously acked and
                // pruned is safe; an owed write healed by the in-call retry
                // is too). The deciding client hands
                // "consent_outbox_persist_failed" to its SetConsent caller;
                // a sibling / late registrant has no caller, so the same
                // code is surfaced on the stats error surface instead —
                // NEVER swallowed. The append already marked the write owed
                // (AppendToMirror), so beyond this surfacing the receipt is
                // protected exactly like the deciding client's: the owed
                // write retries at every consent dispatch point (and in the
                // pause/quit persist), and the shutdown check refuses
                // teardown while it is owed — a clean close can never lose
                // it. No relaunch re-mint exists by CONTRACT: receipts are
                // never minted from a merely-loaded persisted grant,
                // and the reloaded durable outbox is the only cross-launch
                // carrier — persisted-grant-with-empty-outbox is the
                // normal delivered-and-pruned state, indistinguishable from
                // a loss.
                var retainedWithoutDurableHome =
                    (!_consentOutbox.HasDurableBackend || _consentOutbox.IsDisowned)
                        ? _consentOutbox.OwnScope.Contains(entry.IdempotencyKey)
                        : (_consentOutbox.Dirty && !PersistConsentOutbox() &&
                           _consentOutbox.OwnScope.Contains(entry.IdempotencyKey));
                if (retainedWithoutDurableHome)
                {
                    // The Contains check distinguishes an owed APPEND from an
                    // owed PRUNE, exactly like the deciding path's judgment:
                    // a receipt the arming kick above already got
                    // ACKNOWLEDGED (and pruned from the mirror) is SETTLED
                    // server-side even while the pruned record's rewrite is
                    // still failing — that owed prune retries as usual (at
                    // worst the next launch re-sends a stale entry the
                    // server de-duplicates) and must not fail the
                    // sibling judgment for a receipt that is already safe.
                    _stats.RecordLastConsentError("consent_outbox_persist_failed");
                    SafeLog(
                        "shardpilot own-scope grant receipt armed but not durably persisted " +
                        "(consent_outbox_persist_failed): the write stays owed, retries at every " +
                        "dispatch point, and holds shutdown teardown until it lands");
                }
                else
                {
                    // The armed receipt is durably retained (or settled):
                    // NOW the scope's cap-refusal debt is repaid — the
                    // durable marker armed by an earlier refusal is
                    // discharged only against durable receipt evidence
                    // (a clear at the mirror-only
                    // append could outlive a failed persist and relaunch
                    // the scope receipt-less under the shared grant).
                    ShardPilotConsent.ClearScopedPendingDowngrade(_settingsStore, _consentScopeHash);
                }

                WakeWorker();
                return !retainedWithoutDurableHome;
            }

            return true;
        }

        /// <summary>
        /// The deciding flow's POST-PERSIST supersede stamp for this
        /// sibling scope (via <see
        /// cref="ShardPilotDispatcher.StampSupersededConsentGrants"/>): the
        /// decision's consent state is durably persisted by the time this
        /// runs, so this scope's OLDER retained pending grants are no
        /// longer its backers — stamp them, exempting the fresh receipt the
        /// same decision's propagation armed here (its key was remembered
        /// at the append). A closed or displaced client stands down; an
        /// unstamped scope merely re-downgrades once at its next launch
        /// (fail-closed residual).
        /// </summary>
        internal void StampOwnScopeSupersededAfterGlobalPersist()
        {
            if (_consentOutbox.IsDisowned)
            {
                return;
            }

            string exceptKey;
            lock (_gate)
            {
                if (IntakeClosed)
                {
                    return;
                }

                exceptKey = _lastPropagatedGrantReceiptKey;
                _lastPropagatedGrantReceiptKey = null;
            }

            _consentOutbox.OwnScope.MarkRetainedAnalyticsGrantsSuperseded(exceptKey);
            PersistConsentOutbox();
        }

        /// <summary>
        /// Builds a grant-receipt outbox entry scoped to THIS client's
        /// config (fresh idempotency key, decided_at = the session grant
        /// decision's ORIGINAL time from the process-wide marker — never the
        /// arming time), or null when no receipt can ever be posted (no HTTP
        /// sender / no valid actor identifier / no in-session grant decision
        /// on record).
        /// </summary>
        private ShardPilotConsentOutboxEntry CreateOwnScopeGrantEntry()
        {
            var decision = ShardPilotConsent.SessionGrantDecision;
            if (decision == null)
            {
                return null;
            }

            return CreateConsentReceiptEntry(true, decision.DecidedAtUtc, null, notice: decision.Notice);
        }

        /// <summary>
        /// Fails this LIVE client closed when the persisted consent record
        /// is UNREADABLE or malformed at a post-construction re-read: the
        /// record may have been a denial, so nothing loaded under the
        /// earlier in-memory state may transmit. Consent resolves to
        /// Unknown (refusing new events with the consent-unknown reason —
        /// not Denied, which would claim a decision the record cannot
        /// prove), the pending queue / restored backlog / overflow
        /// accounting are dropped and the spools purged exactly as a
        /// construction-time read failure purges them, and the crash plane
        /// raises the consent-read-failure hold (lifted only by the next
        /// explicit consent decision). An already-denied client keeps its
        /// denial — that state is maximally fail-closed already, and its
        /// wipes have run. Idempotent for a client already holding the
        /// read-failure posture.
        /// </summary>
        private void ApplyConsentReadFailureFailClosed()
        {
            var cleared = 0;
            lock (_gate)
            {
                if (_closed ||
                    ShardPilotConsent.IsDeniedState(_consentState) ||
                    (_consentReadFailureCrashHold && _consentState == ShardPilotConsentState.Unknown))
                {
                    return;
                }

                ApplyConsentState(ShardPilotConsentState.Unknown);

                // Advancing the denial epoch invalidates whatever the worker
                // drained under the stale grant; advancing the crash epoch
                // aborts any capture in flight across the hold raised below.
                AdvanceConsentDenialEpochLocked();
                _crashDisableEpoch++;
                _consentReadFailureCrashHold = true;
                cleared = _queue.Count + _restoredPending.Count + _spooledOverflowIds.Count;
                _queue.Clear();
                _restoredPending.Clear();
                _spooledOverflowIds.Clear();
                _spooledOverflowOrder.Clear();

                // The unreadable record may have been a denial, and a denial
                // clears the trail — so the maybe-denial must too.
                _breadcrumbs.Clear();

                // Trail semantics (defold-canonical D1): receipts already
                // appended to the outbox are NOT withdrawn — a receipt
                // documents an explicit decision the player really made, and
                // the outbox delivers under denied/unknown alike; the read-failure posture gates EVENTS, not the
                // decision trail. What clears is the process-wide grant
                // MARKER, exactly like the denial path: without this, a
                // later readable-granted re-read (or another client's
                // registration) would arm and send a grant receipt with no
                // new explicit SetConsent(true) behind it.
                ShardPilotConsent.RecordSessionGrantDecision(null);
            }

            if (cleared > 0)
            {
                _stats.RecordDropped(cleared);
            }

            if (_experiments != null)
            {
                // The read-failure fail-closed purge condemns queued
                // analytics facts exactly like a denial: same experiments
                // purge hook, same re-arm semantics.
                _experiments.OnAnalyticsPurge();
            }

            if (_spool != null)
            {
                _spool.Wipe();
            }

            if (_crashSpool != null)
            {
                _crashSpool.Wipe();
            }

            ShardPilotCrashSpool.ClearRetryAfterDeadline(_settingsStore, _config);
            lock (_gate)
            {
                _crashResendNotBefore = DateTime.MinValue;
            }

            // The maybe-denial condemns sibling scopes' files too — the same
            // sweeps a construction-time read failure (and a denial) runs.
            QuarantineSpoolIfSweepOwed(ShardPilotSpool.WipeAllScopesAtResolvedDirectory(
                _config, _settingsStore, message => SafeLog("{0}", message)));
            QuarantineCrashSpoolIfSweepOwed(ShardPilotCrashSpool.WipeAllScopesAtResolvedDirectory(
                _config, _settingsStore, message => SafeLog("{0}", message)));

            // Synchronize with any in-flight crash dispatch, like every
            // other blocked transition: when this fail-close returns, no
            // send that predates the hold can still be started.
            lock (_crashDispatchGate)
            {
            }

            SafeLog(
                "shardpilot consent state could not be re-read; failing closed: pending analytics dropped, spools purged, crash plane held until an explicit consent decision");
            WakeWorker();
        }

        /// <summary>
        /// Re-resolves this client's privacy state from the persisted
        /// records — invoked by the lifecycle registry at registration to
        /// close the construction/propagation gap: a client that loaded the
        /// OLD values can miss a decision's live propagation when the
        /// propagation snapshot enumerated before this client registered.
        /// The registration happens after that propagation, which happens
        /// after the decision persisted (or recorded its process-wide
        /// pending shadow), so this re-read observes the newest state.
        /// A consent record that turned UNREADABLE or malformed since
        /// construction fails this client closed BEFORE
        /// <see cref="StartDispatchAfterRegistration"/> can arm any egress:
        /// the record may have been a denial, so a stale in-memory grant —
        /// restored events included — must not transmit (see
        /// <see cref="ApplyConsentReadFailureFailClosed"/>). Explicit
        /// persisted decisions apply through the same idempotent paths the
        /// live propagation uses.
        /// </summary>
        internal void RefreshPrivacyStateFromSettings()
        {
            if (_settingsStore == null)
            {
                return;
            }

            // The [read persisted -> apply] pair runs under the process-wide
            // decision gate: a concurrent explicit decision either completes
            // its persist+propagation entirely before this refresh (which
            // then reads the NEW value) or starts entirely after it (its
            // propagation then supersedes whatever this refresh applied) —
            // this refresh can never re-apply a value the decision already
            // replaced.
            lock (ShardPilotDispatcher.PrivacyDecisionGate)
            {
                RefreshPrivacyStateUnderDecisionGate();
            }
        }

        private void RefreshPrivacyStateUnderDecisionGate()
        {
            // The identity-downgrade shadow is SNAPSHOTTED atomically with
            // (i.e. before) the persisted read: the dispatch-point retry
            // that clears store+shadow is itself serialized under this same
            // decision gate now, and this ordering additionally guarantees
            // that even a not-yet-gated consult could never pair a STALE
            // granted read with a POST-CLEAR shadow check and resurrect the
            // receipt-less grant.
            var pendingIdentityDowngrade =
                ShardPilotConsent.HasPendingDowngrade(_settingsStore, consultDurableMarker: true);
            bool consentReadFailed;
            var persistedConsent = ShardPilotConsent.Load(_settingsStore, _config.AnonymousId, out consentReadFailed);

            // THE SESSION HOLD OUTRANKS A READABLE GRANT HERE TOO. The
            // production constructor always registers, and this refresh reads
            // only the consent RECORD — so a grant that construction had just
            // held fail-closed (unjudgeable marker slot, or an unreadable or
            // corrupt backup) was restored moments later, undoing the hold on
            // the one path that always runs. A denial
            // still applies: the hold blocks resurrecting a GRANT, never a
            // fresh denial.
            // Skips the CONSENT application only — the rest of this refresh
            // (crash privacy, the downgrade shadow) still has to run. Returning outright left the crash plane
            // reading a stale setting for the whole session.
            var holdRefusesThisRefresh =
                ConsentEvidenceUnjudgeableLocked() && !consentReadFailed &&
                !ShardPilotConsent.IsDeniedState(persistedConsent);

            if (holdRefusesThisRefresh)
            {
                // fall through to the non-consent refresh below
            }
            else if (consentReadFailed)
            {
                ApplyConsentReadFailureFailClosed();
            }
            else if (ShardPilotConsent.IsDeniedState(persistedConsent))
            {
                // When the denial was decided LIVE THIS SESSION (marker read
                // serialized with the decision under the shared decision
                // gate), a client registering after the propagation snapshot
                // is catching up on that same in-session decision arriving
                // late — NOT minting from a relaunch-loaded record, which is
                // the explicit-decision rule forbids (a relaunch has no marker and mints
                // nothing). It appends its OWN scope's denial receipt with
                // the ORIGINAL decision moment whenever this scope's trail
                // does NOT already end with THIS decision or a newer one —
                // an empty trail mints (empty is indistinguishable from
                // delivered-and-pruned, so a scope whose grant receipt
                // already delivered would otherwise leave the backend's
                // last word GRANTED after the player denied), and a tail
                // denial OLDER than the live decision mints too: the trail
                // is append-only, and suppressing the newer denial would
                // freeze the backend — which resolves receipts
                // last-write-wins by decided_at — on the older decision's
                // moment and reason (a later forced-minor denial would
                // never be documented). A possibly-superfluous denial
                // receipt is harmless; a suppressed one is a compliance
                // gap. The suppression is therefore only (a) a tail denial
                // at or after the live moment — parsed at the receipt
                // format's full tick precision, so a same-decision tail
                // (e.g. this scope's own propagated receipt) compares
                // EQUAL and suppresses, while an unparseable tail
                // timestamp mints (the superfluous-harmless direction) —
                // and (b) the per-decision guard below (this scope's
                // client, keyed on the marker's decision moment), which
                // keeps repeated catch-ups — e.g. registration refreshes
                // after the minted receipt delivered and pruned — from
                // re-minting the same decision; it runs under the
                // process-wide decision gate, which serializes every
                // refresh with every decision.
                var sessionDenial = ShardPilotConsent.SessionDenialDecision;
                var sessionDenialDecidedAt = sessionDenial == null ? (DateTime?)null : sessionDenial.DecidedAtUtc;
                DateTime? catchUpDecidedAt = null;
                if (sessionDenialDecidedAt.HasValue &&
                    _catchUpDenialMintedForTicks != sessionDenialDecidedAt.Value.Ticks)
                {
                    var tailDeniedMoment =
                        ShardPilotConsentOutbox.ParseReceiptTimestamp(_consentOutbox.OwnScope.TailDeniedDecidedAt);
                    var tailDocumentsThisOrNewer =
                        tailDeniedMoment.HasValue &&
                        tailDeniedMoment.Value >= sessionDenialDecidedAt.Value;
                    if (!tailDocumentsThisOrNewer)
                    {
                        catchUpDecidedAt = sessionDenialDecidedAt;
                        _catchUpDenialMintedForTicks = sessionDenialDecidedAt.Value.Ticks;
                    }
                }

                ApplyGlobalConsentDenial(null, persistedConsent, catchUpDecidedAt,
                    notice: sessionDenial == null ? null : sessionDenial.Notice);
            }
            else if (persistedConsent == ShardPilotConsentState.Granted)
            {
                // An OWED identity-downgrade clear on this backend outranks
                // the stale persisted grant it could not remove: re-applying
                // it here would resurrect the receipt-less grant the Mode-B
                // identity binding downgraded. The SNAPSHOT taken before the
                // load governs (see the note at the top); retry the durable
                // clear and apply nothing — fresh consent is still required.
                if (pendingIdentityDowngrade)
                {
                    // The shadow may be DURABLE-marker-only here (recorded by
                    // an earlier process whose clear never landed before the
                    // kill): re-record it so the in-process set backs it for
                    // this session's dispatch-point retries too.
                    ShardPilotConsent.RecordPendingDowngrade(_settingsStore);
                    if (ShardPilotConsent.HasPendingDowngrade(_settingsStore) &&
                        ShardPilotConsent.Clear(_settingsStore))
                    {
                        ShardPilotConsent.ClearPendingDowngrade(_settingsStore);
                    }

                    // And this client's LIVE state converges down with the
                    // record: a client that loaded the grant before the
                    // downgrade (and missed its live propagation by
                    // registering after the snapshot) must not keep
                    // publishing under it — the shadowed record is
                    // effectively the absent/Unknown state the branch below
                    // handles for a clean read. Stands down for a live
                    // in-session explicit decision (decision generation),
                    // whose own Save superseded — or will supersede — the
                    // shadow.
                    if (ApplyIdentityGrantDowngrade())
                    {
                        SafeLog(
                            "shardpilot consent: registration refresh found this backend's persisted " +
                            "grant awaiting its identity-downgrade clear; the loaded grant is " +
                            "downgraded — a fresh explicit SetConsent is required");
                    }
                }
                else
                {
                    // When an explicit grant was decided THIS session (marker
                    // read is serialized with the decision itself by the
                    // process-wide decision gate both run under), this client
                    // may have been constructed after the propagation
                    // snapshot: arm its OWN scope's receipt so its first
                    // post-grant batch enforces the same receipt-before-batch
                    // ordering as every live sibling — and its scope records
                    // its own consent row. A grant merely LOADED from
                    // persistence (no in-session decision) must not mint
                    // receipts: receipts exist only for explicit decisions.
                    ApplyGlobalConsentGrant(
                        armOwnScopeReceipt: ShardPilotConsent.SessionGrantReceiptsActive);
                }
            }
            else
            {
                // Persisted UNDECIDED (a clean read of the absent record).
                // A client still holding a GRANT it loaded at construction
                // reads this when a Mode-B identity downgrade CLEARED that
                // grant after the load and the live propagation missed this
                // client (constructed after the propagation snapshot,
                // registering now): the persisted layer is the newest
                // gate-serialized truth, so the inherited grant converges
                // DOWN to the fail-closed Unknown it would load today —
                // the same idempotent downgrade the propagation applies,
                // minting nothing. A grant backed by a LIVE in-session
                // explicit decision stands (decision generation above
                // zero): notably an explicit grant whose OBSERVABLE persist
                // is still WITHHELD by the receipt-first ordering reads
                // persisted-absent here, and wiping it would discard the
                // player's newest decision — its withheld completion (or
                // the SetConsent retry) persists it instead.
                if (ApplyIdentityGrantDowngrade())
                {
                    SafeLog(
                        "shardpilot consent: registration refresh read an undecided persisted state " +
                        "under a live loaded grant; the inherited grant is downgraded (fresh " +
                        "explicit SetConsent required)");
                }
            }

            string crashReason;
            var crashEnabled = ShardPilotCrashSettings.Load(_settingsStore, out crashReason);
            var becameEnabled = false;
            lock (_gate)
            {
                if (IntakeClosed && crashEnabled)
                {
                    return;
                }

                becameEnabled = crashEnabled && !_crashReportingEnabled;
                _crashReportingEnabled = crashEnabled;
                _crashReportingDisabledReason = crashEnabled ? null : crashReason;
                if (!crashEnabled)
                {
                    _breadcrumbs.Clear();
                    _crashDisableEpoch++;
                }
            }

            if (becameEnabled)
            {
                MaybeStartCrashResendPass();
                WakeWorker();
            }
            else if (!crashEnabled)
            {
                lock (_crashDispatchGate)
                {
                }

                if (crashReason == ShardPilotCrashSettings.OptOutReason)
                {
                    // The refresh converged this client onto a persisted
                    // EXPLICIT opt-out it may have missed live (constructed
                    // before the decision, registered after the propagation
                    // snapshot): apply the same era wipe + suppression
                    // floor the propagated opt-out applies, with the
                    // null-initiator directory semantics the denial refresh
                    // gets through ApplyGlobalConsentDenial(null, ...) —
                    // own scope wiped and floored, own directory's sibling
                    // era files swept (the sweep floors each swept scope).
                    // Idempotent on every registration under a standing
                    // opt-out; a READ-FAILURE disable deliberately replays
                    // nothing (the stored decision is unknowable).
                    ShardPilotAnrEraStore.WipeScope(_config, _settingsStore, message => SafeLog("{0}", message));
                    ShardPilotAeiCursor.RecordSuppressionFloor(
                        _settingsStore, _config, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                    ShardPilotAnrEraStore.WipeAllScopesAtResolvedDirectory(
                        _config, _settingsStore, message => SafeLog("{0}", message));
                }
            }
        }

        internal ShardPilotSpool SpoolForTesting
        {
            get { return _spool; }
        }

        internal ShardPilotCrashSpool CrashSpoolForTesting
        {
            get { return _crashSpool; }
        }

        internal long BatchSettlementsForTesting
        {
            get
            {
                lock (_gate)
                {
                    return _batchSettlements;
                }
            }
        }

        private Task<ShardPilotResult> EnqueueFlushRequest(bool close, TimeSpan? timeout, bool holds = false)
        {
            // A CLOSE that times out must still be a durable barrier: the
            // request's timeout completion persists the pending remnant
            // (thread-safe, synchronous file write, no network) BEFORE the
            // awaited task resolves. The worker's own later persist then
            // double-persists harmlessly — appends de-duplicate by event id.
            // Plain flushes pass no callback: a flush timeout makes no
            // durability promise.
            var rejections = new RejectionWindow();

            // A close abandons the experiment fetches still in flight: a
            // non-adult age declaration riding one ends now, unanswered, and
            // its withdrawal lands durably BEFORE the teardown below (a
            // torn-down consumer withdraws nothing, and a relaunch would
            // otherwise restore and serve the assignment the host declared
            // the subject ineligible for). Outside _gate: the withdrawal
            // hands its pipeline scan to this client's gate.
            if (close && _experiments != null)
            {
                _experiments.AbortDeclarationsForClose();
            }

            var budget = timeout ?? TimeSpan.FromSeconds(10);
            if (budget <= TimeSpan.Zero)
            {
                budget = TimeSpan.FromSeconds(10);
            }

            // A close arms its timeout only once it enters the worker's
            // queue, after any session end it owes: the remnant the timeout
            // persists then always carries that end.
            // Every request registers its rejection observer before arming,
            // so a short deadline cannot leave an observer behind.
            var request = new FlushRequest(
                budget,
                close ? new Func<FlushRequest, ShardPilotResult>(CloseDeadlineAnswer) : null,
                close,
                holds,
                result => CompleteRejections(result, rejections),
                false);
            SessionEndClaim closeEnd = null;
            lock (_gate)
            {
                if (_workerStopped || (!close && (holds ? IntakeClosed : _closed)))
                {
                    request.Complete(StoppedResultFor(request));
                    return request.Task;
                }

                request.Boundary = _intakeSequence;
                RegisterRejections(rejections, request.Boundary, close ? _closeCompletion : null);
                if (!close) request.Arm();
                if (close) _owningCloseRequest = request;

                if (close)
                {
                    // The open session ends first, once, claimed in the same
                    // hold that closes admission: no event can open a session
                    // between the claim and the close.
                    if (!_closed)
                    {
                        closeEnd = ClaimCloseSessionEndLocked(DateTime.UtcNow + budget);
                    }

                    _closed = true;

                    // Drop the lifecycle subscription at submission, for the
                    // same reason the experiments consumer tears down here: the
                    // event is STATIC, so a closed client left subscribed is
                    // both leaked and still reachable — a later resume would
                    // call SessionEnd on it, which refuses under _closed but
                    // only after the fan-out has already reached a dead object.
                    try
                    {
                        ShardPilotDispatcher.PauseStateChanged -= OnPauseStateChangedForSession;
                    }
                    catch
                    {
                        // Teardown must never throw.
                    }

                    // The experiments consumer tears down AT SUBMISSION —
                    // before this close waits on (or its timeout abandons)
                    // any in-flight work — not only when the worker's drain
                    // reaches its stop: a close resolved by the request's
                    // own timer (a hung final publish, or a worker-less
                    // client whose request nothing drains) would otherwise
                    // leave _tornDown false, and an assignment response
                    // still in flight could pass the HandleResponse guard
                    // after the awaited CloseAsync resolved — invoking game
                    // callbacks and rewriting the shared cache post-close.
                    // Teardown sets the consumer's stop flag and then
                    // BARRIERS the egress gate (an in-flight gated send
                    // completes before the close proceeds; later sends
                    // observe the flag in-gate) — no callbacks, and the
                    // only lock it takes beyond the consumer's own is the
                    // egress gate, the denial path's sanctioned
                    // under-_gate edge, so calling it under _gate is safe;
                    // the close drain's owed housekeeping (durable-sync
                    // retries, the exposure sweep, the persist-barrier
                    // capture) is deliberately not torn-down-gated and
                    // still lands what is owed. The worker's own stop-side
                    // Teardown remains as the idempotent second edge.
                    if (_experiments != null)
                    {
                        _experiments.Teardown();
                    }
                }

                if (closeEnd != null)
                {
                    _closesAfterEnd = new List<FlushRequest>();
                }
                else if (close && _closesAfterEnd != null)
                {
                    _closesAfterEnd.Add(request);
                }
                else
                {
                    _closeRequested |= close;
                    _flushRequests.Add(request);
                    if (close)
                    {
                        request.Arm();
                    }
                }
            }

            if (closeEnd != null)
            {
                // Queued ahead of the close request, so the close's own drain
                // carries it. A failed end does not stop the close.
                try
                {
                    EnqueueSessionEnd(closeEnd, true);
                }
                catch (Exception error)
                {
                    SafeLog("shardpilot close could not queue the session end: {0}", error.GetType().Name);
                }

                lock (_gate)
                {
                    var closes = _closesAfterEnd;
                    _closesAfterEnd = null;
                    closes.Insert(0, request);
                    if (_workerStopped)
                    {
                        for (var i = 0; i < closes.Count; i++)
                        {
                            closes[i].Complete(StoppedResultFor(closes[i]));
                        }

                        return request.Task;
                    }

                    _closeRequested = true;
                    _flushRequests.AddRange(closes);
                    for (var i = 0; i < closes.Count; i++)
                    {
                        closes[i].Arm();
                    }
                }
            }

            WakeWorker();
            return request.Task;
        }

        /// <summary>
        /// The end a close owes the open session: an expired background pause
        /// ends at its deadline, anything else with <c>app_final</c> now. A
        /// start still awaiting its verdict is waited for, but no later than
        /// <paramref name="waitUntilUtc"/>; past it the close ends nothing and
        /// records why. Caller holds _gate.
        /// </summary>
        private SessionEndClaim ClaimCloseSessionEndLocked(DateTime waitUntilUtc)
        {
            while (_sessionState.Lifecycle.PendingStartThreadId != 0)
            {
                var remaining = waitUntilUtc - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero ||
                    _sessionState.Lifecycle.PendingStartThreadId == Thread.CurrentThread.ManagedThreadId)
                {
                    _stats.RecordFailure("close_session_end_unresolved");
                    return null;
                }

                Monitor.Wait(_gate, remaining);
            }

            return ClaimExpiredSessionLocked() ??
                ClaimSessionEndLocked("app_final", _utcNow(), _sessionState.Lifecycle);
        }

#if SHARDPILOT_SDK_TEST_SUPPORT
        // HEADLESS TEST BUILD ONLY — absent from the shipped package.
        //
        // A client that owes a denial witness cannot be closed: the shutdown
        // ladder is deliberately stay-alive while one is owed. So a TEST that
        // builds such a client and walks away leaves a worker retrying a
        // refused write for the rest of the process, and every refused retry
        // re-arms ShardPilotConsent's PROCESS-WIDE pending denial — which
        // Load() returns ahead of the store, inside whatever test is running by
        // then. That cost a day once, diagnosed from the far end.
        //
        // Registered here rather than in ShardPilotDispatcher because clients
        // built by the internal test constructors never join its lifecycle
        // registry, so it cannot see the ones that matter.
        private static readonly List<WeakReference<ShardPilotClient>> ConstructedForTesting =
            new List<WeakReference<ShardPilotClient>>();

        private volatile bool _quiescedForTesting;

        /// <summary>
        /// Drops every client's owed denial obligation, WITHOUT writing
        /// anything. Called after each test: a client that owes a witness
        /// cannot be closed and keeps retrying a refused write for the rest of
        /// the process, and each refused retry re-arms the process-wide pending
        /// denial that the NEXT test's client then resolves as its own.
        ///
        /// Deliberately not a close and not a write. Closing converges the
        /// trail, and that convergence is itself a refused write — measured, it
        /// made the poisoning worse, not better. Nothing durable happens here;
        /// the test is over and its store is about to be discarded.
        /// </summary>
        internal static void QuiesceDenialObligationsForTesting()
        {
            lock (ConstructedForTesting)
            {
                for (var i = 0; i < ConstructedForTesting.Count; i++)
                {
                    ShardPilotClient client;
                    if (!ConstructedForTesting[i].TryGetTarget(out client))
                    {
                        continue;
                    }

                    // The FLAG, not just the fields: clearing them is a
                    // snapshot, and a worker already inside a boot/belt pass has
                    // read its own copies and can re-arm them after this
                    // returns. The flag outlives that — it makes the one choke
                    // point where a refused write arms the process-wide pending
                    // denial refuse to run at all.
                    // The volatile flag FIRST and outside the lock: it stops
                    // every FUTURE write immediately, and it must land even if
                    // the lock never comes.
                    client._quiescedForTesting = true;

                    // Then a BOUNDED wait for the gate. A leaked worker can be
                    // holding it across a store call inside the witness-heal
                    // retry, and blocking teardown on that would trade a flaky
                    // suite for a hung one. Waiting at
                    // all is still worth it: it lets a write already in flight
                    // finish HERE rather than after the caller's reset — and if
                    // it does not, that reset runs afterwards and clears
                    // whatever the straggler armed.
                    //
                    // Giving up on the lock costs nothing durable: the flag has
                    // already fenced the client, and the fields it guards are
                    // moot once the test's store is discarded.
                    if (Monitor.TryEnter(client._gate, TimeSpan.FromMilliseconds(250)))
                    {
                        try
                        {
                            client._consentDenialRecordPending = false;
                            client._consentDenialMarkerPending = false;
                            client._withheldMarkerDischargeOwed = false;
                        }
                        finally
                        {
                            Monitor.Exit(client._gate);
                        }
                    }
                }
            }
        }

        internal static void ClearConstructedRegistryForTesting()
        {
            lock (ConstructedForTesting)
            {
                ConstructedForTesting.Clear();
            }
        }

#endif

        private void WakeWorker()
        {
#if !(UNITY_WEBGL && !UNITY_EDITOR)
            _wake.Set();
#endif
        }

        /// <summary>
        /// The bounded ceiling a flush/close will wait for an in-flight crash
        /// send to complete. A crash send uses the configured HTTP timeout, so a
        /// completed (or timed-out) send returns within it; the flush request's
        /// own timeout is the outer guard.
        /// </summary>
        private TimeSpan PendingCrashSendWaitBudget()
        {
            var budget = _config.HttpTimeout;
            return budget > TimeSpan.Zero ? budget : TimeSpan.FromSeconds(10);
        }

#if !(UNITY_WEBGL && !UNITY_EDITOR)
        /// <summary>
        /// Blocks the worker thread (bounded) until every crash send handed to
        /// the transport has completed, so flush/close does not return while a
        /// crash is still in flight. The crash send completion runs on the
        /// dispatcher (a different thread), which signals <c>_wake</c> as each
        /// send finishes — safe to block on here.
        /// </summary>
        private void WaitForPendingCrashSends()
        {
            if (Interlocked.Read(ref _pendingCrashSends) == 0)
            {
                return;
            }

            var deadline = DateTime.UtcNow + PendingCrashSendWaitBudget();
            while (Interlocked.Read(ref _pendingCrashSends) > 0)
            {
                var remaining = deadline - DateTime.UtcNow;
                if (remaining <= TimeSpan.Zero)
                {
                    return;
                }

                // _wake is signalled by the send completion (and by ordinary
                // Track/Flush/consent activity). Re-checking the count on every
                // wakeup keeps an unrelated signal from ending the wait early.
                _wake.WaitOne(remaining);
            }
        }
#else
        /// <summary>
        /// Yields the coroutine worker (bounded) until every crash send has
        /// completed. The crash send completion is pumped on this same main
        /// thread, so the worker must YIELD rather than block — a blocking wait
        /// would deadlock the very pump that drives the send to completion.
        /// </summary>
        private IEnumerator WaitForPendingCrashSendsCoroutine()
        {
            if (Interlocked.Read(ref _pendingCrashSends) == 0)
            {
                yield break;
            }

            var deadline = DateTime.UtcNow + PendingCrashSendWaitBudget();
            while (Interlocked.Read(ref _pendingCrashSends) > 0 && DateTime.UtcNow < deadline)
            {
                yield return null;
            }
        }
#endif

        private ShardPilotEvent PrepareEvent(ShardPilotEvent ev, bool stampTimestamp = true,
            string normalizedCustomName = null)
        {
            if (ev == null)
            {
                throw new ArgumentException("event is required");
            }

            var copy = ev.Clone();
            copy.Name = normalizedCustomName ?? ShardPilotConfig.Trim(copy.Name);
            if (string.IsNullOrEmpty(copy.Name))
            {
                throw new ArgumentException("event name is required");
            }

            copy.Id = ShardPilotConfig.Trim(copy.Id).ToLowerInvariant();
            if (string.IsNullOrEmpty(copy.Id))
            {
                copy.Id = ShardPilotIds.NewUuidV7();
            }

            if (copy.SealedFact == null && !ShardPilotIds.IsUuidV7(copy.Id))
            {
                throw new ArgumentException("event id must be UUIDv7");
            }

            if (copy.TimestampUtc == default(DateTime))
            {
                if (stampTimestamp) copy.TimestampUtc = DateTime.UtcNow;
            }
            else
            {
                copy.TimestampUtc = copy.TimestampUtc.ToUniversalTime();
            }

            // The identifier clamp applies to PER-EVENT identity overrides too: the
            // construction-time gate clamps only the config identities, but
            // an event enqueued with its own oversized UserId/AnonymousId
            // would otherwise ride the batch verbatim. Same refusal surface
            // as the config-level gate — the invalid override is REJECTED
            // (never truncated: truncation could collide distinct identities
            // and mis-attribute the event) and the event falls back to the
            // already-clamped config identity below.
            if (!string.IsNullOrEmpty(copy.UserId) &&
                !ShardPilotConsentOutbox.IsValidIdentifier(copy.UserId))
            {
                copy.UserId = string.Empty;
                SafeLog("shardpilot event user id rejected: exceeds {0} bytes (invalid_user_id)",
                    ShardPilotConsentOutbox.MaxIdentifierBytes);
            }

            if (!string.IsNullOrEmpty(copy.AnonymousId) &&
                !ShardPilotConsentOutbox.IsValidIdentifier(copy.AnonymousId))
            {
                copy.AnonymousId = string.Empty;
                SafeLog("shardpilot event anonymous id rejected: exceeds {0} bytes (invalid_anonymous_id)",
                    ShardPilotConsentOutbox.MaxIdentifierBytes);
            }

            copy.UserId = FirstNonEmpty(copy.UserId, _config.UserId);
            copy.AnonymousId = FirstNonEmpty(copy.AnonymousId, _config.AnonymousId);
            copy.Platform = FirstNonEmpty(copy.Platform, _config.Platform);
            copy.AppVersion = FirstNonEmpty(copy.AppVersion, _config.AppVersion);
            copy.AppBuild = FirstNonEmpty(copy.AppBuild, _config.AppBuild);

            // Session id/sequence are attached at ENQUEUE time (AttachSessionLocked),
            // not here, so a denied/closed/over-buffer event never leaks a lazily
            // created session or an advanced sequence into a later accepted event.
            return copy;
        }

#if !(UNITY_WEBGL && !UNITY_EDITOR)
        private void RunWorker()
        {
            // The held batch and the reconciled-epoch holder are fields so
            // PersistPendingEvents can snapshot them under _gate on
            // application pause/quit; only this worker mutates them.
            var batch = _workerBatch;
            var consentEpoch = _workerConsentEpoch;
            // Server-backpressure deadline (429 Retry-After). _wake is an
            // AutoResetEvent also signalled by Track/Flush/consent changes, so
            // waiting on its duration alone lets an unrelated wakeup defeat the
            // backpressure. Gating publishes on an absolute deadline survives any
            // number of early wakeups. Worker-thread-local: no synchronization.
            var backoffUntil = DateTime.MinValue;
            // Set when a Retry-After/backoff deadline elapses, so the retained batch
            // is retried IMMEDIATELY instead of waiting an additional flush interval
            // on top of the server-requested delay.
            var publishImmediately = false;
            // The ABSOLUTE deadline for the partial batch currently held, armed
            // when the batch first becomes non-empty and cleared when it is
            // published or drained. DateTime.MinValue means nothing is held.
            //
            // Absolute, because every accepted event wakes this worker: a wait
            // re-derived from _config.FlushInterval on each loop was RESTARTED
            // by the next event, so the interval bounded the gap between
            // events rather than the age of the batch. At the 1s default that
            // was invisible; at 15s a game emitting one event every 10s held
            // its first event until the batch filled — roughly four minutes —
            // or until traffic went quiet. The documented maximum has to hold
            // on the path players actually run, which is this one; the WebGL
            // worker already keeps its deadline this way.
            var partialBatchDeadline = DateTime.MinValue;

            try
            {
                while (true)
                {
                    if (_experiments != null)
                    {
                        // Experiments housekeeping: the revalidation cadence,
                        // the owed-exposure sweep, and owed durable-sync
                        // retries. Runs before the drain so a fact enqueued
                        // by this tick rides this same cycle's publish;
                        // internally a no-op unless consent is granted and an
                        // assignment is cached. Never holds _gate across the
                        // tick itself. A PENDING CLOSE suppresses the tick:
                        // a closing (possibly replaced) client must not run
                        // one more revalidation and rewrite the shared cache
                        // after its successor already wrote.
                        bool tickAllowed;
                        lock (_gate)
                        {
                            tickAllowed = !_closeRequested && !_closed;
                        }

                        if (tickAllowed)
                        {
                            _experiments.Tick();
                            PruneExperimentFactKeys();
                        }
                    }

                    List<FlushRequest> requests = null;
                    var closeArrived = false;
                    bool freshFlush;
                    var epochBeforeDrain = consentEpoch.Value;

                    // Test seam: lets a test enqueue an event in the window
                    // between a drain emptying the held batch and the drain
                    // below refilling it — which is the only window in which
                    // a fresh batch can inherit the previous one's deadline,
                    // and one a test cannot otherwise schedule. No-op in
                    // production.
                    var preDrainHook = WorkerPreDrainHookForTesting;
                    if (preDrainHook != null)
                    {
                        preDrainHook();
                    }

                    if (batch.Count == 0)
                    {
                        // The held batch is gone — published below, drained by
                        // an explicit Flush, or dropped — so its deadline must
                        // not outlive it: the next event to arrive is a NEW
                        // partial batch and starts its own interval.
                        //
                        // Observed HERE, before the drain refills the list,
                        // because that is the only point every iteration
                        // passes through while the batch is still empty. The
                        // later `batch.Count == 0` branch misses the case this
                        // exists for: an event queued between an explicit
                        // Flush draining the batch and the next iteration
                        // refills it first, so the batch that inherits the old
                        // deadline never appears empty to that check — and
                        // publishes early, or immediately if the old deadline
                        // had already passed.
                        partialBatchDeadline = DateTime.MinValue;
                    }

                    lock (_gate)
                    {
                        if (DrainQueueInto(batch, consentEpoch))
                        {
                            // Reconciliation threw the held batch away and the
                            // same call may have refilled the list from the
                            // queue. That list is a NEW partial batch and its
                            // deadline is its own — the check above cannot see
                            // this, because the list was non-empty before the
                            // drain and is non-empty after it.
                            partialBatchDeadline = DateTime.MinValue;
                        }

                        requests = TakeFlushRequestsLocked(out freshFlush);
                        if (requests != null)
                        {
                            for (var i = 0; i < requests.Count; i++)
                            {
                                closeArrived |= requests[i].Close;
                            }
                        }

                        if (_closeRequested && batch.Count == 0 && _queue.Count == 0 && requests == null)
                        {
                            // A close with nothing left to drain stops here.
                            // No closing client stays alive past its close:
                            // an owed denial witness stays in force in memory
                            // for the process instead.
                            KeepUnbackedDenialForProcessLocked();
                            _workerStopped = true;
                            if (_experiments != null)
                            {
                                // The idle stop is a real teardown too:
                                // in-flight assignment responses must be
                                // discarded from here on. Teardown sets
                                // the stop flag and barriers the egress
                                // gate (the denial path's sanctioned
                                // under-_gate edge, no callbacks), so
                                // calling it under _gate is safe.
                                _experiments.Teardown();
                            }

                            return;
                        }
                    }

                    if (consentEpoch.Value != epochBeforeDrain)
                    {
                        // A consent denial landed since the last pass: whatever
                        // the deadline was protecting is condemned, so clear it
                        // (with the streak DropDeniedBatchLocked already reset)
                        // or a deny→re-grant round trip would hold FRESH
                        // post-grant events behind backoff that belonged to
                        // discarded data. A held flush re-drains now: the
                        // purged events it waited on count as delivered.
                        backoffUntil = DateTime.MinValue;
                        _heldFlushRetryAtUtc = DateTime.MinValue;
                    }

                    if (requests != null && !closeArrived && batch.Count > 0 && DeferHoldingFlushes(requests, backoffUntil))
                    {
                        requests = null;
                    }

                    // Re-arm the crash re-send pass once its backoff (or a
                    // persisted Retry-After deadline from an earlier launch)
                    // elapses. Cheap no-op when nothing is pending; the pass
                    // itself runs on the sender's completion callbacks, not
                    // on this worker.
                    MaybeStartCrashResendPass();

                    // Every worker cycle is a consent dispatch point (the
                    // unity analogue of defold's update-driven cadence):
                    // retries the owed outbox write and re-attempts a
                    // retained head once its Retry-After/backoff window
                    // elapses. Cheap no-op while the outbox is empty.
                    // ... and NOT a worker cadence when this cycle was woken by
                    // an explicit flush: the pace exists to stop the background
                    // loop spinning on a dead token backend, never to refuse a
                    // caller asking for progress now.
                    MaybeKickConsentOutbox(workerCadence: !freshFlush);

                    // Honor an active Retry-After deadline before any automatic
                    // publish. Skipped for explicit flush requests (user intent)
                    // and on close (responsive shutdown). Re-loops — re-draining
                    // newly queued events — until the deadline elapses.
                    if (requests == null && batch.Count > 0 && !_closeRequested)
                    {
                        var remaining = backoffUntil - DateTime.UtcNow;
                        if (remaining > TimeSpan.Zero)
                        {
                            // Capped so a crash re-send deadline that lands
                            // before the events backoff still re-arms on time
                            // (the re-loop hits the pass check at the top).
                            _wake.WaitOne(CapWaitToHeldFlush(
                                CapWaitToCrashResendDeadline(CapWaitToConsentRetryDeadline(remaining))));
                            continue;
                        }
                        if (_heldFlushes.Count > 0)
                        {
                            // A held flush retries the batch with its own
                            // drain on the next pass.
                            backoffUntil = DateTime.MinValue;
                            continue;
                        }

                        if (backoffUntil != DateTime.MinValue)
                        {
                            // The server-requested backoff just elapsed; retry the
                            // retained batch NOW rather than adding another full
                            // flush-interval wait on top of the Retry-After.
                            backoffUntil = DateTime.MinValue;
                            publishImmediately = true;
                        }
                    }

                    if (requests != null)
                    {
                        _passTerminal = null;
                        var result = PublishUntilDrained(batch, consentEpoch);
                        if (closeArrived && !result.Success && result.Retryable && batch.Count > 0)
                        {
                            // The final drain hit a retryable failure with the batch
                            // still retained. The common cause is a Mode-B 401, which
                            // IsRetryableForPublish already handled by invalidating
                            // the cached JWT — so one bounded retry mints a fresh
                            // token and resends instead of dropping the final batch
                            // at shutdown. (Mode A's 401 is non-retryable and never
                            // reaches here; a persistent 429 simply re-fails once and
                            // is then dropped on close.)
                            result = PublishUntilDrained(batch, consentEpoch);
                        }
                        if (closeArrived && _experiments != null)
                        {
                            // Shutdown housekeeping, after the final drain
                            // freed queue room: owed durable syncs retry (a
                            // kill/not-assigned drop whose cache write
                            // failed transiently must not stay reload truth
                            // on disk), and owed exposure facts sweep — a
                            // treatment applied under a FULL queue must not
                            // exit without its fact. The flush→sweep→flush
                            // cycle LOOPS while it makes progress: with a
                            // tiny buffer one freed slot admits one fact per
                            // pass, and a single pass would strand the rest
                            // (the starvation case). Bounded by the owed
                            // count strictly decreasing plus a hard cap, so
                            // a persistently failing publish or a
                            // consent-closed sweep can never wedge the
                            // close. Anything still undelivered is persisted
                            // to the spool below; best-effort by design.
                            var passBudget = _experiments.OwedExposureCount + 1;
                            for (var housekeepingPass = 0; housekeepingPass < passBudget; housekeepingPass++)
                            {
                                var owedBefore = _experiments.OwedExposureCount;
                                try
                                {
                                    // The drain window lets the INTERNAL
                                    // sweep enqueue past the _closed gate;
                                    // opened only around the synchronous
                                    // housekeeping call.
                                    _experimentFactDrainWindow = true;
                                    _experiments.ShutdownHousekeeping();
                                }
                                finally
                                {
                                    _experimentFactDrainWindow = false;
                                }

                                lock (_gate)
                                {
                                    DrainQueueInto(batch, consentEpoch);
                                }

                                if (batch.Count > 0)
                                {
                                    result = PublishUntilDrained(batch, consentEpoch);
                                }

                                var owedAfter = _experiments.OwedExposureCount;
                                if (owedAfter == 0 || owedAfter >= owedBefore)
                                {
                                    break;
                                }
                            }
                        }

                        // Before completing the flush/close, await any in-flight
                        // crash send (bounded) so a crash captured just before
                        // shutdown is not lost mid-flight, mirroring the batch
                        // drain above. The crash send completion runs on the
                        // dispatcher (a different thread from this worker), so a
                        // blocking wait here cannot deadlock it.
                        WaitForPendingCrashSends();

                        // A public flush answers for the whole drain: a
                        // retryable failure holds it for the worker's retry
                        // instead of answering with this attempt.
                        DateTime? retryAtUtc = null;
                        if (!closeArrived && !result.Success && result.Retryable)
                        {
                            retryAtUtc = RetryDeadlineFor(result);
                        }

                        SettleHoldingFlushes(requests, result, retryAtUtc, batch);
                        var persisted = PersistOutcome.Nothing;
                        if (closeArrived)
                        {
                            // Whatever the final drain could not deliver (it
                            // failed transiently, or never got an attempt) is
                            // persisted to the offline spool BEFORE the close
                            // request completes: awaiting CloseAsync is the
                            // shutdown barrier, so when it resolves the
                            // undelivered leftovers are already on disk.
                            persisted = PersistPendingEventsCore();

                            // Shutdown durability contract:
                            // after the final drain, retry the owed outbox
                            // write and the standing denial's witness writes
                            // (storage may have healed). While ConsentDurabilityOwedLocked() is true,
                            // or the drain is gated behind a
                            // retained grant receipt, the close is HELD rather
                            // than answered: this worker keeps retrying and
                            // draining until they clear or the close's
                            // deadline answers it. Nothing is kept alive past
                            // that deadline.
                            RetryOwedConsentWriteIfDirty();
                            RetryOwedDenialWitnessWrites();
                            if (HoldCloseUntilDeadline(requests, result, batch))
                            {
                                _wake.WaitOne(CloseHoldRetry);
                                continue;
                            }
                        }
                        if (closeArrived)
                        {
                            // Test seam: lets a test land a propagated
                            // denial between the earlier shutdown durability check and
                            // the teardown decision below. No-op in
                            // production.
                            var preStopHook = CloseTeardownPreStopHookForTesting;
                            if (preStopHook != null)
                            {
                                preStopHook();
                            }

                            // Teardown is COMPLETE before the close request
                            // resolves: a caller that has awaited CloseAsync
                            // must observe a fully torn-down client. The
                            // shutdown durability check is RE-RUN atomically with the stop
                            // flag: a propagated denial appends its receipt
                            // to closing clients BY DESIGN, and such an
                            // append can land between the earlier check and
                            // this point. Before the deadline, consent debt
                            // identified by ConsentDurabilityOwedLocked() holds
                            // close; at the deadline that debt is reported as
                            // spool_persist_failed and teardown proceeds.
                            bool consentOwed;
                            var stopped = false;
                            lock (_gate)
                            {
                                consentOwed = ConsentDurabilityOwedLocked();
                                if (consentOwed && !Volatile.Read(ref _closeDeadlineReached))
                                {
                                    HoldCloseRequestsLocked(requests, result);
                                }
                                else
                                {
                                    _workerStopped = true;
                                    stopped = true;
                                }
                            }

                            if (!stopped)
                            {
                                _wake.WaitOne(CloseHoldRetry);
                                continue;
                            }

                            if (_experiments != null)
                            {
                                // Stop the experiments consumer WITH the
                                // teardown: an assignment response still in
                                // flight must not install, persist, or call
                                // back into game code from now on.
                                _experiments.Teardown();
                            }

                            CompleteAll(requests, consentOwed
                                ? ConsentUnpersistedAtShutdown()
                                : CloseResultFor(result, persisted));
                            return;
                        }

                        CompleteAll(requests, result);
                        if (retryAtUtc.HasValue)
                        {
                            // A grant-receipt gate refusal takes no backoff: the batch
                            // waits for the receipt's own progress (its ack
                            // wakes this worker) or the next flush tick.
                            backoffUntil = retryAtUtc.Value;
                        }
                        else
                        {
                            // The explicit drain left nothing retained
                            // (delivered, or dropped as permanent): a deadline
                            // armed for that batch's earlier failures is stale
                            // and must not hold fresh traffic.
                            backoffUntil = DateTime.MinValue;
                        }
                        continue;
                    }

                    if (_heldFlushes.Count > 0)
                    {
                        // A held flush publishes the batch with its own
                        // drain, so every outcome reaches it.
                        _wake.WaitOne(CapWaitToHeldFlush(
                            CapWaitToCrashResendDeadline(CapWaitToConsentRetryDeadline(_config.FlushInterval))));
                        continue;
                    }

                    if (batch.Count >= _config.BatchSize)
                    {
                        publishImmediately = false;
                        var result = PublishCurrentBatch(batch, consentEpoch);
                        if (!result.Success && result.Retryable)
                        {
                            // Set the deadline; the top-of-loop gate enforces it
                            // even if _wake fires early for an unrelated reason.
                            // A grant-receipt gate refusal takes no backoff (see above).
                            backoffUntil = string.Equals(
                                result.Error, ShardPilotConsentOutbox.ReceiptPendingError, StringComparison.Ordinal)
                                ? ConsentGateBackoffUntil()
                                : DateTime.UtcNow + RetryWait(result);
                        }
                        continue;
                    }

                    if (batch.Count == 0)
                    {
                        publishImmediately = false;
                        // Nothing is held, so nothing is owed a deadline: the
                        // next event arms a fresh one.
                        partialBatchDeadline = DateTime.MinValue;
                        // Capped so a pending crash re-send deadline shorter
                        // than the flush interval wakes the worker on time.
                        _wake.WaitOne(CapWaitToHeldFlush(
                            CapWaitToCrashResendDeadline(CapWaitToConsentRetryDeadline(_config.FlushInterval))));
                        continue;
                    }

                    // A pending retry whose backoff just elapsed publishes now; an
                    // ordinary partial batch waits out the REMAINDER of its own
                    // deadline — armed once, when it first became non-empty.
                    // An early wake (another event, a crash re-send cap, a
                    // consent retry cap) therefore only ever SHORTENS the wait;
                    // it cannot push the batch's publish further out.
                    if (partialBatchDeadline == DateTime.MinValue)
                    {
                        partialBatchDeadline = DateTime.UtcNow + _config.FlushInterval;
                    }

                    var untilPartialBatchDeadline = partialBatchDeadline - DateTime.UtcNow;
                    if (untilPartialBatchDeadline < TimeSpan.Zero)
                    {
                        untilPartialBatchDeadline = TimeSpan.Zero;
                    }
                    if (!publishImmediately && untilPartialBatchDeadline > TimeSpan.Zero)
                    {
                        _wake.WaitOne(CapWaitToHeldFlush(CapWaitToCrashResendDeadline(
                            CapWaitToConsentRetryDeadline(untilPartialBatchDeadline))));
                    }
                    if (publishImmediately || DateTime.UtcNow >= partialBatchDeadline)
                    {
                        publishImmediately = false;
                        partialBatchDeadline = DateTime.MinValue;
                        var result = PublishCurrentBatch(batch, consentEpoch);
                        if (!result.Success && result.Retryable)
                        {
                            // A grant-receipt gate refusal takes no backoff (see above).
                            backoffUntil = string.Equals(
                                result.Error, ShardPilotConsentOutbox.ReceiptPendingError, StringComparison.Ordinal)
                                ? ConsentGateBackoffUntil()
                                : DateTime.UtcNow + RetryWait(result);
                        }
                    }
                }
            }
            finally
            {
                List<FlushRequest> pending = null;
                lock (_gate)
                {
                    if (_flushRequests.Count > 0)
                    {
                        pending = new List<FlushRequest>(_flushRequests);
                        _flushRequests.Clear();
                    }
                    _workerStopped = true;
                }

                if (pending != null)
                {
                    for (var i = 0; i < pending.Count; i++)
                    {
                        pending[i].Complete(StoppedResultFor(pending[i]));
                    }
                }

                CompleteHeldFlushesOnStop();

                // A stopped client must not keep rewriting the spool file on
                // application pause/quit over a newer client for the same
                // scope. Pure C# — safe on this worker thread; a no-op when
                // this client never registered.
                ShardPilotDispatcher.UnregisterLifecycleClient(this);

                // …and it must not keep a WRITER doing the same thing. Leaving
                // the registry removes the pause/quit callbacks; it does not
                // stop the background thread, which goes on retrying an owed
                // rewrite of a path a newer client may already own — and which
                // the handover can no longer disown, because this client just
                // left the registry. Drain, then stop it.
                if (_spool != null)
                {
                    _spool.CloseWriter();
                }
            }
        }
#else
        private IEnumerator RunCoroutineWorker()
        {
            // Fields for the same reason as the threaded worker (see
            // RunWorker); on this profile everything runs on the main thread.
            var batch = _workerBatch;
            var consentEpoch = _workerConsentEpoch;
            var nextFlushAtUtc = DateTime.UtcNow + _config.FlushInterval;
            // Server-backpressure deadline (429 Retry-After), kept separate from
            // the flush-interval timer so a FULL batch still publishes immediately
            // when there is no active backoff, but a retryable failure defers BOTH
            // the full-batch and flush-interval publish branches until it elapses.
            var backoffUntil = DateTime.MinValue;

            try
            {
                while (true)
                {
                    if (_experiments != null)
                    {
                        // Experiments housekeeping (mirrors the threaded
                        // worker's loop-top tick, pending-close gate
                        // included).
                        bool tickAllowed;
                        lock (_gate)
                        {
                            tickAllowed = !_closeRequested && !_closed;
                        }

                        if (tickAllowed)
                        {
                            _experiments.Tick();
                            PruneExperimentFactKeys();
                        }
                    }

                    List<FlushRequest> requests = null;
                    var closeArrived = false;
                    bool freshFlush;
                    var shouldStop = false;
                    var hadBatch = batch.Count > 0;
                    var epochBeforeDrain = consentEpoch.Value;

                    lock (_gate)
                    {
                        DrainQueueInto(batch, consentEpoch);
                        if (!hadBatch && batch.Count > 0)
                        {
                            nextFlushAtUtc = DateTime.UtcNow + _config.FlushInterval;
                        }

                        requests = TakeFlushRequestsLocked(out freshFlush);
                        if (requests != null)
                        {
                            for (var i = 0; i < requests.Count; i++)
                            {
                                closeArrived |= requests[i].Close;
                            }
                        }

                        if (_closeRequested && batch.Count == 0 && _queue.Count == 0 && requests == null)
                        {
                            // Mirrors the threaded worker's idle stop.
                            KeepUnbackedDenialForProcessLocked();
                            _workerStopped = true;
                            shouldStop = true;
                            if (_experiments != null)
                            {
                                // The idle stop is a real teardown too
                                // (see the threaded worker's note).
                                _experiments.Teardown();
                            }
                        }
                    }

                    if (consentEpoch.Value != epochBeforeDrain)
                    {
                        // Mirrors the threaded worker: a denial condemned the
                        // data the deadline was protecting — clear it so
                        // post-re-grant events are not held behind it.
                        backoffUntil = DateTime.MinValue;
                        _heldFlushRetryAtUtc = DateTime.MinValue;
                    }

                    if (requests != null && !closeArrived && batch.Count > 0 && DeferHoldingFlushes(requests, backoffUntil))
                    {
                        requests = null;
                    }

                    if (shouldStop)
                    {
                        yield break;
                    }

                    // Every worker cycle is a consent dispatch point
                    // (mirrors the threaded worker). Cheap no-op while the
                    // outbox is empty.
                    // ... and NOT a worker cadence when this cycle was woken by
                    // an explicit flush: the pace exists to stop the background
                    // loop spinning on a dead token backend, never to refuse a
                    // caller asking for progress now.
                    MaybeKickConsentOutbox(workerCadence: !freshFlush);

                    if (requests != null)
                    {
                        _passTerminal = null;
                        var result = ShardPilotResult.Ok;
                        yield return PublishUntilDrainedCoroutine(batch, consentEpoch, value => result = value);
                        if (closeArrived && !result.Success && result.Retryable && batch.Count > 0)
                        {
                            // One bounded retry on close (mirrors the threaded
                            // worker): a Mode-B 401 already invalidated the cached
                            // JWT, so a fresh token resends the final batch instead
                            // of dropping it at shutdown.
                            yield return PublishUntilDrainedCoroutine(batch, consentEpoch, value => result = value);
                        }
                        if (closeArrived && _experiments != null)
                        {
                            // Shutdown housekeeping, mirroring the threaded
                            // worker: the flush→sweep→flush cycle loops
                            // while the owed count strictly decreases (hard
                            // capped), so a one-slot buffer drains every
                            // owed fact instead of stranding all but the
                            // first. Best-effort by design.
                            var passBudget = _experiments.OwedExposureCount + 1;
                            for (var housekeepingPass = 0; housekeepingPass < passBudget; housekeepingPass++)
                            {
                                var owedBefore = _experiments.OwedExposureCount;
                                try
                                {
                                    _experimentFactDrainWindow = true;
                                    _experiments.ShutdownHousekeeping();
                                }
                                finally
                                {
                                    _experimentFactDrainWindow = false;
                                }

                                lock (_gate)
                                {
                                    DrainQueueInto(batch, consentEpoch);
                                }

                                if (batch.Count > 0)
                                {
                                    yield return PublishUntilDrainedCoroutine(batch, consentEpoch, value => result = value);
                                }

                                var owedAfter = _experiments.OwedExposureCount;
                                if (owedAfter == 0 || owedAfter >= owedBefore)
                                {
                                    break;
                                }
                            }
                        }

                        // Await any in-flight crash send (bounded) before
                        // completing the flush/close, mirroring the threaded
                        // worker. The crash send completion is pumped on this
                        // same main thread, so we YIELD (never block) until the
                        // in-flight count drains or the bound elapses.
                        yield return WaitForPendingCrashSendsCoroutine();

                        // A public flush answers for the whole drain
                        // (mirrors the threaded worker).
                        DateTime? retryAtUtc = null;
                        if (!closeArrived && !result.Success && result.Retryable)
                        {
                            retryAtUtc = RetryDeadlineFor(result);
                        }

                        SettleHoldingFlushes(requests, result, retryAtUtc, batch);
                        var persisted = PersistOutcome.Nothing;
                        if (closeArrived)
                        {
                            // Mirrors the threaded worker: persist the final
                            // undelivered remnant BEFORE the close request
                            // completes (on WebGL there is no spool, so a
                            // remnant answers spool_persist_failed), then
                            // retry the owed consent writes and hold the close
                            // while they stay owed or the drain is gated,
                            // until its deadline.
                            persisted = PersistPendingEventsCore();
                            RetryOwedConsentWriteIfDirty();
                            RetryOwedDenialWitnessWrites();
                            if (HoldCloseUntilDeadline(requests, result, batch))
                            {
                                var holdUntil = DateTime.UtcNow + CloseHoldRetry;
                                while (DateTime.UtcNow < holdUntil && !Volatile.Read(ref _closeDeadlineReached))
                                {
                                    yield return null;
                                }

                                continue;
                            }
                        }
                        if (closeArrived)
                        {
                            // Test seam, mirroring the threaded worker.
                            var preStopHook = CloseTeardownPreStopHookForTesting;
                            if (preStopHook != null)
                            {
                                preStopHook();
                            }

                            // Mirrors the threaded worker: the shutdown durability check is
                            // re-run atomically with the stop flag; debt identified
                            // by ConsentDurabilityOwedLocked() holds close until its deadline.
                            bool consentOwed;
                            var stopped = false;
                            lock (_gate)
                            {
                                consentOwed = ConsentDurabilityOwedLocked();
                                if (consentOwed && !Volatile.Read(ref _closeDeadlineReached))
                                {
                                    HoldCloseRequestsLocked(requests, result);
                                }
                                else
                                {
                                    _workerStopped = true;
                                    stopped = true;
                                }
                            }

                            if (!stopped)
                            {
                                var holdUntil = DateTime.UtcNow + CloseHoldRetry;
                                while (DateTime.UtcNow < holdUntil && !Volatile.Read(ref _closeDeadlineReached))
                                {
                                    yield return null;
                                }

                                continue;
                            }

                            if (_experiments != null)
                            {
                                // Stop the experiments consumer WITH the
                                // teardown (mirrors the threaded worker).
                                _experiments.Teardown();
                            }

                            CompleteAll(requests, consentOwed
                                ? ConsentUnpersistedAtShutdown()
                                : CloseResultFor(result, persisted));
                            yield break;
                        }

                        CompleteAll(requests, result);

                        // An explicit flush that hit a retryable 429 must honor the
                        // server's Retry-After on the next automatic loop, not just
                        // the normal flush interval (mirrors the threaded worker).
                        if (retryAtUtc.HasValue)
                        {
                            // A grant-receipt gate refusal takes no backoff (mirrors the
                            // threaded worker).
                            backoffUntil = retryAtUtc.Value;
                        }
                        else
                        {
                            // Mirrors the threaded worker: the explicit drain
                            // left nothing retained, so a deadline armed for
                            // its earlier failures is stale.
                            backoffUntil = DateTime.MinValue;
                        }
                        nextFlushAtUtc = DateTime.UtcNow + _config.FlushInterval;
                        continue;
                    }

                    // A held flush retries the batch with its own drain.
                    if (batch.Count >= _config.BatchSize && DateTime.UtcNow >= backoffUntil && _heldFlushes.Count == 0)
                    {
                        var result = ShardPilotResult.Ok;
                        yield return PublishCurrentBatchCoroutine(batch, consentEpoch, value => result = value);
                        if (!result.Success && result.Retryable)
                        {
                            backoffUntil = string.Equals(
                                result.Error, ShardPilotConsentOutbox.ReceiptPendingError, StringComparison.Ordinal)
                                ? ConsentGateBackoffUntil()
                                : DateTime.UtcNow + RetryWait(result);
                        }
                        continue;
                    }

                    if (batch.Count > 0 && DateTime.UtcNow >= nextFlushAtUtc && DateTime.UtcNow >= backoffUntil &&
                        _heldFlushes.Count == 0)
                    {
                        var result = ShardPilotResult.Ok;
                        yield return PublishCurrentBatchCoroutine(batch, consentEpoch, value => result = value);
                        if (!result.Success && result.Retryable)
                        {
                            backoffUntil = string.Equals(
                                result.Error, ShardPilotConsentOutbox.ReceiptPendingError, StringComparison.Ordinal)
                                ? ConsentGateBackoffUntil()
                                : DateTime.UtcNow + RetryWait(result);
                        }
                        continue;
                    }

                    yield return null;
                }
            }
            finally
            {
                List<FlushRequest> pending = null;
                lock (_gate)
                {
                    if (_flushRequests.Count > 0)
                    {
                        pending = new List<FlushRequest>(_flushRequests);
                        _flushRequests.Clear();
                    }
                    _workerStopped = true;
                }

                if (pending != null)
                {
                    for (var i = 0; i < pending.Count; i++)
                    {
                        pending[i].Complete(StoppedResultFor(pending[i]));
                    }
                }

                CompleteHeldFlushesOnStop();

                // Mirrors the threaded worker: a stopped client leaves the
                // lifecycle registry (no-op when it never registered), and
                // stops its writer. CloseWriter is a no-op on this profile —
                // WebGL has no writer thread — and is called anyway so the two
                // close paths do not drift.
                ShardPilotDispatcher.UnregisterLifecycleClient(this);
                if (_spool != null)
                {
                    _spool.CloseWriter();
                }
            }
        }

        private IEnumerator PublishUntilDrainedCoroutine(List<ShardPilotEvent> batch, ConsentEpoch consentEpoch, Action<ShardPilotResult> complete)
        {
            ShardPilotResult firstPermanentError = null;
            while (true)
            {
                if (batch.Count > 0)
                {
                    if (Volatile.Read(ref _closeDeadlineReached))
                    {
                        // Mirrors PublishUntilDrained: no publish past the
                        // close's deadline.
                        complete(firstPermanentError ?? new ShardPilotResult(false, ShardPilotFlushCodes.Pending, true));
                        yield break;
                    }

                    var result = ShardPilotResult.Ok;
                    yield return PublishCurrentBatchCoroutine(batch, consentEpoch, value => result = value);
                    if (!result.Success)
                    {
                        if (result.Retryable)
                        {
                            complete(result);
                            yield break;
                        }

                        if (firstPermanentError == null)
                        {
                            firstPermanentError = result;
                        }
                    }
                }

                lock (_gate)
                {
                    DrainQueueInto(batch, consentEpoch);
                    if (_queue.Count == 0 && batch.Count == 0)
                    {
                        complete(DrainedResultLocked(firstPermanentError));
                        yield break;
                    }
                }
            }
        }

        private IEnumerator PublishCurrentBatchCoroutine(List<ShardPilotEvent> batch, ConsentEpoch consentEpoch, Action<ShardPilotResult> complete)
        {
            if (ClosingDecisionPending || Volatile.Read(ref _closeDeadlineReached))
            {
                complete(new ShardPilotResult(false, ShardPilotFlushCodes.Pending, true));
                yield break;
            }
            lock (_gate)
            {
                DropDeniedBatchLocked(batch, consentEpoch);
                EvictCondemnedFactsLocked(batch);
            }

            if (batch.Count == 0)
            {
                complete(ShardPilotResult.Ok);
                yield break;
            }

            var count = batch.Count;
            CancellationTokenSource cts = null;
            Task<ShardPilotBatchResult> task = null;
            var handoff = new TransportHandoff();
            try
            {
                task = PublishCurrentBatchAsync(batch, consentEpoch, out cts, handoff);
            }
            catch (ShardPilotConsentReceiptPendingException)
            {
                if (cts != null)
                {
                    cts.Dispose();
                }

                complete(new ShardPilotResult(false, ShardPilotConsentOutbox.ReceiptPendingError, true));
                yield break;
            }
            catch (Exception ex)
            {
                if (cts != null)
                {
                    cts.Dispose();
                }

                complete(RecordPublishFailure(batch, EffectiveCount(handoff, count), ex, handoff.Handed, handoff));
                yield break;
            }

            while (!task.IsCompleted)
            {
                yield return null;
            }

            try
            {
                complete(RecordPublishOutcome(
                    batch, EffectiveCount(handoff, count), task.GetAwaiter().GetResult(), consentEpoch, handoff));
            }
            catch (ShardPilotConsentReceiptPendingException)
            {
                // grant-receipt dispatch gate: distinct retryable refusal, no
                // failed-batch accounting (see the threaded wrapper).
                complete(new ShardPilotResult(false, ShardPilotConsentOutbox.ReceiptPendingError, true));
            }
            catch (Exception ex)
            {
                complete(RecordPublishFailure(batch, EffectiveCount(handoff, count), ex, handoff.Handed, handoff));
            }
            finally
            {
                cts.Dispose();
            }
        }
#endif

        private ShardPilotResult PublishUntilDrained(List<ShardPilotEvent> batch, ConsentEpoch consentEpoch)
        {
            ShardPilotResult firstPermanentError = null;
            while (true)
            {
                if (batch.Count > 0)
                {
                    if (Volatile.Read(ref _closeDeadlineReached))
                    {
                        // The close's deadline answered it: no further publish.
                        return firstPermanentError ?? new ShardPilotResult(false, ShardPilotFlushCodes.Pending, true);
                    }

                    var result = PublishCurrentBatch(batch, consentEpoch);
                    if (!result.Success)
                    {
                        if (result.Retryable)
                        {
                            return result;
                        }

                        if (firstPermanentError == null)
                        {
                            firstPermanentError = result;
                        }
                    }
                }

                lock (_gate)
                {
                    DrainQueueInto(batch, consentEpoch);
                    if (_queue.Count == 0 && batch.Count == 0)
                    {
                        return DrainedResultLocked(firstPermanentError);
                    }
                }
            }
        }

        private ShardPilotResult PublishCurrentBatch(List<ShardPilotEvent> batch, ConsentEpoch consentEpoch)
        {
            if (ClosingDecisionPending || Volatile.Read(ref _closeDeadlineReached))
                return new ShardPilotResult(false, ShardPilotFlushCodes.Pending, true);
            lock (_gate)
            {
                DropDeniedBatchLocked(batch, consentEpoch);

                // Belt-and-braces like the denial recheck above: a sentinel
                // purge that landed after this batch was drained must not
                // see its withdrawn facts go out on the wire.
                EvictCondemnedFactsLocked(batch);
            }

            if (batch.Count == 0)
            {
                return ShardPilotResult.Ok;
            }

            var count = batch.Count;
            var handoff = new TransportHandoff();
            try
            {
                CancellationTokenSource cts;
                using (cts = new CancellationTokenSource(_config.HttpTimeout))
                {
                    var batchResult = PublishCurrentBatchAsync(batch, cts, consentEpoch, handoff).GetAwaiter().GetResult();
                    return RecordPublishOutcome(batch, EffectiveCount(handoff, count), batchResult, consentEpoch, handoff);
                }
            }
            catch (ShardPilotConsentReceiptPendingException)
            {
                // The grant-receipt dispatch gate held this batch behind a retained
                // undispatched grant receipt: a distinct, retryable refusal
                // — the batch stays retained, nothing was attempted, so no
                // failed-batch accounting and no spool insurance churn (the
                // pause/quit persist still covers a kill).
                return new ShardPilotResult(false, ShardPilotConsentOutbox.ReceiptPendingError, true);
            }
            catch (Exception ex)
            {
                return RecordPublishFailure(batch, EffectiveCount(handoff, count), ex, handoff.Handed, handoff);
            }
        }

        /// <summary>Set exactly at the transport handoff: wire ambiguity for
        /// the batch's exposure facts begins strictly there. A failure with
        /// this still false — a Mode-B bearer resolve throw, a serialization
        /// throw, the receipt-dispatch gate — happened before any batch byte
        /// could leave the device and is unambiguous non-delivery.</summary>
        private sealed class TransportHandoff
        {
            public volatile bool Handed;
            public ShardPilotTransportRequest Request;
            public string ResolvedBearer;
            public string DiagnosticBearer => Request?.ApiKey ?? ResolvedBearer;
            public readonly Dictionary<ShardPilotEvent, ShardPilotAnalyticsClock> AnalyticsStarted =
                new Dictionary<ShardPilotEvent, ShardPilotAnalyticsClock>();

            /// <summary>The batch size actually carried past the publish's
            /// final under-gate condemned-fact eviction (set with it, -1
            /// until the publish reaches that point). Outcome/failure
            /// accounting uses it over the caller's pre-call snapshot so a
            /// fact evicted mid-publish — already counted dropped by the
            /// eviction — is never also counted published or failed.</summary>
            public volatile int SentCount = -1;
        }

        /// <summary>The count the publish outcome/failure accounting should
        /// use: the post-eviction size once the publish reached its final
        /// under-gate re-check, the caller's pre-call snapshot before that
        /// point (bearer-resolve throws, the receipt gate, the epoch
        /// discard).</summary>
        private static int EffectiveCount(TransportHandoff handoff, int count)
        {
            var sent = handoff != null ? handoff.SentCount : -1;
            return sent >= 0 ? sent : count;
        }

        private async Task<ShardPilotBatchResult> PublishCurrentBatchAsync(
            List<ShardPilotEvent> batch,
            CancellationTokenSource cts,
            ConsentEpoch consentEpoch,
            TransportHandoff handoff)
        {
            // Dual-mode Bearer: Mode B awaits the cached/refreshed
            // per-tenant JWT off the game thread (the cancellation token tracks
            // the publish HTTP timeout); Mode A uses the static sp_ key. The
            // anonymous_id is serialized into the payload identically in both
            // modes — only the Authorization Bearer differs. The consent auth
            // epoch is snapshotted FIRST so the receipts-first dispatch below
            // stands down if a consent 401 invalidates the token while this
            // resolve is in flight.
            // The GLOBAL denial epoch as it stood when this publish was
            // authorized, snapshotted at the handoff below. The transport
            // compares it again before it begins the uncompressed fallback;
            // see ShardPilotTransportRequest.StillPermitted.
            long denialEpochAtHandoff;
            long consentAuthEpochAtResolve;
            lock (_gate)
            {
                consentAuthEpochAtResolve = _consentAuthEpoch;
            }

            var bearer = await ResolveBearerAsync(cts.Token).ConfigureAwait(false);
            handoff.ResolvedBearer = bearer;

            // Recheck consent AFTER the bearer resolves and BEFORE the transport
            // send. In Mode B the await above can spend the whole provider
            // latency; a SetConsent(false) that landed during it advances the
            // denial epoch. Without this check, the just-denied pre-denial batch
            // would still be serialized onto the wire (it is only counted dropped
            // afterward, in RecordPublishOutcome). Mirror the DropDeniedBatchLocked
            // epoch mechanism: if the epoch moved, discard the batch instead of
            // sending it. RecordPublishOutcome sees the same epoch change and
            // does the dropped accounting. A denial does NOT touch the
            // receipt trail: the outbox delivers under denied/unknown alike.
            lock (_gate)
            {
                if (consentEpoch.Value != _consentDenialEpoch)
                {
                    return null;
                }
            }

            // Test seam: lets a test land a consent transition inside the
            // window between the epoch check above and the receipt dispatch
            // below. No-op in production.
            var preDispatchHook = ConsentReceiptPreDispatchHookForTesting;
            if (preDispatchHook != null)
            {
                preDispatchHook();
            }

            // ORDERING: hand the
            // outbox HEAD to the sender BEFORE this batch, within this same
            // flush cycle, on this publish's own resolved Bearer. On a
            // strict-enforce workspace a batch that reaches the server
            // before the grant's receipt row exists is terminally suppressed
            // per-event (suppressed_no_consent in the 202), so within one
            // cycle the receipt handoff must precede the batch handoff. This
            // SEQUENCES THE HANDOFFS ONLY: the batch send below proceeds in
            // this same cycle without waiting for the receipt's
            // acknowledgment — ack-gating (variant b) was deliberately not
            // taken. The dispatch runs under the consent dispatch gate, so a
            // decision-time handoff caught mid-flight is waited out (a local
            // enqueue wait, never a network wait) instead of being mistaken
            // for "nothing pending". Mode-agnostic:
            // a Mode-A grant dispatches under the sp_ key and the server's
            // documented terminal 403 drops it and releases the gate.
            //
            // MOST-VOUCHING interplay: this pump rides the PUBLISH's own
            // already-resolved bearer (mode-level: the JWT when a provider
            // exists, else the sp_ key), and the claim's credential-class
            // recheck STANDS DOWN when the current head requires the OTHER
            // class (in dual mode an anon-keyed head rides the publishable
            // key while batches ride the JWT). Ordering is preserved either
            // way: an undispatched GRANT head still holds this batch via
            // the grant-receipt gate below, the flush refusal is retryable, and the
            // worker's consent dispatch point — which resolves the head's
            // OWN credential — delivers it before the next cycle publishes.
            // A stood-down DENIAL head never gated batches to begin with.
            DispatchConsentOutboxHead(bearer, consentAuthEpochAtResolve, _tokenManager == null);

            string payload;
            lock (_gate)
            {
                // A denial can land while the dispatch above waited out an
                // in-flight decision-time handoff: re-check the epoch so the
                // condemned batch never starts its transport handoff
                // (RecordPublishOutcome sees the same epoch move and does
                // the dropped accounting). The receipts already dispatched
                // or retained are unaffected — the trail delivers in order.
                if (consentEpoch.Value != _consentDenialEpoch)
                {
                    return null;
                }

                // The value the fallback veto compares against. It has to be
                // the GLOBAL epoch, not the worker's mirror of it: the mirror
                // only advances inside DropDeniedBatchLocked, which this
                // worker cannot reach while it is blocked awaiting the
                // transport — so a veto reading the mirror can never observe
                // a denial that lands during the compressed attempt, which is
                // the only denial it exists to catch.
                denialEpochAtHandoff = _consentDenialEpoch;

                // A real-subjects sentinel that landed while the bearer and
                // receipt legs above were in flight could only park its
                // batch-resident ids for the worker's NEXT cycle — but THIS
                // cycle's payload has not been built yet, so honor the
                // withdrawal now, atomically with the payload build and the
                // handoff snapshot below: a fact is either evicted here
                // (never serialized, the eviction did the dropped
                // accounting) or snapshotted in-flight and sent, with the
                // wire-ambiguity rules governing from the handoff on.
                EvictCondemnedFactsLocked(batch);
                EvictAgeWithdrawnFactsLocked(batch);

                // Dispatch gate: while ANY analytics-GRANT receipt
                // is retained undispatched — parked behind a Retry-After or
                // backoff window, queued behind another receipt (serial FIFO
                // delivery: only the head can be in flight, and an in-flight
                // head releases the gate ONLY for itself), or reloaded from
                // the durable outbox after a relaunch — the event-batch leg
                // refuses with the distinct consent_receipt_pending reason:
                // events dispatched now would overtake the grant on the wire
                // and be terminally suppressed per-event. An empty pipeline
                // is never gated, so a retained receipt alone cannot block
                // shutdown teardown.
                if (batch.Count > 0 &&
                    _consentOutbox.OwnScope.HasUndispatchedAnalyticsGrant(_consentInFlightKey))
                {
                    throw new ShardPilotConsentReceiptPendingException();
                }

                if (handoff != null)
                {
                    handoff.SentCount = batch.Count;
                }

                if (batch.Count == 0)
                {
                    // The mid-publish eviction withdrew everything this
                    // batch held: nothing to send, and nothing to account —
                    // the eviction already counted the drops. An empty
                    // accepted result keeps the worker loop moving without
                    // failure bookkeeping.
                    return new ShardPilotBatchResult();
                }

                // The payload is serialized INSIDE the gate, atomically
                // with the eviction above and the handoff snapshot below —
                // a sentinel landing after this block finds the facts
                // already snapshotted in-flight and settles them under the
                // wire-ambiguity rules instead of expecting removal.
                payload = ShardPilotJson.SerializeBatch(_config, batch);

            }

            var request = new ShardPilotTransportRequest
            {
                Endpoint = _batchEndpoint,
                ApiKey = bearer,
                Payload = payload,
                // The schema-revision declaration rides the batch publish
                // ONLY, as a request header (never a body field). Empty =
                // the configured stop-declaring escape hatch.
                Timeout = _config.HttpTimeout,
                // The uncompressed fallback is a SECOND request, begun after
                // this method's consent-epoch check has already passed. So a
                // denial that completes mid-flight stops the retry from
                // starting rather than putting the condemned batch on the wire
                // behind it — read against the global denial epoch, which
                // SetConsent(false) advances directly.
                StillPermitted = () => Interlocked.Read(ref _consentDenialEpoch) == denialEpochAtHandoff
            };
            handoff.Request = request;
            request.BeforeSend = () => PrepareAnalyticsOwnedSend(batch, consentEpoch, handoff, request);
            request.BeforeSendStillCurrent = () => AnalyticsPreparationIsCurrent(batch, consentEpoch);
            if (!PrepareAnalyticsRequest(batch, consentEpoch, handoff, request, false,
                _transport is IShardPilotFinalSendGate))
                return new ShardPilotBatchResult();
            lock (_gate)
            {
                if (consentEpoch.Value != _consentDenialEpoch) return null;
                if (handoff != null)
                {
                    handoff.Handed = true;
                }


            }
            return await _transport.PublishAsync(request, cts.Token).ConfigureAwait(false);
        }

        /// <summary>
        /// Resolves the Authorization Bearer for a publish: the cached/refreshed
        /// Mode-B JWT when a token provider is configured, otherwise the static
        /// Mode-A <c>sp_</c> key. A Mode-B refresh failure surfaces as a
        /// retryable publish failure (the worker re-attempts on the next
        /// dispatch rather than dropping the batch).
        /// </summary>
        private Task<string> ResolveBearerAsync(CancellationToken cancellationToken)
        {
            if (_tokenManager == null)
            {
                return Task.FromResult(_config.PublishableKey);
            }

            return _tokenManager.GetTokenAsync(cancellationToken);
        }

#if UNITY_WEBGL && !UNITY_EDITOR
        private Task<ShardPilotBatchResult> PublishCurrentBatchAsync(
            List<ShardPilotEvent> batch,
            ConsentEpoch consentEpoch,
            out CancellationTokenSource cts,
            TransportHandoff handoff)
        {
            cts = new CancellationTokenSource(_config.HttpTimeout);
            return PublishCurrentBatchAsync(batch, cts, consentEpoch, handoff);
        }
#endif

        /// <summary>
        /// Records the outcome of a publish whose transport call has already
        /// returned. The locked epoch check in DropDeniedBatchLocked happens
        /// before the (unlocked, possibly slow) transport send, so a denial
        /// can land while the batch is in flight; rechecking here keeps the
        /// client's posture consistent — the events are counted as Dropped,
        /// never Published, even though the bytes may have reached the server.
        /// </summary>
        private ShardPilotResult RecordPublishOutcome(
            List<ShardPilotEvent> batch,
            int count,
            ShardPilotBatchResult result,
            ConsentEpoch consentEpoch,
            TransportHandoff handoff)
        {
            lock (_gate)
            {
                if (consentEpoch.Value != _consentDenialEpoch)
                {
                    // A non-null result means the server ACCEPTED the batch
                    // (2xx) before the denial landed: the exposure facts in
                    // it are on the wire as a matter of fact, so their
                    // delivery lifecycle records Published even though the
                    // events are counted dropped below — the consent-purge
                    // re-arm must not re-emit facts the server already
                    // holds under a LATER session's distinct ids.
                    if (result != null && _experiments != null)
                    {
                        _experiments.OnFactsDelivered(CollectExperimentFactEventIds(batch));
                    }

                    consentEpoch.Value = _consentDenialEpoch;
                    _stats.RecordDropped(count);
                    // Discarding by denial settles the held batch too (a
                    // racing pause snapshot must not re-spool these events).
                    RecordSettlementLocked(batch);
                    batch.Clear();
                    // The failure streak belonged to the discarded batch: a
                    // deny→re-grant round trip must not start fresh post-grant
                    // events deep in the backoff schedule.
                    _publishBackoffAttempts = 0;
                    return ShardPilotResult.Ok;
                }
            }

            if (_experiments != null)
            {
                // The server accepted the batch (2xx): exposure facts in it
                // are delivered — per-event non-accepted verdicts included
                // (terminal server decisions; the producer lane is
                // decision-gated dark today, so per-event rejects are the
                // expected steady state and must not re-arm forever).
                _experiments.OnFactsDelivered(CollectExperimentFactEventIds(batch));
            }

            return RecordPublishSuccess(batch, count, result, handoff?.DiagnosticBearer);
        }

        private ShardPilotResult RecordPublishSuccess(List<ShardPilotEvent> batch, int count, ShardPilotBatchResult result,
            string requestBearer)
        {
            // The server accepted a batch: the outage (if any) is over, so the
            // hint-less failure streak — and the backoff window it drives —
            // starts from scratch on the next failure.
            _publishBackoffAttempts = 0;
            _stats.RecordBatch(result, count);
            RecordRejections(batch, result, requestBearer);
            SurfaceNonAcceptedOutcomes(result, requestBearer);

            // The server acknowledged the batch (2xx): remove any spooled
            // copies of these events. This single ack-removal path covers both
            // a batch that was spooled after an earlier transient failure or a
            // pause snapshot and then delivered in-process, and entries loaded
            // from a previous launch. Per-event non-accepted verdicts inside
            // the 2xx (rejected/duplicate/...) are terminal server decisions —
            // those events are settled, not necessarily stored, and are never
            // re-sent either. Rejected entries remain in the diagnostic ring.
            //
            // ORDER MATTERS: the settlement is recorded — and the held batch
            // copied-then-cleared — under _gate BEFORE the unlocked file
            // removal. A pause snapshot racing this outcome then either no
            // longer sees the held events (already cleared) or sees the moved
            // settlement counter in its post-append recheck; in both
            // interleavings the delivered events cannot linger on disk. Were
            // the removal to run first, a pause append could land between it
            // and the bump and its recheck would compare a not-yet-moved
            // counter. The file IO itself stays outside _gate so Track
            // callers never stall on spool IO.
            SettleHeldBatch(batch);
            return ShardPilotResult.Ok;
        }

        /// <summary>True for the two experiment fact event names, on LIVE
        /// events (Name populated) and RESTORED spool stubs alike. A
        /// restored event carries only its id and the verbatim serialized
        /// envelope, so the envelope is PARSED and the TOP-LEVEL
        /// <c>event_name</c> checked: props and context serialize as
        /// ordinary nested JSON fields (not an escaped blob), so a raw
        /// byte-sequence match would also classify an innocent event whose
        /// props map carries an <c>event_name</c> key with a fact name as
        /// its value — registering it on the condemnation plane or
        /// fail-closed-dropping it at load. The restored-path-only parse
        /// cost is bounded by the spool caps. An envelope that does not
        /// parse falls back to the byte match: those bytes ship verbatim
        /// on the wire, and the conservative side of a withdrawal is to
        /// keep treating a possible fact as one.</summary>
        private static bool IsExperimentFactEvent(ShardPilotEvent ev)
        {
            if (ev == null)
            {
                return false;
            }

            if (!string.IsNullOrEmpty(ev.Name))
            {
                return string.Equals(ev.Name, "experiment_exposure", StringComparison.Ordinal) ||
                       string.Equals(ev.Name, "experiment_outcome", StringComparison.Ordinal);
            }

            var envelope = ev.SerializedEnvelope;
            if (envelope == null ||
                (!envelope.Contains("\"event_name\":\"experiment_exposure\"") &&
                 !envelope.Contains("\"event_name\":\"experiment_outcome\"")))
            {
                // The byte sequences appear nowhere at all — no parse can
                // find a top-level fact name either (the fast common path).
                return false;
            }

            Dictionary<string, object> parsed;
            if (!ShardPilotJsonParser.TryParseObject(envelope, out parsed))
            {
                return true;
            }

            object name;
            var eventName = parsed.TryGetValue("event_name", out name) ? name as string : null;
            return string.Equals(eventName, "experiment_exposure", StringComparison.Ordinal) ||
                   string.Equals(eventName, "experiment_outcome", StringComparison.Ordinal);
        }

        /// <summary>Describes a RESTORED experiment fact for the
        /// condemnation-plane registration: false for non-facts; true with
        /// the event id plus whatever assignment coordinates
        /// (<c>experiment_key</c>, integral <c>experiment_version</c>,
        /// <c>assignment_key</c>) the envelope's props yield — the
        /// registration recovers the fact's true freshness from the
        /// matching serving entry, so a post-re-enable fact survives a
        /// stale condemnation marker. An unparseable envelope (already
        /// conservatively classified a fact by the byte match) registers
        /// with no coordinates and keeps the floor stamp.</summary>
        private static bool TryDescribeRestoredExperimentFact(
            ShardPilotEvent ev, out ShardPilotRestoredFact fact)
        {
            fact = null;
            if (ev == null || string.IsNullOrEmpty(ev.Id) || !IsExperimentFactEvent(ev))
            {
                return false;
            }

            fact = new ShardPilotRestoredFact { EventId = ev.Id };
            if (ev.TimestampUtc != default(DateTime))
            {
                fact.TimestampUtc = ev.TimestampUtc;
            }

            // A LIVE fact — a handoff snapshot's queue-resident event, not
            // a spool restore — carries its assignment data in Props and
            // normally has NO serialized envelope: read the coordinate
            // triple from there FIRST, mirroring the envelope path's
            // fields exactly. Falling straight to the envelope fallback
            // registered live facts coordinate-less, so a queued
            // retired-subject fact (an owed exposure after a re-mint)
            // registered under the CURRENT scope — and a later
            // real-subjects sentinel purged or spared the wrong fact.
            if (ev.SealedFact != null)
            {
                fact.ExperimentKey = ev.SealedFact.ExperimentKey;
                fact.AssignmentKey = ev.SealedFact.SubjectFactKey;
                fact.Version = ev.SealedFact.ExperimentVersion;
                return true;
            }

            var liveProps = ev.Props;
            if (liveProps != null)
            {
                object liveValue;
                var liveExperimentKey = liveProps.TryGetValue("experiment_key", out liveValue)
                    ? liveValue as string
                    : null;
                var liveAssignmentKey = liveProps.TryGetValue("assignment_key", out liveValue)
                    ? liveValue as string
                    : null;
                long? liveVersion = null;
                if (liveProps.TryGetValue("experiment_version", out liveValue) && liveValue is long)
                {
                    liveVersion = (long)liveValue;
                }

                if (liveExperimentKey != null || liveAssignmentKey != null || liveVersion.HasValue)
                {
                    fact.ExperimentKey = liveExperimentKey;
                    fact.AssignmentKey = liveAssignmentKey;
                    fact.Version = liveVersion;
                    return true;
                }
            }

            var envelope = ev.SerializedEnvelope;
            Dictionary<string, object> parsed;
            if (envelope == null || !ShardPilotJsonParser.TryParseObject(envelope, out parsed))
            {
                return true;
            }

            object propsValue;
            var props = parsed.TryGetValue("props", out propsValue)
                ? propsValue as Dictionary<string, object>
                : null;
            if (props == null)
            {
                return true;
            }

            object value;
            DateTime eventTs;
            if (!fact.TimestampUtc.HasValue && parsed.TryGetValue("event_ts", out value) && value is string &&
                DateTime.TryParse(
                    (string)value, System.Globalization.CultureInfo.InvariantCulture,
                    System.Globalization.DateTimeStyles.AdjustToUniversal |
                    System.Globalization.DateTimeStyles.AssumeUniversal,
                    out eventTs))
            {
                fact.TimestampUtc = eventTs;
            }

            fact.ExperimentKey = props.TryGetValue("experiment_key", out value) ? value as string : null;
            fact.AssignmentKey = props.TryGetValue("assignment_key", out value) ? value as string : null;
            if (props.TryGetValue("experiment_version", out value) && value is long)
            {
                fact.Version = (long)value;
            }

            return true;
        }

        /// <summary>The event ids of the experiment facts in a batch —
        /// exposures AND outcomes, live and RESTORED alike — or null when
        /// it carries none (the common case — no allocation). Outcomes
        /// must settle from the delivery plumbing exactly like exposures:
        /// a delivered outcome id left in the bounded fact-scope index
        /// would occupy (and evict) capacity a later sentinel needs for
        /// real pipeline-resident facts. The ambiguity path tolerates them
        /// (no de-dupe tuple is registered for an outcome, so the burn
        /// mark is a no-op).</summary>
        private static List<string> CollectExperimentFactEventIds(List<ShardPilotEvent> batch)
        {
            List<string> ids = null;
            for (var i = 0; i < batch.Count; i++)
            {
                var ev = batch[i];

                // The envelope-aware check: a RESTORED spool fact carries
                // only its id + serialized envelope, and its delivery must
                // settle it out of the condemnation index exactly like a
                // live emission — a stale delivered id would consume
                // bounded index capacity and later mint bogus purge debt.
                if (ev != null && !string.IsNullOrEmpty(ev.Id) && IsExperimentFactEvent(ev))
                {
                    if (ids == null)
                    {
                        ids = new List<string>();
                    }

                    ids.Add(ev.Id);
                }
            }

            return ids;
        }

        /// <summary>
        /// Settles the worker-held batch after a terminal publish outcome
        /// (2xx delivery or permanent rejection): under <c>_gate</c>, records
        /// the settlement and copies-then-clears the batch; then, OUTSIDE the
        /// lock, removes the settled events' spool copies.
        /// </summary>
        private void SettleHeldBatch(List<ShardPilotEvent> batch)
        {
            List<ShardPilotEvent> settled = null;
            lock (_gate)
            {
                if (batch.Count > 0)
                {
                    RecordSettlementLocked(batch);
                    settled = new List<ShardPilotEvent>(batch);
                    batch.Clear();
                }
            }

            if (settled != null)
            {
                SpoolRemove(settled);

                // Delivered or dead-lettered facts may have been the last
                // ones under a retired fact key.
                PruneExperimentFactKeys();
            }
        }

        /// <summary>
        /// Marks a batch settlement (delivery, permanent rejection, or a
        /// consent-denial discard): bumps the settlement counter and, while a
        /// <see cref="PersistPendingEvents"/> append is in flight, records
        /// the settled events so the persist's post-append reconciliation can
        /// remove exactly the snapshot entries whose own settlement removal
        /// ran before that append put them on disk. Caller must hold
        /// <c>_gate</c> and call BEFORE clearing the batch.
        /// </summary>
        private void RecordSettlementLocked(List<ShardPilotEvent> batch)
        {
            _batchSettlements++;
            if (_persistInFlight && batch.Count > 0)
            {
                if (_settledDuringPersist == null)
                {
                    _settledDuringPersist = new List<ShardPilotEvent>();
                }

                _settledDuringPersist.AddRange(batch);
            }

            // A settled event is no longer pending anywhere: prune it from
            // the overflow-pending record (disk-only overflow events are not
            // publishable in this session, so this is defensive bookkeeping
            // that keeps the denial-time dropped count honest).
            if (_spooledOverflowIds.Count > 0)
            {
                for (var i = 0; i < batch.Count; i++)
                {
                    if (!string.IsNullOrEmpty(batch[i].Id))
                    {
                        _spooledOverflowIds.Remove(batch[i].Id);
                    }
                }
            }
        }

        /// <summary>
        /// Surfaces each non-accepted per-event verdict from a 202 batch
        /// response through the SDK logger so integrators learn their events
        /// were observed (unregistered in the tracking plan), rejected
        /// (blocked/unknown property/schema mismatch), or suppressed by
        /// consent — outcomes that are otherwise silently lost inside a
        /// "successful" 202. These are terminal server decisions, not transport
        /// failures: the batch is never re-sent on their account (a duplicate
        /// in particular is terminal, never retried).
        /// </summary>
        private void SurfaceNonAcceptedOutcomes(ShardPilotBatchResult result, string requestBearer)
        {
            var outcomes = result == null ? null : result.NonAcceptedOutcomes;
            if (outcomes == null || outcomes.Count == 0)
            {
                return;
            }

            for (var i = 0; i < outcomes.Count; i++)
            {
                var outcome = outcomes[i];
                if (outcome == null)
                {
                    continue;
                }

                if (!string.IsNullOrEmpty(outcome.Status) && outcome.Status != "accepted")
                {
                    var issue = string.IsNullOrEmpty(outcome.Code) ? outcome.Status : outcome.Status + ":" + outcome.Code;
                    _stats.RecordEventIssue(ShardPilotDiagnostics.Redact(_config, issue, requestBearer));
                }

                if (_config.Logger == null && outcome.Status == "rejected")
                {
                    WarnAboutRejection(outcome, requestBearer);
                    continue;
                }

                SafeLogAt(outcome.Status == "rejected" ? ShardPilotLogLevel.Warn : ShardPilotLogLevel.Info, requestBearer,
                    "shardpilot event {0} not accepted: status={1} code={2} message={3}",
                    string.IsNullOrEmpty(outcome.EventId) ? "(unknown)" : outcome.EventId,
                    string.IsNullOrEmpty(outcome.Status) ? "(unknown)" : outcome.Status,
                    string.IsNullOrEmpty(outcome.Code) ? "(none)" : outcome.Code,
                    outcome.Message ?? string.Empty);
            }
        }

        private ShardPilotResult RecordPublishFailure(
            List<ShardPilotEvent> batch,
            int count,
            Exception ex,
            bool handedToTransport,
            TransportHandoff handoff = null)
        {
            var retryable = IsRetryableForPublish(ex);
            var retryAfter = RetryAfterFor(ex);
            var message = ShardPilotDiagnostics.Redact(_config, SafeErrorMessage(ex), handoff?.DiagnosticBearer);
            var failure = new ShardPilotResult(false, message, retryable, retryAfter);

            // Eligibility and terminal publication are one gate transaction.
            // The timer reads the terminal through this same gate, so it cannot
            // choose pending between the scan and publication. No production
            // rollback, logging or settlement work runs in this hold.
            var oldest = long.MaxValue;
            var ownedByClose = false;
            lock (_gate)
            {
                for (var i = 0; i < batch.Count; i++)
                {
                    oldest = Math.Min(oldest, batch[i].IntakeSequence);
                    ownedByClose |= _closeCompletion != null && ReferenceEquals(batch[i].CloseOwner, _closeCompletion);
                }

                CloseTerminalBeforeRecordHookForTesting?.Invoke();
                if (!retryable && _closeCompletion != null && !_closeCompletion.Task.IsCompleted &&
                    (_owningCloseRequest == null || oldest <= _owningCloseRequest.Boundary || ownedByClose) &&
                    _closeTerminal == null)
                    _closeTerminal = failure;
            }

            if (handoff != null)
            {
                var request = handoff.Request;
                var status = ex as ShardPilotHttpStatusException;
                var definite = !handedToTransport ||
                    (request != null && request.DeliveryEvidenceKnown && request.DefinitelyNotDelivered) ||
                    ((request == null || !request.DeliveryEvidenceKnown) && status != null &&
                     status.StatusCode >= 400 && status.StatusCode < 500);
                if (definite) RollbackAnalyticsAttempt(handoff, true);
            }

            _stats.RecordFailure(message);
            SafeLogAt(ShardPilotLogLevel.Error, null, "shardpilot batch publish failed: {0}", message);
            if (IsSchemaRevisionMismatch(ex))
            {
                SafeLog(
                    "shardpilot schema revision mismatch: ingest rejected the batch terminally (409 {0}); " +
                    "it is never retried. This engine SDK declares no revision; inspect the server compatibility policy.",
                    SchemaRevisionMismatchCode);
            }
            if (retryable && retryAfter <= TimeSpan.Zero)
            {
                // A retryable failure the server attached no Retry-After to:
                // advance the hint-less failure streak so RetryWait's backoff
                // fallback grows. A hinted failure leaves the streak alone —
                // the server is pacing the fleet, not the client.
                _publishBackoffAttempts++;
            }
            else if (!retryable && ex is ShardPilotHttpStatusException)
            {
                // A permanent HTTP response is still a RESPONSE: the endpoint
                // answered, so the unreachability streak is over — the next
                // batch's first transient failure must start at the flush
                // cadence, not deep in a schedule that belonged to the batch
                // dropped below. Client-side permanent failures (encode,
                // serialization) say nothing about the endpoint and leave the
                // streak alone.
                _publishBackoffAttempts = 0;
            }

            if (!retryable)
            {
                _stats.RecordDropped(count);

                // The batch's experiment facts terminally leave the
                // pipeline with the drop below: settle their ids out of the
                // condemnation-plane index (the same envelope-aware
                // collector the delivery settle uses) — ids leave the index
                // whenever their facts terminally leave the pipeline,
                // delivered OR permanently dropped. Squatting ids would
                // evict live queued ids from the bounded index and hand a
                // later sentinel ghosts to collect. An index settle only:
                // delivery state and the once-per-fact drop accounting
                // above stay untouched. Collected BEFORE SettleHeldBatch
                // clears the batch.
                if (_experiments != null)
                {
                    _experiments.OnFactsSettledWithoutDelivery(CollectExperimentFactEventIds(batch));
                }

                // A permanently rejected batch (non-retryable 4xx) is also
                // removed from the offline spool: re-sending it on a later
                // launch would fail forever. The rejection itself was logged
                // above. Same settle-then-remove ordering discipline as the
                // success path.
                SettleHeldBatch(batch);
            }
            else
            {
                // Insurance for a transient failure (connection error,
                // timeout, 429, 5xx): persist the held batch AND a snapshot
                // of the queued remnant so a sustained outage survives the
                // app being killed — with the queue full, the held batch
                // alone would be the only insured segment while everything
                // behind it stayed memory-only. The in-memory retry behavior
                // is unchanged: the worker retains the batch, delivery keeps
                // draining the queue, and the eventual acknowledgment removes
                // the spooled copies (Append de-duplicates by event id, so
                // repeated failures only write events not yet spooled).
                // Skipped when a consent denial landed after this batch was
                // drained: the epoch moved, the worker will discard the batch,
                // and the denial already wiped the spool.
                long epochBefore = 0;
                List<ShardPilotEvent> insurance = null;
                lock (_gate)
                {
                    epochBefore = _consentDenialEpoch;
                    if (_workerConsentEpoch.Value == epochBefore && _spool != null)
                    {
                        insurance = new List<ShardPilotEvent>(batch.Count + _queue.Count);
                        AddUncondemnedLocked(insurance, batch);
                        AddUncondemnedLocked(insurance, _queue);
                        if (insurance.Count > 0)
                        {
                            // Counted in the SAME hold as the snapshot (like
                            // the persist append), so a sentinel purge in
                            // the snapshot-to-append window defers its
                            // marker retirement past this append.
                            _spoolAppendsInFlight++;
                        }
                        else
                        {
                            insurance = null;
                        }
                    }
                }

                if (insurance != null)
                {
                    try
                    {
                        // Test seam: lets a test land a SetConsent(false) inside
                        // the snapshot-to-append window. No-op in production.
                        var preAppendHook = SpoolPreAppendHookForTesting;
                        if (preAppendHook != null)
                        {
                            preAppendHook();
                        }

                        SpoolAppend(insurance);

                        // The append ran unlocked, so a SetConsent(false) can have
                        // landed in between: its spool wipe may have run BEFORE
                        // the append, which would put the just-denied events back
                        // on disk. Re-check the epoch AFTER the append and undo
                        // this append if a denial raced it (removal is idempotent
                        // and keyed by event id, so a wipe that ran after the
                        // append — already clean — is unaffected).
                        bool denialLanded;
                        lock (_gate)
                        {
                            denialLanded = _consentDenialEpoch != epochBefore;
                        }

                        if (denialLanded)
                        {
                            RemoveDeniedOrFailClosed(insurance);
                        }
                    }
                    finally
                    {
                        lock (_gate)
                        {
                            _spoolAppendsInFlight--;
                        }

                        // The landed append re-runs the marker-driven
                        // removal, retiring anything a mid-append sentinel
                        // deferred. A clean marker makes this a no-op.
                        RetryCondemnedFactPurgeDebt();
                    }
                }
            }

            if (!retryable && _passTerminal == null)
            {
                _passTerminal = failure;
                _passTerminalOldest = oldest;
            }

            return failure;
        }

        /// <summary>
        /// Mode-aware retry classification for a publish failure. In Mode B
        /// (token provider configured) a 401 is an auth problem, not a permanent
        /// rejection: drop the stale cached JWT so the next attempt refreshes it,
        /// and treat the failure as retryable so the batch is retained and
        /// re-sent with a fresh Bearer (mirroring Defold client.lua's
        /// unauthorized handling). A token-provider failure
        /// (<see cref="ShardPilotTokenException"/>) is likewise retryable — the
        /// host's backend may be momentarily unreachable. Mode A keeps the base
        /// <see cref="IsRetryable"/> classification (a 401 there is a bad sp_ key
        /// and stays permanent).
        /// </summary>
        private bool IsRetryableForPublish(Exception ex)
        {
            if (IsSchemaRevisionMismatch(ex))
            {
                // Terminal by server contract: the declared schema revision
                // can never match this server from this build, so retrying
                // the identical batch is guaranteed to 409 again. Classified
                // FIRST, by error.code (never by the 409 status alone — the
                // ingest service shares 409 with other conflict codes), so
                // this batch stays terminal even if some other 409 shape ever
                // gains a retry or split path.
                return false;
            }

            if (_tokenManager != null)
            {
                if (ex is ShardPilotTokenException)
                {
                    return true;
                }

                var status = ex as ShardPilotHttpStatusException;
                if (status != null && status.StatusCode == 401)
                {
                    // The server rejected the JWT: re-ask the provider on the
                    // next attempt instead of resending the same stale Bearer.
                    _tokenManager.Invalidate();
                    return true;
                }
            }

            return IsRetryable(ex);
        }

        /// <summary>
        /// The ingest service's error code for a batch whose declared
        /// <c>X-ShardPilot-Schema-Revision</c> does not match the schema set
        /// the server serves. 409 with this code is TERMINAL for the batch.
        /// </summary>
        private const string SchemaRevisionMismatchCode = "schema_revision_mismatch";

        /// <summary>
        /// True when <paramref name="ex"/> is the ingest service's 409
        /// schema-revision-mismatch rejection, discriminated by
        /// <c>error.code</c> — never by the 409 status alone, which the
        /// server shares with unrelated conflict codes.
        /// </summary>
        internal static bool IsSchemaRevisionMismatch(Exception ex)
        {
            var status = ex as ShardPilotHttpStatusException;
            return status != null && status.StatusCode == 409 &&
                   string.Equals(status.ErrorCode, SchemaRevisionMismatchCode, StringComparison.Ordinal);
        }

        /// <summary>
        /// The delay before the worker's next publish attempt after a
        /// retryable failure: the server's <c>Retry-After</c> hint (429
        /// backpressure) when one was given, else the client-side full-jitter
        /// exponential backoff for the current hint-less failure streak.
        ///
        /// It is NOT floored at the flush interval any more, and that floor
        /// was the defect. Retry pacing and batching cadence are different
        /// questions — an integrator lengthening the idle flush interval is
        /// asking for fewer requests when nothing is happening, not for a
        /// slower recovery when something is wrong — and the floor made the
        /// second answer the first. Concretely, a server saying
        /// <c>Retry-After: 2</c> was obeyed at 2s only while the flush
        /// interval was shorter than that; at 15s the client waited 15.
        ///
        /// The backoff schedule is its own floor now: every failure, the
        /// first included, defers by at least <c>PublishBackoffBaseSeconds</c>.
        /// </summary>
        private TimeSpan RetryWait(ShardPilotResult result)
        {
            var retryAfter = result == null ? TimeSpan.Zero : result.RetryAfter;
            return retryAfter > TimeSpan.Zero ? retryAfter : NextPublishBackoffWait();
        }

        // Full-jitter exponential-backoff fallback for retryable publish
        // failures without a server Retry-After hint, mirroring the
        // shardpilot-defold reference semantics: window [base, base·2^(n−2)]
        // capped at 60s.
        private const double PublishBackoffBaseSeconds = 1.0;
        private const double PublishBackoffCapSeconds = 60.0;

        // The crash-plane re-send floor. Deliberately its own constant rather
        // than a reuse of PublishBackoffBaseSeconds: they happen to share a
        // value today, and a future change to analytics publish backoff must
        // not silently move crash re-send pacing with it.
        private const double CrashResendRetryFloorSeconds = 1.0;

        /// <summary>
        /// The backoff wait for the given consecutive hint-less-failure count:
        /// the base for the first failure (it no longer borrows the flush
        /// cadence), then a uniform random duration in
        /// [base, ceiling] with the ceiling doubling per failure
        /// (base·2^(attempt−2)) up to the 60s cap — never below the base, so a
        /// dead endpoint always gets breathing room, and jittered so a fleet
        /// of clients does not retry in lockstep. <paramref name="jitter"/> is
        /// a uniform [0, 1) draw; the exponent is clamped so an arbitrarily
        /// long outage cannot overflow.
        /// </summary>
        internal static TimeSpan PublishBackoffWait(int attempt, double jitter)
        {
            if (attempt < 1)
            {
                return TimeSpan.Zero;
            }

            // The FIRST failure shares the second's window rather than having
            // none. Its old zero meant "no backoff", which handed the retry to
            // RetryWait's FlushInterval floor — indistinguishable from prompt
            // while that interval was one second, and a fifteen-second stall
            // at the interval this SDK is moving to. Giving it exactly the
            // base reproduces the timing the old default actually delivered
            // while making it independent of the batching cadence.
            var exp = attempt - 2;
            if (exp < 0)
            {
                exp = 0;
            }

            if (exp > 16)
            {
                exp = 16;
            }

            var ceiling = PublishBackoffBaseSeconds * Math.Pow(2.0, exp);
            if (ceiling > PublishBackoffCapSeconds)
            {
                ceiling = PublishBackoffCapSeconds;
            }

            return TimeSpan.FromSeconds(PublishBackoffBaseSeconds + jitter * (ceiling - PublishBackoffBaseSeconds));
        }

        private TimeSpan NextPublishBackoffWait()
        {
            var hook = PublishBackoffJitterForTesting;
            var jitter = hook != null ? hook() : _publishBackoffJitter.NextDouble();
            return PublishBackoffWait(_publishBackoffAttempts, jitter);
        }

        /// <summary>
        /// Extracts the whole-seconds <c>Retry-After</c> the server attached to
        /// a 429 so the worker can defer its next publish by at least that long.
        /// Clamped to a one-day sanity bound (the monthly hard cap can advertise
        /// up to 86400s); absent for any non-status error.
        /// </summary>
        private static TimeSpan RetryAfterFor(Exception ex)
        {
            var status = ex as ShardPilotHttpStatusException;
            if (status == null || status.RetryAfterSeconds <= 0)
            {
                return TimeSpan.Zero;
            }

            var seconds = Math.Min(status.RetryAfterSeconds, 86400);
            return TimeSpan.FromSeconds(seconds);
        }

        private ShardPilotResult DrainedResultLocked(ShardPilotResult firstPermanentError)
        {
            return firstPermanentError ?? (_restoredPending.Count > 0
                ? new ShardPilotResult(false, ShardPilotFlushCodes.Pending, true) : ShardPilotResult.Ok);
        }

        /// <summary>
        /// Reconciles the held batch against consent and condemned facts, then
        /// fills it from the restored/live queues. Caller must hold _gate.
        ///
        /// Returns whether reconciliation DISCARDED a held batch — the whole of
        /// it, not part. The caller owns a partial-batch deadline that belongs
        /// to the batch that armed it, and a discard here can be followed
        /// immediately by a refill in the same call, so the list is non-empty
        /// on both sides of it and no later emptiness check can see that the
        /// batch was replaced. Measured on the count rather than reported by
        /// the two helpers, so a future path that empties the batch is covered
        /// by construction.
        ///
        /// A PARTIAL eviction deliberately does not count: the events left
        /// behind are the ones that armed the deadline, and their age is what
        /// it bounds.
        /// </summary>
        private bool DrainQueueInto(List<ShardPilotEvent> batch, ConsentEpoch consentEpoch)
        {
            var held = batch.Count;
            DropDeniedBatchLocked(batch, consentEpoch);
            EvictCondemnedFactsLocked(batch);
            var discarded = held > 0 && batch.Count == 0;

            // CONSENT-FIRST belt-and-braces: while consent is anything but an
            // explicit grant, the worker never assembles a batch. The enqueue
            // gate already keeps the queue empty in non-granted states and
            // the constructor purges (never loads) the spool without a
            // persisted grant, so this is structural insurance that no code
            // path can drain events toward the wire without a grant.
            if (_consentState != ShardPilotConsentState.Granted)
            {
                return discarded;
            }

            // Events restored from the offline spool are drained AHEAD of the
            // live queue (oldest first), chunked to the same batch size and
            // published through the same path — including the consent epoch
            // checks — as freshly tracked events.
            var restoredCount = _restoredPending.Count;
            for (var i = 0; i < restoredCount && batch.Count < _config.BatchSize; i++)
            {
                var restored = _restoredPending.Dequeue();
                ShardPilotRestoredFact fact;
                if (TryDescribeRestoredExperimentFact(restored, out fact) &&
                    (_withdrawalFramesUnknown || _experiments == null || !_experiments.MayPublishRestoredFact(fact.ExperimentKey)))
                {
                    // Sealed experiment facts are client-id applications.
                    // Keep their signed bytes and spool claim while allowing
                    // ordinary analytics behind them to make progress.
                    _restoredPending.Enqueue(restored);
                }
                else batch.Add(restored);
            }

            // Blast-radius isolation: a batch carrying restored entries
            // publishes ALONE — it is never topped up with fresh queue
            // events, which ride the next cycle instead (one flush cycle of
            // added latency, by design). A restored envelope from a previous
            // launch is the likeliest to be permanently rejected (its
            // registration may have changed server-side since), and a
            // permanent rejection drops the WHOLE batch: mixing would drag
            // fresh events down with it. Restored entries only ever occupy a
            // batch from its start (they are drained first and never refill
            // after construction), so checking the first element suffices.
            if (batch.Count > 0 && !string.IsNullOrEmpty(batch[0].SerializedEnvelope))
            {
                return discarded;
            }

            while (batch.Count < _config.BatchSize && _queue.Count > 0)
            {
                batch.Add(_queue.Dequeue());
            }

            return discarded;
        }

        /// <summary>
        /// Discards the worker-held batch when a consent denial happened after
        /// those events were drained, and re-synchronizes the worker's observed
        /// denial epoch. Reconciling before every drain keeps pre-denial events
        /// from ever mixing with post-grant events, and reconciling again right
        /// before each publish keeps a just-denied batch from going out on the
        /// wire. Caller must hold _gate.
        /// </summary>
        private void DropDeniedBatchLocked(List<ShardPilotEvent> batch, ConsentEpoch consentEpoch)
        {
            if (consentEpoch.Value == _consentDenialEpoch)
            {
                return;
            }

            consentEpoch.Value = _consentDenialEpoch;
            if (batch.Count > 0)
            {
                _stats.RecordDropped(batch.Count);
                // Discarding by denial settles the held batch too (a racing
                // pause snapshot must not re-spool these events).
                RecordSettlementLocked(batch);

                // A sentinel may have indexed facts sitting in this batch
                // for the worker's eviction pass; the denial discards them
                // FIRST, so that pass would see an empty batch and the ids
                // would squat in the bounded condemnation indexes. A
                // denial-discarded fact is out of the pipeline — release
                // its index entries with it.
                if (_condemnedFactIds.Count > 0 || _condemnedBatchOverflow.Count > 0)
                {
                    for (var i = 0; i < batch.Count; i++)
                    {
                        var discarded = batch[i];
                        if (discarded != null && !string.IsNullOrEmpty(discarded.Id))
                        {
                            _condemnedFactIds.Remove(discarded.Id);
                            _condemnedBatchOverflow.Remove(discarded.Id);
                        }
                    }
                }

                batch.Clear();
                // The failure streak belonged to the discarded batch (mirrors
                // the in-flight-denial reset in RecordPublishOutcome).
                _publishBackoffAttempts = 0;
            }
        }

        internal static bool IsRetryable(Exception ex)
        {
            var status = ex as ShardPilotHttpStatusException;
            if (status != null)
            {
                return status.Retryable;
            }

            var transport = ex as ShardPilotTransportException;
            if (transport != null)
            {
                return transport.Retryable;
            }

            return ex is OperationCanceledException || ex is ShardPilotAnalyticsClockWriteException;
        }

        private static string SafeErrorMessage(Exception ex)
        {
            var status = ex as ShardPilotHttpStatusException;
            if (status != null)
            {
                return status.Message;
            }

            return ex.GetType().Name + ": " + ex.Message;
        }

        private void CompleteAll(List<FlushRequest> requests, ShardPilotResult result)
        {
            if (result.Success && _experiments != null && _experiments.HasUnadmittedApplications)
                result = new ShardPilotResult(false, ShardPilotFlushCodes.Pending, true);

            for (var i = 0; i < requests.Count; i++)
            {
                requests[i].Complete(result);
            }
        }

        /// <summary>
        /// Answers the holding flushes a drain served and removes them from
        /// <paramref name="requests"/>; close requests and the close drain's
        /// own flushes stay for the caller. Each flush answers for the events
        /// accepted before its call: once none of them is left in the
        /// pipeline it answers success (they were delivered or removed
        /// locally), unless an experiment application is still unadmitted. A
        /// terminal rejection that reaches back to its events answers it at
        /// once. Otherwise a retryable drain holds it for the worker to retry
        /// at <paramref name="retryAtUtc"/> (null: the failure's own retry
        /// deadline, taken only when something is held). Worker only.
        /// </summary>
        private void SettleHoldingFlushes(List<FlushRequest> requests, ShardPilotResult result, DateTime? retryAtUtc,
            List<ShardPilotEvent> batch)
        {
            long oldestPending;
            lock (_gate)
            {
                oldestPending = OldestPendingSequenceLocked(batch);
            }

            var unadmitted = _experiments != null && _experiments.HasUnadmittedApplications;
            var retryable = !result.Success && result.Retryable;
            var receiptGated = retryable && string.Equals(
                result.Error, ShardPilotConsentOutbox.ReceiptPendingError, StringComparison.Ordinal);
            for (var i = requests.Count - 1; i >= 0; i--)
            {
                var request = requests[i];
                if (!request.Holds)
                {
                    continue;
                }

                requests.RemoveAt(i);
                if (_passTerminal != null && _passTerminalOldest <= request.Boundary)
                {
                    request.Complete(_passTerminal);
                    continue;
                }

                var ownEventsLeft = oldestPending > request.Boundary;
                if (ownEventsLeft ? !unadmitted : !retryable)
                {
                    request.Complete(ownEventsLeft ? ShardPilotResult.Ok : result);
                    continue;
                }

                request.ReceiptGated = receiptGated;
                _heldFlushes.Add(request);
            }

            if (_heldFlushes.Count > 0)
            {
                _heldFlushRetryAtUtc = retryable
                    ? retryAtUtc ?? RetryDeadlineFor(result)
                    : DateTime.UtcNow + HeldFlushApplicationRecheck;
            }
        }

        /// <summary>
        /// The lowest intake sequence still in the pipeline: the retained
        /// batch, the queue and the restored backlog. Caller holds _gate.
        /// </summary>
        private long OldestPendingSequenceLocked(List<ShardPilotEvent> batch)
        {
            var oldest = long.MaxValue;
            foreach (var restored in _restoredPending)
            {
                oldest = Math.Min(oldest, restored.IntakeSequence);
            }

            for (var i = 0; i < batch.Count; i++)
            {
                oldest = Math.Min(oldest, batch[i].IntakeSequence);
            }

            if (_queue.Count > 0)
            {
                oldest = Math.Min(oldest, _queue.Peek().IntakeSequence);
            }

            return oldest;
        }

        /// <summary>
        /// What a close answers for its final drain: <c>spool_persist_failed</c>
        /// when events are left that storage did not take, a terminal
        /// rejection from any of its passes (it survives the bounded retry and
        /// the shutdown housekeeping passes that follow), success, or
        /// <c>pending</c> when the drain ended retryable and the undelivered
        /// remainder is on disk. Worker only.
        /// </summary>
        private ShardPilotResult CloseResultFor(ShardPilotResult result, PersistOutcome persisted)
        {
            if (persisted == PersistOutcome.Failed)
            {
                return new ShardPilotResult(false, ShardPilotFlushCodes.SpoolPersistFailed, false);
            }

            var terminal = CloseTerminalFor();
            if (terminal != null)
            {
                return terminal;
            }

            if (!result.Success && result.Retryable)
            {
                bool receiptGated;
                lock (_gate) receiptGated = CloseReceiptGateHeldLocked();
                return new ShardPilotResult(false, receiptGated
                    ? ShardPilotFlushCodes.ConsentReceiptPending
                    : ShardPilotFlushCodes.Pending, true);
            }
            return result;
        }

        private ShardPilotResult CloseTerminalFor(FlushRequest request = null)
        {
            lock (_gate)
            {
                return _owningCloseRequest != null &&
                    (request == null || ReferenceEquals(request, _owningCloseRequest)) ? _closeTerminal : null;
            }
        }

        // Both timer and worker completions classify the live scope's remnant,
        // so neither a missing request flag nor the task race changes its code.
        private bool CloseReceiptGateHeldLocked()
        {
            var heldCounts = _workerConsentEpoch.Value == _consentDenialEpoch && _workerBatch.Count > 0;
            return (_queue.Count > 0 || heldCounts) &&
                   _consentOutbox.OwnScope.HasUndispatchedAnalyticsGrant(_consentInFlightKey);
        }

        /// <summary>
        /// A close's answer at its deadline, on the timer thread. The remnant
        /// is persisted, and the worker is told to tear down after any publish
        /// in flight: nothing is kept alive past the deadline. Owed consent
        /// writes and a remnant storage did not take answer
        /// <c>spool_persist_failed</c>; a remnant held behind an undispatched
        /// consent receipt answers <c>consent_receipt_pending</c>.
        /// </summary>
        private ShardPilotResult CloseDeadlineAnswer(FlushRequest request)
        {
            // Fence new publishes before any potentially blocked persistence I/O.
            Volatile.Write(ref _closeDeadlineReached, true);
            var persisted = PersistPendingEventsCore();
            bool consentOwed;
            bool receiptGated;
            lock (_gate)
            {
                consentOwed = ConsentDurabilityOwedLocked();
                receiptGated = CloseReceiptGateHeldLocked();
            }

            WakeWorker();
            if (consentOwed)
            {
                return ConsentUnpersistedAtShutdown();
            }

            if (persisted == PersistOutcome.Failed)
            {
                return new ShardPilotResult(false, ShardPilotFlushCodes.SpoolPersistFailed, false);
            }

            var terminal = CloseTerminalFor(request);
            if (terminal != null) return terminal;

            return new ShardPilotResult(false, receiptGated
                ? ShardPilotFlushCodes.ConsentReceiptPending
                : ShardPilotFlushCodes.Pending, true);
        }

        /// <summary>
        /// The answer of a close that tears down with consent writes still
        /// owed, recorded once: the diagnostic, the counter, and the standing
        /// denial kept in force in memory for the rest of the process.
        /// </summary>
        private ShardPilotResult ConsentUnpersistedAtShutdown()
        {
            // Every answer publishes the witness before host diagnostics;
            // the reporting flag only suppresses duplicate diagnostics.
            lock (_gate)
            {
                _closingDecision?.KeepDenial(_settingsStore, _config.AnonymousId);
                KeepUnbackedDenialForProcessLocked();
            }
            if (Interlocked.Exchange(ref _consentUnpersistedReported, 1) == 0)
            {
                _stats.RecordFailure("consent_unpersisted_at_shutdown");
                _stats.RecordConsentWritesUnpersistedAtShutdown();
                SafeLog("shardpilot close tore down with consent writes still owed; a denial stays in force for this process");
            }

            return new ShardPilotResult(false, ShardPilotFlushCodes.SpoolPersistFailed, false);
        }

        private static readonly TimeSpan CloseHoldRetry = TimeSpan.FromMilliseconds(250);

        /// <summary>
        /// Holds the pass's close requests while the final drain is gated
        /// behind a retained grant receipt, until it clears or the close's
        /// deadline answers it. True when held. Owed consent writes are held
        /// by the teardown re-check, which runs atomically with the stop
        /// flag. Worker only.
        /// </summary>
        private bool HoldCloseUntilDeadline(List<FlushRequest> requests, ShardPilotResult result, List<ShardPilotEvent> batch)
        {
            lock (_gate)
            {
                var receiptGated = !result.Success &&
                                   result.Retryable &&
                                   string.Equals(result.Error, ShardPilotConsentOutbox.ReceiptPendingError, StringComparison.Ordinal) &&
                                   (batch.Count > 0 || _queue.Count > 0);
                for (var i = 0; i < requests.Count; i++)
                {
                    if (requests[i].Close)
                    {
                        requests[i].ReceiptGated = receiptGated;
                    }
                }

                if (!receiptGated || Volatile.Read(ref _closeDeadlineReached))
                {
                    return false;
                }

                HoldCloseRequestsLocked(requests, result);
                return true;
            }
        }

        /// <summary>
        /// Requeues the close requests and answers their coalesced one-attempt
        /// drains with this pass's result. Caller holds _gate.
        /// </summary>
        private void HoldCloseRequestsLocked(List<FlushRequest> requests, ShardPilotResult result)
        {
            var drains = new List<FlushRequest>();
            for (var i = requests.Count - 1; i >= 0; i--)
            {
                if (requests[i].Close)
                {
                    _flushRequests.Insert(0, requests[i]);
                }
                else
                {
                    drains.Add(requests[i]);
                }
            }
            CompleteAll(drains, result);
        }

        /// <summary>
        /// A teardown with a standing denial that storage never witnessed keeps
        /// that denial in force for the rest of the process: a later client on
        /// the same storage and actor boots denied and takes over the owed
        /// writes. Caller holds _gate.
        /// </summary>
        private void KeepUnbackedDenialForProcessLocked()
        {
            if (_settingsStore != null && DenialWitnessUnbackedLocked())
            {
                ShardPilotConsent.KeepSessionDenial(_settingsStore, _config.AnonymousId, _consentState,
                    _consentDenialPendingDecidedAt, _consentDenialPendingSeq);
            }
        }

        /// <summary>
        /// Consent writes a teardown would leave owed only in memory: retained
        /// receipts not durably persisted, an owed recovery downgrade or
        /// withheld-marker discharge, or a standing denial with no durable
        /// witness. Caller holds _gate.
        /// </summary>
        private bool ConsentDurabilityOwedLocked()
        {
            return ClosingDecisionPending || (_consentOutbox.Pending &&
                    (!_consentOutbox.HasDurableBackend ||
                     _consentOutbox.Dirty ||
                     _consentOutbox.RecoveryDroppedPendingGrant || _consentOutbox.RecoveryMergedParkedPendingGrant)) ||
                   _withheldMarkerDischargeOwed ||
                   DenialWitnessUnbackedLocked();
        }

        /// <summary>
        /// Moves the held flushes that are due (or every one, when a new
        /// flush arrived) into this pass's requests. Caller holds _gate.
        /// </summary>
        private List<FlushRequest> TakeFlushRequestsLocked(out bool fresh)
        {
            fresh = _flushRequests.Count > 0;
            if (!fresh && (_heldFlushes.Count == 0 || DateTime.UtcNow < _heldFlushRetryAtUtc))
            {
                return null;
            }

            var requests = new List<FlushRequest>(_heldFlushes.Count + _flushRequests.Count);
            for (var i = 0; i < _heldFlushes.Count; i++)
            {
                // The deadline already answered these.
                if (!_heldFlushes[i].Task.IsCompleted)
                {
                    requests.Add(_heldFlushes[i]);
                }
            }

            _heldFlushes.Clear();
            requests.AddRange(_flushRequests);
            _flushRequests.Clear();
            return requests.Count > 0 ? requests : null;
        }

        /// <summary>
        /// Holds a pass made only of public flushes until an active retry
        /// deadline passes: a server Retry-After is a floor for the next
        /// attempt, and a flush waits anyway. A close or a one-attempt drain
        /// in the pass still goes now. The caller defers only while a batch is
        /// retained; with nothing retained the pass answers at once. Worker only.
        /// </summary>
        private bool DeferHoldingFlushes(List<FlushRequest> requests, DateTime backoffUntilUtc)
        {
            if (DateTime.UtcNow >= backoffUntilUtc)
            {
                return false;
            }

            for (var i = 0; i < requests.Count; i++)
            {
                if (!requests[i].Holds)
                {
                    return false;
                }
            }

            // A flush that waits behind the consent receipt gate names it at
            // its deadline, attempt or not.
            bool receiptGated;
            lock (_gate)
            {
                receiptGated = _consentOutbox.OwnScope.HasUndispatchedAnalyticsGrant(_consentInFlightKey);
            }

            if (receiptGated)
            {
                for (var i = 0; i < requests.Count; i++)
                {
                    requests[i].ReceiptGated = true;
                }
            }

            _heldFlushes.AddRange(requests);
            _heldFlushRetryAtUtc = backoffUntilUtc;
            return true;
        }

        /// <summary>
        /// The retry deadline a retryable drain failure takes: a consent
        /// receipt gate waits for the receipt plane, anything else backs off
        /// (honouring a server Retry-After).
        /// </summary>
        private DateTime RetryDeadlineFor(ShardPilotResult result)
        {
            return string.Equals(result.Error, ShardPilotConsentOutbox.ReceiptPendingError, StringComparison.Ordinal)
                ? ConsentGateBackoffUntil()
                : DateTime.UtcNow + RetryWait(result);
        }

        /// <summary>Caps a worker wait so a held flush retries on time.</summary>
        private TimeSpan CapWaitToHeldFlush(TimeSpan wait)
        {
            if (_heldFlushes.Count == 0)
            {
                return wait;
            }

            var untilRetry = _heldFlushRetryAtUtc - DateTime.UtcNow;
            if (untilRetry < TimeSpan.Zero)
            {
                untilRetry = TimeSpan.Zero;
            }

            return untilRetry < wait ? untilRetry : wait;
        }

        /// <summary>
        /// Answers every held flush when the worker stops: plain flushes
        /// answer shutdown. Worker only.
        /// </summary>
        private void CompleteHeldFlushesOnStop()
        {
            for (var i = 0; i < _heldFlushes.Count; i++)
            {
                _heldFlushes[i].Complete(new ShardPilotResult(false, ShardPilotFlushCodes.Shutdown, false));
            }

            _heldFlushes.Clear();
        }

        /// <summary>The answer to a flush the stopped worker never served.</summary>
        private static ShardPilotResult StoppedResultFor(FlushRequest request)
        {
            return request.Holds
                ? new ShardPilotResult(false, ShardPilotFlushCodes.Shutdown, false)
                : new ShardPilotResult(false, "shardpilot client is closed", false);
        }

        private void SafeLog(string format, params object[] args)
        {
            SafeLogRequest(null, format, args);
        }

        private void SafeLogRequest(string requestBearer, string format, params object[] args)
        {
            SafeLogAt(ShardPilotLogLevel.Warn, requestBearer, format, args);
        }

        private void SafeLogAt(ShardPilotLogLevel level, string requestBearer, string format, params object[] args)
        {
            try
            {
                ShardPilotDiagnostics.Log(_config, level, string.Format(format, args), requestBearer);
            }
            catch
            {
                // A custom logger must never change SDK semantics. In particular,
                // SurfaceNonAcceptedOutcomes logs on the publish SUCCESS path inside
                // the publish try; a throwing logger there would otherwise be caught
                // as a publish failure and flip an accepted 202 to failed/dropped.
                // Logging is best-effort and there is nowhere safe to report a
                // logger that itself throws, so swallow it.
            }
        }

        private static string FirstNonEmpty(params string[] values)
        {
            for (var i = 0; i < values.Length; i++)
            {
                if (!string.IsNullOrEmpty(values[i]))
                {
                    return values[i];
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// The denial epoch a worker last reconciled its local batch against.
        /// A mutable holder (not a ref parameter) because the WebGL coroutine
        /// worker is an iterator and iterators cannot take ref parameters.
        /// </summary>
        private sealed class ConsentEpoch
        {
            public long Value;
        }

        private sealed class FlushRequest
        {
            private readonly TaskCompletionSource<ShardPilotResult> _completion =
                new TaskCompletionSource<ShardPilotResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            private readonly Timer _timer;
            private readonly TimeSpan _timeout;
            private readonly System.Diagnostics.Stopwatch _age = System.Diagnostics.Stopwatch.StartNew();
            private readonly Func<ShardPilotResult, ShardPilotResult> _withRejections;
            private int _completionClaimed;

            /// <summary>
            /// True for a CLOSE request. The worker keys its teardown on the
            /// requests it actually DRAINED — never on the live
            /// _closeRequested flag alone, which a close enqueued mid-flush
            /// could set while its own request is still queued: acting on
            /// the flag then would tear the worker down with that close
            /// request undrained, orphaning it to the exit path's
            /// "client is closed" completion.
            /// </summary>
            public readonly bool Close;

            /// <summary>
            /// True for a public FlushAsync: it answers for the whole drain,
            /// so a retryable failure keeps it open (the worker holds it and
            /// retries) until delivery or its deadline. The close drain's own
            /// internal flushes do not hold: they answer each attempt.
            /// </summary>
            public readonly bool Holds;

            /// <summary>
            /// Set by the worker when it holds this request behind the
            /// consent receipt gate, so the deadline names that cause.
            /// </summary>
            public volatile bool ReceiptGated;

            /// <summary>
            /// The intake sequence current at the call: this flush answers for
            /// the events at or below it. Set under _gate at enqueue.
            /// </summary>
            public long Boundary;

            public FlushRequest(TimeSpan timeout, Func<FlushRequest, ShardPilotResult> deadlineAnswer, bool close, bool holds,
                Func<ShardPilotResult, ShardPilotResult> withRejections, bool armed = true)
            {
                Close = close;
                Holds = holds;
                _withRejections = withRejections;
                if (timeout <= TimeSpan.Zero)
                {
                    timeout = TimeSpan.FromSeconds(10);
                }

                _timeout = timeout;
                _timer = new Timer(_ =>
                {
                    // Runs on a timer thread while the worker may still be
                    // draining. A close's answer persists its remnant BEFORE
                    // the task resolves, so an awaited close is a durable
                    // barrier even when it times out; skipped when the
                    // worker already completed the request.
                    ShardPilotResult answer = null;
                    if (!_completion.Task.IsCompleted && deadlineAnswer != null)
                    {
                        try
                        {
                            answer = deadlineAnswer(this);
                        }
                        catch
                        {
                            // The timeout completion must never be blocked.
                        }
                    }

                    // The deadline passed with work undelivered.
                    Complete(answer ?? new ShardPilotResult(false, Holds && ReceiptGated
                        ? ShardPilotFlushCodes.ConsentReceiptPending
                        : ShardPilotFlushCodes.Pending, true));
                }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                // Publish the timer before a deadline can call Complete.
                // Sub-millisecond timeouts may fire immediately on another thread.
                if (armed) _timer.Change(timeout, Timeout.InfiniteTimeSpan);
            }

            /// <summary>
            /// Arms a request created unarmed with what is left of its
            /// timeout, counted from its creation.
            /// </summary>
            public void Arm()
            {
                var remaining = _timeout - _age.Elapsed;
                if (remaining < TimeSpan.FromMilliseconds(1))
                {
                    remaining = TimeSpan.FromMilliseconds(1);
                }

                try
                {
                    _timer.Change(remaining, Timeout.InfiniteTimeSpan);
                }
                catch (ObjectDisposedException)
                {
                    // Already answered.
                }
            }

            public Task<ShardPilotResult> Task
            {
                get { return _completion.Task; }
            }

            public void Complete(ShardPilotResult result)
            {
                // Finalizing the result retires its rejection observer. Only
                // the completion that owns the answer may freeze that state.
                if (Interlocked.CompareExchange(ref _completionClaimed, 1, 0) != 0) return;
                if (_completion.TrySetResult(_withRejections(result)))
                {
                    _timer.Dispose();
                }
            }
        }
    }
}
