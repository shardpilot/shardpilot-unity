using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

namespace ShardPilot
{
    /// <summary>
    /// One persisted process-era snapshot: the actor-digest
    /// inputs and the consent state that were live when the era STARTED.
    /// A next-launch ApplicationExitInfo report attributes from the era that
    /// covers its exit timestamp — frozen to the era subject — while the
    /// upload gate evaluates CURRENT consent fail-closed.
    /// </summary>
    internal sealed class ShardPilotAnrEraRecord
    {
        public string EraId;
        public long StartedAtMs;

        /// <summary>
        /// END-BOUND of this era's attribution window (exclusive), or 0
        /// while the era is still OPEN. Stamped by the NEXT era's
        /// <see cref="ShardPilotAnrEraStore.PersistEra"/>: when a launch
        /// appends its own era it closes every still-open predecessor at
        /// the next retained era's start, so a CLOSED era covers exactly
        /// [started_at, ended_at) and can never silently extend over a
        /// LATER era whose own line went missing (persist failed, line
        /// damaged) — the gap between windows degrades those records to
        /// device-scoped anonymous instead of the WRONG subject.
        /// FORMAT: an ADDITIVE field on the version-1 era line (the file
        /// header stays <c>{"version":1}</c>) — chosen over a version bump
        /// because it keeps era files readable in BOTH directions: an old
        /// reader ignores the unknown key, and a new reader treats an
        /// end-less v1 line as open and chain-closes it at the next
        /// persist, which reproduces exactly the windows the pre-upgrade
        /// largest-start-at-or-before rule implied for consecutive eras.
        /// </summary>
        public long EndedAtMs;

        /// <summary>True once a successor era's persist end-bounded this one.</summary>
        public bool IsClosed
        {
            get { return EndedAtMs > 0; }
        }

        public string AnonymousId;

        /// <summary>
        /// The app version and build id of the process that LIVED this era,
        /// captured from that launch's config when the era is persisted.
        /// A next-launch AEI report describes the CRASHED process, so its
        /// wire <c>app.version</c>/<c>app.build_id</c> must come from the
        /// attributing era's snapshot — after an app update in between, the
        /// reporting launch's own values would attribute the historical ANR
        /// to the wrong build. FORMAT: additive v1 fields like
        /// <see cref="EndedAtMs"/> (the header stays <c>{"version":1}</c>):
        /// an old reader ignores the unknown keys, and a new reader treats
        /// their absence as unknown — the report then carries NO version
        /// rather than a borrowed one, the same never-borrow direction as
        /// the anonymous-id rule.
        /// </summary>
        public string AppVersion;

        /// <summary>See <see cref="AppVersion"/>.</summary>
        public string BuildId;

        /// <summary>"granted" | "denied" | "unknown" — the analytics consent state at era start.</summary>
        public string ConsentState;

        /// <summary>The crash plane's enabled state at era start (fail-closed: a read failure records false).</summary>
        public bool CrashEnabled;

        /// <summary>
        /// True when a record from this era must be SETTLED WITHOUT
        /// reporting: the era's own posture already forbade collection —
        /// analytics consent was denied (the denial suppresses crash too) or
        /// the crash plane was disabled when the era started. Honest limit,
        /// stated: a MID-era consent change is not re-snapshotted (the
        /// snapshot writer runs at SDK init only); the CURRENT-consent
        /// upload gate still suppresses every report while an opt-out or
        /// denial stands at the next launch.
        /// </summary>
        public bool SuppressesReports
        {
            get { return !CrashEnabled || ConsentState == "denied"; }
        }
    }

    /// <summary>
    /// The per-scope era-snapshot store for the ANR lane — the same
    /// persistence class as the durable crash spool: a JSON
    /// Lines file with the <c>{"version":1}</c> header in the SAME spool
    /// directory, atomic temp+replace writes, per-line damage recovery,
    /// bounded retention (the spool's 7-day TTL discipline plus a count
    /// cap), and a fail-closed owed-wipe marker honored BEFORE any load.
    /// The file holds era ids, start timestamps, END-BOUNDS (the additive
    /// v1 <c>ended_at_ms</c> a successor era's persist stamps — see
    /// <see cref="ShardPilotAnrEraRecord.EndedAtMs"/>), the era subject's
    /// anonymous id, and the era's consent posture — never payloads, never
    /// auth material.
    /// </summary>
    internal sealed class ShardPilotAnrEraStore
    {
        private const string FileNamePrefix = "shardpilot.aei-era.";
        private const string FileNameSuffix = ".jsonl";
        private const string HeaderLine = "{\"version\":1}";
        internal const string WipeOwedMarkerKeyPrefix = "shardpilot.aei_era.wipe_owed.";

        /// <summary>
        /// Directory-scoped "sweep owed" marker for the all-scope denial
        /// sweep, mirroring the crash spool's: set when
        /// <see cref="WipeAllScopesAtResolvedDirectory"/> could not even
        /// complete its enumeration (per-file failures carry their own
        /// per-scope owed-wipe markers), retried fail-closed before any
        /// era store construction in that directory.
        /// </summary>

        /// <summary>
        /// Unix-millisecond bounds a persisted era timestamp must fall in
        /// to be convertible at all (<see cref="FromUnixMs"/> throws
        /// outside DateTimeOffset's range). A syntactically valid line
        /// whose timestamp lies outside is CORRUPT DATA — dropped per line
        /// like any other damage, never allowed to throw out of a load or
        /// prune and disable the whole scope's attribution.
        /// </summary>
        internal const long MinValidUnixMs = 0;
        internal const long MaxValidUnixMs = 253402300799999; // 9999-12-31T23:59:59.999Z

        /// <summary>Newest-first count cap on retained era snapshots.</summary>
        internal const int MaxEras = 20;

        /// <summary>
        /// How long an era snapshot is retained — matching the crash
        /// spool's pending-report retention, since the snapshots exist only
        /// to attribute reports the spool discipline would still deliver.
        /// </summary>
        internal static readonly TimeSpan EraRetention = TimeSpan.FromDays(7);

        /// <summary>
        /// Forward-clock tolerance, mirroring the crash spool: a
        /// future-dated snapshot beyond it is expired (an untrusted
        /// timestamp must not extend the bound).
        /// </summary>
        internal static readonly TimeSpan MaxForwardClockSkew = TimeSpan.FromHours(1);

        /// <summary>
        /// Test-only: when set, era file deletions throw this instead of
        /// deleting (the owed-wipe escalation path). Static so it reaches
        /// the static wipe helpers; tests clear it in a finally block.
        /// Never assigned in production.
        /// </summary>
        internal static Exception DeleteFailureForTesting;

        /// <summary>
        /// Test-only: when set, the era file rewrite throws this instead of
        /// writing (the persist-failure attribution path). Tests clear it
        /// in a finally block. Never assigned in production.
        /// </summary>
        internal static Exception WriteFailureForTesting;

        private readonly object _lock = new object();
        private readonly List<ShardPilotAnrEraRecord> _eras = new List<ShardPilotAnrEraRecord>();
        private readonly string _path;
        private readonly IShardPilotKeyValueStore _markerStore;
        private readonly string _markerKey;
        private readonly Action<string> _log;

        private ShardPilotAnrEraStore(string path, IShardPilotKeyValueStore markerStore, string markerKey, Action<string> log)
        {
            _path = path;
            _markerStore = markerStore;
            _markerKey = markerKey;
            _log = log;
        }

        /// <summary>The absolute era file path for this scope, or null when no directory resolves.</summary>
        public static string ResolveFilePath(ShardPilotConfig config)
        {
            try
            {
                var directory = ShardPilotSpool.ResolveDirectory(config);
                if (string.IsNullOrEmpty(directory))
                {
                    return null;
                }

                return Path.Combine(directory, FileNamePrefix + ShardPilotCrashSpool.ScopeHashFor(config) + FileNameSuffix);
            }
            catch
            {
                return null;
            }
        }

        internal static string WipeOwedMarkerKeyFor(ShardPilotConfig config)
        {
            return WipeOwedMarkerKeyPrefix + ShardPilotCrashSpool.ScopeHashFor(config);
        }

        /// <summary>
        /// Creates the era store for the scope and loads the retained
        /// snapshots. Fail-closed like the crash spool: an owed wipe
        /// recorded by a previous failed suppression is retried BEFORE
        /// anything is loaded, and while it remains owed this returns null —
        /// quarantined era data (a revoked subject's actor inputs) must
        /// never attribute another report. Null also when no spool
        /// directory resolves (reports then go device-scoped anonymous).
        /// Never throws.
        /// </summary>
        public static ShardPilotAnrEraStore TryCreate(
            ShardPilotConfig config,
            IShardPilotKeyValueStore markerStore,
            Action<string> log)
        {
            try
            {
                var path = ResolveFilePath(config);
                if (string.IsNullOrEmpty(path))
                {
                    return null;
                }

                // A denial's all-scope directory sweep that could not even
                // enumerate is retried BEFORE anything loads, exactly like
                // the per-scope owed wipe below: while either remains owed,
                // quarantined era data (a revoked subject's actor inputs)
                // must never attribute another report.
                if (!TrySettleOwedDirectorySweep(ShardPilotSpool.ResolveDirectory(config), markerStore, log))
                {
                    SafeLog(log, "shardpilot anr era store disabled: an owed era directory sweep could not be settled");
                    return null;
                }

                var markerKey = WipeOwedMarkerKeyFor(config);
                if (!TrySettleOwedWipe(markerStore, markerKey))
                {
                    SafeLog(log, "shardpilot anr era store disabled: an owed era wipe could not be settled");
                    return null;
                }

                var store = new ShardPilotAnrEraStore(path, markerStore, markerKey, log);
                store.LoadFromDisk();
                return store;
            }
            catch
            {
                return null;
            }
        }

        internal int EraCountForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _eras.Count;
                }
            }
        }

        /// <summary>
        /// Appends the CURRENT process era's snapshot, END-BOUNDS every
        /// still-open predecessor (each open era except the newest gets
        /// <c>ended_at_ms</c> = the next retained era's start — for the
        /// normal single open predecessor that is THIS era's start; a
        /// legacy end-less v1 file is chain-closed the same way, which
        /// reproduces its pre-upgrade attribution windows), prunes
        /// retention, and rewrites the file atomically.
        ///
        /// A FAILED rewrite is not best-effort-ignored: the on-disk file
        /// then still ends with an OPEN predecessor while this era's line
        /// is missing, and the next launch cannot distinguish "that open
        /// era is my immediate predecessor" from "an era whose own line was
        /// lost ran after it" — closing it there would over-cover the lost
        /// era's records with the WRONG subject. So a failed persist WIPES
        /// the era file through the fail-closed owed-marker machinery
        /// (in-memory state is kept — THIS launch's pass still attributes
        /// correctly): every record the lost history would have claimed
        /// degrades to device-scoped anonymous, the direction that borrows
        /// no login. Residual honest bound: if the rewrite, the delete, AND
        /// the owed-marker write all fail in one launch, a stale open era
        /// can survive on disk. Never throws.
        /// </summary>
        public void PersistEra(ShardPilotAnrEraRecord era)
        {
            if (era == null || string.IsNullOrEmpty(era.EraId))
            {
                return;
            }

            var written = true;
            var nonMonotonic = false;
            try
            {
                lock (_lock)
                {
                    // Clean the retained set FIRST so the monotonicity check
                    // below sees only valid, unexpired eras (a since-expired
                    // future-skewed line must not trigger a false wipe).
                    PruneLocked(DateTime.UtcNow);

                    // NON-MONOTONIC APPEND GUARD (backward clock): the era
                    // file's windows are chain-closed at the NEXT era's start
                    // (CloseOpenPredecessorsLocked), which is only sound when
                    // appends are monotonic in start time — the list is kept
                    // sorted ascending, so the newest is the last element. A
                    // backward wall clock can append an era whose start
                    // PRECEDES a still-retained era's; after the sort it would
                    // sort BEFORE its true predecessor, and the close would
                    // then bound the WRONG era and let the old one cover LATER
                    // timestamps — cross-attributing a later ANR to a stale
                    // era's subject. Attribution must never derive ownership
                    // from sorted wall-clock alone. Safer, testable degrade
                    // (over threading an append-order sequence number through
                    // closing + attribution — chosen because it is a single,
                    // self-evidently correct rule and matches the failed-
                    // persist wipe already below): WIPE the scope's era file
                    // and drop in-memory state, so every record this scope
                    // could claim reports device-scoped ANONYMOUS (borrows no
                    // login) rather than risk the wrong subject.
                    if (_eras.Count > 0 && era.StartedAtMs < _eras[_eras.Count - 1].StartedAtMs)
                    {
                        _eras.Clear();
                        nonMonotonic = true;
                    }
                    else
                    {
                        _eras.Add(era);
                        PruneLocked(DateTime.UtcNow);
                        CloseOpenPredecessorsLocked();
                        written = TryWriteLocked();
                    }
                }
            }
            catch (Exception ex)
            {
                SafeLog(_log, "shardpilot anr era persist failed: " + ex.Message);
            }

            if (nonMonotonic)
            {
                // Outside the in-memory lock, like every wipe — file IO.
                SafeLog(_log, "shardpilot anr era persist saw a non-monotonic (backward-clock) era start; " +
                              "wiping the era file — attribution degrades to anonymous rather than risk the wrong subject");
                WipeFile(_path, _markerStore, _markerKey, _log);
                return;
            }

            if (!written)
            {
                // Outside the in-memory lock, like every wipe — file IO.
                SafeLog(_log, "shardpilot anr era persist could not rewrite the file; wiping it — " +
                              "a half-persisted era chain would misattribute a lost era's records");
                WipeFile(_path, _markerStore, _markerKey, _log);
            }
        }

        /// <summary>
        /// End-bounds every open era except the newest at the NEXT retained
        /// era's start. <c>_eras</c> is sorted ascending by start here
        /// (<see cref="PruneLocked"/> just sorted it).
        /// </summary>
        private void CloseOpenPredecessorsLocked()
        {
            for (var i = 0; i < _eras.Count - 1; i++)
            {
                if (!_eras[i].IsClosed)
                {
                    _eras[i].EndedAtMs = _eras[i + 1].StartedAtMs;
                }
            }
        }

        /// <summary>
        /// The era covering an exit timestamp under the END-BOUNDED window
        /// rule. Null when none covers it — the record then reports
        /// device-scoped anonymous, borrowing no login.
        /// </summary>
        public ShardPilotAnrEraRecord FindEraFor(long timestampMs)
        {
            return FindEraFor(timestampMs, null);
        }

        /// <summary>
        /// End-bounded era attribution, restricted to eras OTHER than
        /// <paramref name="excludeEraId"/> (the next-launch AEI pass passes
        /// the CURRENT launch's era id — see below).
        ///
        /// The rule: a CLOSED era covers exactly [started_at, ended_at);
        /// an OPEN (end-less) era covers ts &gt;= its start ONLY when it is
        /// the NEWEST retained era. A record covered by no window degrades
        /// to device-scoped anonymous. Why this is sound for the pass:
        ///
        /// - An era's end is stamped by its SUCCESSOR's PersistEra, and the
        ///   current launch ALWAYS persists its own era (closing every open
        ///   predecessor) BEFORE the pass runs — so at pass time every
        ///   retained era is closed EXCEPT the current launch's own, which
        ///   is the newest and is excluded via excludeEraId. Effectively the
        ///   pass attributes through CLOSED windows only.
        /// - A record whose true era is MISSING — its PersistEra write
        ///   failed, or its line was damaged and dropped at load — falls in
        ///   the GAP between the previous era's end and the next era's
        ///   start, so it can never ride the previous retained era's
        ///   subject; it reports anonymous. (A failed persist additionally
        ///   wipes the file so a stale OPEN predecessor cannot be
        ///   mis-closed by a non-immediate successor — see PersistEra.)
        /// - Normal consecutive eras are unchanged: era k is closed at
        ///   era k+1's start, exactly the window the old
        ///   largest-start-at-or-before rule implied.
        /// - The open-newest allowance exists for IN-SESSION lookups of the
        ///   just-persisted current era (nothing can have ended it yet);
        ///   the pass never reaches it because of the exclusion, so a
        ///   rolled-back wall clock still cannot attribute an old exit to
        ///   the new launch's identity.
        /// </summary>
        public ShardPilotAnrEraRecord FindEraFor(long timestampMs, string excludeEraId)
        {
            lock (_lock)
            {
                // _eras is kept sorted ascending by start (PruneLocked runs
                // on every mutation), so the newest era is the last element.
                var newestIndex = _eras.Count - 1;
                ShardPilotAnrEraRecord best = null;
                for (var i = 0; i < _eras.Count; i++)
                {
                    var era = _eras[i];
                    if (!string.IsNullOrEmpty(excludeEraId) &&
                        string.Equals(era.EraId, excludeEraId, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (era.StartedAtMs > timestampMs)
                    {
                        continue;
                    }

                    if (era.IsClosed ? timestampMs >= era.EndedAtMs : i != newestIndex)
                    {
                        // Outside the closed window, or an open era that is
                        // not the newest (possible only transiently before a
                        // persist chain-closes a legacy end-less file): no
                        // coverage.
                        continue;
                    }

                    if (best == null || era.StartedAtMs > best.StartedAtMs)
                    {
                        best = era;
                    }
                }

                return best;
            }
        }

        /// <summary>
        /// Destroys every era snapshot of this scope — the suppression
        /// path's owed-wipe: the era subject's actor inputs must not
        /// survive an opt-out/denial observed at the next launch. Delegates
        /// to the static wipe so the instance and no-instance paths share
        /// one fail-closed rule. Never throws.
        /// </summary>
        public void WipeAll()
        {
            lock (_lock)
            {
                _eras.Clear();
            }

            WipeFile(_path, _markerStore, _markerKey, _log);
        }

        /// <summary>
        /// Static suppression/revocation wipe for callers WITHOUT a live
        /// instance: the next-launch AEI pass when it observes a standing
        /// opt-out/denial before ever constructing the store, and the
        /// client's RUNTIME crash opt-out and consent-denial paths — which
        /// call it at the same moment they clear the breadcrumb ring, so a
        /// revoked subject's era snapshots (anonymous id + consent posture)
        /// are destroyed IMMEDIATELY instead of surviving until a later
        /// AEI pass (which never runs while the ANR flags are dark) or the
        /// retention TTL. Deletes the scope's era file and temp sibling;
        /// when either cannot be confirmed gone, persists the owed-wipe
        /// marker that keeps the store disabled — fail closed — until a
        /// later launch removes it. Idempotent and cheap when no era file
        /// exists; runs file IO, so call it outside in-memory locks like
        /// the spool wipes. Never throws. (A consent READ-FAILURE hold
        /// deliberately does not call this: nothing is destroyed while the
        /// stored decision is unknowable, and the identity-grant downgrade
        /// mirrors the crash spool — which it also leaves in place.)
        /// A consent DENIAL and the crash-plane OPT-OUT both additionally
        /// sweep every OTHER scope's era file in the directory via
        /// <see cref="WipeAllScopesAtResolvedDirectory"/>: both persisted
        /// decisions are GLOBAL single keys while era files are per-scope
        /// (the opt-out's spool posture stays scope-local — it destroys no
        /// spools anywhere — but its era revocation is as global as the
        /// setting that carries it).
        /// </summary>
        public static void WipeScope(ShardPilotConfig config, IShardPilotKeyValueStore markerStore, Action<string> log)
        {
            var path = ResolveFilePath(config);
            if (string.IsNullOrEmpty(path))
            {
                return;
            }

            WipeFile(path, markerStore, WipeOwedMarkerKeyFor(config), log);
        }

        /// <summary>
        /// One-scope fail-closed wipe: returns true when the era file (and
        /// its temp sibling) are confirmed gone and the scope's owed marker
        /// is cleared; false when the wipe stays OWED (marker persisted —
        /// the scope's store is disabled until a later launch settles it).
        /// </summary>
        private static bool WipeFile(string path, IShardPilotKeyValueStore markerStore, string markerKey, Action<string> log)
        {
            try
            {
                if (TryDeleteEraFiles(path))
                {
                    ClearMarker(markerStore, markerKey);
                    return true;
                }

                SetMarker(markerStore, markerKey, path);
                SafeLog(log, "shardpilot anr era wipe could not remove the file; " +
                             "wipe owed — the era store stays disabled until a later launch removes it");
            }
            catch
            {
                // Best-effort escalation only; the marker (set above where
                // reachable) makes the next construction retry fail-closed.
            }

            return false;
        }

        /// <summary>
        /// Fail-closed revocation wipe of EVERY scope's era snapshots in
        /// the directory the config resolves to — the same all-scope sweep
        /// the event and crash spools run when a denial lands, because the
        /// revoking decision is GLOBAL while era files are per-scope: an
        /// INACTIVE scope's snapshot would otherwise survive the revocation
        /// and attribute historical ANRs with the pre-revocation anonymous
        /// id after a later re-grant initializes that scope. Called by the
        /// consent-DENIAL paths and — since the crash-reporting-enabled
        /// setting is itself one GLOBAL persisted key
        /// (<see cref="ShardPilotCrashSettings.StorageKey"/>, no scope
        /// suffix) — by the crash-plane OPT-OUT paths too; only the
        /// opt-out's SPOOL posture stays scope-local (it destroys no spools
        /// anywhere). Each swept scope ALSO gets its AEI suppression floor
        /// stamped, keyed by the scope hash from the swept filename: the
        /// wipe alone destroys attribution but not the records Android
        /// retains, and an inactive scope re-initialized after a later
        /// re-grant/re-enable would otherwise report revoked-interval ANRs
        /// (device-scoped anonymous, but still disclaimed data). Only files
        /// matching the canonical era filename shape are touched (orphaned
        /// temp siblings included); each scope that cannot be confirmed
        /// deleted keeps its own per-scope owed-wipe marker, and a sweep
        /// that cannot even enumerate records the directory-scoped
        /// sweep-owed marker — both honored fail-closed before any later
        /// era store construction. Returns nothing: unlike the spools there
        /// is no long-lived era store instance riding in the client to
        /// quarantine — fail-closed enforcement lives entirely in the
        /// markers TryCreate honors. Never throws.
        /// </summary>
        public static void WipeAllScopesAtResolvedDirectory(
            ShardPilotConfig config,
            IShardPilotKeyValueStore markerStore,
            Action<string> log)
        {
            try
            {
                var directory = ShardPilotSpool.ResolveDirectory(config);
                if (string.IsNullOrEmpty(directory))
                {
                    // Nothing resolvable means no era file could ever have
                    // existed here — nothing is owed.
                    return;
                }

                SweepDirectory(directory, markerStore, log);
            }
            catch
            {
                // A consent wipe must never throw into game code.
            }
        }

        /// <summary>
        /// The re-enable/re-grant twin of the all-scope opt-out sweep: raises
        /// the AEI suppression-floor marker to <paramref name="nowUnixMs"/>
        /// for EVERY scope this directory can enumerate through its era files,
        /// not just the current one. A global re-enable/re-grant closes the
        /// dark window everywhere, but only the current scope's marker is
        /// raised directly by the client — an INACTIVE scope the opt-out sweep
        /// stamped would otherwise keep a stale (revocation-time) floor. Same
        /// era-file enumeration the sweep uses; each discovered scope's marker
        /// is raised forward-only (never created, never lowered) and its ARMED
        /// flag is left standing so its own next enabled pass still runs the
        /// clock-proof settle-all. Enumerability, stated honestly: the opt-out
        /// sweep normally WIPED these era files, so at re-enable this usually
        /// finds only owed-wipe survivors and the current scope's freshly
        /// re-persisted era — the per-scope ARMED flag (a KV marker that
        /// survives the era wipe) is the real backstop for a scope that inits
        /// later; the KV-only-scope residual (a scope with a cursor but no era
        /// file and no live client here) stays the documented honest limit,
        /// unchanged from the opt-out side. Never throws.
        /// </summary>
        public static void RaiseSuppressionFloorForAllScopesAtResolvedDirectory(
            ShardPilotConfig config,
            IShardPilotKeyValueStore markerStore,
            long nowUnixMs,
            Action<string> log)
        {
            try
            {
                if (markerStore == null)
                {
                    return;
                }

                var directory = ShardPilotSpool.ResolveDirectory(config);
                if (string.IsNullOrEmpty(directory))
                {
                    return;
                }

                bool exists;
                try
                {
                    exists = Directory.Exists(directory);
                }
                catch
                {
                    return;
                }

                if (!exists)
                {
                    return;
                }

                var scopeLivePaths = new Dictionary<string, string>(StringComparer.Ordinal);
                CollectScopeFiles(directory, FileNamePrefix + "*" + FileNameSuffix, false, scopeLivePaths);
                CollectScopeFiles(directory, FileNamePrefix + "*" + FileNameSuffix + ".tmp", true, scopeLivePaths);
                foreach (var pair in scopeLivePaths)
                {
                    ShardPilotAeiCursor.RaiseSuppressionFloorIfMarkedForScopeHash(
                        markerStore, pair.Key, nowUnixMs);
                }
            }
            catch
            {
                // A re-enable must never throw into game code; the armed flag
                // still carries the correctness for any scope missed here.
            }
        }

        private static bool SweepDirectory(string directory, IShardPilotKeyValueStore markerStore, Action<string> log) =>
            ShardPilotDirectorySweep.Sweep(markerStore, ShardPilotDirectorySweep.EraKey, directory,
                owed => SweepDirectoryFiles(owed, markerStore, log));

        private static bool SweepDirectoryFiles(string directory, IShardPilotKeyValueStore markerStore, Action<string> log)
        {
            var succeeded = true;
            try
            {
                var exists = false;
                try
                {
                    exists = Directory.Exists(directory);
                }
                catch
                {
                    succeeded = false;
                }

                if (succeeded && exists)
                {
                    // One wipe per DISCOVERED scope (live file and orphaned
                    // temp deduplicate onto the scope's live path — the
                    // per-scope wipe removes both siblings), so each failure
                    // lands on that scope's own owed marker.
                    var scopeLivePaths = new Dictionary<string, string>(StringComparer.Ordinal);
                    if (!CollectScopeFiles(directory, FileNamePrefix + "*" + FileNameSuffix, false, scopeLivePaths))
                    {
                        succeeded = false;
                    }

                    if (!CollectScopeFiles(directory, FileNamePrefix + "*" + FileNameSuffix + ".tmp", true, scopeLivePaths))
                    {
                        succeeded = false;
                    }

                    var nowUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    foreach (var pair in scopeLivePaths)
                    {
                        // The swept scope's AEI suppression floor, stamped
                        // BEFORE its wipe (both keyed by the same 16-hex
                        // scope hash — the filename carries it): the wipe
                        // destroys the era subject's attribution inputs,
                        // but Android still retains the exit records, and
                        // an INACTIVE scope has no live client to stamp its
                        // floor through propagation — without this, a later
                        // re-grant/re-enable plus that scope's next init
                        // reports revoked-interval ANRs over its zero or
                        // stale cursor. Floor-then-wipe so a kill between
                        // the two cannot leave a floorless scope whose era
                        // file is already gone (the sweep retry could no
                        // longer discover it). Forward-only max; on an
                        // owed-sweep RETRY the re-stamp lands at the retry
                        // moment — later than the revocation, the
                        // fail-closed direction for a disclaimed window.
                        // Best-effort like every floor stamp: a lost marker
                        // write degrades to the live consent gate's
                        // suppression, never to a thrown sweep.
                        ShardPilotAeiCursor.RecordSuppressionFloorForScopeHash(markerStore, pair.Key, nowUnixMs);
                        if (!WipeFile(pair.Value, markerStore, WipeOwedMarkerKeyPrefix + pair.Key, log))
                        {
                            succeeded = false;
                        }
                    }
                }
            }
            catch
            {
                succeeded = false;
            }

            return succeeded;
        }

        /// <summary>
        /// Adds every canonical-shaped era file under <paramref name="pattern"/>
        /// to the scope→live-path map. Returns false when the enumeration
        /// itself failed (the caller then owes the directory sweep).
        /// </summary>
        private static bool CollectScopeFiles(
            string directory, string pattern, bool trimTempSuffix, Dictionary<string, string> scopeLivePaths)
        {
            string[] files;
            try
            {
                files = Directory.GetFiles(directory, pattern);
            }
            catch
            {
                return false;
            }

            for (var i = 0; i < files.Length; i++)
            {
                var name = Path.GetFileName(files[i]);
                if (trimTempSuffix)
                {
                    if (name == null || !name.EndsWith(".tmp", StringComparison.Ordinal))
                    {
                        continue;
                    }

                    name = name.Substring(0, name.Length - 4);
                }

                var scopeHash = ExtractScopeHash(name);
                if (scopeHash == null)
                {
                    // Not the canonical era filename shape: never touch a
                    // foreign file.
                    continue;
                }

                var livePath = trimTempSuffix
                    ? files[i].Substring(0, files[i].Length - 4)
                    : files[i];
                if (!scopeLivePaths.ContainsKey(scopeHash))
                {
                    scopeLivePaths[scopeHash] = livePath;
                }
            }

            return true;
        }

        /// <summary>
        /// Extracts the scope hash from an era file name, or null when the
        /// name is not EXACTLY the canonical
        /// <c>shardpilot.aei-era.&lt;16 lowercase hex&gt;.jsonl</c> shape —
        /// the strictness keeps directory sweeps away from foreign files.
        /// </summary>
        private static string ExtractScopeHash(string fileName)
        {
            if (fileName == null ||
                !fileName.StartsWith(FileNamePrefix, StringComparison.Ordinal) ||
                !fileName.EndsWith(FileNameSuffix, StringComparison.Ordinal))
            {
                return null;
            }

            var length = fileName.Length - FileNamePrefix.Length - FileNameSuffix.Length;
            if (length != 16)
            {
                return null;
            }

            var hash = fileName.Substring(FileNamePrefix.Length, length);
            for (var i = 0; i < hash.Length; i++)
            {
                var ch = hash[i];
                if (!(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f'))
                {
                    return null;
                }
            }

            return hash;
        }

        private static bool TrySettleOwedDirectorySweep(string directory, IShardPilotKeyValueStore store, Action<string> log) =>
            ShardPilotDirectorySweep.Retry(store, ShardPilotDirectorySweep.EraKey, directory,
                owed => SweepDirectoryFiles(owed, store, log));

        // ------------------------------------------------------------------
        // Load + retention
        // ------------------------------------------------------------------

        private void LoadFromDisk()
        {
            lock (_lock)
            {
                string text;
                try
                {
                    if (!File.Exists(_path))
                    {
                        return;
                    }

                    text = File.ReadAllText(_path);
                }
                catch
                {
                    // Unreadable: attribution degrades to device-scoped
                    // anonymous; deletion is attempted so a permanently
                    // unreadable file does not linger.
                    TryDeleteEraFiles(_path);
                    return;
                }

                var lines = text.Split('\n');
                if (lines.Length == 0 ||
                    !string.Equals(TrimLine(lines[0]), HeaderLine, StringComparison.Ordinal))
                {
                    TryDeleteEraFiles(_path);
                    return;
                }

                var parsedCount = 0;
                for (var i = 1; i < lines.Length; i++)
                {
                    var line = TrimLine(lines[i]);
                    if (line.Length == 0)
                    {
                        continue;
                    }

                    parsedCount++;

                    // Per-line recovery, like the crash spool: one damaged
                    // line never destroys the other eras' attribution.
                    var era = ParseEraLine(line);
                    if (era != null)
                    {
                        _eras.Add(era);
                    }
                }

                var before = _eras.Count;
                PruneLocked(DateTime.UtcNow);
                if (_eras.Count != parsedCount || _eras.Count != before)
                {
                    // Reconcile once when damage drops or retention pruned
                    // anything; a failed reconcile only leaves extra lines
                    // for the next load to trim.
                    TryWriteLocked();
                }
            }
        }

        private void PruneLocked(DateTime nowUtc)
        {
            var oldest = nowUtc - EraRetention;
            var newestAllowed = nowUtc + MaxForwardClockSkew;
            for (var i = _eras.Count - 1; i >= 0; i--)
            {
                // Belt over the parse-time range gate: an out-of-range
                // timestamp must never make FromUnixMs throw out of a load
                // or persist — it is corrupt data, pruned like any expired
                // line, never scope-fatal.
                if (!IsValidUnixMs(_eras[i].StartedAtMs))
                {
                    _eras.RemoveAt(i);
                    continue;
                }

                var started = FromUnixMs(_eras[i].StartedAtMs);
                if (started < oldest || started > newestAllowed)
                {
                    _eras.RemoveAt(i);
                }
            }

            // Count cap, newest kept: sort ascending by start, drop from the
            // front. (The list is near-sorted already — one append per
            // launch.)
            _eras.Sort(CompareByStart);
            while (_eras.Count > MaxEras)
            {
                _eras.RemoveAt(0);
            }
        }

        private static int CompareByStart(ShardPilotAnrEraRecord left, ShardPilotAnrEraRecord right)
        {
            return left.StartedAtMs.CompareTo(right.StartedAtMs);
        }

        internal static DateTime FromUnixMs(long unixMs)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(unixMs).UtcDateTime;
        }

        /// <summary>True when <see cref="FromUnixMs"/> can convert the value.</summary>
        internal static bool IsValidUnixMs(long unixMs)
        {
            return unixMs >= MinValidUnixMs && unixMs <= MaxValidUnixMs;
        }

        // ------------------------------------------------------------------
        // Serialization
        // ------------------------------------------------------------------

        private static ShardPilotAnrEraRecord ParseEraLine(string line)
        {
            Dictionary<string, object> parsed;
            if (!ShardPilotJsonParser.TryParseObject(line, out parsed))
            {
                return null;
            }

            var eraId = GetString(parsed, "era_id");
            if (string.IsNullOrEmpty(eraId))
            {
                return null;
            }

            object startedRaw;
            if (!parsed.TryGetValue("started_at_ms", out startedRaw) || !(startedRaw is long) ||
                !IsValidUnixMs((long)startedRaw))
            {
                // Syntactically valid but unconvertible (for example
                // long.MaxValue after file damage) is CORRUPT: dropped per
                // line so the rest of the scope keeps attributing.
                return null;
            }

            var started = (long)startedRaw;

            // ended_at_ms is the ADDITIVE v1 end-bound (absent on lines
            // written before end-bounding shipped = era still open — see
            // ShardPilotAnrEraRecord.EndedAtMs for the format decision). A
            // present-but-damaged value (wrong type, out of range, or an
            // end before its own start) condemns the LINE, not the scope.
            var ended = 0L;
            object endedRaw;
            if (parsed.TryGetValue("ended_at_ms", out endedRaw))
            {
                if (!(endedRaw is long) || !IsValidUnixMs((long)endedRaw) || (long)endedRaw < started)
                {
                    return null;
                }

                ended = (long)endedRaw;
            }

            return new ShardPilotAnrEraRecord
            {
                EraId = eraId,
                StartedAtMs = started,
                EndedAtMs = ended,
                AnonymousId = GetString(parsed, "anonymous_id"),
                // Additive v1 build snapshot (absent on pre-upgrade lines =
                // unknown); a wrong-typed value degrades to empty like
                // anonymous_id — the report then carries NO version, never
                // a borrowed one.
                AppVersion = GetString(parsed, "app_version"),
                BuildId = GetString(parsed, "build_id"),
                ConsentState = NormalizeConsent(GetString(parsed, "consent")),
                CrashEnabled = GetBool(parsed, "crash_enabled")
            };
        }

        private static string NormalizeConsent(string value)
        {
            // Closed set; anything unrecognized reads as "denied" — the
            // fail-closed direction for a value that decides suppression.
            switch (value)
            {
                case "granted":
                case "unknown":
                case "denied":
                    return value;
                default:
                    return "denied";
            }
        }

        private static string GetString(Dictionary<string, object> parsed, string key)
        {
            object value;
            return parsed.TryGetValue(key, out value) ? value as string ?? string.Empty : string.Empty;
        }

        private static bool GetBool(Dictionary<string, object> parsed, string key)
        {
            object value;
            return parsed.TryGetValue(key, out value) && value is bool && (bool)value;
        }

        private static string SerializeEraLine(ShardPilotAnrEraRecord era)
        {
            var builder = new StringBuilder();
            builder.Append("{\"era_id\":");
            ShardPilotJson.AppendQuoted(builder, era.EraId);
            builder.Append(",\"started_at_ms\":");
            builder.Append(era.StartedAtMs.ToString(CultureInfo.InvariantCulture));
            if (era.IsClosed)
            {
                builder.Append(",\"ended_at_ms\":");
                builder.Append(era.EndedAtMs.ToString(CultureInfo.InvariantCulture));
            }

            builder.Append(",\"anonymous_id\":");
            ShardPilotJson.AppendQuoted(builder, era.AnonymousId ?? string.Empty);
            if (!string.IsNullOrEmpty(era.AppVersion))
            {
                builder.Append(",\"app_version\":");
                ShardPilotJson.AppendQuoted(builder, era.AppVersion);
            }

            if (!string.IsNullOrEmpty(era.BuildId))
            {
                builder.Append(",\"build_id\":");
                ShardPilotJson.AppendQuoted(builder, era.BuildId);
            }

            builder.Append(",\"consent\":");
            ShardPilotJson.AppendQuoted(builder, NormalizeConsent(era.ConsentState));
            builder.Append(",\"crash_enabled\":");
            builder.Append(era.CrashEnabled ? "true" : "false");
            builder.Append('}');
            return builder.ToString();
        }

        /// <summary>
        /// Atomic temp+replace rewrite. Returns false on failure. Only
        /// <see cref="PersistEra"/> escalates a false to the file wipe —
        /// a failed LOAD-time reconcile merely leaves already-valid lines
        /// (with their existing end-bounds) for the next load to re-trim,
        /// which cannot misattribute; a failed PERSIST leaves an open
        /// predecessor without its successor's line, which can.
        /// </summary>
        private bool TryWriteLocked()
        {
            var builder = new StringBuilder();
            builder.Append(HeaderLine);
            for (var i = 0; i < _eras.Count; i++)
            {
                builder.Append('\n');
                builder.Append(SerializeEraLine(_eras[i]));
            }

            builder.Append('\n');

            try
            {
                var failure = WriteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                var directory = Path.GetDirectoryName(_path);
                if (!string.IsNullOrEmpty(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                var tempPath = _path + ".tmp";
                File.WriteAllText(tempPath, builder.ToString());
                MoveOver(tempPath, _path);
                return true;
            }
            catch (Exception ex)
            {
                SafeLog(_log, "shardpilot anr era write failed: " + ex.Message);
                return false;
            }
        }

        private static void MoveOver(string tempPath, string path)
        {
            try
            {
                if (File.Exists(path))
                {
                    File.Replace(tempPath, path, null);
                }
                else
                {
                    File.Move(tempPath, path);
                }
            }
            catch (Exception ex) when (ex is IOException ||
                                       ex is UnauthorizedAccessException ||
                                       ex is PlatformNotSupportedException)
            {
                File.Delete(path);
                File.Move(tempPath, path);
            }
        }

        // ------------------------------------------------------------------
        // Fail-closed wipe machinery (mirrors the crash spool)
        // ------------------------------------------------------------------

        private static bool TrySettleOwedWipe(IShardPilotKeyValueStore store, string markerKey)
        {
            if (store == null)
            {
                return true;
            }

            string owedPath;
            try
            {
                owedPath = ShardPilotConfig.Trim(store.Get(markerKey));
            }
            catch
            {
                // An unreadable store is a separate failure domain; fail
                // open on the STORE — the wipe itself already ran
                // best-effort when the marker was set. Mirrors the spool.
                return true;
            }

            if (string.IsNullOrEmpty(owedPath))
            {
                return true;
            }

            if (!TryDeleteEraFiles(owedPath))
            {
                return false;
            }

            ClearMarker(store, markerKey);
            return true;
        }

        private static bool TryDeleteEraFiles(string path)
        {
            var liveConfirmed = false;
            try
            {
                var failure = DeleteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                File.Delete(path);
                liveConfirmed = true;
            }
            catch
            {
                // Fall through to the guarded existence probe below.
            }

            var tempPath = path + ".tmp";
            var tempConfirmed = false;
            try
            {
                var failure = DeleteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                File.Delete(tempPath);
                tempConfirmed = true;
            }
            catch
            {
                // Fall through to the guarded existence probe below.
            }

            return (liveConfirmed || ConfirmedAbsent(path)) &&
                   (tempConfirmed || ConfirmedAbsent(tempPath));
        }

        private static bool ConfirmedAbsent(string path)
        {
            try
            {
                return !File.Exists(path);
            }
            catch
            {
                return false;
            }
        }

        private static void SetMarker(IShardPilotKeyValueStore store, string markerKey, string path)
        {
            if (store == null)
            {
                return;
            }

            try
            {
                store.Set(markerKey, path);
            }
            catch
            {
                // Best-effort: the next suppression or launch retries.
            }
        }

        private static void ClearMarker(IShardPilotKeyValueStore store, string markerKey)
        {
            if (store == null)
            {
                return;
            }

            try
            {
                if (!string.IsNullOrEmpty(ShardPilotConfig.Trim(store.Get(markerKey))))
                {
                    store.Set(markerKey, string.Empty);
                }
            }
            catch
            {
                // Best-effort; an uncleared marker only costs one redundant
                // (idempotent) wipe on the next launch.
            }
        }

        private static string TrimLine(string line)
        {
            return line == null ? string.Empty : line.Trim('\r', ' ', '\t');
        }

        private static void SafeLog(Action<string> log, string message)
        {
            if (log == null)
            {
                return;
            }

            try
            {
                log(message);
            }
            catch
            {
                // Logging must never change store semantics.
            }
        }
    }
}
