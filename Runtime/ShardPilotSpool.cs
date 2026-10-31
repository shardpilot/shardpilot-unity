using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Text;
using UnityEngine;

namespace ShardPilot
{
    /// <summary>
    /// Durable on-disk spool for undeliverable analytics events.
    ///
    /// The spool holds each unchanged wire envelope plus separate SDK intake and
    /// first ambiguous-attempt clocks. Auth material (keys, tokens, headers) is
    /// never written. Valid envelopes replay verbatim; load-time compatibility
    /// handling repairs old platforms and drops obsolete session names. The stable UUIDv7
    /// <c>event_id</c> (stamped once at enqueue, never regenerated) lets the
    /// ingest service de-duplicate events that were delivered but not observed
    /// as acknowledged.
    ///
    /// Version 3 is a JSON header followed by newline-terminated records:
    /// two tab-separated int64 millisecond clocks, then the envelope. Formatted
    /// envelopes use a JSON string record so signed newlines survive verbatim.
    /// The pre-existing version 1 reader retains envelopes with missing clocks,
    /// which expire conservatively at the client replay gate. A torn final
    /// record is dropped while complete records survive. Other malformed
    /// envelope data discards the file. The file is scoped by a hash of
    /// (workspace, app, environment) in the app-private spool directory;
    /// no method throws into game code.
    ///
    /// Capacity is bounded by BOTH an event count and a serialized-bytes cap
    /// (the summed UTF-8 size of the stored envelopes); when either is
    /// exceeded, the OLDEST entries are evicted first. Caps are enforced on
    /// every append and again on load.
    /// </summary>
    internal sealed class ShardPilotSpool
    {
        private const string FileNamePrefix = "shardpilot.spool.";
        private const string FileNameSuffix = ".json";
        // Version 1: one JSON document rewritten in full on every change.
        // Still READ (a spool written by an older build loads and is rewritten
        // as v3 on first load); never written.
        private const string DocumentPrefix = "{\"version\":1,\"events\":[";
        private const string DocumentSuffix = "]}";

        // Version 2 (read only): a header line, then one envelope per line.
        // Version 3 adds two local clock fields before each unchanged envelope.
        //
        // The record boundary is what the format is for. A v1 append can only
        // be a full rewrite — O(spool size) per event, on the calling thread at
        // overflow — and splicing into its array instead is unsafe: a torn
        // splice leaves a partial trailing record, the document then fails to
        // parse as a whole, and the load path discards the ENTIRE backlog. The
        // obvious repair (trim to the last record boundary) is impossible
        // there, because `}` occurs inside every envelope's props and `},{`
        // can occur inside a JSON string a game is free to write, so a scanner
        // cannot tell a delimiter from data.
        //
        // A newline can. Generated envelopes escape string newlines; formatted
        // signed envelopes are quoted as one storage record. Therefore a
        // raw newline in the file is only ever a terminator: a torn append
        // leaves a final line with none, the load path drops exactly that
        // line, and every complete record survives. Crash safety by
        // construction rather than by heuristic.
        //
        // Append-only records preserve every complete envelope after a torn write.
        private const string DocumentHeaderV3Prefix = "{\"version\":3";

        /// <summary>
        /// The v3 header line. It records the CAPS the file was written under,
        /// and that is not decoration.
        ///
        /// Eviction is logical: a dropped entry's bytes stay in the file until
        /// a compaction reclaims them, and nothing in the record distinguishes
        /// a dead one from a live one. The load path reconstructs the live set
        /// by trimming oldest-first to the caps — which reproduces the writer's
        /// cut exactly, as long as the caps are the same. Raise SpoolMaxEvents
        /// in a new app version and they are not: records the previous run had
        /// already dropped now fit, so they load, re-send, and arrive as events
        /// the SDK had already reported dropped.
        ///
        /// So the file states its own caps, the load applies THOSE first, and
        /// only then the current ones. A config change can shrink what a file
        /// carries forward; it can never resurrect what was already gone.
        /// </summary>
        private static string DocumentHeaderFor(int maxEvents, long maxBytes)
        {
            return DocumentHeaderV3Prefix +
                ",\"max_events\":" + maxEvents.ToString(CultureInfo.InvariantCulture) +
                ",\"max_bytes\":" + maxBytes.ToString(CultureInfo.InvariantCulture) + "}";
        }
        private const char RecordSeparator = '\n';

        // Compaction. Evicting an entry means its bytes have to leave the file,
        // and the only way to remove bytes from the middle of a file is to
        // rewrite it — the exact O(spool size) cost on the calling thread this
        // format exists to remove. So eviction is LOGICAL: the entry leaves the
        // committed state immediately, its bytes stay on disk as a dead record,
        // and one rewrite reclaims them once the file is more than
        // CompactionSlackFactor times the live content it holds.
        //
        // That makes compaction O(live) once per roughly (live record count)
        // appends — O(1) amortised — and, more to the point, rare enough to sit
        // OUTSIDE p99 instead of exactly on it.
        //
        // Chunked eviction (compact to 90% of the caps, then ~200 pure appends)
        // was the first attempt and it does not achieve that, for a reason
        // worth recording: the bound's "at a full spool" window is the last
        // ~100 appends before the cap, and that window contains exactly ONE
        // compaction however large the chunk is. Its density there is pinned
        // near 1%, which is where p99 reads — so p99 measured the rewrite no
        // matter how the chunk was sized. Measured 1.7-3.3 ms against a 1.0 ms
        // bound in the recorded full-spool overflow measurement.
        //
        // Load defines the truth and needs no cooperation from this: it reads
        // every record and trims oldest-first to the caps, and dead records are
        // by construction the oldest — so a reload of a file with dead records
        // yields exactly the committed state that was live when it was written.
        //
        // The slack is a DISK cost and is sized as one: half again the live
        // content. At the 2000-event / 1 MiB defaults that is ~460 KB of dead
        // records, so a rewrite falls due about once per 1000 appends — 0.1%,
        // a factor of ten below the 1% where p99 reads — and it caps the spool
        // FILE at roughly 1.4 MiB plus clock framing for unquoted records.
        // SpoolMaxBytes bounds the wire-envelope backlog. The file threshold
        // uses 1.5x encoded live records plus header and floor; quoting can
        // double wire bytes, making that bound about 3x wire bytes plus framing.
        // Quoting is live data rather than a reason to rewrite. See SpoolMaxBytes.
        //
        // Both halves of that trade are real. A larger slack buys a rarer
        // rewrite and costs disk; a smaller one costs latency at the tail. It
        // is not tunable because a caller has no way to tell which they are
        // choosing.
        private const long CompactionFloorBytes = 4 * 1024;

        /// <summary>
        /// Guards THE FILE. Every path that does spool file I/O takes it, and
        /// takes it BEFORE <c>_lock</c> — never the reverse.
        ///
        /// It is taken OUTSIDE each method's existing <c>lock (_lock)</c>
        /// rather than around the I/O itself, and that is the whole
        /// discipline: every file-touching method here opens <c>_lock</c> a
        /// few lines into its body and does its I/O inside that hold, so a
        /// gate added at the I/O would read <c>_lock</c> → <c>_fileGate</c>
        /// and cycle against the writer's <c>_fileGate</c> → <c>_lock</c>.
        /// This order applies to the background writer, consent wipe and
        /// every other path that touches the spool file.
        /// </summary>
        private readonly object _fileGate = new object();

        private readonly object _lock = new object();

#if !(UNITY_WEBGL && !UNITY_EDITOR)
        // ── The writer ──────────────────────────────────────────────────────
        //
        // Memory admission is synchronous,
        // disk is asynchronous. Append commits the in-memory state under _lock
        // and posts the new records here; this thread writes them.
        //
        // The spool is never constructed on WebGL (ShardPilotClient returns
        // null there), so the platform without threads does not grow a worker
        // it cannot run.

        /// <summary>
        /// Records committed to memory and not yet on disk. Guarded by
        /// <c>_lock</c>; drained by the writer under <c>_fileGate</c>.
        /// </summary>
        private readonly List<SpoolEntry> _pendingWrites = new List<SpoolEntry>();
        private long _pendingWriteBytes;

        /// <summary>
        /// A full rewrite is owed: an append could not be expressed as one, or
        /// a write failed. The committed state is the truth and a rewrite from
        /// it covers whatever the pending records carried — which is why a
        /// failed write is NOT re-queued. Nothing is held in two places, which
        /// is where a queue-based design usually loses a record. Guarded by
        /// <c>_lock</c>.
        /// </summary>
        private bool _rewriteOwed;

        /// <summary>
        /// Bumped whenever an append creates work for the writer. A cycle
        /// snapshots it before its I/O and compares afterwards, so it can tell
        /// whether the state it wrote is still the whole state — the only way
        /// it can, now that an append no longer waits for the file gate and
        /// can therefore commit while a cycle is mid-write. Guarded by
        /// <c>_lock</c>.
        /// </summary>
        private long _writeEpoch;

        private Thread _writerThread;
        private readonly AutoResetEvent _writeSignal = new AutoResetEvent(false);
        private volatile bool _writerStopping;
#endif
        private readonly List<SpoolEntry> _entries = new List<SpoolEntry>();
        private readonly HashSet<string> _ids = new HashSet<string>(StringComparer.Ordinal);
        private readonly string _directory;
        private readonly string _path;
        private readonly int _maxEvents;
        private readonly long _maxBytes;
        // Whether the file on disk is a v3 document this class may extend by
        // appending. False until a full write establishes one — so an absent,
        // empty, legacy, or failed-write file takes the rewrite path, which is
        // also how v1 spools are rewritten on first load.
        //
        // EVERY failed write clears it, which is also the only way a rewrite
        // debt (_staleFileRewriteOwed) is incurred — so a debt always implies
        // this is false, and the append fast path does not need to re-check
        // the debt to know the file matches the committed state.
        private bool _fileIsAppendable;

        // The v3 header this spool writes, built once from the configured
        // caps. See DocumentHeaderFor for why the caps are in the file.
        private readonly string _header;

        // Bytes this class has written to the file, live and dead together.
        // The file may hold DEAD records — entries evicted from the committed
        // state whose bytes are still on disk — so this is >= the live content
        // and is what the compaction trigger reads. Meaningful only while
        // _fileIsAppendable; every full write resets it to what it wrote.
        private long _fileBytes;
        private readonly Action<string> _log;
        private long _totalBytes;
        private long _totalRecordBytes; // encoded live records, including terminators

        /// <summary>
        /// Set once when this instance loses its claim on the file: either
        /// ownership transferred to a newer client for the same scope (see
        /// <see cref="Disown"/>), or a consent wipe failed and the instance
        /// is tombstoned until a relaunch retries it (see <see cref="Wipe"/>).
        /// Every later file mutation through this instance becomes a no-op.
        /// Guarded by <c>_lock</c>.
        /// </summary>
        private bool _disowned;

        /// <summary>
        /// True while the FILE is known to still hold entries the committed
        /// in-memory state already settled: a <see cref="Remove"/> rewrite
        /// failed AFTER its in-memory commit. While owed, "no matching
        /// in-memory entries" proves nothing about the disk, so Remove must
        /// rewrite the file and confirm before reporting a removal
        /// CONFIRMED — durability-gated retirement for callers keying a
        /// durable marker to that confirmation (the condemned-fact purge:
        /// clearing its marker off a vacuous true would leave the withdrawn
        /// fact restorable with nothing left to filter it at relaunch).
        /// Discharged by ANY later full-state write or delete that lands (a
        /// Remove retry's rewrite, an append's combined write, an undo's
        /// rewrite, a wipe's delete). Guarded by <c>_lock</c>.
        /// </summary>
        private bool _staleFileRewriteOwed;

        /// <summary>
        /// Test-only: when set, the next atomic rewrites throw this instead of
        /// writing, simulating a full or unwritable disk. Cleared by the test.
        /// Never assigned in production.
        /// </summary>
        internal Exception WriteFailureForTesting;
        internal Action RewriteForTesting;

        /// <summary>
        /// Test-only: invoked at the start of <see cref="Remove"/>, so a test
        /// can observe ordering (e.g. that a publish settlement was recorded
        /// before the unlocked removal runs). Never assigned in production.
        /// </summary>
        internal Action RemoveHookForTesting;

        /// <summary>
        /// Test-only: invoked at the start of <see cref="UndoAppend"/>, so a
        /// test can arm a write failure between an append and its undo. Never
        /// assigned in production.
        /// </summary>
        internal Action UndoHookForTesting;

        /// <summary>
        /// Test-only: invoked inside an append AFTER its records have been
        /// written and flushed to the file, but BEFORE the durability flush
        /// that lets the append report success. Throwing here reproduces the
        /// failure shape <see cref="WriteFailureForTesting"/> cannot — a
        /// disk-full or device-I/O error surfacing at fsync, with COMPLETE
        /// newline-terminated records already on disk. Never assigned in
        /// production.
        /// </summary>
        internal Action AppendDurabilityHookForTesting;

#if !(UNITY_WEBGL && !UNITY_EDITOR)
        /// <summary>
        /// Test-only: invoked inside a writer cycle after its snapshot has
        /// been taken and its locks released, but BEFORE the I/O — the window
        /// in which an append may now commit, because appends no longer wait
        /// for <c>_fileGate</c>. It makes that interleaving reproducible
        /// instead of leaving it to thread scheduling, which is the only way
        /// to pin the epoch check deterministically. Never assigned in
        /// production.
        /// </summary>
        internal Action WriteCycleMidIoHookForTesting;
#endif

        internal Action AnalyticsClockBeforeWriteHookForTesting;
        internal Action DisownBeforeCommitHookForTesting;

        /// <summary>
        /// Test-only: when set, consent-wipe file deletions throw this instead
        /// of deleting, simulating an undeletable file. STATIC so it also
        /// reaches the forced wipe inside <see cref="TryCreate"/>; tests clear
        /// it in a finally block. Never assigned in production.
        /// </summary>
        internal static Exception DeleteFailureForTesting;

        /// <summary>
        /// Test-only: when set, consent-sweep directory enumerations throw
        /// this, simulating an unlistable directory. STATIC so it also
        /// reaches the owed-sweep retry inside <see cref="TryCreate"/>; tests
        /// clear it in a finally block. Never assigned in production.
        /// </summary>
        internal static Exception EnumerationFailureForTesting;

        private const string WipeOwedMarkerKeyPrefix = "shardpilot.spool.wipe_owed.";
        private const string SuppressedMarkerKeyPrefix = "shardpilot.spool.suppressed.";

        /// <summary>
        /// Upper bound of the persisted suppression list (ids whose failed
        /// undo left them on disk): a handful, oldest dropped first — it
        /// backs an already-rare double failure, not a routine mechanism.
        /// </summary>
        private const int SuppressedIdCap = 8;

        /// <summary>
        /// The key-value store carrying the per-scope "wipe owed" marker (the
        /// same store the SDK uses for identity and consent), or null when
        /// none is available — the marker then degrades to best-effort.
        /// </summary>
        private readonly IShardPilotKeyValueStore _markerStore;
        private readonly string _markerKey;
        private readonly string _suppressedKey;

        private ShardPilotSpool(
            string directory,
            string path,
            int maxEvents,
            long maxBytes,
            IShardPilotKeyValueStore markerStore,
            string markerKey,
            string suppressedKey,
            Action<string> log)
        {
            _directory = directory;
            _path = path;
            _maxEvents = maxEvents;
            _maxBytes = maxBytes;
            _header = DocumentHeaderFor(maxEvents, maxBytes);
            _markerStore = markerStore;
            _markerKey = markerKey;
            _suppressedKey = suppressedKey;
            _log = log;
        }

        /// <summary>
        /// Creates a spool for the given (already normalized) config and loads
        /// any previously persisted events, enforcing the caps and discarding a
        /// corrupt file. Returns null when no spool directory is resolvable —
        /// the client then simply runs without offline durability.
        ///
        /// Consent fail-closed: when a previous consent wipe could not durably
        /// remove the file, a per-scope "wipe owed" marker persisted in
        /// <paramref name="markerStore"/> forces the wipe to be retried HERE,
        /// before anything is loaded. Only a successful wipe clears the
        /// marker; until then this returns null — the spool stays disabled
        /// (never loaded, never appended), so a stale pre-denial file can
        /// never be re-sent after a later grant.
        /// </summary>
        public static ShardPilotSpool TryCreate(
            ShardPilotConfig config,
            IShardPilotKeyValueStore markerStore,
            Action<string> log)
        {
            try
            {
                var filePath = ResolveFilePath(config);
                if (string.IsNullOrEmpty(filePath))
                {
                    return null;
                }

                var scopeHash = ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey);
                var spool = new ShardPilotSpool(
                    Path.GetDirectoryName(filePath),
                    filePath,
                    config.SpoolMaxEvents,
                    config.SpoolMaxBytes,
                    markerStore,
                    WipeOwedMarkerKeyPrefix + scopeHash,
                    SuppressedMarkerKeyPrefix + scopeHash,
                    log);

                // A previous consent SWEEP of this directory that failed
                // (enumeration or a file wipe) is retried BEFORE any load;
                // while it remains owed the spool for this directory stays
                // disabled — fail-closed, mirroring the file-level marker.
                if (!TrySettleOwedSweep(Path.GetDirectoryName(filePath), markerStore, log))
                {
                    if (log != null)
                    {
                        try
                        {
                            log("shardpilot spool disabled: a consent sweep is still owed for its directory");
                        }
                        catch
                        {
                            // Logging is best-effort.
                        }
                    }

                    return null;
                }

                // A previous consent wipe that failed left a marker recording
                // THE FILE it could not remove: retry that exact path BEFORE
                // any load — regardless of the directory this launch uses, so
                // a different-directory launch can never clear another
                // directory's owed marker without actually removing its file.
                // Only a confirmed removal clears the marker; until then, fail
                // closed: no spool this launch (never loaded, never appended).
                if (!TrySettleOwedWipe(markerStore, WipeOwedMarkerKeyPrefix + scopeHash, log))
                {
                    if (log != null)
                    {
                        try
                        {
                            log("shardpilot spool disabled: a consent wipe is still owed and the file could not be removed");
                        }
                        catch
                        {
                            // Logging is best-effort.
                        }
                    }

                    return null;
                }

                spool.LoadFromDisk();
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                // AFTER the load: the writer must never race the reconcile it
                // depends on.
                spool.StartWriter();
#endif
                return spool;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Resolves the spool file path a NORMALIZED config maps to — the
        /// same directory-defaulting and (workspace, app, environment) scope
        /// hashing <see cref="TryCreate"/> uses — WITHOUT touching the file
        /// system state. Null when no directory is resolvable. The result is
        /// CANONICALIZED (absolute, "." and ".." segments resolved) so the
        /// lifecycle registry, the ownership handoff, and the wipe-owed
        /// markers all agree on one spelling per physical file; comparisons
        /// on it are case-insensitive (see the registry). Used before a
        /// client is constructed to hand the file's ownership over so the
        /// construction-time load already sees the displaced remnant.
        /// </summary>
        public static string ResolveFilePath(ShardPilotConfig config)
        {
            try
            {
                var directory = ResolveDirectory(config);
                if (string.IsNullOrEmpty(directory))
                {
                    return null;
                }

                return Path.Combine(
                    directory,
                    FileNamePrefix + ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey) + FileNameSuffix);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Resolves and CANONICALIZES the spool directory a config maps to
        /// (explicit <see cref="ShardPilotConfig.SpoolDir"/>, else the
        /// app-private default), so every path derived from it — file paths,
        /// ownership comparisons, owed markers — shares one spelling per
        /// physical directory. Null when unresolvable. Internal so the
        /// consent-denial propagation can de-duplicate directory sweeps by
        /// canonical directory.
        /// </summary>
        internal static string ResolveDirectory(ShardPilotConfig config)
        {
            var directory = config.SpoolDir;
            if (string.IsNullOrEmpty(directory))
            {
                try
                {
                    directory = Application.persistentDataPath;
                }
                catch
                {
                    directory = null;
                }
            }

            if (string.IsNullOrEmpty(directory))
            {
                return null;
            }

            try
            {
                // Canonicalize: two spellings of one directory (a "." segment,
                // a relative form) must map to ONE owner.
                return Path.GetFullPath(directory);
            }
            catch
            {
                // An uncanonicalizable directory still names the location.
                return directory;
            }
        }

        /// <summary>
        /// The per-scope key of the "wipe owed" marker, exposed so tests can
        /// seed and inspect it.
        /// </summary>
        internal static string WipeOwedMarkerKeyForTesting(ShardPilotConfig config)
        {
            return WipeOwedMarkerKeyPrefix +
                   ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey);
        }

        /// <summary>
        /// The per-scope key of the suppressed-ids marker, exposed so tests
        /// can seed and inspect it.
        /// </summary>
        internal static string SuppressedMarkerKeyForTesting(ShardPilotConfig config)
        {
            return SuppressedMarkerKeyPrefix +
                   ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey);
        }

        /// <summary>
        /// Retries an owed consent wipe recorded in the marker (the value IS
        /// the absolute path of the file the wipe could not remove). Returns
        /// true when nothing is owed or the recorded file is confirmed gone
        /// (the marker is then cleared); false when it still cannot be
        /// removed — the caller fails closed. Never throws.
        /// </summary>
        private static bool TrySettleOwedWipe(IShardPilotKeyValueStore store, string markerKey, Action<string> log)
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
                // An unreadable store is a separate failure domain; treat it
                // like the consent loader does (fail open on the STORE — the
                // wipe itself already ran best-effort when the marker was set).
                return true;
            }

            if (string.IsNullOrEmpty(owedPath))
            {
                return true;
            }

            if (!TryDeleteSpoolFile(owedPath))
            {
                return false;
            }

            try
            {
                store.Set(markerKey, string.Empty);
            }
            catch
            {
                // Best-effort; an uncleared marker only costs one redundant
                // (idempotent) wipe attempt on the next launch.
            }

            return true;
        }

        private void SetWipeOwedLocked()
        {
            SetWipeOwed(_markerStore, _markerKey, _path);
        }

        private void ClearWipeOwedLocked()
        {
            ClearWipeOwedIfSatisfied(_markerStore, _markerKey, _path);
        }

        private static void SetWipeOwed(IShardPilotKeyValueStore store, string markerKey, string path)
        {
            if (store == null)
            {
                return;
            }

            try
            {
                // The marker records WHICH file the wipe could not remove, so
                // a later launch retries that exact path even if it resolves
                // a different spool directory.
                store.Set(markerKey, path);
            }
            catch
            {
                // Best-effort: the next denial or launch retries the wipe.
            }
        }

        private static void ClearWipeOwedIfSatisfied(IShardPilotKeyValueStore store, string markerKey, string path)
        {
            if (store == null)
            {
                return;
            }

            try
            {
                // Clear ONLY a marker that this successful wipe actually
                // satisfies: one recording our own file (case-insensitive, the
                // same conservative comparison the lifecycle registry uses),
                // or one whose recorded file is already gone. A marker
                // recording a DIFFERENT, still existing file must survive for
                // the launch that can remove it.
                var owedPath = ShardPilotConfig.Trim(store.Get(markerKey));
                if (string.IsNullOrEmpty(owedPath))
                {
                    return;
                }

                if (string.Equals(owedPath, path, StringComparison.OrdinalIgnoreCase) || ConfirmedAbsent(owedPath))
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

        /// <summary>
        /// Fail-closed consent wipe of EVERY spool scope in the directory the
        /// config resolves to. The persisted consent decision is GLOBAL while
        /// spool files are per-(workspace, app, environment) scope, so a
        /// denial must reach the other scopes' files too — otherwise a denial
        /// under one scope, a grant, and a relaunch under another scope would
        /// re-send that scope's pre-denial backlog. Only files matching the
        /// spool's exact canonical filename shape are touched (never foreign
        /// files); each file that cannot be confirmed deleted gets the same
        /// path-scoped "wipe owed" marker as the single-file wipe, keyed by
        /// the scope hash embedded in its name, so the launch that CAN remove
        /// it retries before any load. LIMITATION (documented): only the
        /// ACTIVE directory is enumerable — files under a different custom
        /// SpoolDir from an earlier run are out of reach; keep
        /// SpoolDir stable. Never throws.
        /// Returns true when the sweep fully completed (or there was
        /// nothing to sweep); false while it remains OWED — the caller must
        /// treat the directory as quarantined and keep any LIVE spool it
        /// already holds out of play, or data appended after this sweep
        /// would be destroyed when a later launch retries the owed sweep.
        /// </summary>
        public static bool WipeAllScopesAtResolvedDirectory(
            ShardPilotConfig config,
            IShardPilotKeyValueStore markerStore,
            Action<string> log)
        {
            try
            {
                var directory = ResolveDirectory(config);
                if (string.IsNullOrEmpty(directory))
                {
                    // Nothing resolvable means no spool could ever have
                    // existed here — nothing is owed.
                    return true;
                }

                return SweepDirectory(directory, markerStore, log);
            }
            catch
            {
                // A consent wipe must never throw into game code — but an
                // unknown sweep outcome is not a clean one.
                return false;
            }
        }

        /// <summary>
        /// Unknown metadata holds this plane; known pending paths hold this
        /// directory and possible aliases until checked original-path retries
        /// settle them. Independent directories stay available. Never throws.
        /// </summary>
        internal static bool IsSweepOwedAtResolvedDirectory(
            ShardPilotConfig config,
            IShardPilotKeyValueStore store)
        {
            try
            {
                var directory = ResolveDirectory(config);
                if (string.IsNullOrEmpty(directory) || store == null)
                {
                    return false;
                }

                return ShardPilotDirectorySweep.Owed(store, ShardPilotDirectorySweep.EventKey, directory);
            }
            catch
            {
                return false;
            }
        }

        internal static bool RetryOwedPurge(ShardPilotConfig config, IShardPilotKeyValueStore store,
            Action<string> log, bool sweepOwed)
        {
            var directory = ResolveDirectory(config);
            var swept = string.IsNullOrEmpty(directory) || ShardPilotDirectorySweep.Retry(store,
                ShardPilotDirectorySweep.EventKey, directory, owed => SweepDirectoryFiles(owed, store, log), sweepOwed);
            // A denied append can leave a file marker after the directory sweep.
            return swept && TrySettleOwedWipe(store, WipeOwedMarkerKeyPrefix +
                ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey), log);
        }

        /// <summary>
        /// Captures directory obligations before file I/O and retires only those
        /// captures after a successful sweep. Failed or unknown metadata withholds
        /// acknowledgment; later construction retries the captured original paths.
        /// </summary>
        private static bool SweepDirectory(string directory, IShardPilotKeyValueStore markerStore, Action<string> log) =>
            ShardPilotDirectorySweep.Sweep(markerStore, ShardPilotDirectorySweep.EventKey, directory,
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
                    string[] files = null;
                    try
                    {
                        var enumerationFailure = EnumerationFailureForTesting;
                        if (enumerationFailure != null)
                        {
                            throw enumerationFailure;
                        }

                        files = Directory.GetFiles(directory, FileNamePrefix + "*" + FileNameSuffix);
                    }
                    catch
                    {
                        succeeded = false;
                    }

                    if (files != null)
                    {
                        for (var i = 0; i < files.Length; i++)
                        {
                            var scopeHash = ExtractScopeHash(Path.GetFileName(files[i]));
                            if (scopeHash == null)
                            {
                                // Not the canonical spool filename shape:
                                // never touch a foreign file.
                                continue;
                            }

                            var markerKey = WipeOwedMarkerKeyPrefix + scopeHash;
                            if (TryDeleteSpoolFile(files[i]))
                            {
                                ClearWipeOwedIfSatisfied(markerStore, markerKey, files[i]);
                            }
                            else
                            {
                                succeeded = false;
                                SetWipeOwed(markerStore, markerKey, files[i]);
                                if (log != null)
                                {
                                    try
                                    {
                                        log("shardpilot spool: consent wipe could not remove a scope's file; " +
                                            "wipe owed — retried before any load on a later launch");
                                    }
                                    catch
                                    {
                                        // Logging is best-effort.
                                    }
                                }
                            }
                        }
                    }

                    // ORPHANED temp files: a process that died between the
                    // temp write and the atomic move leaves envelope JSON in
                    // `<file>.tmp` with NO live file — the glob above never
                    // sees it (TryDeleteSpoolFile removes the temp SIBLING of
                    // each live file, but an orphan has no live file to ride
                    // along with, and its return value lets a failed sibling
                    // delete pass). A consent denial must reach those bytes
                    // too. Only temp names whose BASE is the exact canonical
                    // spool shape are touched, and the temp is deleted
                    // DIRECTLY with strict confirmation: a non-throwing
                    // Delete confirms; after a throwing one, only a
                    // non-throwing negative existence probe does. When the
                    // scope's LIVE file also survived this sweep, its owed
                    // marker (set above) is never overwritten — that marker's
                    // retry removes the temp sibling along with the live
                    // file.
                    string[] tempFiles = null;
                    try
                    {
                        var enumerationFailure = EnumerationFailureForTesting;
                        if (enumerationFailure != null)
                        {
                            throw enumerationFailure;
                        }

                        tempFiles = Directory.GetFiles(directory, FileNamePrefix + "*" + FileNameSuffix + ".tmp");
                    }
                    catch
                    {
                        succeeded = false;
                    }

                    if (tempFiles != null)
                    {
                        for (var i = 0; i < tempFiles.Length; i++)
                        {
                            var name = Path.GetFileName(tempFiles[i]);
                            var scopeHash = name != null && name.EndsWith(".tmp", StringComparison.Ordinal)
                                ? ExtractScopeHash(name.Substring(0, name.Length - 4))
                                : null;
                            if (scopeHash == null)
                            {
                                continue;
                            }

                            var tempDeleted = false;
                            try
                            {
                                var failure = DeleteFailureForTesting;
                                if (failure != null)
                                {
                                    throw failure;
                                }

                                File.Delete(tempFiles[i]);
                                tempDeleted = true;
                            }
                            catch
                            {
                                // Fall through to the guarded probe below.
                            }

                            if (!tempDeleted && !ConfirmedAbsent(tempFiles[i]))
                            {
                                succeeded = false;
                                var livePath = tempFiles[i].Substring(0, tempFiles[i].Length - 4);
                                if (ConfirmedAbsent(livePath))
                                {
                                    SetWipeOwed(markerStore, WipeOwedMarkerKeyPrefix + scopeHash, tempFiles[i]);
                                }
                                if (log != null)
                                {
                                    try
                                    {
                                        log("shardpilot spool: consent wipe could not remove an orphaned temp file; " +
                                            "wipe owed — retried before any load on a later launch");
                                    }
                                    catch
                                    {
                                        // Logging is best-effort.
                                    }
                                }
                            }
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
        /// Retries an owed directory sweep recorded by a previous failed
        /// consent sweep. Returns true when nothing is owed or the re-run
        /// sweep completed (marker cleared); false while it remains owed —
        /// the caller fails closed for this directory. Never throws.
        /// </summary>
        private static bool TrySettleOwedSweep(string directory, IShardPilotKeyValueStore store, Action<string> log) =>
            ShardPilotDirectorySweep.Retry(store, ShardPilotDirectorySweep.EventKey, directory,
                owed => SweepDirectoryFiles(owed, store, log));

        /// <summary>
        /// Extracts the scope hash from a spool file name, or null when the
        /// name is not EXACTLY the canonical
        /// <c>shardpilot.spool.&lt;16 lowercase hex&gt;.json</c> shape — the
        /// strictness is what keeps directory sweeps away from foreign files.
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

        /// <summary>
        /// Records event ids whose provisional append could not be UNDONE on
        /// disk (a failed undo rewrite after a close race): the next launch
        /// skips them at load and removes them from the file, so an event
        /// whose Track was rejected can never deliver. Deliberately NOT a
        /// wipe — one stuck event must not destroy legitimate backlog. The
        /// persisted list is bounded (oldest dropped first); it backs an
        /// already-rare double failure. Never throws.
        /// </summary>
        public void SuppressOnNextLaunch(IReadOnlyList<string> ids)
        {
            if (ids == null || ids.Count == 0)
            {
                return;
            }

            try
            {
                lock (_lock)
                {
                    var existing = ReadSuppressedIdsLocked();
                    var merged = new List<string>();
                    if (existing != null)
                    {
                        merged.AddRange(existing);
                    }

                    for (var i = 0; i < ids.Count; i++)
                    {
                        if (!string.IsNullOrEmpty(ids[i]) && !merged.Contains(ids[i]))
                        {
                            merged.Add(ids[i]);
                        }
                    }

                    while (merged.Count > SuppressedIdCap)
                    {
                        merged.RemoveAt(0);
                    }

                    WriteSuppressedIdsLocked(merged);
                }
            }
            catch
            {
                // Best-effort: without the marker the entry is simply re-sent
                // and de-duplicated by the ingest service.
            }
        }

        /// <summary>
        /// Reads the persisted suppression list (comma-joined ids) as an
        /// ordered list, or null when empty/absent/unreadable. Caller holds
        /// <c>_lock</c> (or runs during single-threaded construction).
        /// </summary>
        private List<string> ReadSuppressedIdsLocked()
        {
            if (_markerStore == null)
            {
                return null;
            }

            try
            {
                var raw = ShardPilotConfig.Trim(_markerStore.Get(_suppressedKey));
                if (string.IsNullOrEmpty(raw))
                {
                    return null;
                }

                var parts = raw.Split(',');
                List<string> ids = null;
                for (var i = 0; i < parts.Length; i++)
                {
                    var id = ShardPilotConfig.Trim(parts[i]);
                    if (!string.IsNullOrEmpty(id))
                    {
                        if (ids == null)
                        {
                            ids = new List<string>();
                        }

                        ids.Add(id);
                    }
                }

                return ids;
            }
            catch
            {
                return null;
            }
        }

        private void WriteSuppressedIdsLocked(List<string> ids)
        {
            if (_markerStore == null)
            {
                return;
            }

            try
            {
                if (ids == null || ids.Count == 0)
                {
                    // Only rewrite when something is set.
                    if (!string.IsNullOrEmpty(ShardPilotConfig.Trim(_markerStore.Get(_suppressedKey))))
                    {
                        _markerStore.Set(_suppressedKey, string.Empty);
                    }

                    return;
                }

                _markerStore.Set(_suppressedKey, string.Join(",", ids.ToArray()));
            }
            catch
            {
                // Best-effort; a stale list self-heals at the next load.
            }
        }

        /// <summary>
        /// Deletes one spool file (plus its temp sibling) and reports whether
        /// the live file is CONFIRMED gone. Shared by the instance consent
        /// wipe, the owed-wipe retry at construction, and the directory
        /// sweep. Confirmation is strict: a Delete call that completes
        /// without throwing is itself confirmation (it removed the file or
        /// found nothing to remove); after a THROWING delete, only a
        /// non-throwing, negative existence probe confirms — any inability
        /// to confirm counts as NOT deleted, so a storage hiccup can never
        /// clear an owed marker while the file survives.
        /// </summary>
        private static bool TryDeleteSpoolFile(string path)
        {
            var deleteConfirmed = false;
            try
            {
                var failure = DeleteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                File.Delete(path);
                deleteConfirmed = true;
            }
            catch
            {
                // Fall through to the guarded existence probe below.
            }

            try
            {
                File.Delete(path + ".tmp");
            }
            catch
            {
                // The temp file is auxiliary; the live file governs.
            }

            return deleteConfirmed || ConfirmedAbsent(path);
        }

        /// <summary>
        /// True only when a NON-THROWING existence probe reports the file
        /// absent. A probe that throws — or anything else preventing
        /// confirmation — counts as "cannot confirm", i.e. present, so
        /// fail-closed callers keep their owed markers.
        /// </summary>
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

        /// <summary>The absolute path of the spool file for this scope.</summary>
        public string FilePath
        {
            get { return _path; }
        }

        /// <summary>
        /// True once this instance lost its claim on the file (ownership
        /// transfer or consent tombstone) — its persist paths then hand
        /// snapshots to the successor instead of writing.
        /// </summary>
        internal bool IsDisowned
        {
            get
            {
                lock (_lock)
                {
                    return _disowned;
                }
            }
        }

        internal int EventCountForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _entries.Count;
                }
            }
        }

        internal void RefreshSuppressedClocks()
        {
            // Registration can follow a provisional rejection after load.
            // Read on that same main-thread barrier: the settings backend
            // may use a different fallback when read from a worker.
            lock (_lock)
            {
                var suppressed = ReadSuppressedIdsLocked();
                if (suppressed != null)
                    foreach (var entry in _entries)
                        if (suppressed.Contains(entry.Id)) entry.Clock?.MarkSettled();
            }
        }

        internal bool AdoptAnalyticsClocks(IReadOnlyList<ShardPilotEvent> events)
        {
            var byId = new Dictionary<string, ShardPilotEvent>(StringComparer.Ordinal);
            foreach (var ev in events) byId[ev.Id] = ev;
            lock (_lock)
            {
                if (_disowned) return false;
                foreach (var entry in _entries)
                {
                    ShardPilotEvent ev;
                    if (!byId.TryGetValue(entry.Id, out ev)) continue;
                    var retained = entry.Clock;
                    var current = ev.AnalyticsClock;
                    if (retained == null || current == null) continue;
                    if (retained.AmbiguousMs != 0 &&
                        (current.AmbiguousMs == 0 || retained.AmbiguousMs < current.AmbiguousMs))
                        ev.AnalyticsClock = retained;
                }
                return true;
            }
        }

        /// <summary>
        /// Fences retry metadata for already retained entries through the normal
        /// writer. This never appends or evicts events. Track callers still use
        /// only the memory lock while the writer performs I/O.
        /// </summary>
        public bool PersistAnalyticsClocks(IReadOnlyList<ShardPilotEvent> events)
        {
            var clocks = new Dictionary<string, ShardPilotAnalyticsClock>(StringComparer.Ordinal);
            for (var i = 0; i < events.Count; i++)
            {
                if (events[i].AnalyticsClock == null) return false;
                clocks[events[i].Id] = events[i].AnalyticsClock;
            }
            lock (_fileGate)
            {
                var retained = false;
                var changed = false;
                var previous = new Dictionary<SpoolEntry, SpoolEntry>();
                lock (_lock)
                {
                    if (_disowned) return false;
                    for (var i = 0; i < _entries.Count; i++)
                    {
                        ShardPilotAnalyticsClock clock;
                        var entry = _entries[i];
                        if (!clocks.TryGetValue(entry.Id, out clock)) continue;
                        retained = true;
                        if (entry.Clock != null && entry.Clock.IntakeMs == clock.IntakeMs &&
                            entry.Clock.AmbiguousMs == clock.AmbiguousMs) continue;
                        var replacement = new SpoolEntry(entry.Id, entry.Json, entry.Bytes, clock);
                        previous.Add(replacement, entry);
                        _totalRecordBytes += replacement.RecordBytes - entry.RecordBytes;
                        _entries[i] = replacement;
                        changed = true;
                    }
                    if (!retained) return true;
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                    if (changed)
                    {
                        _rewriteOwed = true;
                        _writeEpoch++;
                        SignalWriter();
                    }
#endif
                }
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                // Reentrant file gate, with no memory lock held. A concurrent
                // append moves the existing writer epoch and retains its debt.
                AnalyticsClockBeforeWriteHookForTesting?.Invoke();
                var cycle = RunWriteCycle();
                var saved = cycle == WriteCycleResult.Written || cycle == WriteCycleResult.Idle;
#else
                bool saved;
                lock (_lock) saved = !changed || TryWriteLocked(_entries);
#endif
                lock (_lock)
                {
                    // A completed transfer invalidates even an idle cycle.
                    // The caller must repeat the operation at the new owner.
                    if (_disowned) return false;
                    if (saved) return true;
                    // Keep the file gate until the failed candidate is gone
                    // from the writer's source of truth. A later recovery
                    // flush must publish the exact pre-fence clock. Restore
                    // by entry identity: concurrent eviction, replacement or
                    // removal must not resurrect a superseded occurrence.
                    var restored = false;
                    for (var i = 0; i < _entries.Count; i++)
                    {
                        SpoolEntry entry;
                        if (!previous.TryGetValue(_entries[i], out entry)) continue;
                        _totalRecordBytes += entry.RecordBytes - _entries[i].RecordBytes;
                        _entries[i] = entry;
                        restored = true;
                    }
                    if (restored)
                    {
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                        _rewriteOwed = true;
                        _writeEpoch++;
                        SignalWriter();
#endif
                    }
                }
                return false;
            }
        }

        internal bool RestoreAnalyticsClocks(
            IReadOnlyDictionary<string, KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>> previous, bool persist)
        {
            lock (_fileGate)
            {
                var changed = false;
                lock (_lock)
                {
                    if (_disowned) return false;
                    for (var i = 0; i < _entries.Count; i++)
                    {
                        var entry = _entries[i];
                        KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock> clocks;
                        if (entry.Clock == null || !previous.TryGetValue(entry.Id, out clocks) ||
                            entry.Clock.IntakeMs != clocks.Key.IntakeMs || entry.Clock.AmbiguousMs != clocks.Key.AmbiguousMs) continue;
                        var replacement = new SpoolEntry(entry.Id, entry.Json, entry.Bytes, clocks.Value);
                        _totalRecordBytes += replacement.RecordBytes - entry.RecordBytes;
                        _entries[i] = replacement;
                        changed = true;
                    }
                    if (!changed && !persist) return true;
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                    _rewriteOwed = true;
                    _writeEpoch++;
                    SignalWriter();
#endif
                }
                // This state is known not to have been submitted. A failed
                // disk write must retain the corrected memory and its debt.
                if (!persist) return true;
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                var cycle = RunWriteCycle();
                lock (_lock) return !_disowned && cycle != WriteCycleResult.Failed;
#else
                lock (_lock) return !_disowned && TryWriteLocked(_entries);
#endif
            }
        }

        /// <summary>
        /// Materializes the loaded entries as events ready for re-send: each
        /// carries its local retry clocks, original ID and verbatim
        /// serialized envelope, which the batch serializer emits unchanged.
        /// Entries REMAIN in the spool until the server acknowledges them
        /// (ack-based removal), so an app killed mid-resend loses nothing.
        /// </summary>
        public List<ShardPilotEvent> CreateRestoredEvents()
        {
            lock (_lock)
            {
                var restored = new List<ShardPilotEvent>(_entries.Count);
                for (var i = 0; i < _entries.Count; i++)
                {
                    restored.Add(new ShardPilotEvent
                    {
                        Id = _entries[i].Id,
                        SerializedEnvelope = _entries[i].Json,
                        AnalyticsClock = _entries[i].Clock
                    });
                }

                return restored;
            }
        }

        /// <summary>
        /// Permanently releases this spool's claim on its file: every later
        /// append/remove/wipe (any file mutation) through this instance
        /// becomes a no-op. Called when a NEWER client for the same
        /// (workspace, app, environment) scope registers — the newest client
        /// owns the file, and a still-draining or closing older client must
        /// not rewrite it from a stale in-memory view. Thread-safe; already
        /// restored in-memory events are unaffected.
        /// </summary>
        public void Disown()
        {
#if !(UNITY_WEBGL && !UNITY_EDITOR)
            lock (_lock)
            {
                if (_disowned)
                {
                    return;
                }
            }

            // DRAIN, then disown — an ownership transfer is not a denial.
            // Records committed before the handover belong on disk exactly as
            // they would under a synchronous write, and the successor loads
            // them. Only the consent wipe drops queued records, and it is the
            // one caller that must.
            //
            // Both calls outside the lock: a cycle takes _fileGate then _lock,
            // so running either under _lock deadlocks against one.
            FlushPendingWrites();
            if (!StopWriter())
            {
                // Ownership is released ANYWAY — refusing would strand the
                // scope for every future client — but a successor may now be
                // loading a file the predecessor is still writing, and that is
                // worth a line in the log rather than a silent race.
                Log("shardpilot spool: writer did not stop within the join bound; ownership released with a write possibly in flight");
            }
#endif

            DisownBeforeCommitHookForTesting?.Invoke();
            lock (_lock)
            {
                if (_disowned)
                {
                    return;
                }

                _disowned = true;
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                _pendingWrites.Clear();
                _pendingWriteBytes = 0;
#endif
            }

            Log("shardpilot spool: file ownership transferred to a newer client for this scope");
        }

        /// <summary>
        /// Appends events to the spool, de-duplicated by <c>event_id</c>: an
        /// event that is already spooled (a retried batch, or a pause snapshot
        /// overlapping an earlier failure append) is skipped, and when nothing
        /// new was added the file is not rewritten. Events are serialized with
        /// the same envelope writer the wire path uses (an event restored from
        /// the spool keeps its original envelope verbatim).
        ///
        /// Two hard rules of this path:
        /// - A single envelope larger than the byte cap on its own is rejected
        ///   UP FRONT, without participating in eviction — otherwise it would
        ///   evict the whole backlog trying to make room it can never fit in.
        /// - Additions are committed to the in-memory index ONLY after the
        ///   file rewrite succeeded. A failed write (full disk, permissions)
        ///   must not mark the events as spooled, or later appends would
        ///   de-duplicate them away and a recovered disk would never get them.
        ///
        /// Returns true when everything passed is durably in the spool file
        /// after the call (already-present duplicates count as present, and
        /// nothing-new counts as success); false when the spool is disowned,
        /// an event was rejected, or the rewrite failed. Never throws.
        /// </summary>
        public bool Append(ShardPilotConfig config, IReadOnlyList<ShardPilotEvent> events)
        {
            AppendUndoToken unusedToken;
            return AppendCore(config, events, false, out unusedToken);
        }

        /// <summary>
        /// Like <see cref="Append(ShardPilotConfig, IReadOnlyList{ShardPilotEvent})"/>,
        /// additionally reporting through the token what the append staged
        /// and what its cap enforcement FIFO-evicted (the caller prunes its
        /// overflow accounting with the latter).
        /// </summary>
        public bool Append(ShardPilotConfig config, IReadOnlyList<ShardPilotEvent> events, out AppendUndoToken undoToken)
        {
            return AppendCore(config, events, false, out undoToken);
        }

        /// <summary>
        /// PROVISIONAL append — one a race check may still reject after the
        /// fact (a close racing an overflow accept). Deliberately
        /// NON-EVICTING: when the spool is at its caps the append is REJECTED
        /// instead of displacing durable backlog it might later have to put
        /// back, so an undo only ever removes what it staged and can never
        /// cost unrelated entries. The caller drops the event on rejection —
        /// the same contract as a full in-memory queue without a spool.
        /// </summary>
        public bool AppendProvisional(ShardPilotConfig config, IReadOnlyList<ShardPilotEvent> events, out AppendUndoToken undoToken)
        {
            return AppendCore(config, events, true, out undoToken);
        }

        private bool AppendCore(ShardPilotConfig config, IReadOnlyList<ShardPilotEvent> events, bool rejectWhenFull, out AppendUndoToken undoToken)
        {
            undoToken = null;
            if (events == null || events.Count == 0)
            {
                return true;
            }

            try
            {
#if UNITY_WEBGL && !UNITY_EDITOR
                // WebGL ONLY. There the append still does its own I/O inline
                // (no threads, so no writer), and the gate keeps the invariant
                // "every file-I/O entry point is under _fileGate" true for the
                // branch that actually touches the disk.
                //
                // On threaded targets this method performs NO I/O — it commits
                // memory, posts to _pendingWrites and signals the writer — so
                // taking the gate here would make every Track wait out whatever
                // disk operation the writer happens to be inside. That is the
                // latency synchronous memory admission must avoid: this path
                // takes only _lock and cannot queue behind a writer's file I/O.
                lock (_fileGate)
#endif
                lock (_lock)
                {
                    if (_disowned)
                    {
                        return false;
                    }

                    List<SpoolEntry> staged = null;
                    HashSet<string> stagedIds = null;
                    var rejected = false;
                    for (var i = 0; i < events.Count; i++)
                    {
                        var ev = events[i];
                        if (ev == null || ev.NonObjectionOrigin != null || string.IsNullOrEmpty(ev.Id) || _ids.Contains(ev.Id) ||
                            (stagedIds != null && stagedIds.Contains(ev.Id)))
                        {
                            continue;
                        }

                        string json;
                        try
                        {
                            // The compare-exchanged canonical envelope: if a
                            // racing batch build stamped first, ITS string is
                            // used here (the local snapshot is discarded), so
                            // the spooled copy and the wire attempt are the
                            // same bytes even under concurrent caller
                            // mutation of a shared nested props map.
                            json = ShardPilotJson.CanonicalEnvelope(config, ev);
                        }
                        catch
                        {
                            // An event that cannot be serialized cannot be
                            // spooled; it would have failed the wire path too.
                            rejected = true;
                            continue;
                        }

                        // A self-written file must always be self-readable:
                        // the serializer has no nesting limit, but the load
                        // path validates with the SDK's JSON parser (bounded
                        // depth) and discards what it cannot parse. Round-trip
                        // the candidate through that same parser NOW and
                        // reject an envelope the next launch could not read,
                        // instead of letting one pathological event condemn
                        // the whole backlog. The corruption fallback stays for
                        // external damage.
                        //
                        // v3 parses each RECORD on its own, so the round-trip
                        // check is the envelope by itself — which is both
                        // simpler and a truer match for the load context than
                        // wrapping it in document framing was.
                        Dictionary<string, object> roundTripped;
                        ShardPilotSealedExperimentFact sealedFact;
                        if (!ShardPilotExperimentApply.AdmitEnvelope(json, ev.Id, out sealedFact) ||
                            !ShardPilotJsonParser.TryParseObject(json, out roundTripped))
                        {
                            rejected = true;
                            Log("shardpilot spool: rejected event " + ev.Id +
                                " whose envelope cannot be re-read on load (e.g. nested too deeply); existing entries kept");
                            continue;
                        }

                        var bytes = Encoding.UTF8.GetByteCount(json);
                        if (bytes > _maxBytes)
                        {
                            rejected = true;
                            Log("shardpilot spool: rejected oversized event " + ev.Id + " (" +
                                bytes.ToString(CultureInfo.InvariantCulture) + " bytes > cap " +
                                _maxBytes.ToString(CultureInfo.InvariantCulture) + "); existing entries kept");
                            continue;
                        }

                        if (staged == null)
                        {
                            staged = new List<SpoolEntry>();
                            stagedIds = new HashSet<string>(StringComparer.Ordinal);
                        }

                        staged.Add(new SpoolEntry(ev.Id, json, bytes, ev.AnalyticsClock));
                        stagedIds.Add(ev.Id);
                    }

                    if (staged == null)
                    {
                        // Nothing new to write: no file churn (an idempotent
                        // retry of an already-spooled batch lands here).
                        return !rejected;
                    }

                    // The prospective state is the committed entries plus the
                    // staged ones, oldest first. It is deliberately NOT
                    // materialised here: building that list and re-deriving
                    // the id index from it is O(spool size) per append — the
                    // same shape as the file rewrite, just in memory instead
                    // of on disk — and with the file write already an append
                    // it was what remained of the growth the bound forbids.
                    // So the caps are worked out from the running totals, and
                    // the combined list is built only on the paths that
                    // rewrite the whole file anyway.
                    long stagedBytes = 0;
                    long stagedRecordBytes = 0;
                    for (var i = 0; i < staged.Count; i++)
                    {
                        stagedBytes += staged[i].Bytes;
                        stagedRecordBytes += staged[i].RecordBytes + 1;
                    }

                    var overCaps = _entries.Count + staged.Count > _maxEvents ||
                        _totalBytes + stagedBytes > _maxBytes;

                    // A PROVISIONAL append is rejected when it does not fit,
                    // rather than evicting to make room (see
                    // AppendProvisional); a NORMAL append FIFO-evicts.
                    if (rejectWhenFull && overCaps)
                    {
                        Log("shardpilot spool full: rejected a provisional append (oldest backlog preserved)");
                        return false;
                    }

                    int evictFromCommitted;
                    int evictFromStaged;
                    var evicted = PlanEvictionLocked(
                        staged, stagedBytes, out evictFromCommitted, out evictFromStaged);

                    long evictedRecordBytes = 0;
                    var evictedCount = 0;
                    if (evicted != null)
                    {
                        evictedCount = evicted.Count;
                        for (var i = 0; i < evicted.Count; i++)
                        {
                            evictedRecordBytes += evicted[i].RecordBytes + 1;
                        }
                    }

                    // What the spool will HOLD once this append lands, as
                    // against what the file will CONTAIN — the two diverge by
                    // the dead records, and that divergence is what compaction
                    // reads.
                    var liveRecordBytes = _totalRecordBytes + stagedRecordBytes - evictedRecordBytes;

                    // Appends are appends. The fast path writes ONLY the new
                    // records to the end of the file, so an overflow costs what
                    // it adds rather than what the spool already holds — the
                    // O(spool size) rewrite this path used to do is why a
                    // single overflow event cost 2.6 ms at a full spool
                    // in the recorded overflow measurement. Eviction does not
                    // force a rewrite: it drops the entry from the committed
                    // state and leaves its bytes behind as a dead record, which
                    // compaction reclaims later (see CompactionSlackFactor).
                    //
                    // A full rewrite is still needed when the file is not in an
                    // appendable state, when compaction is due, or when this
                    // append is so large that it evicts its OWN records — a
                    // batch bigger than the whole spool, where appending bytes
                    // that are already dead is pointless.
                    var appendBytes = stagedRecordBytes;
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                    // Memory commits here; disk follows asynchronously.
                    //
                    // No file I/O on this path and no wait for any, which is
                    // exactly what the bound measures. Append takes only
                    // _lock, never _fileGate, so it can never queue behind a
                    // writer cycle that is mid-I/O.
                    //
                    // The pending bytes count toward the compaction decision:
                    // they are going to be in the file, and leaving them out
                    // let the trigger read a file smaller than the one the
                    // writer is about to produce.
                    var rewriteNow = _rewriteOwed || !_fileIsAppendable || evictFromStaged > 0 ||
                        CompactionDueLocked(
                            _fileBytes + _pendingWriteBytes + appendBytes, liveRecordBytes);
                    if (rewriteNow)
                    {
                        var combinedState = CombineLocked(staged);
                        if (evictedCount > 0)
                        {
                            combinedState.RemoveRange(0, evictFromCommitted + evictFromStaged);
                        }

                        CommitLocked(combinedState);
                        // A rewrite supersedes every pending record: they are
                        // all in the committed state it will be written from.
                        _pendingWrites.Clear();
                        _pendingWriteBytes = 0;
                        _rewriteOwed = true;
                    }
                    else
                    {
                        AppendCommitLocked(staged, stagedBytes, evictFromCommitted);
                        _pendingWrites.AddRange(staged);
                        _pendingWriteBytes += appendBytes;
                    }

                    // Work now exists that a cycle already inside its I/O
                    // cannot know about. This is the ONE path that can reach
                    // here while a cycle runs — every other file-touching
                    // entry point still takes _fileGate and therefore waits
                    // the cycle out — and it is why the writer checks the
                    // epoch before clearing a debt. See RunWriteCycle.
                    _writeEpoch++;
                    SignalWriter();
#else
                    var rewrite = !_fileIsAppendable || evictFromStaged > 0 ||
                        CompactionDueLocked(_fileBytes + appendBytes, liveRecordBytes);
                    var appendMayHaveLanded = false;
                    if (!rewrite && !TryAppendLocked(staged, appendBytes))
                    {
                        // The append did not land. Fall back to the full
                        // rewrite, which also re-establishes a document shape
                        // this class knows — and which reports honestly if the
                        // disk is genuinely unwritable.
                        //
                        // "Did not land" is weaker than "changed nothing": the
                        // records are written one after another and the throw
                        // can come from the final flush, so COMPLETE
                        // newline-terminated records may already be in the
                        // file. The rewrite below normally erases them; if it
                        // fails too, they are still there and this append is
                        // about to be reported rejected.
                        appendMayHaveLanded = true;
                        rewrite = true;
                    }

                    if (rewrite)
                    {
                        var combined = CombineLocked(staged);

                        // Exactly the planned eviction, applied to the same
                        // prospective state. PlanEvictionLocked only reaches
                        // the staged records after exhausting the committed
                        // ones, so what it evicted is always a contiguous
                        // PREFIX of `combined` — taking it that way keeps the
                        // two paths from ever disagreeing about which entries
                        // this append displaced.
                        if (evictedCount > 0)
                        {
                            combined.RemoveRange(0, evictFromCommitted + evictFromStaged);
                        }

                        if (!TryWriteLocked(combined))
                        {
                            // ONLY for a provisional append. `false` from an
                            // ordinary Append does not mean the events were
                            // rejected — SpoolAppend after a publish failure
                            // and PersistPendingEvents at shutdown both keep
                            // them, in the queue or the batch, and they stay
                            // eligible for delivery. Suppressing there would
                            // make the next launch DELETE complete records
                            // that did survive, and the marker is not cleared
                            // by a later ordinary append that rewrites them —
                            // so it would destroy the only durable copy of an
                            // accepted event. AppendProvisional is the one
                            // whose caller reports Track as rejected, and it
                            // is the only one with an event to suppress.
                            if (rejectWhenFull && appendMayHaveLanded)
                            {
                                // Both writes failed, and the first one may
                                // have left complete records behind. This
                                // append is about to be reported REJECTED —
                                // the caller creates no undo token, so nothing
                                // else will ever go looking for those records,
                                // and the next launch would parse and send
                                // events this SDK told the game it had not
                                // accepted. Same remedy as a failed undo: the
                                // next launch skips them and removes them from
                                // the file.
                                var stagedIdList = new List<string>(staged.Count);
                                for (var i = 0; i < staged.Count; i++)
                                {
                                    stagedIdList.Add(staged[i].Id);
                                }

                                SuppressOnNextLaunch(stagedIdList);
                            }

                            if (appendMayHaveLanded)
                            {
                                // Complete records may be in the file for ids
                                // that were never committed to memory, so from
                                // here "nothing matched the committed state"
                                // proves nothing about the file. Without this,
                                // a later Remove for those ids — the retained
                                // batch succeeding on its next network attempt
                                // — finds no in-memory entries, sees no debt,
                                // and confirms the removal from memory alone.
                                // The records survive, and the next launch
                                // re-sends events that were already delivered
                                // or permanently settled.
                                //
                                // Recorded for the ordinary append too, not
                                // just the provisional one: suppression is
                                // about what the NEXT launch must skip, and
                                // this is about what THIS session may confirm.
                                _staleFileRewriteOwed = true;
                            }

                            return false;
                        }

                        CommitLocked(combined);
                    }
                    else
                    {
                        AppendCommitLocked(staged, stagedBytes, evictFromCommitted);
                    }
#endif

                    // The token records EXACTLY what this append staged (plus
                    // what its eviction displaced, reported for accounting):
                    // an undo must touch only the staged ids. A pure-duplicate
                    // append returns earlier with no token — there is nothing
                    // to undo, and the pre-existing durable entries must
                    // survive an undone re-append.
                    var committedIds = new List<string>(staged.Count);
                    for (var i = 0; i < staged.Count; i++)
                    {
                        committedIds.Add(staged[i].Id);
                    }

                    undoToken = new AppendUndoToken { StagedIds = committedIds, EvictedEntries = evicted };
                    if (evicted != null)
                    {
                        Log("shardpilot spool over capacity: evicted the oldest " +
                            evicted.Count.ToString(CultureInfo.InvariantCulture) + " event(s)");
                    }

                    return !rejected;
                }
            }
            catch (Exception ex)
            {
                Log("shardpilot spool append failed: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Removes events by <c>event_id</c>. Called when the server
        /// acknowledged a batch (2xx — the spooled copies are no longer
        /// needed) and when a batch was permanently rejected (a non-retryable
        /// 4xx would fail forever on every future launch). No-ops without a
        /// file rewrite when none of the ids are spooled AND no earlier
        /// removal left a rewrite owed — while one is owed, absence from the
        /// committed memory proves nothing about the file, so the rewrite
        /// runs (and must land) before anything is confirmed. Returns true
        /// when the removal is CONFIRMED on disk (nothing needed removing
        /// with a clean file, or the rewrite/empty transition succeeded);
        /// false when the file could not be updated or the spool is disowned
        /// — settlement callers may ignore it (the committed memory filters
        /// the ids out of every later successful rewrite, and a re-sent
        /// leftover is de-duplicated or re-rejected), but a consent-denial
        /// caller must escalate to the fail-closed wipe and a marker-retiring
        /// caller (the condemned-fact purge) must keep its marker. Never
        /// throws.
        /// </summary>
        public bool Remove(IReadOnlyList<ShardPilotEvent> events)
        {
            if (events == null || events.Count == 0)
            {
                return true;
            }

            // Test seam (ordering observation); no locks held here.
            var removeHook = RemoveHookForTesting;
            if (removeHook != null)
            {
                removeHook();
            }

            try
            {
                // _fileGate BEFORE _lock; see _fileGate.
                lock (_fileGate)
                lock (_lock)
                {
                    if (_disowned)
                    {
                        return false;
                    }

                    var fileIsStale = FileIsStaleLocked();
                    if (RemoveEntriesLocked(events))
                    {
                        // Unlike Append, the in-memory removal deliberately
                        // commits BEFORE the rewrite: if the rewrite fails,
                        // the already-settled entries merely linger on disk
                        // and the worst case is a re-send on a later launch
                        // that the ingest service de-duplicates by event_id —
                        // benign, so no rollback is needed here. The outcome
                        // is still reported truthfully for callers that must
                        // fail closed instead — and the failure is REMEMBERED
                        // as a rewrite debt: the committed memory no longer
                        // matches the file, so no later call may confirm a
                        // removal from memory alone until a full-state write
                        // lands.
                        if (TryWriteLocked(_entries))
                        {
                            ClearRewriteDebtLocked();
                            return true;
                        }

                        _staleFileRewriteOwed = true;
                        return false;
                    }

                    if (fileIsStale)
                    {
                        // The file is known NOT to match the committed state,
                        // so "nothing matched the committed state" proves
                        // NOTHING about the file. Two ways to get here:
                        //
                        // - A previous removal committed in memory and its
                        //   rewrite failed; the settled entries are still on
                        //   disk. A same-process retry lands here, the first
                        //   attempt having already dropped the ids from
                        //   memory.
                        // - A rewrite is owed to the writer (or was owed and
                        //   its cycle failed), so the file may hold records
                        //   for ids the committed state does not have.
                        //
                        // Either way this must rewrite the file regardless of
                        // in-memory state and confirm only when the write
                        // lands, or a caller retiring a durable marker on this
                        // result (the condemned-fact purge) would clear it
                        // while the withdrawn facts remain restorable; a crash
                        // and relaunch would then re-send them with no marker
                        // left to filter them.
                        if (TryWriteLocked(_entries))
                        {
                            ClearRewriteDebtLocked();
                            return true;
                        }

                        return false;
                    }

                    // Nothing was present in the committed state; disk
                    // matches it (no rewrite debt), so there is nothing to
                    // confirm.
                    return true;
                }
            }
            catch (Exception ex)
            {
                Log("shardpilot spool remove failed: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Opaque handle a provisional <see cref="Append"/> hands back when
        /// its cap enforcement evicted durable entries; consumed by
        /// <see cref="UndoAppend"/> to restore them if the append is undone.
        /// </summary>
        public sealed class AppendUndoToken
        {
            /// <summary>The ids THIS append actually staged and committed.</summary>
            internal List<string> StagedIds;

            /// <summary>
            /// The durable entries a NORMAL append's cap enforcement
            /// FIFO-evicted; null when none. Reported so the client can prune
            /// its overflow accounting — provisional appends never evict
            /// (they are rejected at the caps), so undone appends never have
            /// entries to restore.
            /// </summary>
            internal List<SpoolEntry> EvictedEntries;
        }

        /// <summary>
        /// Undoes a PROVISIONAL append that a race check rejected after the
        /// fact (e.g. a close racing an overflow accept): removes EXACTLY the
        /// entries THAT append staged — nothing else. Provisional appends are
        /// NON-EVICTING (rejected at the caps instead), so an undo never has
        /// displaced backlog to restore. A NULL token means the append staged
        /// nothing (its ids were already durably spooled by an earlier
        /// append): there is nothing to undo, and the pre-existing entries
        /// must survive, so this is a strict no-op — it never falls back to
        /// removing by the caller's event list. NOT used for consent-denial
        /// undos, which use a plain <see cref="Remove"/>. Returns true when
        /// the undo is CONFIRMED on disk (or there was nothing to undo);
        /// false when the rewrite failed or the spool is disowned — the
        /// caller then records the ids on the persisted suppression list so
        /// the next launch skips and removes them. Never throws.
        /// </summary>
        public bool UndoAppend(AppendUndoToken undoToken)
        {
            if (undoToken == null || undoToken.StagedIds == null || undoToken.StagedIds.Count == 0)
            {
                return true;
            }

            // Test seam (failure injection between the append and its undo);
            // no locks held here.
            var undoHook = UndoHookForTesting;
            if (undoHook != null)
            {
                undoHook();
            }

            try
            {
                // _fileGate BEFORE _lock; see _fileGate.
                lock (_fileGate)
                lock (_lock)
                {
                    if (_disowned)
                    {
                        return false;
                    }

                    // Build the prospective post-undo state WITHOUT touching
                    // the committed one: the current entries minus exactly
                    // the ids this append staged.
                    var undoneIds = new HashSet<string>(StringComparer.Ordinal);
                    for (var i = 0; i < undoToken.StagedIds.Count; i++)
                    {
                        if (!string.IsNullOrEmpty(undoToken.StagedIds[i]))
                        {
                            undoneIds.Add(undoToken.StagedIds[i]);
                        }
                    }

                    var working = new List<SpoolEntry>(_entries.Count);
                    for (var i = 0; i < _entries.Count; i++)
                    {
                        if (!undoneIds.Contains(_entries[i].Id))
                        {
                            working.Add(_entries[i]);
                        }
                        else _entries[i].Clock?.MarkSettled();
                    }

                    // Mirror the append path's ordering: write the reduced
                    // state atomically FIRST, and commit memory only on
                    // success. A failed rewrite leaves the undo pending —
                    // memory and the file both keep the previous generation,
                    // and the caller suppresses the staged ids for the next
                    // launch.
                    if (TryWriteLocked(working))
                    {
                        CommitLocked(working);
                        return true;
                    }

                    Log("shardpilot spool: undo rewrite failed; the undo stays pending");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log("shardpilot spool undo failed: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Removes the given events from the committed in-memory state by
        /// event id. Caller must hold <c>_lock</c>; returns true when at
        /// least one entry was removed.
        /// </summary>
        private bool RemoveEntriesLocked(IReadOnlyList<ShardPilotEvent> events)
        {
            if (events == null || events.Count == 0)
            {
                return false;
            }

            var removed = false;
            for (var i = 0; i < events.Count; i++)
            {
                var ev = events[i];
                if (ev == null || string.IsNullOrEmpty(ev.Id) || !_ids.Remove(ev.Id))
                {
                    continue;
                }

                for (var j = 0; j < _entries.Count; j++)
                {
                    if (string.Equals(_entries[j].Id, ev.Id, StringComparison.Ordinal))
                    {
                        _totalBytes -= _entries[j].Bytes;
                        _totalRecordBytes -= _entries[j].RecordBytes + 1;
                        _entries.RemoveAt(j);
                        break;
                    }
                }

                removed = true;
            }

            return removed;
        }

        /// <summary>
        /// Destroys the spool: clears every entry and deletes the file. Used
        /// when analytics consent is denied — previously undeliverable events
        /// must never be re-sent on a later launch. FAIL-CLOSED: when the
        /// file cannot be durably removed, a per-scope "wipe owed" marker is
        /// persisted; the next construction retries the wipe BEFORE any load
        /// and keeps the spool disabled until it succeeds, so a swallowed
        /// deletion failure can never leak a pre-denial backlog after a later
        /// grant. Never throws.
        /// </summary>
        public void Wipe()
        {
            try
            {
                // _fileGate BEFORE _lock. Nesting it around the delete instead
                // would invert the order against the writer; see _fileGate.
                lock (_fileGate)
                lock (_lock)
                {
                    _entries.Clear();
                    _ids.Clear();
                    _totalBytes = 0;
                    _totalRecordBytes = 0;
#if !(UNITY_WEBGL && !UNITY_EDITOR)
                    // Queued records must NEVER reach the disk after a denial.
                    // This is the other half of the fail-closed guarantee: the
                    // gate above stops the wipe racing an in-flight cycle, and
                    // this stops a cycle that has not started from writing
                    // what the denial just condemned. Dropping them is
                    // correct — they are pre-denial events, and the wipe
                    // exists to destroy exactly those.
                    _pendingWrites.Clear();
                    _pendingWriteBytes = 0;
                    _rewriteOwed = false;
#endif
                    if (_disowned)
                    {
                        // In-memory state is cleared, but the FILE belongs to
                        // a newer client for this scope now; leave it alone.
                        return;
                    }

                    if (TryDeleteFilesLocked())
                    {
                        // Also clears a marker left by an earlier failed wipe
                        // once the retry finally succeeds — and any pending
                        // Remove rewrite debt: the file is gone, so no stale
                        // settled entries can linger in it.
                        _staleFileRewriteOwed = false;
                        // The file is GONE, so it cannot be appended to. This
                        // is not bookkeeping: leaving _fileIsAppendable true
                        // sends the next append down the fast path, where
                        // FileMode.Append RECREATES the deleted file and
                        // writes records into it with no v3 header. The next
                        // launch can parse that as neither format and
                        // discards the whole backlog — so a consent denial
                        // followed by a re-grant in the same session silently
                        // costs every post-grant event, which is the opposite
                        // of what the wipe is for.
                        _fileIsAppendable = false;
                        _fileBytes = 0;
                        ClearWipeOwedLocked();
                    }
                    else
                    {
                        SetWipeOwedLocked();

                        // Tombstone THIS instance too (the same flag the
                        // ownership transfer uses): the stale file must not
                        // accumulate post-grant appends this session only for
                        // the owed wipe to delete them on the next launch. No
                        // loads or appends happen until a relaunch whose
                        // retried wipe succeeds and starts a fresh spool.
                        _disowned = true;
                        Log("shardpilot spool: consent wipe could not remove the file; " +
                            "wipe owed — the spool is disabled until a later launch removes it");
                    }
                }
            }
            catch
            {
                // Best-effort: a wipe that cannot delete the file has still
                // cleared the in-memory entries; the owed marker (set above
                // where reachable) makes the next launch retry fail-closed.
            }
        }

        /// <summary>
        /// Consent-wipe deletion with a truthful outcome: returns true only
        /// when the spool file is durably gone afterwards.
        /// </summary>
        private bool TryDeleteFilesLocked()
        {
            return TryDeleteSpoolFile(_path);
        }

        private void LoadFromDisk()
        {
            // _fileGate BEFORE _lock; see _fileGate.
            lock (_fileGate)
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
                    // Unreadable: treat as absent. Deletion is attempted so a
                    // permanently unreadable file does not linger forever.
                    DeleteFilesLocked();
                    return;
                }

                // A v1 file is READ here and rewritten as v3 below, so the
                // migration rides a path that already rewrites.
                var wasLegacyFormat = text != null &&
                    !text.StartsWith(DocumentHeaderV3Prefix, StringComparison.Ordinal);
                bool droppedTornTail;
                int fileMaxEvents;
                long fileMaxBytes;
                var entries = ParseDocument(text, out droppedTornTail, out fileMaxEvents, out fileMaxBytes);
                if (entries == null)
                {
                    // Corrupt or truncated: silently start clean.
                    Log("shardpilot spool: discarded an unparseable spool file");
                    DeleteFilesLocked();
                    return;
                }

                // Ids on the persisted suppression list belong to events
                // whose rejection could not be undone on disk (a failed undo
                // rewrite after a close race): they are SKIPPED here — never
                // restored, never re-sent — and removed from the file by the
                // reconcile write below.
                var suppressed = ReadSuppressedIdsLocked();
                List<string> suppressedInFile = null;

                var loaded = new List<SpoolEntry>(entries.Count);

                // The LAST occurrence of an id wins, not the first.
                //
                // Logical eviction leaves the dead record on disk AND drops
                // its id from `_ids` — so nothing stops the same event being
                // appended again, and a re-insured retry batch does exactly
                // that. The file then holds the id twice. The older copy is by
                // construction the dead one (a LIVE id is never re-appended;
                // the append path skips ids `_ids` still holds), and the newer
                // copy is the live entry.
                //
                // Keeping the first occurrence got this backwards twice over:
                // the dead copy also sits in an older cap slot, so trimming
                // to the caps then resurrected a record the writing run had
                // already dropped AND lost one it still held. With cap 3,
                // A,B,C,D,A on disk is live state C,D,A; first-seen loaded
                // B,C,D.
                //
                // Taking the last occurrence reproduces the writer exactly,
                // because eviction is strictly FIFO: the live list is always
                // the tail of the append sequence with earlier duplicates
                // removed, which is what this leaves for the cap trim below.
                var lastIndexForId = new Dictionary<string, int>(entries.Count, StringComparer.Ordinal);
                for (var i = 0; i < entries.Count; i++)
                {
                    lastIndexForId[entries[i].Id] = i;
                }

                for (var i = 0; i < entries.Count; i++)
                {
                    if (suppressed != null && suppressed.Contains(entries[i].Id))
                    {
                        if (suppressedInFile == null)
                        {
                            suppressedInFile = new List<string>();
                        }

                        suppressedInFile.Add(entries[i].Id);
                        continue;
                    }

                    if (lastIndexForId[entries[i].Id] == i)
                    {
                        loaded.Add(entries[i]);
                    }
                }

                // The FILE's caps first, then the current ones. Eviction is
                // logical, so the file may carry records the run that wrote it
                // had already dropped; trimming under its caps reproduces that
                // run's cut. Without this, RAISING SpoolMaxEvents (or
                // SpoolMaxBytes) in a new app version made those records fit
                // again — and the SDK re-sent events it had already counted
                // dropped. A config change may shrink what a file carries
                // forward; it may not resurrect what was already gone.
                var evictedByFileCaps = fileMaxEvents > 0 || fileMaxBytes > 0
                    ? EvictToExplicitCaps(
                        loaded,
                        fileMaxEvents > 0 ? fileMaxEvents : _maxEvents,
                        fileMaxBytes > 0 ? fileMaxBytes : _maxBytes)
                    : null;
                // FOLD THE BACKLOG. Envelopes spooled by an EARLIER version carry
                // the platform that version wrote, and the resend path emits them
                // verbatim rather than rebuilding them -- so folding at the write
                // alone would leave exactly the events that motivated the upgrade
                // still failing. A batch is rejected WHOLE, so each stale envelope
                // would take the fresh events batched beside it down with it.
                //
                // An envelope whose repair cannot be PROVEN is dropped rather than
                // sent: it carries a platform the door rejects, so it cannot be
                // delivered as it stands, and keeping it would poison every batch
                // it rides in until the server drops it.
                var repairedCount = 0;
                var undeliverable = 0;
                var legacySessionCount = 0;
                for (var i = loaded.Count - 1; i >= 0; i--)
                {
                    // Old session names are unregistered and reject their whole
                    // batch. Read the actual top-level name, including JSON
                    // escapes; props may legitimately contain the same strings.
                    Dictionary<string, object> envelope;
                    object nameValue;
                    if (ShardPilotJsonParser.TryParseObject(loaded[i].Json, out envelope) &&
                        envelope.TryGetValue("event_name", out nameValue) &&
                        (nameValue as string == "session_start" || nameValue as string == "session_end"))
                    {
                        loaded.RemoveAt(i);
                        legacySessionCount++;
                        continue;
                    }

                    ShardPilotSealedExperimentFact sealedFact;
                    if (!ShardPilotExperimentApply.AdmitEnvelope(loaded[i].Json, loaded[i].Id, out sealedFact))
                    {
                        loaded.RemoveAt(i); undeliverable++; continue;
                    }
                    // A sealed envelope is already canonical. Keep every signed token intact.
                    if (sealedFact != null) continue;

                    string rewritten;
                    var outcome = ShardPilotJson.RepairSpooledPlatform(loaded[i].Json, out rewritten);
                    if (outcome == ShardPilotJson.PlatformRepair.Repaired)
                    {
                        loaded[i] = new SpoolEntry(
                            loaded[i].Id, rewritten, Encoding.UTF8.GetByteCount(rewritten), loaded[i].Clock);
                        repairedCount++;
                    }
                    else if (outcome == ShardPilotJson.PlatformRepair.Unsafe)
                    {
                        loaded.RemoveAt(i);
                        undeliverable++;
                    }
                }

                if (legacySessionCount > 0)
                {
                    Log("shardpilot spool load: dropped " +
                        legacySessionCount.ToString(CultureInfo.InvariantCulture) +
                        " obsolete session envelope(s) with unregistered event names");
                }

                if (repairedCount > 0 || undeliverable > 0)
                {
                    Log("shardpilot spool platform fold on load: repaired " +
                        repairedCount.ToString(CultureInfo.InvariantCulture) +
                        " envelope(s), dropped " +
                        undeliverable.ToString(CultureInfo.InvariantCulture) +
                        " that could not be repaired and would have failed their batch");
                }

                var evicted = EvictToCaps(loaded);
                if (evicted != null || evictedByFileCaps != null)
                {
                    var count = (evicted == null ? 0 : evicted.Count) +
                        (evictedByFileCaps == null ? 0 : evictedByFileCaps.Count);
                    Log("shardpilot spool over capacity on load: evicted the oldest " +
                        count.ToString(CultureInfo.InvariantCulture) + " event(s)");
                }

                CommitLocked(loaded);
                var reconciled = true;
                // A file whose stated caps differ from the configured ones is
                // rewritten so its header stops describing a previous version's
                // configuration — the next load must trim under THIS run's caps.
                var capsMatchFile = fileMaxEvents == _maxEvents && fileMaxBytes == _maxBytes;
                // `repairedCount == 0` is part of this test, not decoration. A
                // platform repair changes an entry's BYTES without changing the
                // entry COUNT, so a count-only comparison would mark the file
                // appendable while its content no longer matches memory -- breaking
                // the "memory is a subset of the file" invariant the reconcile
                // branch below relies on, and leaving `_fileBytes` describing the
                // UNREPAIRED text. A repair that shortens a near-cap backlog would
                // then let an append fit by the in-memory accounting while the file
                // exceeded the cap, and the next load applies the file caps BEFORE
                // repairing -- evicting an event this run still considered live.
                // Any repair therefore forces the reconciliation rewrite.
                if (_entries.Count == entries.Count && repairedCount == 0 &&
                    !wasLegacyFormat && !droppedTornTail && capsMatchFile)
                {
                    // The file is a v3 document holding exactly what was just
                    // committed, so the next append may extend it instead of
                    // rewriting it. Without this, the first append after every
                    // launch pays a full rewrite for nothing.
                    _fileIsAppendable = true;
                    _fileBytes = Encoding.UTF8.GetByteCount(text);
                }
                else
                {
                    // De-duplication, cap enforcement, or suppression changed
                    // the content; reconcile the file once. Memory is a subset
                    // of the file here, so a failed reconcile only leaves
                    // extra entries on disk for the next load to trim.
                    //
                    // A v1 file is rewritten even when nothing changed: that
                    // rewrite IS the migration, and until it lands the file
                    // cannot be appended to (a v1 document has no record
                    // terminators to extend).
                    //
                    // A RECOVERED TEAR is rewritten for the same kind of
                    // reason and it is the more dangerous of the two: the
                    // records parsed cleanly, so nothing else here would have
                    // noticed, while the partial bytes sat at the end of a
                    // file marked appendable. The next append would have
                    // completed them into a malformed line and cost the whole
                    // backlog at the launch after that.
                    reconciled = TryWriteLocked(_entries);
                }

                if (suppressed != null)
                {
                    // A suppressed id stays on the list only while the FILE
                    // still contains it (present and the reconcile write
                    // failed); ids removed by the rewrite — or never present —
                    // are cleared, and an empty list clears the marker.
                    WriteSuppressedIdsLocked(!reconciled ? suppressedInFile : null);
                    if (suppressedInFile != null && reconciled)
                    {
                        Log("shardpilot spool: removed " +
                            suppressedInFile.Count.ToString(CultureInfo.InvariantCulture) +
                            " suppressed undeliverable event(s) at load");
                    }
                }

                if (_entries.Count > 0)
                {
                    Log("shardpilot spool: loaded " + _entries.Count.ToString(CultureInfo.InvariantCulture) +
                        " undelivered event(s) for re-send");
                }
            }
        }

        /// <summary>
        /// Reads version 3 framed clock/envelope records or a version 1 JSON
        /// array. Other versions follow the existing discard path.
        /// A torn final record is dropped; a malformed complete envelope
        /// discards the file. Invalid clock fields retain their envelope so
        /// the client can count and report conservative expiry by original ID.
        /// </summary>
        private static List<SpoolEntry> ParseDocument(
            string text, out bool droppedTornTail, out int fileMaxEvents, out long fileMaxBytes)
        {
            droppedTornTail = false;
            fileMaxEvents = 0;
            fileMaxBytes = 0;
            if (text != null && text.StartsWith(DocumentHeaderV3Prefix, StringComparison.Ordinal))
            {
                return ParseRecords(text, out droppedTornTail, out fileMaxEvents, out fileMaxBytes);
            }

            return ParseDocumentV1(text);
        }

        /// <summary>
        /// Parses v3 records, reporting whether a TORN TAIL was dropped.
        ///
        /// The caller needs that separately from the entry count, and the
        /// reason is the sharpest edge in this format: dropping the tail keeps
        /// the RECORDS intact, but the torn bytes are still at the end of the
        /// file. Appending onto them concatenates the new record to a partial
        /// one, and the new record's terminator turns the pair into a COMPLETE
        /// malformed line — which the next launch reads, correctly, as damage,
        /// and discards the whole backlog for. A crash that should have cost
        /// one record would cost every record instead, through the very
        /// mechanism chosen to prevent it.
        ///
        /// So a recovered tear forces the reconcile rewrite, and the file is
        /// clean before anything can extend it.
        /// </summary>
        private static List<SpoolEntry> ParseRecords(
            string text, out bool droppedTornTail, out int fileMaxEvents, out long fileMaxBytes)
        {
            droppedTornTail = false;
            fileMaxEvents = 0;
            fileMaxBytes = 0;
            var entries = new List<SpoolEntry>();
            // The header line, then the records. A header not followed by a
            // terminator is a file torn before its first record: valid, and
            // empty.
            var cursor = text.IndexOf(RecordSeparator);
            if (cursor < 0)
            {
                return entries;
            }

            // The caps the file was written under, when it states them. A file
            // written before this field existed states nothing and is read
            // under the CURRENT caps, which is what it was already doing.
            Dictionary<string, object> header;
            if (!ShardPilotJsonParser.TryParseObject(text.Substring(0, cursor), out header))
            {
                return null;
            }

            object headerValue;
            if (header.TryGetValue("max_events", out headerValue) && headerValue is long)
            {
                fileMaxEvents = (int)(long)headerValue;
            }
            if (header.TryGetValue("max_bytes", out headerValue) && headerValue is long)
            {
                fileMaxBytes = (long)headerValue;
            }
            cursor++;

            while (cursor < text.Length)
            {
                var terminator = text.IndexOf(RecordSeparator, cursor);
                if (terminator < 0)
                {
                    // The torn tail. Dropping it is the recovery this format
                    // exists to make unambiguous: a raw newline can only be a
                    // terminator, because the serializer escapes newlines
                    // inside strings, so what remains is a record that was
                    // never finished being written.
                    //
                    // REPORTED, not merely skipped: the bytes are still on
                    // disk, so the file must be rewritten before it is
                    // appended to again. See the summary above.
                    droppedTornTail = true;
                    break;
                }

                var line = text.Substring(cursor, terminator - cursor);
                cursor = terminator + 1;
                if (line.Length == 0)
                {
                    continue;
                }

                ShardPilotAnalyticsClock clock = null;
                // Two tab-delimited int64 clocks precede the untouched JSON.
                // JSON strings escape tabs, so framing never changes parser
                // depth or any byte of the original wire envelope.
                var firstTab = line.IndexOf('\t');
                var secondTab = firstTab < 0 ? -1 : line.IndexOf('\t', firstTab + 1);
                if (secondTab >= 0)
                {
                    long intake, ambiguous;
                    if (long.TryParse(line.Substring(0, firstTab), NumberStyles.Integer, CultureInfo.InvariantCulture, out intake) &&
                        long.TryParse(line.Substring(firstTab + 1, secondTab - firstTab - 1), NumberStyles.Integer, CultureInfo.InvariantCulture, out ambiguous))
                        clock = new ShardPilotAnalyticsClock(intake, ambiguous);
                    line = line.Substring(secondTab + 1);
                }
                // A quoted record carries formatted signed JSON without raw
                // delimiters. Decode storage framing, never rewrite the envelope.
                if (line.Length > 0 && line[0] == '"')
                {
                    object decoded;
                    if (!ShardPilotJsonParser.TryParse(line, out decoded) || !(decoded is string)) return null;
                    line = (string)decoded;
                }
                Dictionary<string, object> envelope;
                if (!ShardPilotJsonParser.TryParseObject(line, out envelope))
                {
                    // A COMPLETE line that does not parse is damage, not a
                    // torn write — the file is not one this class produced.
                    return null;
                }

                object idValue;
                var id = envelope.TryGetValue("event_id", out idValue) ? idValue as string : null;
                if (string.IsNullOrEmpty(id))
                {
                    return null;
                }

                entries.Add(new SpoolEntry(id, line, Encoding.UTF8.GetByteCount(line), clock));
            }

            return entries;
        }

        private static List<SpoolEntry> ParseDocumentV1(string text)
        {
            Dictionary<string, object> root;
            if (!ShardPilotJsonParser.TryParseObject(text, out root))
            {
                return null;
            }

            object versionValue;
            if (!root.TryGetValue("version", out versionValue) || !(versionValue is long) || (long)versionValue != 1)
            {
                return null;
            }

            object eventsValue;
            var events = root.TryGetValue("events", out eventsValue) ? eventsValue as List<object> : null;
            if (events == null)
            {
                return null;
            }

            var rawEnvelopes = ExtractRawEnvelopes(text);
            if (rawEnvelopes == null || rawEnvelopes.Count != events.Count)
            {
                return null;
            }

            var entries = new List<SpoolEntry>(events.Count);
            for (var i = 0; i < events.Count; i++)
            {
                var envelope = events[i] as Dictionary<string, object>;
                if (envelope == null)
                {
                    return null;
                }

                object idValue;
                var id = envelope.TryGetValue("event_id", out idValue) ? idValue as string : null;
                if (string.IsNullOrEmpty(id))
                {
                    return null;
                }

                entries.Add(new SpoolEntry(id, rawEnvelopes[i], Encoding.UTF8.GetByteCount(rawEnvelopes[i])));
            }

            return entries;
        }

        /// <summary>
        /// Splits the document's events array into its raw element substrings.
        /// Only the canonical compact layout this class writes is accepted;
        /// the caller has already validated the text as JSON, so the scanner
        /// only needs string/escape awareness and bracket depth.
        /// </summary>
        private static List<string> ExtractRawEnvelopes(string text)
        {
            if (!text.StartsWith(DocumentPrefix, StringComparison.Ordinal) ||
                !text.EndsWith(DocumentSuffix, StringComparison.Ordinal) ||
                text.Length < DocumentPrefix.Length + DocumentSuffix.Length)
            {
                return null;
            }

            var inner = text.Substring(
                DocumentPrefix.Length,
                text.Length - DocumentPrefix.Length - DocumentSuffix.Length);
            var elements = new List<string>();
            if (inner.Length == 0)
            {
                return elements;
            }

            var depth = 0;
            var inString = false;
            var escaped = false;
            var start = 0;
            for (var i = 0; i < inner.Length; i++)
            {
                var ch = inner[i];
                if (inString)
                {
                    if (escaped)
                    {
                        escaped = false;
                    }
                    else if (ch == '\\')
                    {
                        escaped = true;
                    }
                    else if (ch == '"')
                    {
                        inString = false;
                    }

                    continue;
                }

                switch (ch)
                {
                    case '"':
                        inString = true;
                        break;
                    case '{':
                    case '[':
                        depth++;
                        break;
                    case '}':
                    case ']':
                        depth--;
                        break;
                    case ',':
                        if (depth == 0)
                        {
                            elements.Add(inner.Substring(start, i - start));
                            start = i + 1;
                        }

                        break;
                }
            }

            elements.Add(inner.Substring(start));
            return elements;
        }

        /// <summary>
        /// FIFO-evicts the OLDEST entries from <paramref name="entries"/>
        /// until both caps are met, returning the evicted entries in their
        /// original (oldest-first) order — or null when nothing was evicted.
        /// Operates on a caller-owned list so cap enforcement can run on a
        /// PROSPECTIVE state that is only committed after a successful write.
        /// Oversized single envelopes never reach this loop (rejected at
        /// append), so the newest entry always survives.
        /// </summary>
        /// <summary>
        /// Works out which entries an append has to evict, WITHOUT
        /// materialising the prospective state: the caps are checked against
        /// the running committed totals, so the cost is the number of entries
        /// actually evicted rather than the size of the spool.
        ///
        /// Evicts oldest-first — the committed entries before the staged ones,
        /// which is their order in the prospective state — and reports the
        /// split, so the caller can apply exactly this eviction whether it
        /// takes the append fast path or the rewrite. Reaching the staged
        /// records at all means this one batch is larger than the whole spool;
        /// the caller rewrites in that case rather than appending bytes that
        /// are already dead.
        ///
        /// Returns the evicted entries oldest-first, or null when nothing had
        /// to go. Oversized single envelopes are rejected before they reach
        /// here, so the newest entry always survives.
        /// </summary>
        private List<SpoolEntry> PlanEvictionLocked(
            List<SpoolEntry> staged, long stagedBytes, out int fromCommitted, out int fromStaged)
        {
            fromCommitted = 0;
            fromStaged = 0;

            var count = _entries.Count + staged.Count;
            var bytes = _totalBytes + stagedBytes;
            List<SpoolEntry> evicted = null;
            while (count > 0 && (count > _maxEvents || bytes > _maxBytes))
            {
                SpoolEntry oldest;
                if (fromCommitted < _entries.Count)
                {
                    oldest = _entries[fromCommitted];
                    fromCommitted++;
                }
                else
                {
                    oldest = staged[fromStaged];
                    fromStaged++;
                }

                if (evicted == null)
                {
                    evicted = new List<SpoolEntry>();
                }

                evicted.Add(oldest);
                count--;
                bytes -= oldest.Bytes;
            }

            return evicted;
        }

        /// <summary>
        /// True when the file carries enough DEAD records — bytes of entries
        /// the committed state has already evicted — to be worth one rewrite.
        ///
        /// The trigger is the ratio, not a fixed size, so it scales with
        /// whatever the spool actually holds: a rewrite becomes due after
        /// roughly as many appends as there are live records, which is O(1)
        /// amortised and, at the 2000-event default, puts compaction two
        /// orders of magnitude below the 1% density where p99 reads. The floor
        /// keeps a nearly-empty spool from compacting over a few hundred
        /// bytes.
        /// </summary>
        private bool CompactionDueLocked(long fileBytes, long liveRecordBytes)
        {
            var liveFileBytes = _header.Length + 1 + liveRecordBytes;
            return fileBytes > liveFileBytes + liveFileBytes / 2 + CompactionFloorBytes;
        }

        /// <summary>
        /// FIFO-evicts the OLDEST entries from <paramref name="entries"/>
        /// until both caps are met, returning the evicted entries in their
        /// original (oldest-first) order — or null when nothing was evicted.
        /// Operates on a caller-owned list, for the LOAD path, which has the
        /// whole document in hand and no committed state to compare against.
        /// </summary>
        private List<SpoolEntry> EvictToCaps(List<SpoolEntry> entries)
        {
            return EvictToExplicitCaps(entries, _maxEvents, _maxBytes);
        }

        /// <summary>
        /// FIFO-evicts to the GIVEN caps rather than the configured ones. The
        /// load path needs both: the file's own caps reproduce the cut the run
        /// that wrote it had already made, and the configured caps then apply
        /// this run's limits on top.
        /// </summary>
        private static List<SpoolEntry> EvictToExplicitCaps(List<SpoolEntry> entries, int maxEvents, long maxBytes)
        {
            long total = 0;
            for (var i = 0; i < entries.Count; i++)
            {
                total += entries[i].Bytes;
            }

            List<SpoolEntry> evicted = null;
            while (entries.Count > 0 && (entries.Count > maxEvents || total > maxBytes))
            {
                if (evicted == null)
                {
                    evicted = new List<SpoolEntry>();
                }

                total -= entries[0].Bytes;
                evicted.Add(entries[0]);
                entries.RemoveAt(0);
            }

            return evicted;
        }

        /// <summary>
        /// Replaces the committed in-memory state (entries, id index, byte
        /// total) with <paramref name="entries"/>. Called only after the
        /// matching file write succeeded, so memory and disk stay in step —
        /// which also discharges any pending rewrite debt a failed Remove
        /// left behind (the full-state write just landed).
        /// </summary>
        /// <summary>
        /// Whether the file is known NOT to match the committed state — it may
        /// hold records for ids the committed state does not have. A removal
        /// cannot be confirmed from memory alone while this is true.
        ///
        /// The two debts mean the same thing about the file and differ only in
        /// who owes the write: <c>_staleFileRewriteOwed</c> is a failed removal
        /// rewrite, <c>_rewriteOwed</c> is a rewrite the writer has not
        /// performed (or whose cycle failed). Reading only the first one let a
        /// removal confirm against a file the second one describes as stale.
        /// Caller must hold <c>_lock</c>.
        /// </summary>
        private bool FileIsStaleLocked()
        {
            if (_staleFileRewriteOwed)
            {
                return true;
            }

#if !(UNITY_WEBGL && !UNITY_EDITOR)
            if (_rewriteOwed)
            {
                return true;
            }
#endif

            return false;
        }

        /// <summary>
        /// Discharges every rewrite debt after a full-state write landed. The
        /// pending records go with them: a rewrite from the committed state
        /// supersedes each one, because they are all in it already — the same
        /// rule the writer's own rewrite branch follows. Safe to clear here
        /// because the caller holds <c>_fileGate</c>, so no cycle can be
        /// mid-I/O and no append can interleave under <c>_lock</c>.
        /// </summary>
        private void ClearRewriteDebtLocked()
        {
            _staleFileRewriteOwed = false;
#if !(UNITY_WEBGL && !UNITY_EDITOR)
            _rewriteOwed = false;
            _pendingWrites.Clear();
            _pendingWriteBytes = 0;
#endif
        }

        private void CommitLocked(List<SpoolEntry> entries)
        {
            _staleFileRewriteOwed = false;
            _entries.Clear();
            _entries.AddRange(entries);
            _ids.Clear();
            _totalBytes = 0;
            _totalRecordBytes = 0;
            for (var i = 0; i < _entries.Count; i++)
            {
                _ids.Add(_entries[i].Id);
                _totalBytes += _entries[i].Bytes;
                _totalRecordBytes += _entries[i].RecordBytes + 1;
            }
        }

        /// <summary>
        /// The committed-state twin of <see cref="TryAppendLocked"/>: folds in
        /// ONLY the newly appended records and drops ONLY the evicted ones, in
        /// O(records) rather than O(spool size). Valid exactly where the append
        /// fast path is — the file already matched the committed state
        /// (<c>_fileIsAppendable</c>, which every failed write clears) — so
        /// there is no rewrite debt to discharge and nothing else changed.
        ///
        /// The evicted entries leave MEMORY here and stay on disk as dead
        /// records; compaction reclaims their bytes later.
        /// </summary>
        private void AppendCommitLocked(List<SpoolEntry> staged, long stagedBytes, int evictFromCommitted)
        {
            for (var i = 0; i < evictFromCommitted; i++)
            {
                _ids.Remove(_entries[i].Id);
                _totalBytes -= _entries[i].Bytes;
                _totalRecordBytes -= _entries[i].RecordBytes + 1;
            }

            if (evictFromCommitted > 0)
            {
                _entries.RemoveRange(0, evictFromCommitted);
            }

            for (var i = 0; i < staged.Count; i++)
            {
                _entries.Add(staged[i]);
                _ids.Add(staged[i].Id);
                _totalRecordBytes += staged[i].RecordBytes + 1;
            }

            _totalBytes += stagedBytes;
        }

        /// <summary>
        /// Materialises the prospective state — committed entries then the
        /// staged ones, oldest first. O(spool size), so it is built only on
        /// the paths that rewrite the whole file anyway.
        /// </summary>
        private List<SpoolEntry> CombineLocked(List<SpoolEntry> staged)
        {
            var combined = new List<SpoolEntry>(_entries.Count + staged.Count);
            combined.AddRange(_entries);
            combined.AddRange(staged);
            return combined;
        }

        /// <summary>
        /// Writes the given entries as the spool document, atomically: the
        /// content goes to a temp file in the SAME directory which is then
        /// moved/replaced over the previous file, so a crash mid-write can
        /// never leave a half-written spool as the live file. An empty list
        /// deletes the file instead. Returns false (after logging) when the
        /// write failed — the caller decides whether its in-memory state may
        /// commit. Never writes once the spool is disowned.
        /// </summary>
        /// <summary>
        /// Rewrites the WHOLE spool file atomically (temp file, then rename).
        /// Used for the paths that genuinely change existing content —
        /// removal after an ack, compaction, load reconciliation, undo — and
        /// for the first write of an empty file.
        ///
        /// It is NOT the append path. Appending through here is what made an
        /// overflow cost O(spool size) on the calling thread; see
        /// <see cref="TryAppendLocked"/>.
        /// </summary>
        private bool TryWriteLocked(List<SpoolEntry> entries)
        {
            if (_disowned)
            {
                return false;
            }

            if (entries.Count == 0)
            {
                // Prefer deleting an emptied spool — but VERIFY it: a settled
                // event (delivered, or permanently rejected and doomed to be
                // rejected forever) must not resurrect on the next launch
                // because a locked file silently survived. When the delete
                // cannot be confirmed, fall through and atomically write an
                // EMPTY document over the survivor instead: emptiness is what
                // correctness needs, not deletion. Only if that write fails
                // too does this return false — the caller's committed memory
                // (already without the settled entries) then filters them out
                // of every later successful rewrite.
                if (TryDeleteSpoolFile(_path))
                {
                    // Nothing on disk to append to; the next append rebuilds
                    // the document in full.
                    _fileIsAppendable = false;
                    _fileBytes = 0;
                    return true;
                }
            }

            long written;
            if (WriteDocumentBytes(entries, out written))
            {
                // The file is now a v3 document this class wrote, so the next
                // append may extend it — and it holds exactly the live
                // entries, so this rewrite IS the compaction.
                _fileIsAppendable = true;
                _fileBytes = written;
                return true;
            }

            // Whatever is on disk is not something this class may append to:
            // the rename may have landed, may not, or may have left a temp
            // file. The next write rebuilds it in full.
            _fileIsAppendable = false;
            return false;
        }

#if !(UNITY_WEBGL && !UNITY_EDITOR)
        /// <summary>
        /// Starts the writer, AFTER the load has reconciled the file so the
        /// writer never races the construction it depends on.
        /// </summary>
        private void StartWriter()
        {
            _writerThread = new Thread(WriterLoop)
            {
                IsBackground = true,
                Name = "shardpilot-spool-writer"
            };
            _writerThread.Start();
        }

        /// <summary>
        /// The first retry delay after a failed write, and the ceiling the
        /// doubling stops at. A failed write is almost never transient on the
        /// timescale of one frame — the storage is full, or the path is gone,
        /// or the device revoked access — so the first retry is soon enough to
        /// catch the ones that ARE transient, and the ceiling is what keeps a
        /// permanently unwritable device from paying for the optimism forever.
        /// </summary>
        internal const int WriteRetryMinDelayMs = 250;
        internal const int WriteRetryMaxDelayMs = 30000;

        /// <summary>
        /// The retry pacing, as a function so it can be tested without a
        /// thread: zero (nothing owed) means the next wait has no timeout at
        /// all, and a failure doubles the previous delay up to the ceiling.
        /// The doubling is computed against the ceiling rather than performed
        /// and then clamped, so it cannot overflow on the way there.
        /// </summary>
        internal static int NextWriteRetryDelayMs(int previousDelayMs)
        {
            if (previousDelayMs <= 0)
            {
                return WriteRetryMinDelayMs;
            }

            if (previousDelayMs >= WriteRetryMaxDelayMs / 2)
            {
                return WriteRetryMaxDelayMs;
            }

            return previousDelayMs * 2;
        }

        /// <summary>
        /// How many times the writer thread has come out of its wait. The one
        /// observable an idle writer has: a thread that finds nothing to do
        /// leaves no other trace, so a test that wants to assert it is ASLEEP
        /// — rather than spinning cheaply — has to count the waking. Written
        /// only by the writer thread.
        /// </summary>
        private volatile int _writerWakeups;

        internal int WriterWakeupsForTesting
        {
            get { return _writerWakeups; }
        }

        private void WriterLoop()
        {
            // Zero means "nothing is owed": the wait below then has NO
            // timeout. A signal is posted on every post and by StopWriter, so
            // a timeout here would not be pacing — it would be a wakeup that
            // finds nothing to do, four times a second, per live client, for
            // the life of the process. The stop race the old backstop was
            // written for is covered by the final cycle after the loop, not by
            // polling: StopWriter sets the flag and signals, and a record
            // posted while that was happening is drained there.
            var retryDelayMs = 0;

            while (!_writerStopping)
            {
                if (retryDelayMs == 0)
                {
                    _writeSignal.WaitOne();
                }
                else
                {
                    WaitOutRetryDelay(retryDelayMs);
                }

                _writerWakeups++;
                retryDelayMs = RunWriteCycle() == WriteCycleResult.Failed
                    ? NextWriteRetryDelayMs(retryDelayMs)
                    : 0;
            }

            // One final cycle after the stop flag, so records posted while the
            // thread was being asked to stop are not stranded.
            RunWriteCycle();
        }

        /// <summary>
        /// Waits out a retry delay, treating it as a floor between ATTEMPTS
        /// rather than as a wait that a signal cancels. A signal means more
        /// records were committed, and on storage that is currently rejecting
        /// writes, more records are not a reason to try again sooner — a busy
        /// game would otherwise turn the failure into one attempt (and one log
        /// line) per event. Nothing is lost by not attempting: the records are
        /// in committed memory, and the rewrite that eventually succeeds
        /// writes the whole of it.
        ///
        /// The stop flag DOES cut it short, so closing a client never waits
        /// out a backoff; the final cycle then makes the attempt that this
        /// wait was deferring.
        /// </summary>
        private void WaitOutRetryDelay(int delayMs)
        {
            var deadline = MonotonicMs() + delayMs;
            var remaining = delayMs;
            while (remaining > 0 && !_writerStopping)
            {
                _writeSignal.WaitOne(remaining);
                remaining = (int)(deadline - MonotonicMs());
            }
        }

        private static long MonotonicMs()
        {
            return (long)(System.Diagnostics.Stopwatch.GetTimestamp() /
                          (double)System.Diagnostics.Stopwatch.Frequency * 1000.0);
        }

        private void SignalWriter()
        {
            if (_writerThread != null)
            {
                _writeSignal.Set();
            }
        }

        /// <summary>
        /// Stops the writer, draining what it still owes. MUST be called with
        /// no lock held: the cycle takes <c>_fileGate</c> then <c>_lock</c>,
        /// so joining while holding either deadlocks against a cycle waiting
        /// for it.
        /// </summary>
        private bool StopWriter()
        {
            var thread = _writerThread;
            if (thread == null)
            {
                return true;
            }

            _writerThread = null;
            _writerStopping = true;
            _writeSignal.Set();
            try
            {
                // Bounded: a cycle is one file write, and the caller may be a
                // lifecycle callback with a deadline of its own.
                //
                // The RESULT is returned rather than discarded. A false join
                // means the old thread may still be inside a write while the
                // caller goes on to release ownership — and the successor
                // takes a DIFFERENT _fileGate, so nothing serialises the two
                // against each other. The caller cannot refuse to hand over
                // (that would strand the scope forever), but it must not do it
                // silently.
                return thread.Join(TimeSpan.FromSeconds(2));
            }
            catch
            {
                // Best-effort; a writer that cannot be joined has already had
                // its pending list cleared by the disown/wipe that got here.
                return false;
            }
        }

        /// <summary>
        /// Stops the writer for a client that is CLOSING, after draining what
        /// it owes. Ownership is NOT transferred — this spool keeps its file
        /// and its committed state; only the thread goes away.
        ///
        /// Without this a closed client kept a background thread for the life
        /// of the process, and the leak was the harmless half. If its last
        /// write failed it also kept <c>_rewriteOwed</c> set, so that
        /// unregistered predecessor went on retrying a full rewrite of a path a
        /// NEWER client may by then own — and because it had already left the
        /// lifecycle registry, the handover could no longer find it to disown.
        /// A thread nobody can reach, writing a file somebody else owns.
        /// </summary>
        public void CloseWriter()
        {
            // Drain first: closing is not a denial, and records committed
            // before it belong on disk. Both outside any lock, for the reason
            // StopWriter documents.
            FlushPendingWrites();
            if (!StopWriter())
            {
                Log("shardpilot spool: writer did not stop within the join bound on close");
            }
        }

        /// <summary>
        /// Writes what is owed, on the CALLING thread, and returns once it is
        /// down. This is what makes "persist" mean the bytes are on disk
        /// rather than that a worker has been asked — pause, quit and the
        /// close snapshot all funnel through it, and so do the publish-path
        /// appends, which are on a worker already and keep every promise the
        /// synchronous spool made.
        ///
        /// One cycle is enough: <c>_fileGate</c> serialises it with the
        /// writer, so anything posted before this call is either inside the
        /// cycle this one waits out or still in <c>_pendingWrites</c> for this
        /// one to take.
        /// </summary>
        public void FlushPendingWrites()
        {
            RunWriteCycle();
        }

        /// <summary>
        /// <see cref="FlushPendingWrites"/>, reporting whether what was owed
        /// is now on disk: false only when the cycle's write failed (the debt
        /// stays owed for the background writer's retry).
        /// </summary>
        public bool FlushPendingWritesDurably()
        {
            return RunWriteCycle() != WriteCycleResult.Failed;
        }
#endif

#if UNITY_WEBGL && !UNITY_EDITOR
        /// <summary>
        /// No-op: WebGL has no writer because it has no threads, and the spool
        /// is never constructed there at all. Present so callers and tests do
        /// not need the platform guard at every site.
        /// </summary>
        public void FlushPendingWrites()
        {
        }

        /// <summary>No writer to flush on WebGL; the append already wrote inline.</summary>
        public bool FlushPendingWritesDurably()
        {
            return true;
        }

        /// <summary>
        /// No-op: WebGL has no writer thread to stop. Present so the client's
        /// close path is one code path on both profiles rather than a
        /// platform-guarded call site.
        /// </summary>
        public void CloseWriter()
        {
        }
#endif

#if !(UNITY_WEBGL && !UNITY_EDITOR)

        /// <summary>
        /// One writer cycle: decide under <c>_lock</c>, do the I/O holding NO
        /// lock, record the outcome under <c>_lock</c> again.
        ///
        /// <c>_fileGate</c> wraps the whole thing — taken BEFORE <c>_lock</c>,
        /// the order every other file-touching path here follows — so a wipe
        /// or a removal waits out an in-flight cycle instead of running under
        /// it.
        ///
        /// Distinguishes a write, an idle owner, disownership and failure.
        /// Only failure asks the background writer for a timed retry;
        /// disownership must never confirm a durable fence. Callers that want
        /// a best-effort flush
        /// (<see cref="FlushPendingWrites"/>) ignore it — a failed cycle has
        /// already recorded the debt in <c>_rewriteOwed</c>.
        /// </summary>
        private enum WriteCycleResult { Written, Idle, Disowned, Failed }

        private WriteCycleResult RunWriteCycle()
        {
            lock (_fileGate)
            {
                List<SpoolEntry> snapshot = null;
                List<SpoolEntry> records = null;
                long appendBytes = 0;
                long epoch;

                lock (_lock)
                {
                    if (_disowned)
                    {
                        // The file is not ours any more (ownership transfer,
                        // or a consent wipe). Whatever was queued must NOT
                        // reach the disk: dropping it is half of the
                        // fail-closed guarantee, the gate above being the
                        // other half.
                        _pendingWrites.Clear();
                        _pendingWriteBytes = 0;
                        return WriteCycleResult.Disowned;
                    }

                    if (!_rewriteOwed && _pendingWrites.Count == 0)
                    {
                        return WriteCycleResult.Idle;
                    }

                    epoch = _writeEpoch;

                    if (_rewriteOwed || !_fileIsAppendable)
                    {
                        // The committed state is the truth, and a rewrite from
                        // it supersedes every pending record — they are all in
                        // it already.
                        snapshot = new List<SpoolEntry>(_entries);
                        _pendingWrites.Clear();
                        _pendingWriteBytes = 0;
                    }
                    else
                    {
                        records = new List<SpoolEntry>(_pendingWrites);
                        appendBytes = _pendingWriteBytes;
                        _pendingWrites.Clear();
                        _pendingWriteBytes = 0;
                    }
                }

                // ── No lock held. This is the whole point of the phase. ──
                var midIoHook = WriteCycleMidIoHookForTesting;
                if (midIoHook != null)
                {
                    midIoHook();
                }

                if (snapshot != null)
                {
                    long written = 0;
                    var wrote = false;
                    // An emptied spool prefers deletion — but only a CONFIRMED
                    // one; otherwise an empty document is written over the
                    // survivor, exactly as the synchronous path does.
                    var emptied = snapshot.Count == 0 && TryDeleteSpoolFile(_path);
                    if (!emptied)
                    {
                        wrote = WriteDocumentBytes(snapshot, out written);
                    }

                    lock (_lock)
                    {
                        // A debt may only be cleared by a write that covers
                        // the state the debt was incurred FOR. An append that
                        // committed while this I/O was in flight moved the
                        // epoch, and what just went to disk is a photograph of
                        // the state before it — so the file is described
                        // honestly (it IS a v3 document of that many bytes)
                        // and the debt is left standing for the next cycle.
                        //
                        // Clearing it unconditionally is a lost update, and it
                        // was invisible while Append held _fileGate: the gate
                        // made this interleaving impossible, so the bug and
                        // the latency cost were the same line. The worst shape
                        // is an empty spool — a cycle deletes the file, an
                        // append commits during the delete, and the cleared
                        // debt strands every one of those records in memory
                        // with the file reporting itself up to date.
                        //
                        // A retained debt needs no backoff and reports none:
                        // the append that moved the epoch signalled the writer
                        // under the same lock, so the next wait returns at
                        // once. Backing off here would charge a disk failure's
                        // pacing to an ordinary concurrent append.
                        var covered = _writeEpoch == epoch;
                        if (emptied)
                        {
                            _fileIsAppendable = false;
                            _fileBytes = 0;
                            if (covered)
                            {
                                _rewriteOwed = false;
                                _staleFileRewriteOwed = false;
                            }
                        }
                        else if (wrote)
                        {
                            _fileIsAppendable = true;
                            _fileBytes = written;
                            if (covered)
                            {
                                _rewriteOwed = false;
                                _staleFileRewriteOwed = false;
                            }
                        }
                        else
                        {
                            _fileIsAppendable = false;
                            _rewriteOwed = true;
                            return WriteCycleResult.Failed;
                        }

                    }
                }
                else if (records != null && records.Count > 0)
                {
                    var wrote = AppendRecordBytes(records);
                    lock (_lock)
                    {
                        if (wrote)
                        {
                            _fileBytes += appendBytes;
                        }
                        else
                        {
                            _fileIsAppendable = false;
                            _rewriteOwed = true;
                            return WriteCycleResult.Failed;
                        }
                    }
                }

                return WriteCycleResult.Written;
            }
        }
#endif

        /// <summary>
        /// The document write itself — temp file, atomic move — holding NO
        /// lock and touching no mutable state: everything it reads
        /// (<c>_path</c>, <c>_directory</c>, <c>_header</c>) is readonly, and
        /// it reports what it wrote instead of recording it.
        ///
        /// Split out from <see cref="TryWriteLocked"/> so a caller that must
        /// do this I/O without <c>_lock</c> held runs the SAME bytes-to-disk
        /// code. Two implementations of an atomic rewrite are two chances to
        /// disagree about what atomic means, and the disagreement would show
        /// up only as a corrupt spool on a customer's device.
        /// </summary>
        private bool WriteDocumentBytes(List<SpoolEntry> entries, out long written)
        {
            written = 0;
            try
            {
                RewriteForTesting?.Invoke();
                var failure = WriteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                Directory.CreateDirectory(_directory);
                var tempPath = _path + ".tmp";

                // Streamed, not built into a string first. A full spool is
                // ~900 KB, so buffering the document and then materialising it
                // put ~4 MB per rewrite on the large-object heap, and the
                // collections that bought landed on LATER appends: with the
                // rewrite done this way, sustained overflow measured p99
                // 1.687 ms and a pure-append tail to 3.3 ms, against a 0.5 ms
                // p95. The rewrite's own cost is charged to the append that
                // evicts (and the bound leaves `max` free for it); making the
                // neighbours pay for it in GC was not part of that bargain.
                written = _header.Length + 1;
                using (var stream = new FileStream(
                    tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
                using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
                {
                    writer.Write(_header);
                    writer.Write(RecordSeparator);
                    for (var i = 0; i < entries.Count; i++)
                    {
                        writer.Write(entries[i].Record);
                        writer.Write(RecordSeparator);
                        written += entries[i].RecordBytes + 1;
                    }

                    writer.Flush();
                    stream.Flush(true);
                }

                MoveOver(tempPath, _path);
                return true;
            }
            catch (Exception ex)
            {
                Log("shardpilot spool write failed: " + ex.Message);
                written = 0;
                return false;
            }
        }

        /// <summary>
        /// Appends records to the END of the spool file, without reading or
        /// rewriting what is already there. This is the overflow path, and its
        /// cost is the size of what it appends — not the size of the spool.
        ///
        /// The CALLER decides whether appending is legal — it holds
        /// <c>_fileIsAppendable</c> and the compaction decision together. This
        /// carried a second copy of the appendable check until a mutation
        /// showed it could be deleted with no test failing anywhere: it was a
        /// duplicate of a condition already evaluated one frame up, and a
        /// duplicated guard is a guard that can silently disagree with itself.
        ///
        /// Returns false when the write failed; the caller falls back to a full
        /// rewrite, which is also how a v1 spool becomes a v3 one.
        ///
        /// Durability: each record is newline-terminated and the stream is
        /// flushed before returning, so a kill leaves either a complete record
        /// or a final line with no terminator — and the load path drops
        /// exactly that line. A failed append leaves the file's existing
        /// records untouched, which is the property the temp-and-rename write
        /// above provides for the rewriting paths.
        /// </summary>
        private bool TryAppendLocked(List<SpoolEntry> records, long appendBytes)
        {
            if (records == null || records.Count == 0)
            {
                return true;
            }

            if (AppendRecordBytes(records))
            {
                _fileBytes += appendBytes;
                return true;
            }

            // A partial append may have landed. The record is
            // newline-terminated, so the load path drops an unterminated tail
            // — but this class stops appending until a full write has
            // re-established a document it knows the shape of.
            _fileIsAppendable = false;
            return false;
        }

        /// <summary>
        /// The record append itself, holding NO lock and touching no mutable
        /// state — the twin of <see cref="WriteDocumentBytes"/>, and split out
        /// for the same reason.
        /// </summary>
        private bool AppendRecordBytes(List<SpoolEntry> records)
        {
            try
            {
                var failure = WriteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                Directory.CreateDirectory(_directory);
                using (var stream = new FileStream(
                    _path, FileMode.Append, FileAccess.Write, FileShare.None))
                using (var writer = new StreamWriter(stream, new UTF8Encoding(false)))
                {
                    for (var i = 0; i < records.Count; i++)
                    {
                        writer.Write(records[i].Record);
                        writer.Write(RecordSeparator);
                    }

                    writer.Flush();

                    // Test seam only; see AppendDurabilityHookForTesting. The
                    // records are in the file at this point and the enclosing
                    // using-blocks flush them out on the way past, which is
                    // exactly the state a real fsync failure leaves.
                    var durabilityHook = AppendDurabilityHookForTesting;
                    if (durabilityHook != null)
                    {
                        durabilityHook();
                    }

                    stream.Flush(true);
                }

                return true;
            }
            catch (Exception ex)
            {
                Log("shardpilot spool append failed: " + ex.Message);
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
                // File.Replace is not supported on every platform/filesystem,
                // and the rejection can surface as an IO, access, or
                // platform-support error; fall back to delete-then-move (a
                // crash exactly between the two calls loses only the previous
                // spool generation). Anything the fallback itself throws is
                // caught and logged by the caller.
                File.Delete(path);
                File.Move(tempPath, path);
            }
        }

        private void DeleteFilesLocked()
        {
            try
            {
                File.Delete(_path);
            }
            catch
            {
                // Best-effort.
            }

            try
            {
                File.Delete(_path + ".tmp");
            }
            catch
            {
                // Best-effort.
            }
        }

        /// <summary>
        /// A short stable hash of the (workspace, app, environment) scope,
        /// used in the file name so two configurations never share a spool.
        /// Hex output only — always filesystem-safe regardless of what the
        /// scope identifiers contain.
        /// </summary>
        private static string ScopeHash(string workspaceId, string appId, string environmentId)
        {
            return HashText(workspaceId + "\n" + appId + "\n" + environmentId);
        }

        private static string HashText(string text)
        {
            using (var sha = SHA256.Create())
            {
                var digest = sha.ComputeHash(Encoding.UTF8.GetBytes(text));
                var builder = new StringBuilder(16);
                for (var i = 0; i < 8; i++)
                {
                    builder.Append(digest[i].ToString("x2", CultureInfo.InvariantCulture));
                }

                return builder.ToString();
            }
        }

        private void Log(string message)
        {
            if (_log == null)
            {
                return;
            }

            try
            {
                _log(message);
            }
            catch
            {
                // Logging is best-effort and must never change spool semantics.
            }
        }

        internal sealed class SpoolEntry
        {
            public SpoolEntry(string id, string json, int bytes, ShardPilotAnalyticsClock clock = null)
            {
                Id = id;
                Json = json;
                Bytes = bytes;
                Clock = clock;
                var stored = json;
                if (json.IndexOf(RecordSeparator) >= 0)
                {
                    var quoted = new StringBuilder();
                    ShardPilotJson.AppendQuoted(quoted, json);
                    stored = quoted.ToString();
                }
                Record = (clock == null ? "0\t0\t" :
                    clock.IntakeMs.ToString(CultureInfo.InvariantCulture) + "\t" +
                    clock.AmbiguousMs.ToString(CultureInfo.InvariantCulture) + "\t") + stored;
                RecordBytes = Encoding.UTF8.GetByteCount(Record);
            }

            internal ShardPilotAnalyticsClock Clock { get; private set; }
            internal string Record { get; private set; }
            internal int RecordBytes { get; private set; }
            public string Id { get; private set; }
            public string Json { get; private set; }
            public int Bytes { get; private set; }
        }
    }
}
