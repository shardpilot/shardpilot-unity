using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using UnityEngine;

namespace ShardPilot
{
    /// <summary>
    /// Durable on-disk spool for crash reports, so a report captured in a
    /// dying process (or one whose send failed transiently) is re-sent on the
    /// next launch instead of being lost. This is a SEPARATE spool from the
    /// analytics event spool: crash reports ship one-per-request to the
    /// dedicated crash ingest endpoint with their own JSON body shape, so
    /// they cannot ride the batch spool — but the two spools share the same
    /// proven mechanics (atomic temp+replace writes, count and byte caps,
    /// fail-closed consent wipe markers, per-scope files).
    ///
    /// The spool holds the exact serialized crash-ingest request body that
    /// was going to the wire — already PII-scrubbed and sanitized — nothing
    /// else. Auth material (keys, headers) is never written. Each payload is
    /// stored VERBATIM, so a re-send on a later launch is byte-identical to
    /// the original attempt: the stable UUIDv7 <c>crash_id</c> (stamped once
    /// at capture, never regenerated) lets the crash ingest service
    /// de-duplicate a report that was delivered but not observed as
    /// acknowledged.
    ///
    /// File format (JSON Lines, written atomically as a whole):
    /// <code>
    /// {"version":1}
    /// &lt;crash-ingest body&gt;
    /// &lt;crash-ingest body&gt;
    /// </code>
    /// one report body per line, oldest first. Unlike the event spool's
    /// single-document format, the line-per-record layout lets the loader
    /// recover the intact records of a damaged file: a corrupt or truncated
    /// LINE is dropped while every parseable line survives — crash reports
    /// are the most valuable diagnostics signal, so external damage to one
    /// record must not destroy the backlog. A file whose header line is
    /// unreadable is discarded entirely (its format cannot be trusted). No
    /// method of this class ever throws into game code.
    ///
    /// Capacity is bounded by BOTH a report count and a serialized-bytes cap;
    /// when either is exceeded the OLDEST reports are evicted first (a fresh
    /// report describes the current build's failure and is worth more than a
    /// stale one). A single report larger than the byte cap is rejected up
    /// front, without evicting anything. Caps are enforced on every append
    /// and again on load.
    /// </summary>
    internal sealed class ShardPilotCrashSpool
    {
        private const string FileNamePrefix = "shardpilot.crashspool.";
        private const string FileNameSuffix = ".jsonl";
        private const string HeaderLine = "{\"version\":1}";

        private const string WipeOwedMarkerKeyPrefix = "shardpilot.crashspool.wipe_owed.";
        private const string RetryAfterMarkerKeyPrefix = "shardpilot.crashspool.retry_after.";

        /// <summary>
        /// Upper bound accepted for a crash Retry-After delay (one day,
        /// matching the analytics publish path's clamp), applied to BOTH the
        /// persisted marker and the in-memory re-send deadline the client
        /// derives from a response header: a corrupt marker or an absurd
        /// server value must never park crash re-send effectively forever.
        /// </summary>
        internal const long MaxRetryAfterSeconds = 86400;

        /// <summary>
        /// How long a pending (undelivered) crash report may stay in the
        /// spool: reports older than this are pruned by the maintenance
        /// read every construction performs — INCLUDING while crash
        /// reporting is disabled, so an opt-out cannot park reports on disk
        /// indefinitely. Age is derived from the report's UUIDv7
        /// <c>crash_id</c> (minted once at capture, never regenerated); a
        /// record whose id cannot be dated is treated as expired — retention
        /// must be provable.
        /// </summary>
        internal static readonly TimeSpan PendingReportRetention = TimeSpan.FromDays(7);

        /// <summary>
        /// How far a record's UUIDv7 timestamp may sit in the FUTURE before
        /// the record is treated as expired. A future-dated id (a wrong
        /// device clock at mint time, or a caller-supplied CrashId) would
        /// otherwise survive until its future timestamp plus the retention
        /// window, breaking the 7-day bound. The tolerance absorbs ordinary
        /// clock corrections (an NTP sync stepping the clock back after the
        /// id was minted) without letting the bound stretch meaningfully:
        /// worst-case retention is the window plus this hour.
        /// </summary>
        internal static readonly TimeSpan MaxForwardClockSkew = TimeSpan.FromHours(1);

        /// <summary>
        /// The single retention predicate both the construction-time load
        /// and the in-session prune apply: expired when the UUIDv7 crash_id
        /// is undatable (retention must be provable), older than the
        /// retention window, or future-dated beyond the skew tolerance
        /// (an untrusted timestamp must not extend the bound).
        /// </summary>
        internal static bool IsRecordExpired(string crashId, DateTime nowUtc)
        {
            DateTime mintedUtc;
            if (!ShardPilotIds.TryGetUuidV7TimestampUtc(crashId, out mintedUtc))
            {
                return true;
            }

            return mintedUtc < nowUtc - PendingReportRetention ||
                   mintedUtc > nowUtc + MaxForwardClockSkew;
        }

        /// <summary>
        /// Clamps a server-supplied Retry-After delay to the crash plane's
        /// one-day maximum before it is turned into a deadline. Negative
        /// values clamp to zero.
        /// </summary>
        public static TimeSpan ClampRetryAfter(TimeSpan retryAfter)
        {
            if (retryAfter <= TimeSpan.Zero)
            {
                return TimeSpan.Zero;
            }

            var max = TimeSpan.FromSeconds(MaxRetryAfterSeconds);
            return retryAfter > max ? max : retryAfter;
        }

        private readonly object _lock = new object();
        private readonly List<CrashSpoolRecord> _records = new List<CrashSpoolRecord>();
        private readonly HashSet<string> _ids = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>
        /// Ids loaded from disk at construction (bounded by the report cap).
        /// Guarded by <c>_lock</c>.
        /// </summary>
        private readonly HashSet<string> _loadedIds = new HashSet<string>(StringComparer.Ordinal);

        /// <summary>
        /// Tombstones for LOADED ids that were later settled (removed): a
        /// re-append of such an id is a no-op reported as success. This is
        /// what keeps the ownership-handoff backlog forward from
        /// resurrecting a report the successor's construction-time re-send
        /// pass already delivered or permanently rejected — the
        /// predecessor's stale in-memory backlog still lists it. Bounded by
        /// the loaded set (crash ids are unique UUIDv7s, so a fresh capture
        /// can never collide with a tombstone). Guarded by <c>_lock</c>.
        /// </summary>
        private readonly HashSet<string> _settledLoadedIds = new HashSet<string>(StringComparer.Ordinal);
        private readonly string _directory;
        private readonly string _path;
        private readonly int _maxReports;
        private readonly long _maxBytes;
        private readonly IShardPilotKeyValueStore _markerStore;
        private readonly string _markerKey;
        private readonly Action<string> _log;
        private long _totalBytes;

        /// <summary>
        /// Set once when this instance loses its claim on the file: either
        /// ownership transferred to a newer client for the same scope, or a
        /// consent wipe failed and the instance is tombstoned until a
        /// relaunch retries it. Every later file mutation through this
        /// instance becomes a no-op. Guarded by <c>_lock</c>.
        /// </summary>
        private bool _disowned;

        /// <summary>
        /// Test-only: when set, the next atomic rewrites throw this instead
        /// of writing, simulating a full or unwritable disk. Cleared by the
        /// test. Never assigned in production.
        /// </summary>
        internal Exception WriteFailureForTesting;

        /// <summary>
        /// Test-only: when set, consent-wipe file deletions throw this
        /// instead of deleting, simulating an undeletable file. STATIC so it
        /// also reaches the owed-wipe retry inside <see cref="TryCreate"/>;
        /// tests clear it in a finally block. Never assigned in production.
        /// </summary>
        internal static Exception DeleteFailureForTesting;

        /// <summary>
        /// Test-only: when set, consent-sweep directory enumerations throw
        /// this, simulating an unlistable directory. Never assigned in
        /// production.
        /// </summary>
        internal static Exception EnumerationFailureForTesting;

        private ShardPilotCrashSpool(
            string directory,
            string path,
            int maxReports,
            long maxBytes,
            IShardPilotKeyValueStore markerStore,
            string markerKey,
            Action<string> log)
        {
            _directory = directory;
            _path = path;
            _maxReports = maxReports;
            _maxBytes = maxBytes;
            _markerStore = markerStore;
            _markerKey = markerKey;
            _log = log;
        }

        /// <summary>
        /// Creates the crash spool for the given (already normalized) config
        /// and loads any previously persisted reports, enforcing the caps and
        /// recovering what it can from a damaged file. Returns null when no
        /// spool directory is resolvable — the client then runs without crash
        /// durability, exactly as before this spool existed.
        ///
        /// Consent fail-closed, mirroring the event spool: an owed directory
        /// sweep or an owed file wipe recorded by a previous failed consent
        /// wipe is retried BEFORE anything is loaded; while either remains
        /// owed this returns null — the spool stays disabled (never loaded,
        /// never appended), so a pre-denial report can never be re-sent after
        /// a later grant.
        /// </summary>
        public static ShardPilotCrashSpool TryCreate(
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
                var spool = new ShardPilotCrashSpool(
                    Path.GetDirectoryName(filePath),
                    filePath,
                    config.CrashSpoolMaxReports,
                    config.CrashSpoolMaxBytes,
                    markerStore,
                    WipeOwedMarkerKeyPrefix + scopeHash,
                    log);

                if (!TrySettleOwedSweep(Path.GetDirectoryName(filePath), markerStore, log))
                {
                    SafeLog(log, "shardpilot crash spool disabled: a consent sweep is still owed for its directory");
                    return null;
                }

                if (!TrySettleOwedWipe(markerStore, WipeOwedMarkerKeyPrefix + scopeHash))
                {
                    SafeLog(log, "shardpilot crash spool disabled: a consent wipe is still owed and the file could not be removed");
                    return null;
                }

                spool.LoadFromDisk();
                return spool;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Resolves the crash spool file path a NORMALIZED config maps to —
        /// the same directory-defaulting and (workspace, app, environment)
        /// scope hashing the event spool uses, with the crash spool's own
        /// file name — WITHOUT touching file system state. Null when no
        /// directory is resolvable. Canonicalized, so the lifecycle
        /// registry's ownership comparisons and the wipe-owed markers agree
        /// on one spelling per physical file.
        /// </summary>
        public static string ResolveFilePath(ShardPilotConfig config)
        {
            try
            {
                var directory = ShardPilotSpool.ResolveDirectory(config);
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

        /// <summary>The absolute path of the crash spool file for this scope.</summary>
        public string FilePath
        {
            get { return _path; }
        }

        /// <summary>
        /// True once this instance lost its claim on the file (ownership
        /// transfer or consent tombstone) — every later mutation through it
        /// is a no-op; the capture path then hands reports to the successor
        /// client instead.
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

        internal int ReportCountForTesting
        {
            get
            {
                lock (_lock)
                {
                    return _records.Count;
                }
            }
        }

        /// <summary>
        /// Cheap emptiness probe for the re-send pass's worker-tick re-arm
        /// check — no snapshot allocation on the (frequent) nothing-pending
        /// path. Pure in-memory read under the spool lock (a leaf lock).
        /// </summary>
        internal bool HasPendingRecords
        {
            get
            {
                lock (_lock)
                {
                    return _records.Count > 0;
                }
            }
        }

        /// <summary>
        /// Permanently releases this spool's claim on its file: every later
        /// append/remove/wipe through this instance becomes a no-op. Called
        /// when a NEWER client for the same scope registers — the newest
        /// client owns the file, and a still-draining older client must not
        /// rewrite it from a stale in-memory view.
        /// </summary>
        public void Disown()
        {
            lock (_lock)
            {
                if (_disowned)
                {
                    return;
                }

                _disowned = true;
            }

            Log("shardpilot crash spool: file ownership transferred to a newer client for this scope");
        }

        /// <summary>
        /// Appends one crash report — the exact serialized ingest body that
        /// was (or would have been) sent — de-duplicated by <c>crash_id</c>:
        /// a report that is already spooled is left in place and counts as
        /// present. The payload must round-trip through the SDK's JSON
        /// parser and carry the matching <c>crash_id</c>, so a record the
        /// next launch could not read is rejected here instead of poisoning
        /// the file.
        ///
        /// Two rules shared with the event spool:
        /// - A single payload larger than the byte cap on its own is
        ///   rejected UP FRONT, without evicting anything — it can never fit.
        /// - The in-memory index commits ONLY after the file rewrite
        ///   succeeded, so a failed write never marks a report as durable.
        ///
        /// Returns true when the report is durably in the file after the
        /// call (an already-present duplicate counts); false when it was
        /// rejected, the write failed, or the spool is disowned.
        /// <paramref name="evictedCount"/> reports how many OLDEST records
        /// the cap enforcement displaced; <paramref name="undoToken"/>
        /// records what the append staged and what it evicted, so a
        /// close-raced capture can be undone WITH its displaced backlog
        /// restored (see <see cref="UndoAppend"/>). Never throws.
        /// </summary>
        public bool Append(string crashId, string payload, out int evictedCount, out AppendUndoToken undoToken)
        {
            evictedCount = 0;
            undoToken = null;
            try
            {
                crashId = ShardPilotConfig.Trim(crashId);
                if (string.IsNullOrEmpty(crashId) || string.IsNullOrEmpty(payload))
                {
                    return false;
                }

                // The file is line-framed: a payload with a raw newline would
                // split into unparseable lines on load. The SDK serializer
                // escapes all control characters, so this only rejects
                // payloads that did not come from it.
                if (payload.IndexOfAny(new[] { '\r', '\n' }) >= 0)
                {
                    Log("shardpilot crash spool: rejected report " + crashId + " (payload is not single-line)");
                    return false;
                }

                // A self-written record must be self-readable on the next
                // launch, with the id the settlement paths will remove by.
                Dictionary<string, object> parsed;
                if (!ShardPilotJsonParser.TryParseObject(payload, out parsed) ||
                    !string.Equals(ExtractCrashId(parsed), crashId, StringComparison.Ordinal))
                {
                    Log("shardpilot crash spool: rejected report " + crashId + " whose payload cannot be re-read on load");
                    return false;
                }

                var bytes = Encoding.UTF8.GetByteCount(payload);
                if (bytes > _maxBytes)
                {
                    Log("shardpilot crash spool: rejected oversized report " + crashId + " (" +
                        bytes.ToString(CultureInfo.InvariantCulture) + " bytes > cap " +
                        _maxBytes.ToString(CultureInfo.InvariantCulture) + "); existing reports kept");
                    return false;
                }

                lock (_lock)
                {
                    if (_disowned)
                    {
                        return false;
                    }

                    if (_ids.Contains(crashId))
                    {
                        // Already durable (an idempotent re-append): no churn.
                        return true;
                    }

                    if (_settledLoadedIds.Contains(crashId))
                    {
                        // A loaded report that was already settled this
                        // session (delivered or permanently rejected):
                        // re-appending it — a stale ownership-handoff
                        // forward, typically — would re-send an
                        // acknowledged report on the next launch. Success
                        // without churn.
                        return true;
                    }

                    var combined = new List<CrashSpoolRecord>(_records.Count + 1);
                    combined.AddRange(_records);
                    combined.Add(new CrashSpoolRecord(crashId, payload, bytes));

                    var evicted = EvictToCaps(combined);
                    if (!TryWriteLocked(combined))
                    {
                        return false;
                    }

                    CommitLocked(combined);
                    undoToken = new AppendUndoToken { AppendedId = crashId, EvictedRecords = evicted };
                    if (evicted != null)
                    {
                        evictedCount = evicted.Count;
                        Log("shardpilot crash spool over capacity: evicted the oldest " +
                            evicted.Count.ToString(CultureInfo.InvariantCulture) + " report(s)");
                    }

                    return true;
                }
            }
            catch (Exception ex)
            {
                Log("shardpilot crash spool append failed: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Opaque handle an <see cref="Append"/> hands back recording what
        /// it staged and what its cap enforcement evicted; consumed by
        /// <see cref="UndoAppend"/> when a close races the capture.
        /// </summary>
        public sealed class AppendUndoToken
        {
            internal string AppendedId;

            /// <summary>The OLDEST records the append displaced; null when none.</summary>
            internal List<CrashSpoolRecord> EvictedRecords;
        }

        /// <summary>
        /// Bounded memory of capture ids whose appends were UNDONE (their
        /// captures reported false): a concurrent close can race SEVERAL
        /// in-flight captures at once in a full spool, where one capture's
        /// append evicts another in-flight capture's record — a later undo
        /// must not restore a record whose own capture was already refused,
        /// in either undo order. Oldest dropped first. Guarded by
        /// <c>_lock</c>.
        /// </summary>
        private readonly List<string> _undoneCaptureIds = new List<string>();

        private const int UndoneCaptureIdCap = 64;

        /// <summary>
        /// Undoes one capture-time append that a close raced: removes
        /// EXACTLY the record that append staged and RESTORES the durable
        /// backlog its cap enforcement evicted, in original (oldest-first)
        /// order — a capture that reports false must not deliver later, and
        /// it must not have cost unrelated reports their durability either.
        /// Restores are RECONCILED against the current state: a record that
        /// is already present, was itself undone by another close-raced
        /// capture, or was settled after load is skipped — so concurrent
        /// undos converge to the same file regardless of order (allowing a
        /// bounded transient cap overshoot that the next append or load
        /// re-enforces). A null token is a strict
        /// no-op (the append staged nothing). NOT used for denial races — a
        /// denial condemns the evicted records too, so those use a plain
        /// <see cref="Remove"/> or the wipe. Returns true when the undo is
        /// confirmed on disk; false when the rewrite failed or the spool is
        /// disowned (the leftover re-sends later and the ingest service
        /// de-duplicates). Never throws.
        /// </summary>
        public bool UndoAppend(AppendUndoToken undoToken)
        {
            if (undoToken == null || string.IsNullOrEmpty(undoToken.AppendedId))
            {
                return true;
            }

            try
            {
                lock (_lock)
                {
                    if (_disowned)
                    {
                        return false;
                    }

                    // Remember the refused capture FIRST, so an undo running
                    // after this one never restores its record.
                    if (!_undoneCaptureIds.Contains(undoToken.AppendedId))
                    {
                        _undoneCaptureIds.Add(undoToken.AppendedId);
                        while (_undoneCaptureIds.Count > UndoneCaptureIdCap)
                        {
                            _undoneCaptureIds.RemoveAt(0);
                        }
                    }

                    var working = new List<CrashSpoolRecord>(
                        _records.Count + (undoToken.EvictedRecords == null ? 0 : undoToken.EvictedRecords.Count));
                    if (undoToken.EvictedRecords != null)
                    {
                        // The evicted records were the OLDEST; restoring them
                        // at the front reproduces the pre-append order.
                        // Skipped: records already present (another undo beat
                        // this one), records whose OWN captures were undone,
                        // and settled-after-load tombstones.
                        for (var i = 0; i < undoToken.EvictedRecords.Count; i++)
                        {
                            var evicted = undoToken.EvictedRecords[i];
                            if (evicted == null ||
                                _ids.Contains(evicted.Id) ||
                                _undoneCaptureIds.Contains(evicted.Id) ||
                                _settledLoadedIds.Contains(evicted.Id))
                            {
                                continue;
                            }

                            working.Add(evicted);
                        }
                    }

                    for (var i = 0; i < _records.Count; i++)
                    {
                        if (!string.Equals(_records[i].Id, undoToken.AppendedId, StringComparison.Ordinal))
                        {
                            working.Add(_records[i]);
                        }
                    }

                    // Deliberately NOT cap-enforced here: with two refused
                    // captures interleaved, the slot the restore needs may
                    // still be held by the OTHER refused record whose undo
                    // has not run yet — evicting now would lose the restored
                    // backlog for good. The transient overshoot is bounded by
                    // the number of concurrently close-raced captures and is
                    // re-enforced at the very next append and at load.

                    // Write-then-commit, like the append itself: a failed
                    // rewrite keeps the previous generation in memory AND on
                    // disk.
                    if (TryWriteLocked(working))
                    {
                        CommitLocked(working);
                        return true;
                    }

                    Log("shardpilot crash spool: close-race undo rewrite failed; the record re-sends later and is de-duplicated");
                    return false;
                }
            }
            catch (Exception ex)
            {
                Log("shardpilot crash spool undo failed: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Snapshot of the pending reports, oldest first, for the re-send
        /// pass. Records REMAIN in the spool until the crash ingest service
        /// acknowledges them (ack-based removal), so an app killed mid-resend
        /// loses nothing; a double delivery is de-duplicated server-side by
        /// the stable <c>crash_id</c>.
        /// </summary>
        public List<CrashSpoolRecord> PendingRecords()
        {
            lock (_lock)
            {
                return new List<CrashSpoolRecord>(_records);
            }
        }

        /// <summary>
        /// True while a report with this <c>crash_id</c> is still PENDING in
        /// this spool (in the committed memory backing the file's current
        /// generation). The AEI pass's end-of-pass settlement probe: a
        /// pass-appended record still present here is durable — the re-send
        /// pass covers it even if its live send never completes — while an
        /// id that a LATER append's cap enforcement evicted (or that was
        /// never appended, was removed, or was settled) reads false. Never
        /// throws.
        /// </summary>
        public bool ContainsPendingRecord(string crashId)
        {
            crashId = ShardPilotConfig.Trim(crashId);
            if (string.IsNullOrEmpty(crashId))
            {
                return false;
            }

            lock (_lock)
            {
                return _ids.Contains(crashId);
            }
        }

        /// <summary>
        /// Removes one report by <c>crash_id</c>. Called when the crash
        /// ingest service acknowledged it (2xx — including an accepted-but-
        /// suppressed report) and when it was permanently rejected (a
        /// non-retryable 4xx would fail forever on every future launch).
        /// Returns true when the removal is CONFIRMED on disk (nothing
        /// needed removing, or the rewrite/empty transition succeeded);
        /// false when the file could not be updated or the spool is
        /// disowned — settlement callers may ignore that (the committed
        /// memory filters the id out of every later successful rewrite, and
        /// a re-sent leftover is de-duplicated), but a consent-denial caller
        /// must escalate to the fail-closed wipe. Never throws.
        /// </summary>
        /// <summary>
        /// Prunes records past <see cref="PendingReportRetention"/> from
        /// memory AND the file — the same rule the construction-time load
        /// applies — so a long-lived process cannot re-send reports that
        /// EXPIRED after the load (a crash opt-out lasting past the window
        /// followed by a re-enable, say). Pruned loaded records are
        /// tombstoned like settled ones, so a stale ownership-handoff
        /// forward cannot re-append them. Memory commits before the
        /// best-effort rewrite, mirroring <see cref="Remove"/>: a failed
        /// rewrite leaves expired lines on disk for the next load's prune,
        /// but they can no longer re-send from this process. Returns the
        /// number pruned. Never throws.
        /// </summary>
        public int PruneExpiredRecords()
        {
            try
            {
                lock (_lock)
                {
                    if (_disowned || _records.Count == 0)
                    {
                        return 0;
                    }

                    var nowUtc = DateTime.UtcNow;
                    var kept = new List<CrashSpoolRecord>(_records.Count);
                    var expired = 0;
                    for (var i = 0; i < _records.Count; i++)
                    {
                        if (IsRecordExpired(_records[i].Id, nowUtc))
                        {
                            expired++;
                            if (_loadedIds.Contains(_records[i].Id))
                            {
                                _settledLoadedIds.Add(_records[i].Id);
                            }
                        }
                        else
                        {
                            kept.Add(_records[i]);
                        }
                    }

                    if (expired == 0)
                    {
                        return 0;
                    }

                    CommitLocked(kept);
                    TryWriteLocked(_records);
                    Log("shardpilot crash spool: pruned " +
                        expired.ToString(CultureInfo.InvariantCulture) +
                        " expired pending crash report(s) past the retention window");
                    return expired;
                }
            }
            catch (Exception ex)
            {
                Log("shardpilot crash spool retention prune failed: " + ex.Message);
                return 0;
            }
        }

        public bool Remove(string crashId)
        {
            if (string.IsNullOrEmpty(crashId))
            {
                return true;
            }

            try
            {
                lock (_lock)
                {
                    if (_disowned)
                    {
                        return false;
                    }

                    if (!_ids.Remove(crashId))
                    {
                        // Nothing present in the committed state; disk
                        // normally matches it, so there is nothing to confirm.
                        return true;
                    }

                    for (var i = 0; i < _records.Count; i++)
                    {
                        if (string.Equals(_records[i].Id, crashId, StringComparison.Ordinal))
                        {
                            _totalBytes -= _records[i].Bytes;
                            _records.RemoveAt(i);
                            break;
                        }
                    }

                    // A settled LOADED report is tombstoned so a stale
                    // handoff forward cannot re-append it (see Append).
                    if (_loadedIds.Contains(crashId))
                    {
                        _settledLoadedIds.Add(crashId);
                    }

                    // Memory commits BEFORE the rewrite, like the event
                    // spool's settlement removal: a failed rewrite leaves the
                    // settled report lingering on disk, and the worst case is
                    // one re-send on a later launch that the ingest service
                    // de-duplicates by crash_id — benign. The outcome is
                    // still reported truthfully for fail-closed callers.
                    return TryWriteLocked(_records);
                }
            }
            catch (Exception ex)
            {
                Log("shardpilot crash spool remove failed: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Removes ONE record from the spool file even when this instance is
        /// DISOWNED. Used exclusively for a rejected capture whose handoff
        /// was REFUSED by a succession chain that ends WITHOUT a crash spool
        /// (a no-crash-key reinitialization): the file then has no live
        /// owner at all — the no-key successor never loads, rewrites, or
        /// prunes it — yet a later keyed initialization of this scope would
        /// load and send a record whose CaptureCrash reported false. The
        /// frozen in-memory view is the last committed state of the file,
        /// and rewriting it is safe precisely because nothing else owns the
        /// path; the caller (see
        /// ShardPilotClient.RemoveRejectedCaptureFromOrphanedSpoolFile)
        /// verifies that topology before calling. Never throws.
        /// </summary>
        internal void RemoveRefusedCaptureRecord(string crashId)
        {
            if (string.IsNullOrEmpty(crashId))
            {
                return;
            }

            try
            {
                lock (_lock)
                {
                    if (!_ids.Remove(crashId))
                    {
                        return;
                    }

                    for (var i = 0; i < _records.Count; i++)
                    {
                        if (string.Equals(_records[i].Id, crashId, StringComparison.Ordinal))
                        {
                            _totalBytes -= _records[i].Bytes;
                            _records.RemoveAt(i);
                            break;
                        }
                    }

                    if (_loadedIds.Contains(crashId))
                    {
                        _settledLoadedIds.Add(crashId);
                    }

                    if (!TryWriteIgnoringDisownLocked(_records))
                    {
                        // FAIL CLOSED: this rewrite is the only thing keeping
                        // the refused record out of a later keyed launch's
                        // load — nothing owns the orphaned file, so no other
                        // path will ever settle or prune it. The owed-wipe
                        // marker makes the next construction destroy the file
                        // BEFORE any load; losing the enabled-period backlog
                        // with it is the same fail-closed price the denial
                        // wipe's escalation pays.
                        SetWipeOwed(_markerStore, _markerKey, _path);
                        Log("shardpilot crash spool: refused-capture rewrite failed; " +
                            "wipe owed — the orphaned file is destroyed before the next load");
                    }
                }
            }
            catch (Exception ex)
            {
                Log("shardpilot crash spool refused-capture removal failed: " + ex.Message);
                try
                {
                    SetWipeOwed(_markerStore, _markerKey, _path);
                }
                catch
                {
                    // Marker persistence is itself best-effort here; the
                    // in-memory removal above still blocks this process.
                }
            }
        }

        /// <summary>
        /// Destroys the crash spool: clears every record and deletes the
        /// file. Used when analytics consent is denied — a crash report
        /// carries the same consent subject (anonymous_id, breadcrumbs) as
        /// the events, so previously undeliverable reports must never be
        /// re-sent on a later launch. FAIL-CLOSED, mirroring the event
        /// spool: when the file cannot be durably removed, a per-scope
        /// "wipe owed" marker is persisted and this instance is tombstoned;
        /// the next construction retries the wipe BEFORE any load and keeps
        /// the spool disabled until it succeeds. Never throws.
        /// </summary>
        public void Wipe()
        {
            try
            {
                lock (_lock)
                {
                    _records.Clear();
                    _ids.Clear();
                    _loadedIds.Clear();
                    _settledLoadedIds.Clear();
                    _totalBytes = 0;
                    if (_disowned)
                    {
                        // In-memory state is cleared, but the FILE belongs to
                        // a newer client for this scope now; leave it alone.
                        return;
                    }

                    if (TryDeleteSpoolFile(_path))
                    {
                        ClearWipeOwedIfSatisfied(_markerStore, _markerKey, _path);
                    }
                    else
                    {
                        SetWipeOwed(_markerStore, _markerKey, _path);
                        _disowned = true;
                        Log("shardpilot crash spool: consent wipe could not remove the file; " +
                            "wipe owed — the spool is disabled until a later launch removes it");
                    }
                }
            }
            catch
            {
                // Best-effort: the in-memory records are cleared; the owed
                // marker (set above where reachable) makes the next launch
                // retry fail-closed.
            }
        }

        /// <summary>
        /// Fail-closed consent wipe of EVERY crash spool scope in the
        /// directory the config resolves to, mirroring the event spool's
        /// directory sweep: the persisted consent decision is GLOBAL while
        /// spool files are per-scope. Only files matching the crash spool's
        /// exact canonical filename shape are touched; each file that cannot
        /// be confirmed deleted gets a path-scoped "wipe owed" marker, and a
        /// sweep that cannot even enumerate the directory records a
        /// directory-scoped "sweep owed" marker — both honored fail-closed
        /// before any later load. Never throws.
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
                var directory = ShardPilotSpool.ResolveDirectory(config);
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
                var directory = ShardPilotSpool.ResolveDirectory(config);
                if (string.IsNullOrEmpty(directory) || store == null)
                {
                    return false;
                }

                return ShardPilotDirectorySweep.Owed(store, ShardPilotDirectorySweep.CrashKey, directory);
            }
            catch
            {
                return false;
            }
        }

        internal static bool RetryOwedPurge(ShardPilotConfig config, IShardPilotKeyValueStore store,
            Action<string> log, bool sweepOwed)
        {
            var directory = ShardPilotSpool.ResolveDirectory(config);
            var swept = string.IsNullOrEmpty(directory) || ShardPilotDirectorySweep.Retry(store,
                ShardPilotDirectorySweep.CrashKey, directory, owed => SweepDirectoryFiles(owed, store, log), sweepOwed);
            // A denied append can leave a file marker after the directory sweep.
            return swept && TrySettleOwedWipe(store, WipeOwedMarkerKeyPrefix +
                ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey));
        }

        // ------------------------------------------------------------------
        // Persisted Retry-After deadline
        // ------------------------------------------------------------------

        /// <summary>
        /// Persists the absolute UTC deadline before which the crash re-send
        /// pass must not run, recorded when the crash ingest service answered
        /// a send with 429/Retry-After. Stored per scope in the same
        /// key-value store as the consent markers so the backpressure request
        /// survives a relaunch (the in-memory analytics backoff does not).
        /// Clamped to at most one day ahead. Best-effort; never throws.
        /// </summary>
        public static void WriteRetryAfterDeadline(
            IShardPilotKeyValueStore store, ShardPilotConfig config, DateTime deadlineUtc)
        {
            if (store == null)
            {
                return;
            }

            try
            {
                var max = DateTime.UtcNow.AddSeconds(MaxRetryAfterSeconds);
                if (deadlineUtc > max)
                {
                    deadlineUtc = max;
                }

                var seconds = (long)(deadlineUtc - UnixEpoch).TotalSeconds;
                store.Set(RetryAfterMarkerKey(config), seconds.ToString(CultureInfo.InvariantCulture));
            }
            catch
            {
                // Best-effort: without the marker the next launch simply
                // retries immediately and honors the fresh Retry-After the
                // server answers with.
            }
        }

        /// <summary>
        /// Reads the persisted Retry-After deadline, or
        /// <see cref="DateTime.MinValue"/> when none is set, it already
        /// elapsed, or the value is unreadable/absurd (more than a day
        /// ahead — a corrupt marker must not disable re-send). An invalid
        /// or spent marker is CLEARED on classification (best-effort): an
        /// absurd future timestamp left in place would eventually drift
        /// into the accepted window and gate re-send for up to a day on
        /// some later launch. Never throws.
        /// </summary>
        public static DateTime ReadRetryAfterDeadline(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            if (store == null)
            {
                return DateTime.MinValue;
            }

            try
            {
                var raw = ShardPilotConfig.Trim(store.Get(RetryAfterMarkerKey(config)));
                if (string.IsNullOrEmpty(raw))
                {
                    return DateTime.MinValue;
                }

                long seconds;
                if (!long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out seconds))
                {
                    ClearRetryAfterDeadline(store, config);
                    return DateTime.MinValue;
                }

                var deadline = UnixEpoch.AddSeconds(seconds);
                var now = DateTime.UtcNow;
                if (deadline <= now || deadline > now.AddSeconds(MaxRetryAfterSeconds))
                {
                    ClearRetryAfterDeadline(store, config);
                    return DateTime.MinValue;
                }

                return deadline;
            }
            catch
            {
                return DateTime.MinValue;
            }
        }

        /// <summary>Clears the persisted Retry-After deadline. Best-effort; never throws.</summary>
        public static void ClearRetryAfterDeadline(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            if (store == null)
            {
                return;
            }

            try
            {
                if (!string.IsNullOrEmpty(ShardPilotConfig.Trim(store.Get(RetryAfterMarkerKey(config)))))
                {
                    store.Set(RetryAfterMarkerKey(config), string.Empty);
                }
            }
            catch
            {
                // Best-effort; a stale marker self-expires by comparison.
            }
        }

        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private static string RetryAfterMarkerKey(ShardPilotConfig config)
        {
            return RetryAfterMarkerKeyPrefix +
                   ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey);
        }

        /// <summary>
        /// Clears the persisted Retry-After deadline of a scope encountered
        /// by a consent sweep (keyed by the scope hash embedded in its file
        /// name, since the sweep has no config for sibling scopes).
        /// Best-effort; never throws.
        /// </summary>
        private static void ClearRetryAfterMarkerForScope(IShardPilotKeyValueStore store, string scopeHash)
        {
            if (store == null)
            {
                return;
            }

            try
            {
                var key = RetryAfterMarkerKeyPrefix + scopeHash;
                if (!string.IsNullOrEmpty(ShardPilotConfig.Trim(store.Get(key))))
                {
                    store.Set(key, string.Empty);
                }
            }
            catch
            {
                // Best-effort; a stale marker self-expires by comparison.
            }
        }

        // ------------------------------------------------------------------
        // Marker keys exposed for tests
        // ------------------------------------------------------------------

        internal static string WipeOwedMarkerKeyForTesting(ShardPilotConfig config)
        {
            return WipeOwedMarkerKeyPrefix +
                   ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey);
        }

        internal static string RetryAfterMarkerKeyForTesting(ShardPilotConfig config)
        {
            return RetryAfterMarkerKey(config);
        }

        // ------------------------------------------------------------------
        // Load + corruption recovery
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
                        // A process that died between the temp write and the
                        // atomic move — on the FIRST write for this scope —
                        // leaves the whole document durably in `<file>.tmp`
                        // with no live file. That report was written exactly
                        // so this launch could re-send it: promote the temp
                        // to the live file before concluding nothing is
                        // pending. (When a live file EXISTS, it is the
                        // committed generation and a leftover temp is
                        // discarded as usual — never promoted over it.)
                        TryPromoteOrphanTempLocked();
                        if (!File.Exists(_path))
                        {
                            return;
                        }
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

                var lines = text.Split('\n');
                if (lines.Length == 0 ||
                    !string.Equals(TrimLine(lines[0]), HeaderLine, StringComparison.Ordinal))
                {
                    // The header is the format contract; without it the file
                    // cannot be trusted at all.
                    Log("shardpilot crash spool: discarded a spool file with an unreadable header");
                    DeleteFilesLocked();
                    return;
                }

                var loaded = new List<CrashSpoolRecord>();
                var seen = new HashSet<string>(StringComparer.Ordinal);
                var dropped = 0;
                for (var i = 1; i < lines.Length; i++)
                {
                    var line = TrimLine(lines[i]);
                    if (line.Length == 0)
                    {
                        continue;
                    }

                    // Per-line recovery: a damaged or truncated line is
                    // dropped, every intact line survives — one corrupt
                    // record must not destroy the crash backlog.
                    Dictionary<string, object> parsed;
                    string id;
                    if (!ShardPilotJsonParser.TryParseObject(line, out parsed) ||
                        string.IsNullOrEmpty(id = ExtractCrashId(parsed)))
                    {
                        dropped++;
                        continue;
                    }

                    if (seen.Add(id))
                    {
                        loaded.Add(new CrashSpoolRecord(id, line, Encoding.UTF8.GetByteCount(line)));
                    }
                }

                // Retention window (the maintenance prune): a report that
                // has waited longer than PendingReportRetention for delivery
                // is dropped here — on EVERY construction, crash reporting
                // enabled or not, so a disabled client still enforces the
                // bound and an opt-out never parks reports on disk
                // indefinitely. The age comes from the UUIDv7 crash_id; an
                // undatable OR future-dated id is expired by definition
                // (fail closed on retention — see IsRecordExpired). The
                // reconcile write below rewrites the file whenever anything
                // was dropped.
                var expired = 0;
                var nowUtc = DateTime.UtcNow;
                for (var i = loaded.Count - 1; i >= 0; i--)
                {
                    if (IsRecordExpired(loaded[i].Id, nowUtc))
                    {
                        // Tombstoned like a settled loaded record: a
                        // displaced predecessor forwarding its still-in-
                        // memory backlog must not re-append an id this load
                        // already expired (see Append's tombstone check).
                        _settledLoadedIds.Add(loaded[i].Id);
                        loaded.RemoveAt(i);
                        expired++;
                    }
                }

                if (expired > 0)
                {
                    Log("shardpilot crash spool: pruned " +
                        expired.ToString(CultureInfo.InvariantCulture) +
                        " expired pending crash report(s) past the retention window");
                }

                var evicted = EvictToCaps(loaded);
                if (evicted != null)
                {
                    // Tombstoned like the expired records above: a displaced
                    // predecessor's backlog forward must not re-append what
                    // this load's cap enforcement just removed (re-appending
                    // at the tail could evict NEWER kept reports).
                    for (var i = 0; i < evicted.Count; i++)
                    {
                        _settledLoadedIds.Add(evicted[i].Id);
                    }

                    Log("shardpilot crash spool over capacity on load: evicted the oldest " +
                        evicted.Count.ToString(CultureInfo.InvariantCulture) + " report(s)");
                }

                CommitLocked(loaded);

                // Remember what THIS launch loaded: a loaded report that is
                // later settled gets a tombstone (see Remove/Append), which
                // keeps a stale ownership-handoff forward from re-appending
                // an already-settled report.
                _loadedIds.Clear();
                for (var i = 0; i < _records.Count; i++)
                {
                    _loadedIds.Add(_records[i].Id);
                }

                if (dropped > 0)
                {
                    Log("shardpilot crash spool: dropped " +
                        dropped.ToString(CultureInfo.InvariantCulture) +
                        " damaged record(s); " +
                        _records.Count.ToString(CultureInfo.InvariantCulture) + " intact record(s) recovered");
                }

                // Every reduction — damage drops, de-duplication, cap
                // eviction — makes the kept count smaller than the parsed
                // line count; reconcile the file once when anything changed.
                // Memory is a subset of the file here, so a failed reconcile
                // only leaves extra lines on disk for the next load to trim.
                if (_records.Count != CountPayloadLines(lines))
                {
                    TryWriteLocked(_records);
                }

                if (_records.Count > 0)
                {
                    Log("shardpilot crash spool: loaded " +
                        _records.Count.ToString(CultureInfo.InvariantCulture) +
                        " undelivered crash report(s) for re-send");
                }
            }
        }

        /// <summary>
        /// Promotes an orphaned temp file (temp present, live file absent —
        /// a death between the temp write and the atomic move) to the live
        /// path so its records load normally. Only a temp whose header line
        /// is intact is promoted — the write starts with the header, so a
        /// torn temp missing it holds nothing recoverable and is deleted;
        /// a torn TAIL is exactly what the per-line recovery handles.
        /// Caller holds <c>_lock</c>. Never throws.
        /// </summary>
        private void TryPromoteOrphanTempLocked()
        {
            var tempPath = _path + ".tmp";
            try
            {
                if (!File.Exists(tempPath))
                {
                    return;
                }

                var text = File.ReadAllText(tempPath);
                if (!string.Equals(TrimLine(text.Split('\n')[0]), HeaderLine, StringComparison.Ordinal))
                {
                    File.Delete(tempPath);
                    return;
                }

                File.Move(tempPath, _path);
                Log("shardpilot crash spool: recovered an orphaned temp file from an interrupted write");
            }
            catch
            {
                // Best-effort: an unpromotable temp is left for the next
                // write to overwrite (or the consent sweep to remove).
            }
        }

        /// <summary>Non-empty lines after the header — parseable or not.</summary>
        private static int CountPayloadLines(string[] lines)
        {
            var count = 0;
            for (var i = 1; i < lines.Length; i++)
            {
                if (TrimLine(lines[i]).Length > 0)
                {
                    count++;
                }
            }

            return count;
        }

        private static string TrimLine(string line)
        {
            return line == null ? string.Empty : line.Trim('\r', ' ', '\t');
        }

        private static string ExtractCrashId(Dictionary<string, object> parsed)
        {
            object value;
            var id = parsed.TryGetValue("crash_id", out value) ? value as string : null;
            return string.IsNullOrEmpty(id) ? null : id;
        }

        // ------------------------------------------------------------------
        // Caps, commit, atomic write
        // ------------------------------------------------------------------

        private List<CrashSpoolRecord> EvictToCaps(List<CrashSpoolRecord> records)
        {
            long total = 0;
            for (var i = 0; i < records.Count; i++)
            {
                total += records[i].Bytes;
            }

            List<CrashSpoolRecord> evicted = null;
            while (records.Count > 0 && (records.Count > _maxReports || total > _maxBytes))
            {
                if (evicted == null)
                {
                    evicted = new List<CrashSpoolRecord>();
                }

                total -= records[0].Bytes;
                evicted.Add(records[0]);
                records.RemoveAt(0);
            }

            return evicted;
        }

        private void CommitLocked(List<CrashSpoolRecord> records)
        {
            _records.Clear();
            _records.AddRange(records);
            _ids.Clear();
            _totalBytes = 0;
            for (var i = 0; i < _records.Count; i++)
            {
                _ids.Add(_records[i].Id);
                _totalBytes += _records[i].Bytes;
            }
        }

        /// <summary>
        /// Writes the records as the spool document, atomically: the content
        /// goes to a temp file in the SAME directory which is then
        /// moved/replaced over the previous file, so a crash mid-write can
        /// never leave a half-written spool as the live file. An empty list
        /// deletes the file (verified; falls back to writing an empty
        /// document over an undeletable survivor). Returns false (after
        /// logging) when the write failed. Never writes once disowned.
        /// </summary>
        private bool TryWriteLocked(List<CrashSpoolRecord> records)
        {
            if (_disowned)
            {
                return false;
            }

            return TryWriteIgnoringDisownLocked(records);
        }

        /// <summary>
        /// The write body without the disown guard — for the ONE caller that
        /// legitimately rewrites a disowned instance's file
        /// (<see cref="RemoveRefusedCaptureRecord"/>, whose topology check
        /// guarantees no other instance owns the path). Every ordinary
        /// rewrite goes through <see cref="TryWriteLocked"/>.
        /// </summary>
        private bool TryWriteIgnoringDisownLocked(List<CrashSpoolRecord> records)
        {
            if (records.Count == 0)
            {
                // Prefer deleting an emptied spool — but VERIFY it: a settled
                // report must not resurrect on the next launch because a
                // locked file silently survived. When the delete cannot be
                // confirmed, fall through and atomically write a header-only
                // document over the survivor instead: emptiness is what
                // correctness needs, not deletion.
                if (TryDeleteSpoolFile(_path))
                {
                    return true;
                }
            }

            var builder = new StringBuilder();
            builder.Append(HeaderLine);
            for (var i = 0; i < records.Count; i++)
            {
                builder.Append('\n');
                builder.Append(records[i].Payload);
            }

            builder.Append('\n');

            try
            {
                var failure = WriteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                Directory.CreateDirectory(_directory);
                var tempPath = _path + ".tmp";
                File.WriteAllText(tempPath, builder.ToString());
                MoveOver(tempPath, _path);
                return true;
            }
            catch (Exception ex)
            {
                Log("shardpilot crash spool write failed: " + ex.Message);
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
                // File.Replace is not supported on every platform/filesystem;
                // fall back to delete-then-move (a crash exactly between the
                // two calls loses only the previous spool generation).
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

        // ------------------------------------------------------------------
        // Fail-closed wipe/sweep markers (mirrors the event spool)
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
                // An unreadable store is a separate failure domain; fail open
                // on the STORE — the wipe itself already ran best-effort when
                // the marker was set. Mirrors the event spool.
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

        private static bool TrySettleOwedSweep(string directory, IShardPilotKeyValueStore store, Action<string> log) =>
            ShardPilotDirectorySweep.Retry(store, ShardPilotDirectorySweep.CrashKey, directory,
                owed => SweepDirectoryFiles(owed, store, log));

        private static bool SweepDirectory(string directory, IShardPilotKeyValueStore markerStore, Action<string> log) =>
            ShardPilotDirectorySweep.Sweep(markerStore, ShardPilotDirectorySweep.CrashKey, directory,
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
                                // Not the canonical crash spool filename
                                // shape: never touch a foreign file.
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
                                SafeLog(log, "shardpilot crash spool: consent wipe could not remove a scope's file; " +
                                             "wipe owed — retried before any load on a later launch");
                            }

                            // The scope's persisted Retry-After marker goes
                            // with its wiped backlog: a stale backpressure
                            // deadline must not gate a post-re-grant client's
                            // fresh reports. (Backpressure is transient server
                            // state — clearing early merely lets the server
                            // re-issue it.)
                            ClearRetryAfterMarkerForScope(markerStore, scopeHash);
                        }
                    }

                    // ORPHANED temp files: a process that died between the
                    // temp write and the atomic move leaves the crash payload
                    // in `<file>.tmp` with NO live file — the glob above
                    // never sees it (TryDeleteSpoolFile removes the temp
                    // SIBLING of each live file, but an orphan has no live
                    // file to ride along with). A consent denial must reach
                    // those bytes too. Only temp names whose BASE is the
                    // exact canonical spool shape are touched.
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

                            if (!TryDeleteFileConfirmed(tempFiles[i]))
                            {
                                succeeded = false;

                                // Fail closed like the live files. The owed
                                // marker records the temp path itself —
                                // UNLESS the scope's live file also survived
                                // this sweep: the live-file marker set above
                                // must not be overwritten (one key per
                                // scope), and its owed-wipe retry removes the
                                // temp sibling along with the live file.
                                var livePath = tempFiles[i].Substring(0, tempFiles[i].Length - 4);
                                if (ConfirmedAbsent(livePath))
                                {
                                    SetWipeOwed(markerStore, WipeOwedMarkerKeyPrefix + scopeHash, tempFiles[i]);
                                }

                                SafeLog(log, "shardpilot crash spool: consent wipe could not remove an orphaned temp file; " +
                                             "wipe owed — retried before any load on a later launch");
                            }

                            ClearRetryAfterMarkerForScope(markerStore, scopeHash);
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
        /// Extracts the scope hash from a crash spool file name, or null when
        /// the name is not EXACTLY the canonical
        /// <c>shardpilot.crashspool.&lt;16 lowercase hex&gt;.jsonl</c> shape —
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
                // Clear ONLY a marker this successful wipe actually
                // satisfies: one recording our own file (case-insensitive,
                // matching the ownership comparisons), or one whose recorded
                // file is already gone. A marker recording a DIFFERENT,
                // still existing file must survive for the launch that can
                // remove it.
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
        /// Deletes one crash spool file plus its temp sibling and reports
        /// whether BOTH are CONFIRMED gone. Confirmation is strict: a Delete
        /// call that completes without throwing is itself confirmation
        /// (removed, or nothing to remove); after a THROWING delete, only a
        /// non-throwing, negative existence probe confirms. Unlike the event
        /// spool — where the temp sibling is auxiliary — the crash spool's
        /// temp file is LOAD-BEARING: a live-absent temp is promoted back to
        /// the spool at the next load (an interrupted first write is
        /// recoverable crash data), so a consent wipe that leaves the temp
        /// behind would let a condemned report resurrect. A surviving temp
        /// therefore fails the wipe, keeping the owed marker armed until a
        /// launch removes it for good.
        /// </summary>
        private static bool TryDeleteSpoolFile(string path)
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

        /// <summary>
        /// Deletes ONE exact file (no temp-sibling logic — used for the
        /// orphaned temp files themselves) with the same strict confirmation
        /// as <see cref="TryDeleteSpoolFile"/>: a non-throwing Delete
        /// confirms; after a throwing one, only a non-throwing negative
        /// existence probe does.
        /// </summary>
        private static bool TryDeleteFileConfirmed(string path)
        {
            try
            {
                var failure = DeleteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                File.Delete(path);
                return true;
            }
            catch
            {
                return ConfirmedAbsent(path);
            }
        }

        // ------------------------------------------------------------------
        // Scope hashing + logging
        // ------------------------------------------------------------------

        private static string ScopeHash(string workspaceId, string appId, string environmentId)
        {
            return HashText(workspaceId + "\n" + appId + "\n" + environmentId);
        }

        /// <summary>
        /// The 16-hex (workspace, app, environment) scope hash this spool's
        /// file names and marker keys are derived from, for the OTHER
        /// crash-plane persistence that must scope the same way (the
        /// ANR cursor and era-snapshot store): one hashing rule
        /// per scope, so every crash-plane artifact of a scope shares one
        /// spelling.
        /// </summary>
        internal static string ScopeHashFor(ShardPilotConfig config)
        {
            return ScopeHash(config.WorkspaceKey, config.AppKey, config.EnvironmentKey);
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
            SafeLog(_log, message);
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
                // Logging is best-effort and must never change spool semantics.
            }
        }

        internal sealed class CrashSpoolRecord
        {
            public CrashSpoolRecord(string id, string payload, int bytes)
            {
                Id = id;
                Payload = payload;
                Bytes = bytes;
            }

            public string Id { get; private set; }
            public string Payload { get; private set; }
            public int Bytes { get; private set; }
        }
    }
}
