using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace ShardPilot
{
    /// <summary>
    /// One historical process-exit record, as read from Android's
    /// <c>ApplicationExitInfo</c> (API 30+). This is the pure-C# projection
    /// the ANR lane operates on; the JNI-backed production source and the
    /// test fakes both produce it, so every rule downstream of the read —
    /// cursor comparison, reason policy, key derivation, degraded paths —
    /// is exercised headlessly.
    /// </summary>
    internal sealed class ShardPilotAeiExitRecord
    {
        /// <summary>ApplicationExitInfo.getTimestamp() — unix epoch ms.</summary>
        public long TimestampMs;

        /// <summary>ApplicationExitInfo.getReason().</summary>
        public int Reason;

        /// <summary>ApplicationExitInfo.getPid().</summary>
        public int Pid;

        /// <summary>ApplicationExitInfo.getDefiningUid().</summary>
        public int DefiningUid;

        /// <summary>
        /// Lazy access to <c>ApplicationExitInfo.getDescription()</c> — the
        /// platform's free-text exit description. Invoked ONLY on the
        /// reporting path, for a record that actually reports: enumeration,
        /// cursor comparison, settle-only handling, and the opt-out/denial
        /// suppression touch the IDENTITY fields above exclusively, so a
        /// suppressed launch never pulls exit-description free text into
        /// managed memory. A null delegate, a null return, or a throw all
        /// read as "no description" (the report's reason is then empty).
        /// </summary>
        public Func<string> DescriptionReader;

        /// <summary>
        /// Lazy, bounded access to the record's trace stream
        /// (<c>getTraceInputStream()</c>), invoked with the byte cap and
        /// returning the decoded text or null. BOTH missing-stream shapes the
        /// degraded rule names collapse onto the same two
        /// observable states here: a record whose platform never exposes a
        /// stream carries a NULL delegate (the API-30 shape), and an API-31+
        /// record whose bounded global buffer was overwritten carries a
        /// delegate that RETURNS null. Either way the reporter emits the same
        /// metadata-only degraded report — never a drop, never a parse
        /// failure.
        /// </summary>
        public Func<int, string> TraceTextReader;

        /// <summary>
        /// Lazy, bounded access to the record's BINARY trace stream — the
        /// raw AOSP <c>Tombstone</c> protobuf a <c>REASON_CRASH_NATIVE</c>
        /// record exposes on API 31+ (the server parses it).
        /// Invoked with the byte cap ONLY on the reporting path of a native
        /// record. The missing-stream shapes mirror the text reader's: a
        /// NULL delegate is the API-30 no-stream shape, and a NULL return is
        /// the API-31+ overwritten-buffer shape (or any read failure) —
        /// both degrade to the metadata-only JSON report. A stream LARGER
        /// than the cap returns an <see cref="ShardPilotAeiTombstoneRead.OverCap"/>
        /// result WITHOUT the bytes: an over-cap tombstone is skipped, never
        /// truncated — a truncated protobuf is unparseable by construction,
        /// so partial bytes must never reach the wire.
        /// </summary>
        public Func<int, ShardPilotAeiTombstoneRead> TraceBytesReader;
    }

    /// <summary>
    /// The outcome of one bounded tombstone-stream read. Exactly one of the
    /// shapes holds: <see cref="Bytes"/> non-empty (a complete raw tombstone
    /// within the cap — ship it UNMODIFIED), <see cref="OverCap"/> true (the
    /// stream exceeds the server's payload cap — skip the upload and report
    /// degraded; the reader never materializes the over-cap bytes into
    /// managed memory), or neither (treated as a missing stream). The
    /// reporter re-asserts the cap on whatever a reader returns, so an
    /// ill-behaved reader can never push over-cap or truncated bytes to the
    /// wire.
    /// </summary>
    internal sealed class ShardPilotAeiTombstoneRead
    {
        /// <summary>The complete raw tombstone protobuf bytes, or null.</summary>
        public byte[] Bytes;

        /// <summary>True when the stream exceeded the byte cap.</summary>
        public bool OverCap;
    }

    /// <summary>
    /// The seam between the ANR lane and the Android SDK surface: production
    /// wires the <c>AndroidJavaObject</c>-backed implementation (compiled
    /// only into Android players), the headless harness wires fakes. Reads
    /// are bounded and must never throw — a broken JNI surface returns an
    /// empty list.
    /// </summary>
    internal interface IShardPilotAeiSource
    {
        /// <summary>
        /// EVERY historical exit record of the OWN package the platform
        /// still holds (the OS's own bounded ApplicationExitInfo ring is
        /// the size limit). Deliberately uncapped: the reporter must see
        /// the FULL set to keep its cursor no-skip — its per-pass
        /// processing cap applies AFTER enumeration, never at the read (a
        /// capped read returns only the NEWEST records, and settling the
        /// cursor over that window would strand every older unseen record
        /// below the high-water mark forever). Enumeration must stay
        /// cheap: identity fields plus LAZY description/trace providers —
        /// no free text and no stream is materialized by the read itself.
        /// Empty on any failure.
        /// </summary>
        List<ShardPilotAeiExitRecord> ReadExitRecords();
    }

    /// <summary>
    /// The reason policy: which <c>ApplicationExitInfo</c> reasons this SDK
    /// build reports, and as what event kind — each reason under its OWN
    /// static-config flag (the per-feature flag
    /// discipline): <c>REASON_ANR</c> → kind <c>anr</c> behind
    /// <see cref="ShardPilotConfig.AnrExitInfoEnabled"/>, and
    /// <c>REASON_CRASH_NATIVE</c> → the base <c>crash</c> kind behind
    /// <see cref="ShardPilotConfig.NativeCrashExitInfoEnabled"/> (the tombstone
    /// lane; no new kind flag server-side, native crash is
    /// the always-admitted base kind). The cursor, key derivation, era
    /// attribution, and degraded rules stay reason-agnostic. Records whose
    /// reason has no ENABLED mapping are SETTLED (the cursor advances over
    /// them) without reporting, per the replay rule: a reason
    /// armed later reports NEW records only, never a back-catalog.
    /// </summary>
    internal static class ShardPilotAeiReasons
    {
        /// <summary>android.app.ApplicationExitInfo.REASON_CRASH_NATIVE.</summary>
        public const int ReasonCrashNative = 5;

        /// <summary>android.app.ApplicationExitInfo.REASON_ANR.</summary>
        public const int ReasonAnr = 6;

        /// <summary>
        /// Maps an exit reason to the wire kind it reports as under the
        /// config's flags, or false when this build (or this configuration)
        /// does not report the reason.
        /// </summary>
        public static bool TryMapReasonToKind(int reason, ShardPilotConfig config, out string kind)
        {
            if (reason == ReasonAnr && config != null && config.AnrExitInfoEnabled)
            {
                kind = ShardPilotAnrIds.AnrKind;
                return true;
            }

            if (reason == ReasonCrashNative && config != null && config.NativeCrashExitInfoEnabled)
            {
                kind = ShardPilotAnrIds.CrashKind;
                return true;
            }

            kind = null;
            return false;
        }
    }

    /// <summary>
    /// The persisted high-water cursor over exit records:
    /// the composite of (timestamp, reason, pid, definingUid), compared
    /// component-wise in that order. Only records with a STRICTLY greater
    /// composite are considered on a pass; everything at or below the
    /// cursor has been settled — reported, degraded-reported, or
    /// deliberately suppressed. Persisted per (workspace, app, environment)
    /// scope in the SDK's key-value store, so a staging→prod credential
    /// switch on one device replays the same records into each
    /// environment's own occurrence stream (environments stay
    /// separable; the server-side aei_record_key index is environment-
    /// scoped for the same reason). Cursor loss is expected and safe: the
    /// deterministic aei_record_key collapses replays server-side.
    /// </summary>
    internal sealed class ShardPilotAeiCursor
    {
        private const string Prefix = "v1:";
        internal const string StorageKeyPrefix = "shardpilot.aei_cursor.";

        /// <summary>
        /// Per-scope SUPPRESSION-FLOOR marker key prefix, in the cursor's
        /// own KV namespace. Stamped (now-ms) by the client's four runtime
        /// revocation paths — local crash opt-out, propagated opt-out,
        /// local consent denial, propagated denial — which run on any
        /// platform and while the ANR lane may be completely DARK: no AEI
        /// read happens at revocation time (JNI may not even be safe off
        /// Android), the marker alone records "records at or before this
        /// moment were disclaimed". The next ENABLED pass floors its
        /// cursor to max(stored cursor, marker) BEFORE reporting, so an
        /// opted-out-period record Android retained cannot resurface as
        /// new after a re-enable plus a later flag-on launch; the
        /// suppression pass folds the marker into its own floor the same
        /// way. Consumed (cleared) only once the floored cursor is
        /// durably saved.
        /// </summary>
        internal const string SuppressionFloorKeyPrefix = "shardpilot.aei_suppress_floor.";

        /// <summary>
        /// Per-scope ARMED-suppression key prefix, a sibling of the timestamp
        /// floor in the cursor's KV namespace. Set (forward-only epoch) by the
        /// same revocation paths that stamp the floor and cleared ONLY by a
        /// completed enabled pass, it records that an opted-out DARK WINDOW is
        /// still OPEN. It is the clock-rollback backstop the pure timestamp
        /// floor cannot be: a wall clock that jumped FORWARD during the
        /// disclaimed window then rolled BACK before re-enable leaves a record
        /// dated ABOVE the floor, and the armed flag makes the first enabled
        /// pass settle past everything enumerable on that evidence (see the
        /// reporter's ConsumeSuppressionFloorMarker) — disclaiming the whole
        /// window regardless of clock games while the floor stays the cheap
        /// fast-path for the common (clock-behaved) case. Deliberately a
        /// SEPARATE key from the floor: the floor keeps its bare-decimal wire
        /// shape, and a partial write (floor without arm, or arm without
        /// floor) degrades safely — the missing half falls back to the other
        /// mechanism, never to a leak.
        /// </summary>
        internal const string SuppressionArmedKeyPrefix = "shardpilot.aei_suppress_armed.";

        public long TimestampMs;
        public int Reason;
        public int Pid;
        public int DefiningUid;

        /// <summary>True when the record's composite is strictly newer than this cursor.</summary>
        public bool IsStrictlyOlderThan(ShardPilotAeiExitRecord record)
        {
            if (record.TimestampMs != TimestampMs)
            {
                return record.TimestampMs > TimestampMs;
            }

            if (record.Reason != Reason)
            {
                return record.Reason > Reason;
            }

            if (record.Pid != Pid)
            {
                return record.Pid > Pid;
            }

            return record.DefiningUid > DefiningUid;
        }

        public void AdvanceTo(ShardPilotAeiExitRecord record)
        {
            TimestampMs = record.TimestampMs;
            Reason = record.Reason;
            Pid = record.Pid;
            DefiningUid = record.DefiningUid;
        }

        /// <summary>
        /// True when <paramref name="other"/>'s composite is STRICTLY greater
        /// than this cursor's, under the same component order
        /// (<see cref="IsStrictlyOlderThan"/>) the high-water compares on. Used
        /// by the forward-only merge in <see cref="TrySave"/>.
        /// </summary>
        internal bool CompositeLessThan(ShardPilotAeiCursor other)
        {
            if (TimestampMs != other.TimestampMs)
            {
                return TimestampMs < other.TimestampMs;
            }

            if (Reason != other.Reason)
            {
                return Reason < other.Reason;
            }

            if (Pid != other.Pid)
            {
                return Pid < other.Pid;
            }

            return DefiningUid < other.DefiningUid;
        }

        public string Serialize()
        {
            return Prefix +
                   TimestampMs.ToString(CultureInfo.InvariantCulture) + ":" +
                   Reason.ToString(CultureInfo.InvariantCulture) + ":" +
                   Pid.ToString(CultureInfo.InvariantCulture) + ":" +
                   DefiningUid.ToString(CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Parses a persisted cursor. An absent, foreign-version, or corrupt
        /// value yields the ZERO cursor (everything is newer): the pass then
        /// re-reads the batch and the server-side aei_record_key dedup
        /// absorbs the replay — a bad cursor must never fail the lane.
        /// </summary>
        public static ShardPilotAeiCursor Parse(string raw)
        {
            var cursor = new ShardPilotAeiCursor();
            raw = ShardPilotConfig.Trim(raw);
            if (string.IsNullOrEmpty(raw) || !raw.StartsWith(Prefix, StringComparison.Ordinal))
            {
                return cursor;
            }

            var parts = raw.Substring(Prefix.Length).Split(':');
            if (parts.Length != 4)
            {
                return cursor;
            }

            long timestamp;
            int reason;
            int pid;
            int definingUid;
            if (!long.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out timestamp) ||
                !int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out reason) ||
                !int.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out pid) ||
                !int.TryParse(parts[3], NumberStyles.Integer, CultureInfo.InvariantCulture, out definingUid))
            {
                return new ShardPilotAeiCursor();
            }

            cursor.TimestampMs = timestamp;
            cursor.Reason = reason;
            cursor.Pid = pid;
            cursor.DefiningUid = definingUid;
            return cursor;
        }

        public static string StorageKey(ShardPilotConfig config)
        {
            return StorageKeyPrefix + ShardPilotCrashSpool.ScopeHashFor(config);
        }

        /// <summary>Loads the scope's cursor; unreadable = the zero cursor.</summary>
        public static ShardPilotAeiCursor Load(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            try
            {
                return Parse(store.Get(StorageKey(config)));
            }
            catch
            {
                return new ShardPilotAeiCursor();
            }
        }

        /// <summary>
        /// Persists the cursor FORWARD-ONLY (read-modify-write): it merges with
        /// the currently-stored value and saves only the MAX composite, so a
        /// STALE async settle from an older pass — a late transport-completion
        /// callback, say — can never move the persisted mark BACKWARDS past a
        /// newer pass's advance or a saved-and-cleared suppression floor.
        /// Returns false when the write failed — callers on the SUPPRESSION
        /// path treat that as "the floor did not stick" and keep the in-memory
        /// suppression for this process only.
        ///
        /// Raciness bound: the KV store exposes no compare-and-swap, so this
        /// RMW is not atomic against a concurrent writer. In practice the
        /// cursor is single-writer — the reporter pass runs at SDK init on the
        /// main thread, its late-ack settles post back to the main thread
        /// through the dispatcher, and every runtime floor/raise
        /// write is a main-thread PlayerPrefs op — so the read and the write
        /// cannot interleave with another writer. The merge is the defensive
        /// backstop for the residual: it keeps the mark monotone even if a
        /// settle ever raced a newer advance, and a record a defeated merge
        /// replays collapses server-side on aei_record_key.
        /// </summary>
        public static bool TrySave(IShardPilotKeyValueStore store, ShardPilotConfig config, ShardPilotAeiCursor cursor)
        {
            try
            {
                // Read-modify-write under whatever atomicity the KV gives:
                // keep max(stored, incoming). A stored value that is already
                // at-or-ahead of the incoming cursor is preserved as-is.
                var stored = Load(store, config);
                var forward = stored.CompositeLessThan(cursor) ? cursor : stored;
                store.Set(StorageKey(config), forward.Serialize());
                return true;
            }
            catch
            {
                return false;
            }
        }

        internal static string SuppressionFloorKey(ShardPilotConfig config)
        {
            return SuppressionFloorKeyForScopeHash(ShardPilotCrashSpool.ScopeHashFor(config));
        }

        /// <summary>
        /// The marker key for a scope addressed by its 16-hex scope hash
        /// directly — the SAME <see cref="ShardPilotCrashSpool.ScopeHashFor"/>
        /// digest the era filenames embed, so a revocation sweep that
        /// discovers an INACTIVE scope only through its era file on disk can
        /// still stamp that scope's floor without ever reconstructing its
        /// (workspace, app, environment) config.
        /// </summary>
        internal static string SuppressionFloorKeyForScopeHash(string scopeHash)
        {
            return SuppressionFloorKeyPrefix + scopeHash;
        }

        /// <summary>
        /// Records the scope's suppression-floor marker at revocation time
        /// (see <see cref="SuppressionFloorKeyPrefix"/>). Forward-only: an
        /// existing later marker is never regressed. A plain KV write —
        /// deliberately no AEI read, no era-store touch, safe on every
        /// platform and with the lane dark. Best-effort; never throws.
        /// </summary>
        public static void RecordSuppressionFloor(
            IShardPilotKeyValueStore store, ShardPilotConfig config, long nowUnixMs)
        {
            if (config == null)
            {
                return;
            }

            RecordSuppressionFloorForScopeHash(store, ShardPilotCrashSpool.ScopeHashFor(config), nowUnixMs);
        }

        /// <summary>
        /// The hash-addressed twin of <see cref="RecordSuppressionFloor"/>,
        /// for the all-scope revocation sweeps: a global denial/opt-out must
        /// floor scopes it can only enumerate by the scope hash in their
        /// era filenames — a swept INACTIVE scope otherwise re-initializes
        /// after a later re-grant/re-enable with a zero or stale cursor and
        /// reports Android-retained records from the revoked interval. Same
        /// forward-only-max semantics; best-effort; never throws.
        /// </summary>
        public static void RecordSuppressionFloorForScopeHash(
            IShardPilotKeyValueStore store, string scopeHash, long nowUnixMs)
        {
            if (store == null || string.IsNullOrEmpty(scopeHash))
            {
                return;
            }

            try
            {
                var key = SuppressionFloorKeyForScopeHash(scopeHash);
                long existing;
                if (!long.TryParse(
                        ShardPilotConfig.Trim(store.Get(key)),
                        NumberStyles.Integer, CultureInfo.InvariantCulture, out existing) ||
                    nowUnixMs > existing)
                {
                    // Forward-only: never regress an existing later floor.
                    store.Set(key, nowUnixMs.ToString(CultureInfo.InvariantCulture));
                }

                // ARM the dark window (clock-rollback backstop): the timestamp
                // floor above is the cheap fast-path, but a clock that jumped
                // forward mid-window then rolled back before re-enable can
                // leave a record dated ABOVE it. The armed flag makes the first
                // ENABLED pass settle past everything enumerable on that
                // evidence. Armed even when the floor did not move (arming is
                // orthogonal to the timestamp) and cleared ONLY by a completed
                // enabled pass.
                ArmSuppressionForScopeHash(store, scopeHash, nowUnixMs);
            }
            catch
            {
                // Best-effort: the flag-on pass's live consent gate still
                // suppresses a standing opt-out; the marker only covers the
                // dark-lane window.
            }
        }

        /// <summary>
        /// Raises an EXISTING marker to <paramref name="nowUnixMs"/> —
        /// called by the re-enable/re-grant twins of the four revocation
        /// paths: a marker still standing there means the dark disclaimed
        /// window ran until THIS moment, so records from inside it (an ANR
        /// during the opted-out period, not just the backlog standing when
        /// the opt-out landed) die at the next enabled pass too. Never
        /// creates a marker (no revocation window — nothing disclaimed) and
        /// never lowers one (forward-only under clock rollback). Records
        /// after the re-enable are strictly above the raised floor and
        /// report normally. Best-effort; never throws.
        /// </summary>
        public static void RaiseSuppressionFloorIfMarked(
            IShardPilotKeyValueStore store, ShardPilotConfig config, long nowUnixMs)
        {
            if (store == null || config == null)
            {
                return;
            }

            RaiseSuppressionFloorIfMarkedForScopeHash(
                store, ShardPilotCrashSpool.ScopeHashFor(config), nowUnixMs);
        }

        /// <summary>
        /// The hash-addressed twin of <see cref="RaiseSuppressionFloorIfMarked"/>,
        /// for the re-enable/re-grant ALL-SCOPE raise
        /// (<see cref="ShardPilotAnrEraStore.RaiseSuppressionFloorForAllScopesAtResolvedDirectory"/>):
        /// a GLOBAL re-grant/re-enable must close the dark window for every
        /// scope it can enumerate through the era files, not just the current
        /// one, mirroring the all-scope opt-out sweep that stamps them. Raises
        /// an EXISTING marker to <paramref name="nowUnixMs"/> only — never
        /// creates one (no marker = no revocation window here) and never lowers
        /// one (forward-only under clock rollback). The scope's ARMED flag is
        /// left untouched: only a completed enabled pass clears it, so a scope
        /// swept-and-armed while inactive stays disclaimed until its own next
        /// enabled pass. Best-effort; never throws.
        /// </summary>
        public static void RaiseSuppressionFloorIfMarkedForScopeHash(
            IShardPilotKeyValueStore store, string scopeHash, long nowUnixMs)
        {
            if (store == null || string.IsNullOrEmpty(scopeHash))
            {
                return;
            }

            try
            {
                var key = SuppressionFloorKeyForScopeHash(scopeHash);
                long existing;
                if (long.TryParse(
                        ShardPilotConfig.Trim(store.Get(key)),
                        NumberStyles.Integer, CultureInfo.InvariantCulture, out existing) &&
                    existing > 0 && nowUnixMs > existing)
                {
                    store.Set(key, nowUnixMs.ToString(CultureInfo.InvariantCulture));
                }
            }
            catch
            {
                // Best-effort; the stamped floor still covers the window up
                // to the revocation moment, and the armed flag is the
                // clock-proof backstop.
            }
        }

        /// <summary>
        /// Reads the scope's suppression-floor marker; 0 when absent or
        /// unreadable (an unreadable marker must not wedge the pass — the
        /// live consent gates still rule).
        /// </summary>
        public static long ReadSuppressionFloor(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            try
            {
                long value;
                return long.TryParse(
                    ShardPilotConfig.Trim(store.Get(SuppressionFloorKey(config))),
                    NumberStyles.Integer, CultureInfo.InvariantCulture, out value) && value > 0
                    ? value
                    : 0;
            }
            catch
            {
                return 0;
            }
        }

        /// <summary>
        /// Clears the marker once a floored cursor at or above it is
        /// durably saved. Best-effort; never throws.
        /// </summary>
        public static void ClearSuppressionFloor(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            try
            {
                var key = SuppressionFloorKey(config);
                if (!string.IsNullOrEmpty(ShardPilotConfig.Trim(store.Get(key))))
                {
                    store.Set(key, string.Empty);
                }
            }
            catch
            {
                // Best-effort; an uncleared marker only costs one redundant
                // (idempotent) floor on a later pass.
            }
        }

        internal static string SuppressionArmedKey(ShardPilotConfig config)
        {
            return SuppressionArmedKeyForScopeHash(ShardPilotCrashSpool.ScopeHashFor(config));
        }

        /// <summary>
        /// The hash-addressed ARMED key — the per-scope "an opted-out dark
        /// window is OPEN" epoch, keyed by the same 16-hex scope hash the era
        /// filenames embed so an all-scope sweep can arm an INACTIVE scope it
        /// only discovers on disk.
        /// </summary>
        internal static string SuppressionArmedKeyForScopeHash(string scopeHash)
        {
            return SuppressionArmedKeyPrefix + scopeHash;
        }

        /// <summary>
        /// Arms the scope's dark window (best-effort, forward-only epoch).
        /// Presence — any positive value — means "armed"; the epoch is stored
        /// for debuggability and to keep the write forward-only. Own try/catch
        /// so a store failure degrades to the timestamp-floor fast path.
        /// </summary>
        private static void ArmSuppressionForScopeHash(
            IShardPilotKeyValueStore store, string scopeHash, long nowUnixMs)
        {
            try
            {
                var key = SuppressionArmedKeyForScopeHash(scopeHash);
                long existing;
                if (!long.TryParse(
                        ShardPilotConfig.Trim(store.Get(key)),
                        NumberStyles.Integer, CultureInfo.InvariantCulture, out existing) ||
                    nowUnixMs > existing)
                {
                    store.Set(key, nowUnixMs.ToString(CultureInfo.InvariantCulture));
                }
            }
            catch
            {
                // Best-effort; the timestamp floor still covers the common
                // (clock-behaved) window without the armed backstop.
            }
        }

        /// <summary>
        /// True when the scope's dark window is ARMED — a revocation opened it
        /// and no completed enabled pass has settled it yet. The first enabled
        /// pass consults this to decide whether to run the clock-rollback
        /// settle-all backstop (see the reporter's
        /// ConsumeSuppressionFloorMarker): a record dated in the launch's
        /// future is the clock-game signature the armed flag lets that pass
        /// act on. Unreadable = not armed (the live consent gate still rules).
        /// </summary>
        public static bool IsSuppressionArmed(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            if (store == null || config == null)
            {
                return false;
            }

            try
            {
                long value;
                return long.TryParse(
                    ShardPilotConfig.Trim(store.Get(SuppressionArmedKey(config))),
                    NumberStyles.Integer, CultureInfo.InvariantCulture, out value) && value > 0;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Disarms the scope's dark window once a completed enabled pass has
        /// settled it. Best-effort; never throws.
        /// </summary>
        public static void ClearSuppressionArmed(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            if (store == null || config == null)
            {
                return;
            }

            try
            {
                var key = SuppressionArmedKey(config);
                if (!string.IsNullOrEmpty(ShardPilotConfig.Trim(store.Get(key))))
                {
                    store.Set(key, string.Empty);
                }
            }
            catch
            {
                // Best-effort; a stuck armed flag only costs one extra
                // (idempotent) settle-all check on the next enabled pass.
            }
        }
    }

    /// <summary>
    /// Deterministic identity derivation for the ANR lane, plus the small
    /// shared constants. The wire field is <c>aei_record_key</c>:
    /// a stable SHA-256-derived UUID-form
    /// value the server's partial unique index on
    /// (workspace, app, environment_id, aei_record_key) de-duplicates on.
    /// </summary>
    internal static class ShardPilotAnrIds
    {
        /// <summary>The event kind the ANR lane reports.</summary>
        public const string AnrKind = "anr";

        /// <summary>
        /// The BASE crash kind a REASON_CRASH_NATIVE record
        /// reports as. On the JSON wire the base kind is OMITTED (an absent
        /// <c>kind</c> field IS <c>crash</c>), and the tombstone multipart
        /// lane is base-kind by construction — this token exists for the
        /// reason policy's mapping decision, never as a wire value.
        /// </summary>
        public const string CrashKind = "crash";

        private const string AeiDomain = "shardpilot.aei.v1";
        private const string WatchdogDomain = "shardpilot.anr.watchdog.v1";

        /// <summary>
        /// The aei_record_key of an ApplicationExitInfo record: SHA-256 over
        /// the domain-separated exit-record identity composite
        /// <c>"shardpilot.aei.v1\n{timestampMs}\n{reason}\n{pid}\n{definingUid}"</c>
        /// (invariant decimal), first 16 digest bytes rendered as a UUID
        /// (version nibble 8, RFC 4122 variant). Deterministic by
        /// construction — a replayed record (cursor loss, reinstall,
        /// multi-process races) derives the SAME key and collapses
        /// server-side.
        /// </summary>
        public static string AeiRecordKeyFor(long timestampMs, int reason, int pid, int definingUid)
        {
            return DeriveRecordKey(
                AeiDomain + "\n" +
                timestampMs.ToString(CultureInfo.InvariantCulture) + "\n" +
                reason.ToString(CultureInfo.InvariantCulture) + "\n" +
                pid.ToString(CultureInfo.InvariantCulture) + "\n" +
                definingUid.ToString(CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// The aei_record_key of an in-session watchdog stall report:
        /// SHA-256 over
        /// <c>"shardpilot.anr.watchdog.v1\n{eraId}\n{stallStartMs}"</c> —
        /// the process era id plus the stall's start on the era's monotonic
        /// clock. Stable for one stall episode, so the spool's re-send of an
        /// unacknowledged report on a later launch collapses server-side
        /// instead of double-counting.
        /// </summary>
        public static string WatchdogRecordKeyFor(string eraId, long stallStartMs)
        {
            return DeriveRecordKey(
                WatchdogDomain + "\n" +
                (eraId ?? string.Empty) + "\n" +
                stallStartMs.ToString(CultureInfo.InvariantCulture));
        }

        private static string DeriveRecordKey(string input)
        {
            using (var sha = SHA256.Create())
            {
                var digest = sha.ComputeHash(Encoding.UTF8.GetBytes(input));

                // UUID-form: first 16 digest bytes with the version nibble
                // forced to 8 (a hash-derived, non-timestamp UUID — the
                // RFC 9562 custom-format version) and the RFC 4122 variant
                // bits, so the value can never collide with the SDK's
                // UUIDv7 crash/event ids by shape.
                digest[6] = (byte)((digest[6] & 0x0f) | 0x80);
                digest[8] = (byte)((digest[8] & 0x3f) | 0x80);

                var builder = new StringBuilder(36);
                for (var i = 0; i < 16; i++)
                {
                    if (i == 4 || i == 6 || i == 8 || i == 10)
                    {
                        builder.Append('-');
                    }

                    builder.Append(digest[i].ToString("x2", CultureInfo.InvariantCulture));
                }

                return builder.ToString();
            }
        }

        /// <summary>
        /// True when the value has exactly the canonical lowercase UUID
        /// shape the derivations above produce — the sanitizer drops
        /// anything else rather than shipping a malformed key the server's
        /// unique index would treat as a distinct identity.
        /// </summary>
        public static bool IsRecordKeyShape(string value)
        {
            if (value == null || value.Length != 36)
            {
                return false;
            }

            for (var i = 0; i < value.Length; i++)
            {
                var ch = value[i];
                if (i == 8 || i == 13 || i == 18 || i == 23)
                {
                    if (ch != '-')
                    {
                        return false;
                    }

                    continue;
                }

                if (!(ch >= '0' && ch <= '9' || ch >= 'a' && ch <= 'f'))
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// The byte budget for an ApplicationExitInfo ANR trace: a quarter
        /// of the crash spool's default TOTAL byte cap
        /// (<see cref="ShardPilotConfig.DefaultCrashSpoolMaxBytes"/>), so a
        /// trace-carrying report — with JSON escaping overhead — still
        /// coexists with other spooled reports instead of evicting the
        /// backlog. The stalled main thread sits at the top of the dump, so
        /// truncation costs the least relevant tail.
        /// </summary>
        public const int MaxAnrTraceBytes = 128 * 1024;

        /// <summary>
        /// Bounds raw trace text to <see cref="MaxAnrTraceBytes"/> of UTF-8,
        /// cutting on a char boundary. Size-bounding only — free-text PII
        /// scrubbing for raw_text is server-side.
        /// </summary>
        public static string BoundRawText(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return string.Empty;
            }

            if (Encoding.UTF8.GetByteCount(value) <= MaxAnrTraceBytes)
            {
                return value;
            }

            var low = LongestUtf8PrefixLength(value, MaxAnrTraceBytes);
            return low == 0 ? string.Empty : value.Substring(0, low);
        }

        /// <summary>
        /// Bounded line-by-line trace accumulation — the ONE algorithm the
        /// JNI reader runs on devices and the headless suite exercises with
        /// fake line feeds. Every line is capped to the REMAINING byte
        /// budget BEFORE it is appended, so a single pathological
        /// multi-megabyte dump line can never be RETAINED past
        /// <paramref name="maxBytes"/> (<see cref="BoundRawText"/> trimming
        /// after the fact is the wire-side cap, not an allocation cap). On
        /// the line that crosses the budget, the in-budget prefix is kept
        /// plus AT MOST ONE more character (a surrogate pair moves whole,
        /// so at most 4 bytes past the cap): that deliberate one-char
        /// overshoot is the truncation signal the reporter's existing
        /// derived flag (<c>BoundRawText(text).Length &lt; text.Length</c>)
        /// keys on, and <see cref="BoundRawText"/> trims it back off the
        /// wire. The rest of the crossing line and the rest of the stream
        /// are DISCARDED unread. Streams that fit the budget come back
        /// verbatim (each line + '\n'), byte-identical to an unbounded
        /// accumulation. Null for an empty stream; a throw from
        /// <paramref name="readNextLine"/> propagates — the platform reader
        /// owns stream-failure handling (any failure is the degraded
        /// metadata-only shape).
        /// </summary>
        public static string ReadBoundedTraceText(Func<string> readNextLine, int maxBytes)
        {
            if (readNextLine == null || maxBytes <= 0)
            {
                return null;
            }

            var builder = new StringBuilder();
            var bytes = 0;
            while (bytes <= maxBytes)
            {
                var line = readNextLine();
                if (line == null)
                {
                    break;
                }

                var lineBytes = Encoding.UTF8.GetByteCount(line);
                if (bytes + lineBytes + 1 <= maxBytes)
                {
                    builder.Append(line);
                    builder.Append('\n');
                    bytes += lineBytes + 1;
                    continue;
                }

                // This line (or its terminating '\n') crosses the budget:
                // keep the prefix that fits plus the one-char truncation
                // signal, then stop — nothing more is pulled from the
                // stream.
                var keep = LongestUtf8PrefixLength(line, maxBytes - bytes);
                builder.Append(line, 0, keep);
                if (keep < line.Length)
                {
                    var signalChars =
                        char.IsHighSurrogate(line[keep]) && keep + 1 < line.Length ? 2 : 1;
                    builder.Append(line, keep, signalChars);
                }
                else
                {
                    builder.Append('\n');
                }

                break;
            }

            return builder.Length == 0 ? null : builder.ToString();
        }

        /// <summary>
        /// The chunk granularity of the bounded tombstone read: the most
        /// bytes one pump call may be asked to move. Each request is
        /// additionally budgeted to the remaining cap allowance inside
        /// <see cref="ReadBoundedTombstoneBytes"/>, so the constant only
        /// sizes the transfer buffer (a full 4 MiB read costs ~64 pump
        /// calls; the accumulator never holds more than maxBytes + 1).
        /// </summary>
        public const int TombstoneReadChunkBytes = 64 * 1024;

        /// <summary>
        /// Bounded chunked tombstone-byte accumulation — the ONE loop the
        /// JNI reader runs on devices and the headless suite drives with
        /// fakes (the binary sibling of <see cref="ReadBoundedTraceText"/>).
        /// Reads AT MOST <paramref name="maxBytes"/> + 1 bytes total: every
        /// <paramref name="readChunkIntoAccumulator"/> call is budgeted to
        /// the remaining allowance (never more than
        /// <see cref="TombstoneReadChunkBytes"/> at once), and the FIRST
        /// byte past the cap classifies the stream
        /// <see cref="ShardPilotAeiTombstoneRead.OverCap"/> and STOPS — the
        /// rest of an over-cap dump is never drained into the accumulator
        /// (a multi-megabyte tombstone must not be fully buffered on the
        /// next-launch pass just to be skipped), and
        /// <paramref name="materializeAccumulated"/> is never invoked for
        /// it, so over-cap bytes are neither copied out nor marshalled. An
        /// EOF within the cap materializes the accumulated bytes exactly
        /// once; an empty stream is null — the missing-stream degraded
        /// shape. A throw from either delegate propagates — the platform
        /// reader owns stream-failure handling (any failure is the degraded
        /// metadata-only shape). A pump that CLAIMS more than it was asked
        /// for still counts in full, so an ill-behaved reader classifies
        /// over-cap rather than smuggling past the budget (the reporter
        /// re-asserts the cap on the materialized bytes besides).
        /// </summary>
        public static ShardPilotAeiTombstoneRead ReadBoundedTombstoneBytes(
            Func<int, int> readChunkIntoAccumulator,
            Func<byte[]> materializeAccumulated,
            int maxBytes)
        {
            if (readChunkIntoAccumulator == null || maxBytes <= 0)
            {
                return null;
            }

            var allowance = (long)maxBytes + 1;
            long total = 0;
            while (total < allowance)
            {
                var budget = allowance - total;
                var request = budget < TombstoneReadChunkBytes ? (int)budget : TombstoneReadChunkBytes;
                var read = readChunkIntoAccumulator(request);
                if (read <= 0)
                {
                    break;
                }

                total += read;
            }

            if (total == 0)
            {
                return null;
            }

            if (total > maxBytes)
            {
                return new ShardPilotAeiTombstoneRead { OverCap = true };
            }

            var bytes = materializeAccumulated == null ? null : materializeAccumulated();
            if (bytes == null || bytes.Length == 0)
            {
                return null;
            }

            return new ShardPilotAeiTombstoneRead { Bytes = bytes };
        }

        /// <summary>
        /// The longest char-prefix length of <paramref name="value"/> whose
        /// UTF-8 encoding fits <paramref name="maxBytes"/>: binary search
        /// (byte count is monotonic in prefix length), then back off a
        /// split surrogate pair. Zero when nothing fits.
        /// </summary>
        private static int LongestUtf8PrefixLength(string value, int maxBytes)
        {
            if (string.IsNullOrEmpty(value) || maxBytes <= 0)
            {
                return 0;
            }

            var low = 0;
            var high = Math.Min(value.Length, maxBytes);
            while (low < high)
            {
                var mid = low + (high - low + 1) / 2;
                if (Encoding.UTF8.GetByteCount(value, 0, mid) <= maxBytes)
                {
                    low = mid;
                }
                else
                {
                    high = mid - 1;
                }
            }

            if (low > 0 && char.IsHighSurrogate(value[low - 1]))
            {
                low--;
            }

            return low;
        }
    }
}
