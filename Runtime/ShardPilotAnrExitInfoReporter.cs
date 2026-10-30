using System;
using System.Collections.Generic;

namespace ShardPilot
{
    /// <summary>
    /// The next-launch ApplicationExitInfo pass: enumerates
    /// EVERY historical exit record the platform still holds (the OS's own
    /// AEI ring bounds the enumeration; the per-pass cap bounds the WORK
    /// and applies only after enumeration — see the no-skip rule on
    /// <see cref="MaxExitRecordsPerPass"/>), applies the persisted
    /// high-water cursor, maps reasons through the additive policy
    /// (REASON_ANR behind the ANR flag; REASON_CRASH_NATIVE behind the
    /// native-crash flag), attributes each report from
    /// the MATCHING era snapshot, and ships through the client's existing
    /// consent-gated crash capture — the write-ahead spool, the opt-out and
    /// denial gates, and the fail-closed read-failure hold all apply
    /// unchanged. A native record whose tombstone stream is present and
    /// within the 4 MiB lane cap ships the RAW protobuf over the dedicated
    /// multipart tombstone lane instead (ack-gated — no spool ever holds
    /// dump-class bytes); its missing-stream and over-cap shapes degrade to
    /// the metadata-only JSON report on the existing lane.
    ///
    /// Upload gating vs. attribution, per the ADR: the UPLOAD GATE is the
    /// CURRENT consent state, evaluated fail-closed exactly like a live
    /// crash send; ATTRIBUTION is frozen to the era snapshot — and only to
    /// a PRE-existing era: the current launch's own era is excluded, so a
    /// rolled-back wall clock can never attribute an old exit to the new
    /// launch's identity. When the current state is an opt-out or a consent
    /// denial, the pass ships NOTHING and settles the backlog permanently —
    /// suppression is CONSUME-WITHOUT-REPORTING: the floor covers every
    /// record currently readable (including future-dated ones a since-
    /// corrected clock left behind) and the era snapshots (a revoked
    /// subject's actor inputs) are wiped with the fail-closed owed-marker
    /// machinery. A read-failure hold ships nothing AND settles nothing:
    /// the next launch resolves it.
    /// </summary>
    internal static class ShardPilotAnrExitInfoReporter
    {
        /// <summary>
        /// Bounded work, no-skip: how many unsettled exit records one pass
        /// PROCESSES (reports or settles). The cap is applied AFTER the
        /// full enumeration and the pass runs in ascending identity order,
        /// so the OLDEST unsettled records are processed first and the
        /// cursor settles only through the newest PROCESSED record —
        /// capped-out newer records stay strictly above the cursor and
        /// report on the next launch. The cap must never be applied at the
        /// READ: a capped read returns only the newest records, and
        /// settling over that window would strand every older unseen
        /// record below the cursor forever.
        /// </summary>
        internal const int MaxExitRecordsPerPass = 16;

        private const int MaxReasonDescriptionChars = 256;

        /// <summary>
        /// Runs one next-launch pass. Never throws; a null source (non-
        /// Android, JNI unavailable) or a flag-off config is zero behavior.
        /// <paramref name="currentEraId"/> is THIS launch's era id, excluded
        /// from attribution (every record read here predates this process —
        /// see <see cref="ShardPilotAnrEraStore.FindEraFor(long, string)"/>).
        /// <paramref name="nowUnixMs"/> is the pass's wall clock, injectable
        /// for the harness.
        /// </summary>
        public static void RunNextLaunchPass(
            ShardPilotClient client,
            ShardPilotConfig config,
            IShardPilotAeiSource source,
            IShardPilotKeyValueStore store,
            ShardPilotAnrEraStore eraStore,
            string currentEraId,
            long nowUnixMs,
            Action<string> log)
        {
            try
            {
                if (client == null || config == null || store == null ||
                    (!config.AnrExitInfoEnabled && !config.NativeCrashExitInfoEnabled))
                {
                    // Neither AEI lane is armed: zero behavior — no read, no
                    // cursor write. Each REASON still reports only under its
                    // own flag (the mapping below); this gate is about
                    // whether the shared sweep runs at all.
                    return;
                }

                string disabledReason;
                if (!client.IsCrashReportingEnabled(out disabledReason))
                {
                    if (disabledReason == ShardPilotCrashSettings.OptOutReason ||
                        disabledReason == ShardPilotConsent.DeniedError)
                    {
                        ApplySuppression(config, source, store, eraStore, nowUnixMs, log);
                    }

                    // settings_read_failed holds (nothing sent, nothing
                    // settled, nothing wiped — the stored decision is
                    // unknowable and the next launch resolves it);
                    // not_configured is a no-crash-plane init and owes
                    // nothing.
                    return;
                }

                if (source == null)
                {
                    return;
                }

                List<ShardPilotAeiExitRecord> records;
                try
                {
                    records = source.ReadExitRecords();
                }
                catch
                {
                    records = null;
                }

                if (records == null || records.Count == 0)
                {
                    return;
                }

                for (var i = records.Count - 1; i >= 0; i--)
                {
                    if (records[i] == null)
                    {
                        records.RemoveAt(i);
                    }
                }

                records.Sort(CompareRecords);

                var cursor = ShardPilotAeiCursor.Load(store, config);

                // A revocation that landed while the lane was DARK left the
                // suppression-floor marker AND the armed flag (no pass ran to
                // floor the cursor then): fold them in BEFORE reporting, so
                // records from the opted-out period die silently instead of
                // resurfacing as new after re-enable. The marker's timestamp
                // floor is the fast path; the armed flag is the clock-rollback
                // backstop, which needs the enumerated records and this pass's
                // wall clock to spot a forward-dated record and settle past it.
                ConsumeSuppressionFloorMarker(store, config, cursor, records, nowUnixMs);

                // EVICTION-SAFE SETTLEMENT (see PassSettlementLedger): every
                // settle flows through the pass ledger, which holds a
                // SPOOLED record's cursor-settle until the rest of the pass
                // can no longer evict its write-ahead copy, and settles a
                // completed send by the crash spool's own disposal rule —
                // acknowledged OR permanently rejected.
                var ledger = new PassSettlementLedger(cursor, store, config, client);

                var processed = 0;
                ShardPilotAeiExitRecord previousProcessed = null;
                for (var i = 0; i < records.Count; i++)
                {
                    var record = records[i];
                    if (!cursor.IsStrictlyOlderThan(record))
                    {
                        // At or below the high-water mark: already settled
                        // on an earlier launch (reported, degraded-reported,
                        // or deliberately suppressed).
                        continue;
                    }

                    if (previousProcessed != null && CompareRecords(previousProcessed, record) == 0)
                    {
                        // With settles deferred to the ledger the shared
                        // cursor no longer advances per record, so an exact-
                        // duplicate enumeration entry is skipped against the
                        // previous identity instead (the old per-record
                        // settle used to absorb it via the cursor check).
                        continue;
                    }

                    previousProcessed = record;

                    if (!ShardPilotAnrEraStore.IsValidUnixMs(record.TimestampMs))
                    {
                        // A corrupt platform timestamp outside the
                        // convertible unix-ms range can neither build a
                        // report (FromUnixMs would throw this record into
                        // the pass catch and abort the WHOLE pass, every
                        // launch) nor safely settle (advancing the forward-
                        // only cursor to a garbage high-water — long.Max,
                        // say — would suppress every real future record
                        // forever). Skipped without settling: a negative
                        // value sits below any real cursor and an over-range
                        // one sorts after every real record, so no real
                        // record is ever blocked, and the OS's own bounded
                        // AEI ring ages the corrupt record out.
                        SafeLog(log, "shardpilot anr pass skipped an exit record with an unrepresentable timestamp");
                        continue;
                    }

                    if (processed >= MaxExitRecordsPerPass)
                    {
                        // The per-pass processing cap, applied AFTER the
                        // full enumeration: the records ran oldest-first,
                        // the cursor settles only through the newest
                        // PROCESSED-and-durable record, and everything from
                        // here on is still strictly newer than the cursor —
                        // the next launch's pass resumes exactly here.
                        // Nothing is ever skipped (see MaxExitRecordsPerPass).
                        SafeLog(log, "shardpilot anr pass reached the per-pass record cap; remaining newer records report next launch");
                        break;
                    }

                    processed++;

                    string kind;
                    if (!ShardPilotAeiReasons.TryMapReasonToKind(record.Reason, config, out kind))
                    {
                        // A reason this build (or this configuration) does
                        // not report. The cursor STILL advances over it —
                        // the high-water covers all seen exit records, so a
                        // reason armed in a later phase reports NEW records
                        // only, never this back-catalog (the
                        // replay rule).
                        ledger.SettleWithoutReport(record);
                        continue;
                    }

                    var era = eraStore == null ? null : eraStore.FindEraFor(record.TimestampMs, currentEraId);
                    if (era != null && era.SuppressesReports)
                    {
                        // The exit happened in an era whose own posture
                        // already forbade collection (opt-out or denial live
                        // at era start): settled without reporting.
                        ledger.SettleWithoutReport(record);
                        continue;
                    }

                    ShardPilotCrashEvent crashEvent;
                    if (record.Reason == ShardPilotAeiReasons.ReasonCrashNative)
                    {
                        // DUMP-CLASS RECHECK + READ, one atomic section
                        // under the crash DISPATCH GATE:
                        // the pass-start gate is not enough — an opt-out/
                        // denial that COMPLETED while this pass was already
                        // running must not have one more raw tombstone
                        // copied into managed memory just for the upload
                        // gate to refuse it later. Re-evaluating current
                        // state immediately before the read closes the
                        // mid-pass window only up to the recheck; the GATE
                        // closes the recheck-to-read gap itself: a
                        // revocation acquires this same gate after flipping
                        // state, so a revocation that RETURNED to its caller
                        // either flipped before this hold (the recheck here
                        // refuses) or is still blocked on the gate until the
                        // read finishes — a completed revocation is never
                        // followed by a dump-class read (same-thread
                        // reentrancy is the documented residual, exactly as
                        // on the send handoff). When the recheck refuses,
                        // STOP without reading and without advancing —
                        // exactly this lane's existing denied-path
                        // discipline (the mid-pass refusal stop below): the
                        // record stays above the cursor and the NEXT
                        // launch's pass-start gate settles it — the
                        // ApplySuppression consume-without-reporting floor
                        // for an opt-out/denial, the hold for a settings
                        // read failure.
                        ShardPilotAeiTombstoneRead tombstone = null;
                        string gateStopMessage = null;
                        lock (client.CrashDispatchGateForAeiPass)
                        {
                            string recheckDisabledReason;
                            if (!client.IsCrashReportingEnabled(out recheckDisabledReason))
                            {
                                gateStopMessage = "shardpilot aei pass stopped: crash reporting was disabled mid-pass; " +
                                                  "no tombstone stream opened, this and newer records settle next launch";
                            }
                            // Server backpressure, honored BEFORE the stream
                            // is opened: a persisted crash-plane Retry-After
                            // deadline (a 429/5xx answered an earlier send —
                            // a tombstone upload or a JSON crash send; same
                            // service) parks the retry, so a crash loop
                            // cannot hammer a backpressured ingest with a
                            // fresh multipart upload every launch. This is
                            // the SAME persist-and-honor marker the crash
                            // spool's re-send pass gates on (the read self-
                            // clears a spent marker). Nothing is skipped:
                            // the pass stops here, order preserved, and
                            // resumes once the deadline elapses.
                            else if (ShardPilotCrashSpool.ReadRetryAfterDeadline(store, config) > DateTime.UtcNow)
                            {
                                gateStopMessage = "shardpilot aei pass stopped: crash-plane Retry-After backpressure is active; " +
                                                  "the tombstone retry waits for the deadline";
                            }
                            else
                            {
                                // The native-crash record (the server parses
                                // its tombstone): a present, within-cap trace
                                // stream ships the RAW tombstone protobuf
                                // over the dedicated multipart lane — never
                                // the JSON lane, never parsed or truncated
                                // here.
                                tombstone = ReadTombstoneBytes(record);
                            }
                        }

                        if (gateStopMessage != null)
                        {
                            SafeLog(log, gateStopMessage);
                            break;
                        }
                        if (tombstone != null && tombstone.Bytes != null && tombstone.Bytes.Length > 0 &&
                            !tombstone.OverCap)
                        {
                            // NO SPOOL for dump-class bytes, so the settle is
                            // ACK-GATED — exactly the ledger's completed-send
                            // rule: a 2xx (incl. the server's 202 dedup) or a
                            // permanent reject settles; a retryable failure
                            // leaves the record above the cursor to retry
                            // next launch (collapsing server-side).
                            var tombstoneEntry = ledger.BeginReportedRecord(record);
                            var uploadDispatched = client.TryUploadAeiTombstone(
                                BuildTombstoneMetadataEvent(record, era),
                                tombstone.Bytes,
                                result => ledger.OnSendCompleted(tombstoneEntry, result));

                            if (!uploadDispatched)
                            {
                                // Refused outright (a consent gate raced
                                // shut, no key/sender): nothing left this
                                // pass. STOP without advancing — this and
                                // later records retry on the next launch.
                                ledger.DiscardRefused(tombstoneEntry);
                                SafeLog(log, "shardpilot aei pass stopped: tombstone upload refused; remaining records retry next launch");
                                break;
                            }

                            ledger.MarkAckGated(tombstoneEntry);
                            if (ledger.SendCompletedSettleably(tombstoneEntry))
                            {
                                continue;
                            }

                            // Dispatched but not yet completed: mirror the
                            // ack-gated stop discipline — this and newer
                            // records wait for the next launch (order
                            // preserved, nothing skipped), and the late
                            // completion settles through the ledger after
                            // the stop.
                            SafeLog(log, "shardpilot aei pass stopped: tombstone upload ack pending; " +
                                         "this and newer records settle on completion or retry next launch (de-duplicated server-side)");
                            break;
                        }

                        // Missing stream (the API-30 no-stream shape, an
                        // API-31+ NULL from the overwritten buffer, or a
                        // failed read) or an OVER-CAP tombstone: the ONE
                        // degraded rule — a metadata-only base-kind crash
                        // report on the EXISTING JSON lane, never a drop,
                        // never a raw byte, never a truncated proto.
                        crashEvent = BuildNativeCrashDegradedEvent(
                            record, era, tombstone != null && tombstone.OverCap);
                    }
                    else
                    {
                        crashEvent = BuildExitRecordEvent(record, era);
                    }

                    // SETTLE ONLY AFTER DURABILITY (or a completed send):
                    // the durability seam on CaptureCrash tells this pass
                    // what the write-ahead persist achieved, and the ledger
                    // entry — created BEFORE the capture so a synchronous
                    // transport callback finds it — collects the send's
                    // completion. The report's crash id is pre-stamped so
                    // the end-of-pass reconcile can probe whether the
                    // spooled copy SURVIVED the rest of the pass.
                    var entry = ledger.BeginReportedRecord(record);
                    crashEvent.CrashId = entry.CrashId;
                    ShardPilotCrashCaptureDurability durability;
                    var dispatched = client.CaptureCrash(
                        crashEvent,
                        result => ledger.OnSendCompleted(entry, result),
                        out durability);

                    if (durability == ShardPilotCrashCaptureDurability.Spooled)
                    {
                        // Durable write-ahead landed — but a LATER append in
                        // this same pass can still evict it from the capped
                        // spool before the live send lands, so the cursor-
                        // settle is a CANDIDATE held by the ledger: it
                        // becomes final at end of pass only if the record is
                        // still spooled (the re-send pass covers a kill) or
                        // its send completed meanwhile.
                        ledger.MarkSpooled(entry);
                        continue;
                    }

                    if (dispatched && durability == ShardPilotCrashCaptureDurability.NoSpool)
                    {
                        // No crash spool exists in this configuration
                        // (SpoolEnabled/CrashSpoolEnabled off, or no spool
                        // directory): there is nothing to be durable IN — and
                        // nothing whose caps could evict an earlier record —
                        // so dispatch-then-settle is the documented
                        // best-effort for spool-less integrations, matching
                        // the crash plane's own posture there.
                        ledger.MarkUnconditional(entry);
                        continue;
                    }

                    if (dispatched && durability == ShardPilotCrashCaptureDurability.SpoolAppendFailed)
                    {
                        // Dispatched, but the spool append failed: settle
                        // only on a COMPLETED send — the ingest ack, or a
                        // permanent (non-retryable 4xx) reject, which the
                        // crash spool itself disposes of rather than
                        // retrying (a poisoned record must not block this
                        // pass on every launch). A synchronous transport may
                        // have completed already — continue if so; otherwise
                        // STOP the pass: this and the newer records wait for
                        // the next launch (order preserved, nothing
                        // skipped), and a late completion settles through
                        // the ledger after the stop.
                        ledger.MarkAckGated(entry);
                        if (ledger.SendCompletedSettleably(entry))
                        {
                            continue;
                        }

                        SafeLog(log, "shardpilot anr pass stopped: the write-ahead spool append failed and no ack has landed; " +
                                     "this and newer records retry next launch (de-duplicated server-side by aei_record_key)");
                        break;
                    }

                    // Refused outright: a gate raced shut mid-pass and
                    // nothing durable remains of the capture. STOP without
                    // advancing — this and later records retry on the next
                    // launch, and a report that did leave twice collapses
                    // server-side on aei_record_key.
                    ledger.DiscardRefused(entry);
                    SafeLog(log, "shardpilot anr pass stopped: capture refused; remaining records retry next launch");
                    break;
                }

                ledger.CompletePass();

                if (ledger.CursorSaveFailed)
                {
                    SafeLog(log, "shardpilot anr pass: cursor persist failed; settled records may replay next launch (de-duplicated server-side by aei_record_key)");
                }
            }
            catch
            {
                // The pass must never throw into SDK initialization.
            }
        }

        /// <summary>
        /// The per-pass settlement ledger — the cursor's single writer for
        /// one reporting pass. It exists because the two obvious settle
        /// moments are both wrong at the edges:
        ///
        /// - Settling a SPOOLED record the moment its write-ahead append
        ///   lands is unsafe: a LATER append in the SAME pass (several
        ///   max-trace ANRs against the capped crash spool) can evict the
        ///   earlier record before its live send completes — the cursor
        ///   would then suppress a report that no longer exists anywhere,
        ///   losing the ANR permanently. So a spooled settle is only a
        ///   CANDIDATE during the pass; it becomes final at END OF PASS,
        ///   when nothing can evict it anymore, and only if the record is
        ///   still spooled (the re-send pass covers a kill) or its send
        ///   completed meanwhile.
        /// - Settling an ack-gated record (spool append FAILED) only on a
        ///   2xx ack would let one permanently rejected report (400/401/403
        ///   — <c>Retryable=false</c>) block the pass at the same record on
        ///   every launch. A completed send SETTLES on acknowledged OR
        ///   permanently rejected, exactly the crash spool's own disposal
        ///   rule (see the client's SettleCrashReport); only RETRYABLE
        ///   failures keep the record unsettled.
        ///
        /// The cursor advances only through the highest CONTIGUOUS prefix
        /// of settle-eligible entries, in processing (ascending identity)
        /// order — an entry beyond a non-durable one stays above the cursor
        /// even when itself durable, so nothing is ever skipped; the replay
        /// this costs collapses server-side on aei_record_key. While no
        /// conditional entry is pending, the prefix advances (and persists)
        /// PER RECORD, preserving the old crash-inside-the-pass replay
        /// bound. Entries are created BEFORE their capture is dispatched so
        /// a synchronous transport completion finds its entry; completions
        /// arriving AFTER the pass (late acks) reconcile through the same
        /// prefix rule, probing the spool for entries the pass could no
        /// longer settle. All state is guarded by one leaf lock; nothing
        /// here throws into the transport callback.
        /// </summary>
        private sealed class PassSettlementLedger
        {
            internal sealed class Entry
            {
                /// <summary>The exit record this entry settles.</summary>
                public ShardPilotAeiExitRecord Record;

                /// <summary>
                /// The report's pre-stamped crash id — the write-ahead
                /// record's spool identity, probed at reconcile time.
                /// Empty for settle-without-report entries.
                /// </summary>
                public string CrashId;

                public EntryKind Kind;

                /// <summary>
                /// The send COMPLETED settleably: acknowledged (2xx) or
                /// permanently rejected (non-retryable) — the crash spool's
                /// own disposal rule. A retryable failure never sets this.
                /// </summary>
                public bool SendSettles;

                /// <summary>The cursor advanced through this entry.</summary>
                public bool Settled;
            }

            internal enum EntryKind
            {
                /// <summary>
                /// Created, capture not yet classified. Settles only via a
                /// completed send (which implies a real dispatch) — never by
                /// position, and never by a spool probe.
                /// </summary>
                Pending,

                /// <summary>Settle-eligible by itself (no durability question).</summary>
                Unconditional,

                /// <summary>
                /// Write-ahead spooled: eligible once the send completed, or
                /// at/after end of pass while the spooled copy still exists.
                /// </summary>
                Spooled,

                /// <summary>
                /// Spool append failed: eligible ONLY via a completed send.
                /// </summary>
                AckGated
            }

            private readonly object _gate = new object();
            private readonly List<Entry> _entries = new List<Entry>();
            private readonly ShardPilotAeiCursor _cursor;
            private readonly IShardPilotKeyValueStore _store;
            private readonly ShardPilotConfig _config;
            private readonly ShardPilotClient _client;
            private int _firstUnsettled;
            private bool _passComplete;
            private bool _cursorSaveFailed;

            public PassSettlementLedger(
                ShardPilotAeiCursor cursor,
                IShardPilotKeyValueStore store,
                ShardPilotConfig config,
                ShardPilotClient client)
            {
                _cursor = cursor;
                _store = store;
                _config = config;
                _client = client;
            }

            /// <summary>Any cursor persist failed (sticky) — logged once by the pass.</summary>
            public bool CursorSaveFailed
            {
                get
                {
                    lock (_gate)
                    {
                        return _cursorSaveFailed;
                    }
                }
            }

            /// <summary>
            /// Settles a record that ships nothing (foreign reason,
            /// forbidding era): unconditional, so with no conditional entry
            /// pending it advances and persists the cursor immediately —
            /// the old per-record discipline — and otherwise queues behind
            /// the blocker in order.
            /// </summary>
            public void SettleWithoutReport(ShardPilotAeiExitRecord record)
            {
                lock (_gate)
                {
                    _entries.Add(new Entry { Record = record, Kind = EntryKind.Unconditional });
                    AdvancePrefixLocked();
                }
            }

            /// <summary>
            /// Creates the entry for a record about to be captured, with the
            /// report's pre-stamped crash id. Created BEFORE the dispatch so
            /// a synchronous completion callback finds it.
            /// </summary>
            public Entry BeginReportedRecord(ShardPilotAeiExitRecord record)
            {
                var entry = new Entry
                {
                    Record = record,
                    CrashId = ShardPilotIds.NewUuidV7(),
                    Kind = EntryKind.Pending
                };
                lock (_gate)
                {
                    _entries.Add(entry);
                }

                return entry;
            }

            public void MarkSpooled(Entry entry)
            {
                Classify(entry, EntryKind.Spooled);
            }

            public void MarkUnconditional(Entry entry)
            {
                Classify(entry, EntryKind.Unconditional);
            }

            public void MarkAckGated(Entry entry)
            {
                Classify(entry, EntryKind.AckGated);
            }

            /// <summary>
            /// Removes the entry of a capture that was REFUSED outright:
            /// nothing durable remains and no completion will ever arrive
            /// (a refusal never dispatched), so the record must not hold a
            /// position in the settle order.
            /// </summary>
            public void DiscardRefused(Entry entry)
            {
                lock (_gate)
                {
                    _entries.Remove(entry);
                }
            }

            /// <summary>True when the entry's send already completed settleably (see Entry.SendSettles).</summary>
            public bool SendCompletedSettleably(Entry entry)
            {
                lock (_gate)
                {
                    return entry.SendSettles;
                }
            }

            /// <summary>
            /// The transport-completion sink for every reported record —
            /// during the pass (a synchronous transport) and after it (late
            /// acks) alike. Applies the crash spool's disposal rule:
            /// acknowledged or permanently rejected completes the entry;
            /// retryable failures leave it owed. Never throws into the
            /// transport callback.
            /// </summary>
            public void OnSendCompleted(Entry entry, ShardPilotResult result)
            {
                try
                {
                    if (result == null || (!result.Success && result.Retryable))
                    {
                        return;
                    }

                    lock (_gate)
                    {
                        entry.SendSettles = true;
                        AdvancePrefixLocked();
                    }
                }
                catch
                {
                    // A failed completion-settle degrades to a next-launch
                    // replay the server-side aei_record_key dedup absorbs.
                }
            }

            /// <summary>
            /// End of pass: nothing can evict a pass-appended record
            /// anymore, so spooled candidates whose write-ahead copy
            /// SURVIVED become settle-eligible via the spool probe, and the
            /// prefix advances one final time. Late completions after this
            /// keep reconciling through the same rule.
            /// </summary>
            public void CompletePass()
            {
                lock (_gate)
                {
                    _passComplete = true;
                    AdvancePrefixLocked();
                }
            }

            private void Classify(Entry entry, EntryKind kind)
            {
                lock (_gate)
                {
                    entry.Kind = kind;
                    AdvancePrefixLocked();
                }
            }

            /// <summary>
            /// Advances the cursor through the highest contiguous prefix of
            /// settle-eligible entries and persists per advanced entry.
            /// Eligibility: a completed send settles ANY kind; Unconditional
            /// settles by itself; Spooled additionally settles at/after end
            /// of pass while its write-ahead copy still exists (probed
            /// through the client so a mid-pass ownership handoff is
            /// followed). Pending and AckGated entries otherwise block —
            /// and everything behind a blocker waits, so a lost record is
            /// never skipped. Caller holds the gate.
            /// </summary>
            private void AdvancePrefixLocked()
            {
                while (_firstUnsettled < _entries.Count)
                {
                    var entry = _entries[_firstUnsettled];
                    var eligible = entry.SendSettles ||
                                   entry.Kind == EntryKind.Unconditional ||
                                   (entry.Kind == EntryKind.Spooled && _passComplete &&
                                    _client.CrashReportRemainsDurablySpooled(entry.CrashId));
                    if (!eligible)
                    {
                        return;
                    }

                    _cursor.AdvanceTo(entry.Record);

                    // Persisted per settled entry, not per pass: a crash
                    // inside the pass must not replay already-settled
                    // records wholesale. A failed save degrades to a
                    // next-launch replay the dedup absorbs.
                    if (!ShardPilotAeiCursor.TrySave(_store, _config, _cursor))
                    {
                        _cursorSaveFailed = true;
                    }

                    entry.Settled = true;
                    _firstUnsettled++;
                }
            }
        }

        /// <summary>
        /// Folds the scope's dark-lane suppression marker (stamped by a
        /// runtime opt-out/denial while no pass could run) into the live
        /// cursor. Two mechanisms, threaded together:
        ///
        /// - TIMESTAMP FLOOR (fast path): records at or before the marker are
        ///   already disclaimed and die silently. This is exact when the wall
        ///   clock behaved — a legit post-re-enable record is dated ABOVE the
        ///   re-enable floor and reports normally.
        /// - ARMED BACKSTOP (clock-rollback correctness): the floor alone
        ///   misses a record left in a FORWARD portion of the window — a clock
        ///   that jumped ahead mid-window then rolled back before re-enable
        ///   dates it ABOVE the floor. Such a record is dated in THIS launch's
        ///   FUTURE — impossible for a past process exit — which is the
        ///   clock-game signature. On that evidence the (raised) floor cannot
        ///   be trusted, so the armed pass settles past EVERYTHING enumerable
        ///   up to now — disclaiming the whole window regardless of clock
        ///   games. Without the evidence the armed flag adds no suppression:
        ///   the floor already bounds the window and the common case keeps
        ///   reporting legit post-re-enable records (dated above the floor, at
        ///   or below now). The armed flag only forces the CHECK to happen at
        ///   the first enabled pass, and is then cleared.
        ///
        /// Both the floor and the armed flag are cleared only once the floored
        /// cursor is durably saved; while the save fails the in-memory floor
        /// still holds for this launch and the marker retries later. Honest
        /// residual bounds: (a) a SMALL forward jump leaving the record dated
        /// at or below this launch's clock is indistinguishable from a legit
        /// post-re-enable record and is not caught; (b) an INACTIVE scope whose
        /// era file the opt-out sweep WIPED cannot have its floor raised by the
        /// re-enable (nothing to enumerate), so a plain during-window record
        /// stays at the sweep floor — the KV-only-scope residual — and
        /// the re-enable all-scope raise covers only scopes still enumerable.
        /// </summary>
        private static void ConsumeSuppressionFloorMarker(
            IShardPilotKeyValueStore store, ShardPilotConfig config, ShardPilotAeiCursor cursor,
            List<ShardPilotAeiExitRecord> records, long nowUnixMs)
        {
            var floorMs = ShardPilotAeiCursor.ReadSuppressionFloor(store, config);
            var armed = ShardPilotAeiCursor.IsSuppressionArmed(store, config);
            if (floorMs <= 0 && !armed)
            {
                return;
            }

            var settleTs = floorMs;
            if (armed && HasFutureDatedRecord(records, nowUnixMs))
            {
                // Clock-game evidence (a record dated in this launch's
                // future): settle past everything currently enumerable up to
                // now, range-checked so a corrupt out-of-range timestamp is
                // excluded (see ComputeSettleAllFloorTimestamp).
                settleTs = ComputeSettleAllFloorTimestamp(
                    floorMs > nowUnixMs ? floorMs : nowUnixMs, records);
            }

            if (settleTs <= 0)
            {
                // Armed with nothing to floor (the floor was already consumed
                // and no clock-game evidence stands): just disarm — a
                // completed enabled pass is the only thing that clears it.
                ClearConsumedSuppression(store, config);
                return;
            }

            var floorProbe = new ShardPilotAeiExitRecord
            {
                TimestampMs = settleTs,
                Reason = int.MaxValue,
                Pid = int.MaxValue,
                DefiningUid = int.MaxValue
            };

            if (!cursor.IsStrictlyOlderThan(floorProbe))
            {
                // The cursor already covers the settle point: consumed.
                ClearConsumedSuppression(store, config);
                return;
            }

            cursor.AdvanceTo(floorProbe);
            if (ShardPilotAeiCursor.TrySave(store, config, cursor))
            {
                ClearConsumedSuppression(store, config);
            }
        }

        /// <summary>
        /// True when any enumerable record is dated STRICTLY after this
        /// launch's wall clock — impossible for a past process exit, so it is
        /// the clock-rollback signature the armed backstop keys on. Corrupt
        /// out-of-range timestamps are excluded (a long.MaxValue record is
        /// handled by the reporting path's own skip, not treated as clock-game
        /// evidence).
        /// </summary>
        private static bool HasFutureDatedRecord(List<ShardPilotAeiExitRecord> records, long nowUnixMs)
        {
            if (records == null)
            {
                return false;
            }

            for (var i = 0; i < records.Count; i++)
            {
                var record = records[i];
                if (record != null &&
                    ShardPilotAnrEraStore.IsValidUnixMs(record.TimestampMs) &&
                    record.TimestampMs > nowUnixMs)
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// The settle-all floor timestamp: the max of <paramref name="baseTimestampMs"/>
        /// and every enumerable record's timestamp that PASSES the
        /// <see cref="ShardPilotAnrEraStore.IsValidUnixMs"/> range check —
        /// the SAME check the reporting path applies. An
        /// out-of-range record (a malformed long.MaxValue, a negative) is
        /// EXCLUDED, so it can never poison the floor into permanent silence;
        /// it is skipped without settling on the reporting path instead. Reads
        /// only the identity timestamp — never the lazy description/trace
        /// providers — so the "no subject-derived free text on a suppressed
        /// path" contract holds.
        /// </summary>
        private static long ComputeSettleAllFloorTimestamp(
            long baseTimestampMs, List<ShardPilotAeiExitRecord> records)
        {
            var floorTimestampMs = baseTimestampMs;
            if (records != null)
            {
                for (var i = 0; i < records.Count; i++)
                {
                    var record = records[i];
                    if (record == null || !ShardPilotAnrEraStore.IsValidUnixMs(record.TimestampMs))
                    {
                        continue;
                    }

                    if (record.TimestampMs > floorTimestampMs)
                    {
                        floorTimestampMs = record.TimestampMs;
                    }
                }
            }

            return floorTimestampMs;
        }

        /// <summary>Clears both halves of a consumed dark-window marker (floor + armed).</summary>
        private static void ClearConsumedSuppression(IShardPilotKeyValueStore store, ShardPilotConfig config)
        {
            ShardPilotAeiCursor.ClearSuppressionFloor(store, config);
            ShardPilotAeiCursor.ClearSuppressionArmed(store, config);
        }

        /// <summary>
        /// The opt-out/denial suppression — CONSUME WITHOUT REPORTING: the
        /// cursor is floored at (max(now, newest readable record), max, max,
        /// max) so everything that already exists is settled permanently.
        /// The enumeration is read for its IDENTITY TUPLES ONLY — the
        /// newest timestamp decides the floor; the records' lazy
        /// description and trace providers are NEVER invoked here, so an
        /// opted-out/denied launch pulls no free-text exit description and
        /// opens no trace stream (nothing subject-derived enters managed
        /// memory), no event is built, nothing ships. The full enumeration
        /// matters because a wall clock corrected BACKWARD between the kill
        /// and this launch can leave records future-dated relative to
        /// <paramref name="nowUnixMs"/>, and a now-only floor would let
        /// that "suppressed" backlog resurface after a later re-enable. A
        /// record produced AFTER a re-enable is strictly newer than the
        /// floor and reports normally. The era snapshots are wiped
        /// alongside (fail-closed owed marker when the file survives).
        /// Honest bound, stated: if the floor's persist fails, suppression
        /// holds in-memory for this launch and the era wipe (or its owed
        /// marker) still destroys the actor inputs — a later launch could
        /// then report the stale records device-scoped ANONYMOUS at worst,
        /// never as the revoked subject.
        /// </summary>
        internal static void ApplySuppression(
            ShardPilotConfig config,
            IShardPilotAeiSource source,
            IShardPilotKeyValueStore store,
            ShardPilotAnrEraStore eraStore,
            long nowUnixMs,
            Action<string> log)
        {
            // A dark-lane revocation marker raises the floor too: the
            // revocation's own moment was stamped under a possibly LATER
            // clock than this launch's (a rollback in between), and the
            // disclaimed window must stay covered either way.
            var floorTimestampMs = nowUnixMs;
            var suppressionMarkerMs = ShardPilotAeiCursor.ReadSuppressionFloor(store, config);
            if (suppressionMarkerMs > floorTimestampMs)
            {
                floorTimestampMs = suppressionMarkerMs;
            }

            if (source != null)
            {
                List<ShardPilotAeiExitRecord> records;
                try
                {
                    records = source.ReadExitRecords();
                }
                catch
                {
                    records = null;
                }

                // Identity fields only — by contract the providers
                // (description, trace) stay uninvoked on this path. The
                // range check EXCLUDES an out-of-range record: a
                // malformed long.MaxValue timestamp must never poison the
                // floor into permanent silence — it is skipped without
                // settling on the reporting path instead (corrupt-
                // record handling).
                floorTimestampMs = ComputeSettleAllFloorTimestamp(floorTimestampMs, records);
            }

            var existing = ShardPilotAeiCursor.Load(store, config);
            var floor = new ShardPilotAeiCursor
            {
                TimestampMs = floorTimestampMs,
                Reason = int.MaxValue,
                Pid = int.MaxValue,
                DefiningUid = int.MaxValue
            };

            // Only ever move FORWARD: a device clock sitting behind the
            // cursor's last advance must not regress the high-water mark.
            var floorProbe = new ShardPilotAeiExitRecord
            {
                TimestampMs = floor.TimestampMs,
                Reason = floor.Reason,
                Pid = floor.Pid,
                DefiningUid = floor.DefiningUid
            };
            if (!existing.IsStrictlyOlderThan(floorProbe))
            {
                // The cursor already covers the whole floor (the marker
                // included): the marker is consumed.
                ShardPilotAeiCursor.ClearSuppressionFloor(store, config);
            }
            else if (ShardPilotAeiCursor.TrySave(store, config, floor))
            {
                // The durable floor is at or above the marker by
                // construction — consumed. A FAILED save keeps the marker:
                // suppression then holds in-memory for this launch and the
                // marker retries on a later pass.
                ShardPilotAeiCursor.ClearSuppressionFloor(store, config);
            }

            if (eraStore != null)
            {
                eraStore.WipeAll();
            }
            else
            {
                ShardPilotAnrEraStore.WipeScope(config, store, log);
            }

            SafeLog(log, "shardpilot anr pass suppressed: crash reporting is opted out/denied; backlog settled, era snapshots wiped");
        }

        /// <summary>
        /// Builds the kind-"anr" event for one exit record. Trace-carrying
        /// when the bounded stream read yields text; otherwise the ADR's
        /// ONE degraded rule: a missing stream — the API-30 no-stream shape
        /// or an API-31+ NULL from an overwritten buffer — produces the
        /// SAME metadata-only report, never a drop or a parse failure.
        /// Fatal: the system killed the process. Attribution rides the era
        /// snapshot (empty/absent = device-scoped anonymous), and the
        /// current session's breadcrumbs are suppressed — they belong to
        /// the WRONG era.
        /// </summary>
        internal static ShardPilotCrashEvent BuildExitRecordEvent(
            ShardPilotAeiExitRecord record,
            ShardPilotAnrEraRecord era)
        {
            string trace = null;
            var reader = record.TraceTextReader;
            if (reader != null)
            {
                try
                {
                    trace = reader(ShardPilotAnrIds.MaxAnrTraceBytes);
                }
                catch
                {
                    trace = null;
                }
            }

            // The ONLY place the record's free-text description is pulled:
            // a record that never reaches this builder (suppressed launch,
            // foreign reason, forbidding era, capped-out) never
            // materializes it. A failed read is an empty reason — never a
            // dropped report.
            string description = null;
            var descriptionReader = record.DescriptionReader;
            if (descriptionReader != null)
            {
                try
                {
                    description = descriptionReader();
                }
                catch
                {
                    description = null;
                }
            }

            var degraded = string.IsNullOrEmpty(trace);
            var truncated = false;
            if (!degraded)
            {
                var bounded = ShardPilotAnrIds.BoundRawText(trace);
                truncated = bounded.Length < trace.Length;
                trace = bounded;
            }

            var context = new Dictionary<string, string>
            {
                {"anr_source", "application_exit_info"}
            };
            if (degraded)
            {
                context["anr_degraded"] = "no_trace_stream";
            }

            if (truncated)
            {
                context["anr_trace_truncated"] = "true";
            }

            return new ShardPilotCrashEvent
            {
                CrashType = "ANR",
                ExceptionReason = FirstLineBounded(description),
                Fatal = true,
                OccurredAtUtc = ShardPilotAnrEraStore.FromUnixMs(record.TimestampMs),
                ThreadState = "main",
                Kind = ShardPilotAnrIds.AnrKind,
                AeiRecordKey = ShardPilotAnrIds.AeiRecordKeyFor(
                    record.TimestampMs, record.Reason, record.Pid, record.DefiningUid),
                RawText = degraded ? string.Empty : trace,
                UseEraAttribution = true,
                EraAnonymousId = era == null ? string.Empty : era.AnonymousId,
                // The CRASHED process's build, from the attributing era's
                // snapshot — the exit predates this launch, so the current
                // config's values would be wrong after an app update. No
                // era (device-scoped anonymous) or a pre-upgrade era line
                // = empty: the report carries NO version rather than a
                // borrowed one, and CaptureCrash's config backfill is
                // gated off for era-attributed events for the same reason.
                AppVersion = era == null ? string.Empty : era.AppVersion,
                BuildId = era == null ? string.Empty : era.BuildId,
                OmitBreadcrumbs = true,
                Context = context
            };
        }

        /// <summary>
        /// The bounded tombstone read for one REASON_CRASH_NATIVE record:
        /// invokes the record's lazy binary reader with the lane's 4 MiB
        /// cap, then RE-ASSERTS the cap on whatever came back (an
        /// ill-behaved reader can never push over-cap bytes onward). Null =
        /// no stream — the API-30 shape (null delegate), the API-31+
        /// overwritten-buffer NULL, an empty stream, or any read failure —
        /// which degrades to the metadata-only report. An over-cap result
        /// carries NO bytes: skipped, never truncated (a truncated
        /// proto is unparseable).
        /// </summary>
        internal static ShardPilotAeiTombstoneRead ReadTombstoneBytes(ShardPilotAeiExitRecord record)
        {
            var reader = record.TraceBytesReader;
            if (reader == null)
            {
                return null;
            }

            ShardPilotAeiTombstoneRead read;
            try
            {
                read = reader(ShardPilotTombstoneUpload.MaxTombstoneBytes);
            }
            catch
            {
                return null;
            }

            if (read == null)
            {
                return null;
            }

            if (read.OverCap)
            {
                return new ShardPilotAeiTombstoneRead { OverCap = true };
            }

            var bytes = read.Bytes;
            if (bytes == null || bytes.Length == 0)
            {
                return null;
            }

            if (bytes.Length > ShardPilotTombstoneUpload.MaxTombstoneBytes)
            {
                // Reporter-side hard cap: the bytes crossed the seam but must
                // never reach the wire — whole-or-nothing, so over-cap means
                // the degraded path, not a truncation.
                return new ShardPilotAeiTombstoneRead { OverCap = true };
            }

            return read;
        }

        /// <summary>
        /// Builds the metadata ENVELOPE event for a raw-tombstone upload:
        /// the same envelope fields the AEI JSON reports carry (era-frozen
        /// attribution, the crashed era's app version/build, the
        /// deterministic aei_record_key, the exit description as the
        /// exception reason) and NOTHING stack-bearing — threads, frames,
        /// modules, and raw_text are the SERVER's to derive from the
        /// tombstone; the serializer omits them and
        /// the server would refuse them. Kind stays empty: native crash is
        /// the base <c>crash</c> kind.
        /// </summary>
        internal static ShardPilotCrashEvent BuildTombstoneMetadataEvent(
            ShardPilotAeiExitRecord record,
            ShardPilotAnrEraRecord era)
        {
            return BuildNativeCrashEventCore(record, era, null);
        }

        /// <summary>
        /// Builds the DEGRADED metadata-only report for a native-crash
        /// record without shippable tombstone bytes — the
        /// degraded-transport rule: a normal base-kind crash JSON event on
        /// the EXISTING <c>/api/v1/crashes/ingest</c> lane (exit metadata,
        /// no frames, no raw bytes), never a drop. <paramref name="overCap"/>
        /// distinguishes the over-cap skip from the missing-stream shapes in
        /// the context marker.
        /// </summary>
        internal static ShardPilotCrashEvent BuildNativeCrashDegradedEvent(
            ShardPilotAeiExitRecord record,
            ShardPilotAnrEraRecord era,
            bool overCap)
        {
            return BuildNativeCrashEventCore(record, era, overCap ? "tombstone_over_cap" : "no_trace_stream");
        }

        private static ShardPilotCrashEvent BuildNativeCrashEventCore(
            ShardPilotAeiExitRecord record,
            ShardPilotAnrEraRecord era,
            string degradedMarker)
        {
            // The reporting path is the only place the record's free-text
            // description is pulled (same contract as the ANR builder): a
            // failed read is an empty reason, never a dropped report.
            string description = null;
            var descriptionReader = record.DescriptionReader;
            if (descriptionReader != null)
            {
                try
                {
                    description = descriptionReader();
                }
                catch
                {
                    description = null;
                }
            }

            var context = new Dictionary<string, string>
            {
                {"native_crash_source", "application_exit_info"}
            };
            if (!string.IsNullOrEmpty(degradedMarker))
            {
                context["native_crash_degraded"] = degradedMarker;
            }

            return new ShardPilotCrashEvent
            {
                CrashType = "NativeCrash",
                ExceptionReason = FirstLineBounded(description),
                Fatal = true,
                OccurredAtUtc = ShardPilotAnrEraStore.FromUnixMs(record.TimestampMs),
                // The tombstone lane is Android-only (the server's closed
                // platform slug); the degraded JSON twin carries the same.
                Platform = "android",
                // Kind stays EMPTY — the base "crash" kind is an absent
                // field on the JSON wire and the tombstone lane's implied
                // kind; only "anr" is ever stamped client-side.
                Kind = string.Empty,
                AeiRecordKey = ShardPilotAnrIds.AeiRecordKeyFor(
                    record.TimestampMs, record.Reason, record.Pid, record.DefiningUid),
                RawText = string.Empty,
                UseEraAttribution = true,
                EraAnonymousId = era == null ? string.Empty : era.AnonymousId,
                // The CRASHED process's build, from the attributing era's
                // snapshot — empty (no era / pre-upgrade line) ships as
                // unknown rather than borrowing this launch's config.
                AppVersion = era == null ? string.Empty : era.AppVersion,
                BuildId = era == null ? string.Empty : era.BuildId,
                OmitBreadcrumbs = true,
                Context = context
            };
        }

        private static int CompareRecords(ShardPilotAeiExitRecord left, ShardPilotAeiExitRecord right)
        {
            if (left.TimestampMs != right.TimestampMs)
            {
                return left.TimestampMs.CompareTo(right.TimestampMs);
            }

            if (left.Reason != right.Reason)
            {
                return left.Reason.CompareTo(right.Reason);
            }

            if (left.Pid != right.Pid)
            {
                return left.Pid.CompareTo(right.Pid);
            }

            return left.DefiningUid.CompareTo(right.DefiningUid);
        }

        private static string FirstLineBounded(string value)
        {
            value = ShardPilotConfig.Trim(value);
            if (string.IsNullOrEmpty(value))
            {
                return string.Empty;
            }

            var newline = value.IndexOfAny(new[] { '\r', '\n' });
            if (newline >= 0)
            {
                value = value.Substring(0, newline);
            }

            return value.Length > MaxReasonDescriptionChars
                ? value.Substring(0, MaxReasonDescriptionChars)
                : value;
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
                // Logging must never change pass semantics.
            }
        }
    }
}
