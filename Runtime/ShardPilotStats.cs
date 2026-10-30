using System.Threading;

namespace ShardPilot
{
#nullable enable annotations
    public sealed class ShardPilotStats
    {
        private static readonly System.Collections.Generic.IReadOnlyDictionary<string, ulong> EmptyDrops =
            new System.Collections.ObjectModel.ReadOnlyDictionary<string, ulong>(new System.Collections.Generic.Dictionary<string, ulong>());
        public ulong Enqueued { get; internal set; }
        public ulong Dropped { get; internal set; }
        public ulong Published { get; internal set; }
        public ulong FailedBatches { get; internal set; }
        public ulong Accepted { get; internal set; }
        public ulong Rejected { get; internal set; }
        public ulong Duplicates { get; internal set; }
        /// <summary>
        /// Most recent non-accepted per-event verdict from a successful batch,
        /// rendered as status or status:code with credentials redacted. Empty
        /// before the first such verdict; accepted-only batches do not clear it.
        /// This is client-instance diagnostic history, independent of logging.
        /// </summary>
        public string LastEventIssue { get; internal set; } = string.Empty;
        /// <summary>Terminal pre-queue experiment drops by code; separate from Dropped.</summary>
        public System.Collections.Generic.IReadOnlyDictionary<string, ulong> ExperimentExposureDrops { get; internal set; } = EmptyDrops;
        public System.Collections.Generic.IReadOnlyDictionary<string, ulong> ExperimentOutcomeDrops { get; internal set; } = EmptyDrops;

        /// <summary>
        /// Crash reports successfully POSTed to the dedicated crash
        /// ingest endpoint. Distinct from analytics
        /// <see cref="Published"/> — crashes ride a separate endpoint.
        /// </summary>
        public ulong CrashesSent { get; internal set; }

        /// <summary>
        /// Events accepted while the in-memory queue was full and persisted
        /// directly to the offline spool. They are not queued in memory; they
        /// ship on the NEXT LAUNCH through the spool's normal restore path.
        /// Also counted in <see cref="Enqueued"/>.
        /// </summary>
        public ulong SpooledOverflow { get; internal set; }

        /// <summary>Analytics events dropped because their local retry age expired.</summary>
        public ulong SpoolExpired { get; internal set; }

        /// <summary>
        /// Crash reports persisted to the durable crash spool at capture
        /// time. A spooled report is removed once the crash ingest service
        /// acknowledges it (this launch or a later one), so this counts
        /// captures made durable — not reports still pending.
        /// </summary>
        public ulong CrashesSpooled { get; internal set; }

        /// <summary>
        /// Crash reports the crash spool could not keep: the oldest reports
        /// evicted when a full spool accepted a newer one, plus reports
        /// rejected outright (oversized, or the file write failed). These
        /// reports lost DURABILITY only — the capture-time send attempt was
        /// still made; what this counts is reports that will not survive to
        /// a next-launch re-send.
        /// </summary>
        public ulong CrashSpoolDropped { get; internal set; }

        /// <summary>Consent receipts acknowledged (2xx) by the consent endpoint.</summary>
        public ulong ConsentRecorded { get; internal set; }

        /// <summary>
        /// Consent-receipt dispatch failures — transport errors, non-2xx
        /// responses, and decision-time sends that could not resolve a
        /// bearer. The failing code is kept in <see cref="LastConsentError"/>;
        /// retryable failures leave the receipt retained in the durable
        /// outbox and re-attempt at the next dispatch point.
        /// </summary>
        public ulong ConsentFailed { get; internal set; }

        /// <summary>
        /// Failed durable persists of the consent DECISION record itself
        /// (the identity/consent state store). The decision still applies in
        /// memory and the receipt still sends; call SetConsent again to
        /// retry persistence.
        /// </summary>
        public ulong ConsentPersistFailed { get; internal set; }

        /// <summary>
        /// Receipts evicted OLDEST-first when the 32-entry consent outbox
        /// cap was exceeded on a successful durable write (a FAILED write
        /// never evicts).
        /// </summary>
        public ulong ConsentOutboxEvicted { get; internal set; }

        /// <summary>
        /// Failed durable writes of the consent-receipt outbox record. The
        /// in-memory mirror keeps ruling the process (retained receipts
        /// still deliver) and the owed write retries at every consent
        /// dispatch point, in the pause/quit persist, and at shutdown.
        /// </summary>
        public ulong ConsentOutboxPersistFailed { get; internal set; }

        /// <summary>
        /// Closes that tore down at their deadline with consent writes still
        /// owed: a retained receipt, an owed downgrade or a denial's witness
        /// that storage never accepted. The close answered
        /// <c>spool_persist_failed</c>; a standing denial stays in force in
        /// memory for the rest of the process.
        /// </summary>
        public ulong ConsentWritesUnpersistedAtShutdown { get; internal set; }

        /// <summary>
        /// The most recent consent-plane error: a receipt dispatch failure
        /// (e.g. <c>http_401</c>, <c>http_0</c>, <c>token_unavailable</c>),
        /// or <c>consent_outbox_persist_failed</c> when a receipt armed
        /// WITHOUT a SetConsent caller (a propagated sibling or late
        /// registrant arming its own scope's grant receipt) could not be
        /// durably persisted — the deciding client surfaces that same code
        /// as SetConsent's return instead. Empty when nothing has failed.
        /// </summary>
        public string? LastConsentError { get; internal set; }

        public string? LastError { get; internal set; }
    }

    public sealed class ShardPilotResult
    {
        public static readonly ShardPilotResult Ok = new ShardPilotResult(true, null, false);

        public bool Success { get; private set; }
        public string? Error { get; private set; }
        public bool Retryable { get; private set; }

        /// <summary>Terminal rejections observed for this flush/close's own events while pending.
        /// Success still describes transport completion, not per-event acceptance.</summary>
        public ulong Rejected { get; private set; }

        /// <summary>Most recent rejections from this operation, oldest first.
        /// Bounded by RejectionCapacity; Rejected counts even entries evicted.</summary>
        public System.Collections.Generic.IReadOnlyList<ShardPilotRejection> Rejections { get; private set; } =
            System.Array.AsReadOnly(new ShardPilotRejection[0]);

        internal ShardPilotResult WithRejections(ulong count,
            System.Collections.Generic.IReadOnlyList<ShardPilotRejection> entries)
        {
            return new ShardPilotResult(Success, Error, Retryable, RetryAfter, Suppressed, Warnings)
            {
                Rejected = count,
                Rejections = entries
            };
        }

        /// <summary>
        /// The minimum delay before the next publish attempt, taken from the
        /// server's whole-seconds <c>Retry-After</c> header on a 429. Zero when
        /// the server gave no hint, in which case the worker falls back to its
        /// own flush-interval backoff.
        /// </summary>
        public System.TimeSpan RetryAfter { get; private set; }

        /// <summary>
        /// True when the server accepted the request (2xx) but did NOT store the crash
        /// because the actor withheld consent. <see cref="Success"/> is still true in this
        /// case, so a caller that needs delivery-and-store confirmation must check this.
        /// </summary>
        public bool Suppressed { get; private set; }

        /// <summary>
        /// Non-fatal processing or local durability notices. Never null.
        /// </summary>
        public System.Collections.Generic.IReadOnlyList<string> Warnings { get; private set; }

        public ShardPilotResult(bool success, string? error, bool retryable)
            : this(success, error, retryable, System.TimeSpan.Zero)
        {
        }

        public ShardPilotResult(bool success, string? error, bool retryable, System.TimeSpan retryAfter)
            : this(success, error, retryable, retryAfter, false, null)
        {
        }

        internal ShardPilotResult(bool success, string? error, bool retryable, System.TimeSpan retryAfter,
            bool suppressed, System.Collections.Generic.IReadOnlyList<string>? warnings)
        {
            Success = success;
            Error = error;
            Retryable = retryable;
            RetryAfter = retryAfter < System.TimeSpan.Zero ? System.TimeSpan.Zero : retryAfter;
            Suppressed = suppressed;
            // Inline empty (not a shared static): the static `Ok` field initializes before
            // any other static would, so a shared empty-array field would still be null here.
            Warnings = warnings ?? new string[0];
        }
    }

    /// <summary>
    /// The <see cref="ShardPilotResult.Error"/> codes a <c>FlushAsync</c> answers
    /// with when it does not succeed. A flush answers for the whole drain: it
    /// waits through retryable failures until everything queued before it is
    /// delivered (success) or its deadline passes. Events the client removes on
    /// its own (a consent withdrawal's purge, an expired retry age) count as
    /// delivered. A terminal ingest rejection answers at once with its own
    /// message and <see cref="ShardPilotResult.Retryable"/> false.
    /// </summary>
    public static class ShardPilotFlushCodes
    {
        /// <summary>The deadline passed with work still undelivered. Retryable: the
        /// client keeps retrying it in the background.</summary>
        public const string Pending = "pending";

        /// <summary>The undelivered remainder could not be written to the offline
        /// spool at close. Not answered by a plain flush.</summary>
        public const string SpoolPersistFailed = "spool_persist_failed";

        /// <summary>The deadline passed while events waited behind a consent receipt
        /// that has not been delivered yet. Retryable.</summary>
        public const string ConsentReceiptPending = "consent_receipt_pending";

        /// <summary>The <c>ShardPilotSdk</c> facade has no client.</summary>
        public const string NotInitialized = "not_initialized";

        /// <summary>The client is closed, or closed before the flush's work was
        /// delivered.</summary>
        public const string Shutdown = "shutdown";
    }

#nullable restore annotations
    internal sealed class ShardPilotStatsCollector
    {
        private long _enqueued;
        private long _dropped;
        private long _published;
        private long _failedBatches;
        private long _accepted;
        private long _rejected;
        private long _duplicates;
        private readonly System.Collections.Generic.Dictionary<string, ulong> _experimentExposureDrops = new System.Collections.Generic.Dictionary<string, ulong>();
        private readonly System.Collections.Generic.Dictionary<string, ulong> _experimentOutcomeDrops = new System.Collections.Generic.Dictionary<string, ulong>();
        private long _crashesSent;
        private long _spooledOverflow;
        private long _spoolExpired;
        private long _crashesSpooled;
        private long _crashSpoolDropped;
        private long _consentRecorded;
        private long _consentFailed;
        private long _consentPersistFailed;
        private long _consentOutboxEvicted;
        private long _consentOutboxPersistFailed;
        private long _consentWritesUnpersistedAtShutdown;
        private string _lastConsentError = string.Empty;
        private string _lastError = string.Empty;
        private string _lastEventIssue = string.Empty;
        private readonly object _gate = new object();
        internal System.Func<string, string> DiagnosticRedactor;
        private string Diagnostic(string value) => DiagnosticRedactor == null ? value : DiagnosticRedactor(value);

        public ShardPilotStats Snapshot()
        {
            lock (_gate)
            {
                return new ShardPilotStats
                {
                    Enqueued = (ulong)Interlocked.Read(ref _enqueued),
                    Dropped = (ulong)Interlocked.Read(ref _dropped),
                    Published = (ulong)Interlocked.Read(ref _published),
                    FailedBatches = (ulong)Interlocked.Read(ref _failedBatches),
                    Accepted = (ulong)Interlocked.Read(ref _accepted),
                    Rejected = (ulong)Interlocked.Read(ref _rejected),
                    Duplicates = (ulong)Interlocked.Read(ref _duplicates),
                    LastEventIssue = _lastEventIssue,
                    ExperimentExposureDrops = new System.Collections.ObjectModel.ReadOnlyDictionary<string, ulong>(new System.Collections.Generic.Dictionary<string, ulong>(_experimentExposureDrops)),
                    ExperimentOutcomeDrops = new System.Collections.ObjectModel.ReadOnlyDictionary<string, ulong>(new System.Collections.Generic.Dictionary<string, ulong>(_experimentOutcomeDrops)),
                    CrashesSent = (ulong)Interlocked.Read(ref _crashesSent),
                    SpooledOverflow = (ulong)Interlocked.Read(ref _spooledOverflow),
                    SpoolExpired = (ulong)Interlocked.Read(ref _spoolExpired),
                    CrashesSpooled = (ulong)Interlocked.Read(ref _crashesSpooled),
                    CrashSpoolDropped = (ulong)Interlocked.Read(ref _crashSpoolDropped),
                    ConsentRecorded = (ulong)Interlocked.Read(ref _consentRecorded),
                    ConsentFailed = (ulong)Interlocked.Read(ref _consentFailed),
                    ConsentPersistFailed = (ulong)Interlocked.Read(ref _consentPersistFailed),
                    ConsentOutboxEvicted = (ulong)Interlocked.Read(ref _consentOutboxEvicted),
                    ConsentOutboxPersistFailed = (ulong)Interlocked.Read(ref _consentOutboxPersistFailed),
                    ConsentWritesUnpersistedAtShutdown = (ulong)Interlocked.Read(ref _consentWritesUnpersistedAtShutdown),
                    LastConsentError = _lastConsentError,
                    LastError = _lastError
                };
            }
        }

        public void RecordEnqueued()
        {
            Interlocked.Increment(ref _enqueued);
        }

        public void RecordExperimentDrop(string name, string code, string message)
        {
            lock (_gate)
            {
                var drops = name == "experiment_outcome" ? _experimentOutcomeDrops : _experimentExposureDrops;
                ulong count; drops.TryGetValue(code, out count); drops[code] = count + 1;
                _lastError = Diagnostic(message);
            }
        }

        public void RecordSpoolExpired()
        {
            Interlocked.Increment(ref _spoolExpired);
            RecordRefusal("spool_expired");
        }

        public void RecordDropped(int count)
        {
            if (count > 0)
            {
                Interlocked.Add(ref _dropped, count);
            }
        }

        /// <summary>
        /// Records a policy refusal — an event or crash dropped at the gate
        /// because consent/opt-out did not allow it. Counts one Dropped and
        /// surfaces the distinct refusal code (e.g. <c>consent_unknown</c>,
        /// <c>consent_denied</c>, <c>crash_disabled</c>) via
        /// <see cref="ShardPilotStats.LastError"/> WITHOUT counting a failed
        /// batch — nothing was attempted, so nothing failed.
        /// </summary>
        public void RecordRefusal(string reason)
        {
            Interlocked.Increment(ref _dropped);
            lock (_gate)
            {
                _lastError = Diagnostic(reason);
            }
        }

        public void RecordBatch(ShardPilotBatchResult result, int size)
        {
            Interlocked.Add(ref _published, size);
            Interlocked.Add(ref _accepted, result.Accepted);
            Interlocked.Add(ref _rejected, result.Rejected);
            Interlocked.Add(ref _duplicates, result.Duplicates);
        }

        public void RecordEventIssue(string issue)
        {
            lock (_gate)
            {
                _lastEventIssue = Diagnostic(issue ?? string.Empty);
            }
        }

        public void RecordCrashSent()
        {
            Interlocked.Increment(ref _crashesSent);
        }

        public void RecordSpooledOverflow()
        {
            Interlocked.Increment(ref _spooledOverflow);
        }

        public void RecordCrashSpooled()
        {
            Interlocked.Increment(ref _crashesSpooled);
        }

        public void RecordCrashSpoolDropped(int count)
        {
            if (count > 0)
            {
                Interlocked.Add(ref _crashSpoolDropped, count);
            }
        }

        public void RecordFailure(string error)
        {
            Interlocked.Increment(ref _failedBatches);
            lock (_gate)
            {
                _lastError = Diagnostic(error);
            }
        }

        /// <summary>A consent receipt was acknowledged (2xx) by the consent endpoint.</summary>
        public void RecordConsentRecorded()
        {
            Interlocked.Increment(ref _consentRecorded);
        }

        /// <summary>
        /// A consent-receipt dispatch failed (or could not start): counts
        /// one failure and keeps the code as the last consent error, without
        /// touching the event-batch failure counters — the consent plane's
        /// bookkeeping is independent of the events plane's.
        /// </summary>
        public void RecordConsentFailed(string error)
        {
            Interlocked.Increment(ref _consentFailed);
            lock (_gate)
            {
                _lastConsentError = Diagnostic(error ?? string.Empty);
            }
        }

        public void RecordConsentPersistFailed()
        {
            Interlocked.Increment(ref _consentPersistFailed);
        }

        public void RecordConsentOutboxEvicted(int count)
        {
            if (count > 0)
            {
                Interlocked.Add(ref _consentOutboxEvicted, count);
            }
        }

        public void RecordConsentOutboxPersistFailed()
        {
            Interlocked.Increment(ref _consentOutboxPersistFailed);
        }

        public void RecordConsentWritesUnpersistedAtShutdown()
        {
            Interlocked.Increment(ref _consentWritesUnpersistedAtShutdown);
        }

        /// <summary>
        /// Stamps <see cref="ShardPilotStats.LastConsentError"/> WITHOUT
        /// moving any counter. Used by consent-plane failures that have no
        /// caller to hand an error code to — a propagated sibling / late
        /// registrant arming its own-scope receipt surfaces
        /// <c>consent_outbox_persist_failed</c> here, where the deciding
        /// client surfaces the same code as <c>SetConsent</c>'s return (the
        /// counter side is already accounted where the failure happened, so
        /// counting again here would double-book it).
        /// </summary>
        public void RecordLastConsentError(string error)
        {
            lock (_gate)
            {
                _lastConsentError = Diagnostic(error ?? string.Empty);
            }
        }
    }
}
