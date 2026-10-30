using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace ShardPilot
{
    internal sealed partial class ShardPilotExperiments
    {
        internal delegate bool SealedFactEmitter(ShardPilotSealedExperimentFact fact, string sessionId,
            string anonymousId, string sessionMarker, long consentEpoch, out string code);
        private readonly SealedFactEmitter _emitSealed;
        private readonly Dictionary<string, OwedExposure> _lastApplications = new Dictionary<string, OwedExposure>(StringComparer.Ordinal);
        private readonly Dictionary<string, OwedExposure> _appliedTuples = new Dictionary<string, OwedExposure>(StringComparer.Ordinal);
        private string _applicationSession;
        private readonly HashSet<string> _terminalAssignments = new HashSet<string>(StringComparer.Ordinal);

        public string ApplyExperimentVariant(string experimentKey)
        {
            ShardPilotExperimentEntry entry; string code;
            return RecordApplication(experimentKey, false, out entry, out code) ? entry.VariantKey : null;
        }

        public IReadOnlyDictionary<string, object> ApplyExperimentPayload(string experimentKey)
        {
            ShardPilotExperimentEntry entry; string code;
            return RecordApplication(experimentKey, false, out entry, out code)
                ? ShardPilotExperimentsLogic.CopyPayload(entry.VariantPayload) : null;
        }

        private static string NewOccurrence()
        {
            var bytes = new byte[16];
            using (var random = RandomNumberGenerator.Create()) random.GetBytes(bytes);
            var value = new StringBuilder(32);
            foreach (var b in bytes) value.Append(b.ToString("x2", CultureInfo.InvariantCulture));
            return value.ToString();
        }

        private void ApplicationSessionLocked()
        {
            var session = CurrentAnalyticsSessionId();
            if (string.Equals(session, _applicationSession, StringComparison.Ordinal)) return;
            _applicationSession = session;
            _lastApplications.Clear();
            _appliedTuples.Clear();
        }

        private bool RecordApplication(string key, bool additional, out ShardPilotExperimentEntry entry, out string code)
        {
            entry = null;
            key = ShardPilotConfig.Trim(key);
            if (string.IsNullOrEmpty(key)) { code = "experiment_key_required"; return false; }
            var consentEpoch = ConsentEpochNow();
            lock (_lock) if (!ReadApplicationEntryLocked(key, out entry, out code)) return false;
            if (RefuseReplacedFact(out code) || !AdmitCurrentFact(out code)) return false;
            lock (_lock)
            {
                code = ConsentRefusal(_consent());
                if (code != null || ConsentEpochNow() != consentEpoch) { code = code ?? "consent_changed"; return false; }
                if (!ReadApplicationEntryLocked(key, out entry, out code)) return false;
                var subject = entry.SubjectKey;
                ApplicationSessionLocked();
                if (string.IsNullOrEmpty(_applicationSession)) { code = "session_start_pending"; return false; }
                var tuple = ShardPilotExperimentsLogic.ExposureTuple(key, entry);
                if (!additional && _appliedTuples.ContainsKey(tuple)) return true;
                // The assignment object is immutable after install except its ownership metadata.
                // Capture every request pin and targeting attribute before returning to gameplay.
                var capture = new OwedExposure
                {
                    Entry = entry, Session = _sessionMarker, SessionId = _applicationSession,
                    AnonymousId = CurrentAnonymousId(), ReadGeneration = _withdrawalGeneration,
                    CaptureOrder = NextApplicationOrder(),
                    Application = new ShardPilotExperimentApplication
                    {
                        ExperimentKey = key, VariantKey = entry.VariantKey, SubjectKey = subject,
                        ExperimentVersion = entry.Version, ServedRevision = entry.ServedRevision,
                        ServedKillGate = entry.ServedKillGate, ServedAt = entry.ServedAt,
                        AppliedAt = ShardPilotJson.FormatTimestamp(NowUtc()), OccurrenceId = NewOccurrence(),
                        Attributes = entry.Attributes == null ? null : new List<KeyValuePair<string, string>>(entry.Attributes)
                    }
                };
                _lastApplications[key] = capture;
                if (!additional) _appliedTuples[tuple] = capture;
                QueueApplicationLocked(key, capture);
                return true;
            }
        }

        private bool ReadApplicationEntryLocked(string key, out ShardPilotExperimentEntry entry, out string code)
        {
            entry = null;
            code = ConsentRefusal(_consent());
            if (code != null) return false;
            if (_config.Source != ShardPilotSource.Client) { code = "client_source_required"; return false; }
            if (_automaticLaneDisowned) { code = ReplacedCode; return false; }
            if (_closingApplications) { code = "shutdown"; return false; }
            var subject = CurrentSubjectId();
            entry = subject == null ? null : LoadServableEntryLocked(ScopeFor(subject), key);
            if (_tornDown || _authBlocked || entry == null) { code = "no_assignment"; return false; }
            if (entry.AssignmentUnit != "client_id" || entry.ServedAt == null || entry.ServedKillGate)
            { code = "application_pins_required"; return false; }
            return true;
        }

        private bool ReadLastApplicationLocked(string key, out OwedExposure last)
        {
            ApplicationSessionLocked();
            last = null;
            return !_tornDown && !_closingApplications && !_automaticLaneDisowned && !_authBlocked && !_declaring.ContainsKey(key) &&
                _lastApplications.TryGetValue(key, out last) && CurrentSubjectId() == last.Entry.SubjectKey &&
                ApplicationAllowedLocked(key, last);
        }

        private bool RecordOutcome(string key, string outcomeKey, double value, out string code)
        {
            code = null;
            if (double.IsNaN(value) || double.IsInfinity(value) || Math.Truncate(value) != value || Math.Abs(value) > 9007199254740991d)
            { code = "invalid_outcome_value"; return false; }
            key = ShardPilotConfig.Trim(key); outcomeKey = ShardPilotConfig.Trim(outcomeKey);
            if (string.IsNullOrEmpty(key)) { code = "experiment_key_required"; return false; }
            if (string.IsNullOrEmpty(outcomeKey)) { code = "invalid_outcome_key"; return false; }
            var consentEpoch = ConsentEpochNow();
            lock (_lock)
            {
                code = ConsentRefusal(_consent());
                if (code != null) return false;
                if (_automaticLaneDisowned) { code = ReplacedCode; return false; }
                OwedExposure last;
                if (!ReadLastApplicationLocked(key, out last)) { code = "no_application"; return false; }
            }
            if (RefuseReplacedFact(out code) || !AdmitCurrentFact(out code)) return false;
            lock (_lock)
            {
                code = ConsentRefusal(_consent());
                if (code != null || ConsentEpochNow() != consentEpoch) { code = code ?? "consent_changed"; return false; }
                if (_automaticLaneDisowned) { code = ReplacedCode; return false; }
                OwedExposure last;
                if (!ReadLastApplicationLocked(key, out last)) { code = "no_application"; return false; }
                var pins = last.Application;
                QueueApplicationLocked(key, new OwedExposure
                {
                    Entry = last.Entry, Session = last.Session, SessionId = last.SessionId,
                    AnonymousId = last.AnonymousId, ReadGeneration = last.ReadGeneration,
                    CaptureOrder = last.CaptureOrder,
                    Application = new ShardPilotExperimentApplication
                    {
                        ExperimentKey = key, VariantKey = pins.VariantKey, SubjectKey = pins.SubjectKey,
                        ExperimentVersion = pins.ExperimentVersion, ServedRevision = pins.ServedRevision,
                        ServedKillGate = pins.ServedKillGate, ServedAt = pins.ServedAt, AppliedAt = pins.AppliedAt,
                        Attributes = pins.Attributes, OccurrenceId = NewOccurrence(), OutcomeKey = outcomeKey,
                        OutcomeValue = (long)value, OccurredAt = ShardPilotJson.FormatTimestamp(NowUtc())
                    }
                });
                return true;
            }
        }

        private void QueueApplicationLocked(string key, OwedExposure application)
        {
            List<OwedExposure> queue;
            if (!_pendingExposure.TryGetValue(key, out queue))
                _pendingExposure[key] = queue = new List<OwedExposure>();
            queue.Add(application);
            if (queue.Count > MaxOwedExposures)
            {
                queue.RemoveAt(0);
                Diagnose("experiment_application_owed_overflow");
            }
        }

        private void ForgetApplicationsLocked(string scope, string key)
        {
            OwedExposure last;
            if (_lastApplications.TryGetValue(key, out last) && ScopeFor(last.Entry.SubjectKey) == scope)
                _lastApplications.Remove(key);
            var remove = new List<string>();
            foreach (var pair in _appliedTuples)
                if (pair.Value.Application.ExperimentKey == key && ScopeFor(pair.Value.Entry.SubjectKey) == scope) remove.Add(pair.Key);
            foreach (var tuple in remove) _appliedTuples.Remove(tuple);
        }

        private bool ApplicationAllowedLocked(string key, OwedExposure capture)
        {
            DateTime condemned;
            return capture.Application != null &&
                !(_declaring.ContainsKey(key) && AgeGated(capture.Entry)) &&
                !(TryGetCondemnation(ScopeFor(capture.Entry.SubjectKey), out condemned) && capture.Entry.FetchedAtUtc <= condemned) &&
                !WithdrawnSinceLocked(key, capture.Entry.SubjectFactKey, capture.ReadGeneration);
        }

        private void ForgetCondemnedApplicationsLocked()
        {
            foreach (var applications in new[] { _appliedTuples, _lastApplications })
            {
                var remove = new List<string>();
                foreach (var pair in applications)
                {
                    DateTime condemned;
                    var capture = pair.Value;
                    if (TryGetCondemnation(ScopeFor(capture.Entry.SubjectKey), out condemned) && capture.Entry.FetchedAtUtc <= condemned)
                        remove.Add(pair.Key);
                }
                foreach (var key in remove) applications.Remove(key);
            }
        }

        private bool OwnsApplicationLocked(string key, OwedExposure capture)
        {
            List<OwedExposure> queue;
            return _pendingExposure.TryGetValue(key, out queue) && queue.Count > 0 && ReferenceEquals(queue[0], capture);
        }

        private void RemoveApplicationLocked(string key, OwedExposure capture)
        {
            if (!OwnsApplicationLocked(key, capture)) return;
            var queue = _pendingExposure[key]; queue.RemoveAt(0);
            if (queue.Count == 0) _pendingExposure.Remove(key);
        }

        // Dispatch is separate from a persistence barrier: unsealed applications
        // never reach disk and a pause/quit snapshot never performs apply HTTP.
        private void SweepApplications(bool dispatch, string onlyKey = null)
        {
            if (ConsentRefusal(_consent()) != null) return;
            List<string> keys;
            lock (_lock) keys = onlyKey == null ? new List<string>(_pendingExposure.Keys) : new List<string> { onlyKey };
            foreach (var key in keys)
            {
                OwedExposure capture;
                long consentEpoch, authEpoch;
                lock (_lock)
                {
                    List<OwedExposure> queue;
                    if (_authBlocked || _provisionalRenewals.Count > 0 || !_pendingExposure.TryGetValue(key, out queue) || queue.Count == 0) continue;
                    capture = queue[0];
                    if (_declaring.ContainsKey(key)) continue;
                    if (!ApplicationAllowedLocked(key, capture)) { RemoveApplicationLocked(key, capture); continue; }
                    if (capture.InFlight || capture.NotBefore > NowUtc()) continue;
                    if (capture.Sealed == null && (!dispatch || _sender == null || _tornDown)) continue;
                    capture.InFlight = true;
                    consentEpoch = ConsentEpochNow(); authEpoch = _authEpoch;
                }
                if (capture.Sealed != null) { ForwardApplication(key, capture, consentEpoch); continue; }
                var request = ShardPilotExperimentApply.BuildRequest(_config, capture.Application);
                request.RefuseRedirects = true;
                request.BeforeSend = () =>
                {
                    if (ConsentRefusal(_consent()) != null || ConsentEpochNow() != consentEpoch) return false;
                    lock (_lock) return !_tornDown && _authEpoch == authEpoch &&
                        OwnsApplicationLocked(key, capture) && ApplicationAllowedLocked(key, capture);
                };
                var trap = new EgressDispatch();
                Action<ShardPilotHttpResponse> settle = response => SettleApplication(key, capture, request, response, consentEpoch, authEpoch);
                lock (_egressGate)
                {
                    if (ConsentEpochNow() != consentEpoch || _tornDown)
                        trap.Deliver(ShardPilotUnityHttpRequestSender.RefusedBeforeSendResponse(), settle);
                    else
                    {
                        try { _sender.Send(request, response => trap.Deliver(response, settle)); }
                        catch { trap.Deliver(new ShardPilotHttpResponse { TransportError = true }, settle); }
                    }
                }
                trap.Open(settle);
            }
        }

        private void SettleApplication(string key, OwedExposure capture, ShardPilotHttpRequest request,
            ShardPilotHttpResponse response, long consentEpoch, long authEpoch)
        {
            List<string> purge = null; WithdrawnApplications withdrawal = null;
            lock (_lock)
            {
                if (!OwnsApplicationLocked(key, capture)) return;
                capture.InFlight = false;
                if (_tornDown || ConsentRefusal(_consent()) != null ||
                    ConsentEpochNow() != consentEpoch || _authEpoch != authEpoch ||
                    !ApplicationAllowedLocked(key, capture)) return;
                var scope = ScopeFor(capture.Entry.SubjectKey);
                var status = response == null ? 0 : response.StatusCode;
                var redirected = response != null && ((response.StatusCode >= 300 && response.StatusCode < 400) ||
                    (!string.IsNullOrEmpty(response.EffectiveUrl) && response.EffectiveUrl != request.Url));
                if (redirected) status = 302;
                if (status == 401 || status == 403)
                {
                    var outcome = ShardPilotExperimentsLogic.Apply(null, response, NowUtc(), key, _config.AppKey, _config.EnvironmentKey);
                    long seq; lock (StaticGate) seq = ++_fetchSeq;
                    Install(seq, scope, key, outcome, authEpoch, NowUtc(), null, false, false, out purge, out withdrawal);
                }
                else if (status == 200 && !response.TransportError && response.EffectiveUrl == request.Url &&
                    ShardPilotExperimentApply.TryDecodeResponse(_config, capture.Application, response.Body, out capture.Sealed))
                {
                    capture.NotBefore = default(DateTime);
                }
                else if (status >= 300 && status < 500 && status != 408 && status != 429)
                {
                    Dictionary<string, object> body; object reason;
                    if (status == 409 && ShardPilotExperimentsLogic.ResponseErrorText(response) == "not_assigned" &&
                        ShardPilotJsonParser.TryParseObject(response.Body, out body) && body.TryGetValue("reason", out reason) &&
                        reason as string == ShardPilotExperimentsLogic.ReasonAgeIneligible)
                        withdrawal = WithdrawRefusedApplicationsLocked(scope, key, capture.Entry.SubjectFactKey, NowUtc());
                    else RemoveApplicationLocked(key, capture);
                    Diagnose("experiment_apply_terminal_" + status.ToString(CultureInfo.InvariantCulture));
                }
                else
                {
                    capture.Attempts++;
                    var seconds = Math.Max(response == null ? 0 : response.RetryAfterSeconds, Math.Min(60, Math.Pow(2, Math.Min(capture.Attempts, 6))));
                    capture.NotBefore = NowUtc().AddSeconds(seconds);
                    Diagnose("experiment_apply_retry_" + status.ToString(CultureInfo.InvariantCulture));
                }
            }
            InvokeFactPurge(purge);
            InvokeKeyFactWithdrawal(key, withdrawal);
            SweepApplications(false, key);
        }

        private void ForwardApplication(string key, OwedExposure capture, long consentEpoch)
        {
            FactEmission emission = null;
            try
            {
                lock (_lock)
                {
                    if (!OwnsApplicationLocked(key, capture) || !ApplicationAllowedLocked(key, capture))
                    { capture.InFlight = false; return; }
                    var fact = capture.Sealed;
                    var scope = ScopeFor(capture.Entry.SubjectKey);
                    if (fact.SubjectFactKey != capture.Entry.SubjectFactKey)
                    {
                        RetireFactKeyLocked(scope, key, fact.SubjectFactKey);
                        if (!PersistApplicationFactKeyHistoryLocked(scope))
                        { capture.InFlight = false; return; }
                    }
                    emission = EnterFactEmissionLocked(key, capture);
                    TrackFactScopeLocked(fact.EventId, scope, capture.Entry.FetchedAtUtc, capture.ReadGeneration);
                }
                if (!PreserveReadmissionBeforeEmissionLocked(key, capture))
                { lock (_lock) capture.InFlight = false; return; }
                lock (_lock)
                    if (!OwnsApplicationLocked(key, capture) || !ApplicationAllowedLocked(key, capture))
                    { capture.InFlight = false; return; }
                var accepted = false;
                string refusal = null;
                try
                {
                    FactPostCaptureHookForTesting?.Invoke();
                    if (ConsentEpochNow() == consentEpoch && ConsentRefusal(_consent()) == null && _emitSealed != null)
                        accepted = _emitSealed(capture.Sealed, capture.SessionId, capture.AnonymousId, capture.Session, consentEpoch, out refusal);
                }
                catch { Diagnose("experiment_fact_enqueue_failed"); }
                var terminal = !accepted && refusal == ShardPilotExperimentApply.EnvelopeTooLarge;
                List<string> purge = null;
                lock (_lock)
                {
                    capture.InFlight = false;
                    if (!accepted) _factScopes.Remove(capture.Sealed.EventId);
                    if (accepted || terminal)
                    {
                        RemoveApplicationLocked(key, capture);
                        if (accepted && (!ApplicationAllowedLocked(key, capture) || EmissionWasWithdrawn(emission)))
                            purge = new List<string> { capture.Sealed.EventId };
                    }
                }
                if (terminal) Diagnose("experiment_fact_terminal_" + refusal);
                if (purge != null)
                {
                    if (EmissionWasWithdrawn(emission)) RecordLatePurgeResult(emission, false);
                    else InvokeFactPurge(purge);
                }
            }
            finally { LeaveFactEmission(emission); }
        }

        // Application admission changes only history. Assemble its write
        // from the actual record, including a successor's withdrawal intents.
        private bool PersistApplicationFactKeyHistoryLocked(string scope)
        {
            if (string.IsNullOrEmpty(_cachePath)) return true;
            lock (StorageGate)
            {
                bool unreadable;
                var text = ReadBoundedRecordTextLocked(out unreadable);
                if (unreadable) return false;
                string storedScope;
                Dictionary<string, ShardPilotExperimentEntry> entries;
                Dictionary<string, List<string>> history;
                List<ShardPilotWithdrawalIntent> intents;
                if (text == null)
                    return WriteDurableRecordLocked(scope, new Dictionary<string, ShardPilotExperimentEntry>(),
                        new List<ShardPilotWithdrawalIntent>());
                if (!ShardPilotExperimentsLogic.TryParseRecord(text, out storedScope, out entries, out history, out intents) ||
                    !string.Equals(scope, storedScope, StringComparison.Ordinal)) return false;
                Dictionary<string, List<string>> capturedHistory;
                if (_factKeyHistory.TryGetValue(scope, out capturedHistory))
                    foreach (var pair in capturedHistory)
                    {
                        List<string> keys;
                        if (!history.TryGetValue(pair.Key, out keys)) history[pair.Key] = keys = new List<string>();
                        foreach (var factKey in pair.Value) if (!keys.Contains(factKey)) keys.Add(factKey);
                    }
                return WriteDurableRecordLocked(scope, entries, intents, history);
            }
        }

        internal bool HasApplicationsToDrain
        {
            get { lock (_lock) return !_tornDown && _pendingExposure.Count > 0; }
        }

        internal bool HasUnadmittedApplications
        {
            get { lock (_lock) return _pendingExposure.Count > 0; }
        }
    }
}
