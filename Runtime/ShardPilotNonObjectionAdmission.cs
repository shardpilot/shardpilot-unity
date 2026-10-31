using System;
using System.Collections.Generic;

namespace ShardPilot
{
    public sealed partial class ShardPilotClient
    {
        private sealed class NonObjectionAdmission
        {
            internal readonly long Generation, DenialEpoch;
            internal readonly string User, Anonymous;
            internal readonly string Workspace, App, Environment;
            internal readonly string Actor, Kind;
            internal NonObjectionAdmission(long generation, long denialEpoch, string user, string anonymous,
                string workspace, string app, string environment, string actor, string kind)
            {
                Generation = generation;
                DenialEpoch = denialEpoch;
                User = user;
                Anonymous = anonymous;
                Workspace = workspace;
                App = app;
                Environment = environment;
                Actor = actor;
                Kind = kind;
            }
        }
        private NonObjectionAdmission _nonObjectionAdmission;

        // The original context remains attached to its events after a grant.
        // Every caller holds _gate; this predicate performs no storage I/O.
        private bool NonObjectionEpisodeCurrentLocked(NonObjectionAdmission basis) =>
            basis != null && !_consentOutbox.IsDisowned && !_consentConfiguredActorInvalid &&
            basis.Generation == _nonObjectionRecordingGeneration &&
            basis.DenialEpoch == _consentDenialEpoch && !ShardPilotConsent.IsDeniedState(_consentState) &&
            string.Equals(basis.User, _config.UserId, StringComparison.Ordinal) &&
            string.Equals(basis.Anonymous, _config.AnonymousId, StringComparison.Ordinal) &&
            string.Equals(basis.Workspace, _config.WorkspaceKey, StringComparison.Ordinal) &&
            string.Equals(basis.App, _config.AppKey, StringComparison.Ordinal) &&
            string.Equals(basis.Environment, _config.EnvironmentKey, StringComparison.Ordinal);
        private bool NonObjectionContextCurrentLocked(NonObjectionAdmission basis) =>
            NonObjectionEpisodeCurrentLocked(basis) &&
            !_consentReadFailureCrashHold && !_consentConfiguredActorInvalid && !ConsentPurgeOwed &&
            !_consentOutbox.IsDisowned && !_consentOutbox.LoadFailed && !_consentOutbox.LoadedRecordMalformed &&
            !(_consentOutbox.HasDurableBackend && _consentOutbox.Dirty) &&
            !_consentOutbox.OwnScope.SnapshotEntries().Exists(entry => !entry.IsNonObjection &&
                !entry.AnalyticsGranted && entry.ActorIdentifier == basis.Actor && entry.Kind == basis.Kind);

        private bool NonObjectionAdmittedLocked
        {
            get
            {
                if (_nonObjectionAdmission != null && !NonObjectionEpisodeCurrentLocked(_nonObjectionAdmission))
                    RetireNonObjectionAdmissionLocked();
                return _consentState == ShardPilotConsentState.Unknown && NonObjectionContextCurrentLocked(_nonObjectionAdmission);
            }
        }
        private bool AnalyticsAdmittedLocked
        {
            get { var basis = NonObjectionAdmittedLocked; return _consentState == ShardPilotConsentState.Granted || basis; }
        }
        private bool AnalyticsAdmissionCurrent(long? denialEpoch = null)
        {
            lock (_gate)
                return (!denialEpoch.HasValue || denialEpoch.Value == _consentDenialEpoch) && AnalyticsAdmittedLocked;
        }
        private bool BasisEventOwnedLocked(ShardPilotEvent ev) => ev.NonObjectionOrigin == null ||
            (ReferenceEquals(ev.NonObjectionOrigin, _nonObjectionAdmission) &&
             NonObjectionEpisodeCurrentLocked((NonObjectionAdmission)ev.NonObjectionOrigin));
        private bool BasisEventCurrentLocked(ShardPilotEvent ev) => BasisEventOwnedLocked(ev) &&
            (ev.NonObjectionOrigin == null || _consentState == ShardPilotConsentState.Granted ||
             NonObjectionContextCurrentLocked((NonObjectionAdmission)ev.NonObjectionOrigin));
        private bool BasisBatchCurrent(List<ShardPilotEvent> batch)
        {
            lock (_gate) return batch.TrueForAll(BasisEventCurrentLocked);
        }
        private void RetireNonObjectionAdmissionLocked()
        {
            if (_nonObjectionAdmission == null) return;
            _nonObjectionAdmission = null;
            var dropped = new List<ShardPilotEvent>();
            for (var remaining = _queue.Count; remaining > 0; remaining--)
            {
                var ev = _queue.Dequeue();
                if (ev.NonObjectionOrigin != null) dropped.Add(ev); else _queue.Enqueue(ev);
            }
            SettleBasisDropsLocked(dropped);
            if (_sessionState.Lifecycle.NonObjectionOrigin != null)
            {
                _sessionState = new SessionState(string.Empty, 0, false);
                _sessionPause = null;
                _owedSessionStartThreadId = 0;
                System.Threading.Monitor.PulseAll(_gate);
            }
            // The worker owns its held list and reconciles it at its safe points.
            WakeWorker();
        }
        private void DropStaleBasisLocked(List<ShardPilotEvent> batch)
        {
            _ = AnalyticsAdmittedLocked;
            var dropped = batch.FindAll(ev => !BasisEventOwnedLocked(ev));
            if (dropped.Count == 0) return;
            batch.RemoveAll(ev => !BasisEventOwnedLocked(ev));
            SettleBasisDropsLocked(dropped);
        }
        private void SettleBasisDropsLocked(List<ShardPilotEvent> dropped)
        {
            if (dropped.Count == 0) return;
            RecordSettlementLocked(dropped);
            _stats.RecordDropped(dropped.Count);
        }
        private static bool GrantOnlyAnalytics(string name)
        {
            if (name.StartsWith("app.", StringComparison.Ordinal)) name = name.Substring(4);
            return name == AdImpressionRevenueEventName || name == "experiment_exposure" || name == "experiment_outcome";
        }
        private bool TypedAnalyticsAdmittedLocked(string name) => AnalyticsAdmittedLocked &&
            (_consentState == ShardPilotConsentState.Granted || !GrantOnlyAnalytics(name));
        private bool AnalyticsEventAdmittedLocked(ShardPilotEvent ev) => AnalyticsAdmittedLocked &&
            BasisEventCurrentLocked(ev) && (_consentState == ShardPilotConsentState.Granted ||
            (ev.NonObjectionOrigin != null && ev.SealedFact == null && !GrantOnlyAnalytics(ev.Name) &&
             string.Equals(ev.UserId ?? string.Empty, _config.UserId ?? string.Empty, StringComparison.Ordinal) &&
             string.Equals(ev.AnonymousId, _config.AnonymousId, StringComparison.Ordinal)));
    }
}
