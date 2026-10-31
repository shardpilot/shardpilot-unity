using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ShardPilot
{
    internal static class ShardPilotNonObjectionReceipt
    {
        internal static Dictionary<string, object> Host(Dictionary<string, object> wire)
        {
            var host = new Dictionary<string, object>();
            foreach (var key in "advisory age_evidence_class attestation notice_version notice_locale policy_version".Split(' '))
                host[key] = wire.TryGetValue(key, out var value) ? value : null;
            return host;
        }
        internal static string HostKey(string body)
        {
            ShardPilotJsonParser.TryParseObject(body, out var wire);
            return ShardPilotJson.CaptureMap(Host(wire));
        }
        internal static bool Valid(ShardPilotConsentOutboxEntry entry)
        {
            try
            {
                if (entry.AnalyticsGranted || entry.DecisionSeq != 0 || entry.Superseded || entry.Reason != null || !ShardPilotIds.IsUuidV7(entry.IdempotencyKey) ||
                    !ShardPilotConsentOutbox.IsValidIdentifier(entry.AnonymousId) ||
                    (entry.Kind != "anon" && entry.Kind != "user_verified") || (entry.Kind == "anon" && entry.ActorIdentifier != entry.AnonymousId) ||
                    new System.Text.UTF8Encoding(false, true).GetByteCount(entry.NonObjectionBody) > 4096 ||
                    !ShardPilotJsonParser.TryParseUniqueObject(entry.NonObjectionBody, out var wire, out _) || wire.Count != 15) return false;
                var expected = new Dictionary<string, object> { ["workspace_key"] = entry.WorkspaceId, ["app_key"] = entry.AppId,
                    ["environment_key"] = entry.EnvironmentId, ["actor_identifier"] = entry.ActorIdentifier, ["kind"] = entry.Kind,
                    ["decided_at"] = entry.DecidedAt, ["idempotency_key"] = entry.IdempotencyKey, ["admission_basis"] = "advisory_non_objection", ["purpose"] = "analytics" };
                return expected.All(pair => wire.TryGetValue(pair.Key, out var value) && Equals(value, pair.Value)) && ShardPilotNonObjection.Snapshot(Host(wire), out _) != null;
            }
            catch { return false; }
        }
    }

    public sealed partial class ShardPilotClient
    {
        private Action<string> _nonObjectionCancel;
        private Action _nonObjectionPulse;
        private DateTime _nonObjectionRetryNotBefore = DateTime.MinValue;

        private ShardPilotConsentOutboxEntry RetainNonObjection(ShardPilotNonObjection.SnapshotValue snapshot, long generation,
            string actor, string kind, string anonymous, string workspace, string app, string environment, out string error)
        {
            error = "consent_outbox_write_failed";
            if (_consentOutbox.LoadFailed && !PersistConsentOutbox()) return null;
            ShardPilotConsentOutboxEntry entry;
            lock (_gate)
            {
                error = "non_objection_ineligible";
                if (generation != _nonObjectionRecordingGeneration || _closeStarted || _consentState != ShardPilotConsentState.Unknown ||
                    _consentOutbox.IsDisowned || _consentOutbox.LoadFailed || _consentOutbox.LoadedRecordMalformed) return null;
                var entries = _consentOutbox.OwnScope.SnapshotEntries();
                if (entries.Any(item => !item.IsNonObjection && !item.AnalyticsGranted && item.ActorIdentifier == actor && item.Kind == kind)) return null;
                var host = ShardPilotJson.CaptureMap(snapshot.ToMap());
                entry = entries.FirstOrDefault(item => item.IsNonObjection && item.ActorIdentifier == actor && item.Kind == kind &&
                    item.AnonymousId == anonymous && item.WorkspaceId == workspace && item.AppId == app && item.EnvironmentId == environment && ShardPilotNonObjectionReceipt.HostKey(item.NonObjectionBody) == host);
                if (entry == null)
                {
                    error = "consent_outbox_full";
                    if (_consentOutbox.NonObjectionAppendBlockedByCap()) return null;
                    entry = new ShardPilotConsentOutboxEntry { ActorIdentifier = actor, Kind = kind, AnonymousId = anonymous,
                        WorkspaceId = workspace, AppId = app, EnvironmentId = environment, DecidedAt = ShardPilotJson.FormatTimestamp(DateTime.UtcNow), IdempotencyKey = ShardPilotIds.NewUuidV7() };
                    var wire = snapshot.ToMap();
                    wire["actor_identifier"] = actor; wire["kind"] = kind; wire["workspace_key"] = workspace; wire["app_key"] = app; wire["environment_key"] = environment;
                    wire["decided_at"] = entry.DecidedAt; wire["idempotency_key"] = entry.IdempotencyKey; wire["admission_basis"] = "advisory_non_objection"; wire["purpose"] = "analytics";
                    entry.NonObjectionBody = ShardPilotJson.CaptureMap(wire);
                    if (!_consentOutbox.OwnScope.Matches(entry) || !_consentOutbox.AppendToMirror(entry)) return null;
                }
            }
            error = "consent_outbox_write_failed";
            if (_consentOutbox.HasDurableBackend && !PersistConsentOutbox()) return null;
            error = null; return entry;
        }

        private bool RetireNonObjection(ShardPilotConsentOutboxEntry entry, Func<string> current)
        {
            lock (_gate)
            {
                if (current() != null || _consentOutbox.IsDisowned) return false;
                _consentOutbox.OwnScope.RemoveFromMirror(entry.IdempotencyKey);
            }
            if (_consentOutbox.HasDurableBackend) PersistConsentOutbox();
            return true;
        }

        private async Task WaitNonObjectionRetry(long generation, CancellationToken token)
        {
            var ready = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using var delayCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            Action pulse = () => { lock (_gate) if (DateTime.UtcNow >= _nonObjectionRetryNotBefore) ready.TrySetResult(true); };
            TimeSpan wait;
            lock (_gate)
            {
                if (generation != _nonObjectionRecordingGeneration) throw new OperationCanceledException();
                _nonObjectionPulse = pulse; wait = _nonObjectionRetryNotBefore - DateTime.UtcNow;
            }
            try { await Task.WhenAny(ready.Task, Task.Delay(TimeSpan.FromMilliseconds(Math.Max(1, wait.TotalMilliseconds)), delayCancellation.Token)).ConfigureAwait(false); delayCancellation.Cancel(); token.ThrowIfCancellationRequested(); }
            finally { Interlocked.CompareExchange(ref _nonObjectionPulse, null, pulse); }
        }

        private void CloseNonObjection()
        {
            Interlocked.Exchange(ref _nonObjectionCancel, null)?.Invoke("shutdown");
            if (_consentOutbox.HasDurableBackend || _consentOutbox.IsDisowned) return;
            var dropped = false;
            foreach (var entry in _consentOutbox.OwnScope.SnapshotEntries())
                if (entry.IsNonObjection) { _consentOutbox.OwnScope.RemoveFromMirror(entry.IdempotencyKey); dropped = true; }
            if (dropped) SafeLog("non_objection: shutdown_undurable");
        }
    }
}
