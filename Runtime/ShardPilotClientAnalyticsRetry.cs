using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ShardPilot
{
    public sealed partial class ShardPilotClient
    {
        // Settlement may occur after the predecessor is disowned but before
        // construction publishes its successor. Keep pending-record
        // corrections until an active owner can confirm them.
        private readonly Dictionary<string, ShardPilotEvent> _eventSpoolRemovalDebt =
            new Dictionary<string, ShardPilotEvent>(StringComparer.Ordinal);
        private readonly Dictionary<string, KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>> _analyticsRollbackDebt =
            new Dictionary<string, KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>>(StringComparer.Ordinal);

        private void RetryEventSpoolDebts()
        {
            List<ShardPilotEvent> removals;
            Dictionary<string, KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>> rollback;
            lock (_gate)
            {
                removals = new List<ShardPilotEvent>(_eventSpoolRemovalDebt.Values);
                rollback = new Dictionary<string, KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>>(_analyticsRollbackDebt);
            }
            if (removals.Count > 0) SpoolRemove(removals);
            if (rollback.Count > 0) RestoreEventSpoolClocks(rollback, true, false);
        }

        private bool RestoreEventSpoolClocks(
            IReadOnlyDictionary<string, KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>> changed,
            bool persist, bool record = true)
        {
            if (record)
                lock (_gate)
                    foreach (var pair in changed) _analyticsRollbackDebt[pair.Key] = pair.Value;
            var restored = WithActiveEventSpool(owner => owner._spool.RestoreAnalyticsClocks(changed, persist));
            // The original client can close while a later replacement is
            // still being constructed. Leave the debt on the last linked
            // client so that registration, not the closed caller, retries it.
            var successor = _spoolSuccessor;
            if (!restored && successor != null) restored = successor.RestoreEventSpoolClocks(changed, persist);
            if (restored && persist) ClearAnalyticsRollbackDebt(changed);
            return restored;
        }

        private void ClearAnalyticsRollbackDebt(
            IReadOnlyDictionary<string, KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>> confirmed)
        {
            lock (_gate)
                foreach (var pair in confirmed)
                {
                    KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock> current;
                    if (_analyticsRollbackDebt.TryGetValue(pair.Key, out current) && current.Key == pair.Value.Key)
                        _analyticsRollbackDebt.Remove(pair.Key);
                }
        }

        internal Action EventSpoolOperationCompletedHookForTesting;

        // A selection made before an operation is not an ownership receipt.
        // Retry after transfer, including a transfer during file I/O. A client
        // with no configured spool is distinct from one awaiting its successor.
        private bool WithActiveEventSpool(Func<ShardPilotClient, bool> operation, bool withoutSpool = true)
        {
            return WithActiveEventSpool(operation, withoutSpool, false);
        }

        private T WithActiveEventSpool<T>(Func<ShardPilotClient, T> operation, T withoutSpool, T withoutOwner)
        {
            var owner = this;
            while (owner != null)
            {
                if (owner._spool == null) return withoutSpool;
                if (!owner._spool.IsDisowned)
                {
                    var result = operation(owner);
                    owner.EventSpoolOperationCompletedHookForTesting?.Invoke();
                    if (!owner._spool.IsDisowned) return result;
                }
                owner = owner._spoolSuccessor;
            }
            return withoutOwner;
        }

        private void StampAnalyticsIntake(ShardPilotEvent ev)
        {
            ev.AnalyticsClock = new ShardPilotAnalyticsClock(ShardPilotAnalyticsClock.UnixMs(_utcNow()), 0);
        }

        private void FilterAnalyticsAge(List<ShardPilotEvent> batch)
        {
            var expired = new List<ShardPilotEvent>();
            var notices = new List<string>();
            lock (_gate)
            {
                var now = ShardPilotAnalyticsClock.UnixMs(_utcNow());
                for (var i = batch.Count - 1; i >= 0; i--)
                {
                    var ev = batch[i];
                    var clock = ev.AnalyticsClock;
                    if (clock != null && !clock.IsExpired(now)) continue;
                    if (clock == null) ev.AnalyticsClock = clock = new ShardPilotAnalyticsClock(0, 0);
                    batch.RemoveAt(i);
                    expired.Add(ev);
                    if (clock.MarkExpired())
                    {
                        _stats.RecordSpoolExpired();
                        notices.Add(ev.Id);
                    }
                }
                if (expired.Count > 0) RecordSettlementLocked(expired);
            }
            if (expired.Count == 0) return;
            if (_experiments != null)
                _experiments.OnFactsSettledWithoutDelivery(CollectExperimentFactEventIds(expired));
            SpoolRemove(expired);
            PruneExperimentFactKeys();
            // All members are removed and counted before invoking host logging.
            // The caller rechecks consent after this reentrant surface.
            foreach (var id in notices)
                SafeLog("shardpilot event {0} dropped: code=spool_expired", id);
        }

        private void RollbackAnalyticsAttempt(TransportHandoff handoff, bool persist)
        {
            var changed = new Dictionary<string, KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>>(StringComparer.Ordinal);
            lock (_gate)
            {
                foreach (var pair in handoff.AnalyticsStarted)
                {
                    var ev = pair.Key;
                    var clock = ev.AnalyticsClock;
                    if (clock != null && clock.AmbiguousMs > 0 && pair.Value.AmbiguousMs == 0)
                    {
                        ev.AnalyticsClock = pair.Value;
                        changed[ev.Id] = new KeyValuePair<ShardPilotAnalyticsClock, ShardPilotAnalyticsClock>(clock, pair.Value);
                    }
                }
                handoff.AnalyticsStarted.Clear();
            }
            // Even a deferred rollback updates the writer's memory. Otherwise
            // a successful but slow candidate can survive a later failed
            // restamp and be published by the recovering background writer.
            if (changed.Count > 0)
            {
                RestoreEventSpoolClocks(changed, persist);
            }
        }

        private bool PrepareAnalyticsRequest(
            List<ShardPilotEvent> batch, ConsentEpoch epoch, TransportHandoff handoff,
            ShardPilotTransportRequest request, bool provenBeforeSend, bool deferNewAttempt = false)
        {
            while (true)
            {
                RetryEventSpoolDebts();
                lock (_gate)
                {
                    if (epoch.Value != _consentDenialEpoch || _consentState != ShardPilotConsentState.Granted)
                        return false;
                }
                if (provenBeforeSend) RollbackAnalyticsAttempt(handoff, false);
                // Another live reference can have attempted a restored copy.
                // Adopt its retained clock before age checks or a new stamp.
                lock (_gate)
                    if (!WithActiveEventSpool(owner => owner._spool.AdoptAnalyticsClocks(batch)))
                        throw new ShardPilotAnalyticsClockWriteException();
                FilterAnalyticsAge(batch);
                lock (_gate)
                {
                    handoff.SentCount = batch.Count;
                    if (epoch.Value != _consentDenialEpoch || _consentState != ShardPilotConsentState.Granted)
                        return false;
                    if (batch.Count == 0) return false;
                    var now = ShardPilotAnalyticsClock.UnixMs(_utcNow());
                    foreach (var ev in batch)
                    {
                        var clock = ev.AnalyticsClock;
                        if (clock.AmbiguousMs != 0 || deferNewAttempt) continue;
                        handoff.AnalyticsStarted[ev] = clock;
                        ev.AnalyticsClock = clock.WithAmbiguous(now);
                    }
                }
                if (!WithActiveEventSpool(owner => owner._spool.PersistAnalyticsClocks(batch)))
                {
                    RollbackAnalyticsAttempt(handoff, false);
                    throw new ShardPilotAnalyticsClockWriteException();
                }
                lock (_gate)
                {
                    // This fence durably supersedes any earlier correction
                    // before another attempt is allowed to become ambiguous.
                    foreach (var ev in batch) _analyticsRollbackDebt.Remove(ev.Id);
                    if (epoch.Value != _consentDenialEpoch || _consentState != ShardPilotConsentState.Granted)
                        return false;
                    if (AnalyticsPreparationIsCurrent(batch, epoch))
                    {
                        request.Payload = ShardPilotJson.SerializeBatch(_config, batch);
                        return true;
                    }
                }
                // Durable I/O can take the candidate past an age limit. No
                // request has been submitted: undo only this first attempt,
                // then recheck intake and any earlier ambiguous attempt.
                RollbackAnalyticsAttempt(handoff, false);
            }
        }

        private bool AnalyticsPreparationIsCurrent(List<ShardPilotEvent> batch, ConsentEpoch epoch)
        {
            lock (_gate)
            {
                if (epoch.Value != _consentDenialEpoch || _consentState != ShardPilotConsentState.Granted)
                    return false;
                var now = ShardPilotAnalyticsClock.UnixMs(_utcNow());
                foreach (var ev in batch)
                    if (ev.AnalyticsClock == null || ev.AnalyticsClock.IsExpired(now)) return false;
                return true;
            }
        }

        private Task<bool> PrepareAnalyticsOwnedSend(
            List<ShardPilotEvent> batch, ConsentEpoch epoch, TransportHandoff handoff,
            ShardPilotTransportRequest request)
        {
#if UNITY_WEBGL && !UNITY_EDITOR
            // This profile never constructs the durable spool and has no threads.
            return Task.FromResult(PrepareAnalyticsRequest(batch, epoch, handoff, request,
                request.DefinitelyNotDelivered));
#else
            // The owned transport calls from its frame coroutine. Durable I/O
            // belongs off that thread, just like ordinary spool writes.
            return Task.Run(() => PrepareAnalyticsRequest(batch, epoch, handoff, request,
                request.DefinitelyNotDelivered));
#endif
        }

        private sealed class ShardPilotAnalyticsClockWriteException : Exception
        {
            internal ShardPilotAnalyticsClockWriteException() : base("spool_persist_failed") { }
        }
    }
}
