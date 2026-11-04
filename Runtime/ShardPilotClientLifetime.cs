using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ShardPilot
{
    public sealed partial class ShardPilotClient : IDisposable
    {
        private sealed class HostLifetime
        {
            internal volatile bool Exiting;
            internal readonly List<WeakReference<HostConstruction>> Clients = new List<WeakReference<HostConstruction>>();
        }
        private sealed class HostConstruction : IDisposable
        {
            internal readonly HostLifetime Lifetime;
            internal readonly ShardPilotConfig Config;
            internal WeakReference<ShardPilotClient> Client;
            internal TaskCompletionSource<ShardPilotResult> Close;
            internal bool Starting;
            internal bool Startup;
            internal bool Initialized;
            internal HostConstruction(HostLifetime lifetime, ShardPilotConfig config) { Lifetime = lifetime; Config = config; }

            public void Dispose()
            {
                if (Initialized) return;
                lock (HostLifetimeGate)
                    Lifetime.Clients.RemoveAll(reference => reference.TryGetTarget(out var item) && item == this);
            }
        }
        private static readonly object HostLifetimeGate = new object();
        private static HostLifetime _currentHostLifetime = new HostLifetime();
        private readonly HostConstruction _hostConstruction;
        private HostLifetime _hostLifetime => _hostConstruction.Lifetime;
        private volatile bool _hostExiting;
        private volatile bool _hostDispatchStarted;
        private bool StartupDispatchHeld => _hostConstruction.Startup && !_hostDispatchStarted;
        private bool _closeStarted;
        internal bool HostRetiring => _hostExiting || _hostLifetime.Exiting;
        private bool IntakeClosed => _closed || _hostExiting || _hostLifetime.Exiting;

        private static HostConstruction CaptureHostConstruction(ShardPilotConfig config, bool handoff = false, bool startup = false)
        {
            HostConstruction construction;
            lock (HostLifetimeGate)
            {
                _currentHostLifetime.Clients.RemoveAll(reference => !reference.TryGetTarget(out _));
                foreach (var reference in _currentHostLifetime.Clients)
                {
                    if (!reference.TryGetTarget(out var existing)) continue;
                    if (startup && (existing.Startup || existing.Client == null ||
                        (existing.Client.TryGetTarget(out var client) && !client._workerStopped)))
                        return null;
                    if (existing.Startup) throw new InvalidOperationException("SDK startup reconciliation is in progress");
                }
                construction = new HostConstruction(_currentHostLifetime, config) { Startup = startup };
                _currentHostLifetime.Clients.Add(new WeakReference<HostConstruction>(construction));
            }
            ConstructionBoundary("capture");
            if (handoff && !construction.Lifetime.Exiting) PrepareSpoolHandoff(config, () => !construction.Lifetime.Exiting);
            return construction;
        }
#if SHARDPILOT_SDK_TEST_SUPPORT
        internal static Action<string> ConstructionBoundaryForTesting;
#endif
        [System.Diagnostics.Conditional("SHARDPILOT_SDK_TEST_SUPPORT")]
        private static void ConstructionBoundary(string boundary)
        {
#if SHARDPILOT_SDK_TEST_SUPPORT
            ConstructionBoundaryForTesting?.Invoke(boundary);
#endif
        }

        private void CompleteHostConstruction(bool lifecycle) => CompleteHostConstructionCore(lifecycle, null, null);

        private bool CompleteHostConstructionCore(bool lifecycle, Func<bool> canStart, Action afterReconcile)
        {
            ConstructionBoundary("dispatch-refresh");
            _spool?.RefreshSuppressedClocks();
            var construction = _hostConstruction;
            lock (HostLifetimeGate)
            {
                construction.Client = new WeakReference<ShardPilotClient>(this);
                construction.Starting = true;
                try
                {
                    ConstructionBoundary("lifecycle-registration");
                    if (!_hostLifetime.Exiting && lifecycle) ShardPilotDispatcher.RegisterLifecycleClient(this);
                    if (canStart != null && (_hostLifetime.Exiting || !canStart())) return false;
                    afterReconcile?.Invoke();
                    if (canStart != null && _hostLifetime.Exiting) return false;
                    if (!_hostLifetime.Exiting) { _hostDispatchStarted = true; StartDispatchAfterRegistration(); }
                    ConstructionBoundary("lifetime-registration");
                }
                finally { construction.Starting = false; }
                if (construction.Close != null) _closeCompletion = construction.Close;
            }
            if (_hostLifetime.Exiting)
            {
                if (!_hostDispatchStarted)
                {
                    ShardPilotDispatcher.UnregisterLifecycleClient(this);
                    DisownSpool(); _experiments?.Teardown();
                    lock (_gate) _workerStopped = true;
                }
                CloseForHostExit();
            }
            return _hostDispatchStarted;
        }
        internal static ShardPilotClient CreateForStartup(ShardPilotConfig config, IShardPilotTransport transport = null,
            IShardPilotKeyValueStore store = null, IShardPilotHttpRequestSender sender = null, bool startWorker = true)
        {
            var construction = CaptureHostConstruction(config, startup: true);
            if (construction == null) return null;
            try
            {
                return new ShardPilotClient(construction, config,
                    transport ?? new ShardPilotUnityWebRequestTransport(ShardPilotDispatcher.Instance, config.RequestCompressionEnabled),
                    startWorker, store ?? new ShardPilotUnityKeyValueStore(),
                    sender ?? new ShardPilotUnityHttpRequestSender(), deferDispatchForRegistration: true);
            }
            catch
            {
                construction.Dispose();
                throw;
            }
        }

        internal ShardPilotResult ReconcileStartup(ShardPilotConsentDecision decision, bool crashEnabled, ShardPilotConsentNotice? notice, Action afterReconcile)
        {
            if (HostRetiring) return StartupFailure("shutdown");
            if (StartupPrivacyUnreadable()) return StartupFailure("startup_privacy_unreadable");
            var expected = (ShardPilotConsentState)decision;
            if (_consentState == ShardPilotConsentState.DeniedForcedMinor && expected != _consentState)
                return StartupFailure("consent_forced_minor");

            var prior = ShardPilotConsent.LoadRecord(_settingsStore, out var unreadable);
            var restricted = ShardPilotConsent.HasForcedMinorRestriction(_settingsStore, out var restrictionUnreadable);
            if (unreadable || restrictionUnreadable) return StartupFailure("startup_privacy_unreadable");
            if (restricted && expected != ShardPilotConsentState.DeniedForcedMinor)
                return StartupFailure("consent_forced_minor");
            // A modern, current-actor record already embodies a previously reconciled choice.
            var sameDecision = prior != null && !prior.Legacy && prior.State == expected &&
                prior.AnonymousId == _config.AnonymousId && _consentState == expected;
            if (!sameDecision || notice.HasValue)
            {
                if (decision == ShardPilotConsentDecision.Granted && !notice.HasValue)
                {
                    foreach (var receipt in _consentOutbox.OwnScope.SnapshotEntries())
                    {
                        if (!receipt.AnalyticsGranted || receipt.Superseded || receipt.RetainedFromCorruption ||
                            receipt.AnonymousId != _config.AnonymousId || receipt.Notice == null || IsConsentReceiptParked(receipt)) continue;
                        notice = new ShardPilotConsentNotice { NoticeVersion = receipt.Notice.NoticeVersion,
                            NoticeLocale = receipt.Notice.NoticeLocale, PolicyVersion = receipt.Notice.PolicyVersion };
                    }
                    if (!notice.HasValue) return StartupFailure("startup_notice_required");
                }
                if (decision == ShardPilotConsentDecision.Granted &&
                    !ShardPilotConsentNoticeValidator.TrySnapshot(notice, true, out _, out var noticeError, out _))
                    return StartupFailure(noticeError);
                if (!_consentOutbox.HasDurableBackend) return StartupFailure("consent_outbox_persist_failed");
                var consent = SetConsent(decision, notice);
                if (!consent.Success || consent.Warnings.Count != 0)
                    return StartupFailure(consent.Error ?? (consent.Warnings.Count == 0 ? "startup_privacy_unreconciled" : consent.Warnings[0]));
            }

            if (!SetCrashReportingEnabled(crashEnabled)) return StartupFailure("crash_persist_failed");
            bool reconciled() => !StartupPrivacyUnreadable() && !ConsentPurgeOwed &&
                _consentState == expected && _crashReportingEnabled == crashEnabled;
            return reconciled() && CompleteHostConstructionCore(true, reconciled, afterReconcile)
                ? ShardPilotResult.Ok : StartupFailure("startup_privacy_unreconciled");
        }

        private bool StartupPrivacyUnreadable() => _consentReadFailureCrashHold || _consentOutbox.LoadFailed ||
            _crashReportingDisabledReason == ShardPilotCrashSettings.ReadFailedReason;

        internal static ShardPilotResult StartupFailure(string error) => new ShardPilotResult(false, error, false);

        internal void EndStartup(bool succeeded)
        {
            if (!succeeded)
            {
                lock (_gate)
                {
                    _closed = true;
                    _workerStopped = true;
                    _closeCompletion ??= new TaskCompletionSource<ShardPilotResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                    _closeCompletion.TrySetResult(StartupFailure("shutdown"));
                }
                ShardPilotDispatcher.PauseStateChanged -= OnPauseStateChangedForSession;
                ShardPilotDispatcher.UnregisterLifecycleClient(this);
                DisownSpool();
                _experiments?.Teardown();
            }
            lock (HostLifetimeGate)
            {
                _hostConstruction.Startup = false;
                if (!succeeded)
                {
                    _hostConstruction.Close?.TrySetResult(StartupFailure("shutdown"));
                    _hostLifetime.Clients.RemoveAll(reference => reference.TryGetTarget(out var item) && item == _hostConstruction);
                }
            }
        }

        internal static void BeginHostLifetime()
        {
            lock (HostLifetimeGate)
                if (_currentHostLifetime.Exiting) _currentHostLifetime = new HostLifetime();
        }
        internal static Task<ShardPilotResult>[] CloseHostLifetime()
        {
            var clients = new List<ShardPilotClient>();
            var tasks = new List<Task<ShardPilotResult>>();
            lock (HostLifetimeGate)
            {
                _currentHostLifetime.Exiting = true;
                foreach (var reference in _currentHostLifetime.Clients)
                {
                    if (!reference.TryGetTarget(out var construction)) continue;
                    ShardPilotClient client = null;
                    var ready = construction.Client != null && construction.Client.TryGetTarget(out client);
                    if (ready)
                    {
                        if (!construction.Starting) { clients.Add(client); continue; }
                        client._hostExiting = true;
                        construction.Close = client._closeCompletion ?? construction.Close;
                    }
                    construction.Close ??= new TaskCompletionSource<ShardPilotResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                    if (!ready) construction.Close.TrySetResult(new ShardPilotResult(false, ShardPilotFlushCodes.Shutdown, false));
                    tasks.Add(construction.Close.Task);
                }
            }
            foreach (var client in clients) tasks.Add(client.CloseForHostExit());
            return tasks.ToArray();
        }
        internal Task<ShardPilotResult> CloseForHostExit(TimeSpan? timeout = null)
        {
            lock (_gate) _hostExiting = true;
            ShardPilotDispatcher.PauseStateChanged -= OnPauseStateChangedForSession;
            _experiments?.BeginCloseDrain();
            OnApplicationQuitForSession();
            return StartObservedClose(timeout);
        }
        /// <summary>Attempts remnant persistence and starts the retained bounded
        /// close. Await CloseAsync for its result and completed retirement.</summary>
        public void Dispose()
        {
            PersistPendingEvents();
            StartObservedClose(null);
        }
        private Task<ShardPilotResult> StartObservedClose(TimeSpan? timeout)
        {
            var close = CloseAsync(timeout);
            ShardPilotSdk.DetachDisposedClient(this);
            _ = close.ContinueWith(task => { _ = task.Exception; }, CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            return close;
        }
    }
}
