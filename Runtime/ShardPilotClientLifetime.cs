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
        private sealed class HostConstruction
        {
            internal readonly HostLifetime Lifetime;
            internal readonly ShardPilotConfig Config;
            internal WeakReference<ShardPilotClient> Client;
            internal TaskCompletionSource<ShardPilotResult> Close;
            internal bool Starting;
            internal HostConstruction(HostLifetime lifetime, ShardPilotConfig config) { Lifetime = lifetime; Config = config; }
        }
        private static readonly object HostLifetimeGate = new object();
        private static HostLifetime _currentHostLifetime = new HostLifetime();
        private readonly HostConstruction _hostConstruction;
        private HostLifetime _hostLifetime => _hostConstruction.Lifetime;
        private volatile bool _hostExiting;
        private bool _hostDispatchStarted;
        private bool _closeStarted;
        internal bool HostRetiring => _hostExiting || _hostLifetime.Exiting;
        private bool IntakeClosed => _closed || _hostExiting || _hostLifetime.Exiting;

        private static HostConstruction CaptureHostConstruction(ShardPilotConfig config, bool handoff = false)
        {
            HostConstruction construction;
            lock (HostLifetimeGate)
            {
                construction = new HostConstruction(_currentHostLifetime, config);
                _currentHostLifetime.Clients.RemoveAll(reference => !reference.TryGetTarget(out _));
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

        private void CompleteHostConstruction(bool lifecycle)
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
