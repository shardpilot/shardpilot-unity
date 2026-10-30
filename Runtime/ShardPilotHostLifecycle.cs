using System;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

namespace ShardPilot
{
    // Unity invokes these boundaries on its main thread. Client close remains
    // the sole bounded drain owner; this bridge never waits for network work.
    internal static class ShardPilotHostLifecycle
    {
        private static bool _exited;
        [RuntimeInitializeOnLoadMethod(RuntimeInitializeLoadType.SubsystemRegistration)]
        internal static void Enter()
        {
            if (!_exited) return;
            ShardPilotDispatcher.RetireHost(); // A fenced late constructor may have allocated another host.
            ShardPilotSdk.ClearHostRoots(true);
            ShardPilotClient.BeginHostLifetime();
            _exited = false;
        }
        internal static void Exit()
        {
            if (_exited) return;
            _exited = true;
            var roots = ShardPilotSdk.CaptureHostRoots();
            try
            {
                foreach (var close in ShardPilotClient.CloseHostLifetime())
                    _ = close.ContinueWith(task => { _ = task.Exception; GC.KeepAlive(roots); },
                        CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
            finally
            {
                ShardPilotSdk.ClearHostRoots(false);
                ShardPilotDispatcher.RetireHost();
            }
        }
    }
}
