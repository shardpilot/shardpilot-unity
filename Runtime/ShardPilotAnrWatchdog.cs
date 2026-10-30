using System;
using System.Collections.Generic;
using System.Globalization;

namespace ShardPilot
{
    /// <summary>
    /// The seam between the watchdog and whatever pumps the Unity main
    /// thread. Production wires <see cref="ShardPilotDispatcherMainThreadTicker"/>,
    /// which marshals every heartbeat through
    /// <see cref="ShardPilotDispatcher.RunOnMainThread"/> — the SDK's ONE
    /// main-thread pump; no second pump exists. Tests drive the core
    /// directly (no ticker at all) or install a fake through
    /// <see cref="ShardPilotAnrLane.TickerFactoryForTesting"/>.
    /// </summary>
    internal interface IShardPilotMainThreadTicker
    {
        /// <summary>Starts the recurring main-thread heartbeat that invokes <paramref name="onTick"/>.</summary>
        void Start(Action onTick);

        /// <summary>Stops ticking (idempotent).</summary>
        void Stop();
    }

    /// <summary>
    /// The production main-thread heartbeat: a lightweight off-main re-arm
    /// timer requests one tick at a time via
    /// <see cref="ShardPilotDispatcher.RunOnMainThread"/>, and the tick only
    /// executes when the dispatcher's Update pump drains it on the main
    /// thread — while the main thread is stalled the marshaled tick cannot
    /// run, which is precisely the signal. The re-arm MUST come from off
    /// the main thread: RunOnMainThread work enqueued during the pump's
    /// drain is executed by that same drain, so a tick that re-armed itself
    /// inline would extend the drain forever and Update could never return.
    /// At most one tick is in flight at a time, so a long stall accumulates
    /// exactly one queued heartbeat (the recovery frame runs one tick, not
    /// a stall-length backlog). The tick target self-guards (the watchdog's
    /// main-thread tick handler never lets an exception escape), so nothing
    /// here needs to catch. Never used by the headless harness:
    /// its shim pumps dispatcher work to completion inline on the calling
    /// thread, which cannot model a stalled-then-recovered frame loop.
    /// </summary>
    internal sealed class ShardPilotDispatcherMainThreadTicker : IShardPilotMainThreadTicker
    {
        /// <summary>
        /// Re-arm cadence: comfortably finer than any sane stall threshold
        /// (the config default is 5000 ms) at no measurable off-main cost.
        /// The effective heartbeat period is this interval or one frame,
        /// whichever is longer.
        /// </summary>
        internal const int RearmIntervalMs = 50;

        private readonly Action _tickOnce;
        private volatile bool _stopped;
        private Action _onTick;
        private System.Threading.Timer _rearm;
        private int _tickPending;

        public ShardPilotDispatcherMainThreadTicker()
        {
            _tickOnce = TickOnce;
        }

        public void Start(Action onTick)
        {
            _onTick = onTick;

            // Touching Instance here — on the main thread, exactly like the
            // pre-#33 coroutine adapter did — guarantees the dispatcher host
            // exists, so the best-effort RunOnMainThread marshal below
            // always has a live pump to deliver to.
            _ = ShardPilotDispatcher.Instance;

            _rearm = new System.Threading.Timer(OnRearmTimer, null, 0, RearmIntervalMs);
        }

        public void Stop()
        {
            _stopped = true;
            var rearm = _rearm;
            _rearm = null;
            if (rearm != null)
            {
                rearm.Dispose();
            }
        }

        private void OnRearmTimer(object state)
        {
            if (_stopped)
            {
                return;
            }

            if (System.Threading.Interlocked.CompareExchange(ref _tickPending, 1, 0) != 0)
            {
                return;
            }

            ShardPilotDispatcher.RunOnMainThread(_tickOnce);
        }

        private void TickOnce()
        {
            System.Threading.Interlocked.Exchange(ref _tickPending, 0);
            if (_stopped)
            {
                return;
            }

            var onTick = _onTick;
            if (onTick != null)
            {
                onTick();
            }
        }
    }

    /// <summary>
    /// One completed (recovered) main-thread stall episode.
    /// </summary>
    internal sealed class ShardPilotAnrStallReport
    {
        /// <summary>The last pre-stall heartbeat, on the watchdog's monotonic clock.</summary>
        public long StallStartMs;

        /// <summary>How long the main thread went without a heartbeat.</summary>
        public long DurationMs;

        public long ThresholdMs;
    }

    /// <summary>
    /// The watchdog's pure state machine — no threads, no clock of its own,
    /// so every rule is drivable with fake timestamps. Two inputs:
    /// <see cref="OnMainThreadTick"/> (the main-thread heartbeat) and
    /// <see cref="OnMonitorPoll"/> (any off-main observer).
    ///
    /// Detection and reporting are deliberately SPLIT: a poll that sees the
    /// heartbeat older than the threshold only FLAGS the stall; the report
    /// is produced by the next heartbeat — i.e. strictly AFTER the main
    /// thread recovered, in-session. A stall the process
    /// never recovers from produces nothing here by design: the system
    /// kills the app and the NEXT-LAUNCH ApplicationExitInfo lane reports
    /// it with the real ANR trace — the two lanes cannot double-report one
    /// incident. One report per episode (report-once), bounded per era.
    /// </summary>
    internal sealed class ShardPilotAnrWatchdogCore
    {
        /// <summary>
        /// Hard per-era bound on stall reports — a defensive lid on a
        /// pathological stall-recover loop (loading screens); the non-fatal
        /// sample rate bounds the wire volume further.
        /// </summary>
        internal const int MaxReportsPerEra = 10;

        private readonly object _gate = new object();
        private readonly long _thresholdMs;
        private long _lastTickMs;
        private bool _started;
        private bool _stalled;
        private bool _suspended;
        private long _stallStartMs;
        private int _reportsEmitted;

        public ShardPilotAnrWatchdogCore(long thresholdMs)
        {
            _thresholdMs = thresholdMs > 0 ? thresholdMs : (int)ShardPilotConfig.DefaultHangWatchdogThreshold.TotalMilliseconds;
        }

        public long ThresholdMs
        {
            get { return _thresholdMs; }
        }

        /// <summary>Arms the core; the first heartbeat baseline is <paramref name="nowMs"/>.</summary>
        public void Start(long nowMs)
        {
            lock (_gate)
            {
                _started = true;
                _lastTickMs = nowMs;
                _stalled = false;
                _suspended = false;
            }
        }

        /// <summary>
        /// Lifecycle suspension (the app paused/backgrounded): the engine
        /// stops pumping frames, so a silent heartbeat is EXPECTED — the
        /// monitor must never read suspension as a stall. A stall flag
        /// already pending is dropped: its recovery heartbeat can no longer
        /// be told apart from the resume frame, and a lifecycle transition
        /// must not manufacture an ANR report. A stall that persists across
        /// the pause is re-detected by post-resume misses against the fresh
        /// <see cref="Resume"/> baseline.
        /// </summary>
        public void Suspend()
        {
            lock (_gate)
            {
                _suspended = true;
                _stalled = false;
            }
        }

        /// <summary>
        /// Ends a lifecycle suspension: re-arms detection against a FRESH
        /// heartbeat baseline (<paramref name="nowMs"/>), so time spent in
        /// the background can never flag the first resumed frame. A resume
        /// without a matching suspension (some players deliver a spurious
        /// initial un-pause) is a no-op — it must not reset an armed
        /// baseline or clear a genuinely pending stall.
        /// </summary>
        public void Resume(long nowMs)
        {
            lock (_gate)
            {
                if (!_suspended)
                {
                    return;
                }

                _suspended = false;
                _stalled = false;
                _lastTickMs = nowMs;
            }
        }

        /// <summary>
        /// The off-main observation: flags a stall once the heartbeat is at
        /// least the threshold old. NEVER reports — the process may be
        /// about to die, and this lane only reports recoveries. While
        /// suspended it is a no-op: the poll timer may keep firing in the
        /// background, but lifecycle suspension is not a stall.
        /// </summary>
        public void OnMonitorPoll(long nowMs)
        {
            lock (_gate)
            {
                if (!_started || _stalled || _suspended)
                {
                    return;
                }

                if (nowMs - _lastTickMs >= _thresholdMs)
                {
                    _stalled = true;
                    _stallStartMs = _lastTickMs;
                }
            }
        }

        /// <summary>
        /// The main-thread heartbeat. Returns a report exactly once per
        /// flagged episode — on the first heartbeat AFTER the stall, i.e.
        /// after recovery — and null otherwise.
        /// </summary>
        public ShardPilotAnrStallReport OnMainThreadTick(long nowMs)
        {
            lock (_gate)
            {
                if (!_started)
                {
                    return null;
                }

                if (_suspended)
                {
                    // A heartbeat delivered while suspended (the pause
                    // callback and an already-marshaled tick can share the
                    // final frame) only refreshes the baseline — never a
                    // report.
                    _lastTickMs = nowMs;
                    return null;
                }

                if (!_stalled)
                {
                    _lastTickMs = nowMs;
                    return null;
                }

                var report = new ShardPilotAnrStallReport
                {
                    StallStartMs = _stallStartMs,
                    DurationMs = nowMs - _stallStartMs,
                    ThresholdMs = _thresholdMs
                };
                _stalled = false;
                _lastTickMs = nowMs;

                if (_reportsEmitted >= MaxReportsPerEra)
                {
                    return null;
                }

                _reportsEmitted++;
                return report;
            }
        }
    }

    /// <summary>
    /// The production watchdog: wires the core to a main-thread ticker and
    /// a monitor timer, and turns recovered-stall reports into crash-plane
    /// events through the client's existing consent-gated capture path.
    ///
    /// Honest capture limits, stated plainly: pure C# has no safe way to
    /// capture ANOTHER thread's managed stack — the stalled main thread's
    /// frames would need debugger/thread-suspension APIs this SDK
    /// deliberately does not use — so the report carries the stall
    /// duration, the watchdog metadata, and the SDK's own breadcrumb-class
    /// context (the ring holds what led INTO the stall), not frames from
    /// inside the stall. Reports are NON-fatal (the session survived) and
    /// therefore ride the configured non-fatal sample rate like every other
    /// handled-error report.
    /// </summary>
    internal sealed class ShardPilotAnrWatchdog
    {
        /// <summary>Monitor poll cadence: a quarter threshold, at most once a second.</summary>
        internal static long PollIntervalMsFor(long thresholdMs)
        {
            var interval = thresholdMs / 4;
            if (interval < 100)
            {
                interval = 100;
            }

            return interval > 1000 ? 1000 : interval;
        }

        private readonly ShardPilotClient _client;
        private readonly ShardPilotAnrWatchdogCore _core;
        private readonly IShardPilotMainThreadTicker _ticker;
        private readonly Func<long> _monotonicNowMs;
        private readonly string _eraId;
        private System.Threading.Timer _timer;

        public ShardPilotAnrWatchdog(
            ShardPilotClient client,
            string eraId,
            long thresholdMs,
            IShardPilotMainThreadTicker ticker,
            Func<long> monotonicNowMs)
        {
            _client = client;
            _eraId = eraId ?? string.Empty;
            _core = new ShardPilotAnrWatchdogCore(thresholdMs);
            _ticker = ticker;
            _monotonicNowMs = monotonicNowMs ?? MonotonicNowMs;
        }

        internal ShardPilotAnrWatchdogCore CoreForTesting
        {
            get { return _core; }
        }

        public void Start()
        {
            _core.Start(_monotonicNowMs());

            // Pause/background gating: the monitor timer
            // keeps firing while the engine's frame pump is lifecycle-
            // suspended, so without this gate a backgrounded app would read
            // as a stall and the first resumed frame would emit a fake
            // recovered-ANR report. The dispatcher host behaviour is the
            // SDK's one observer of OnApplicationPause; its crash-plane
            // hook fans the transitions into the core (suspend on pause,
            // fresh-baseline re-arm on resume). Focus loss WITHOUT a pause
            // needs no gate: the frame loop keeps pumping heartbeats.
            ShardPilotDispatcher.PauseStateChanged += OnPauseStateChanged;

            _ticker.Start(OnMainThreadTick);
            var interval = TimeSpan.FromMilliseconds(PollIntervalMsFor(_core.ThresholdMs));
            _timer = new System.Threading.Timer(OnMonitorTimer, null, interval, interval);
        }

        public void Stop()
        {
            try
            {
                ShardPilotDispatcher.PauseStateChanged -= OnPauseStateChanged;
            }
            catch
            {
                // Teardown must never throw.
            }

            try
            {
                _ticker.Stop();
            }
            catch
            {
                // Teardown must never throw.
            }

            var timer = _timer;
            _timer = null;
            if (timer != null)
            {
                try
                {
                    timer.Dispose();
                }
                catch
                {
                    // Teardown must never throw.
                }
            }
        }

        private void OnMonitorTimer(object state)
        {
            try
            {
                _core.OnMonitorPoll(_monotonicNowMs());
            }
            catch
            {
                // The monitor must never throw into the timer thread.
            }
        }

        /// <summary>
        /// The app's pause lifecycle, delivered on the main thread by the
        /// dispatcher host behaviour's <c>OnApplicationPause</c>. Internal
        /// so the suite can drive the transition without an engine.
        /// </summary>
        internal void OnPauseStateChanged(bool paused)
        {
            try
            {
                if (paused)
                {
                    _core.Suspend();
                }
                else
                {
                    _core.Resume(_monotonicNowMs());
                }
            }
            catch
            {
                // A lifecycle fan-out must never throw into the engine loop.
            }
        }

        private void OnMainThreadTick()
        {
            ShardPilotAnrStallReport report;
            try
            {
                report = _core.OnMainThreadTick(_monotonicNowMs());
            }
            catch
            {
                return;
            }

            if (report != null)
            {
                Emit(report);
            }
        }

        private void Emit(ShardPilotAnrStallReport report)
        {
            try
            {
                // Emitted from the RECOVERY heartbeat, i.e. on the main
                // thread, through the client's full consent-gated capture
                // path: the crash opt-out, the fail-closed read-failure
                // hold, and an analytics-consent denial all refuse it there.
                _client.CaptureCrash(BuildEvent(report, _eraId, DateTime.UtcNow));
            }
            catch
            {
                // The watchdog must never throw into the engine loop.
            }
        }

        /// <summary>
        /// Builds the recovered-stall event: kind "anr", the deterministic
        /// (era, stall-start) aei_record_key, the stall metrics as validated
        /// context entries, and the current session's normal attribution
        /// (an in-session report belongs to the CURRENT era — no era
        /// override). Static and pure for the harness.
        /// </summary>
        internal static ShardPilotCrashEvent BuildEvent(ShardPilotAnrStallReport report, string eraId, DateTime nowUtc)
        {
            var duration = report.DurationMs < 0 ? 0 : report.DurationMs;
            return new ShardPilotCrashEvent
            {
                CrashType = "ANR",
                ExceptionReason = "main thread stalled for " +
                                  duration.ToString(CultureInfo.InvariantCulture) +
                                  " ms and recovered",
                Fatal = false,
                Kind = ShardPilotAnrIds.AnrKind,
                AeiRecordKey = ShardPilotAnrIds.WatchdogRecordKeyFor(eraId, report.StallStartMs),
                OccurredAtUtc = nowUtc.AddMilliseconds(-duration),
                ThreadState = "main",
                Context = new Dictionary<string, string>
                {
                    {"anr_source", "watchdog"},
                    {"anr_stall_ms", duration.ToString(CultureInfo.InvariantCulture)},
                    {"anr_threshold_ms", report.ThresholdMs.ToString(CultureInfo.InvariantCulture)}
                }
            };
        }

        /// <summary>
        /// The production monotonic clock: Stopwatch ticks scaled to ms —
        /// immune to wall-clock steps (NTP, timezone, manual changes),
        /// which must never fake or hide a stall.
        /// </summary>
        internal static long MonotonicNowMs()
        {
            return (long)(System.Diagnostics.Stopwatch.GetTimestamp() /
                          (double)System.Diagnostics.Stopwatch.Frequency * 1000.0);
        }
    }
}
