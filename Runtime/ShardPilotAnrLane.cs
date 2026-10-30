using System;
using UnityEngine;

namespace ShardPilot
{
    /// <summary>
    /// Production wiring for the Android ANR lane, started
    /// from the crash hooks at SDK init: the per-process ERA SNAPSHOT
    /// persist, the next-launch ApplicationExitInfo pass, and the opt-in
    /// main-thread stall watchdog. Every piece is individually flag-gated
    /// in static config (both flags default OFF — the lane ships dark),
    /// Android-only, and fail-safe: nothing here may throw into
    /// <c>ShardPilotCrashHooks.Install</c>.
    /// </summary>
    internal sealed class ShardPilotAnrLane
    {
        /// <summary>
        /// Test seams (headless harness only; never assigned in
        /// production): the key-value store, the AEI source, and the
        /// watchdog's main-thread ticker factory through
        /// <see cref="IShardPilotMainThreadTicker"/>.
        /// </summary>
        internal static IShardPilotKeyValueStore StoreOverrideForTesting;
        internal static IShardPilotAeiSource SourceOverrideForTesting;
        internal static Func<IShardPilotMainThreadTicker> TickerFactoryForTesting;

        /// <summary>
        /// Test seam for the platform gate. UnityEngine's
        /// <c>Application.platform</c> is a GET-ONLY property in a real
        /// player/Editor build, so tests must inject the answer here rather
        /// than assign the property (an assignment compiles only against the
        /// headless shim's field and would break the license-gated
        /// Editor/GameCI compile). Null in production: the real
        /// <see cref="Application.platform"/> check runs.
        /// </summary>
        internal static Func<bool> IsAndroidOverrideForTesting;

        private readonly ShardPilotAnrWatchdog _watchdog;

        private ShardPilotAnrLane(ShardPilotAnrWatchdog watchdog)
        {
            _watchdog = watchdog;
        }

        /// <summary>
        /// Starts whatever the flags enable. Returns null when nothing
        /// long-lived started (flags dark, non-Android, no crash key, or
        /// only the one-shot AEI pass ran). Never throws.
        /// </summary>
        public static ShardPilotAnrLane TryStart(ShardPilotClient client)
        {
            try
            {
                if (client == null)
                {
                    return null;
                }

                var config = client.CrashPlaneConfig;
                if (config == null ||
                    (!config.AnrExitInfoEnabled && !config.NativeCrashExitInfoEnabled && !config.HangWatchdogEnabled))
                {
                    // All flags dark — ZERO behavior: no store touch, no
                    // JNI, no era snapshot, no ticker.
                    return null;
                }

                if (!IsAndroidPlatform())
                {
                    // The ANR lane is the ANDROID leg; other
                    // platforms' stall detection uses the hang watchdog.
                    return null;
                }

                if (string.IsNullOrEmpty(config.CrashKey))
                {
                    // No crash plane configured: nothing could ever send.
                    return null;
                }

                Action<string> log = message => SafeLog(config, message);
                var store = StoreOverrideForTesting ?? new ShardPilotUnityKeyValueStore();
                var eraId = ShardPilotIds.NewUuidV7();
                var nowUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                string disabledReason;
                var crashPlaneOpen = client.IsCrashReportingEnabled(out disabledReason);

                // The AEI sweep is SHARED by the ANR and native-crash lanes
                // (one cursor, one era store, one pass); each REASON still
                // reports only under its own flag inside the reporter.
                if (config.AnrExitInfoEnabled || config.NativeCrashExitInfoEnabled)
                {
                    ShardPilotAnrEraStore eraStore = null;
                    if (crashPlaneOpen && config.SpoolEnabled)
                    {
                        // The CURRENT era's snapshot is persisted first —
                        // before the pass, so a crash inside the pass still
                        // leaves this era attributable next launch — and
                        // only while the plane is open: a disabled era
                        // collects no actor inputs, and its records are
                        // suppressed at report time anyway.
                        //
                        // SpoolEnabled is the on-disk durability gate for
                        // the whole SDK (the client constructs neither
                        // spool without it), and the era store is gated the
                        // same way AT CONSTRUCTION, mirroring how the
                        // spools themselves are gated: with SpoolEnabled
                        // off, NO era file is ever created — no actor data
                        // on disk — so AEI attribution is always
                        // device-scoped anonymous in that configuration.
                        // The AEI CURSOR (and the suppression/owed markers)
                        // live in the KEY-VALUE store, not the spool
                        // directory, so replay suppression still works;
                        // only attribution degrades. The revocation-time
                        // era WIPES stay unconditional — they must destroy
                        // files a previous SpoolEnabled=true install left
                        // behind.
                        eraStore = ShardPilotAnrEraStore.TryCreate(config, store, log);
                        if (eraStore != null)
                        {
                            eraStore.PersistEra(new ShardPilotAnrEraRecord
                            {
                                EraId = eraId,
                                StartedAtMs = nowUnixMs,
                                AnonymousId = client.AnonymousId,
                                // THIS launch's build, frozen into the era
                                // snapshot: a next-launch AEI report for an
                                // exit in this era must carry the build
                                // that CRASHED — after an app update the
                                // reporting launch's config would attribute
                                // the ANR to the wrong version.
                                AppVersion = config.AppVersion,
                                BuildId = config.AppBuild,
                                ConsentState = ConsentStateName(client.ConsentState),
                                CrashEnabled = true
                            });
                        }
                    }

                    var source = SourceOverrideForTesting ?? CreatePlatformSource();

                    // The pass receives THIS launch's era id to EXCLUDE from
                    // AEI attribution: every exit record it reads predates
                    // this process, so under a rolled-back wall clock (an old
                    // exit timestamp at/after this era's start) the
                    // largest-start-at-or-before rule must fall through to a
                    // PRE-existing era or device-scoped anonymous — never
                    // borrow the new launch's identity.
                    ShardPilotAnrExitInfoReporter.RunNextLaunchPass(
                        client, config, source, store, eraStore, eraId, nowUnixMs, log);
                }

                ShardPilotAnrWatchdog watchdog = null;
                if (config.HangWatchdogEnabled && crashPlaneOpen)
                {
                    var factory = TickerFactoryForTesting;
                    var ticker = factory != null
                        ? factory()
                        : new ShardPilotDispatcherMainThreadTicker();
                    watchdog = new ShardPilotAnrWatchdog(
                        client, eraId, (int)Math.Min(int.MaxValue, config.HangWatchdogThreshold.TotalMilliseconds), ticker, null);
                    watchdog.Start();
                }

                return watchdog == null ? null : new ShardPilotAnrLane(watchdog);
            }
            catch
            {
                return null;
            }
        }

        /// <summary>Stops the watchdog (hook disposal / SDK re-init). Never throws.</summary>
        public void Stop()
        {
            try
            {
                if (_watchdog != null)
                {
                    _watchdog.Stop();
                }
            }
            catch
            {
                // Teardown must never throw.
            }
        }

        private static string ConsentStateName(ShardPilotConsentState state)
        {
            switch (state)
            {
                case ShardPilotConsentState.Granted:
                    return "granted";
                case ShardPilotConsentState.Denied:
                    return "denied";
                default:
                    return "unknown";
            }
        }

        private static bool IsAndroidPlatform()
        {
            var isAndroidOverride = IsAndroidOverrideForTesting;
            if (isAndroidOverride != null)
            {
                return isAndroidOverride();
            }

            return Application.platform == RuntimePlatform.Android;
        }

        private static IShardPilotAeiSource CreatePlatformSource()
        {
#if UNITY_ANDROID
            return new ShardPilotAndroidAeiSource();
#else
            return null;
#endif
        }

        private static void SafeLog(ShardPilotConfig config, string message)
        {
            ShardPilotDiagnostics.Log(config, ShardPilotLogLevel.Warn, message);
        }
    }
}
