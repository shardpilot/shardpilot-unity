// Input maps accept both existing non-null-value and nullable-value dictionaries.
// The invariant value argument stays oblivious; the optional map is annotated.
#nullable disable annotations
using EventProperties = System.Collections.Generic.IDictionary<string, object>;
#nullable enable annotations
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using UnityEngine;

namespace ShardPilot
{
    public static class ShardPilotSdk
    {
        /// <summary>Prepares one strict consent-screen decision without initializing the SDK.</summary>
        public static void PrepareConsentPolicy(ShardPilotConsentPolicyContext context, Action<ShardPilotConsentPolicyDecision> callback)
        {
            ShardPilotConsentPolicy.Prepare(context, callback);
        }

        /// <summary>Fences pending policy replies without initializing or changing consent.</summary>
        public static void InvalidateConsentPolicy() => ShardPilotConsentPolicy.Invalidate();

        /// <summary>Submits a non-objection declaration through the initialized client. The latest host call
        /// supersedes any pending one, which completes with non_objection_superseded.</summary>
        public static void RecordNonObjection(EventProperties? record, Action<ShardPilotResult> callback)
        {
            if (callback == null) throw new ArgumentNullException(nameof(callback));
            var client = Client;
            if (client != null) { client.RecordNonObjection(record, callback); return; }
            try { callback(new ShardPilotResult(false, "not_initialized", false)); } catch { }
        }

        /// <summary>
        /// Single fixed cache file: the payload itself is stamped with the
        /// (workspace, environment, client, remote-config URL) scope it was
        /// fetched for. The client component is empty for identifier-free
        /// requests; ShardPilotRemoteConfig treats a scope mismatch as a miss.
        /// </summary>
        public const string RemoteConfigCacheFileName = "shardpilot.remote-config.json";

        /// <summary>
        /// How long the previous client's fire-and-forget close may run when
        /// the SDK is re-initialized (its background drain delivers what it
        /// can; the remnant was already persisted synchronously up front).
        /// </summary>
        private static readonly TimeSpan ReplacedClientCloseTimeout = TimeSpan.FromSeconds(2);

        private static readonly object Gate = new object();
        private static ShardPilotClient? _client;

        private static TaskCompletionSource<ShardPilotResult> _closeCompletion;

        /// <summary>Test seam: invoked by <see cref="Initialize"/> at the
        /// exact moment the successor becomes the published client, still
        /// under the facade gate — the tightest window a late OLD-lane
        /// experiments response could target. Must not call back into the
        /// facade. No-op in production.</summary>
        internal static Action InitializeSuccessorPublishedHookForTesting;
        private static ShardPilotCrashHooks _crashHooks;
        private static ShardPilotRemoteConfig? _remoteConfig;

        public static ShardPilotClient? Client
        {
            get
            {
                lock (Gate)
                {
                    return _client;
                }
            }
        }

        public static ShardPilotRemoteConfig? RemoteConfig
        {
            get
            {
                lock (Gate)
                {
                    return _remoteConfig;
                }
            }
        }

        public static ShardPilotConsentState ConsentState
        {
            get
            {
                var client = Client;
                return client == null ? ShardPilotConsentState.Unknown : client.ConsentState;
            }
        }

        /// <summary>
        /// Static capability discovery, usable BEFORE <see cref="Initialize"/>
        /// and without version parsing: returns true iff this SDK build
        /// implements the named capability. Unknown names (and null) return
        /// false on older and newer SDKs alike, so integrations can
        /// feature-detect abilities that are not new methods — e.g. the
        /// forced-minor consent decision, which is a new VALUE on an
        /// existing call. Capability KEY STRINGS are canonical cross-SDK
        /// exactly as written (snake_case), regardless of host API naming:
        /// - <c>consent_receipt_outbox</c>: consent receipts are retained in
        ///   a durable per-app outbox, survive process death, and retry
        ///   until acknowledged.
        /// - <c>consent_state_denied_forced_minor</c>: SetConsent accepts
        ///   the forced-minor decision; its receipt records
        ///   <c>reason = "denied_forced_minor"</c>.
        /// - <c>experiments_assignment</c>: consumes experiment assignments.
        /// - <c>experiments_age_band</c>: accepts a declared assignment age band.
        /// - <c>event_schema_version</c>: validates and preserves a per-event version.
        /// - <c>schema_revision_declaration</c>: false; engine builds do not declare it.
        /// - <c>consent_policy_advisory</c>: prepares validated advisory reference snapshots;
        ///   it does not promise non-objection recording or analytics admission.
        /// - <c>analytics_non_objection</c>: a current recorded ACK admits ordinary analytics
        ///   in process while consent stays Unknown; special consumers require a grant.
        /// </summary>
        public static bool Supports(string? capability)
        {
            switch (capability)
            {
                case "consent_receipt_outbox":
                case "consent_policy_advisory":
                case "analytics_non_objection":
                case "consent_state_denied_forced_minor":
                case "experiments_age_band":
                // The experiment-assignment consumer exists: the config knob
                // ExperimentsEnabled (default false — dark) and the
                // FetchExperimentAssignment / GetExperimentVariant /
                // GetExperimentPayload / TrackExperimentExposure /
                // TrackExperimentOutcome surface. Detectable here because a
                // config field unknown to an older SDK would be silently
                // ignored, and the enablement preconditions live
                // server-side.
                case "event_schema_version":
                case "experiments_assignment":
                    return true;
                default:
                    return false;
            }
        }

        /// <summary>
        /// Initializes the SDK. Must be called on the Unity main thread: it
        /// resolves the persisted anonymous id and consent state through
        /// PlayerPrefs and prepares the main-thread web request dispatcher.
        ///
        /// The config is validated FIRST, before anything is torn down: an
        /// invalid config throws and leaves a previously initialized client
        /// fully running (telemetry never goes dark over a bad
        /// re-initialization).
        ///
        /// RE-initialization is one atomic handoff: Initialize holds the
        /// facade gate across the whole critical section — persist the old
        /// client's remnant, construct the replacement (which loads that
        /// remnant and takes spool-file ownership), swap the facade
        /// reference and crash hooks — so a concurrent SDK call briefly
        /// WAITS (one spool load, typically tens of milliseconds, and only
        /// on re-initialization) instead of racing the handoff and landing
        /// on a client whose spool was just disowned. After the swap, the
        /// old client's close runs fire-and-forget in the background (never
        /// awaited: with the default transport the close's delivery drain is
        /// pumped by this same main thread, so waiting here could only ever
        /// burn its timeout); anything it still delivers in parallel is
        /// de-duplicated by the ingest service against the new client's
        /// re-send (stable event id). Direct multi-client construction
        /// outside this facade keeps the documented ownership-handoff
        /// semantics (the handoff runs before the successor loads, but
        /// concurrent Tracks are not serialized).
        ///
        /// Lock order (acyclic, verified): facade gate -> lifecycle registry
        /// -> client persist gate -> client gate -> spool lock. Nothing that
        /// runs under the inner locks calls back into this facade — clients,
        /// the lifecycle host, and the crash hooks all hold direct client
        /// references.
        /// </summary>
        public static ShardPilotClient Initialize(ShardPilotConfig config)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            // The same side-effect-free normalization/validation the client
            // constructor performs on its own copy, run up front: an invalid
            // config must throw HERE, while the previous client (if any) is
            // still fully in place. The returned clone is discarded — the
            // constructor re-runs this on construction.
            config.CloneWithDefaults();

            ShardPilotClient client;
            ShardPilotClient previous;
            using (ShardPilotHandoffNotices.Hold())
            lock (Gate)
            {
                previous = _client;
                if (previous != null)
                {
                    // The replaced client's experiments automatic lane stops
                    // FIRST — before the successor even exists, let alone is
                    // reachable. Experiment response handling never takes
                    // this facade gate, so a disown that waited until after
                    // the swap would leave a window where a late old-lane
                    // revalidation response could still install into the
                    // shared assignment cache the successor already owns.
                    // Owed housekeeping is unaffected: the persist below and
                    // the background close still drain what is owed.
                    previous.DisownExperimentsForHandoff();

                    // Persist the old remnant synchronously (file write only,
                    // no network) BEFORE the replacement exists — this is
                    // what hands the remnant over, since the new client's
                    // construction loads the spool.
                    previous.PersistPendingEvents();
                }

                client = new ShardPilotClient(config);
                if (client.HostRetiring) return client;
                if (previous != null)
                {
                    // Destructive experiment cache debts survive the
                    // replacement: an authoritative drop (or owed sentinel
                    // clear) whose durable write failed on the old client
                    // re-arms under the successor's machinery — otherwise a
                    // stale assignment still on disk would outlive the
                    // server's verdict purely because the client was
                    // replaced before storage healed. Runs BEFORE the
                    // successor is published so nothing can observe the
                    // restored-but-condemned state.
                    client.AdoptExperimentDebtsFromPredecessor(previous);
                }

                _client = client;
                _closeCompletion = null;

                // Test seam: runs at the exact moment the successor became
                // the published client, still under the facade gate — the
                // tightest window a late old-lane response could target.
                // No-op in production.
                var successorPublished = InitializeSuccessorPublishedHookForTesting;
                if (successorPublished != null)
                {
                    successorPublished();
                }

                _remoteConfig = client.CreateRemoteConfig(RemoteConfigCachePath(), null);
                if (_crashHooks != null)
                {
                    _crashHooks.Dispose();
                    _crashHooks = null;
                }

                if (client.CrashPlaneConfig.CrashHooksEnabled)
                {
                    _crashHooks = ShardPilotCrashHooks.Install(client);
                }
            }

            if (previous != null)
            {
                // The replaced client's warning, outside the gate and after
                // the successor adopted its parked debts (#159). A
                // registration inside the gate may already have logged it;
                // it is logged once.
                previous.EmitReplacedNoticeIfOwed();

                // Close outside the gate without waiting for the drain.
                // The old client can remove spool records only while it
                // owns the file; removal after transfer is a no-op. Ingest
                // deduplicates retries. Experiment ownership is released
                // inside the gate before the successor is constructed.
                previous.CloseAsync(ReplacedClientCloseTimeout);
            }

            return client;
        }

        public static bool Track(string eventName, EventProperties? props = null)
        {
            var client = Client;
            return client != null && client.Track(eventName, props);
        }

        public static ShardPilotResult Track(string eventName, EventProperties? props,
            ShardPilotTrackOptions? options)
        {
            var client = Client;
            return client == null
                ? new ShardPilotResult(false, ShardPilotFlushCodes.NotInitialized, false)
                : client.Track(eventName, props, options);
        }

        public static bool SessionStart(string? sessionId = null, EventProperties? props = null)
        {
            var client = Client;
            return client != null && client.SessionStart(sessionId, props);
        }

        /// <summary>
        /// Ends the active session on the singleton client, emitting
        /// <c>app.session_ended</c> — the manual counterpart to
        /// <see cref="SessionStart"/>. Returns true without an event when no
        /// session is active, and false when the SDK is not initialized.
        /// </summary>
        /// <remarks>
        /// Present because a facade that can OPEN a session and not close one is a
        /// dead end for any consumer who does not retain the client returned by
        /// Initialize — which is the documented singleton usage.
        /// </remarks>
        public static bool SessionEnd(string? reason = null)
        {
            var client = Client;
            return client != null && client.SessionEnd(reason);
        }

        /// <summary>
        /// Records an ordinary grant or denial on the active client. Success says
        /// whether it applied; Error names a refusal and Warnings names unfinished work.
        /// Without an initialized client, the result fails with not_initialized.
        /// Call from the Unity main thread.
        /// </summary>
        public static ShardPilotResult SetConsent(bool analyticsGranted)
        {
            return SetConsent(analyticsGranted, (ShardPilotConsentNotice?)null);
        }

        /// <summary>Records a decision with optional per-call notice provenance.</summary>
        public static ShardPilotResult SetConsent(bool analyticsGranted, ShardPilotConsentNotice? notice = null)
        {
            var client = Client;
            if (client == null)
            {
                return new ShardPilotResult(false, "not_initialized", false);
            }

            return client.SetConsent(analyticsGranted, notice);
        }

        /// <summary>
        /// Records an explicit decision using the same application result as every
        /// other overload. Forced-minor denial cannot be reversed by an ordinary grant.
        /// An ordinary denial retains its original state and receipt. Call from the Unity main thread.
        /// </summary>
        public static ShardPilotResult SetConsent(ShardPilotConsentDecision decision)
        {
            return SetConsent(decision, (ShardPilotConsentNotice?)null);
        }

        /// <summary>Records a decision with optional per-call notice provenance.</summary>
        public static ShardPilotResult SetConsent(ShardPilotConsentDecision decision, ShardPilotConsentNotice? notice = null)
        {
            var client = Client;
            return client == null ? new ShardPilotResult(false, "not_initialized", false) : client.SetConsent(decision, notice);
        }

        /// <summary>
        /// Records an explicit decision and also returns its diagnostic code.
        /// The result distinguishes refusal Error from applied-decision Warnings;
        /// the additional output can name either. Call from the Unity main thread.
        /// </summary>
        public static ShardPilotResult SetConsent(ShardPilotConsentDecision decision, out string? error)
        {
            var client = Client;
            if (client == null)
            {
                error = "not_initialized";
                return new ShardPilotResult(false, error, false);
            }

            return client.SetConsent(decision, out error);
        }

        /// <summary>
        /// Records the crash-reporting opt-in/opt-out decision on the active
        /// client — the crash plane's own persisted setting, independent of
        /// analytics consent (crash reporting is ON by default as a
        /// legitimate-interest plane). See
        /// <see cref="ShardPilotClient.SetCrashReportingEnabled"/>. Call
        /// from the Unity main thread. Returns false when the SDK is not
        /// initialized, the client is closed, or the decision could not be
        /// durably persisted (it still applies in memory — call again to
        /// retry persistence).
        /// </summary>
        public static bool SetCrashReportingEnabled(bool enabled)
        {
            var client = Client;
            return client != null && client.SetCrashReportingEnabled(enabled);
        }

        /// <summary>
        /// Whether crash reporting is currently enabled on the active
        /// client. While disabled, <paramref name="disabledReason"/> carries
        /// <c>"not_configured"</c> when the client was initialized without a
        /// <see cref="ShardPilotConfig.CrashKey"/> (crash reporting is off
        /// by configuration), <c>"consent_denied"</c> while analytics
        /// consent is explicitly denied (a denial suppresses crash capture
        /// too), <c>"opt_out"</c>, <c>"settings_read_failed"</c> (fail
        /// closed), or <c>"not_initialized"</c> when no client exists.
        /// </summary>
        public static bool IsCrashReportingEnabled(out string? disabledReason)
        {
            var client = Client;
            if (client == null)
            {
                disabledReason = "not_initialized";
                return false;
            }

            return client.IsCrashReportingEnabled(out disabledReason);
        }

        /// <summary>
        /// Fetches remote configuration from the remote-config endpoint. Requires
        /// AppUrl and RemoteConfigEnabled to be set at Initialize. Returns false
        /// when no fetch starts. A live client still receives a queued refusal;
        /// before Initialize or without a live captured host no callback runs.
        /// </summary>
        public static bool FetchRemoteConfig(Action<ShardPilotRemoteConfigResult>? callback)
        {
            var remoteConfig = RemoteConfig;
            if (remoteConfig == null)
            {
                Client?.RemoteConfigRefused(callback);
                return false;
            }

            return remoteConfig.FetchRemoteConfig(callback);
        }

        /// <summary>
        /// Stores the targeting attributes the remote-config fetch MAY carry
        /// (a defensive copy — null or empty clears).
        /// OPT-IN + CONSENT-GATED: attributes ride a fetch ONLY while
        /// <see cref="ShardPilotConfig.RemoteConfigAttributesEnabled"/>
        /// (default false — dark) is true AND analytics consent is Granted
        /// at fetch time; under unknown or denied consent (forced-minor
        /// included), identifiers and attributes are omitted. Disabling
        /// the opt-in omits attributes. Normalized per fetch
        /// with the shared experiment-attribute vocabulary — see
        /// <see cref="ShardPilotRemoteConfig.SetRemoteConfigAttributes"/>.
        /// Returns false when the SDK is not initialized or remote config is
        /// not configured.
        /// </summary>
        public static bool SetRemoteConfigAttributes(IDictionary<string, string>? attributes)
        {
            var remoteConfig = RemoteConfig;
            if (remoteConfig == null)
            {
                return false;
            }

            remoteConfig.SetRemoteConfigAttributes(attributes);
            return true;
        }

        /// <summary>
        /// Fetches the server-evaluated assignment for one experiment.
        /// Requires
        /// <see cref="ShardPilotConfig.ExperimentsEnabled"/> (default false —
        /// dark) at Initialize. The assignment plane is granted-only: under
        /// unknown or denied consent the callback receives the distinct
        /// refusal instead of any network traffic. Returns false when no fetch
        /// starts; live-client refusals notify on the captured host thread.
        /// Before Initialize or after host/owner retirement no callback runs.
        /// </summary>
        public static bool FetchExperimentAssignment(
            string experimentKey,
            IDictionary<string, string>? attributes,
            Action<ShardPilotExperimentResult>? callback)
        {
            var client = Client;
            if (client == null)
            {
                return false;
            }

            return client.FetchExperimentAssignment(experimentKey, attributes, callback);
        }

        public static bool FetchExperimentAssignment(
            string experimentKey,
            Action<ShardPilotExperimentResult>? callback)
        {
            return FetchExperimentAssignment(experimentKey, null, callback);
        }

        /// <summary>The cached variant key for the experiment, or null when
        /// none is served (not initialized, experiments off, nothing
        /// fetched, not assigned, or consent not granted) — game code ships
        /// one code path with the control experience as its default.</summary>
        public static string? GetExperimentVariant(string experimentKey)
        {
            var client = Client;
            return client == null ? null : client.GetExperimentVariant(experimentKey);
        }

        /// <summary>The cached variant payload (a defensive copy), or null.
        /// Same posture as <see cref="GetExperimentVariant"/>.</summary>
        public static IReadOnlyDictionary<string, object?>? GetExperimentPayload(string experimentKey)
        {
            var client = Client;
            return client == null ? null : client.GetExperimentPayload(experimentKey);
        }

        /// <summary>Applies the cached variant and records one occurrence per session and assignment tuple.</summary>
        public static string? ApplyExperimentVariant(string experimentKey)
        {
            var client = Client;
            return client == null ? null : client.ApplyExperimentVariant(experimentKey);
        }

        /// <summary>Applies the cached payload through the same occurrence slot as ApplyExperimentVariant.</summary>
        public static IReadOnlyDictionary<string, object?>? ApplyExperimentPayload(string experimentKey)
        {
            var client = Client;
            return client == null ? null : client.ApplyExperimentPayload(experimentKey);
        }

        /// <summary>Record an additional application of the cached assignment.
        /// Returns after local capture; CP sealing and analytics delivery are asynchronous.</summary>
        public static bool TrackExperimentExposure(string experimentKey)
        {
            var client = Client;
            return client != null && client.TrackExperimentExposure(experimentKey);
        }

        /// <summary>Record a safe-integer outcome for the last explicit application
        /// in the current session. CP sealing and analytics delivery are asynchronous.</summary>
        public static bool TrackExperimentOutcome(string experimentKey, string outcomeKey, double outcomeValue)
        {
            var client = Client;
            return client != null && client.TrackExperimentOutcome(experimentKey, outcomeKey, outcomeValue);
        }

        /// <summary>Typed progression verb on the singleton client: emits
        /// <c>level_start</c>. Returns false when the SDK is not initialized or
        /// the client refused (invalid input, consent, closed) — the client's
        /// own overload with <c>out string error</c> names the reason.</summary>
        public static bool TrackLevelStart(string levelId, int attempt, EventProperties? props = null)
        {
            var client = Client;
            return client != null && client.TrackLevelStart(levelId, attempt, props);
        }

        /// <summary>Typed progression verb on the singleton client: emits
        /// <c>level_complete</c>; the score is omitted from the wire when not
        /// given.</summary>
        public static bool TrackLevelComplete(string levelId, int attempt, long durationMs, long? score = null, EventProperties? props = null)
        {
            var client = Client;
            return client != null && client.TrackLevelComplete(levelId, attempt, durationMs, score, props);
        }

        /// <summary>Typed progression verb on the singleton client: emits
        /// <c>level_fail</c>; the reason is omitted from the wire when empty.</summary>
        public static bool TrackLevelFail(string levelId, int attempt, long durationMs, string? failReason = null, EventProperties? props = null)
        {
            var client = Client;
            return client != null && client.TrackLevelFail(levelId, attempt, durationMs, failReason, props);
        }

        /// <summary>Typed ad verb on the singleton client: emits
        /// <c>ad_impression_revenue</c>. Returns false when the SDK is not
        /// initialized or the client refused — the client's own overload with
        /// <c>out string error</c> names the reason. The ingest applies a
        /// SECOND consent gate this SDK cannot see (the workspace's
        /// <c>ad_revenue</c> posture) and suppresses the event
        /// per event inside an accepted batch when it is missing.</summary>
        public static bool TrackAdImpressionRevenue(ShardPilotAdImpressionRevenueArgs args)
        {
            var client = Client;
            return client != null && client.TrackAdImpressionRevenue(args);
        }

        /// <summary>Recent terminal rejections on the singleton client, oldest first.</summary>
        public static IReadOnlyList<ShardPilotRejection> LastRejections()
        {
            var client = Client;
            return client == null ? Array.AsReadOnly(new ShardPilotRejection[0]) : client.LastRejections();
        }

        public static Task<ShardPilotResult> FlushAsync(TimeSpan? timeout = null)
        {
            var client = Client;
            return client == null
                ? Task.FromResult(new ShardPilotResult(false, ShardPilotFlushCodes.NotInitialized, false))
                : client.FlushAsync(timeout);
        }

        public static Task<ShardPilotResult> CloseAsync(TimeSpan? timeout = null)
        {
            ShardPilotClient client;
            TaskCompletionSource<ShardPilotResult> completion;
            lock (Gate)
            {
                if (_closeCompletion != null) return _closeCompletion.Task;
                client = _client;
                if (client == null)
                    return Task.FromResult(new ShardPilotResult(false, "shardpilot client is not initialized", false));
                completion = new TaskCompletionSource<ShardPilotResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                _closeCompletion = completion;
                _client = null;
                _remoteConfig = null;
                if (_crashHooks != null)
                {
                    _crashHooks.Dispose();
                    _crashHooks = null;
                }
            }
            _ = CompleteCloseOperationAsync(client, completion, timeout);
            return completion.Task;
        }

        internal static object[] CaptureHostRoots()
        {
            lock (Gate) return new object[] { _client, _remoteConfig, _crashHooks };
        }
        internal static void ClearHostRoots(bool entering)
        {
            lock (Gate)
            {
                _client = null; _remoteConfig = null;
                _crashHooks?.Dispose(); _crashHooks = null;
                if (entering) _closeCompletion = null;
            }
        }

        internal static void DetachDisposedClient(ShardPilotClient client)
        {
            TaskCompletionSource<ShardPilotResult> completion;
            lock (Gate)
            {
                if (!ReferenceEquals(_client, client)) return;
                completion = new TaskCompletionSource<ShardPilotResult>(TaskCreationOptions.RunContinuationsAsynchronously);
                _closeCompletion = completion;
                _client = null;
                _remoteConfig = null;
                if (_crashHooks != null) { _crashHooks.Dispose(); _crashHooks = null; }
            }
            _ = CompleteCloseOperationAsync(client, completion, null);
        }

        private static async Task CompleteCloseOperationAsync(ShardPilotClient client,
            TaskCompletionSource<ShardPilotResult> completion, TimeSpan? timeout)
        {
            try { completion.TrySetResult(await client.CloseAsync(timeout).ConfigureAwait(false)); }
            catch (OperationCanceledException) { completion.TrySetCanceled(); }
            catch (Exception error) { completion.TrySetException(error); }
        }

        /// <summary>Initializes a test client with an explicit hook setting.</summary>
        internal static ShardPilotClient InitializeForTesting(ShardPilotConfig config, bool installCrashHooks)
        {
            if (config == null) throw new ArgumentNullException("config");
            var copy = config.CloneWithDefaults();
            copy.CrashHooksEnabled = installCrashHooks;
            return Initialize(copy);
        }

        /// <summary>
        /// Test seam: attaches an externally constructed client to the
        /// static facade (null detaches), bypassing
        /// <see cref="Initialize"/>'s real-store construction so facade
        /// close-task ownership can be pinned
        /// against injected stores and senders.
        /// <paramref name="installCrashHooks"/> also installs real crash
        /// hooks for the client, so close-time disposal and the absence of
        /// later reinstalls are testable too.
        /// </summary>
        internal static void AttachClientForTesting(ShardPilotClient client, bool installCrashHooks = false)
        {
            lock (Gate)
            {
                _client = client;

                // An explicit install starts a new facade close-task owner,
                // exactly like Initialize does.
                _closeCompletion = null;
                _remoteConfig = null;
                if (_crashHooks != null)
                {
                    _crashHooks.Dispose();
                    _crashHooks = null;
                }

                if (client != null && installCrashHooks)
                {
                    _crashHooks = ShardPilotCrashHooks.Install(client);
                }
            }
        }

        internal static ShardPilotCrashHooks CrashHooksForTesting
        {
            get
            {
                lock (Gate)
                {
                    return _crashHooks;
                }
            }
        }

        private static string RemoteConfigCachePath()
        {
            try
            {
                return Path.Combine(Application.persistentDataPath, RemoteConfigCacheFileName);
            }
            catch
            {
                return null;
            }
        }
    }
}
