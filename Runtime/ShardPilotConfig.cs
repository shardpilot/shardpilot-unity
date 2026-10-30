#nullable enable annotations
using System;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

namespace ShardPilot
{
    public enum ShardPilotSource
    {
        Client,
        Server,
        Backend
    }

    public sealed class ShardPilotConfig
    {
        public const int DefaultBatchSize = 25;
        public const int MaxBatchSize = 100;
        public const int DefaultBufferSize = 1000;
        public const int DefaultSpoolMaxEvents = 2000;
        public const long DefaultSpoolMaxBytes = 1024 * 1024;
        public const int DefaultCrashSpoolMaxReports = 50;
        public const long DefaultCrashSpoolMaxBytes = 512 * 1024;
        public const int DefaultCrashSampleEvery = 10;
        public static readonly TimeSpan DefaultHangWatchdogThreshold = TimeSpan.FromSeconds(5);

        /// <summary>
        /// How long a PARTIAL batch waits before it is published. Was ONE
        /// SECOND, which defeats batching outright at ordinary game event
        /// rates: a title emitting an event every few seconds never reaches
        /// <see cref="BatchSize"/> inside the window, so every event became its
        /// own HTTP request, with its own TLS handshake and radio wake. 15s is
        /// the low end of the cross-SDK 15–30s band.
        /// </summary>
        /// <remarks>
        /// <para>
        /// It is NOT a heartbeat: an empty batch publishes nothing, so a game
        /// that tracks nothing makes no requests at either value. What the old
        /// default cost was one request per event instead of one per batch, and
        /// a worker wake per second on a device running on battery.
        /// </para>
        /// <para>
        /// <see cref="BatchSize"/> is unchanged, so a busy game still publishes
        /// the moment it has a full batch, and <c>Flush</c> still publishes on
        /// demand. Consent-receipt retries and crash re-sends do not lengthen
        /// with it either — the worker caps its wait to those deadlines
        /// (<c>CapWaitToConsentRetryDeadline</c>,
        /// <c>CapWaitToCrashResendDeadline</c>), so they keep their own pacing.
        /// </para>
        /// <para>
        /// A NAMED CONSTANT because the default was written out twice — the
        /// property initializer and the <c>&lt;= TimeSpan.Zero</c> normalisation
        /// below, which is the branch every integration that never sets the
        /// property actually takes. Two literals meant changing one left the old
        /// behaviour live for exactly the callers who did not choose a value.
        /// One constant makes that impossible rather than merely unlikely.
        /// </para>
        /// <para>
        /// A default, not a floor: an explicit <see cref="FlushInterval"/> wins,
        /// including one below this.
        /// </para>
        /// </remarks>
        public static readonly TimeSpan DefaultFlushInterval = TimeSpan.FromSeconds(15);

        /// <summary>
        /// How long the app may stay BACKGROUNDED before the session is
        /// considered over. On the next foreground the SDK emits
        /// <c>app.session_ended</c> and the following event opens a new session,
        /// so session length and sessions-per-DAU become derivable without the
        /// game remembering to end anything.
        /// </summary>
        /// <remarks>
        /// <para>
        /// ⚠ FOREGROUND inactivity does NOT end a session, and that is a
        /// decision rather than an omission. A game that tracks no events for a
        /// while is still being played, and ending its session would UNDERSTATE
        /// session length — the very metric this exists to make derivable. So
        /// the clock runs only while backgrounded.
        /// </para>
        /// <para>
        /// ⚠ The boundary is evaluated on RESUME, not by a timer armed while
        /// backgrounded: a suspended mobile process does not run timers, so a
        /// timer-based end would simply never fire on the platforms that matter.
        /// The consequence is that an app KILLED while backgrounded emits no
        /// <c>app.session_ended</c> at all — a client-analytics limit this
        /// constant does not remove.
        /// </para>
        /// </remarks>
        public static readonly TimeSpan DefaultSessionTimeout = TimeSpan.FromSeconds(30);



        /// <summary>
        /// The component-slug pattern for <see cref="CrashComponent"/>:
        /// a lowercase DNS-style slug ([a-z0-9-], starting alphanumeric, max 63
        /// chars). Matches the crash report's <c>source</c> pattern so an
        /// out-of-shape slug is rejected at config time rather than rejected
        /// by the crash endpoint.
        /// </summary>
        private static readonly Regex CrashSourceSlugPattern = new Regex("^[a-z0-9][a-z0-9-]{0,62}$");

        /// <summary>The default lead before token expiry at which the SDK refreshes a Mode-B JWT.</summary>
        public static readonly TimeSpan DefaultTokenRefreshLead = TimeSpan.FromSeconds(60);

        /// <summary>
        /// Mode A — the publishable ingest key (<c>sp_*</c>) sent as
        /// <c>Authorization: Bearer &lt;key&gt;</c>. Mutually sufficient with
        /// <see cref="TokenProvider"/>: supply exactly one. When both are set,
        /// the per-tenant JWT from <see cref="TokenProvider"/> wins.
        /// </summary>
        public string? PublishableKey { get; set; }

        /// <summary>
        /// Mode B — an async provider that returns a per-tenant HS256 JWT for
        /// the Bearer (dual-mode auth). The host implements it to
        /// fetch/refresh the JWT from ITS OWN backend; the SDK NEVER holds the
        /// per-tenant signing secret, only the returned JWT string. It is
        /// invoked on the initializing host main thread. Return a Task promptly;
        /// the asynchronous answer is awaited without blocking, cached
        /// until expiry, and refreshed a configurable lead before expiry and on
        /// a 401. The <see cref="CancellationToken"/> is manager-owned, with a
        /// 30-second bound including host queue time. Supply either this or
        /// <see cref="PublishableKey"/>; if both, this takes precedence.
        /// </summary>
        public Func<CancellationToken, Task<ShardPilotTokenResult>>? TokenProvider { get; set; }

        /// <summary>
        /// How long before a Mode-B token's <see cref="ShardPilotTokenResult.ExpiresAt"/>
        /// the SDK proactively refreshes it (default 60s). Ignored in Mode A.
        /// A token whose provider reports no expiry is reused until a 401.
        /// </summary>
        public TimeSpan TokenRefreshLead { get; set; } = DefaultTokenRefreshLead;

        public string? IngestUrl { get; set; } = "https://ingest.shardpilot.com";


        /// <summary>
        /// The remote-config base URL used only by the remote-config
        /// fetch (GET /config/v1/...). This is NOT the ingest endpoint: events
        /// and consent go to <see cref="IngestUrl"/>, remote config comes from
        /// this app origin. RemoteConfigEnabled controls whether config fetches run.
        /// </summary>
        public string? AppUrl { get; set; }

        /// <summary>Enables explicit remote-config fetches; AppUrl is only the address.</summary>
        public bool RemoteConfigEnabled { get; set; }

        /// <summary>Installs automatic crash hooks during initialization.</summary>
        public bool CrashHooksEnabled { get; set; } = true;

        /// <summary>
        /// Opts into the experiment-assignment consumer.
        /// Default false — DARK: while off, zero experiment code
        /// paths execute (no subject-id mint, no assignment fetch, no
        /// revalidation, no exposure emit, no new persistence keys).
        /// Requires <see cref="AppUrl"/>: the assignment
        /// endpoint lives on the same host as remote config and authenticates
        /// with the same publishable sp_ key. Enabling it for real traffic
        /// is governed by the platform flag-flip registry preconditions —
        /// while the platform flags are off, every assignment fetch answers
        /// 403 and this SDK fails closed.
        /// </summary>
        public bool ExperimentsEnabled { get; set; }

        /// <summary>
        /// Opts the remote-config fetch into carrying the host-supplied
        /// targeting attributes stored via
        /// <see cref="ShardPilotSdk.SetRemoteConfigAttributes"/> as query
        /// parameters. Default false — OPT-IN ONLY:
        /// attributes ride the remote-config fetch ONLY while this is true
        /// AND analytics consent is Granted. Unknown or denied consent
        /// (including forced-minor denial) uses the identifier-free
        /// <c>GET /config/v1/{workspace_key}/{environment_key}</c> route,
        /// with no targeting attributes or identified cache validator.
        /// With this flag off, a granted fetch still appends its client ID
        /// but omits attributes. Consent is rechecked before dispatch;
        /// a queued denial removes both the ID and attributes, even if a
        /// new grant follows it. Requires
        /// <see cref="AppUrl"/> — without a remote-config fetch
        /// there is nothing for the attributes to ride.
        /// </summary>
        public bool RemoteConfigAttributesEnabled { get; set; }

        /// <summary>
        /// The crash ingest base URL crash reports are POSTed to:
        /// <c>POST {CrashUrl}/api/v1/crashes/ingest</c>. This is a SEPARATE
        /// endpoint from the analytics <see cref="IngestUrl"/> — crashes are
        /// NOT wrapped as analytics events. Required when CrashKey is supplied;
        /// it never falls back to IngestUrl.
        /// Must be a bare base URL (no path/query), validated like
        /// <see cref="IngestUrl"/>.
        /// </summary>
        public string? CrashUrl { get; set; }

        /// <summary>
        /// The <c>crash:write</c> API key sent as
        /// <c>Authorization: Bearer &lt;key&gt;</c> to the crash ingest endpoint.
        /// This is a DIFFERENT scope from the analytics <see cref="PublishableKey"/>
        /// (<c>analytics:ingest</c>) and the Mode-B JWT. When empty, crash
        /// capture is disabled (no key, nowhere to send) and
        /// <see cref="ShardPilotClient.CaptureCrash"/> is a safe no-op. Crash
        /// ingest authenticates ONLY with this static key — the analytics
        /// dual-mode token provider does not apply.
        /// </summary>
        public string? CrashKey { get; set; }

        /// <summary>
        /// The component slug stamped on every crash report's
        /// top-level <c>component</c> field: which component/service within your
        /// app a crash came from (e.g. <c>game-client</c>, <c>main-server</c>),
        /// so a multi-component product attributes crashes per component. Mirrors the
        /// analytics <see cref="Source"/> idiom but is the crash-report
        /// component slug, not the client/server/backend origin. Empty (the
        /// default) means a bare app with no component dimension. Validated as a
        /// lowercase DNS-style slug; an out-of-shape value throws at config time.
        /// </summary>
        public string? CrashComponent { get; set; }


        /// <summary>
        /// Send 1 in every N NON-FATAL crash reports (handled exceptions /
        /// logged exceptions), to bound volume from a noisy non-terminal error.
        /// Default 10 (send every 10th non-fatal) — the cross-SDK canonical
        /// default shared with the Go and Defold SDKs; set 1 to send every
        /// non-fatal report. FATAL crashes are NEVER sampled — a terminating
        /// crash is always sent regardless of this value, bypassing the
        /// sampler, consistent across the ShardPilot SDKs.
        /// </summary>
        public int CrashSampleEvery { get; set; } = DefaultCrashSampleEvery;

        /// <summary>Literal workspace key; never a server database ID.</summary>
        public string? WorkspaceKey { get; set; }
        /// <summary>Literal app key; never a server database ID.</summary>
        public string? AppKey { get; set; }
        /// <summary>Literal environment key; never a server database ID.</summary>
        public string? EnvironmentKey { get; set; }
        public ShardPilotSource Source { get; set; } = ShardPilotSource.Client;
        public string? AppVersion { get; set; }
        public string? AppBuild { get; set; }
        public string? Platform { get; set; }
        public string? UserId { get; set; }
        private string? _anonymousId;
        /// <summary>Literal anonymous identity. Controls and surrounding HTTP
        /// whitespace are refused before this value can become a config header.</summary>
        public string? AnonymousId
        {
            get => _anonymousId;
            set
            {
                if (!string.IsNullOrEmpty(value) && !IsSafeAnonymousIdHeader(value))
                    throw new ShardPilotConfigException("AnonymousId is not a safe HTTP header value");
                _anonymousId = value;
            }
        }

        internal static bool IsSafeAnonymousIdHeader(string? value)
        {
            if (string.IsNullOrEmpty(value) || value[0] == ' ' || value[value.Length - 1] == ' ')
                return false;
            foreach (var ch in value)
                if (ch < ' ' || ch == '\u007f') return false;
            return true;
        }
        public int BatchSize { get; set; } = DefaultBatchSize;
        public int BufferSize { get; set; } = DefaultBufferSize;
        public TimeSpan FlushInterval { get; set; } = DefaultFlushInterval;
        public TimeSpan HttpTimeout { get; set; } = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Backgrounded time after which the session is ended automatically —
        /// see <see cref="DefaultSessionTimeout"/> for what does and does not
        /// arm this clock. Zero or negative restores the default.
        /// </summary>
        public TimeSpan SessionTimeout { get; set; } = DefaultSessionTimeout;

        public bool AllowInsecurePrivateNetwork { get; set; }

        /// <summary>
        /// Whether analytics batch bodies are gzip-compressed. Default
        /// <c>true</c>.
        ///
        /// A batch body is the same envelope keys repeated per event, so it
        /// compresses hard — a full batch measures a few percent of its
        /// uncompressed size. Bodies under
        /// <see cref="ShardPilotRequestCompression.MinimumBytes"/> are sent
        /// uncompressed regardless, because gzip's framing makes a
        /// single-event batch bigger rather than smaller.
        ///
        /// The ingest body cap applies to the UNCOMPRESSED body, so
        /// compression buys throughput and not headroom:
        /// <see cref="BatchSize"/> still governs how large a body gets.
        ///
        /// Turning this off should rarely be necessary. If the deployment
        /// cannot read the coding, the client discovers that on its first
        /// publish and falls back to uncompressed for the rest of the session
        /// rather than dropping batches — including on WebGL, where an edge
        /// that does not allow the header fails the request as a network
        /// error with nothing to match on. Set this to skip even that one
        /// attempt.
        /// </summary>
        public bool RequestCompressionEnabled { get; set; } = true;
        public IShardPilotLogger? Logger { get; set; }
        public ShardPilotLogLevel LogLevel { get; set; } = ShardPilotLogLevel.Warn;

        /// <summary>Maximum retained terminal rejection records per client.
        /// Non-positive values use the default of 64; oldest records leave first.</summary>
        public int RejectionCapacity { get; set; } = 64;

        /// <summary>
        /// Enables the durable offline event spool (default true): a batch
        /// that fails TRANSIENTLY (connection error, timeout, 429, 5xx) and
        /// the pending remnant on application pause/quit/close are persisted
        /// to disk and re-sent through the normal publish path on the next
        /// launch. The spool holds exactly the envelope JSON that was going to
        /// the wire — never auth tokens or headers — and re-sends are
        /// de-duplicated by the ingest service via the stable event id. On
        /// WebGL the spool is a no-op (no real synchronous filesystem).
        /// </summary>
        public bool SpoolEnabled { get; set; } = true;

        /// <summary>
        /// Emits the consent receipt's actor <c>kind</c> on the
        /// <c>/v1/consent</c> wire body (default true — the
        /// kind-carrying consent ingest; the
        /// platform's consent-ingest kind mode defaults to <c>log</c>,
        /// which accepts and records the field). This is the ESCAPE HATCH for
        /// a deployment still running the pre-flip ingest mode <c>off</c>,
        /// whose strict decode 400-rejects a kind-bearing body as an unknown
        /// field — terminal, which would DROP the receipt (denials included):
        /// set false there. The kind is chosen, persisted with the stored
        /// receipt, and used for per-receipt dispatch-credential selection
        /// and parking REGARDLESS of this flag — only the wire field is
        /// gated. Feature-detectable by config-field semantics: an older SDK
        /// ignores unknown config members.
        /// </summary>
        public bool ConsentKindEmissionEnabled { get; set; } = true;

        /// <summary>
        /// Hard cap on the number of spooled events (default 2000). When
        /// exceeded, the OLDEST entries are evicted first. Enforced on every
        /// append and again when the spool is loaded.
        /// </summary>
        public int SpoolMaxEvents { get; set; } = DefaultSpoolMaxEvents;

        /// <summary>
        /// Hard cap on the summed UTF-8 size of the spooled event envelopes
        /// (default 1 MiB). When exceeded, the OLDEST entries are evicted
        /// first. Enforced on every append and again when the spool is loaded.
        ///
        /// This bounds the BACKLOG — what the SDK will re-send. The file it is
        /// kept in may be up to about 1.5x the ENCODED live-record bytes plus
        /// the header and 4 KiB. Each record includes bounded clock framing;
        /// formatted signed envelopes also use storage quoting, which can
        /// double their wire bytes. That population can therefore require up
        /// to about 3x live wire bytes plus framing/header/floor before compaction.
        /// An evicted entry's bytes are reclaimed by a later compaction rather than by a
        /// whole-file rewrite on the append that evicted it; rewriting there
        /// would put an O(spool size) cost on the calling thread, which at
        /// queue overflow is the game thread.
        /// </summary>
        public long SpoolMaxBytes { get; set; } = DefaultSpoolMaxBytes;

        /// <summary>
        /// The directory the spool file lives in. Empty (the default) resolves
        /// to <c>Application.persistentDataPath</c>; injectable for tests. The
        /// file name carries a hash of the (workspace, app, environment)
        /// scope, so two configurations never share a spool. The crash-report
        /// spool lives in this same directory (its own per-scope file), so
        /// one consent sweep covers both.
        /// </summary>
        public string? SpoolDir { get; set; }

        /// <summary>
        /// Enables the durable crash-report spool (default true): every crash
        /// report is persisted to disk at capture time — already scrubbed, in
        /// the exact form that goes to the wire — and removed once the crash
        /// ingest service acknowledges it, so a report captured in a dying
        /// process or during an outage is re-sent on the next launch. This is
        /// a SEPARATE per-scope file from the analytics event spool (crash
        /// reports ship one-per-request to the crash endpoint, not in the
        /// events batch). <see cref="SpoolEnabled"/> is the master switch for
        /// ALL on-disk durability: the crash spool runs only when BOTH flags
        /// are true and a <see cref="CrashKey"/> is configured (without
        /// the key nothing could ever be re-sent, so nothing is persisted).
        /// On WebGL the crash spool is a no-op, like the event spool.
        /// </summary>
        public bool CrashSpoolEnabled { get; set; } = true;

        /// <summary>
        /// Hard cap on the number of spooled crash reports (default 50 — a
        /// fatal crash ends the session, so reports accumulate at most a few
        /// per launch and 50 spans dozens of failed launches). When exceeded,
        /// the OLDEST reports are evicted first: the newest report describes
        /// the current build's failure and is worth more than a stale one.
        /// Enforced on every append and again on load.
        /// </summary>
        public int CrashSpoolMaxReports { get; set; } = DefaultCrashSpoolMaxReports;

        /// <summary>
        /// Hard cap on the summed UTF-8 size of the spooled crash-report
        /// bodies (default 512 KiB). A typical managed-exception report — a
        /// few dozen sanitized frames plus breadcrumbs — serializes to
        /// 2–10 KiB, and the sanitizer bounds a pathological one (256 frames,
        /// 50 breadcrumbs) to roughly 50 KiB, so the default holds dozens of
        /// typical reports and several worst-case ones. When exceeded, the
        /// OLDEST reports are evicted first; a single report larger than the
        /// whole cap is rejected outright rather than evicting everything.
        /// </summary>
        public long CrashSpoolMaxBytes { get; set; } = DefaultCrashSpoolMaxBytes;

        /// <summary>
        /// Enables the Android ANR next-launch lane: on the
        /// next launch after the system killed the app for an ANR, the SDK
        /// reads <c>ApplicationExitInfo</c> records of the OWN package
        /// (API 30+, pure C# over <c>AndroidJavaObject</c> — no native
        /// plugin), filters <c>REASON_ANR</c>, and reports each NEW record as
        /// a crash-plane event with <c>kind:"anr"</c>, replay-protected by a
        /// persisted high-water cursor plus the deterministic
        /// <c>aei_record_key</c> idempotency field. Default FALSE; enable
        /// this opt-in before SDK initialization. Non-Android platforms
        /// ignore the flag entirely. The lane rides the crash plane's
        /// consent posture unchanged: the persisted crash opt-out and an
        /// analytics-consent denial (including a forced-minor denial)
        /// gate every report fail-closed, and
        /// attribution follows the per-era snapshot rules (frozen to the era
        /// subject; device-scoped anonymous when no snapshot covers the
        /// record).
        /// </summary>
        public bool AnrExitInfoEnabled { get; set; }

        /// <summary>
        /// Enables the Android native-crash next-launch lane:
        /// the same <c>ApplicationExitInfo</c>
        /// sweep the ANR lane runs additionally selects
        /// <c>REASON_CRASH_NATIVE</c> records (API 30+). When a record
        /// carries a tombstone trace stream (API 31+), the SDK ships the RAW
        /// AOSP <c>Tombstone</c> protobuf bytes UNMODIFIED — never parsed,
        /// never truncated client-side — as a multipart upload to
        /// <c>POST {CrashUrl}/api/v1/crashes/ingest-tombstone</c>
        /// (metadata envelope JSON part + one <c>tombstone</c> bytes part,
        /// hard-capped at the server's 4 MiB payload limit); the SERVER
        /// decodes, allowlist-strips, and symbolicates them. A record with
        /// no stream (the API-30 shape), a NULL stream (the API-31+
        /// overwritten-buffer shape), or an over-cap tombstone produces a
        /// metadata-only degraded report on the EXISTING JSON crash lane
        /// instead — never a drop, and never a raw byte over JSON. Replay
        /// protection, era-snapshot attribution, and the fail-closed
        /// CURRENT-consent upload gate are shared with
        /// <see cref="AnrExitInfoEnabled"/> (one cursor, one
        /// <c>aei_record_key</c> discipline); each reason reports only under
        /// its own flag. Default FALSE; enable this opt-in before SDK
        /// initialization with a configured crash endpoint and key.
        /// Non-Android platforms ignore the flag entirely.
        /// </summary>
        public bool NativeCrashExitInfoEnabled { get; set; }

        /// <summary>
        /// Enables the OPT-IN in-session main-thread stall watchdog:
        /// a main-thread heartbeat plus a monitor timer that
        /// flags stalls longer than <see cref="HangWatchdogThreshold"/> and
        /// reports AFTER the main thread recovers (a stall the process never
        /// recovers from is the next-launch ApplicationExitInfo lane's job).
        /// Reports carry <c>kind:"anr"</c> and a deterministic
        /// <c>aei_record_key</c> derived from (process era, stall start) so a
        /// spool re-send collapses server-side. Default FALSE — dark, and a
        /// SEPARATE flag from <see cref="AnrExitInfoEnabled"/> per the
        /// per-kind flag discipline. Watchdog reports are
        /// non-fatal (the session survived), so they ride
        /// <see cref="CrashSampleEvery"/> like every other non-fatal
        /// report — set that to 1 to send every stall report.
        /// </summary>
        public bool HangWatchdogEnabled { get; set; }

        /// <summary>
        /// The main-thread stall duration at which the
        /// watchdog flags an ANR-class stall (default five seconds — the
        /// lane's default, matching Android's input-dispatch ANR window).
        /// Values &lt;= TimeSpan.Zero fall back to the default.
        /// </summary>
        public TimeSpan HangWatchdogThreshold { get; set; } = DefaultHangWatchdogThreshold;

        public ShardPilotConfig CloneWithDefaults()
        {
            var clone = (ShardPilotConfig)MemberwiseClone();
            clone.PublishableKey = Trim(clone.PublishableKey);
            clone.IngestUrl = NormalizeEndpoint(clone.IngestUrl, clone.AllowInsecurePrivateNetwork);
            clone.AppUrl = Trim(clone.AppUrl);
            if (!string.IsNullOrEmpty(clone.AppUrl))
            {
                clone.AppUrl = NormalizeEndpoint(clone.AppUrl, clone.AllowInsecurePrivateNetwork);
            }


            // Crash capture uses its own URL and crash:write credential.
            clone.CrashUrl = Trim(clone.CrashUrl);
            if (!string.IsNullOrEmpty(clone.CrashUrl))
                clone.CrashUrl = NormalizeEndpoint(clone.CrashUrl, clone.AllowInsecurePrivateNetwork);
            clone.CrashKey = Trim(clone.CrashKey);
            if (!string.IsNullOrEmpty(clone.CrashKey) && string.IsNullOrEmpty(clone.CrashUrl))
                throw new ShardPilotConfigException("crash reporting requires CrashUrl; it does not use IngestUrl");
            clone.CrashComponent = NormalizeCrashComponent(clone.CrashComponent);
            if (clone.CrashSampleEvery < 0)
            {
                clone.CrashSampleEvery = DefaultCrashSampleEvery;
            }

            clone.WorkspaceKey = Trim(clone.WorkspaceKey);
            clone.AppKey = Trim(clone.AppKey);
            clone.EnvironmentKey = Trim(clone.EnvironmentKey);
            clone.AppVersion = Trim(clone.AppVersion);
            if (string.IsNullOrEmpty(clone.AppVersion)) clone.AppVersion = Trim(Application.version);
            clone.AppBuild = Trim(clone.AppBuild);
            clone.Platform = Trim(clone.Platform);
            clone.UserId = Trim(clone.UserId);
            // AnonymousId is literal: never trim two distinct host identities
            // into one. Its setter rejects unsafe header values at assignment.

            // Dual-mode auth: either a Mode-A publishable key OR a
            // Mode-B async token provider must be present for INGEST.
            //
            // Remote config is a separate endpoint that authenticates with
            // the sp_ PUBLISHABLE key. The Mode-B ingest JWT is scoped
            // analytics:ingest and the remote-config endpoint would reject
            // it, so enabling remote config, experiments or attributes requires
            // the key REGARDLESS of the token provider — otherwise a token-only
            // config would call remote config with no Authorization.
            var hasTokenProvider = clone.TokenProvider != null;
            var appConfigured = !string.IsNullOrEmpty(clone.AppUrl);
            var remoteConfigEnabled = clone.RemoteConfigEnabled;
            if (remoteConfigEnabled && !appConfigured)
                throw new ShardPilotConfigException("remote config requires AppUrl");

            // Experiments are dark by default. The
            // assignment endpoint lives on the same host as
            // the remote-config fetch and authenticates with the same
            // publishable sp_ key, so the opt-in requires AppUrl
            // (which in turn requires the key below). While the flag is off,
            // zero experiment code paths execute.
            if (clone.ExperimentsEnabled && !appConfigured)
            {
                throw new ShardPilotConfigException(
                    "experiments require AppUrl: the assignment endpoint lives on the host the remote-config fetch uses");
            }

            // Remote-config attributes are dark by default
            // too. The pass-through rides the remote-config fetch itself, so
            // the opt-in requires AppUrl; the consent gate is
            // enforced at fetch time (granted-only), not here.
            if (clone.RemoteConfigAttributesEnabled && !appConfigured)
            {
                throw new ShardPilotConfigException(
                    "remote-config attributes require AppUrl: there is no remote-config fetch for them to ride");
            }

            if (!hasTokenProvider || remoteConfigEnabled || clone.ExperimentsEnabled || clone.RemoteConfigAttributesEnabled)
            {
                if (string.IsNullOrEmpty(clone.PublishableKey))
                {
                    throw new ShardPilotConfigException(
                        hasTokenProvider
                            ? "api key (sp_ publishable key) is required when remote config is enabled: the Mode-B ingest JWT cannot authenticate the remote-config endpoint"
                            : "api key or token provider is required");
                }
            }

            // ANY non-empty key must be publishable-shaped, dual-mode
            // included: the most-vouching consent
            // dispatch sends the key for anon-keyed receipts even when a
            // token provider exists, so a historically-ignored junk value
            // would become a live credential whose guaranteed 401 is
            // TERMINAL for the receipt (publishable 401s are never healed
            // by re-minting a JWT). Rejecting the shape at construction
            // fails the misconfiguration closed before any receipt can be
            // minted against it; a dual-mode config that wants no
            // publishable credential simply leaves PublishableKey empty.
            if (!string.IsNullOrEmpty(clone.PublishableKey) &&
                !clone.PublishableKey.StartsWith("sp_", StringComparison.Ordinal))
            {
                throw new ShardPilotConfigException("api key must use the sp_ prefix");
            }

            if (clone.TokenRefreshLead < TimeSpan.Zero)
            {
                clone.TokenRefreshLead = DefaultTokenRefreshLead;
            }

            if (string.IsNullOrEmpty(clone.WorkspaceKey))
            {
                throw new ShardPilotConfigException("workspace key is required");
            }

            if (string.IsNullOrEmpty(clone.AppKey))
            {
                throw new ShardPilotConfigException("app key is required");
            }

            if (string.IsNullOrEmpty(clone.EnvironmentKey))
            {
                throw new ShardPilotConfigException("environment key is required");
            }

            if (clone.RejectionCapacity <= 0)
            {
                clone.RejectionCapacity = 64;
            }

            if (clone.BatchSize <= 0)
            {
                clone.BatchSize = DefaultBatchSize;
            }

            if (clone.BatchSize > MaxBatchSize)
            {
                clone.BatchSize = MaxBatchSize;
            }

            if (clone.BufferSize <= 0)
            {
                clone.BufferSize = DefaultBufferSize;
            }

            if (clone.FlushInterval <= TimeSpan.Zero)
            {
                clone.FlushInterval = DefaultFlushInterval;
            }

            if (clone.HttpTimeout <= TimeSpan.Zero)
            {
                clone.HttpTimeout = TimeSpan.FromSeconds(2);
            }

            if (clone.SessionTimeout <= TimeSpan.Zero)
            {
                clone.SessionTimeout = DefaultSessionTimeout;
            }

            clone.SpoolDir = Trim(clone.SpoolDir);
            if (clone.SpoolMaxEvents <= 0)
            {
                clone.SpoolMaxEvents = DefaultSpoolMaxEvents;
            }

            if (clone.SpoolMaxBytes <= 0)
            {
                clone.SpoolMaxBytes = DefaultSpoolMaxBytes;
            }

            if (clone.CrashSpoolMaxReports <= 0)
            {
                clone.CrashSpoolMaxReports = DefaultCrashSpoolMaxReports;
            }

            if (clone.CrashSpoolMaxBytes <= 0)
            {
                clone.CrashSpoolMaxBytes = DefaultCrashSpoolMaxBytes;
            }

            if (clone.HangWatchdogThreshold <= TimeSpan.Zero)
            {
                clone.HangWatchdogThreshold = DefaultHangWatchdogThreshold;
            }

            if (string.IsNullOrEmpty(clone.Platform))
            {
                clone.Platform = DetectUnityPlatform();
            }

            return clone;
        }

        /// <summary>
        /// True when this config authenticates via the Mode-B async token
        /// provider (the JWT wins over any sp_ key when both are set).
        /// </summary>
        internal bool UsesTokenProvider
        {
            get { return TokenProvider != null; }
        }

        internal string SourceValue
        {
            get
            {
                switch (Source)
                {
                    case ShardPilotSource.Server:
                        return "server";
                    case ShardPilotSource.Backend:
                        return "backend";
                    default:
                        return "client";
                }
            }
        }

        /// <summary>
        /// The validated component slug stamped on crash reports, or
        /// the empty string for a bare app. Populated by
        /// <see cref="CloneWithDefaults"/>.
        /// </summary>
        internal string CrashComponentValue
        {
            get { return CrashComponent ?? string.Empty; }
        }

        /// <summary>
        /// True when <paramref name="value"/> is a well-formed
        /// component slug (^[a-z0-9][a-z0-9-]{0,62}$). The crash sanitizer uses
        /// this to validate a per-event <c>Source</c> override WITHOUT throwing,
        /// mirroring the config-time check in
        /// <see cref="NormalizeCrashComponent"/>. An empty value is not a slug here
        /// (callers treat empty as "fall back to the configured default").
        /// </summary>
        internal static bool IsCrashSourceSlug(string value)
        {
            return !string.IsNullOrEmpty(value) && CrashSourceSlugPattern.IsMatch(value);
        }

        private static string NormalizeCrashComponent(string raw)
        {
            raw = Trim(raw);
            if (string.IsNullOrEmpty(raw))
            {
                // Empty is valid — a bare app with no component dimension.
                return string.Empty;
            }

            if (!CrashSourceSlugPattern.IsMatch(raw))
            {
                throw new ShardPilotConfigException(
                    "crash source must be a lowercase DNS-style slug matching ^[a-z0-9][a-z0-9-]{0,62}$");
            }

            return raw;
        }

        internal static string NormalizePolicyEndpoint(string raw) => NormalizeEndpoint(raw, false);

        private static string NormalizeEndpoint(string raw, bool allowPrivateNetwork)
        {
            raw = Trim(raw);
            if (string.IsNullOrEmpty(raw))
            {
                raw = "https://ingest.shardpilot.com";
            }

            Uri uri;
            if (!Uri.TryCreate(raw, UriKind.Absolute, out uri))
            {
                throw new ShardPilotConfigException("endpoint must be absolute");
            }

            if (!string.IsNullOrEmpty(uri.UserInfo))
            {
                throw new ShardPilotConfigException("endpoint must not include user info");
            }

            if (!string.IsNullOrEmpty(uri.Query))
            {
                throw new ShardPilotConfigException("endpoint must not include query parameters");
            }

            if (!string.IsNullOrEmpty(uri.Fragment))
            {
                throw new ShardPilotConfigException("endpoint must not include a fragment");
            }

            if (!string.IsNullOrEmpty(uri.AbsolutePath) && uri.AbsolutePath != "/")
            {
                throw new ShardPilotConfigException("endpoint must not include a path");
            }

            if (uri.Scheme != Uri.UriSchemeHttps && !AllowInsecureUri(uri, allowPrivateNetwork))
            {
                throw new ShardPilotConfigException("endpoint must use https outside localhost, loopback, or explicitly allowed private networks");
            }

            var builder = new UriBuilder(uri)
            {
                Path = string.Empty,
                Query = string.Empty,
                Fragment = string.Empty
            };
            return builder.Uri.ToString().TrimEnd('/');
        }

        private static bool AllowInsecureUri(Uri uri, bool allowPrivateNetwork)
        {
            if (uri.Scheme != Uri.UriSchemeHttp)
            {
                return false;
            }

            var host = uri.Host.Trim('[', ']');
            if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            IPAddress address;
            if (!IPAddress.TryParse(host, out address))
            {
                return false;
            }

            if (IPAddress.IsLoopback(address))
            {
                return true;
            }

            return allowPrivateNetwork && IsPrivateAddress(address);
        }

        private static bool IsPrivateAddress(IPAddress address)
        {
            var bytes = address.GetAddressBytes();
            if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork && bytes.Length == 4)
            {
                return bytes[0] == 10 ||
                       bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31 ||
                       bytes[0] == 192 && bytes[1] == 168;
            }

            if (address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6)
            {
                return address.IsIPv6LinkLocal || address.IsIPv6SiteLocal || IsIPv6UniqueLocal(address);
            }

            return false;
        }

        private static bool IsIPv6UniqueLocal(IPAddress address)
        {
            var bytes = address.GetAddressBytes();
            return bytes.Length == 16 && (bytes[0] & 0xfe) == 0xfc;
        }

        private static string DetectUnityPlatform()
        {
            switch (Application.platform)
            {
                case RuntimePlatform.IPhonePlayer:
                    return "ios";
                case RuntimePlatform.Android:
                    return "android";
                case RuntimePlatform.WindowsPlayer:
                case RuntimePlatform.WindowsEditor:
                    return "windows";
                case RuntimePlatform.OSXPlayer:
                case RuntimePlatform.OSXEditor:
                    return "macos";
                case RuntimePlatform.LinuxPlayer:
                case RuntimePlatform.LinuxEditor:
                    return "linux";
                case RuntimePlatform.WebGLPlayer:
                    return "web";
                default:
                    // Names also cover platform members unavailable in older
                    // engine assemblies without referring to their enum fields.
                    var native = Application.platform.ToString();
                    switch (native)
                    {
                        case "WSAPlayerX86":
                        case "WSAPlayerX64":
                        case "WSAPlayerARM":
                        case "WindowsServer":
                            return "windows";
                        case "XboxOne":
                        case "GameCoreXboxOne":
                        case "GameCoreXboxSeries":
                            return "xbox";
                        case "EmbeddedLinuxArm64":
                        case "EmbeddedLinuxX64":
                        case "LinuxServer":
                            return "linux";
                        case "OSXServer":
                            return "macos";
                        default:
                            return ShardPilotEnvelopePlatform.Normalize(native);
                    }
            }
        }

        internal static string Trim(string value)
        {
            return string.IsNullOrWhiteSpace(value) ? string.Empty : value.Trim();
        }
    }

    public sealed class ShardPilotConfigException : Exception
    {
        public ShardPilotConfigException(string message) : base("invalid shardpilot config: " + message)
        {
        }
    }

    public enum ShardPilotLogLevel
    {
        Debug = 0,
        Info = 1,
        Warn = 2,
        Error = 3,
        Off = 4
    }

    public interface IShardPilotLogger
    {
        void Log(ShardPilotLogLevel level, string message);
    }
}
