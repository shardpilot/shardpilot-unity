using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

namespace ShardPilot
{
    /// <summary>
    /// Result of a remote-config fetch. Success with FromCache=true means the
    /// cached snapshot was served: after a 304 Not Modified revalidation, or
    /// on a transient failure (connection error, timeout, 408, 429, 5xx, malformed
    /// body) with a usable cache — in the failure case Error carries the
    /// reason. An unauthorized fetch (401/403) always fails closed:
    /// Success=false with no Values, even when a cache exists. Any other
    /// status (a 404 for a removed environment, an unexpected redirect, other
    /// 4xx) is a permanent failure: retrying cannot help and stale values
    /// must not masquerade as a healthy fetch, so Success=false with no
    /// Values — the cache record is left untouched. Terminal outcomes keep
    /// this instance's scope closed to getters and cached/fenced callbacks
    /// until a winning valid 200 or 304, or a new client/session.
    /// </summary>
#nullable enable annotations
    public sealed class ShardPilotRemoteConfigResult
    {
        public bool Success { get; internal set; }
        public bool FromCache { get; internal set; }
        public string? Error { get; internal set; }
        public IReadOnlyDictionary<string, object?>? Values { get; internal set; }

        /// <summary>
        /// The published configuration version from the served payload, or
        /// null when the payload did not carry a numeric one.
        /// </summary>
        public long? Version { get; internal set; }

        internal static ShardPilotRemoteConfigResult Failure(string error)
        {
            return new ShardPilotRemoteConfigResult
            {
                Success = false,
                FromCache = false,
                Error = error
            };
        }
    }

    /// <summary>
    /// Config-fetch only client for the ShardPilot remote-config endpoint:
    /// GET {AppUrl}/config/v1/{workspace_key}/{environment_key}, carrying
    /// X-ShardPilot-Anonymous-Id only with granted analytics consent, with the SDK bearer
    /// token and an ETag/If-None-Match cache scoped to that identity. One
    /// fetch = one HTTP GET with one of these outcomes:
    /// <list type="bullet">
    /// <item>200 with a usable JSON body — fresh values are served and the
    /// cache is overwritten (body + response ETag).</item>
    /// <item>304 Not Modified — the cached snapshot is served
    /// (FromCache=true) and the record's freshness stamp is renewed, in
    /// memory and (best-effort) on disk: the endpoint confirmed the body as
    /// current, so the record outranks same-scope records stamped while the
    /// request was in flight.</item>
    /// <item>A transient failure (connection error, timeout, 408, 429, 5xx,
    /// malformed body) — the cached snapshot is served with FromCache=true
    /// and Error carrying the reason; with no usable cache the fetch
    /// fails.</item>
    /// <item>401/403 — fails CLOSED: the cached snapshot is never served for
    /// this outcome (a revoked or wrong key must not keep supplying
    /// configuration), but the cache file itself is left untouched.
    /// Classified by the RECEIVED status even when the body stalled or was
    /// truncated mid-transfer (status-first stalled-body strictness).</item>
    /// <item>Any other status (a 404 for a removed environment, an
    /// unexpected 3xx, other 4xx) is a PERMANENT failure: the fetch fails
    /// rather than serving the cached snapshot as Success — retrying cannot
    /// help and stale values must not masquerade as a healthy fetch. As with
    /// 401/403, the cache record is left untouched.</item>
    /// </list>
    /// The cache payload is stamped with the (workspace, environment,
    /// client, remote-config URL) scope it was fetched for. The client
    /// component is empty for identifier-free requests; a cache written
    /// by a different scope is treated as a miss (no ETag sent, no offline
    /// serving) and is overwritten by the next successful fetch. So is a
    /// cache whose body no longer yields a configuration — it could neither
    /// be served offline nor recovered from after the 304 its ETag would
    /// provoke. Responses to concurrent fetches may return out of order; an
    /// older response never overwrites a newer configuration (a per-scope
    /// fence settled only by authoritative outcomes — see Install). The
    /// fetch omits identifiers without granted consent. The opt-in
    /// (<see cref="ShardPilotConfig.RemoteConfigAttributesEnabled"/> AND
    /// Granted consent) appends the normalized stored attributes as query
    /// parameters — see <see cref="SetRemoteConfigAttributes"/>. There is
    /// no experiment assignment, no exposure events, and no stats here by
    /// design.
    /// </summary>
    public sealed class ShardPilotRemoteConfig
    {
        private readonly ShardPilotConfig _config;
        private readonly string _cachePath;
        private readonly IShardPilotHttpRequestSender _sender;
        private readonly ShardPilotDispatcher _callbackHost;
        private readonly Func<bool> _hostCurrent;

        /// <summary>
        /// The current analytics consent state, or null when no consent
        /// source is wired (direct construction outside the SDK facade) —
        /// read as Unknown then, the fail-closed direction: an unwired
        /// consent source must keep the fetch identifier-free exactly like an
        /// undecided player.
        /// </summary>
        private readonly Func<ShardPilotConsentState> _consentState;

        /// <summary>
        /// The client's consent-denial epoch (advanced on EVERY entry into
        /// a denied state — see
        /// <c>ShardPilotClient.AdvanceConsentDenialEpochLocked</c>), or null
        /// when none is wired — the epoch check then stands down and the
        /// state read stands alone, the experiments-consumer posture. The
        /// identified-URL build captures it and the pump-time gate refuses
        /// the identified URL when it MOVED since build: a denial the
        /// player entered and re-granted while the request sat enqueued is
        /// an intervening consent transition the current-state read alone
        /// cannot see.
        /// </summary>
        private readonly Func<long> _consentEpoch;
        private readonly Func<bool> _intakeClosed;

        /// <summary>
        /// Guards the host-supplied attribute set below — a per-instance
        /// gate (unlike the shared cache gate): the attribute set is
        /// per-instance state, and the swap must never contend with a
        /// sibling's cache IO.
        /// </summary>
        private readonly object _attributesGate = new object();

        /// <summary>
        /// The stored attribute set: a defensive copy,
        /// never mutated after the swap, so a reference snapshot under the
        /// gate is a stable read. Null when nothing is stored. Normalized at
        /// FETCH time, and sent ONLY while
        /// <see cref="ShardPilotConfig.RemoteConfigAttributesEnabled"/> is
        /// true AND consent is Granted — at build (see
        /// <see cref="BuildConsentedUrl"/>) AND again at the sender's
        /// last-moment gate (see
        /// <see cref="ShardPilotHttpRequest.ConsentedUrlGate"/>).
        /// </summary>
        private Dictionary<string, string> _attributes;

        /// <summary>
        /// Guards the disk cache file and the fetch-fence state below. Shared
        /// across ALL instances — several instances may point at the same
        /// cache file, and the guarded read-then-tombstone sequence on a
        /// failed write must be atomic against a sibling's concurrent write
        /// or it could delete the fresher record the guard exists to
        /// preserve. (One game process owns the cache path; cross-process
        /// sharing is not a supported topology.) Never held while a callback
        /// (sender or game code) runs.
        /// </summary>
        private static readonly object _gate = new object();

        /// <summary>
        /// The in-process cache record. It is updated on every installed
        /// fetch even when the durable write fails, so a later offline fetch
        /// falls back to the FRESHEST served configuration rather than
        /// reviving an older on-disk record.
        /// </summary>
        private ShardPilotRemoteConfigCacheEntry _heldCache;

        // Closure is a serving decision for this client/session, not a
        // deletion of the retained validator/body. Scope history survives
        // identity changes; only a winning valid revalidation reopens it.
        private readonly Dictionary<string, string> _closedScopes = new Dictionary<string, string>(StringComparer.Ordinal);

        /// <summary>
        /// Fetches are numbered, and _settled maps a scope to the highest
        /// sequence whose AUTHORITATIVE outcome has landed for it — a fresh
        /// 200, a 304 revalidation, an unauthorized response, or a permanent
        /// HTTP error. Only a fetch newer than every settled one for ITS
        /// scope may install: with two fetches in flight, responses can
        /// arrive out of order, and an older success must neither roll back
        /// a newer configuration nor sneak values in after a newer
        /// fail-closed outcome. The fence is kept per scope — never reset —
        /// so a changed identity can neither leak one scope's fence into
        /// another nor forget a scope's history on re-entry (the map stays
        /// bounded by the identities this instance has actually used).
        /// Non-authoritative outcomes (a transient failure, a cache-served
        /// fallback) do not settle — they say nothing about the current
        /// configuration, so they must not fence off a fresh response still
        /// in flight. Like the gate, the fence is shared across ALL
        /// instances: siblings pointing at one cache file interleave their
        /// installs, so their in-flight fetches must be ordered against each
        /// other too — a sequence scoped to one instance would let its own
        /// stale response overwrite a sibling's newer install. Scopes keep
        /// unrelated instances from fencing each other.
        /// </summary>
        private static long _fetchSeq;

        private static readonly Dictionary<string, long> _settled = new Dictionary<string, long>(StringComparer.Ordinal);

        private sealed class RetryWindow
        {
            public DateTime NotBefore;
            public long Status;
        }

        // Server-directed windows belong to the scope, including across SDK
        // replacements and siblings with different cache paths. Guarded by _gate.
        private static readonly Dictionary<string, RetryWindow> _retryWindows = new Dictionary<string, RetryWindow>(StringComparer.Ordinal);
        private readonly Func<DateTime> _utcNow;

        private static void PruneRetryWindowsLocked(DateTime now)
        {
            List<string> expired = null;
            foreach (var pair in _retryWindows)
            {
                if (now < pair.Value.NotBefore) continue;
                if (expired == null) expired = new List<string>();
                expired.Add(pair.Key);
            }
            if (expired != null) foreach (var scope in expired) _retryWindows.Remove(scope);
        }

        /// <summary>
        /// Test-only: when set, the next durable cache writes throw this
        /// instead of writing, simulating a full or unwritable disk. Cleared
        /// by the test. Never assigned in production.
        /// </summary>
        internal Exception WriteFailureForTesting;

        /// <summary>
        /// Test-only: when set, the next tombstone deletes throw this instead
        /// of deleting, simulating an undeletable file. Cleared by the test.
        /// Never assigned in production.
        /// </summary>
        internal Exception DeleteFailureForTesting;

        internal ShardPilotRemoteConfig(
            ShardPilotConfig config,
            string cachePath,
            IShardPilotHttpRequestSender sender,
            Func<ShardPilotConsentState> consentState = null,
            Func<long> consentEpoch = null,
            Func<DateTime> utcNow = null, Func<bool> intakeClosed = null, Func<bool> hostCurrent = null)
        {
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            _config = config;
            _cachePath = cachePath;
            _sender = sender ?? throw new ArgumentNullException("sender");
            _callbackHost = ShardPilotDispatcher.CapturedHost;
            _hostCurrent = hostCurrent ?? (() => true);
            _consentState = consentState;
            _consentEpoch = consentEpoch;
            _intakeClosed = intakeClosed;
            _utcNow = utcNow ?? (() => DateTime.UtcNow);
        }

        /// <summary>
        /// Stores the targeting attributes the remote-config fetch MAY carry:
        /// a defensive copy is kept; null or empty CLEARS
        /// the stored set. Storing alone sends nothing — attributes ride a
        /// fetch ONLY while
        /// <see cref="ShardPilotConfig.RemoteConfigAttributesEnabled"/>
        /// (default false) is true AND analytics consent is Granted at fetch
        /// time. Without a grant, the request omits identifiers as well as
        /// attributes. The set is normalized per fetch with the
        /// shared experiment-attribute vocabulary (geo, app_version,
        /// device_type, install_date, user_segment,
        /// custom_attribute_&lt;1-64 chars&gt;; values trimmed and bounded
        /// to 512 UTF-8 bytes, pairs ordinal-sorted and capped at 64;
        /// out-of-vocabulary names dropped, never sent).
        /// </summary>
        public void SetRemoteConfigAttributes(IDictionary<string, string?>? attributes)
        {
            if (_intakeClosed?.Invoke() == true) return;
            Dictionary<string, string> copy = null;
            if (attributes != null && attributes.Count > 0)
            {
                copy = new Dictionary<string, string>(attributes.Count, StringComparer.Ordinal);
                foreach (var pair in attributes)
                {
                    if (pair.Key != null)
                    {
                        copy[pair.Key] = pair.Value;
                    }
                }
            }

            lock (_attributesGate)
            {
                _attributes = copy;
            }
        }

        /// <summary>
        /// Starts a remote-config fetch; returns false when none was started.
        /// Live-owner refusals still notify on the captured Unity host thread.
        /// No callback runs without a captured host or after owner retirement. The
        /// callback receives its own fetch outcome, except a closed scope
        /// cannot supply cached or fenced-out values. Only a winning valid
        /// 200/304 can reopen it; the fetch fence also orders stored values.
        /// </summary>
        public bool FetchRemoteConfig(Action<ShardPilotRemoteConfigResult>? callback)
        {
            if (ReferenceEquals(_callbackHost, null) || !_callbackHost.IsLive || !_hostCurrent()) return false;
            var scopeAtDelivery = CurrentScope();
            var identifiedAtDelivery = false;
            var epochAtDelivery = ConsentEpochNow();
            callback = _callbackHost.WrapCallback(callback, _hostCurrent, result =>
            {
                lock (_gate)
                {
                    if (!result.Success) return result;
                    if (!string.Equals(CurrentScope(identifiedAtDelivery), scopeAtDelivery, StringComparison.Ordinal) ||
                        (identifiedAtDelivery && (ConsentStateNow() != ShardPilotConsentState.Granted || ConsentEpochNow() != epochAtDelivery)))
                        return ShardPilotRemoteConfigResult.Failure("consent_or_scope_changed");
                    return _closedScopes.TryGetValue(scopeAtDelivery, out var error) ? ShardPilotRemoteConfigResult.Failure(error) : result;
                }
            });
            if (_intakeClosed?.Invoke() == true) { Invoke(callback, ShardPilotRemoteConfigResult.Failure("shutdown")); return false; }
            if (!_config.RemoteConfigEnabled || string.IsNullOrEmpty(_config.AppUrl))
            {
                Invoke(callback, ShardPilotRemoteConfigResult.Failure("remote config base url is not configured"));
                return false;
            }

            var clientId = _config.AnonymousId;
            var anonymousScope = ShardPilotRemoteConfigLogic.BuildScope(
                _config.WorkspaceKey, _config.EnvironmentKey, null, _config.AppUrl);
            var identifiedScope = ShardPilotRemoteConfigLogic.BuildScope(
                _config.WorkspaceKey, _config.EnvironmentKey, clientId, _config.AppUrl);
            long seq;
            lock (_gate)
            {
                seq = ++_fetchSeq;
            }

            // Capture both records. The dispatch selection binds the URL,
            // validator and response cache to the same consent scope. An
            // unaware sender uses only the identifier-free baseline.
            var anonymousCache = LoadCache(anonymousScope);
            var identifiedCache = LoadCache(identifiedScope);
            var scope = anonymousScope;
            scopeAtDelivery = scope;
            var includeClientId = false;
            var cache = anonymousCache;
            long consentEpochAtBuild;
            var consentedUrl = BuildConsentedUrl(clientId, out consentEpochAtBuild);
            epochAtDelivery = consentEpochAtBuild;
            var request = new ShardPilotHttpRequest
            {
                Method = "GET",
                Url = ShardPilotRemoteConfigLogic.BuildUrl(
                    _config.AppUrl, _config.WorkspaceKey, _config.EnvironmentKey),
                ConsentedUrl = consentedUrl,
                ConsentedUrlGate = consentedUrl == null
                    ? null
                    : (Func<bool>)(() =>
                        ConsentStateNow() == ShardPilotConsentState.Granted &&
                        ConsentEpochNow() == consentEpochAtBuild),
                Bearer = _config.PublishableKey,
                Timeout = _config.HttpTimeout,
                Headers = ConfigHeaders(anonymousCache, null)
            };
            request.OnUrlSelected = consented =>
            {
                scope = consented ? identifiedScope : anonymousScope;
                includeClientId = consented;
                scopeAtDelivery = scope;
                identifiedAtDelivery = consented;
                cache = consented ? identifiedCache : anonymousCache;
                request.Headers = ConfigHeaders(cache, consented ? clientId : null);
            };
            // The sender first selects the live consent scope. A queued
            // downgrade must use that scope's window, not its enqueue scope.
            request.BeforeDispatch = () =>
            {
                lock (_gate)
                {
                    var now = _utcNow();
                    PruneRetryWindowsLocked(now);
                    RetryWindow window;
                    return _retryWindows.TryGetValue(scope, out window) && now < window.NotBefore
                        ? new ShardPilotHttpResponse { StatusCode = window.Status }
                        : null;
                }
            };

            try
            {
                _sender.Send(request, response =>
                {
                    var outcome = ShardPilotRemoteConfigLogic.Apply(cache, response, _utcNow());
                    Install(seq, scope, includeClientId, outcome, cache, response);
                    Invoke(callback, outcome.Result);
                });
            }
            catch (Exception ex)
            {
                var outcome = ShardPilotRemoteConfigLogic.Apply(cache, null, _utcNow());
                outcome.Result.Error = "remote config fetch failed: " + ex.Message;
                Install(seq, scope, includeClientId, outcome, cache);
                Invoke(callback, outcome.Result);
            }
            return true;
        }

        /// <summary>
        /// Returns the values from the freshest usable cache record for this
        /// client's (workspace, environment, client, remote-config URL)
        /// scope, or null when absent or terminally closed. Without readable granted
        /// consent, the client component is empty. The freshest of the
        /// in-process record (which survives a failed durable write) and disk
        /// record (which another instance may have refreshed) is served —
        /// see LoadCache. No network.
        /// </summary>
        public IReadOnlyDictionary<string, object?>? GetCachedValues()
        {
            var scope = CurrentScope();
            lock (_gate)
            {
                if (_closedScopes.ContainsKey(scope)) return null;
                var cache = LoadCache(scope);
                if (cache == null) return null;

                Dictionary<string, object> values;
                long? version;
                return ShardPilotRemoteConfigLogic.TryParseConfig(cache.Body, out values, out version) ? values : null;
            }
        }

        private string CurrentScope()
        {
            return CurrentScope(ConsentEpochNow() != null && ConsentStateNow() == ShardPilotConsentState.Granted);
        }

        private string CurrentScope(bool includeClientId)
        {
            return ShardPilotRemoteConfigLogic.BuildScope(
                _config.WorkspaceKey,
                _config.EnvironmentKey,
                includeClientId && ShardPilotConfig.IsSafeAnonymousIdHeader(_config.AnonymousId) ? _config.AnonymousId : null,
                _config.AppUrl);
        }

        private static Dictionary<string, string> ConfigHeaders(ShardPilotRemoteConfigCacheEntry cache, string clientId)
        {
            var headers = cache == null || string.IsNullOrEmpty(cache.ETag)
                ? null : new Dictionary<string, string> {{"If-None-Match", cache.ETag}};
            // Defense at construction as well as the public setter: an unsafe
            // internal identity must never become a header or an identified scope.
            if (ShardPilotConfig.IsSafeAnonymousIdHeader(clientId))
            {
                if (headers == null) headers = new Dictionary<string, string>();
                headers["X-ShardPilot-Anonymous-Id"] = clientId;
            }
            return headers;
        }

        /// <summary>
        /// Captures an identified URL only while consent is granted. Read
        /// the epoch before state so a denial between the reads is caught
        /// either here or by the dispatch gate. Later grants cannot upgrade
        /// an already queued identifier-free request.
        /// </summary>
        private string BuildConsentedUrl(string clientId, out long consentEpochAtBuild)
        {
            consentEpochAtBuild = 0;
            var epoch = ConsentEpochNow();
            if (epoch == null || ConsentStateNow() != ShardPilotConsentState.Granted ||
                !ShardPilotConfig.IsSafeAnonymousIdHeader(clientId))
            {
                return null;
            }

            consentEpochAtBuild = epoch.Value;
            Dictionary<string, string> attributes = null;
            if (_config.RemoteConfigAttributesEnabled)
            {
                lock (_attributesGate)
                {
                    attributes = _attributes;
                }
            }

            int dropped;
            var normalized = ShardPilotExperimentsLogic.NormalizeAttributes(attributes, out dropped);
            return ShardPilotRemoteConfigLogic.BuildUrl(
                _config.AppUrl, _config.WorkspaceKey, _config.EnvironmentKey, normalized);
        }

        /// <summary>The wired consent-denial epoch; 0 when none is wired —
        /// the epoch check then stands down and the state read stands
        /// alone, the experiments-consumer posture — or null when the wired
        /// read THREW: unreadable, which both call sites fail closed on (no
        /// identified build; the pump-time comparison against a null never
        /// holds, so the strip wins).</summary>
        private long? ConsentEpochNow()
        {
            var read = _consentEpoch;
            if (read == null)
            {
                return 0;
            }

            try
            {
                return read();
            }
            catch
            {
                return null;
            }
        }

        /// <summary>The wired consent state, or Unknown when none is wired
        /// or the read THROWS — both the fail-closed direction
        /// (identifier-free, like an undecided player).</summary>
        private ShardPilotConsentState ConsentStateNow()
        {
            var read = _consentState;
            if (read == null)
            {
                return ShardPilotConsentState.Unknown;
            }

            try
            {
                return read();
            }
            catch
            {
                return ShardPilotConsentState.Unknown;
            }
        }

        /// <summary>
        /// The usable cache record for the given scope, or null — the
        /// FRESHEST of the in-process record (which survives a failed
        /// durable write) and the durable record (which another same-app
        /// instance may have refreshed since this instance last installed),
        /// compared by their fetched-at stamps; the in-process record wins
        /// ties, being known-good and already served. A durable record
        /// without an identity scope, written for any other scope, or whose
        /// body no longer yields a configuration is a miss: its values are
        /// never served and its ETag is never sent — a corrupt record's ETag
        /// would provoke a 304 with no body to recover from. The next
        /// successful fetch for this scope overwrites it.
        /// </summary>
        private ShardPilotRemoteConfigCacheEntry LoadCache(string scope)
        {
            lock (_gate)
            {
                // In-process records only ever hold a body that parsed when
                // they were installed, so no re-check is needed for this one.
                var held = _heldCache != null && string.Equals(_heldCache.Scope, scope, StringComparison.Ordinal)
                    ? _heldCache
                    : null;
                var durable = ReadDurableCacheLocked(scope);
                if (held != null && (durable == null || durable.FetchedAtUtc <= held.FetchedAtUtc))
                {
                    return held;
                }

                return durable;
            }
        }

        private ShardPilotRemoteConfigCacheEntry ReadDurableCacheLocked(string scope)
        {
            if (string.IsNullOrEmpty(_cachePath))
            {
                return null;
            }

            try
            {
                if (!File.Exists(_cachePath))
                {
                    return null;
                }

                var entry = ShardPilotRemoteConfigLogic.ParseCache(File.ReadAllText(_cachePath));
                if (entry == null || !string.Equals(entry.Scope, scope, StringComparison.Ordinal))
                {
                    return null;
                }

                Dictionary<string, object> values;
                long? version;
                if (!ShardPilotRemoteConfigLogic.TryParseConfig(entry.Body, out values, out version))
                {
                    return null;
                }

                return entry;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Settle an authoritative fetch outcome and, when it may, install
        /// it: the in-process cache record and the durable copy
        /// (best-effort). Closure first suppresses successful cached/fenced
        /// callbacks; the winning scope/sequence decision then closes on a
        /// terminal response or reopens on a valid 200/304. The gates:
        /// <list type="bullet">
        /// <item>the scope selected at dispatch must still match the current
        /// configuration, using that dispatch's identity mode, not live
        /// consent — checked BEFORE anything settles: an identity changed
        /// while an identified response was in flight
        /// makes the response another client's configuration, which must not
        /// be persisted — and it says nothing about the CURRENT scope's
        /// configuration either, so it must not settle the fence.</item>
        /// <item>the sequence fence — an outcome older than the newest
        /// settled one for its scope is dropped, so an older success can
        /// neither roll back a newer configuration nor sneak values in after
        /// a newer fail-closed outcome.</item>
        /// <item>only AUTHORITATIVE outcomes settle the fence (fresh 200,
        /// 304 revalidation, unauthorized, permanent error); a
        /// transient/cache fallback says nothing about the current
        /// configuration and must not fence off a fresh response still in
        /// flight.</item>
        /// <item>a fresh 200 (UpdatedCache exists exactly then) always
        /// installs; a successful 304 revalidation (RevalidatedCache exists
        /// exactly then) installs the captured record with a renewed
        /// freshness stamp; any other cache-served outcome installs only by
        /// ADOPTION — when
        /// the record it served is FRESHER than what this instance holds (or
        /// the instance holds nothing for the scope), e.g. it was discovered
        /// at fetch time after being written by an earlier launch or another
        /// same-app instance. A served record no fresher than the held one
        /// is never adopted: adopting a captured older record could roll
        /// back a fresher body installed while the response was in
        /// flight.</item>
        /// </list>
        /// </summary>
        private void Install(
            long seq,
            string scope,
            bool includeClientId,
            ShardPilotRemoteConfigOutcome outcome,
            ShardPilotRemoteConfigCacheEntry servedCache,
            ShardPilotHttpResponse response = null)
        {
            lock (_gate)
            {
                long settled;
                _settled.TryGetValue(scope, out settled);
                var scopeIsCurrent = string.Equals(CurrentScope(includeClientId), scope, StringComparison.Ordinal);
                var canInstall = scopeIsCurrent && seq > settled;
                string closedError;
                // Closed-scope recovery and installation use the same whole
                // scope/sequence decision; a rejected install cannot serve values.
                if (outcome.Result.Success && _closedScopes.TryGetValue(scope, out closedError) &&
                    (!outcome.Authoritative || !canInstall))
                {
                    outcome.Result = ShardPilotRemoteConfigResult.Failure(outcome.Result.Error ?? closedError);
                }

                if (!canInstall)
                {
                    return;
                }

                // Only a live positive hint can extend a window. Local echoes
                // carry no hint and never settle the response fence.
                if (response != null && (response.StatusCode == 429 || response.StatusCode >= 500) &&
                    response.RetryAfterSeconds > 0)
                {
                    var now = _utcNow();
                    PruneRetryWindowsLocked(now);
                    var deadline = now.AddSeconds(Math.Min(86400, response.RetryAfterSeconds));
                    RetryWindow window;
                    if (!_retryWindows.TryGetValue(scope, out window) || deadline > window.NotBefore)
                    {
                        _retryWindows[scope] = new RetryWindow { NotBefore = deadline, Status = response.StatusCode };
                    }
                }

                if (outcome.Authoritative)
                {
                    _settled[scope] = seq;
                }

                if (!outcome.Result.Success)
                {
                    if (outcome.Authoritative) _closedScopes[scope] = outcome.Result.Error;
                    return;
                }

                if (outcome.Authoritative) _closedScopes.Remove(scope);

                if (outcome.UpdatedCache != null)
                {
                    outcome.UpdatedCache.Scope = scope;
                    RaiseStampAboveSupersededLocked(outcome.UpdatedCache, scope, servedCache);
                    // The in-process record is updated even when the durable
                    // write fails: the freshest served configuration stays
                    // the offline fallback for this instance either way.
                    _heldCache = outcome.UpdatedCache;
                    if (!WriteDurableCacheLocked(outcome.UpdatedCache))
                    {
                        // The stale durable record this fetch captured may
                        // still be on disk, and a restart would revive it OVER
                        // the newer configuration just served. Clear it
                        // (best-effort — with the disk itself failing this
                        // fails too, and the stale record is at least
                        // superseded within this instance): a restart then
                        // starts from the game's defaults rather than from
                        // rolled-back values. Only the record the fetch meant
                        // to supersede (or an older one) is cleared: a sibling
                        // instance may have persisted a FRESHER record while
                        // this response was in flight, and deleting that would
                        // lose the freshest successfully persisted
                        // configuration. With nothing captured at fetch time
                        // there is nothing stale to clear — any record on disk
                        // now is such a sibling write.
                        var durable = ReadDurableCacheLocked(scope);
                        if (durable != null && servedCache != null &&
                            durable.FetchedAtUtc <= servedCache.FetchedAtUtc)
                        {
                            TombstoneDurableCacheLocked();
                        }
                    }

                    return;
                }

                if (outcome.RevalidatedCache != null)
                {
                    // A successful revalidation renews the record's
                    // freshness: the endpoint confirmed this body as current
                    // NOW, so the record outranks any same-scope sibling
                    // write stamped while the request was in flight. The
                    // renewed stamp is persisted too (best-effort), so a
                    // restart ranks the records the same way.
                    outcome.RevalidatedCache.Scope = scope;
                    RaiseStampAboveSupersededLocked(outcome.RevalidatedCache, scope, servedCache);
                    _heldCache = outcome.RevalidatedCache;
                    if (!WriteDurableCacheLocked(outcome.RevalidatedCache))
                    {
                        // The just-confirmed body could not be persisted. A
                        // durable record carrying the SAME body needs no
                        // action — only its stamp is stale, and losing the
                        // body to a tombstone would be worse. A DIFFERENT
                        // record no fresher than the one revalidated is stale
                        // content a restart would revive over the confirmed
                        // body (it can linger here when the write that
                        // superseded it failed and its tombstone failed too)
                        // — clear it like the fresh-200 path. A fresher
                        // sibling write is preserved.
                        var durable = ReadDurableCacheLocked(scope);
                        if (durable != null && servedCache != null &&
                            !string.Equals(durable.Body, servedCache.Body, StringComparison.Ordinal) &&
                            durable.FetchedAtUtc <= servedCache.FetchedAtUtc)
                        {
                            TombstoneDurableCacheLocked();
                        }
                    }

                    return;
                }

                if (outcome.Result.FromCache && servedCache != null &&
                    (_heldCache == null ||
                     !string.Equals(_heldCache.Scope, scope, StringComparison.Ordinal) ||
                     servedCache.FetchedAtUtc > _heldCache.FetchedAtUtc))
                {
                    _heldCache = servedCache;
                }
            }
        }

        /// <summary>
        /// Freshness stamps order the records for a scope, and the wall
        /// clock can move backward (an NTP correction, a user time change):
        /// stamped naively, a record being installed could rank BELOW the
        /// stale records it supersedes, and an offline fetch would roll back
        /// to them. So the stamp is raised to one tick above every record
        /// this install supersedes — the captured record, the held record,
        /// and the durable record for the scope. Only the relative order of
        /// stamps matters, so comparisons stay meaningful across restarts.
        /// </summary>
        private void RaiseStampAboveSupersededLocked(
            ShardPilotRemoteConfigCacheEntry entry,
            string scope,
            ShardPilotRemoteConfigCacheEntry servedCache)
        {
            var floor = default(DateTime);
            if (servedCache != null && servedCache.FetchedAtUtc > floor)
            {
                floor = servedCache.FetchedAtUtc;
            }

            if (_heldCache != null && string.Equals(_heldCache.Scope, scope, StringComparison.Ordinal) &&
                _heldCache.FetchedAtUtc > floor)
            {
                floor = _heldCache.FetchedAtUtc;
            }

            var durable = ReadDurableCacheLocked(scope);
            if (durable != null && durable.FetchedAtUtc > floor)
            {
                floor = durable.FetchedAtUtc;
            }

            if (entry.FetchedAtUtc <= floor)
            {
                entry.FetchedAtUtc = floor.AddTicks(1);
            }
        }

        private bool WriteDurableCacheLocked(ShardPilotRemoteConfigCacheEntry entry)
        {
            if (string.IsNullOrEmpty(_cachePath))
            {
                // No durable store is configured: nothing to write, and
                // nothing stale a restart could roll back to.
                return true;
            }

            try
            {
                var failure = WriteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                var directory = Path.GetDirectoryName(_cachePath);
                if (!string.IsNullOrEmpty(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                File.WriteAllText(_cachePath, ShardPilotRemoteConfigLogic.SerializeCache(entry));
                return true;
            }
            catch
            {
                return false;
            }
        }

        private void TombstoneDurableCacheLocked()
        {
            try
            {
                var failure = DeleteFailureForTesting;
                if (failure != null)
                {
                    throw failure;
                }

                if (File.Exists(_cachePath))
                {
                    File.Delete(_cachePath);
                }
            }
            catch
            {
                // Best-effort: with the disk itself failing there is nothing
                // more to do — the stale record is at least superseded by the
                // in-process record for the rest of this run.
            }
        }

        private static void Invoke(Action<ShardPilotRemoteConfigResult> callback, ShardPilotRemoteConfigResult result)
        {
            if (callback != null)
            {
                callback(result);
            }
        }
    }

#nullable restore annotations
    internal sealed class ShardPilotRemoteConfigCacheEntry
    {
        /// <summary>
        /// The (workspace, environment, client, remote-config URL) scope
        /// fingerprint the cache was fetched for; see
        /// <see cref="ShardPilotRemoteConfigLogic.BuildScope"/>.
        /// </summary>
        public string Scope { get; set; }

        public string ETag { get; set; }
        public string Body { get; set; }
        public DateTime FetchedAtUtc { get; set; }
    }

    internal sealed class ShardPilotRemoteConfigOutcome
    {
        public ShardPilotRemoteConfigCacheEntry UpdatedCache { get; set; }

        /// <summary>
        /// Set exactly for a successful 304 revalidation: the captured cache
        /// record with its freshness stamp renewed to the revalidation time.
        /// The body is unchanged; only the stamp says something new — the
        /// endpoint just confirmed this body as current, so the record must
        /// outrank same-scope records stamped while the request was in
        /// flight.
        /// </summary>
        public ShardPilotRemoteConfigCacheEntry RevalidatedCache { get; set; }

        public ShardPilotRemoteConfigResult Result { get; set; }

        /// <summary>
        /// Marks the outcomes that settle the fetch fence: a fresh 200, a
        /// successful 304 revalidation, an unauthorized response, and a
        /// permanent HTTP error. A transient/cache fallback is NOT
        /// authoritative — it says nothing about the current configuration,
        /// so it must not fence off a fresh response still in flight.
        /// </summary>
        public bool Authoritative { get; set; }
    }

    /// <summary>
    /// Pure remote-config decision logic, separated from UnityWebRequest and
    /// disk IO so it is testable without an editor.
    /// </summary>
    internal static class ShardPilotRemoteConfigLogic
    {
        /// <summary>
        /// Joins the escaped scope components; no escaped component can
        /// contain it, so distinct scopes can never collide into the same
        /// scope string.
        /// </summary>
        private const char ScopeSeparator = '\u001f';

        public static string BuildUrl(string baseUrl, string workspaceId, string environmentId)
        {
            return baseUrl.TrimEnd('/') +
                   "/config/v1/" + Uri.EscapeDataString(workspaceId) +
                   "/" + Uri.EscapeDataString(environmentId);
        }

        /// <summary>
        /// The attribute-carrying fetch URL: the
        /// attribute-less config path plus the already-normalized (name,
        /// value) attribute pairs as query parameters, appended in the given
        /// (ordinal-sorted) order. Both sides are escaped with the same
        /// injective percent-escaping the experiment assignment URL uses, so
        /// a value containing "&amp;", "=", or "#" cannot restructure the
        /// query. A null or EMPTY list appends nothing — the result is
        /// byte-identical to the attribute-less overload. The consent-aware
        /// caller separately selects the anonymous-ID header and attributes.
        /// </summary>
        public static string BuildUrl(
            string baseUrl,
            string workspaceId,
            string environmentId,
            List<KeyValuePair<string, string>> attributes)
        {
            var builder = new StringBuilder(BuildUrl(baseUrl, workspaceId, environmentId));
            if (attributes != null)
            {
                for (var i = 0; i < attributes.Count; i++)
                {
                    builder.Append(i == 0 ? '?' : '&');
                    builder.Append(Uri.EscapeDataString(attributes[i].Key));
                    builder.Append('=');
                    builder.Append(Uri.EscapeDataString(attributes[i].Value));
                }
            }

            return builder.ToString();
        }

        /// <summary>
        /// Identity components are escaped like URL segments — the escaping
        /// is injective ("%" itself is escaped, so every escaped character
        /// has exactly one spelling) — and then joined with a separator no
        /// escaped component can contain. Two distinct (workspace,
        /// environment, client, url) tuples therefore can never collide into
        /// one scope string, even when a raw component contains the
        /// separator. The base URL is the final component and is
        /// re-normalized here so equivalent spellings of the same endpoint
        /// can never produce distinct scopes.
        /// </summary>
        public static string BuildScope(string workspaceId, string environmentId, string clientId, string baseUrl)
        {
            return Uri.EscapeDataString(workspaceId ?? string.Empty) + ScopeSeparator +
                   Uri.EscapeDataString(environmentId ?? string.Empty) + ScopeSeparator +
                   Uri.EscapeDataString(clientId ?? string.Empty) + ScopeSeparator +
                   NormalizeScopeBaseUrl(baseUrl);
        }

        /// <summary>
        /// ShardPilotConfig.CloneWithDefaults already normalizes
        /// AppUrl to scheme://host[:port] with no trailing slash;
        /// this re-applies the trim/trailing-slash part so equivalent spellings
        /// of the same remote-config endpoint can never produce distinct scopes.
        /// </summary>
        private static string NormalizeScopeBaseUrl(string baseUrl)
        {
            return ShardPilotConfig.Trim(baseUrl).TrimEnd('/');
        }

        public static ShardPilotRemoteConfigOutcome Apply(
            ShardPilotRemoteConfigCacheEntry cache,
            ShardPilotHttpResponse response,
            DateTime nowUtc)
        {
            if (response != null && !response.TransportError && response.StatusCode == 200)
            {
                Dictionary<string, object> values;
                long? version;
                if (TryParseConfig(response.Body, out values, out version))
                {
                    return new ShardPilotRemoteConfigOutcome
                    {
                        UpdatedCache = new ShardPilotRemoteConfigCacheEntry
                        {
                            ETag = response.ETag ?? string.Empty,
                            Body = response.Body,
                            FetchedAtUtc = nowUtc
                        },
                        Result = new ShardPilotRemoteConfigResult
                        {
                            Success = true,
                            FromCache = false,
                            Values = values,
                            Version = version
                        },
                        Authoritative = true
                    };
                }

                // A malformed 200 delivered no configuration, so it says
                // nothing about the current one: serve the cache like any
                // other transient failure, and do not settle the fence.
                return ServeCacheOrFail(cache, "remote config response was not a JSON object of values");
            }

            if (response != null && !response.TransportError && response.StatusCode == 304 && cache != null)
            {
                Dictionary<string, object> values;
                long? version;
                if (TryParseConfig(cache.Body, out values, out version))
                {
                    // A successful revalidation is authoritative: the endpoint
                    // just confirmed the cached ETag as CURRENT, so an older
                    // in-flight response must not overwrite it afterwards. The
                    // body is unchanged (a 304 carries none), but the record's
                    // freshness stamp is renewed — the confirmation is NEW
                    // information about how current this body is.
                    return new ShardPilotRemoteConfigOutcome
                    {
                        RevalidatedCache = new ShardPilotRemoteConfigCacheEntry
                        {
                            ETag = cache.ETag,
                            Body = cache.Body,
                            FetchedAtUtc = nowUtc
                        },
                        Result = new ShardPilotRemoteConfigResult
                        {
                            Success = true,
                            FromCache = true,
                            Values = values,
                            Version = version
                        },
                        Authoritative = true
                    };
                }

                // The revalidated cache turned out unreadable: there is
                // nothing left to serve (a 304 carries no body), so this
                // fails rather than re-serving the very cache that just
                // failed to parse.
                return new ShardPilotRemoteConfigOutcome
                {
                    Result = ShardPilotRemoteConfigResult.Failure("remote config cache was unreadable after 304")
                };
            }

            // An unauthorized response is an authoritative "this key may not
            // read this config" from the remote-config endpoint, not a transient
            // outage: serving the cached snapshot would keep a revoked or
            // wrong key supplied with config indefinitely. Fail closed and
            // surface the auth failure; the cache file is kept untouched but
            // is never served for this outcome. Classification is
            // STATUS-FIRST: a received 401/403 whose BODY then stalled or was
            // truncated (UnityWebRequest reports a transport error while
            // responseCode carries the received status) still fails closed —
            // the refusal needs no body, and softening it into a
            // cache-serving transient would let a revoked key keep supplying
            // config through the exact corner a flaky connection produces.
            // A transport-errored 200/304 stays transient above/below: their
            // BODIES are the payload, and a truncated payload proves nothing
            // about the current configuration.
            if (response != null &&
                (response.StatusCode == 401 || response.StatusCode == 403))
            {
                return new ShardPilotRemoteConfigOutcome
                {
                    Result = ShardPilotRemoteConfigResult.Failure(
                        "remote config fetch unauthorized: status " +
                        response.StatusCode.ToString(CultureInfo.InvariantCulture)),
                    Authoritative = true
                };
            }

            // The cache fallback is reserved for failures a retry can
            // plausibly fix: no connection, backpressure, or a server-side
            // error.
            if (response == null || response.TransportError ||
                response.StatusCode == 0 || response.StatusCode == 408 ||
                response.StatusCode == 429 || response.StatusCode >= 500)
            {
                var reason = response == null
                    ? "no response"
                    : response.TransportError
                        ? "connection failed"
                        : "status " + response.StatusCode.ToString(CultureInfo.InvariantCulture);
                return ServeCacheOrFail(cache, "remote config fetch failed: " + reason);
            }

            // Any other status — a 404 for a removed environment, an
            // unexpected redirect, other 4xx, a 304 with no cached record to
            // revalidate — is an authoritative "this configuration is not
            // being served here", so the fetch fails instead of reporting
            // stale values as a healthy Success (the cache record is left
            // untouched).
            return new ShardPilotRemoteConfigOutcome
            {
                Result = ShardPilotRemoteConfigResult.Failure(
                    "remote config fetch rejected: status " +
                    response.StatusCode.ToString(CultureInfo.InvariantCulture)),
                Authoritative = true
            };
        }

        public static string SerializeCache(ShardPilotRemoteConfigCacheEntry entry)
        {
            var builder = new StringBuilder(entry.Body == null ? 96 : entry.Body.Length + 160);
            builder.Append("{\"scope\":");
            ShardPilotJson.AppendQuoted(builder, entry.Scope ?? string.Empty);
            builder.Append(",\"etag\":");
            ShardPilotJson.AppendQuoted(builder, entry.ETag ?? string.Empty);
            builder.Append(",\"fetched_at\":");
            ShardPilotJson.AppendQuoted(builder, ShardPilotJson.FormatTimestamp(entry.FetchedAtUtc));
            builder.Append(",\"body\":");
            ShardPilotJson.AppendQuoted(builder, entry.Body ?? string.Empty);
            builder.Append('}');
            return builder.ToString();
        }

        public static ShardPilotRemoteConfigCacheEntry ParseCache(string text)
        {
            Dictionary<string, object> map;
            if (!ShardPilotJsonParser.TryParseObject(text, out map))
            {
                return null;
            }

            object bodyValue;
            var body = map.TryGetValue("body", out bodyValue) ? bodyValue as string : null;
            if (string.IsNullOrEmpty(body))
            {
                return null;
            }

            // A cache without an identity scope cannot be attributed to any
            // (workspace, environment, client) tuple, so it is unusable.
            object scopeValue;
            var scope = map.TryGetValue("scope", out scopeValue) ? scopeValue as string : null;
            if (string.IsNullOrEmpty(scope))
            {
                return null;
            }

            object etagValue;
            var etag = map.TryGetValue("etag", out etagValue) ? etagValue as string : null;

            var fetchedAt = default(DateTime);
            object fetchedAtValue;
            if (map.TryGetValue("fetched_at", out fetchedAtValue))
            {
                var raw = fetchedAtValue as string;
                DateTime parsed;
                if (raw != null && DateTime.TryParse(
                        raw,
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
                        out parsed))
                {
                    fetchedAt = parsed;
                }
            }

            return new ShardPilotRemoteConfigCacheEntry
            {
                Scope = scope,
                ETag = etag ?? string.Empty,
                Body = body,
                FetchedAtUtc = fetchedAt
            };
        }

        /// <summary>
        /// Parses a configuration body end-to-end. The endpoint answers
        /// {"version": &lt;number&gt;, "values": {key: value}}, and the
        /// configuration map served to callers is the "values" object. A
        /// body whose "values" member is absent or not a keyed object (null,
        /// an array, a string, a number, a boolean — e.g. after a
        /// server-side schema regression) is MALFORMED: serving the body
        /// would expose wrapper fields as configuration and overwrite the
        /// last-known-good cache. The JSON
        /// reader preserves the null/array/object distinction in the decoded
        /// tree, so the shape is decided on the decoded value directly.
        /// Returns false when the body cannot supply a configuration map.
        /// </summary>
        public static bool TryParseConfig(string body, out Dictionary<string, object> values, out long? version)
        {
            values = null;
            version = null;
            Dictionary<string, object> decoded;
            if (!ShardPilotJsonParser.TryParseObject(body, out decoded))
            {
                return false;
            }

            object wrapped;
            if (!decoded.TryGetValue("values", out wrapped))
            {
                return false;
            }

            values = wrapped as Dictionary<string, object>;
            if (values == null)
            {
                return false;
            }

            // The published version is wrapper metadata, read only here.
            object versionValue;
            if (decoded.TryGetValue("version", out versionValue) && versionValue is long)
            {
                version = (long)versionValue;
            }

            return true;
        }

        /// <summary>
        /// Serve the cached snapshot for a transient failure, or fail when no
        /// usable cache exists. A served snapshot is still a SUCCESS — the
        /// game has usable configuration — with FromCache=true and Error
        /// carrying why the network could not refresh it. Never
        /// authoritative: a cache fallback says nothing about the current
        /// configuration, so it must not settle the fetch fence.
        /// </summary>
        private static ShardPilotRemoteConfigOutcome ServeCacheOrFail(ShardPilotRemoteConfigCacheEntry cache, string error)
        {
            Dictionary<string, object> values;
            long? version;
            if (cache != null && TryParseConfig(cache.Body, out values, out version))
            {
                return new ShardPilotRemoteConfigOutcome
                {
                    Result = new ShardPilotRemoteConfigResult
                    {
                        Success = true,
                        FromCache = true,
                        Error = error,
                        Values = values,
                        Version = version
                    }
                };
            }

            return new ShardPilotRemoteConfigOutcome
            {
                Result = ShardPilotRemoteConfigResult.Failure(error)
            };
        }
    }
}
