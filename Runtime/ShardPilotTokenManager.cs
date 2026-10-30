using System;
using System.Threading;
using System.Threading.Tasks;

namespace ShardPilot
{
    /// <summary>
    /// The JWT a Mode-B <see cref="ShardPilotConfig.TokenProvider"/> returns,
    /// optionally stamped with the absolute moment it stops being valid. The
    /// SDK only ever holds this opaque token string and its expiry — never the
    /// per-tenant signing secret, which stays on the host's backend.
    /// </summary>
#nullable enable annotations
    public sealed class ShardPilotTokenResult
    {
        public ShardPilotTokenResult(string? token)
            : this(token, DateTimeOffset.MaxValue)
        {
        }

        public ShardPilotTokenResult(string? token, DateTimeOffset expiresAt)
        {
            Token = token;
            ExpiresAt = expiresAt;
        }

        /// <summary>The Mode-B per-tenant HS256 JWT to send as the Bearer.</summary>
        public string? Token { get; private set; }

        /// <summary>
        /// When the token expires. The manager refreshes a configurable lead
        /// before this. <see cref="DateTimeOffset.MaxValue"/> (the default for
        /// the string-only constructor) means "no known expiry": the token is
        /// reused until a 401 forces a refresh.
        /// </summary>
        public DateTimeOffset ExpiresAt { get; private set; }
    }

    /// <summary>
    /// Caches the Mode-B JWT and drives its refresh, porting the Defold client
    /// token state machine (shardpilot/client.lua:600-645) to async C#:
    ///
    /// - the token + its expiry are cached;
    /// - a refresh is triggered BEFORE expiry (a configurable lead) and on a
    ///   401 (via <see cref="Invalidate"/>);
    /// - a SINGLE refresh is ever in flight — concurrent callers (a flush racing
    ///   the periodic worker, several batches in a drain loop) all await the one
    ///   provider invocation instead of stampeding the host's backend;
    /// - the provider is awaited off the game thread, so the main thread never
    ///   blocks on a token fetch.
    ///
    /// The manager never touches any per-tenant secret: it only stores the JWT
    /// string the provider returns.
    /// </summary>
#nullable restore annotations
    internal sealed class ShardPilotTokenManager
    {
        // Bounds a single shared refresh so a stalled provider cannot wedge
        // _inFlight forever. Manager-owned (never a caller's token), so it does not
        // cancel just because one waiter left.
        private static readonly TimeSpan DefaultProviderTimeout = TimeSpan.FromSeconds(30);

        private readonly Func<CancellationToken, Task<ShardPilotTokenResult>> _provider;
        private readonly ShardPilotDispatcher _providerHost;
        private readonly Func<bool> _hostCurrent;
        private readonly TimeSpan _refreshLead;
        private readonly TimeSpan _providerTimeout;
        private readonly Func<DateTimeOffset> _clock;
        private readonly object _gate = new object();

        private string _token;
        private DateTimeOffset _expiresAt = DateTimeOffset.MinValue;
        private Task<string> _inFlight;
        private long _invalidationGeneration;

        /// <summary>
        /// Test seam: runs on the settling thread AFTER a refresh has completed
        /// and BEFORE its continuation clears <c>_inFlight</c> — i.e. inside the
        /// exact window in which the slot still holds a task whose answer is
        /// already decided. Never set outside tests.
        /// </summary>
        internal volatile Action SettledBeforeSlotClearedForTesting;
        internal Action DeadlineBeforeCancelForTesting;

        internal ShardPilotTokenManager(
            Func<CancellationToken, Task<ShardPilotTokenResult>> provider,
            TimeSpan refreshLead)
            : this(provider, refreshLead, () => DateTimeOffset.UtcNow)
        {
        }

        internal ShardPilotTokenManager(Func<CancellationToken, Task<ShardPilotTokenResult>> provider,
            TimeSpan refreshLead, ShardPilotDispatcher providerHost, Func<bool> hostCurrent)
            : this(provider, refreshLead)
        {
            _providerHost = providerHost;
            _hostCurrent = hostCurrent;
        }

        internal ShardPilotTokenManager(
            Func<CancellationToken, Task<ShardPilotTokenResult>> provider,
            TimeSpan refreshLead,
            Func<DateTimeOffset> clock)
            : this(provider, refreshLead, clock, DefaultProviderTimeout)
        {
        }

        internal ShardPilotTokenManager(
            Func<CancellationToken, Task<ShardPilotTokenResult>> provider,
            TimeSpan refreshLead,
            Func<DateTimeOffset> clock,
            TimeSpan providerTimeout)
        {
            _provider = provider ?? throw new ArgumentNullException("provider");
            _refreshLead = refreshLead < TimeSpan.Zero ? TimeSpan.Zero : refreshLead;
            _clock = clock ?? (() => DateTimeOffset.UtcNow);
            _providerTimeout = providerTimeout <= TimeSpan.Zero ? DefaultProviderTimeout : providerTimeout;
        }

        /// <summary>
        /// Returns a usable Bearer JWT: the cached token when it is still
        /// outside the refresh lead, otherwise the result of a (single
        /// in-flight) provider refresh. Throws when the provider faults or
        /// yields an empty token, so the publish path treats a missing token
        /// the same as any other retryable publish failure.
        /// </summary>
        public Task<string> GetTokenAsync(CancellationToken cancellationToken)
        {
            Task<string> sharedRefresh;
            string cachedToken;
            bool serveCached;
            lock (_gate)
            {
                if (!NeedsRefreshLocked())
                {
                    return Task.FromResult(_token);
                }

                // A cached token that is only INSIDE the refresh lead (not yet
                // expired) is still usable. Serve it immediately and refresh in the
                // background, so a transient provider failure during the proactive
                // refresh never stalls ingestion before the token actually expires
                // or a 401 invalidates it. An expired/absent token must await the
                // refresh.
                serveCached = !IsExpiredLocked();
                cachedToken = _token;
                sharedRefresh = EnsureRefreshLocked();
            }

            if (serveCached)
            {
                return Task.FromResult(cachedToken);
            }

            // Bound THIS caller's wait by its own cancellation without faulting the
            // shared refresh that other callers await: the provider call itself runs
            // under a manager-owned timeout (see RefreshAsync), not any caller's
            // token, so one caller's timeout never cancels a refresh another caller
            // still needs.
            return AwaitRefresh(sharedRefresh, cancellationToken);
        }

        // Returns the single in-flight refresh, starting one if none is running.
        // Caller MUST hold _gate.
        private Task<string> EnsureRefreshLocked()
        {
            // Coalesce concurrent refreshers onto the one in-flight task so a flush
            // + the periodic worker (or several queued batches) never fire multiple
            // overlapping provider calls.
            //
            // Only onto a refresh that is STILL RUNNING. The slot is cleared by the
            // continuation below rather than in the settling path, so between the
            // moment a refresh settles and the moment that continuation takes _gate
            // the slot still holds a task whose answer is already decided. Joining it
            // hands the caller a decided answer to a question it asked LATER: a
            // faulted refresh replays a provider failure that may since have healed
            // (the caller reports "token provider failed" while the backend is up and
            // the provider is never even asked), and a succeeded-then-invalidated one
            // replays the very token a 401 just rejected — its result is returned
            // straight from the completed task, so RefreshAsync's generation check
            // never sees it.
            //
            // A settled slot therefore means "start a fresh refresh", never "share
            // this one". That costs nothing on the success path: RefreshAsync caches
            // the token BEFORE its task completes, so a refresh that succeeded and
            // still stands leaves NeedsRefreshLocked false and this method is not
            // reached at all.
            if (_inFlight != null && !_inFlight.IsCompleted)
            {
                return _inFlight;
            }

            // The provider runs under a manager-owned timeout (see RefreshAsync),
            // not any single waiter's token, so no single caller's cancellation
            // faults the shared refresh. Per-caller deadlines are applied in
            // AwaitRefresh instead.
            var task = RefreshAsync();
            _inFlight = task;

            // Clear the in-flight slot when the refresh settles. Done via a
            // continuation rather than inside RefreshAsync's finally: a provider
            // that completes synchronously would otherwise run that finally BEFORE
            // this assignment, leaving a completed task stuck in _inFlight and
            // starving every later refresh. The identity guard means a newer
            // in-flight refresh is never cleared by an older one's continuation.
            task.ContinueWith(
                t =>
                {
                    // Observe any fault so a background refresh (one whose caller was
                    // served the still-valid cached token and never awaited it) does
                    // not raise TaskScheduler.UnobservedTaskException.
                    _ = t.Exception;

                    // The settled-but-not-yet-cleared window, on demand. The window
                    // is a handful of instructions wide in production — measured at
                    // roughly 1 occurrence in 3000 under contention — so a test that
                    // races for it would be a coin flip in a suite that cannot
                    // afford another one. This seam holds the window open instead,
                    // so the guard above is asserted rather than sampled.
                    var settledHook = SettledBeforeSlotClearedForTesting;
                    if (settledHook != null)
                    {
                        settledHook();
                    }
                    lock (_gate)
                    {
                        if (ReferenceEquals(_inFlight, t))
                        {
                            _inFlight = null;
                        }
                    }
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);

            return task;
        }

        // Awaits the shared refresh, but lets THIS caller's cancellation bound only
        // its own wait — surfacing a retryable token failure — without cancelling
        // the shared provider call that other callers may still be waiting on.
        private static async Task<string> AwaitRefresh(Task<string> refresh, CancellationToken cancellationToken)
        {
            if (!cancellationToken.CanBeCanceled)
            {
                return await refresh.ConfigureAwait(false);
            }

            var cancelSignal = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).TrySetResult(true), cancelSignal))
            {
                var winner = await Task.WhenAny(refresh, cancelSignal.Task).ConfigureAwait(false);
                if (winner != refresh)
                {
                    // This caller's deadline elapsed; the shared refresh keeps
                    // running for others. Surface a retryable token failure so the
                    // publish re-attempts rather than dropping the batch.
                    throw new ShardPilotTokenException("token refresh did not complete within this request's deadline");
                }
            }

            return await refresh.ConfigureAwait(false);
        }

        /// <summary>
        /// Drops the cached token so the next <see cref="GetTokenAsync"/>
        /// refreshes. Called when the server answers a publish with 401: the
        /// JWT the host minted is stale/rejected, so re-ask the provider rather
        /// than resend the same rejected Bearer.
        /// </summary>
        public void Invalidate()
        {
            lock (_gate)
            {
                _token = null;
                _expiresAt = DateTimeOffset.MinValue;
                // Bump the generation so an already-in-flight refresh that began
                // BEFORE this 401 does not write its (now-rejected) result back into
                // the cache when it completes — see RefreshAsync.
                _invalidationGeneration++;
            }
        }

        internal bool HasCachedToken
        {
            get
            {
                lock (_gate)
                {
                    return !string.IsNullOrEmpty(_token);
                }
            }
        }

        private bool NeedsRefreshLocked()
        {
            if (string.IsNullOrEmpty(_token))
            {
                return true;
            }

            // No known expiry => reuse until a 401 invalidates it.
            if (_expiresAt == DateTimeOffset.MaxValue)
            {
                return false;
            }

            // Equivalent to `now >= expiresAt - lead`, but subtracting the lead from
            // a very early expiry (or with an unusually large lead) can underflow
            // DateTimeOffset and throw. Compare the remaining lifetime as a TimeSpan
            // instead — for a finite expiry and a real clock this never overflows,
            // and an already-expired token yields a non-positive span (refresh).
            return _expiresAt - _clock() <= _refreshLead;
        }

        // Whether the cached token is unusable NOW (absent, or past its expiry).
        // Distinct from NeedsRefreshLocked, which also returns true inside the
        // proactive refresh lead while the token is still usable. Caller holds _gate.
        private bool IsExpiredLocked()
        {
            if (string.IsNullOrEmpty(_token))
            {
                return true;
            }

            if (_expiresAt == DateTimeOffset.MaxValue)
            {
                return false;
            }

            return _clock() >= _expiresAt;
        }

        private async Task<string> RefreshAsync()
        {
            // Snapshot the invalidation generation BEFORE invoking the provider so a
            // 401 that lands while this refresh is in flight is detected below and
            // its (now-rejected) result is discarded rather than cached.
            long generation;
            lock (_gate)
            {
                generation = _invalidationGeneration;
            }

            // The provider runs under a MANAGER-owned cancellation source (never a
            // single caller's token), so a refresh that other callers coalesce onto
            // is not cancelled by one caller leaving. A bounded manager timeout
            // still caps it: the WhenAny means even a provider that IGNORES its
            // token cannot wedge _inFlight forever — on timeout this faults, the
            // continuation clears _inFlight, and the next GetTokenAsync starts anew.
            var cts = new CancellationTokenSource();
            // Arm before publishing host work. The delivery predicate also checks
            // the deadline itself, even if the timeout continuation is delayed.
            var deadline = Task.Delay(_providerTimeout);
            var providerTask = _providerHost == null ? InvokeProvider(cts.Token)
                : _providerHost.InvokeOnHost(() => _provider(cts.Token), cts.Token,
                    () => !deadline.IsCompleted && _hostCurrent());
            var winner = await Task.WhenAny(providerTask, deadline).ConfigureAwait(false);
            if (winner != providerTask)
            {
                DeadlineBeforeCancelForTesting?.Invoke();
                cts.Cancel(); // best-effort: signal a cooperative provider to abort
                DropExpiredToken();
                throw new ShardPilotTokenException("token provider did not respond within " + _providerTimeout);
            }

            ShardPilotTokenResult result;
            try
            {
                result = await providerTask.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Retirement abandons the owned invocation too; signal a cooperative provider.
                try { cts.Cancel(); } catch { /* Host cancellation handlers are best-effort. */ }
                DropExpiredToken();
                if (ex is ShardPilotTokenException)
                {
                    throw;
                }

                throw new ShardPilotTokenException("token provider failed: " + ex.Message, ex);
            }

            if (result == null || string.IsNullOrEmpty(result.Token))
            {
                DropExpiredToken();
                throw new ShardPilotTokenException("token provider returned an empty token");
            }

            lock (_gate)
            {
                if (generation != _invalidationGeneration)
                {
                    // A 401 invalidated the token WHILE this refresh (begun before
                    // the 401) was in flight. Caching its result would repopulate the
                    // possibly-rejected token; discard it and surface a retryable
                    // failure so the next attempt forces a fresh, post-401 provider
                    // call. The token is already cleared by Invalidate().
                    throw new ShardPilotTokenException("token invalidated during refresh");
                }

                _token = result.Token;
                _expiresAt = result.ExpiresAt;
            }

            return result.Token;

            // NOTE: _inFlight is cleared by the identity-guarded continuation in
            // EnsureRefreshLocked, NOT here — a finally would run before that method
            // assigns _inFlight when the provider completes synchronously, and would
            // then never clear the stuck completed task.
        }

        // Invokes the provider WITHOUT running its synchronous portion inline on the
        // caller: the delegate is dispatched so that any blocking work it does
        // before returning its Task neither freezes the caller (e.g. the Unity main
        // thread via SetConsent/ResolveBearerForConsent) nor escapes the manager
        // timeout (RefreshAsync's WhenAny is armed before the provider runs). A
        // synchronous throw becomes a faulted task so the failure handling applies
        // uniformly.
        private Task<ShardPilotTokenResult> InvokeProvider(CancellationToken cancellationToken)
        {
#if UNITY_WEBGL && !UNITY_EDITOR
            // Single-threaded WebGL has no worker thread to offload to. Yield first
            // so the timeout is armed and the immediate caller is not blocked inline;
            // the provider then runs on a later cooperative tick. (A provider that
            // does heavy synchronous work still violates the no-block contract.)
            return InvokeProviderDeferred(cancellationToken);
#else
            return Task.Run(() => _provider(cancellationToken) ?? Task.FromResult<ShardPilotTokenResult>(null));
#endif
        }

#if UNITY_WEBGL && !UNITY_EDITOR
        private async Task<ShardPilotTokenResult> InvokeProviderDeferred(CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await (_provider(cancellationToken) ?? Task.FromResult<ShardPilotTokenResult>(null)).ConfigureAwait(false);
        }
#endif

        // Drops the cached token ONLY when it is genuinely expired/absent. A
        // transient proactive-refresh failure within the lead keeps the still-valid
        // token in place so publishes keep flowing (stale-while-revalidate).
        private void DropExpiredToken()
        {
            lock (_gate)
            {
                if (IsExpiredLocked())
                {
                    _token = null;
                    _expiresAt = DateTimeOffset.MinValue;
                }
            }
        }
    }

    /// <summary>
    /// A Mode-B token acquisition failure (the provider faulted or returned an
    /// empty token). Treated as a retryable publish failure so the worker
    /// re-attempts on the next dispatch instead of dropping the batch.
    /// </summary>
    internal sealed class ShardPilotTokenException : Exception
    {
        public ShardPilotTokenException(string message) : base(message)
        {
        }

        public ShardPilotTokenException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
