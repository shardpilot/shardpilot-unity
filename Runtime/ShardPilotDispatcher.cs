using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

namespace ShardPilot
{
    internal sealed class ShardPilotDispatcher : MonoBehaviour
    {
        private static ShardPilotDispatcher _instance;
        private static readonly object LifecycleGate = new object();

        /// <summary>
        /// Serializes every explicit privacy decision's [apply -> persist ->
        /// propagate] sequence against the registration-time persisted-state
        /// refresh (<see cref="ShardPilotClient.RefreshPrivacyStateFromSettings"/>).
        /// Without it, a refresh could read the OLD persisted value, a
        /// concurrent decision could then persist AND propagate the new one
        /// to the (already registered) refreshing client, and the refresh's
        /// stale apply would land LAST — replaying a superseded grant or
        /// undoing a fresh crash opt-out right before the client arms its
        /// egress. Under this gate a refresh runs entirely before a
        /// decision's persist (the decision's propagation then corrects it)
        /// or entirely after its propagation (the refresh reads the new
        /// value) — the stale-apply interleaving cannot happen. Decisions
        /// and registrations are main-thread calls, so this gate is not
        /// acquired while any per-client dispatch gate is blocked on
        /// another thread.
        /// </summary>
        internal static readonly object PrivacyDecisionGate = new object();
        private static readonly List<WeakReference<ShardPilotClient>> LifecycleClients =
            new List<WeakReference<ShardPilotClient>>();
        private readonly Queue<IEnumerator> _work = new Queue<IEnumerator>();
        private readonly object _gate = new object();
        private bool _retired;
        private readonly HashSet<Action> _retirement = new HashSet<Action>();
        private readonly LinkedList<Action> _hostCalls = new LinkedList<Action>();
        internal static ShardPilotDispatcher CapturedHost => _instance;
        internal bool IsLive { get { lock (_gate) return !_retired; } }

        internal Action<T> WrapCallback<T>(Action<T> callback, Func<bool> current, Func<T, T> prepare = null)
        {
            if (callback == null) return null;
            return result =>
            {
                var notify = Interlocked.Exchange(ref callback, null);
                if (notify == null) return;
                _ = InvokeOnHost(() =>
                {
                    var value = prepare == null ? result : prepare(result);
                    try { notify(value); } catch { /* Game code cannot break internal settlement. */ }
                    return Task.FromResult(true);
                }, CancellationToken.None, current);
            };
        }

        public static ShardPilotDispatcher Instance
        {
            get
            {
                if (_instance == null)
                {
                    var existing = FindObjectOfType<ShardPilotDispatcher>();
                    if (existing != null && !existing._retired)
                    {
                        _instance = existing;
                    }
                    else
                    {
                        var gameObject = new GameObject("ShardPilot Dispatcher");
                        DontDestroyOnLoad(gameObject);
                        _instance = gameObject.AddComponent<ShardPilotDispatcher>();
                    }
                }

                return _instance;
            }
        }

        public void Run(IEnumerator routine)
        {
            lock (_gate)
            {
                if (!_retired) _work.Enqueue(routine);
            }
        }

        internal void Run(IEnumerator routine, Action retired)
        {
            lock (_gate)
                if (!_retired) { _retirement.Add(retired); _work.Enqueue(Observe(routine, retired)); return; }
            retired();
        }
        private IEnumerator Observe(IEnumerator routine, Action retired)
        {
            try { yield return routine; }
            finally { lock (_gate) _retirement.Remove(retired); retired(); }
        }

        // Captured by the producing client on initialization, never resolved
        // from a worker or rebound to a successor host. Ownership lasts until
        // the async answer, cancellation or host retirement, beyond the posting thunk.
        internal Task<T> InvokeOnHost<T>(Func<Task<T>> invoke, CancellationToken cancellationToken, Func<bool> current)
        {
            var completion = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            Action cancel = () => completion.TrySetCanceled();
            LinkedListNode<Action> queued = null;
            lock (_gate)
            {
                if (_retired) cancel();
                else
                {
                    _retirement.Add(cancel);
                    queued = _hostCalls.AddLast(() => { if (!completion.Task.IsCompleted) _ = Invoke(); });
                }
            }
            return Await();

            async Task Invoke()
            {
                try
                {
                    if (cancellationToken.IsCancellationRequested || !current()) { cancel(); return; }
                    var value = await (invoke() ?? Task.FromResult(default(T))).ConfigureAwait(false);
                    lock (_gate) { if (!_retired && current()) completion.TrySetResult(value); else cancel(); }
                }
                catch (OperationCanceledException) { cancel(); }
                catch (Exception error) { completion.TrySetException(error); }
            }
            async Task<T> Await()
            {
                using (cancellationToken.Register(cancel))
                {
                    try { return await completion.Task.ConfigureAwait(false); }
                    finally { lock (_gate) { _retirement.Remove(cancel); if (queued?.List != null) _hostCalls.Remove(queued); } }
                }
            }
        }

        internal void DrainHostCalls()
        {
            while (true)
            {
                Action call;
                lock (_gate) { if (_retired || _hostCalls.Count == 0) return; call = _hostCalls.First.Value; _hostCalls.RemoveFirst(); }
                call(); // Host code always runs outside the dispatcher gate.
            }
        }

        internal static void RetireHost()
        {
            var instance = _instance;
            if (instance == null) return;
            Action[] retired;
            lock (instance._gate) { instance._retired = true; instance._work.Clear(); instance._hostCalls.Clear(); retired = new Action[instance._retirement.Count]; instance._retirement.CopyTo(retired); instance._retirement.Clear(); }
            _instance = null;
            foreach (var cancel in retired) { try { cancel(); } catch { /* Settle every owner even if a callback throws. */ } }
            if (Application.isPlaying) Destroy(instance.gameObject);
            else DestroyImmediate(instance.gameObject);
        }

        // Test seam: when set, RunOnMainThread hands the action here instead
        // of the live pump, so a test can capture the deferred work, assert
        // it did NOT run inline, and drive it explicitly. Never assigned in
        // production.
        internal static Action<Action> MainThreadMarshalOverrideForTesting;

        /// <summary>
        /// Queues an action for the next main-thread pump — the marshal for
        /// background completions that must touch Unity APIs (e.g.
        /// re-installing crash hooks after a deferred facade close). Safe to
        /// call from any thread, but only DELIVERS when the dispatcher host
        /// already exists (any Initialize/registration created it): the
        /// getter must never run FindObjectOfType/AddComponent off the main
        /// thread, so with no live instance the action is dropped — callers
        /// must treat delivery as best-effort.
        /// </summary>
        internal static void RunOnMainThread(Action action)
        {
            if (action == null)
            {
                return;
            }

            var overrideForTesting = MainThreadMarshalOverrideForTesting;
            if (overrideForTesting != null)
            {
                overrideForTesting(action);
                return;
            }

            var instance = _instance;
            if (instance == null)
            {
                return;
            }

            instance.Run(RunActionRoutine(action));
        }

        private static IEnumerator RunActionRoutine(Action action)
        {
            action();
            yield break;
        }

        /// <summary>
        /// Registers a client whose pending events are persisted to the
        /// offline spool when the application pauses or quits. Clients are
        /// held weakly so a replaced or closed client never leaks. ONE LIVE
        /// CLIENT PER SPOOL SCOPE owns the file: registering a new client
        /// that owns the same spool file both removes the previous
        /// registration AND transfers file ownership — the replaced client's
        /// spool is disowned, so even its still-running worker (an async
        /// close draining in the background) can no longer rewrite the file
        /// from a stale in-memory view; it keeps its in-memory delivery
        /// behavior only. A predecessor matched ONLY through the experiment
        /// cache scope is different: its experiments lane hands off, but it
        /// keeps its spool ownership AND its registration — the successor
        /// claims none of its spool files, so it stays the one live owner
        /// of that spool scope. A closing client also unregisters itself,
        /// see <see cref="UnregisterLifecycleClient"/>. Call from the Unity
        /// main thread (touching <see cref="Instance"/> guarantees the
        /// DontDestroyOnLoad host object exists to receive the engine's
        /// lifecycle messages).
        /// </summary>
        public static void RegisterLifecycleClient(ShardPilotClient client)
        {
            if (client == null)
            {
                return;
            }

            _ = Instance;
            var spoolFilePath = client.SpoolFilePath;
            var crashSpoolFilePath = client.CrashSpoolFilePath;

            // The CONFIG-resolved cache path, independent of the successor's
            // experiments flag: a flag-off AND spool-less successor exposes
            // no in-use cache path and no spool file at all, yet it still
            // REPLACES an enabled predecessor that owns the same cache — the
            // match must see that, or the old lane keeps installing and the
            // parked destructive debts die instead of tombstoning.
            var configuredExperimentsCachePath = client.ExperimentsCacheScopePath;
            var experimentsCachePath = ShardPilotExperiments.ConfirmCacheIdentity(
                configuredExperimentsCachePath, client.ExperimentsCacheStore);
            BindLifecycleCacheIdentities();
            var experimentsScopeIdentity = client.ExperimentsScopeIdentity;
            var experimentsScopeIdentitySansBaseUrl = client.ExperimentsScopeIdentitySansBaseUrl;
            var experimentsScopeIdentitySansCredentials = client.ExperimentsScopeIdentitySansCredentials;
            var experimentsDarkSuccessor = !client.ExperimentsConfigEnabled;
            var experimentsTokenOnlyDarkSuccessor =
                experimentsDarkSuccessor && client.ExperimentsConfigCredentialless;
            var handedOff = new List<ShardPilotClient>();
            lock (LifecycleGate)
            {
                // The spool lock is a leaf lock, so disowning inside this
                // lock hold keeps replacement + ownership transfer atomic
                // with respect to concurrent registrations.
                LifecycleClients.RemoveAll(reference =>
                {
                    ShardPilotClient existing;
                    if (!reference.TryGetTarget(out existing))
                    {
                        return true;
                    }

                    // Paths are canonicalized at resolution; the comparison
                    // is additionally case-INSENSITIVE on all platforms — a
                    // conservative trade (consistent across our SDKs): two
                    // same-letters spellings are treated as one physical file
                    // even on case-sensitive filesystems, because two
                    // registered owners of one file would overwrite each
                    // other, while a false match merely persists-and-disowns
                    // a client that shares the file's spelling. A client owns
                    // TWO spool files (events + crash reports); sharing
                    // EITHER makes it the same scope — matching on both keeps
                    // the single-owner invariant even when one of the spools
                    // is disabled on one side.
                    var sharesSpool = SharesAnySpoolFile(existing, spoolFilePath, crashSpoolFilePath);

                    // A client sharing NEITHER spool file may still share
                    // the EXPERIMENT CACHE: with both spools
                    // disabled/unavailable there is no spool file to match
                    // on, yet the displaced client's automatic lane would
                    // keep installing and persisting over the assignment
                    // cache this successor now owns, and its parked
                    // destructive debts would die unadopted (or, for a
                    // flag-off successor, never tombstone). The registered
                    // predecessor's IN-USE cache path is therefore matched
                    // against the successor's CONFIG-RESOLVED one — the
                    // successor's own experiments flag does not gate the
                    // match, so a flag-off spool-less replacement still
                    // displaces the enabled predecessor; a DARK predecessor
                    // (no in-use path) matches nothing, so an analytics-only
                    // sibling registration is never evicted by a cache-path
                    // coincidence. The cache leg additionally requires the
                    // config-derived experiment SCOPE identities to match:
                    // the cache file name is FIXED per directory, so an
                    // unrelated workspace/app/environment in the same
                    // persistentDataPath shares the path string without
                    // sharing any experiment state — a path-only match
                    // would let that unrelated (possibly dark) registration
                    // disown this client's live lane and park debts no
                    // successor ever adopts. Scope identity is
                    // config-derived on BOTH sides, so a genuine flag-off
                    // same-scope replacement still matches — and a DARK
                    // successor that also CLEARED AppUrl
                    // matches through the URL-LESS tenant identity: its
                    // full identity carries an empty URL where the enabled
                    // predecessor's carries the old server URL, yet it
                    // has no experiments config to contradict — same
                    // tenant + same cache path suffices. An ENABLED
                    // successor keeps the full-identity requirement (a
                    // different URL there is genuinely a different control
                    // plane). A TOKEN-ONLY dark successor — experiments
                    // off, PublishableKey removed (TokenProvider carries ingest
                    // auth), URL cleared — goes one step further: its
                    // credential fingerprint is EMPTY where the enabled
                    // predecessor's carries the old sp_ key's, so even the
                    // URL-less identity never matches; with its
                    // experiments-plane credentials WHOLLY absent it
                    // asserts no tenant-credential claim to contradict, and
                    // the bare workspace/app/environment triple (plus the
                    // cache path) suffices — the fingerprint exists to
                    // separate tenants under an in-place KEY SWAP, and the
                    // unrelated-tenant protection still holds through the
                    // triple. A dark successor that still CARRIES a key
                    // keeps the credential-checked URL-less compare.
                    var sharesCacheScope = SharesExperimentsCacheScope(
                        existing,
                        experimentsCachePath,
                        configuredExperimentsCachePath,
                        experimentsScopeIdentity,
                        experimentsScopeIdentitySansBaseUrl,
                        experimentsScopeIdentitySansCredentials,
                        experimentsDarkSuccessor,
                        experimentsTokenOnlyDarkSuccessor);
                    if (!sharesSpool && !sharesCacheScope)
                    {
                        return false;
                    }

                    if (sharesSpool)
                    {
                        // Successor link FIRST: from here on the displaced
                        // client's persist paths — pause, close,
                        // close-timeout — finding their spool disowned, hand
                        // their would-be snapshot to this successor's spool
                        // instead, so an event accepted through a still-held
                        // old reference stays durable. Then the last durable
                        // snapshot lands in the file the old client still
                        // owns, and ownership transfers. Normally
                        // PrepareOwnershipHandoff already persisted+disowned
                        // the EVENT spool BEFORE the new client's
                        // construction (so its load saw the remnant); with
                        // the successor now set, this persist forwards
                        // anything accepted in the construction gap, and the
                        // disown is idempotent. The CRASH spool is disowned
                        // only HERE (it stayed owned through the
                        // construction gap so gap captures kept a durable
                        // home), and its backlog is then forwarded so a gap
                        // record the successor's load missed is not dropped
                        // by the successor's next rewrite.
                        existing.SetSpoolSuccessor(client);
                        existing.PersistPendingEvents();
                        existing.DisownSpool();
                        existing.ForwardCrashSpoolBacklogToSuccessor();
                    }
                    else
                    {
                        // Cache-only match: NO spool ownership changes hands
                        // — this successor claims neither of the displaced
                        // client's spool files, so linking it as the spool
                        // successor and disowning would strip durability the
                        // old client still legitimately has (a spool-less
                        // successor accepts no handoff entries, and events
                        // or crash reports accepted through a still-held old
                        // reference would lose their offline home). The
                        // persist BARRIER still runs: it lands the old
                        // client's pending remnant in its OWN, still-owned
                        // spool and retries the owed experiment housekeeping
                        // (durable syncs, purge-marker debt) before the lane
                        // disown below parks what remains.
                        existing.PersistPendingEvents();
                    }

                    // The displaced client's EXPERIMENTS lane hands off like
                    // the facade path's: its automatic revalidation stops (a
                    // late old-lane response must not install or persist an
                    // old assignment over the cache this successor now owns)
                    // and its parked destructive debts — authoritative drops
                    // and the owed whole-record clear whose durable writes
                    // failed on the old client — adopt into the successor's
                    // own convergence machinery (or, for a flag-off
                    // successor, persist as durable tombstones). The disown
                    // normally already ran at PrepareOwnershipHandoff,
                    // before this successor's construction; it is
                    // idempotent, and running it here too covers direct
                    // registrations that skipped the pre-construction
                    // handoff. Ordering matters: disown FIRST (it parks the
                    // debts), adopt second (it drains them).
                    existing.DisownExperimentsForHandoff();
                    client.AdoptExperimentDebtsFromPredecessor(existing);
                    handedOff.Add(existing);

                    // A spool-sharing predecessor leaves the registry (the
                    // successor is the one live owner of that spool scope
                    // now). A cache-only predecessor STAYS registered: it
                    // remains the sole live owner of its own spool files —
                    // dropping it would orphan them (a later same-spool
                    // registration could no longer find and displace it,
                    // and the pause/quit persistence pass would skip it) —
                    // while its experiments lane is disowned above, so the
                    // one-live-lane-per-cache-scope invariant still holds.
                    return sharesSpool;
                });
                LifecycleClients.Add(new WeakReference<ShardPilotClient>(client));
            }

            // The replaced clients' warnings run here, outside the gate and
            // after this successor adopted their parked debts (#159): the
            // warning says "Close it", and a host logger that does so at once
            // must not cost the successor those debts.
            for (var i = 0; i < handedOff.Count; i++)
            {
                handedOff[i].EmitReplacedNoticeIfOwed();
            }

            // Registration-time catch-up: a client constructed concurrently
            // with a consent/opt-out decision can load the OLD persisted
            // value and then miss the decision's live propagation (the
            // propagation snapshot may enumerate before this registration).
            // Re-resolving the persisted state HERE closes the gap: this
            // registration happens after the propagation snapshot, which
            // happens after the decision was persisted (or its process-wide
            // pending shadow recorded), so this re-read observes it.
            // Idempotent when nothing changed.
            client.RefreshPrivacyStateFromSettings();
        }

        /// <summary>
        /// Hands ownership of the given spool files (event and/or crash) over
        /// BEFORE a successor client for their scope is constructed: any
        /// registered client owning EITHER file persists its pending event
        /// remnant (its last durable snapshot) and has its EVENT spool
        /// disowned — so the successor's construction-time load already
        /// contains the displaced remnant, and the old client's later event
        /// rewrites cannot clobber it. The old client's CRASH spool is NOT
        /// disowned here: no successor exists yet to accept crash-report
        /// handoffs, and crash reports have no in-memory remnant — its
        /// disown happens at the successor's registration, right after the
        /// successor link is installed. The successor's config-derived
        /// EXPERIMENT CACHE scope (path + tenant identity) rides along too:
        /// a registered predecessor sharing the cache but NEITHER spool
        /// file (a spool-less replacement on either side) has its
        /// experiments lane disowned HERE, before the successor's
        /// construction loads that cache — the registration-time cache
        /// match alone runs only AFTER the load, leaving a window where
        /// the predecessor's held automatic revalidation could resolve and
        /// persist an old assignment the successor then restores. The old
        /// client stays registered until the successor's registration
        /// replaces it. Touches only OLD clients; registers nothing. Call
        /// from the Unity main thread.
        /// </summary>
        public static void PrepareOwnershipHandoff(
            string spoolFilePath,
            string crashSpoolFilePath,
            string experimentsCachePath = null,
            string experimentsScopeIdentity = null,
            string experimentsScopeIdentitySansBaseUrl = null,
            string experimentsScopeIdentitySansCredentials = null,
            bool experimentsDarkSuccessor = false,
            bool experimentsTokenOnlyDarkSuccessor = false, Func<bool> constructionLive = null,
            string experimentsStoreIdentity = null)
        {
            if (string.IsNullOrEmpty(spoolFilePath) && string.IsNullOrEmpty(crashSpoolFilePath) &&
                string.IsNullOrEmpty(experimentsCachePath))
            {
                return;
            }

            var configuredExperimentsCachePath = experimentsCachePath;
            experimentsCachePath = ShardPilotExperiments.ConfirmCacheIdentity(experimentsCachePath, experimentsStoreIdentity);
            BindLifecycleCacheIdentities();
            lock (LifecycleGate)
            {
                if (constructionLive != null && !constructionLive()) return;
                for (var i = 0; i < LifecycleClients.Count; i++)
                {
                    ShardPilotClient existing;
                    if (!LifecycleClients[i].TryGetTarget(out existing))
                    {
                        continue;
                    }

                    // Case-insensitive, matching the registration compare.
                    // Only the EVENT spool is disowned here: no successor
                    // exists yet to accept crash-report handoffs, and a
                    // disowned crash spool with no successor would drop the
                    // durability of a crash captured inside the construction
                    // gap (a crash report has no in-memory remnant to
                    // recover later). The crash spool's disown happens at
                    // registration, right after the successor link is set.
                    if (SharesAnySpoolFile(existing, spoolFilePath, crashSpoolFilePath))
                    {
                        existing.PersistPendingEvents();
                        if (constructionLive != null && !constructionLive()) return;
                        existing.DisownEventSpoolForHandoff();

                        // The experiments automatic lane stops HERE — before
                        // the successor exists, the facade's ordering: its
                        // construction loads the shared assignment cache,
                        // and a late old-lane revalidation response landing
                        // in the construction gap must not install or
                        // persist over what the successor is about to own.
                        // This also PARKS the old client's destructive cache
                        // debts, which the successor's registration adopts.
                        existing.DisownExperimentsForHandoff();
                    }
                    else if (SharesExperimentsCacheScope(
                                 existing,
                                 experimentsCachePath,
                                 configuredExperimentsCachePath,
                                 experimentsScopeIdentity,
                                 experimentsScopeIdentitySansBaseUrl,
                                 experimentsScopeIdentitySansCredentials,
                                 experimentsDarkSuccessor,
                                 experimentsTokenOnlyDarkSuccessor))
                    {
                        // A predecessor sharing only the EXPERIMENT CACHE —
                        // the registration compare's EXACT cache leg (the
                        // ONE shared predicate: in-use path against the
                        // successor's config-resolved one, plus the tenant
                        // scope identity with the SAME relaxed
                        // dark-successor legs, so an unrelated tenant
                        // coinciding on the fixed cache file name never
                        // matches while a dark or token-only-dark direct
                        // replacement matches HERE too, not only at
                        // registration). No spool changes hands (the
                        // successor claims neither of this client's spool
                        // files), but the experiments lane must stop before
                        // the successor's construction loads the shared
                        // cache — a held revalidation released inside the
                        // construction window would otherwise persist an
                        // old assignment pre-disown, and the successor (or
                        // the next launch) would restore it. The
                        // registration-time disown remains the idempotent
                        // second edge; the parked destructive debts adopt
                        // there.
                        existing.DisownExperimentsForHandoff();
                    }
                }
            }
        }

        /// <summary>
        /// True when the client owns either of the given spool files —
        /// canonicalized paths compared case-insensitively, the registry's
        /// standard comparison. Null paths never match.
        /// </summary>
        private static bool SharesAnySpoolFile(ShardPilotClient client, string spoolFilePath, string crashSpoolFilePath)
        {
            return PathsEqual(client.SpoolFilePath, spoolFilePath) ||
                   PathsEqual(client.CrashSpoolFilePath, crashSpoolFilePath);
        }

        /// <summary>
        /// THE experiment cache-scope match — the ONE predicate BOTH match
        /// sites share (the registration compare and the pre-construction
        /// ownership handoff), so a successor matched at one site is
        /// matched at the other: the registered predecessor's IN-USE cache
        /// path against the successor's CONFIG-resolved one, plus the
        /// config-derived tenant scope identity — full identity for an
        /// ENABLED successor (a different URL there is genuinely a
        /// different server); the URL-LESS identity for a DARK
        /// successor (it has no experiments config to contradict);
        /// the bare workspace/app/environment triple for a TOKEN-ONLY dark
        /// successor whose experiments-plane credentials are wholly absent
        /// (no key = no tenant-credential claim to contradict). Two
        /// sites with diverging legs reopened the construction window for
        /// exactly the successors only the registration recognized: the
        /// predecessor's held revalidation could persist an old assignment
        /// before the late disown.
        /// </summary>
        private static bool SharesExperimentsCacheScope(
            ShardPilotClient existing,
            string experimentsCachePath,
            string configuredExperimentsCachePath,
            string experimentsScopeIdentity,
            string experimentsScopeIdentitySansBaseUrl,
            string experimentsScopeIdentitySansCredentials,
            bool experimentsDarkSuccessor,
            bool experimentsTokenOnlyDarkSuccessor)
        {
            var existingKey = existing.ExperimentsCacheOwnerKey;
            if (existingKey != null || experimentsCachePath != null)
            {
                if (!string.Equals(existingKey, experimentsCachePath, StringComparison.Ordinal)) return false;
            }
            else
            {
                // Before binding, this identity only revokes the old lifecycle lane; it grants no storage authority.
                var inUsePath = existing.ExperimentsCachePath;
                if (string.IsNullOrEmpty(inUsePath) || string.IsNullOrEmpty(configuredExperimentsCachePath)) return false;
                try
                {
                    if (!ShardPilotPathIdentity.Comparer.Equals(System.IO.Path.GetFullPath(inUsePath),
                        System.IO.Path.GetFullPath(configuredExperimentsCachePath))) return false;
                }
                catch (ArgumentException) { return false; }
                catch (NotSupportedException) { return false; }
                catch (System.IO.IOException) { return false; }
                catch (UnauthorizedAccessException) { return false; }
            }

            if (string.Equals(
                    existing.ExperimentsScopeIdentity,
                    experimentsScopeIdentity,
                    StringComparison.Ordinal))
            {
                return true;
            }

            if (experimentsDarkSuccessor &&
                experimentsScopeIdentitySansBaseUrl != null &&
                string.Equals(
                    existing.ExperimentsScopeIdentitySansBaseUrl,
                    experimentsScopeIdentitySansBaseUrl,
                    StringComparison.Ordinal))
            {
                return true;
            }

            return experimentsTokenOnlyDarkSuccessor &&
                   experimentsScopeIdentitySansCredentials != null &&
                   string.Equals(
                       existing.ExperimentsScopeIdentitySansCredentials,
                       experimentsScopeIdentitySansCredentials,
                       StringComparison.Ordinal);
        }

        private static bool PathsEqual(string left, string right)
        {
            return left != null && right != null &&
                   string.Equals(left, right, StringComparison.OrdinalIgnoreCase);
        }

        private static void BindLifecycleCacheIdentities()
        {
            WeakReference<ShardPilotClient>[] snapshot;
            lock (LifecycleGate) snapshot = LifecycleClients.ToArray();
            foreach (var reference in snapshot)
                if (reference.TryGetTarget(out var client)) client.BindExperimentsCacheIdentity();
        }

        /// <summary>
        /// Removes a client from the lifecycle registry. The client's worker
        /// calls this as it exits (close/shutdown): the close drain already
        /// persisted the remnant, and a closed client must not keep rewriting
        /// the spool file over a newer client for the same scope. Pure C# —
        /// safe to call from the worker thread; a no-op for clients that
        /// never registered.
        /// </summary>
        public static void UnregisterLifecycleClient(ShardPilotClient client)
        {
            if (client == null)
            {
                return;
            }

            lock (LifecycleGate)
            {
                LifecycleClients.RemoveAll(reference =>
                {
                    ShardPilotClient existing;
                    return !reference.TryGetTarget(out existing) || ReferenceEquals(existing, client);
                });
            }
        }

        /// <summary>
        /// Propagates a consent denial to every OTHER live registered client.
        /// The persisted decision is one GLOBAL key the others would read at
        /// their next construction; this is eager propagation of that state,
        /// so their in-memory pre-denial events (queue, held batch, restored
        /// backlog) are dropped NOW instead of being deliverable until the
        /// next launch — and each propagated client also sweeps ITS OWN
        /// resolved spool directory (skipped when it canonically equals
        /// <paramref name="sweptDirectory"/>, which the initiator already
        /// swept). Snapshot under the registry lock, invoke outside it — the
        /// same discipline as the pause persistence pass.
        /// </summary>
        public static void PropagateConsentDenial(
            ShardPilotClient source,
            string sweptDirectory,
            ShardPilotConsentState deniedState = ShardPilotConsentState.Denied,
            DateTime? decidedAtUtc = null,
            bool deciderRecordPersisted = true,
            bool deciderMarkerDurable = false,
            string deciderMarkerActor = null,
            long deciderDecisionSeq = 0,
            ShardPilotConsentNoticeSnapshot notice = null)
        {
            List<ShardPilotClient> clients;
            lock (LifecycleGate)
            {
                clients = new List<ShardPilotClient>(LifecycleClients.Count);
                for (var i = 0; i < LifecycleClients.Count; i++)
                {
                    ShardPilotClient client;
                    if (LifecycleClients[i].TryGetTarget(out client) && !ReferenceEquals(client, source))
                    {
                        clients.Add(client);
                    }
                }
            }

            for (var i = 0; i < clients.Count; i++)
            {
                try
                {
                    clients[i].ApplyGlobalConsentDenial(
                        sweptDirectory, deniedState, decidedAtUtc,
                        deciderRecordPersisted, deciderMarkerDurable,
                        deciderMarkerActor, deciderDecisionSeq, freshExplicitDecision: true, notice: notice);
                }
                catch
                {
                    // Propagation must never throw into the denying caller.
                }
            }
        }

        /// <summary>
        /// Applies a consent GRANT decided on one client to every OTHER live
        /// registered client: under consent-first the in-memory state gates
        /// every enqueue, and the persisted key is global — a sibling client
        /// must start accepting events NOW (future events only), not at its
        /// next construction. Mirrors the denial propagation; never throws
        /// into the granting caller. Each sibling arms its OWN scope's
        /// pending consent receipt (receipt scope is baked into the body),
        /// so every sibling's first post-grant batch enforces the same
        /// receipt-before-batch ordering as the deciding client
        /// and every scope records its own consent row.
        /// </summary>
        /// <summary>
        /// Returns whether EVERY propagated sibling's required own-scope
        /// receipt is durably safe (mirroring each sibling's own durability
        /// judgment): the deciding SetConsent folds this into its own
        /// durability result — a memory-only sibling receipt would reload
        /// that scope Granted-but-receipt-less after a kill. A sibling whose
        /// apply THREW is judged unsafe (its receipt state is unknowable),
        /// while the propagation still never throws into the caller.
        /// </summary>
        public static bool PropagateConsentGrant(ShardPilotClient source)
        {
            List<ShardPilotClient> clients;
            lock (LifecycleGate)
            {
                clients = new List<ShardPilotClient>(LifecycleClients.Count);
                for (var i = 0; i < LifecycleClients.Count; i++)
                {
                    ShardPilotClient client;
                    if (LifecycleClients[i].TryGetTarget(out client) && !ReferenceEquals(client, source))
                    {
                        clients.Add(client);
                    }
                }
            }

            var allReceiptsDurablySafe = true;
            for (var i = 0; i < clients.Count; i++)
            {
                try
                {
                    // deferSupersededStampToDecider: the deciding flow's
                    // post-persist StampSupersededConsentGrants pass is
                    // imminent and consumes the exemption key this arm
                    // remembers — the catch-up/settle flavors (no pass
                    // behind them) consume it themselves instead.
                    allReceiptsDurablySafe &= clients[i].ApplyGlobalConsentGrant(
                        armOwnScopeReceipt: true, deferSupersededStampToDecider: true);
                }
                catch
                {
                    // Propagation must never throw into the granting caller;
                    // an unjudgeable sibling reports unsafe (fail-closed).
                    allReceiptsDurablySafe = false;
                }
            }

            return allReceiptsDurablySafe;
        }

        /// <summary>
        /// Stamps every OTHER live registered client's retained older
        /// pending grants SUPERSEDED — called by the deciding flow only
        /// AFTER the decision's consent state has durably persisted (the stamp must never outlive a failed state
        /// persist, so it cannot ride the decision-time propagation, which
        /// runs before the persist). Each sibling exempts the fresh receipt
        /// that same decision's propagation armed on it. Never throws into
        /// the caller.
        /// </summary>
        public static void StampSupersededConsentGrants(ShardPilotClient source)
        {
            List<ShardPilotClient> clients;
            lock (LifecycleGate)
            {
                clients = new List<ShardPilotClient>(LifecycleClients.Count);
                for (var i = 0; i < LifecycleClients.Count; i++)
                {
                    ShardPilotClient client;
                    if (LifecycleClients[i].TryGetTarget(out client) && !ReferenceEquals(client, source))
                    {
                        clients.Add(client);
                    }
                }
            }

            for (var i = 0; i < clients.Count; i++)
            {
                try
                {
                    clients[i].StampOwnScopeSupersededAfterGlobalPersist();
                }
                catch
                {
                    // Never throws into the deciding caller; an unstamped
                    // sibling merely re-downgrades once at its next launch
                    // (fail-closed residual).
                }
            }
        }

        /// <summary>
        /// Distributes a WITHHELD-grant COMPLETION (the settle-time persist
        /// of a grant whose observable state the receipt-first ordering
        /// withheld) to every OTHER live registered client that MISSED the
        /// decision: a sibling constructed inside the withheld window read
        /// the still-absent persisted record at its registration refresh
        /// and sat consent-gated against a decision the player made. Each
        /// such sibling converges to the grant and arms its OWN scope's
        /// receipt (original decided_at from the session marker) — while
        /// already-granted siblings are skipped entirely, so a scope the
        /// decision-time propagation reached never double-mints for one
        /// decision. Sibling receipt durability self-surfaces on each
        /// sibling's stats error surface (this late distribution has no
        /// SetConsent caller to report to). Never throws into the caller.
        /// </summary>
        public static void PropagateWithheldGrantCompletion(ShardPilotClient source, DateTime? decidedAtUtc,
            ShardPilotConsentNoticeSnapshot notice = null)
        {
            List<ShardPilotClient> clients;
            lock (LifecycleGate)
            {
                clients = new List<ShardPilotClient>(LifecycleClients.Count);
                for (var i = 0; i < LifecycleClients.Count; i++)
                {
                    ShardPilotClient client;
                    if (LifecycleClients[i].TryGetTarget(out client) && !ReferenceEquals(client, source))
                    {
                        clients.Add(client);
                    }
                }
            }

            for (var i = 0; i < clients.Count; i++)
            {
                try
                {
                    // The completing obligation's ORIGINAL decided_at rides
                    // along: a POST-RESTART completion has no session grant
                    // marker (process state died with the exit), and without
                    // the override each sibling would mint nothing and stay
                    // gated against the just-persisted grant.
                    clients[i].ApplyGlobalConsentGrant(
                        armOwnScopeReceipt: true,
                        skipAlreadyGranted: true,
                        settledDecidedAtUtc: decidedAtUtc, settledNotice: notice);
                }
                catch
                {
                    // Distribution must never throw into the settle chain.
                }
            }
        }

        /// <summary>
        /// Applies a Mode-B identity-grant DOWNGRADE to every OTHER live
        /// registered client: the persisted grant just fell back to the
        /// absent/Unknown state (its pending receipt was identity-dropped),
        /// and clients that loaded the grant earlier must converge NOW —
        /// the registration refresh only downgrades a stale loaded grant at
        /// REGISTRATION time, so without this propagation an
        /// already-registered live sibling would keep publishing under the
        /// unbacked grant until relaunch. Mirrors the denial propagation's
        /// snapshot-then-apply shape and its fail-closed queue handling,
        /// but mints NO receipts and records no decision — a downgrade to
        /// Unknown is not a player decision; and it never overrides a
        /// sibling whose grant is a live in-session explicit decision (the
        /// sibling's decision-generation guard stands it down). Never
        /// throws into the caller.
        /// </summary>
        public static void PropagateIdentityGrantDowngrade(ShardPilotClient source)
        {
            List<ShardPilotClient> clients;
            lock (LifecycleGate)
            {
                clients = new List<ShardPilotClient>(LifecycleClients.Count);
                for (var i = 0; i < LifecycleClients.Count; i++)
                {
                    ShardPilotClient client;
                    if (LifecycleClients[i].TryGetTarget(out client) && !ReferenceEquals(client, source))
                    {
                        clients.Add(client);
                    }
                }
            }

            for (var i = 0; i < clients.Count; i++)
            {
                try
                {
                    clients[i].ApplyIdentityGrantDowngrade();
                }
                catch
                {
                    // Propagation must never throw into the caller.
                }
            }
        }

        /// <summary>
        /// Applies a crash-reporting opt-in/opt-out decided on one client to
        /// every OTHER live registered client: the persisted record is a
        /// single global key while clients are per-scope, so live clients
        /// must converge on the new decision immediately — an opted-out
        /// player's crashes must stop from every client NOW, not at each
        /// client's next construction. Mirrors the consent-denial
        /// propagation — <paramref name="alreadySweptDirectory"/> carries
        /// the era directory the deciding client's opt-out sweep covered,
        /// so differently-rooted clients sweep their own (see
        /// <see cref="ShardPilotClient.ApplyGlobalCrashReportingEnabled"/>);
        /// never throws into the deciding caller.
        /// </summary>
        public static void PropagateCrashReportingEnabled(
            ShardPilotClient source, bool enabled, string alreadySweptDirectory = null)
        {
            List<ShardPilotClient> clients;
            lock (LifecycleGate)
            {
                clients = new List<ShardPilotClient>(LifecycleClients.Count);
                for (var i = 0; i < LifecycleClients.Count; i++)
                {
                    ShardPilotClient client;
                    if (LifecycleClients[i].TryGetTarget(out client) && !ReferenceEquals(client, source))
                    {
                        clients.Add(client);
                    }
                }
            }

            for (var i = 0; i < clients.Count; i++)
            {
                try
                {
                    clients[i].ApplyGlobalCrashReportingEnabled(enabled, alreadySweptDirectory);
                }
                catch
                {
                    // Propagation must never throw into the deciding caller.
                }
            }
        }

        internal static int LiveLifecycleClientCountForTesting
        {
            get
            {
                lock (LifecycleGate)
                {
                    var count = 0;
                    for (var i = 0; i < LifecycleClients.Count; i++)
                    {
                        ShardPilotClient client;
                        if (LifecycleClients[i].TryGetTarget(out client))
                        {
                            count++;
                        }
                    }

                    return count;
                }
            }
        }

        internal static void ClearLifecycleClientsForTesting()
        {
            lock (LifecycleGate)
            {
                LifecycleClients.Clear();
            }
        }

        /// <summary>
        /// Test entry point for the pause/quit persistence pass: forwards to
        /// the same method the engine's lifecycle messages invoke.
        /// </summary>
        internal static void PersistRegisteredClientsForTesting()
        {
            PersistRegisteredClients();
        }

        /// <summary>
        /// Crash-plane pause hook: the host
        /// behaviour's <c>OnApplicationPause</c> fans its transitions out
        /// here so the ANR watchdog can gate its stall monitor on
        /// backgrounding — lifecycle suspension must never read as a stall.
        /// Raised on the main thread with the engine's pause status.
        /// </summary>
        internal static event Action<bool> PauseStateChanged;

        /// <summary>
        /// Raises <see cref="PauseStateChanged"/>, isolating subscriber
        /// failures from the engine's lifecycle callback. Internal so the
        /// suite can drive the exact production fan-out without an engine.
        /// </summary>
        internal static void RaisePauseStateChanged(bool paused)
        {
            var handler = PauseStateChanged;
            if (handler == null)
            {
                return;
            }

            try
            {
                handler(paused);
            }
            catch
            {
                // A lifecycle fan-out must never throw into the engine loop.
            }
        }

        internal static void ClearPauseStateSubscribersForTesting()
        {
            PauseStateChanged = null;
        }

        private void OnApplicationPause(bool pauseStatus)
        {
            if (_retired) return;
            // Subscribers may enqueue or register a client. Finish the fan-out
            // before taking the final snapshot for the impending suspension.
            RaisePauseStateChanged(pauseStatus);

            // Only the transition INTO the background persists the pending
            // remnant. Resuming changes nothing: the in-memory queue was not
            // cleared, normal delivery continues, and the eventual server
            // acknowledgment removes the spooled copies.
            if (pauseStatus)
            {
                PersistRegisteredClients();
            }
        }

        private void OnApplicationQuit()
        {
            if (_retired) return;
            PersistRegisteredClients(applicationQuit: true);
        }

        private static void PersistRegisteredClients(bool applicationQuit = false)
        {
            List<ShardPilotClient> clients;
            lock (LifecycleGate)
            {
                clients = new List<ShardPilotClient>(LifecycleClients.Count);
                for (var i = 0; i < LifecycleClients.Count; i++)
                {
                    ShardPilotClient client;
                    if (LifecycleClients[i].TryGetTarget(out client))
                    {
                        clients.Add(client);
                    }
                }
            }

            for (var i = 0; i < clients.Count; i++)
            {
                try
                {
                    if (applicationQuit)
                    {
                        clients[i].OnApplicationQuitForSession();
                    }
                    else
                    {
                        clients[i].PersistPendingEvents();
                    }
                }
                catch
                {
                    // A lifecycle callback must never throw into the engine loop.
                }
            }
        }

        private void Update()
        {
            DrainQueuedWork();
        }

        /// <summary>
        /// Starts every routine queued by <see cref="Run"/>. This is the whole
        /// body of <c>Update</c>, extracted so a test can drive the REAL pump
        /// rather than a copy of it — a test-only reimplementation would be
        /// free to diverge from the frame loop it is standing in for.
        /// </summary>
        internal void DrainQueuedWork()
        {
            DrainHostCalls();
            while (true)
            {
                IEnumerator routine;
                lock (_gate)
                {
                    if (_retired || _work.Count == 0)
                    {
                        return;
                    }

                    routine = _work.Dequeue();
                }

                StartCoroutine(routine);
            }
        }
    }

    /// <summary>
    /// Host-logger notices a lifecycle handoff owes, held until the outermost
    /// hold on this thread is released (#159). The facade's re-initialize
    /// holds its process-wide gate across the successor's construction and
    /// registration, and that registration logs the replaced client's
    /// warning; a host logger that calls back into the SDK from another
    /// thread and waits for it would deadlock on the gate. A notice emitted
    /// outside any hold runs at once.
    /// </summary>
    internal static class ShardPilotHandoffNotices
    {
        [ThreadStatic] private static int _depth;
        [ThreadStatic] private static List<Action> _owed;

        /// <summary>Holds this thread's notices until the returned scope and
        /// every enclosing one are disposed.</summary>
        internal static Scope Hold()
        {
            _depth++;
            return new Scope(true);
        }

        internal static void Emit(Action notice)
        {
            if (notice == null)
            {
                return;
            }

            if (_depth > 0)
            {
                if (_owed == null)
                {
                    _owed = new List<Action>();
                }

                _owed.Add(notice);
                return;
            }

            notice();
        }

        private static void Release()
        {
            if (_depth > 0)
            {
                _depth--;
            }

            if (_depth != 0 || _owed == null)
            {
                return;
            }

            var owed = _owed;
            _owed = null;
            for (var i = 0; i < owed.Count; i++)
            {
                try
                {
                    owed[i]();
                }
                catch
                {
                    // A notice reaches host code; it must never break the
                    // handoff that owed it.
                }
            }
        }

        internal struct Scope : IDisposable
        {
            private readonly bool _held;

            internal Scope(bool held)
            {
                _held = held;
            }

            public void Dispose()
            {
                if (_held)
                {
                    Release();
                }
            }
        }
    }
}
