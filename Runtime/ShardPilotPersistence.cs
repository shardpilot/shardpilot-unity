using System;
using System.IO;
using UnityEngine;

namespace ShardPilot
{
    /// <summary>
    /// Minimal string key/value persistence used for the anonymous id, the
    /// local consent decision, and the crash-reporting opt-out.
    /// Implementations must be cheap and synchronous.
    /// </summary>
    internal interface IShardPilotKeyValueStore
    {
        /// <summary>Returns the stored value or an empty string when missing.</summary>
        string Get(string key);

        /// <summary>
        /// Persists the value DURABLY or throws. Implementations must not
        /// swallow a failed write while leaving the new value readable from
        /// an in-process cache: a later same-process read would then treat
        /// an unpersisted value as the stored decision (for the fail-closed
        /// crash opt-out that would reopen a client a durable write never
        /// reopened). Callers that only need best-effort persistence wrap
        /// the call in their own try/catch.
        /// </summary>
        void Set(string key, string value);
    }

    /// <summary>
    /// Optional backend identity for a key-value store, consumed by the
    /// consent outbox's ownership registry: stores addressing a SHARED
    /// backend return one stable token across instances (the
    /// PlayerPrefs-backed store shares one; the file store tokens on its
    /// canonical directory — mirroring how the spool distinguishes backends
    /// by canonical file path), so a same-scope client replacement is
    /// recognized across store instances, while a store WITHOUT an identity
    /// is treated as a private per-instance backend that never displaces —
    /// and is never displaced by — another store's outbox.
    /// </summary>
    internal interface IShardPilotStoreIdentity
    {
        string StoreIdentityToken { get; }
    }

    /// <summary>
    /// Pure file-backed store: one file per key inside a directory. Used as
    /// the fallback when PlayerPrefs throws, and directly in tests.
    /// </summary>
    internal sealed class ShardPilotFileKeyValueStore : IShardPilotKeyValueStore, IShardPilotStoreIdentity
    {
        private readonly string _directory;

        public ShardPilotFileKeyValueStore(string directory)
        {
            _directory = directory;
        }

        /// <summary>
        /// Canonical-directory identity (case-insensitive, like the spool's
        /// ownership comparisons): two file stores over one directory are
        /// one backend.
        /// </summary>
        public string StoreIdentityToken
        {
            get
            {
                if (string.IsNullOrEmpty(_directory))
                {
                    return null;
                }

                string canonical;
                try
                {
                    canonical = Path.GetFullPath(_directory);
                }
                catch
                {
                    canonical = _directory;
                }

                return "file|" + canonical.ToLowerInvariant();
            }
        }

        public string Get(string key)
        {
            var path = PathFor(key);
            if (string.IsNullOrEmpty(path) || !File.Exists(path))
            {
                return string.Empty;
            }

            return File.ReadAllText(path).Trim();
        }

        public void Set(string key, string value)
        {
            var path = PathFor(key);
            if (string.IsNullOrEmpty(path))
            {
                // No resolvable location means nothing can be durably
                // persisted: report it instead of silently succeeding — a
                // swallowed no-op here would let a fail-closed setting (the
                // crash opt-out) claim durability it does not have.
                throw new IOException("shardpilot persistence: no file path is resolvable for key '" + key + "'");
            }

            Directory.CreateDirectory(_directory);
            File.WriteAllText(path, value ?? string.Empty);
        }

        private string PathFor(string key)
        {
            if (string.IsNullOrEmpty(_directory) || string.IsNullOrEmpty(key))
            {
                return null;
            }

            return Path.Combine(_directory, key);
        }
    }

    /// <summary>
    /// PlayerPrefs-backed store with a file fallback under
    /// Application.persistentDataPath when PlayerPrefs throws. PlayerPrefs is
    /// main-thread-only, so construct and use this store on the Unity main
    /// thread; off-main-thread calls degrade to the file fallback.
    /// </summary>
    internal sealed class ShardPilotUnityKeyValueStore : IShardPilotKeyValueStore, IShardPilotStoreIdentity
    {
        private readonly ShardPilotFileKeyValueStore _fileFallback;
        private readonly string _identityToken;

        public ShardPilotUnityKeyValueStore()
        {
            string directory;
            try
            {
                directory = Application.persistentDataPath;
            }
            catch
            {
                directory = null;
            }

            _fileFallback = new ShardPilotFileKeyValueStore(directory);

            // Every instance addresses the ONE PlayerPrefs backend (plus
            // the shared persistent-data fallback directory): one identity
            // token across instances, so a same-scope client replacement is
            // recognized by the consent outbox's ownership registry.
            _identityToken = "unity-playerprefs|" + (directory ?? string.Empty).ToLowerInvariant();
        }

        public string StoreIdentityToken
        {
            get { return _identityToken; }
        }

        /// <summary>
        /// The identity of the backend where the FILE-ROUTED keys — the
        /// experiments subject id among them — physically live: the file
        /// fallback's canonical-directory token. Every instance over one
        /// persistent-data directory shares it, and it equals a raw
        /// <see cref="ShardPilotFileKeyValueStore"/>'s token over the same
        /// directory — the subject-id mint gate keys on exactly that: one
        /// gate per physical subject-id location, whichever store type
        /// addresses it. Distinct from <see cref="StoreIdentityToken"/>,
        /// which names the whole PlayerPrefs-plus-fallback backend for the
        /// consent outbox's ownership registry.
        /// </summary>
        internal string FileRoutedStoreIdentityToken
        {
            get { return _fileFallback.StoreIdentityToken; }
        }

        /// <summary>
        /// Consent-outbox records, the withheld-grant obligation markers,
        /// the consent denial-marker witness slot, the experiments
        /// condemned-facts purge markers, AND the experiments subject id
        /// are FILE-ROUTED: written to and read from
        /// the persistent-data file store exclusively, never PlayerPrefs —
        /// the same discipline the event spool applies to its own durable
        /// data (direct file IO). Routing one way closes the PlayerPrefs
        /// duality for these keys entirely: their owed-write retries run on
        /// the background worker, where the PlayerPrefs leg would throw and
        /// a "successful" fallback write could later be SHADOWED by a stale
        /// non-empty PlayerPrefs value on the next main-thread read — an
        /// outbox append/prune reported durable yet lost or resurrected, a
        /// withheld marker whose worker-side receipt-durability phase
        /// upgrade is shadowed by the main-thread arm-time copy (the
        /// relaunch reads phase=pending and treats a backend-accepted grant
        /// as lost), a condemned-facts marker whose worker/persist-
        /// barrier mutation lands in the fallback while the stale
        /// PlayerPrefs copy wins the next main-thread read — a relaunch
        /// then misses withdrawn fact ids or keeps stale ones — or a
        /// subject-grammar remint (AdoptMintedSubjectId persists the
        /// replacement spcid_ from the revalidation worker) whose fallback
        /// write loses the next main-thread Load to the stale rejected
        /// subject, so the next launch keeps a subject the server refused.
        /// The DENIAL-MARKER witness slot joins them: its heal writes run on the worker and inside the settle/
        /// close ladders — a fallback-parked marker (or retirement)
        /// shadowed by a stale non-empty PlayerPrefs copy would report a
        /// witness durable that the next launch never reads (or resurrect
        /// one a decision retired) — and its READS must keep absent and
        /// unreadable distinguishable (the unjudgeable-slot hold), which
        /// the availability-first PlayerPrefs fallback read cannot.
        /// File-routing also makes the writes thread-safe for the worker.
        /// All five key families are new in this SDK version (verified
        /// against the base: none of the prefixes — nor the subject-id or
        /// denial-marker key — exists before this change), so no legacy
        /// PlayerPrefs copy exists to migrate.
        /// </summary>
        private static bool IsFileRoutedKey(string key)
        {
            return key != null &&
                   (key.StartsWith(ShardPilotConsentOutbox.StorageKeyPrefix, StringComparison.Ordinal) ||
                    key.StartsWith(ShardPilotConsent.WithheldGrantStorageKeyPrefix, StringComparison.Ordinal) ||
                    key.StartsWith(ShardPilotClient.CondemnedFactsStorageKey, StringComparison.Ordinal) ||
                    key == ShardPilotConsent.DenialMarkerStorageKey ||
                    key.StartsWith(ShardPilotConsent.OwedDenialWitnessStorageKeyPrefix, StringComparison.Ordinal) ||
                    key == ShardPilotSubjectId.StorageKey);
        }

        public string Get(string key)
        {
            if (IsFileRoutedKey(key))
            {
                return _fileFallback.Get(key);
            }

            // PlayerPrefs is the AUTHORITATIVE source: its non-empty value
            // wins every read, and Set writes the fallback file only when it
            // can win a future read. When PlayerPrefs cannot be read at all,
            // the winning value is unknowable — serving the fallback could
            // resurrect a value a healthy read would have overridden — so
            // DECISION keys (consent, the crash opt-out, the spool markers)
            // fail CLOSED: the caller sees "unreadable", never a guessed
            // value or a silent "absent". The one exception is the ANONYMOUS
            // ID, where identity continuity beats shadow-correctness: a
            // possibly-older persisted id is still the same stable identity,
            // while failing would mint a fresh id and fragment it — so its
            // read keeps the availability-first fallback behavior.
            string primary;
            try
            {
                primary = PlayerPrefs.GetString(key, string.Empty);
            }
            catch
            {
                if (key == ShardPilotConsent.StorageKey || key == ShardPilotCrashSettings.StorageKey)
                {
                    throw new IOException("shardpilot persistence: settings read failed for key '" + key + "'");
                }

                // Availability-first keys — the identity key AND the
                // internal fail-closed markers — READ from the fallback when
                // the primary is unreadable, mirroring their writes: Set
                // puts them in the fallback file exactly in this state, so a
                // primary-unreadable launch must still honor an owed-wipe/
                // sweep marker a previous launch parked there (a marker that
                // cannot be read back fails OPEN — the quarantine it records
                // would be ignored).
                try
                {
                    return _fileFallback.Get(key);
                }
                catch
                {
                    return string.Empty;
                }
            }

            if (!string.IsNullOrEmpty(primary))
            {
                return primary;
            }

            // PlayerPrefs read clean-and-empty: the fallback file decides. A
            // fallback READ failure surfaces to the caller too (absent and
            // unreadable must stay distinguishable); an absent file or an
            // unresolvable directory reads as absent — nothing could ever
            // have been stored there by this store.
            return _fileFallback.Get(key);
        }

        public void Set(string key, string value)
        {
            if (IsFileRoutedKey(key))
            {
                // Durable-or-throw straight to the file store; PlayerPrefs
                // is never involved for this key (see IsFileRoutedKey).
                _fileFallback.Set(key, value);
                return;
            }

            // Snapshot the current in-process value FIRST: it is both the
            // rollback target (PlayerPrefs.SetString seeds the in-process
            // prefs cache even when the Save() that would make it durable
            // then fails) and the shadow probe for the fallback path below
            // (after a failed Save, PlayerPrefs' own DURABLE copy still holds
            // this pre-call value, and Get prefers a non-empty PlayerPrefs
            // value over the fallback file).
            string previous;
            var previousKnown = false;
            try
            {
                previous = PlayerPrefs.GetString(key, string.Empty);
                previousKnown = true;
            }
            catch
            {
                previous = string.Empty;
            }

            var cacheSeeded = false;
            try
            {
                PlayerPrefs.SetString(key, value);
                cacheSeeded = true;
                PlayerPrefs.Save();

                // A durable EMPTY write (a clear) can still be shadowed on a
                // later read: Get treats an empty primary as absent and
                // consults the fallback file, so a marker parked THERE during
                // a primary-unhealthy period would resurrect the state this
                // clear just removed — an owed-wipe or Retry-After marker
                // could re-quarantine forever. Clear the fallback copy too,
                // best-effort: if it cannot be cleared, the surviving copy is
                // a fail-closed marker that keeps its quarantine until the
                // next successful clear.
                if (string.IsNullOrEmpty(value))
                {
                    try
                    {
                        _fileFallback.Set(key, value);
                    }
                    catch
                    {
                        // Best-effort — see above.
                    }
                }

                return;
            }
            catch
            {
                // PlayerPrefs unavailable or not durable; the file fallback
                // below is the remaining durable path.
            }

            if (key == ShardPilotAnonymousId.StorageKey)
            {
                // Availability-first for the identity key: in a
                // PlayerPrefs-less environment the fallback file is where
                // the id lives, so it is written even when the primary read
                // failed — otherwise a fresh id would be minted and lost on
                // every construction. Shadowing is harmless here: either
                // copy is the same stable identity.
                _fileFallback.Set(key, value);
                return;
            }

            // Every other key — the two DECISION keys and the internal
            // fail-closed markers — only counts a fallback write as DURABLE
            // when a future read would actually serve it:
            // - previous KNOWN empty/equal: the primary yields to the
            //   fallback — the write wins.
            // - previous KNOWN different-non-empty: the still-readable
            //   primary shadows the fallback — reporting success would hide
            //   the new value (a fail-closed marker that Get never returns
            //   is as bad as one never written), so the write FAILS.
            // - previous UNKNOWN (primary unreadable): marker READS fail
            //   over to the fallback in exactly that state (see Get), so the
            //   fallback wins for markers; the decision keys' reads fail
            //   closed instead of consulting the fallback, and a failed
            //   decision write must never be readable EVER — so they still
            //   require a known-safe previous.
            // A value whose write is about to be reported failed is not
            // written anywhere readable.
            var decisionKey = key == ShardPilotConsent.StorageKey || key == ShardPilotCrashSettings.StorageKey;
            var fallbackCanWin = previousKnown
                ? string.IsNullOrEmpty(previous) ||
                  string.Equals(previous, value ?? string.Empty, StringComparison.Ordinal)
                : !decisionKey;

            var fallbackWrote = false;
            if (fallbackCanWin)
            {
                try
                {
                    _fileFallback.Set(key, value);
                    fallbackWrote = true;
                }
                catch
                {
                    // Fall through to the failure path below.
                }
            }

            if (fallbackWrote)
            {
                return;
            }

            // NOT durable: nothing readable holds the new value (or a stale
            // PlayerPrefs copy would shadow it on the next launch). Roll the
            // in-process prefs cache back to the pre-call value (best effort)
            // so a failed write cannot be read back as though it were
            // persisted, then report the failure to the caller.
            if (cacheSeeded && previousKnown)
            {
                try
                {
                    PlayerPrefs.SetString(key, previous);
                }
                catch
                {
                    // The rollback is best-effort.
                }
            }

            throw new IOException("shardpilot persistence: durable write failed for key '" + key + "'");
        }
    }

    /// <summary>
    /// The persisted experiment subject id (`spcid_...`):
    /// a dedicated installation id, SEPARATE from the anonymous id — never
    /// derived from it, never replacing it, and never host-supplied (no
    /// config field is read for it; the consumer mints it lazily at first
    /// need). Loading never mints: an experiments-off client must not create
    /// the key, and a launch without the experiments opt-in must never lose
    /// a previously minted id. A stored value that fails the wire grammar is
    /// reported as absent — the consumer re-mints with fresh-install
    /// (re-bucketing) semantics. In the Unity store the key is FILE-ROUTED
    /// (see <c>ShardPilotUnityKeyValueStore.IsFileRoutedKey</c>): a remint
    /// persists from the revalidation worker, where the PlayerPrefs leg
    /// throws — a fallback write shadowed by a stale non-empty PlayerPrefs
    /// copy would hand the next launch back the rejected subject.
    /// </summary>
    internal static class ShardPilotSubjectId
    {
        public const string StorageKey = "shardpilot.spcid";

        /// <summary>The stored subject id, or null when absent, unreadable,
        /// or not wire-valid. Never mints, never writes.</summary>
        public static string Load(IShardPilotKeyValueStore store)
        {
            if (store == null)
            {
                return null;
            }

            string existing;
            try
            {
                existing = ShardPilotConfig.Trim(store.Get(StorageKey));
            }
            catch
            {
                return null;
            }

            return ShardPilotExperimentsLogic.ValidSubjectId(existing) ? existing : null;
        }

        /// <summary>Persists a minted subject id (best-effort). Returns
        /// false when the store refused the write — the caller keeps the id
        /// in memory so one session stays self-consistent.</summary>
        public static bool Store(IShardPilotKeyValueStore store, string value)
        {
            if (store == null || !ShardPilotExperimentsLogic.ValidSubjectId(value))
            {
                return false;
            }

            try
            {
                store.Set(StorageKey, value);
                return true;
            }
            catch
            {
                return false;
            }
        }
    }

    internal static class ShardPilotAnonymousId
    {
        public const string StorageKey = "shardpilot.anonymous_id";

        public static string LoadOrCreate(IShardPilotKeyValueStore store)
        {
            bool ignored;
            return LoadOrCreate(store, ShardPilotIds.NewUuidV7, out ignored);
        }

        /// <summary>
        /// Load-or-create with the rejection SIGNAL:
        /// <paramref name="replacedRejectedLegacyId"/> is true only when a
        /// stored id EXISTED and failed identity validation — the caller
        /// must then treat durable data written under that identity (the
        /// restored event spool) as carrying a contract-illegal identifier
        /// and purge it fail-safe instead of replaying it verbatim. A merely
        /// absent record (fresh install) reports false.
        /// </summary>
        public static string LoadOrCreate(IShardPilotKeyValueStore store, out bool replacedRejectedLegacyId)
        {
            return LoadOrCreate(store, ShardPilotIds.NewUuidV7, out replacedRejectedLegacyId);
        }

        public static string LoadOrCreate(IShardPilotKeyValueStore store, Func<string> newId)
        {
            bool ignored;
            return LoadOrCreate(store, newId, out ignored);
        }

        public static string LoadOrCreate(
            IShardPilotKeyValueStore store, Func<string> newId, out bool replacedRejectedLegacyId)
        {
            replacedRejectedLegacyId = false;
            if (store == null)
            {
                return newId();
            }

            string existing;
            try
            {
                existing = store.Get(StorageKey);
            }
            catch
            {
                existing = string.Empty;
            }

            // Local storage may be edited or corrupted. Validate its literal
            // value before reuse: trimming an unsafe slot could collapse it
            // into another identity. Rejected values use the existing fresh-ID
            // replacement and spool-refusal signal, never truncation or repair.
            if (!string.IsNullOrEmpty(existing))
            {
                if (ShardPilotConsentOutbox.IsValidIdentifier(existing) &&
                    ShardPilotConfig.IsSafeAnonymousIdHeader(existing))
                {
                    return existing;
                }

                replacedRejectedLegacyId = true;
            }

            var created = newId();
            try
            {
                store.Set(StorageKey, created);
            }
            catch
            {
                // Persistence is best-effort; the id remains valid in-memory.
            }

            return created;
        }
    }
}
