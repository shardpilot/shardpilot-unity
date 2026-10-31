using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace ShardPilot
{
    internal static class ShardPilotPathIdentity
    {
        internal static readonly StringComparer Comparer =
            Path.DirectorySeparatorChar == '\\' ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
    }

    // Acquisition and explicit recovery open files; retaining a lease never does.
    internal sealed class ShardPilotWithdrawalJournalOwner
    {
        private static readonly object RegistryGate = new object();
        private static readonly object IdentityGate = new object();
        private static readonly Dictionary<string, ShardPilotWithdrawalJournalOwner> Owners =
            new Dictionary<string, ShardPilotWithdrawalJournalOwner>(
                ShardPilotPathIdentity.Comparer);
        private readonly object _gate = new object();
        private readonly SemaphoreSlim _writer = new SemaphoreSlim(1, 1);
        private readonly ShardPilotWithdrawalJournal.AdmissionEpoch _admissions = new ShardPilotWithdrawalJournal.AdmissionEpoch();
        private readonly string _path, _identity;
        private ShardPilotWithdrawalJournalStore _store;
        private Func<string, FileStream> _openForTesting;
        private int _references;
        private int _identityReferences;

        private ShardPilotWithdrawalJournalOwner(string path, string identity)
        { _path = path; _identity = identity; }

        internal static Lease AcquireMemory(string identity)
        {
            if (string.IsNullOrEmpty(identity)) return null;
            var owner = new ShardPilotWithdrawalJournalOwner(null, identity) { _references = 1 };
            owner._store = new ShardPilotWithdrawalJournalStore(identity, owner._admissions);
            return new Lease(owner);
        }

        internal static Lease AcquireExisting(string path, string identity, out bool absent)
        { return AcquireCore(path, identity, null, true, out absent); }

        internal static Lease Acquire(string path, string identity, Func<string, FileStream> openForTesting = null)
        { return AcquireCore(path, identity, openForTesting, false, out _); }

        internal static IdentityReference ClaimIdentity(string path, string identity)
        {
            var full = CanonicalPath(path);
            if (full == null || string.IsNullOrEmpty(identity) || !Directory.Exists(Path.GetDirectoryName(full))) return null;
            while (true)
            {
                var owner = LocateOwner(full, identity);
                if (owner == null) return null;
                lock (owner._gate)
                {
                    lock (RegistryGate)
                    {
                        ShardPilotWithdrawalJournalOwner current;
                        if (!Owners.TryGetValue(owner._path, out current) || !ReferenceEquals(current, owner)) continue;
                    }
                    if (owner._identity != identity) return null;
                    owner._identityReferences++;
                    return new IdentityReference(owner);
                }
            }
        }

        private static string CanonicalPath(string path)
        {
            try { return Path.GetFullPath(path); }
            catch (IOException) { return null; }
            catch (UnauthorizedAccessException) { return null; }
            catch (ArgumentException) { return null; }
            catch (NotSupportedException) { return null; }
        }

        // A null lease means known absence only when absent is true.
        private static Lease AcquireCore(string path, string identity, Func<string, FileStream> openForTesting,
            bool existingOnly, out bool absent)
        {
            absent = false;
            if (string.IsNullOrEmpty(identity)) return null;
            var full = CanonicalPath(path);
            if (full == null) return null;
            while (true)
            {
                var owner = LocateOwner(full, identity);
                if (owner == null) return null;
                lock (owner._gate)
                {
                    // An acquire may have waited while the last lease removed this entry.
                    lock (RegistryGate)
                    {
                        ShardPilotWithdrawalJournalOwner current;
                        if (!Owners.TryGetValue(owner._path, out current) || !ReferenceEquals(current, owner)) continue;
                    }
                    if (owner._identity != identity) return null;
                    if (owner._store == null)
                    {
                        owner._openForTesting = openForTesting;
                        owner._store = new ShardPilotWithdrawalJournalStore(owner._path, identity, openForTesting, existingOnly, owner._admissions);
                        if (!owner._store.Ready)
                        {
                            absent = owner._store.LastFailure == ShardPilotWithdrawalJournalStore.Failure.Absent;
                            try { owner.CloseStore(); }
                            finally { owner.RetireIfUnused(); }
                            return null;
                        }
                    }
                    owner._references++;
                    return new Lease(owner);
                }
            }
        }

        private static ShardPilotWithdrawalJournalOwner LocateOwner(string full, string identity)
        {
            lock (IdentityGate) return LocateOwnerSerially(full, identity);
        }

        private static ShardPilotWithdrawalJournalOwner LocateOwnerSerially(string full, string identity)
        {
            ShardPilotWithdrawalJournalOwner owner;
            ShardPilotWithdrawalJournalOwner[] candidates;
            lock (RegistryGate)
            {
                if (Owners.TryGetValue(full, out owner)) return owner;
                candidates = new List<ShardPilotWithdrawalJournalOwner>(Owners.Values).ToArray();
            }
            foreach (var candidate in candidates)
            {
                if (!StringComparer.OrdinalIgnoreCase.Equals(candidate._path, full)) continue;
                bool known;
                var same = SameCaseAlias(candidate._path, full, out known);
                if (!known) return null;
                if (!same) continue;
                lock (RegistryGate)
                {
                    if (Owners.TryGetValue(full, out owner)) return owner;
                    if (Owners.TryGetValue(candidate._path, out owner) && ReferenceEquals(owner, candidate))
                        return owner;
                }
            }
            lock (RegistryGate)
            {
                if (!Owners.TryGetValue(full, out owner))
                {
                    owner = new ShardPilotWithdrawalJournalOwner(full, identity);
                    Owners.Add(full, owner);
                }
                return owner;
            }
        }

        internal static bool SameCaseAlias(string held, string requested, out bool known)
        {
            var suffix = ".alias-check." + Guid.NewGuid().ToString("N");
            var probe = held + suffix;
            var created = false;
            var same = false;
            known = false;
            try
            {
                using (var file = new FileStream(probe, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                {
                    created = true;
                    try { File.GetAttributes(requested + suffix); same = true; known = true; }
                    catch (FileNotFoundException) { known = true; }
                    catch (DirectoryNotFoundException) { known = true; }
                }
            }
            catch { same = false; known = false; }
            finally
            {
                if (created)
                {
                    try { File.Delete(probe); }
                    catch { same = false; known = false; }
                }
            }
            return same;
        }

        private void AddReference() { lock (_gate) _references++; }
        private void Release()
        {
            lock (_gate)
            {
                if (--_references != 0) return;
                try { CloseStore(); }
                finally { RetireIfUnused(); }
            }
        }
        private void CloseStore()
        {
            var store = _store; _store = null;
            store?.Dispose();
        }
        private void RetireIfUnused()
        {
            if (_references != 0 || _identityReferences != 0) return;
            _writer.Dispose();
            lock (RegistryGate) if (_path != null) Owners.Remove(_path);
        }

        // Identity references retain the confirmed name, never an open file.
        internal sealed class IdentityReference : IDisposable
        {
            private ShardPilotWithdrawalJournalOwner _owner;
            private readonly string _key;
            internal IdentityReference(ShardPilotWithdrawalJournalOwner owner)
            { _owner = owner; _key = owner._path; }
            internal string Key { get { return _key; } }
            public void Dispose()
            {
                var owner = Interlocked.Exchange(ref _owner, null);
                if (owner == null) return;
                lock (owner._gate) { owner._identityReferences--; owner.RetireIfUnused(); }
            }
        }

        internal sealed class Lease : IDisposable
        {
            private readonly object _gate = new object();
            private ShardPilotWithdrawalJournalOwner _owner;
            internal Lease(ShardPilotWithdrawalJournalOwner owner) { _owner = owner; }
            private ShardPilotWithdrawalJournalOwner Borrow()
            {
                lock (_gate)
                {
                    if (_owner == null) return null;
                    _owner.AddReference(); return _owner;
                }
            }
            private bool Use(Func<ShardPilotWithdrawalJournalStore, bool> action)
            {
                var owner = Borrow();
                if (owner == null) return false;
                try
                {
                    ShardPilotWithdrawalJournalStore store;
                    lock (owner._gate) store = owner._store;
                    return store != null && action(store);
                }
                finally { owner.Release(); }
            }
            internal bool Ready { get { return Use(store => store.Ready); } }
            internal bool TryRecover()
            {
                var owner = Borrow();
                if (owner == null) return false;
                var entered = false;
                try
                {
                    lock (owner._gate)
                    {
                        if (owner._store != null && owner._store.Ready) return true;
                        if (owner._path == null) return false;
                    }
                    entered = owner._writer.Wait(0);
                    if (!entered) return false;
                    lock (owner._gate)
                    {
                        if (owner._store != null && owner._store.Ready) return true;
                        owner.CloseStore();
                        owner._store = new ShardPilotWithdrawalJournalStore(owner._path, owner._identity,
                            owner._openForTesting, existingOnly: true, admissions: owner._admissions);
                        return owner._store.Ready;
                    }
                }
                finally
                {
                    if (entered) owner._writer.Release();
                    owner.Release();
                }
            }
            internal byte[] CopyBytesForTesting() { byte[] bytes = null; Use(store => { bytes = store.CopyBytesForTesting(); return true; }); return bytes; }
            internal Lease Retain()
            {
                var owner = Borrow();
                return owner == null ? null : new Lease(owner);
            }
            internal bool TryAppend(ShardPilotWithdrawalJournal.Frame expected, ShardPilotWithdrawalJournal.Frame next)
            { return TryWrite(store => store.TryAppend(expected, next)); }
            internal bool TryWrite(Func<ShardPilotWithdrawalJournalStore, bool> action)
            {
                var owner = Borrow();
                if (owner == null) return false;
                var entered = false;
                try
                {
                    entered = owner._writer.Wait(0);
                    return entered && owner._store != null && action(owner._store);
                }
                finally
                {
                    if (entered) owner._writer.Release();
                    owner.Release();
                }
            }
            internal bool CanProject(ShardPilotWithdrawalJournal.Frame frame)
            { return Use(store => store.CanProject(frame)); }
            internal bool CanRetire(ShardPilotWithdrawalJournal.Frame frame)
            { return Use(store => store.CanRetire(frame)); }
            internal bool TryGet(string scope, string id, out ShardPilotWithdrawalJournal.Frame frame)
            {
                ShardPilotWithdrawalJournal.Frame observed = null;
                var ok = Use(store => store.TryGet(scope, id, out observed));
                frame = ok ? observed : null; return ok;
            }
            internal bool TrySnapshot(out ShardPilotWithdrawalJournal.Frame[] frames)
            {
                ShardPilotWithdrawalJournal.Frame[] snapshot = null;
                var ok = Use(store => store.TrySnapshot(out snapshot));
                frames = ok ? snapshot : null;
                return ok;
            }
            internal bool TryListUnfinished(out ShardPilotWithdrawalJournal.Frame[] frames)
            {
                if (!TrySnapshot(out frames)) return false;
                frames = Array.FindAll(frames, frame => !frame.Complete); return true;
            }
            public void Dispose()
            {
                ShardPilotWithdrawalJournalOwner owner;
                lock (_gate) { owner = _owner; _owner = null; }
                if (owner != null) owner.Release();
            }
        }
    }
}
