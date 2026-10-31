using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace ShardPilot
{
    // Metadata receipts and checked replay are separate from filesystem sweep completion.
    internal sealed class ShardPilotDirectorySweepLedger
    {
        internal sealed class Intent
        {
            internal readonly string Id, Directory;
            internal readonly Tuple<string, string> Scope;
            internal Intent(string id, string directory, Tuple<string, string> scope) { Id = id; Directory = directory; Scope = scope; }
        }
        private static readonly ConcurrentDictionary<Tuple<string, string>, SemaphoreSlim> Writers = new ConcurrentDictionary<Tuple<string, string>, SemaphoreSlim>();
        private static readonly UTF8Encoding Utf8 = new UTF8Encoding(false, true);
        private readonly IShardPilotKeyValueStore _store;
        private readonly string _key;
        private readonly Tuple<string, string> _scope;
        private readonly SemaphoreSlim _writer;
        internal ShardPilotDirectorySweepLedger(IShardPilotKeyValueStore store, string key)
        {
            _store = store ?? throw new ArgumentNullException(nameof(store));
            if (string.IsNullOrEmpty(key) || key == "." || key == ".." || key.IndexOfAny(new[] { '/', '\\' }) >= 0) throw new ArgumentException(nameof(key));
            if (Utf8.GetByteCount(key) > 128) throw new ArgumentException("Metadata locator is too long.");
            _key = key;
            var identity = store is IShardPilotStoreIdentity shared ? shared.StoreIdentityToken : ShardPilotConsentOutbox.StoreIdentityToken(store);
            if (string.IsNullOrEmpty(identity)) throw new ArgumentException("Unknown metadata backend identity.");
            var authority = store is ShardPilotFileKeyValueStore file ? "file|" + Normalize(file.DirectoryForLedger) : identity;
            _scope = Tuple.Create(authority, key);
            if (Utf8.GetByteCount(Encode(new Intent[0])) > 65536) throw new ArgumentException("Metadata backend header is too large.");
            _writer = Writers.GetOrAdd(Tuple.Create(identity, key), _ => new SemaphoreSlim(1, 1));
        }
        internal static string Normalize(string directory)
        {
            var full = Path.GetFullPath(directory); var root = Path.GetPathRoot(full);
            return full.Length == root.Length ? full : full.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        }
        private string Encode(IEnumerable<Intent> entries, bool held = false)
        {
            var b = new StringBuilder("{\"v\":1,\"scope\":"); ShardPilotJson.AppendQuoted(b, _key);
            b.Append(",\"backend\":"); ShardPilotJson.AppendQuoted(b, _scope.Item1);
            b.Append(",\"held\":").Append(held ? "true" : "false").Append(",\"entries\":["); var first = true;
            foreach (var e in entries)
            {
                if (!first) b.Append(','); first = false; b.Append('['); ShardPilotJson.AppendQuoted(b, e.Id);
                b.Append(','); ShardPilotJson.AppendQuoted(b, e.Directory); b.Append(']');
            }
            return b.Append("]}").ToString();
        }
        private List<Intent> Load()
        {
            var raw = _store.Get(_key); if (raw == "") return new List<Intent>();
            if (raw == null || raw.Length > 65536 || Utf8.GetByteCount(raw) > 65536 ||
                !ShardPilotJsonParser.TryParseUniqueObject(raw, out var map, out _) ||
                !map.TryGetValue("entries", out var value) || !(value is List<object> rows) || rows.Count > 32) return null;
            var entries = new List<Intent>(); var ids = new HashSet<string>(StringComparer.Ordinal);
            foreach (var row in rows)
            {
                if (!(row is List<object> pair) || pair.Count != 2 || !(pair[0] is string id) || !(pair[1] is string directory) ||
                    !Guid.TryParseExact(id, "N", out _) || !ids.Add(id) || Normalize(directory) != directory) return null;
                entries.Add(new Intent(id, directory, _scope));
            }
            return Encode(entries) == raw ? entries : null;
        }
        private T WithWriter<T>(Func<List<Intent>, T> action)
        {
            if (!_writer.Wait(0)) return default;
            try { var entries = Load(); return entries == null ? default : action(entries); }
            catch { return default; }
            finally { _writer.Release(); }
        }
        internal Intent[] Read() => WithWriter(entries => entries.ToArray());
        internal Intent Record(string directory) => WithWriter(entries =>
        {
            var intent = new Intent(Guid.NewGuid().ToString("N"), Normalize(directory), _scope);
            var next = entries.Concat(new[] { intent }).ToArray(); var image = Encode(next);
            if (entries.Count >= 32 || Utf8.GetByteCount(image) > 65536)
            { _store.Set(_key, Encode(entries, true)); return null; }
            _store.Set(_key, image); return intent;
        });
        internal bool Retire(Intent[] captured) => WithWriter(entries =>
        {
            captured = captured?.ToArray();
            if (captured == null || captured.Any(e => e == null || !e.Scope.Equals(_scope))) return false;
            if (entries.RemoveAll(e => captured.Any(c => c.Id == e.Id && c.Directory == e.Directory)) == 0) return true;
            _store.Set(_key, Encode(entries)); return true;
        });
    }

    internal static class ShardPilotDirectorySweep
    {
        internal const string EventKey = "shardpilot.spool.sweep_ledger";
        internal const string CrashKey = "shardpilot.crashspool.sweep_ledger";
        internal const string EraKey = "shardpilot.aei_era.sweep_ledger";
        internal static ShardPilotDirectorySweepLedger Open(IShardPilotKeyValueStore store, string key)
        {
            var backend = store is ShardPilotUnityKeyValueStore unity ? unity.DirectorySweepStore : store;
            if (backend is ShardPilotFileKeyValueStore file) backend = new ShardPilotFileKeyValueStore(ShardPilotDirectorySweepLedger.Normalize(file.DirectoryForLedger));
            return new ShardPilotDirectorySweepLedger(backend, key);
        }
        private static bool Apply(Func<string, bool> sweep, string directory)
        { try { return sweep(directory); } catch { return false; } }
        private static ShardPilotDirectorySweepLedger.Intent[] Capture(ShardPilotDirectorySweepLedger ledger, string directory)
        {
            var before = ledger.Read(); if (before == null) return null;
            var created = ledger.Record(directory); if (created == null) return null;
            return before.Where(e => e.Directory == created.Directory).Concat(new[] { created }).ToArray();
        }
        private static bool Relevant(string owed, string directory)
        {
            if (owed == directory) return true;
            if (!StringComparer.OrdinalIgnoreCase.Equals(owed, directory)) return false;
            var same = ShardPilotWithdrawalJournalOwner.SameCaseAlias(
                Path.Combine(owed, ".sweep"), Path.Combine(directory, ".sweep"), out var known);
            return !known || same;
        }
        internal static bool Owed(IShardPilotKeyValueStore store, string key, string directory)
        {
            if (store == null) return false;
            try { var entries = Open(store, key).Read(); return entries == null || entries.Any(e => Relevant(e.Directory, ShardPilotDirectorySweepLedger.Normalize(directory))); }
            catch { return true; }
        }
        internal static bool Sweep(IShardPilotKeyValueStore store, string key, string directory, Func<string, bool> sweep)
        {
            if (store == null) return Apply(sweep, directory);
            ShardPilotDirectorySweepLedger ledger;
            try { ledger = Open(store, key); }
            catch { Apply(sweep, directory); return false; }
            var captured = Capture(ledger, directory);
            var complete = Apply(sweep, directory);
            return complete && captured != null && ledger.Retire(captured);
        }
        internal static bool Retry(IShardPilotKeyValueStore store, string key, string directory,
            Func<string, bool> sweep, bool ensureDirectory = false)
        {
            if (store == null) return !ensureDirectory || Apply(sweep, directory);
            try
            {
                var ledger = Open(store, key); var entries = ledger.Read();
                if (entries == null) return false;
                var normalized = ShardPilotDirectorySweepLedger.Normalize(directory);
                if (ensureDirectory && !entries.Any(e => e.Directory == normalized))
                {
                    var created = ledger.Record(normalized);
                    if (created == null) { Apply(sweep, directory); return false; }
                    entries = entries.Concat(new[] { created }).ToArray();
                }
                var complete = true;
                foreach (var group in entries.GroupBy(e => e.Directory, StringComparer.Ordinal))
                {
                    var relevant = Relevant(group.Key, normalized);
                    if (!Apply(sweep, group.Key)) { if (relevant) complete = false; }
                    else if (!ledger.Retire(group.ToArray()))
                    {
                        var remaining = ledger.Read();
                        if (remaining == null || remaining.Any(e => Relevant(e.Directory, normalized))) complete = false;
                    }
                }
                return complete;
            }
            catch { return false; }
        }
    }
}
