using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ShardPilot
{
    // Pure state and framing. Persistence acknowledges a frame only after its write completes.
    internal sealed class ShardPilotWithdrawalJournal
    {
        internal const int MaxFrameBytes = 65536;
        internal const int MaxJournalBytes = 1048576;
        private static readonly UTF8Encoding Utf8 = new UTF8Encoding(false, true);

        internal sealed class Frame
        {
            internal readonly string Store, Scope, Id, Experiment;
            internal readonly long Revision, BoundaryTicks;
            internal readonly bool Fixed, Assignment, Pipeline, WholePopulation;
            internal readonly int Scans;
            internal readonly ReadOnlyCollection<string> Keys, Spared, Pending;
            internal bool Complete { get { return Fixed && Assignment && Pipeline && Scans == 0 && Pending.Count == 0; } }

            internal Frame(string store, string scope, string id, string experiment, long revision,
                long boundaryTicks, bool fixedDecision, bool assignment, bool pipeline, bool wholePopulation,
                int scans, IEnumerable<string> keys, IEnumerable<string> spared, IEnumerable<string> pending)
            {
                if (string.IsNullOrEmpty(store) || string.IsNullOrEmpty(scope) || string.IsNullOrEmpty(id) ||
                    string.IsNullOrEmpty(experiment) || revision < 1 || boundaryTicks < 0 ||
                    boundaryTicks >= DateTime.MaxValue.Ticks || scans < 0 || (assignment && !fixedDecision))
                    throw new ArgumentException("Invalid withdrawal frame");
                Store = store; Scope = scope; Id = id; Experiment = experiment;
                Revision = revision; BoundaryTicks = boundaryTicks; Fixed = fixedDecision;
                Assignment = assignment; Pipeline = pipeline; WholePopulation = wholePopulation; Scans = scans;
                Keys = Normalize(keys); Spared = Normalize(spared); Pending = Normalize(pending);
                if (pipeline && (scans != 0 || Pending.Count != 0)) throw new ArgumentException("Unconfirmed work remains");
            }

            private static ReadOnlyCollection<string> Normalize(IEnumerable<string> values)
            {
                if (values == null) throw new ArgumentNullException(nameof(values));
                var set = new SortedSet<string>(StringComparer.Ordinal);
                foreach (var value in values)
                {
                    if (string.IsNullOrEmpty(value)) throw new ArgumentException("Empty withdrawal identity");
                    set.Add(value);
                }
                return new List<string>(set).AsReadOnly();
            }
        }

        internal sealed class Reclamation
        {
            private readonly object _version;
            internal readonly int RetainedBytes;
            internal readonly ReadOnlyCollection<Frame> Retained;
            internal Reclamation(object version, int retainedBytes, List<Frame> retained)
            { _version = version; RetainedBytes = retainedBytes; Retained = retained.AsReadOnly(); }
            internal bool Matches(object version) { return ReferenceEquals(_version, version); }
        }

        internal sealed class AdmissionEpoch
        {
            private readonly object _gate = new object();
            private readonly ConditionalWeakTable<Frame, object> _captures = new ConditionalWeakTable<Frame, object>();
            private object _current = new object();
            internal bool Allows(Frame frame, bool capture)
            {
                lock (_gate)
                {
                    object epoch;
                    if (_captures.TryGetValue(frame, out epoch)) return ReferenceEquals(epoch, _current);
                    if (capture) _captures.Add(frame, _current);
                    return true;
                }
            }
            internal void Advance() { lock (_gate) _current = new object(); }
            internal void Forget(Frame frame) { lock (_gate) _captures.Remove(frame); }
        }

        private readonly object _gate = new object();
        private readonly string _store;
        private readonly AdmissionEpoch _admissions;
        private readonly Dictionary<string, Frame> _durable = new Dictionary<string, Frame>(StringComparer.Ordinal);
        private readonly Dictionary<string, Frame> _pending = new Dictionary<string, Frame>(StringComparer.Ordinal);
        private readonly Dictionary<string, int> _historyBytes = new Dictionary<string, int>(StringComparer.Ordinal);
        private object _version = new object();
        private int _used, _reserved;
        internal int UsedBytes { get { lock (_gate) return _used; } }
        internal void ForgetUnwritten(Frame frame) { _admissions.Forget(frame); }
        internal void RecoveredReclamation() { _admissions.Advance(); }

        internal ShardPilotWithdrawalJournal(string store, AdmissionEpoch admissions = null)
        {
            if (string.IsNullOrEmpty(store)) throw new ArgumentException("Missing journal identity");
            _store = store;
            _admissions = admissions ?? new AdmissionEpoch();
        }

        private static string Key(string scope, string id) { return scope.Length.ToString(CultureInfo.InvariantCulture) + ":" + scope + id; }
        private static string Key(Frame frame) { return Key(frame.Scope, frame.Id); }
        internal Frame Get(string scope, string id)
        {
            if (string.IsNullOrEmpty(scope) || string.IsNullOrEmpty(id)) return null;
            lock (_gate) { Frame frame; _durable.TryGetValue(Key(scope, id), out frame); return frame; }
        }
        internal Frame[] Snapshot()
        {
            lock (_gate)
            {
                var frames = new Frame[_durable.Count];
                _durable.Values.CopyTo(frames, 0);
                Array.Sort(frames, (a, b) => StringComparer.Ordinal.Compare(Key(a), Key(b)));
                return frames;
            }
        }
        private static bool ContainsAll(IList<string> values, IList<string> required)
        {
            var set = new HashSet<string>(values, StringComparer.Ordinal);
            foreach (var value in required) if (!set.Contains(value)) return false;
            return true;
        }

        internal static bool MateriallyChanges(Frame previous, Frame next)
        {
            return next.Fixed != previous.Fixed ||
                next.BoundaryTicks != previous.BoundaryTicks || next.Keys.Count != previous.Keys.Count ||
                next.Spared.Count != previous.Spared.Count || next.Scans > previous.Scans || !ContainsAll(previous.Pending, next.Pending);
        }

        private static bool Advances(Frame previous, Frame next)
        {
            if (previous == null) return next.Revision == 1 && !next.Fixed && !next.Assignment && !next.Pipeline;
            if (previous.Complete || previous.Revision == long.MaxValue || next.Revision != previous.Revision + 1 ||
                next.Experiment != previous.Experiment || next.WholePopulation != previous.WholePopulation ||
                next.BoundaryTicks < previous.BoundaryTicks || (previous.Fixed && !next.Fixed) ||
                !ContainsAll(next.Keys, previous.Keys) || !ContainsAll(next.Spared, previous.Spared)) return false;
            if (MateriallyChanges(previous, next)) return !next.Assignment && !next.Pipeline;
            return (!previous.Assignment || next.Assignment) && (!previous.Pipeline || next.Pipeline);
        }

        private bool TryTransitionLocked(Frame expected, Frame next, out byte[] encoded)
        {
            encoded = null;
            if (next == null || next.Store != _store) return false;
            if (expected == null && !_admissions.Allows(next, false)) return false;
            var key = Key(next); Frame previous;
            _durable.TryGetValue(key, out previous);
            return ReferenceEquals(previous, expected) && !_pending.ContainsKey(key) && Advances(previous, next) &&
                TryEncode(next, out encoded);
        }

        internal bool TryStage(Frame expected, Frame next)
        {
            lock (_gate)
            {
                byte[] encoded;
                if (!TryTransitionLocked(expected, next, out encoded) || encoded.Length > MaxJournalBytes - _used - _reserved ||
                    (expected == null && !_admissions.Allows(next, true))) return false;
                _pending.Add(Key(next), next); _reserved += encoded.Length;
                return true;
            }
        }

        internal bool TryPlanReclamation(Frame expected, Frame next, out Reclamation plan)
        {
            plan = null;
            lock (_gate)
            {
                byte[] encoded;
                if (_pending.Count != 0 || !TryTransitionLocked(expected, next, out encoded) ||
                    encoded.Length <= MaxJournalBytes - _used) return false;
                var retained = new List<Frame>(); var bytes = 0;
                foreach (var frame in _durable.Values)
                    if (!frame.Complete) { retained.Add(frame); bytes += _historyBytes[Key(frame)]; }
                if (bytes == _used || encoded.Length > MaxJournalBytes - bytes) return false;
                plan = new Reclamation(_version, bytes, retained); return true;
            }
        }

        // Storage commits this plan only after its retained histories are safe.
        internal bool CommitReclamation(Reclamation plan)
        {
            lock (_gate)
            {
                if (plan == null || !plan.Matches(_version) || _pending.Count != 0) return false;
                foreach (var key in new List<string>(_durable.Keys))
                    if (_durable[key].Complete) { _durable.Remove(key); _historyBytes.Remove(key); }
                _used = plan.RetainedBytes; _version = new object(); _admissions.Advance(); return true;
            }
        }

        internal bool AcknowledgePersisted(Frame frame)
        {
            if (frame == null) return false;
            lock (_gate)
            {
                var key = Key(frame); Frame staged; byte[] encoded;
                if (!_pending.TryGetValue(key, out staged) || !ReferenceEquals(staged, frame) || !TryEncode(frame, out encoded)) return false;
                _durable[key] = frame; _pending.Remove(key); _reserved -= encoded.Length; _used += encoded.Length;
                int history; _historyBytes.TryGetValue(key, out history); _historyBytes[key] = history + encoded.Length;
                _version = new object();
                return true;
            }
        }

        private bool Current(Frame frame)
        {
            if (frame == null) return false;
            Frame held; var key = Key(frame);
            return !_pending.ContainsKey(key) && _durable.TryGetValue(key, out held) && ReferenceEquals(held, frame);
        }
        internal bool CanProject(Frame frame) { lock (_gate) return Current(frame) && !frame.Complete; }
        internal bool CanRetire(Frame frame) { lock (_gate) return Current(frame) && frame.Complete; }

        internal bool TryReplay(byte[] encoded)
        {
            Frame frame;
            if (!TryDecode(encoded, out frame)) return false;
            lock (_gate)
            {
                if (_pending.Count != 0) return false;
                Frame previous; _durable.TryGetValue(Key(frame), out previous);
                return TryStage(previous, frame) && AcknowledgePersisted(frame);
            }
        }

        private static void Strings(StringBuilder b, string name, IEnumerable<string> values)
        {
            b.Append(','); ShardPilotJson.AppendQuoted(b, name); b.Append(":[");
            var first = true;
            foreach (var value in values) { if (!first) b.Append(','); first = false; ShardPilotJson.AppendQuoted(b, value); }
            b.Append(']');
        }
        private static string Digest(string body)
        {
            using (var hash = SHA256.Create()) return BitConverter.ToString(hash.ComputeHash(Utf8.GetBytes(body))).Replace("-", "").ToLowerInvariant();
        }

        internal static bool TryEncode(Frame frame, out byte[] encoded)
        {
            encoded = null;
            if (frame == null) return false;
            try
            {
                var b = new StringBuilder("{\"v\":1,\"store\":"); ShardPilotJson.AppendQuoted(b, frame.Store);
                b.Append(",\"scope\":"); ShardPilotJson.AppendQuoted(b, frame.Scope);
                b.Append(",\"id\":"); ShardPilotJson.AppendQuoted(b, frame.Id);
                b.Append(",\"experiment\":"); ShardPilotJson.AppendQuoted(b, frame.Experiment);
                b.Append(",\"revision\":").Append(frame.Revision.ToString(CultureInfo.InvariantCulture));
                b.Append(",\"boundary\":").Append(frame.BoundaryTicks.ToString(CultureInfo.InvariantCulture));
                b.Append(",\"fixed\":").Append(frame.Fixed ? "true" : "false");
                b.Append(",\"assignment\":").Append(frame.Assignment ? "true" : "false");
                b.Append(",\"pipeline\":").Append(frame.Pipeline ? "true" : "false");
                b.Append(",\"whole\":").Append(frame.WholePopulation ? "true" : "false");
                b.Append(",\"scans\":").Append(frame.Scans.ToString(CultureInfo.InvariantCulture));
                Strings(b, "keys", frame.Keys); Strings(b, "spared", frame.Spared); Strings(b, "pending", frame.Pending); b.Append('}');
                var body = b.ToString(); var bytes = Utf8.GetBytes("wj1|" + Digest(body) + "|" + body + "\n");
                if (bytes.Length > MaxFrameBytes) return false;
                encoded = bytes; return true;
            }
            catch (ArgumentException) { return false; }
        }

        private static IEnumerable<string> ReadStrings(object value)
        {
            var list = value as List<object>;
            if (list == null) throw new ArgumentException("Invalid withdrawal identities");
            var values = new List<string>();
            foreach (var item in list)
            {
                var text = item as string;
                if (text == null) throw new ArgumentException("Invalid withdrawal identity");
                values.Add(text);
            }
            return values;
        }

        internal static bool TryDecode(byte[] encoded, out Frame frame)
        {
            frame = null;
            if (encoded == null || encoded.Length < 71 || encoded.Length > MaxFrameBytes) return false;
            try
            {
                var text = Utf8.GetString(encoded);
                if (text.Length < 71 || !text.StartsWith("wj1|", StringComparison.Ordinal) || text[68] != '|' || text[text.Length - 1] != '\n') return false;
                var body = text.Substring(69, text.Length - 70);
                if (text.Substring(4, 64) != Digest(body)) return false;
                Dictionary<string, object> d; Dictionary<string, string> raw;
                if (!ShardPilotJsonParser.TryParseUniqueObject(body, out d, out raw) || d.Count != 15 || !(d["v"] is long) || (long)d["v"] != 1) return false;
                if (!(d["revision"] is long) || !(d["boundary"] is long) || !(d["scans"] is long) ||
                    !(d["fixed"] is bool) || !(d["assignment"] is bool) || !(d["pipeline"] is bool) || !(d["whole"] is bool)) return false;
                var scans = (long)d["scans"];
                if (scans < 0 || scans > int.MaxValue) return false;
                var candidate = new Frame((string)d["store"], (string)d["scope"], (string)d["id"], (string)d["experiment"],
                    (long)d["revision"], (long)d["boundary"], (bool)d["fixed"], (bool)d["assignment"], (bool)d["pipeline"],
                    (bool)d["whole"], (int)scans, ReadStrings(d["keys"]), ReadStrings(d["spared"]), ReadStrings(d["pending"]));
                byte[] canonical;
                if (!TryEncode(candidate, out canonical) || encoded.Length != canonical.Length) return false;
                for (var i = 0; i < encoded.Length; i++) if (encoded[i] != canonical[i]) return false;
                frame = candidate; return true;
            }
            catch (ArgumentException) { return false; }
            catch (InvalidCastException) { return false; }
            catch (KeyNotFoundException) { return false; }
        }
    }
}
