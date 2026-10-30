using System;
using System.Collections.Generic;
using System.Threading;
using Frame = ShardPilot.ShardPilotWithdrawalJournal.Frame;

namespace ShardPilot
{
    // Only SharedState owns a journal lease. This object pins one borrowed operation.
    internal sealed class ShardPilotWithdrawalWork : IDisposable
    {
        private readonly SharedState _state;
        private int _disposed;
        internal readonly string Store;
        internal readonly bool Persistent;
        private ShardPilotWithdrawalWork(SharedState state)
        { _state = state; Store = state.Store; Persistent = !string.IsNullOrEmpty(state.CachePath); }
        private static readonly object StatesGate = new object();
        private static readonly Dictionary<string, SharedState> States = new Dictionary<string, SharedState>(StringComparer.Ordinal);
        private static readonly Dictionary<ShardPilotConfig, SharedState> MemoryStates = new Dictionary<ShardPilotConfig, SharedState>();
        private static int _opening;

        internal sealed class SharedState
        {
            private readonly object _gate = new object();
            private readonly Dictionary<string, Frame> _frames = new Dictionary<string, Frame>(StringComparer.Ordinal);
            internal readonly string Store, CachePath;
            private ShardPilotWithdrawalJournalOwner.Lease _lease;
            private int _operations;
            private bool _invalid;
            internal SharedState(ShardPilotConfig config, string cachePath)
            { CachePath = cachePath; Store = StoreFor(config); }
            private static string Key(Frame frame) { return frame.Scope.Length + ":" + frame.Scope + frame.Id; }
            internal Frame Remember(Frame frame, bool persisted = false)
            {
                lock (_gate)
                {
                    if (_invalid || frame == null || frame.Store != Store) return null;
                    Frame prior;
                    if (_frames.TryGetValue(Key(frame), out prior) &&
                        (prior.Revision > frame.Revision || (prior.Revision == frame.Revision && !persisted))) return prior;
                    _frames[Key(frame)] = frame; return frame;
                }
            }
            internal Frame Current(Frame fallback)
            {
                lock (_gate)
                { Frame frame; return fallback != null && _frames.TryGetValue(Key(fallback), out frame) ? frame : fallback; }
            }
            internal Frame[] Unfinished()
            {
                lock (_gate)
                { var result = new List<Frame>(); if (!_invalid) foreach (var frame in _frames.Values) if (!frame.Complete) result.Add(frame); return result.ToArray(); }
            }
            private bool HasUnfinishedLocked()
            { foreach (var frame in _frames.Values) if (!frame.Complete) return true; return false; }
            private ShardPilotWithdrawalJournalOwner.Lease OperationLease
            { get { lock (_gate) return _invalid ? null : _lease; } }
            internal ShardPilotWithdrawalWork Borrow(bool create, out bool absent)
            {
                absent = false;
                lock (StatesGate) lock (_gate)
                { if (_invalid) return null; _operations++; }
                var accepted = false;
                try
                {
                    var lease = OperationLease;
                    if (lease == null)
                    {
                        ShardPilotWithdrawalJournalOwner.Lease candidate;
                        if (string.IsNullOrEmpty(CachePath))
                        { absent = !create; candidate = create ? ShardPilotWithdrawalJournalOwner.AcquireMemory(Store) : null; }
                        else candidate = create ? ShardPilotWithdrawalJournalOwner.Acquire(JournalPath(CachePath, Store), Store) :
                            ShardPilotWithdrawalJournalOwner.AcquireExisting(JournalPath(CachePath, Store), Store, out absent);
                        lock (_gate) { if (_lease == null) { _lease = candidate; candidate = null; } lease = _lease; }
                        candidate?.Dispose();
                    }
                    Frame[] frames;
                    if (lease == null || !lease.TryRecover() || !ShardPilotWithdrawalForm.TryRead(lease, out frames)) return null;
                    foreach (var frame in frames) Remember(frame, true);
                    accepted = true; return new ShardPilotWithdrawalWork(this);
                }
                finally { if (!accepted) ReleaseOperation(); }
            }
            internal ShardPilotWithdrawalWork Pin()
            {
                lock (StatesGate) lock (_gate)
                { if (_invalid || _lease == null) return null; _operations++; return new ShardPilotWithdrawalWork(this); }
            }
            internal void ReleaseOperation()
            {
                ShardPilotWithdrawalJournalOwner.Lease released = null;
                lock (_gate)
                { _operations--; if (_operations == 0 && !HasUnfinishedLocked()) { released = _lease; _lease = null; } }
                released?.Dispose();
            }
            internal void CloseQuiescent()
            {
                ShardPilotWithdrawalJournalOwner.Lease released;
                lock (_gate) { released = _lease; _lease = null; _frames.Clear(); }
                released?.Dispose();
            }
            private ShardPilotWithdrawalJournalOwner.Lease Lease { get { return OperationLease; } }
            internal bool TryRecover() { return Lease != null && Lease.TryRecover(); }
            internal bool TryRead(out Frame[] frames) { return ShardPilotWithdrawalForm.TryRead(Lease, out frames); }
            internal bool TryGet(string scope, string id, out Frame frame) { return Lease.TryGet(scope, id, out frame); }
            internal bool TryPersist(Frame expected, Frame proposed, out Frame frame)
            { if (expected == null && ShardPilotWithdrawalForm.Valid(proposed)) Remember(proposed); var ok = ShardPilotWithdrawalForm.TryPersist(Lease, expected, proposed, out frame); if (ok) Remember(frame, true); return ok; }
            internal byte[] CopyBytesForTesting() { return Lease.CopyBytesForTesting(); }
            internal bool TrySnapshotForTesting(out Frame[] frames) { return Lease.TrySnapshot(out frames); }
            internal bool TryApply(Frame frame, ShardPilotWithdrawalProjection.Leg leg, Func<Frame, bool> apply, out Frame receipt)
            { var ok = ShardPilotWithdrawalProjection.TryApply(Lease, frame, leg, apply, out receipt); if (ok) Remember(receipt, true); return ok; }
            internal Frame Converge(Frame proposed, Func<Frame, long?> observe,
                Func<Frame, bool> assignment, Func<Frame, bool> pipeline)
            {
                if (proposed == null || proposed.Store != Store || !ShardPilotWithdrawalForm.Valid(proposed)) return null;
                if (!TryRecover()) return null;
                Frame current;
                if (!TryGet(proposed.Scope, proposed.Id, out current)) return null;
                if (current == null && !TryPersist(null, proposed, out current)) return null;
                if (!current.Fixed)
                {
                    Frame fixedFrame;
                    if (!TryFix(current, () => observe(current), out fixedFrame)) return current;
                    current = fixedFrame;
                }
                Frame receipt;
                if (!current.Assignment && TryApply(current,
                    ShardPilotWithdrawalProjection.Leg.Assignment, assignment, out receipt)) current = receipt;
                if (!current.Pipeline && TryApply(current,
                    ShardPilotWithdrawalProjection.Leg.Pipeline, pipeline, out receipt)) current = receipt;
                Remember(current, true); return current;
            }
            internal bool TryFix(Frame frame, Func<long?> observe, out Frame fixedFrame)
            {
                fixedFrame = null;
                if (!ShardPilotWithdrawalForm.Valid(frame)) return false;
                Frame accepted = null;
                try
                {
                    var ok = Lease.TryWrite(storage =>
                    {
                        if (!storage.CanProject(frame)) return false;
                        if (frame.Fixed) { accepted = frame; return true; }
                        if (frame.Revision == long.MaxValue) return false;
                        var observed = observe == null ? null : observe();
                        if (!observed.HasValue || observed < 0 || observed > DateTime.MaxValue.Ticks) return false;
                        ShardPilotWithdrawalForm.Kind kind; ShardPilotWithdrawalForm.TryKind(frame, out kind);
                        var boundary = frame.BoundaryTicks;
                        if ((kind == ShardPilotWithdrawalForm.Kind.AgeDrop || kind == ShardPilotWithdrawalForm.Kind.Drop) && observed >= boundary)
                        { if (observed >= DateTime.MaxValue.Ticks - 1) return false; boundary = observed.Value + 1; }
                        var next = new Frame(frame.Store, frame.Scope, frame.Id, frame.Experiment, frame.Revision + 1,
                            boundary, true, false, false, frame.WholePopulation, frame.Scans, frame.Keys, frame.Spared, frame.Pending);
                        if (!storage.TryAppend(frame, next)) return false;
                        accepted = next; return true;
                    });
                    fixedFrame = ok ? accepted : null; if (ok) Remember(fixedFrame, true); return ok;
                }
                catch { return false; }
            }
            internal bool Busy { get { lock (_gate) return _operations != 0; } }
            internal void Invalidate() { lock (_gate) _invalid = true; }
        }

        // Called with an already confirmed namespace; this path performs no I/O.
        internal static SharedState Register(ShardPilotConfig config, string cachePath, string identity, Frame frame = null)
        {
            lock (StatesGate)
            {
                SharedState state;
                if (string.IsNullOrEmpty(cachePath))
                { if (!MemoryStates.TryGetValue(config, out state)) MemoryStates.Add(config, state = new SharedState(config, cachePath)); }
                else
                {
                    if (identity == null) return null;
                    if (!States.TryGetValue(identity, out state)) States.Add(identity, state = new SharedState(config, cachePath));
                }
                if (state.Store != StoreFor(config)) return null;
                if (frame != null) state.Remember(frame);
                return state;
            }
        }
        internal static SharedState[] DetachForReset()
        {
            lock (StatesGate)
            {
                var states = new List<SharedState>(States.Values); states.AddRange(MemoryStates.Values);
                if (_opening != 0 || states.Exists(state => state.Busy)) throw new InvalidOperationException("Withdrawal operations must be quiescent before reset");
                foreach (var state in states) state.Invalidate();
                States.Clear(); MemoryStates.Clear(); return states.ToArray();
            }
        }
        internal static string StoreFor(ShardPilotConfig config)
        { return ShardPilotExperimentsLogic.BuildScope(config.WorkspaceKey, config.AppKey, config.EnvironmentKey, null, null, null); }
        private static string JournalPath(string cachePath, string store)
        { return cachePath + ".withdrawals." + ShardPilotExperimentsLogic.CredentialFingerprint(store); }
        internal static ShardPilotWithdrawalJournalOwner.IdentityReference ClaimIdentity(string cachePath, string store)
        { return string.IsNullOrEmpty(cachePath) || string.IsNullOrEmpty(store) ? null : ShardPilotWithdrawalJournalOwner.ClaimIdentity(JournalPath(cachePath, store), store); }
        internal static ShardPilotWithdrawalWork Open(ShardPilotConfig config, string cachePath, bool create, out bool absent)
        {
            absent = false;
            if (config == null) return null;
            lock (StatesGate) _opening++;
            try
            {
                var identity = string.IsNullOrEmpty(cachePath) ? null : ShardPilotExperiments.ConfirmCacheIdentity(cachePath, StoreFor(config));
                if (string.IsNullOrEmpty(cachePath) || identity != null) return Register(config, cachePath, identity)?.Borrow(create, out absent);
                // Acquisition establishes a missing permitted root and distinguishes known absence from unavailable I/O.
                var candidate = new SharedState(config, cachePath);
                var opened = candidate.Borrow(create, out absent);
                if (opened == null) return null;
                identity = ShardPilotExperiments.ConfirmCacheIdentity(cachePath, candidate.Store);
                SharedState selected = null;
                if (identity != null)
                    lock (StatesGate)
                    { if (!States.TryGetValue(identity, out selected)) States.Add(identity, selected = candidate); }
                if (ReferenceEquals(selected, candidate)) return opened;
                opened.Dispose(); candidate.CloseQuiescent();
                return selected?.Borrow(create, out absent);
            }
            finally { lock (StatesGate) _opening--; }
        }
        private bool Alive { get { return Volatile.Read(ref _disposed) == 0; } }
        private ShardPilotWithdrawalWork Pin() { return Alive ? _state.Pin() : null; }
        internal bool TryRecover() { using (var pin = Pin()) return pin != null && _state.TryRecover(); }
        internal ShardPilotWithdrawalWork Borrow() { return Alive ? _state.Pin() : null; }
        internal Frame[] Unfinished() { return _state.Unfinished(); }
        internal bool TryRead(out Frame[] frames) { frames = null; using (var pin = Pin()) return pin != null && _state.TryRead(out frames); }
        internal bool TryGet(string scope, string id, out Frame frame) { frame = null; using (var pin = Pin()) return pin != null && _state.TryGet(scope, id, out frame); }
        internal bool TryPersist(Frame expected, Frame proposed, out Frame frame)
        { frame = null; using (var pin = Pin()) return pin != null && _state.TryPersist(expected, proposed, out frame); }
        internal byte[] CopyBytesForTesting() { using (var pin = Pin()) return pin != null ? _state.CopyBytesForTesting() : null; }
        internal bool TrySnapshotForTesting(out Frame[] frames) { frames = null; using (var pin = Pin()) return pin != null && _state.TrySnapshotForTesting(out frames); }
        internal bool TryApply(Frame frame, ShardPilotWithdrawalProjection.Leg leg, Func<Frame, bool> apply, out Frame receipt)
        { receipt = null; using (var pin = Pin()) return pin != null && _state.TryApply(frame, leg, apply, out receipt); }
        internal Frame Converge(Frame proposed, Func<Frame, long?> observe, Func<Frame, bool> assignment, Func<Frame, bool> pipeline)
        { using (var pin = Pin()) return pin != null ? _state.Converge(proposed, observe, assignment, pipeline) : null; }
        internal bool TryFix(Frame frame, Func<long?> observe, out Frame fixedFrame)
        { fixedFrame = null; using (var pin = Pin()) return pin != null && _state.TryFix(frame, observe, out fixedFrame); }
        public void Dispose() { if (Interlocked.Exchange(ref _disposed, 1) == 0) _state.ReleaseOperation(); }
    }
}
