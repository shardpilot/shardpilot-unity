using System;
using System.IO;

namespace ShardPilot
{
    // File storage acknowledges after managed flush; memory mode confirms process state only.
    internal sealed class ShardPilotWithdrawalJournalStore : IDisposable
    {
        internal enum Failure { None, Unavailable, Corrupt, TooLarge, Changed, Write, Flush, Transition, Closed, Absent }
        private readonly object _gate = new object();
        private readonly ShardPilotWithdrawalJournal _state;
        private readonly ShardPilotWithdrawalJournalCheckpoint _checkpoint;
        private FileStream _file;
        private readonly bool _memory;
        internal ShardPilotWithdrawalJournalStore(string store, ShardPilotWithdrawalJournal.AdmissionEpoch admissions = null)
        { _state = new ShardPilotWithdrawalJournal(store, admissions); _memory = true; _healthy = true; }
        private volatile bool _healthy, _writing;
        private volatile Failure _failure;
        internal bool Ready { get { return _healthy && !_writing; } }
        internal Failure LastFailure { get { return _failure; } }
        internal byte[] CopyBytesForTesting()
        {
            lock (_gate)
            {
                if (!_healthy || _writing || _memory) throw new IOException("Journal snapshot unavailable");
                var position = _file.Position;
                try
                {
                    _file.Position = 0; var bytes = new byte[_state.UsedBytes]; var read = 0;
                    while (read < bytes.Length) { var n = _file.Read(bytes, read, bytes.Length - read); if (n == 0) throw new EndOfStreamException(); read += n; }
                    return bytes;
                }
                finally { _file.Position = position; }
            }
        }

        internal ShardPilotWithdrawalJournalStore(string path, string store, Func<string, FileStream> openForTesting = null,
            bool existingOnly = false, ShardPilotWithdrawalJournal.AdmissionEpoch admissions = null,
            Action<string> checkpointForTesting = null, Func<string, FileStream> prepareForTesting = null)
        {
            try
            {
                _state = new ShardPilotWithdrawalJournal(store, admissions);
                var full = Path.GetFullPath(path);
                _checkpoint = new ShardPilotWithdrawalJournalCheckpoint(full, store, checkpointForTesting, prepareForTesting);
                if (!existingOnly) Directory.CreateDirectory(Path.GetDirectoryName(full));
                try
                {
                    _file = openForTesting != null ? openForTesting(full) :
                        new FileStream(full, existingOnly ? FileMode.Open : FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
                }
                catch (FileNotFoundException) when (existingOnly)
                { Fail(_checkpoint.ReadPrepared() == null ? Failure.Absent : Failure.Unavailable); return; }
                catch (DirectoryNotFoundException) when (existingOnly)
                { Fail(_checkpoint.ReadPrepared() == null ? Failure.Absent : Failure.Unavailable); return; }
                _checkpoint.Recover(_file, _state.RecoveredReclamation);
                _file.Position = 0;
                if (!Replay()) return;
                // Reopening also synchronizes a complete frame left by an interrupted writer.
                _file.Flush(true);
                _healthy = true;
            }
            catch (InvalidDataException) { Fail(Failure.Corrupt); }
            catch (IOException) { Fail(Failure.Unavailable); }
            catch (UnauthorizedAccessException) { Fail(Failure.Unavailable); }
            catch (ArgumentException) { Fail(Failure.Unavailable); }
            catch (NotSupportedException) { Fail(Failure.Unavailable); }
            finally { if (!_healthy) CloseFile(); }
        }

        private bool Fail(Failure failure)
        {
            _failure = failure; _healthy = false;
            return false;
        }

        private bool Replay()
        {
            var length = _file.Length;
            if (length > ShardPilotWithdrawalJournal.MaxJournalBytes) return Fail(Failure.TooLarge);
            var data = new byte[(int)length];
            var read = 0;
            while (read < data.Length)
            {
                var n = _file.Read(data, read, data.Length - read);
                if (n == 0) return Fail(Failure.Changed);
                read += n;
            }
            if (_file.Length != length) return Fail(Failure.Changed);
            var start = 0;
            for (var i = 0; i < data.Length; i++)
            {
                if (i - start >= ShardPilotWithdrawalJournal.MaxFrameBytes) return Fail(Failure.Corrupt);
                if (data[i] != (byte)'\n') continue;
                var frame = new byte[i - start + 1];
                Buffer.BlockCopy(data, start, frame, 0, frame.Length);
                if (!_state.TryReplay(frame)) return Fail(Failure.Corrupt);
                start = i + 1;
            }
            // Do not discard a damaged suffix or expose a successfully decoded prefix.
            return start == data.Length || Fail(Failure.Corrupt);
        }

        // False means unavailable or busy; true with null means known absence.
        internal bool TryGet(string scope, string id, out ShardPilotWithdrawalJournal.Frame frame)
        {
            frame = null;
            if (!Ready) return false;
            var current = _state.Get(scope, id);
            if (!Ready) return false;
            frame = current; return true;
        }

        internal bool CanProject(ShardPilotWithdrawalJournal.Frame frame)
        { return Ready && _state.CanProject(frame) && Ready; }
        internal bool CanRetire(ShardPilotWithdrawalJournal.Frame frame)
        { return Ready && _state.CanRetire(frame) && Ready; }

        internal bool TrySnapshot(out ShardPilotWithdrawalJournal.Frame[] frames)
        {
            frames = null;
            if (!Ready) return false;
            var snapshot = _state.Snapshot();
            if (!Ready) return false;
            frames = snapshot; return true;
        }

        internal bool TryAppend(ShardPilotWithdrawalJournal.Frame expected, ShardPilotWithdrawalJournal.Frame next)
        {
            lock (_gate)
            {
                if (!Ready) return false;
                var flushing = false;
                var stagedForAppend = false;
                var appendAttempted = false;
                long writeStart = -1;
                _writing = true;
                try
                {
                    if (!_memory && _file.Length != _state.UsedBytes) return Fail(Failure.Changed);
                    if (!_state.TryStage(expected, next))
                    {
                        ShardPilotWithdrawalJournal.Reclamation plan;
                        if (!_state.TryPlanReclamation(expected, next, out plan))
                        { _failure = Failure.Transition; return false; }
                        if (_memory)
                        { if (!_state.CommitReclamation(plan)) return Fail(Failure.Transition); }
                        else _checkpoint.Reclaim(_file, _state, plan, syncing => flushing = syncing);
                        if (!_state.TryStage(expected, next)) return Fail(Failure.Transition);
                    }
                    byte[] encoded;
                    if (!ShardPilotWithdrawalJournal.TryEncode(next, out encoded)) return Fail(Failure.Transition);
                    if (!_memory)
                    {
                        stagedForAppend = true;
                        var length = _file.Length;
                        writeStart = length;
                        _file.Position = length;
                        appendAttempted = true;
                        _file.Write(encoded, 0, encoded.Length);
                        if (_file.Length != length + encoded.Length) return Fail(Failure.Write);
                        flushing = true;
                        _file.Flush(true);
                    }
                    if (!_state.AcknowledgePersisted(next)) return Fail(Failure.Transition);
                    _failure = Failure.None;
                    return true;
                }
                catch (InvalidDataException) { return Fail(Failure.Corrupt); }
                catch (IOException) { return Fail(flushing ? Failure.Flush : Failure.Write); }
                catch (UnauthorizedAccessException) { return Fail(flushing ? Failure.Flush : Failure.Write); }
                catch (NotSupportedException) { return Fail(flushing ? Failure.Flush : Failure.Write); }
                finally
                {
                    // A staged frame with no append attempt has no durable bytes.
                    // After an attempt, only proven zero-byte failures release it.
                    if (!_healthy && stagedForAppend && next.Revision == 1)
                        try { if (!appendAttempted || _file.Length == writeStart) _state.ForgetUnwritten(next); }
                        catch (IOException) { }
                        catch (NotSupportedException) { }
                    _writing = false;
                }
            }
        }

        private void CloseFile()
        {
            if (_file == null) return;
            try { _file.Dispose(); }
            catch (IOException) { }
            finally { _file = null; }
        }

        public void Dispose()
        {
            lock (_gate)
            {
                _healthy = false;
                if (_failure == Failure.None || _failure == Failure.Transition) _failure = Failure.Closed;
                CloseFile();
            }
        }
    }
}
