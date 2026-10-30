using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace ShardPilot
{
    // A published, flushed image owns recovery until rebuilding and cleanup finish.
    internal sealed class ShardPilotWithdrawalJournalCheckpoint
    {
        private readonly string _path, _store;
        private readonly Action<string> _phase;
        private readonly Func<string, FileStream> _prepare;
        internal ShardPilotWithdrawalJournalCheckpoint(string path, string store, Action<string> phase, Func<string, FileStream> prepare)
        { _path = path + ".prepared"; _store = store; _phase = phase; _prepare = prepare; }
        private static string Digest(byte[] bytes)
        { using (var sha = SHA256.Create()) return BitConverter.ToString(sha.ComputeHash(bytes)).Replace("-", "").ToLowerInvariant(); }
        private static string Key(ShardPilotWithdrawalJournal.Frame frame)
        { return frame.Scope.Length.ToString(CultureInfo.InvariantCulture) + ":" + frame.Scope + frame.Id; }
        private static byte[] Read(FileStream file, int bound)
        {
            var length = file.Length;
            if (length > bound) throw new InvalidDataException("Withdrawal image exceeds its bound");
            file.Position = 0; var bytes = new byte[(int)length]; var offset = 0;
            while (offset < bytes.Length)
            { var count = file.Read(bytes, offset, bytes.Length - offset); if (count == 0) throw new EndOfStreamException(); offset += count; }
            if (file.Length != length) throw new IOException("Withdrawal image changed");
            return bytes;
        }
        internal byte[] ReadPrepared()
        {
            try
            { using (var file = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.None)) return Read(file, ShardPilotWithdrawalJournal.MaxJournalBytes + 160); }
            catch (FileNotFoundException) { return null; }
            catch (DirectoryNotFoundException) { return null; }
        }
        private static IEnumerable<byte[]> Records(byte[] data)
        {
            var start = 0;
            for (var i = 0; i < data.Length; i++)
            {
                if (i - start >= ShardPilotWithdrawalJournal.MaxFrameBytes) throw new InvalidDataException("Withdrawal record exceeds its bound");
                if (data[i] != (byte)'\n') continue;
                var record = new byte[i - start + 1]; Buffer.BlockCopy(data, start, record, 0, record.Length);
                yield return record; start = i + 1;
            }
            if (start != data.Length) throw new InvalidDataException("Incomplete withdrawal image");
        }
        private ShardPilotWithdrawalJournal Check(byte[] data)
        {
            var journal = new ShardPilotWithdrawalJournal(_store);
            foreach (var record in Records(data))
                if (!journal.TryReplay(record)) throw new InvalidDataException("Invalid withdrawal history");
            return journal;
        }
        private static byte[] Encode(ShardPilotWithdrawalJournal.Frame frame)
        {
            byte[] bytes;
            if (!ShardPilotWithdrawalJournal.TryEncode(frame, out bytes)) throw new InvalidDataException("Invalid withdrawal frame");
            return bytes;
        }
        private byte[] Payload(byte[] prepared)
        {
            var end = Array.IndexOf(prepared, (byte)'\n');
            if (end < 0 || end >= 160) throw new InvalidDataException("Invalid withdrawal image header");
            var header = Encoding.ASCII.GetString(prepared, 0, end).Split('|'); int length;
            if (header.Length != 4 || header[0] != "wjc1" || header[1] != Digest(Encoding.UTF8.GetBytes(_store)) ||
                !int.TryParse(header[2], NumberStyles.None, CultureInfo.InvariantCulture, out length) ||
                length > ShardPilotWithdrawalJournal.MaxJournalBytes || length != prepared.Length - end - 1)
                throw new InvalidDataException("Mismatched withdrawal image");
            var data = new byte[length]; Buffer.BlockCopy(prepared, end + 1, data, 0, length);
            if (header[3] != Digest(data)) throw new InvalidDataException("Incomplete withdrawal image");
            Check(data); return data;
        }
        private void Finish(FileStream file, byte[] data, Action commit, Action<bool> syncing)
        {
            _phase?.Invoke("rebuilding");
            file.Position = 0; file.Write(data, 0, data.Length);
            if (file.Position != data.Length) throw new IOException("Incomplete withdrawal rebuild");
            file.SetLength(data.Length); syncing?.Invoke(true); file.Flush(true); syncing?.Invoke(false);
            if (!Read(file, ShardPilotWithdrawalJournal.MaxJournalBytes).SequenceEqual(data))
                throw new IOException("Withdrawal rebuild changed");
            commit(); _phase?.Invoke("cleaning"); File.Delete(_path);
        }
        internal void Recover(FileStream file, Action commit)
        {
            var prepared = ReadPrepared();
            if (prepared != null) Finish(file, Payload(prepared), commit, null);
        }
        internal void Reclaim(FileStream file, ShardPilotWithdrawalJournal state,
            ShardPilotWithdrawalJournal.Reclamation plan, Action<bool> syncing)
        {
            var source = Read(file, ShardPilotWithdrawalJournal.MaxJournalBytes);
            var checkedFrames = Check(source).Snapshot(); var current = state.Snapshot();
            if (checkedFrames.Length != current.Length || checkedFrames.Where((frame, i) => !Encode(frame).SequenceEqual(Encode(current[i]))).Any())
                throw new InvalidDataException("Withdrawal history changed before reclamation");
            var retained = new HashSet<string>(plan.Retained.Select(Key), StringComparer.Ordinal); byte[] data;
            using (var output = new MemoryStream())
            {
                foreach (var record in Records(source))
                {
                    ShardPilotWithdrawalJournal.Frame frame;
                    if (!ShardPilotWithdrawalJournal.TryDecode(record, out frame)) throw new InvalidDataException("Invalid withdrawal history");
                    if (retained.Contains(Key(frame))) output.Write(record, 0, record.Length);
                }
                data = output.ToArray();
            }
            if (data.Length != plan.RetainedBytes) throw new InvalidDataException("Withdrawal plan byte count changed");
            var header = Encoding.ASCII.GetBytes("wjc1|" + Digest(Encoding.UTF8.GetBytes(_store)) + "|" +
                data.Length.ToString(CultureInfo.InvariantCulture) + "|" + Digest(data) + "\n");
            using (var temporary = _prepare != null ? _prepare(_path + ".tmp") :
                new FileStream(_path + ".tmp", FileMode.Create, FileAccess.Write, FileShare.None))
            {
                temporary.SetLength(0); temporary.Position = 0;
                temporary.Write(header, 0, header.Length); _phase?.Invoke("preparing"); temporary.Write(data, 0, data.Length);
                if (temporary.Length != header.Length + data.Length) throw new IOException("Incomplete withdrawal preparation");
                syncing(true); temporary.Flush(true); syncing(false);
            }
            _phase?.Invoke("publishing"); File.Move(_path + ".tmp", _path);
            Finish(file, data, () =>
            { if (!state.CommitReclamation(plan)) throw new IOException("Withdrawal plan changed"); }, syncing);
        }
    }
}
