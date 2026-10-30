using System;
using System.Linq;

namespace ShardPilot
{
    internal static class ShardPilotWithdrawalForm
    {
        internal enum Kind { AgeDrop, AgeFacts, Drop, Purge }
        internal static string NewId(Kind kind)
        { return (int)kind < 0 || (int)kind > 3 ? null : (char)('0' + (int)kind) + ":" + Guid.NewGuid().ToString("N"); }
        internal static bool TryKind(ShardPilotWithdrawalJournal.Frame frame, out Kind kind)
        {
            kind = Kind.AgeDrop; Guid id;
            if (frame == null || frame.Id.Length != 34 || frame.Id[1] != ':' || frame.Id[0] < '0' || frame.Id[0] > '3' ||
                !Guid.TryParseExact(frame.Id.Substring(2), "N", out id) || id.ToString("N") != frame.Id.Substring(2)) return false;
            kind = (Kind)(frame.Id[0] - '0'); return true;
        }
        internal static bool TryRead(ShardPilotWithdrawalJournalOwner.Lease lease, out ShardPilotWithdrawalJournal.Frame[] frames)
        {
            frames = null; ShardPilotWithdrawalJournal.Frame[] all;
            if (lease == null || !lease.TrySnapshot(out all) || all.Any(frame => !Valid(frame))) return false;
            frames = Array.FindAll(all, frame => !frame.Complete); return true;
        }
        internal static bool Valid(ShardPilotWithdrawalJournal.Frame frame)
        {
            Kind kind; if (!TryKind(frame, out kind)) return false;
            if ((int)kind < 2) return frame.WholePopulation;
            return !frame.WholePopulation && frame.Keys.Count == 0 && frame.Spared.Count == 0 &&
                frame.Scans == 0 && (kind == Kind.Purge || frame.Pending.Count == 0);
        }
        private static bool Same(ShardPilotWithdrawalJournal.Frame a, ShardPilotWithdrawalJournal.Frame b)
        {
            return a.Store == b.Store && a.Scope == b.Scope && a.Id == b.Id && a.Experiment == b.Experiment &&
                a.BoundaryTicks == b.BoundaryTicks && a.Fixed == b.Fixed && a.WholePopulation == b.WholePopulation &&
                a.Scans == b.Scans && a.Keys.SequenceEqual(b.Keys) && a.Spared.SequenceEqual(b.Spared) && a.Pending.SequenceEqual(b.Pending);
        }
        internal static bool TryPersist(ShardPilotWithdrawalJournalOwner.Lease lease,
            ShardPilotWithdrawalJournal.Frame expected, ShardPilotWithdrawalJournal.Frame proposed,
            out ShardPilotWithdrawalJournal.Frame persisted)
        {
            persisted = null;
            if (lease == null || !Valid(proposed) || proposed.Assignment || proposed.Pipeline) return false;
            ShardPilotWithdrawalJournal.Frame accepted = null;
            var ok = lease.TryWrite(store =>
            {
                ShardPilotWithdrawalJournal.Frame current;
                if (!store.TryGet(proposed.Scope, proposed.Id, out current) || !ReferenceEquals(current, expected) ||
                    (expected != null && (!Valid(expected) || expected.Revision == long.MaxValue || proposed.Revision != expected.Revision + 1))) return false;
                if (expected != null && Same(expected, proposed)) { accepted = expected; return true; }
                var next = proposed;
                if (expected != null && !ShardPilotWithdrawalJournal.MateriallyChanges(expected, proposed))
                    next = new ShardPilotWithdrawalJournal.Frame(proposed.Store, proposed.Scope, proposed.Id, proposed.Experiment,
                        proposed.Revision, proposed.BoundaryTicks, proposed.Fixed, expected.Assignment, expected.Pipeline,
                        proposed.WholePopulation, proposed.Scans, proposed.Keys, proposed.Spared, proposed.Pending);
                if (!store.TryAppend(expected, next)) return false;
                accepted = next; return true;
            });
            persisted = ok ? accepted : null; return ok;
        }
    }
}
