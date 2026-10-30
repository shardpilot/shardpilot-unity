using System;

namespace ShardPilot
{
    internal static class ShardPilotWithdrawalProjection
    {
        internal enum Leg { Assignment, Pipeline }

        // A successful operation confirms the whole named projection for this exact frame.
        // Pipeline success includes every scan and pending removal; false keeps that work owed.
        internal static bool TryApply(ShardPilotWithdrawalJournalOwner.Lease lease,
            ShardPilotWithdrawalJournal.Frame frame, Leg leg,
            Func<ShardPilotWithdrawalJournal.Frame, bool> apply, out ShardPilotWithdrawalJournal.Frame receipt)
        {
            receipt = null;
            if (lease == null || frame == null || apply == null ||
                (leg != Leg.Assignment && leg != Leg.Pipeline)) return false;
            ShardPilotWithdrawalJournal.Frame persisted = null;
            var assignment = leg == Leg.Assignment;
            var ok = lease.TryWrite(store =>
            {
                if (!frame.Fixed || frame.Revision == long.MaxValue || !store.CanProject(frame) ||
                    (assignment ? frame.Assignment : frame.Pipeline)) return false;
                var next = new ShardPilotWithdrawalJournal.Frame(frame.Store, frame.Scope, frame.Id,
                    frame.Experiment, frame.Revision + 1, frame.BoundaryTicks, frame.Fixed,
                    assignment || frame.Assignment, !assignment || frame.Pipeline, frame.WholePopulation,
                    assignment ? frame.Scans : 0, frame.Keys, frame.Spared,
                    assignment ? frame.Pending : new string[0]);
                if (!apply(frame) || !store.TryAppend(frame, next)) return false;
                persisted = next; return true;
            });
            receipt = ok ? persisted : null; return ok;
        }
    }
}
