using System;
using System.Collections.Generic;

namespace ShardPilot
{
    /// <summary>A terminal per-event rejection reported inside an accepted batch.</summary>
#nullable enable annotations
    public sealed class ShardPilotRejection
    {
        internal ShardPilotRejection(ShardPilotEventOutcome outcome, ShardPilotConfig config, string requestBearer)
        {
            EventId = ShardPilotDiagnostics.Redact(config, outcome.EventId ?? string.Empty, requestBearer);
            Status = ShardPilotDiagnostics.Redact(config, outcome.Status ?? string.Empty, requestBearer);
            Code = ShardPilotDiagnostics.Redact(config, outcome.Code ?? string.Empty, requestBearer);
            Message = ShardPilotDiagnostics.Redact(config, outcome.Message ?? string.Empty, requestBearer);
        }

        public string EventId { get; private set; }
        public string Status { get; private set; }
        public string Code { get; private set; }
        public string Message { get; private set; }
    }

#nullable restore annotations
    public sealed partial class ShardPilotClient
    {
        private readonly object _rejectionGate = new object();
        private readonly Queue<ShardPilotRejection> _rejections = new Queue<ShardPilotRejection>();
        private readonly List<RejectionWindow> _pendingRejections = new List<RejectionWindow>();
        private readonly HashSet<string> _rejectionWarningCodes = new HashSet<string>(StringComparer.Ordinal);
        private int _rejectionWarnings;

        /// <summary>
        /// Returns an immutable snapshot, oldest first, of the most recent terminal
        /// rejections. Capacity defaults to 64. Entries are diagnostic records,
        /// not a retry queue, and survive only for this client instance.
        /// </summary>
        public IReadOnlyList<ShardPilotRejection> LastRejections()
        {
            lock (_rejectionGate)
            {
                return Array.AsReadOnly(_rejections.ToArray());
            }
        }

        private sealed class RejectionWindow
        {
            public long Boundary;
            public object CloseOwner;
            public ulong Count;
            public readonly Queue<ShardPilotRejection> Entries = new Queue<ShardPilotRejection>();
        }

        // Caller holds _gate, so registration and the intake boundary name
        // the same point in the admission order. Arm the request afterward.
        private void RegisterRejections(RejectionWindow window, long boundary, object closeOwner)
        {
            lock (_rejectionGate)
            {
                window.Boundary = boundary;
                window.CloseOwner = closeOwner;
                _pendingRejections.Add(window);
            }
        }

        private void RecordRejections(List<ShardPilotEvent> batch, ShardPilotBatchResult result, string requestBearer)
        {
            if (result.NonAcceptedOutcomes == null)
            {
                return;
            }

            lock (_rejectionGate)
            {
                foreach (var outcome in result.NonAcceptedOutcomes)
                {
                    if (outcome == null || outcome.Status != "rejected")
                    {
                        continue;
                    }

                    var entry = new ShardPilotRejection(outcome, _config, requestBearer);
                    _rejections.Enqueue(entry);
                    while (_rejections.Count > _config.RejectionCapacity)
                    {
                        _rejections.Dequeue();
                    }

                    var sequence = long.MaxValue;
                    object closeOwner = null;
                    foreach (var ev in batch)
                    {
                        if (ev == null || !string.Equals(ev.Id, outcome.EventId, StringComparison.Ordinal)) continue;
                        sequence = Math.Min(sequence, ev.IntakeSequence);
                        if (ev.CloseOwner != null) closeOwner = ev.CloseOwner;
                    }
                    if (sequence == long.MaxValue) continue;

                    foreach (var window in _pendingRejections)
                    {
                        if (sequence > window.Boundary &&
                            (window.CloseOwner == null || !ReferenceEquals(window.CloseOwner, closeOwner))) continue;
                        window.Count++;
                        window.Entries.Enqueue(entry);
                        while (window.Entries.Count > _config.RejectionCapacity) window.Entries.Dequeue();
                    }
                }
            }
        }

        private ShardPilotResult CompleteRejections(ShardPilotResult result, RejectionWindow window)
        {
            lock (_rejectionGate)
            {
                // Completion and timeout both retire the observer. Its retained
                // entries remain stable if another completion races this one.
                _pendingRejections.Remove(window);
                if (window.Count == 0)
                {
                    return result;
                }

                return result.WithRejections(window.Count, Array.AsReadOnly(window.Entries.ToArray()));
            }
        }

        private void WarnAboutRejection(ShardPilotEventOutcome outcome, string requestBearer)
        {
            try
            {
                var emit = false;
                lock (_rejectionGate)
                {
                    // First ten events, then one per previously unseen code. Retaining
                    // at most 64 code keys bounds both memory and the session's output.
                    emit = _rejectionWarnings < 10;
                    if (_rejectionWarningCodes.Count < 64 && _rejectionWarningCodes.Add(ShardPilotDiagnostics.WarningCodeKey(outcome.Code)))
                    {
                        emit = true;
                    }
                    if (emit) _rejectionWarnings++;
                }
                if (emit)
                {
                    ShardPilotDiagnostics.Log(_config, ShardPilotLogLevel.Warn, string.Format(
                        "ShardPilot event rejected: id={0} code={1} message={2}",
                        outcome.EventId, outcome.Code, outcome.Message), requestBearer);
                }
            }
            catch
            {
                // Diagnostic bookkeeping and sinks cannot turn an accepted
                // batch into a failure or cause a terminal verdict to retry.
            }
        }
    }
}
