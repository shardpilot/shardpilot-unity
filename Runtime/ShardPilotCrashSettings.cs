namespace ShardPilot
{
    /// <summary>
    /// Persisted crash-reporting opt-out — the crash plane's own dedicated
    /// setting, INDEPENDENT of analytics consent (separate storage key,
    /// separate gate). Crash diagnostics run on a legitimate-interest basis:
    /// they are ON by default with no first-run decision, and the player can
    /// opt out at any time through
    /// <see cref="ShardPilotClient.SetCrashReportingEnabled"/>.
    ///
    /// Fail-closed resolution: an ABSENT record is the legitimate-interest
    /// default (enabled), but a record that cannot be READ — or holds a
    /// malformed (non-boolean) value — disables the plane for the session
    /// (reason <see cref="ReadFailedReason"/>): an unreadable record may
    /// have carried an opt-out, and a corrupt one proves the storage cannot
    /// be trusted, so neither may silently default to sending.
    /// </summary>
    internal static class ShardPilotCrashSettings
    {
        public const string StorageKey = "shardpilot.crash.enabled";
        private const string EnabledValue = "true";
        private const string DisabledValue = "false";

        /// <summary>Disabled because the player opted out.</summary>
        public const string OptOutReason = "opt_out";

        /// <summary>
        /// Disabled because the persisted setting could not be read or held
        /// a malformed (non-boolean) value — fail closed.
        /// </summary>
        public const string ReadFailedReason = "settings_read_failed";

        /// <summary>
        /// Disabled because the client was initialized without a
        /// <see cref="ShardPilotConfig.CrashKey"/>: the crash plane is off
        /// by configuration — no capture, no send — regardless of the
        /// persisted opt-in/opt-out decision.
        /// </summary>
        public const string NotConfiguredReason = "not_configured";

        /// <summary>
        /// The refusal reason surfaced when a crash-plane call is refused
        /// while crash reporting is disabled (opt-out or read failure).
        /// </summary>
        public const string DisabledError = "crash_disabled";

        /// <summary>
        /// The error surfaced when an opt-in/opt-out decision could not be
        /// durably persisted. The in-memory decision still applies for the
        /// session; call <see cref="ShardPilotClient.SetCrashReportingEnabled"/>
        /// again to retry persistence.
        /// </summary>
        public const string PersistFailedError = "crash_persist_failed";

        /// <summary>
        /// A PROCESS-WIDE shadow of an opt-out whose durable persist FAILED:
        /// the decision "still applies for the session", and the session is
        /// the process — a same-process re-initialization must not reload
        /// the stale/absent stored value and default the plane back on.
        /// Set only on a failed DISABLE persist and cleared by the next
        /// successful persist; the inverse direction is deliberately never
        /// shadowed — a failed ENABLE must not reopen a fail-closed client
        /// at a later construction.
        /// </summary>
        private static volatile bool _sessionOptOutPending;

        /// <summary>Test seam: clears the process-wide pending opt-out.</summary>
        internal static void ResetSessionStateForTesting()
        {
            _sessionOptOutPending = false;
        }

        /// <summary>
        /// Resolves the crash-reporting enabled state from the persisted
        /// record. <paramref name="disabledReason"/> is null when enabled;
        /// <see cref="OptOutReason"/> or <see cref="ReadFailedReason"/> when
        /// disabled. A null store means no persistence is wired at all —
        /// that is "absent" (the legitimate-interest default), not a read
        /// failure.
        /// </summary>
        public static bool Load(IShardPilotKeyValueStore store, out string disabledReason)
        {
            disabledReason = null;

            // An opt-out recorded earlier in this process whose persist
            // failed outranks whatever the store holds: it is the player's
            // latest decision, and only a successful durable persist (which
            // clears this) supersedes it.
            if (_sessionOptOutPending)
            {
                disabledReason = OptOutReason;
                return false;
            }

            if (store == null)
            {
                return true;
            }

            string raw;
            try
            {
                raw = ShardPilotConfig.Trim(store.Get(StorageKey));
            }
            catch
            {
                // The stored setting may have been an opt-out: fail CLOSED.
                disabledReason = ReadFailedReason;
                return false;
            }

            if (string.IsNullOrEmpty(raw))
            {
                // Absent record: the legitimate-interest default is ON.
                return true;
            }

            switch (raw)
            {
                case EnabledValue:
                    return true;
                case DisabledValue:
                    disabledReason = OptOutReason;
                    return false;
                default:
                    // Malformed (non-boolean) value = a read failure: the
                    // record exists but cannot be trusted — fail CLOSED
                    // rather than silently defaulting to enabled.
                    disabledReason = ReadFailedReason;
                    return false;
            }
        }

        /// <summary>
        /// Persists an explicit enable/disable decision. Returns true only
        /// when the write succeeded (a null store trivially "succeeds" —
        /// there is nothing to persist to, and nothing a later init could
        /// misread). Never throws.
        /// </summary>
        public static bool TrySave(IShardPilotKeyValueStore store, bool enabled)
        {
            if (store == null)
            {
                return true;
            }

            try
            {
                store.Set(StorageKey, enabled ? EnabledValue : DisabledValue);

                // A durable decision supersedes any pending failed opt-out.
                _sessionOptOutPending = false;
                return true;
            }
            catch
            {
                if (!enabled)
                {
                    // The opt-out applies in memory for the whole process:
                    // a later same-process construction must resolve to it
                    // (see Load) instead of reloading the stale value.
                    _sessionOptOutPending = true;
                }

                return false;
            }
        }
    }
}
