using System.Collections.Generic;
using ShardPilot;
using UnityEngine;
using Debug = UnityEngine.Debug;

public sealed class ShardPilotQuickstart : MonoBehaviour
{
    private void Awake()
    {
        ShardPilotSdk.Initialize(new ShardPilotConfig
        {
            PublishableKey = "sp_replace_me",
            WorkspaceKey = "workspace_example",
            AppKey = "app_example",
            EnvironmentKey = "develop",
            AppVersion = Application.version
        });
    }

    private void Start()
    {
        // CONSENT-FIRST: analytics starts gated and nothing is tracked,
        // buffered, or sent until the player grants. In a real game, call
        // SetConsent(true) from your consent dialog's accept handler (the
        // decision persists, so ask once); SetConsent(false) records a
        // denial. A configured crash key enables crash reporting; opt out
        // via ShardPilotSdk.SetCrashReportingEnabled(false). Denied and
        // DeniedForcedMinor also suppress crash capture.
        // Every overload reports whether the decision applied. A refusal
        // leaves analytics gated; durability warnings keep retrying but must
        // be surfaced when losing the receipt on exit is unacceptable.
        var consent = ShardPilotSdk.SetConsent(ShardPilotConsentDecision.Granted);
        if (!consent.Success)
        {
            Debug.LogWarning("ShardPilot: consent decision refused (" + consent.Error + ").");
            return;
        }

        foreach (var warning in consent.Warnings)
        {
            Debug.LogWarning("ShardPilot: consent decision applied with warning (" + warning + ").");
        }

        ShardPilotSdk.SessionStart();
        ShardPilotSdk.Track("app.screen_view", new Dictionary<string, object>
        {
            {"screen_name", "main_menu"}
        });
    }

    // NO SHUTDOWN CODE, deliberately. The SDK installs its own lifecycle host
    // and, on the quit and pause callbacks Unity raises, SYNCHRONOUSLY retries
    // the writes it still owes — pending events where a spool exists, and the
    // consent receipt. That is one retry, not a guarantee: a backend that is
    // still refusing writes leaves the receipt in memory and quitting destroys
    // it, which is why SetConsent's return value is checked above. For the
    // ordinary case a quickstart owes nothing here.
    //
    // Calling CloseAsync from OnApplicationQuit would be worse than nothing:
    // that callback returns void, so the first await hands control back to
    // Unity and the continuation may never run.
    //
    // Holding the quit open for the final NETWORK flush is a real thing to
    // want, and it is genuinely harder than it looks — see "Shutdown" in the
    // README for the contract and the four cases such a barrier has to get
    // right before it is an improvement on this.
}
