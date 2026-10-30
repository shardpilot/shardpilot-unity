using System;
using System.Collections;
using System.Collections.Generic;
using ShardPilot;
using UnityEngine;

public sealed class ShardPilotEngineSmokeSample : MonoBehaviour
{
    [SerializeField] private string ingestUrl = "https://ingest.example.com";
    [SerializeField] private string publishableKey = "sp_local_placeholder";
    // Crash reporting uses its own URL and credential; it never falls back
    // to the analytics host. The component identifies the emitting surface.
    [SerializeField] private string crashUrl = "https://crash.example.com";
    [SerializeField] private string crashKey = "crash_local_placeholder";
    [SerializeField] private string crashComponent = "unity-smoke-client";
    [SerializeField] private string workspaceKey = "workspace_example";
    [SerializeField] private string appKey = "unity-smoke-sample";
    [SerializeField] private string environmentKey = "develop";
    [SerializeField] private bool emitCrashOnStart = true;

    private IEnumerator Start()
    {
        ShardPilotSdk.Initialize(new ShardPilotConfig
        {
            PublishableKey = publishableKey,
            IngestUrl = ingestUrl,
            CrashUrl = crashUrl,
            CrashKey = crashKey,
            CrashComponent = crashComponent,
            WorkspaceKey = workspaceKey,
            AppKey = appKey,
            EnvironmentKey = environmentKey,
            AppVersion = Application.version,
            // The smoke emits exactly ONE synthetic handled (non-fatal)
            // exception below and the runbook expects it on the crash
            // service, so send every non-fatal report instead of the
            // default 1-in-10 sampling.
            CrashSampleEvery = 1
        });

        // Consent-first: the smoke grants analytics consent up front so the
        // synthetic events below can leave the device.
        ShardPilotSdk.SetConsent(true);

        ShardPilotSdk.SessionStart();
        ShardPilotSdk.Track("unity_engine_smoke", new Dictionary<string, object>
        {
            {"surface", "sample"},
            {"synthetic", true}
        });

        var eventFlush = ShardPilotSdk.FlushAsync();
        while (!eventFlush.IsCompleted)
        {
            yield return null;
        }

        if (emitCrashOnStart)
        {
            try
            {
                throw new InvalidOperationException("sample handled smoke crash");
            }
            catch (Exception ex)
            {
                Debug.LogException(ex);
            }

            var crashFlush = ShardPilotSdk.FlushAsync();
            while (!crashFlush.IsCompleted)
            {
                yield return null;
            }
        }
    }

    private async void OnApplicationQuit()
    {
        await ShardPilotSdk.CloseAsync();
    }
}
