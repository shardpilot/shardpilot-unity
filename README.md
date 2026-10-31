# shardpilot-unity

> C# Unity Package Manager (UPM) SDK for ShardPilot: app-first analytics, sanitized crash capture, and ETag-cached remote config from Unity games to ShardPilot ingest.

ShardPilot SDK for Unity. Implements the app-first event envelope, the dual-mode client ingest auth contract, and the sanitized crash report shape, consistent with the other ShardPilot SDKs.

Every service request includes `X-ShardPilot-SDK: unity/<version>`, using the
package release version without build metadata. This also identifies compressed
requests and their uncompressed retries; request-specific headers cannot replace it.

## Status

- A [directory-sweep intent ledger](Documentation~/directory-sweep-ledger.md) preserves captured owning-path debts across independent directories and retries original paths before loading. Known independent paths stay available after checked replay of refused retirement writes; unknown metadata or alias identity withholds acknowledgment.
- Directory-sweep recovery fixtures pair persisted denial witnesses with sender controls across separate processes.
- Sweep debt isolation fixtures pair independent directories with same-canonical-directory acknowledgment controls.

- Withdrawal durability uses immutable journal frames, checked file storage and canonical shared-state ownership. Discovery follows the spool's workspace/app/environment namespace; each frame retains its captured assignment scope. Fixed decisions and independent assignment/pipeline receipts require flushed journal revisions. A failed shared journal can reopen and synchronize replay in-process while retaining its owner; damaged input grants no authority. Unknown storage withholds authority, and later admissions persist their sealed identity exemptions before enqueue; see [withdrawal projections](Documentation~/withdrawal-projections.md).
- Only shared state owns a journal Work lease. Pending objects, wrappers and clients borrow; completed work releases its lease after its last active operation finishes. Incomplete memory-only work remains in canonical state. Test reset is [quiescent global teardown](Documentation~/withdrawal-projections.md#shared-state-reset-contract) and refuses atomically while an operation is active.
- Dark-successor handoff completion is observed through a held full journal snapshot; incomplete-only discovery intentionally omits completed frames.
- A withdrawal journal reclaims completed history when a valid admission needs capacity, preserving unfinished canonical frames and their full historical byte charge. File storage publishes a checked, flushed recovery image before rebuilding through its exclusively held journal handle; incomplete recovery withholds authority. Invalid transitions and all-unfinished capacity refuse without eviction. Memory-only storage remains non-persistent.
- A held journal rebuild retains its writer reservation and actual-file exclusion while client disposal returns promptly; independent paths remain writable. Barrier-controlled checks verify admission and replay after the hold is released.
- An owner-scoped reclaim epoch fences stale revision-one captures, preventing a retired withdrawal from becoming a fresh destructive operation after its history is removed. Weak capture metadata follows live canonical frames. A staged initial append that never attempts Write releases its capture; an attempted write requires proof of zero bytes. Ordinary replay retains the epoch, while completing prepared reclamation advances it. Fresh post-reclaim admissions remain possible.
- Experiment cache fences, emissions, progress, mint fallback and lifecycle matching share the journal owner's confirmed namespace, separately from conservative condemnation. Regressions pair independent case-distinct directories with canonical and confirmed-alias controls. Permitted initialized consumers establish the ordinary cache root before binding; closed gates and flag-off consumers create no directory or journal. Assignment clear serializes its availability check, deletion and corrupt-marker cleanup with sibling publication.
- When both lifecycle identities remain unbound, a matching configured namespace and tenant scope can revoke the enabled predecessor without I/O or directory creation. This pre-binding match grants no cache or projection authority; independent directories and tenants remain independent.
- `0.14.0-alpha` is the current release candidate; its changes are recorded under that version in the CHANGELOG. The owner publishes its tag after the version bump merges.
- Before that tag is published, use the local-package flow for this candidate. The public API is unstable before v1.
- Implemented and tested: app-first event tracking, sessions, bounded async batching over UnityWebRequest, durable offline spools (analytics events AND crash reports) with resend on next launch, bearer auth, consent-first analytics gating, an independent persisted crash-reporting opt-out, sanitized crash capture, and an ETag/If-None-Match remote-config disk cache.
- `IngestUrl` defaults to `https://ingest.shardpilot.com` and a build that omits it uses that. The IDENTIFIERS and KEYS in the samples below are placeholders: `WorkspaceKey`, `AppKey`, `EnvironmentKey` and `PublishableKey` come from your ShardPilot workspace and resolve to nothing until you supply your own.
- CI runs gitleaks (working tree and all history), package/asmdef JSON validation, and conditional `dotnet format`. The Unity Test Runner jobs (EditMode + PlayMode via GameCI) are skipped unless `UNITY_LICENSE`, `UNITY_EMAIL`, and `UNITY_PASSWORD` repository secrets are present.

## What it does

- Tracks app-first events (`Track`) and sessions (`SessionStart`) and ships them as batches to `POST {IngestUrl}/v1/events:batch`.
- Buffers events in a bounded in-memory queue, drained by a single SDK worker on a flush cadence; supports explicit `FlushAsync` and `CloseAsync`.
- Persists undeliverable grant-admitted events to a durable on-disk spool and re-sends them on the next launch, de-duplicated by the ingest service via the stable `event_id` — see [Offline durability](#offline-durability).
- Captures sanitized crashes from Unity log exceptions and unhandled CLR exceptions and ships them to a dedicated crash ingest endpoint (`POST {CrashUrl}/api/v1/crashes/ingest`), separate from analytics events, as a crash report JSON body stamped with the component `component` slug; fatal crashes are never sampled away. Every report is persisted to a durable crash spool at capture time and re-sent on the next launch until the crash ingest service acknowledges it — see [Crash capture](#crash-capture).
- Gates analytics **consent-first**: ordinary analytics requires an explicit `SetConsent(true)` or a current recorded non-objection basis (memory-only); the persisted tri-state decision posts a receipt to `POST {IngestUrl}/v1/consent` through a durable outbox (retried until acknowledged, delivered in decision order **per actor** — dispatch skips receipts parked for an actor this session cannot vouch for, so a newer receipt for the current actor can land before an older parked one; see [Authentication](#authentication-dual-mode)). Crash reporting is an independent legitimate-interest plane — on by default, persisted opt-out via `SetCrashReportingEnabled(false)`.
- Fetches remote config from the remote-config endpoint with an on-disk ETag cache that serves stale snapshots on `304`/transient failure, fails closed on `401`/`403`, and fails permanently (no stale serving) on other unexpected statuses.

## Installation

After `v0.14.0-alpha` is published, add it as a UPM git dependency in your project's Packages/manifest.json:

```json
{
  "dependencies": {
    "com.shardpilot.unity": "https://github.com/shardpilot/shardpilot-unity.git#v0.14.0-alpha"
  }
}
```

The snippet targets the `v0.14.0-alpha` release tag. Until it is published, add the package folder through Unity Package Manager's local-package flow. Requires Unity `2022.3` or later.

## Quick start

This quickstart uses the `0.14.0-alpha` scope API: `WorkspaceKey`,
`AppKey` and `EnvironmentKey`. The earlier `v0.11.0-alpha` tag uses `*Id`
settings; current source has no aliases for those retired property names.

A complete `MonoBehaviour`. The block below is a **verbatim copy** of the `Basic Telemetry` sample (`Samples~/BasicTelemetry/ShardPilotQuickstart.cs`), so importing the UPM sample and copying this snippet give you the same code:

```csharp
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
```

`Initialize` must run on the Unity main thread (it resolves persisted anonymous-id and consent state through PlayerPrefs). `Track` is safe to call from any thread. Keep a non-real `sp_` placeholder in source control and load production keys from your normal secret delivery path.

### Android capture opt-ins

These default-off opt-ins are included in `0.14.0-alpha`. Until its tag is
published, use UPM's local-package flow with the release candidate.
Add them to the initializer above only after configuring crash credentials and
an eligible age/consent decision (see [Crash capture](#crash-capture)):

```csharp
NativeCrashExitInfoEnabled = true,
AnrExitInfoEnabled = true,
```

### Per-event schema version

Ordinary events default to schema version 1. Select an explicit integer from
1 through 65535 with `ShardPilotEvent.SchemaVersion` for direct `Enqueue`, or
with the required third argument to facade/client `Track`:

```csharp
var result = ShardPilotSdk.Track("level_finished", props,
    new ShardPilotTrackOptions { SchemaVersion = 2 });
```

Null options or a null member selects 1. The new overload returns
`ShardPilotResult`; invalid versions refuse before admission with nonretryable
`schema_version_invalid` in `Error`. Direct `Enqueue` reports the same code in
`Snapshot().LastError`. Existing two-argument calls still return bool. Each
accepted event snapshots its integer for mixed batches, retry and newly written
spool replay; changing the caller's option afterwards cannot change it.
`Supports("event_schema_version")` is true before initialization.

SDK-owned producers retain their contract versions; sealed experiment facts
remain version 2. This option does not permit a reserved event name, change a
schema-revision declaration/header, negotiate a latest version or retry with a
different version. Other existing boolean-only admission failures use
`event_rejected` in the new result when no more specific per-call cause exists.

### Flush

`FlushAsync(timeout)` (default 10 s) answers for the whole drain, not for one
request. A retryable failure (no response, `408`, `429`, `5xx`) keeps it waiting while
the worker retries under its normal backoff, honouring a server `Retry-After`.

`v0.12.0-alpha` also includes HTTP `408` retries on consent receipts and crash
writes. Event retries retain their original envelopes; consent retries retain
the original decision and idempotency key, and a pending grant keeps event
batches gated. Crash capture and replay keep the original durable report until
acknowledgment. Existing retry pacing, retention limits, privacy gates and
credential-specific terminal refusals continue to apply.

`FlushAsync` answers when everything queued before the call is delivered, or when the
deadline passes. On failure, `ShardPilotResult.Error` holds one of the
`ShardPilotFlushCodes`:

| Code | Retryable | Meaning |
|---|---|---|
| `pending` | yes | The deadline passed with work still undelivered: events, or an experiment application not yet admitted. The client keeps retrying in the background. |
| `consent_receipt_pending` | yes | The deadline passed while events waited behind a consent receipt that has not been delivered yet. |
| `shutdown` | no | The client is closed, or closed before the flush's work was delivered. A flush whose work a close delivers answers success. |
| `not_initialized` | no | `ShardPilotSdk.FlushAsync` was called with no client. |

A terminal ingest rejection answers at once with its own message and `Retryable`
false. Events the client removes on its own, such as the purge on a consent
withdrawal or an expired retry age, count as delivered. `spool_persist_failed`
belongs to `CloseAsync`; a flush never answers it. `CloseAsync` reports its own
results; an experiment application still unadmitted at close answers `pending`.

### Shutdown

When Unity calls `OnApplicationQuit`, each registered live client ends its active
session with `app.session_ended` and reason `app_final`, then synchronously attempts
to persist the pending tail through the event spool. A background session whose
timeout has already elapsed keeps its earlier `idle_timeout` boundary. No session
is created just to end it, and a repeated quit callback does not end the same
session twice. The end follows the normal consent, buffer, and spool limits;
disabled/unavailable spooling or a refused disk write still means it may be lost.
`CloseAsync` ends the session the same way (see below); between them a session
ends once, and a pause does not end it. Platforms that terminate the process
without a quit callback cannot emit this boundary.

A shared consent decision already in progress when `CloseAsync` starts belongs
to the same shutdown budget, including its persistence and sibling propagation.
The accepted local state is recorded before synchronous cleanup can wait.
Close returns its task without waiting on the consent lock on the calling
thread; it stops admitting new events immediately and holds the final batch
while that decision is unfinished. When the decision converges, the existing
drain continues. If the budget expires first, close reports `spool_persist_failed`
and the consent shutdown diagnostic below; an accepted denial stays effective
for a same-store, same-actor successor in the process. Later close callers receive
the first caller's task and cannot restart its budget. The asynchronous wait
cannot cancel a synchronous host storage callback that never returns.

`CloseAsync(timeout)` (default 10 s) ends the open session first, in the same
step that stops accepting events: one `app.session_ended` with reason
`app_final`, or with `idle_timeout` at the deadline when a background pause has
already expired. The end rides the close's final drain. With no open session it
sends nothing. A session start still waiting for its spool write on another
thread is waited for, but never past the close's deadline; past it the close
ends nothing and records `close_session_end_unresolved` in
`Snapshot().LastError`. The close then drains what is queued, retrying a
retryable failure once, and answers:

| Result | Retryable | Meaning |
|---|---|---|
| success | | Everything queued before the close was delivered. |
| `pending` | yes | The deadline passed, or the final drain ended with a retryable failure. The undelivered remainder was written to the spool before the task resolved. |
| `consent_receipt_pending` | yes | The deadline passed while the remaining events waited behind a consent receipt not yet delivered. They are in the spool. |
| `spool_persist_failed` | no | Something the close owed could not be made durable by its deadline: undelivered events with no spool (`SpoolEnabled = false`, or WebGL) or a failed spool write, or a consent write storage still refused. |
| a terminal ingest rejection | no | A rejection recorded for this close's drain is kept across later retryable failures. At the deadline, the client chooses from rejections recorded before that decision. Later responses cannot change the completed result. |

No close keeps the client running past its deadline. While a consent write is
still owed, or the remaining events wait behind a consent receipt, the close
keeps retrying and delivering until that clears or the deadline passes; either
way the client then stops. A consent write lost this way is counted in
`Snapshot().ConsentWritesUnpersistedAtShutdown` and named
`consent_unpersisted_at_shutdown` in `Snapshot().LastError`. A denial among
them stays in force for the rest of the process: a later client on the same
storage and identity starts denied and retries the write.

An expired background end uses the earlier of the timeout deadline and current
wall time, then raises that timestamp to the session's latest accepted event if
needed. This prevents a backward clock correction from putting the deadline in
the future while preserving event order when accepted activity itself is later
than the corrected clock.

`SessionEnd(reason)` applies that same expired-pause boundary before a manual
end: it emits one `idle_timeout` end and opens no replacement session. With no
expired pause it uses the caller's reason, or `session_end` for a missing or
empty reason. With no active session, including a second end call, it emits
nothing and returns `true`. Otherwise its return value says whether the end event
was accepted for delivery; a closed client returns `false`.

`SessionStart()` replaces a session that is still open, including one opened
lazily by an earlier event. It first ends that session with reason
`session_start`, or with `idle_timeout` at the deadline when its background pause
has already expired, and then starts the new one. The end is admitted together
with the start: when the start is refused, no end is sent and the open session,
with its pending background deadline, stays as it was. Two things can happen
while a refused start is being written to the spool. If the app resumes after
the session's deadline, the session still ends at that deadline and a new one
starts, as the resume would have done. If another event is accepted into the new
session, the new session stays, and the replaced session still ends.

Analytics consent loss clears the current session and its pending background
deadline locally, without sending an end. After a later grant, the next activity
or resume announces a fresh session with `app.session_started`; it cannot
continue the pre-denial session or emit its retained timeout end. An end already
claimed before consent loss is also refused if it reaches admission after that
loss, even if consent has since been granted again. A client that had no session
or pending start before denial does not create one merely on resume.

**A quickstart owes no shutdown code.** ShardPilot installs its own lifecycle host and, from Unity's quit and pause callbacks, **synchronously retries the writes it still owes** — the consent receipt, and pending events *where the event spool is available*. Retries, not guarantees: it is one more attempt, so a store that is still refusing writes leaves the receipt in memory and quitting destroys it. Every `SetConsent` overload returns `ShardPilotResult`: `Success` reports whether the decision applied, `Error` names a refusal, and `Warnings` names unfinished persistence, purge or receipt work. The additional `out error` overload retains a diagnostic code which names either a refusal or the first warning, with durability warnings before actor validation; applied decisions keep `Error` null. Use the result fields to distinguish them. If losing a consent decision is unacceptable for your game, observe that return value rather than the shutdown path. That covers an ordinary exit without you writing anything, with one real limit: `PersistPendingEvents` has nowhere to write when the spool is off (`SpoolEnabled = false`) or unavailable (WebGL, where the spool is a documented no-op), so in those configurations a reported quit still loses the accepted in-memory event tail. On WebGL in particular, flush before you quit. Do not put `CloseAsync` in `OnApplicationQuit`: that callback returns `void`, so the first `await` inside it hands control back to Unity and the continuation may never run — a barrier there is not a barrier.

What the SDK's hook cannot do is the final **network** flush, because that needs the app to stay alive. Getting that is a real thing to want, and `Application.wantsToQuit` — the one hook that can *cancel* a quit — is where it goes. Before writing one, know that a correct barrier has to handle four things, each of which is a way to be worse than no barrier at all:

1. **Re-entrancy.** A second quit request arriving while the barrier runs must be vetoed too, not allowed to start a second barrier. The facade's `CloseAsync` detaches the client synchronously, so a concurrent second call returns the non-retryable `not initialized` immediately — and a handler that reads that as success will quit while the first close is still in flight. Track *in progress* separately from *complete*.
2. **A per-attempt timeout.** `CloseAsync()` with no argument uses the SDK's 10-second default. A wall-clock budget checked only *between* calls does not bound anything: pass the remaining budget as each call's timeout, or the barrier outlives the budget it advertises.
3. **The Editor.** `Application.Quit()` is a no-op there, so a handler that vetoes the Stop and then calls it leaves play mode running and demands a second press. Re-issue with `UnityEditor.EditorApplication.isPlaying = false` under `#if UNITY_EDITOR`.
4. **Giving up.** Every close answers by its own deadline and stops the client, so there is nothing to retry. A `spool_persist_failed` result means something could not be made durable: log it, then quit.

And be exact about what durability means here. A quit or pause that Unity *reports* is durable through the SDK's own hook, with or without your barrier. A termination that raises no callback at all — an OS task-kill, a `SIGKILL`, a hard crash — runs that hook no more than it runs yours: what is already spooled survives, and the in-memory tail since the last flush does not. No shutdown code changes that.

## AI-assisted integration

Integrating with an AI coding tool (Claude Code or similar)? This repository ships a machine-readable integration guide at [`.claude/skills/shardpilot-unity-integration/SKILL.md`](.claude/skills/shardpilot-unity-integration/SKILL.md): install, credentials, the consent-first contract, crash reporting, remote config, offline durability, and an integration-verification checklist, all verified against this SDK's source (CI checks the guide's install pin against `package.json`). Claude Code picks it up automatically when working in this repository; point other tools at the file directly. The ShardPilot docs site (`docs.shardpilot.com` — pre-launch, not yet live) will additionally publish an llms.txt family of machine-readable docs for the whole platform once it launches.

## Configuration

`ShardPilotConfig` requires authentication — **either** `PublishableKey` (Mode A) **or** `TokenProvider` (Mode B), see [Authentication](#authentication-dual-mode) — plus `WorkspaceKey`, `AppKey`, and `EnvironmentKey`. Set those properties to the literal workspace, app and environment keys used by analytics; these property names do not accept the retired `*Id` spellings. Other fields and their defaults:

| Field | Default | Notes |
|---|---|---|
| `PublishableKey` | — | **Mode A**: publishable `sp_…` key, sent verbatim as `Authorization: Bearer`. Supply this or `TokenProvider`. |
| `TokenProvider` | — | **Mode B**: `Func<CancellationToken, Task<ShardPilotTokenResult>>` returning a per-tenant JWT (+ optional `ExpiresAt`). Supply this or `PublishableKey`. |
| `TokenRefreshLead` | `60s` | Mode B only: lead before a JWT's `ExpiresAt` at which the SDK proactively refreshes. |
| `IngestUrl` | `https://ingest.shardpilot.com` | Ingest base URL. Validated: absolute, https outside loopback/private nets, no path/query/fragment. |
| `AppUrl` | empty | App origin for config and experiments; an address does not enable remote config. |
| `RemoteConfigEnabled` | `false` | Enable explicit config fetches; requires `AppUrl` and `PublishableKey`. |
| `CrashHooksEnabled` | `true` | Install automatic managed-exception hooks during initialization. |
| `NativeCrashExitInfoEnabled` | `false` | **`0.14.0-alpha`.** Android next-launch native/IL2CPP crash capture; API 30+, binary tombstone streams on API 31+. |
| `AnrExitInfoEnabled` | `false` | **`0.14.0-alpha`.** Android next-launch reports after a system-killed ANR; API 30+. |
| `Source` | `client` | Analytics-envelope origin: `client` / `server` / `backend`. Distinct from `CrashComponent`. |
| `CrashUrl` | empty | Separate crash ingest origin, without a route path, query or fragment. Required when `CrashKey` is set; never falls back to `IngestUrl`. |
| `CrashKey` | — | The `crash:write` Bearer for crash ingest (a different scope from `PublishableKey`/the Mode-B JWT). **When empty, crash capture is disabled** (`CaptureCrash` is a safe no-op). |
| `CrashComponent` | empty (bare app) | The component slug stamped on every crash report's top-level `component`: which component/service within your app a crash came from (e.g. `game-client`, `main-server`). Validated as a lowercase DNS-style slug `^[a-z0-9][a-z0-9-]{0,62}$`. Empty = no component dimension. |
| `CrashSampleEvery` | `10` | Send every N-th **non-fatal** crash report — a deterministic per-client counter transmits that client's calls N, 2N, 3N, … (the cross-SDK canonical shared with the Go and Defold SDKs; set `1` to send every one, or `0` to send no non-fatal reports). ONE counter serves every non-fatal source — managed non-fatals, ANR watchdog stalls, anything else — so the rate is one in N of the client's non-fatal reports in aggregate, never one in N of any single kind. The counter is an instance field on `ShardPilotClient`, and `Initialize` constructs a fresh client — so a re-initialization restarts the count from zero. The first N−1 non-fatals of each client instance are always dropped, so a client emitting fewer than N non-fatals in its lifetime reports none. The `10` default is published as of `v0.7.0-alpha`; `v0.6.0-alpha` and earlier defaulted to `1` (send every non-fatal report). **Fatal crashes are never sampled** — a terminating crash is always sent (consistent across the ShardPilot SDKs). |
| `BatchSize` | `25` | Capped at `MaxBatchSize` = `100`. |
| `BufferSize` | `1000` | Bounded queue capacity. When full, new events overflow to the offline spool instead of being dropped — see [Offline durability](#offline-durability). |
| `FlushInterval` | `15s` | How long a PARTIAL batch waits before publishing (was `1s`). Not a heartbeat — an empty batch publishes nothing, so a game that tracks nothing makes no requests at either value. At `1s` a title emitting an event every few seconds never filled a batch, so every event became its own request; `15s` lets batching actually happen. A full `BatchSize` still publishes immediately and `FlushAsync()` on demand; consent-receipt retries and crash re-sends keep their own pacing (the worker caps its wait to those deadlines). A default, not a floor: an explicit value below it is honoured. Retry pacing is INDEPENDENT of this value: retryable publish failures honor the server's `Retry-After` at its own value, and without a hint the worker backs off with full-jitter exponential growth — every failure, the first included, waits at least 1s, with the ceiling doubling from the third consecutive failure up to 60s, reset on success — so an outage is probed with growing, de-synchronized spacing instead of a fixed-cadence retry storm. The full-jitter backoff is published as of `v0.7.0-alpha`; `v0.6.0-alpha` and earlier retried hint-less failures at the fixed flush cadence. |
| `HttpTimeout` | `2s` | Per-request timeout. |
| `RequestCompressionEnabled` **(new in `v0.7.1-alpha`)** | `true` | gzip-compress analytics batch bodies (`Content-Encoding: gzip`). A batch body is the same envelope keys repeated per event, so it compresses hard — measured on a 100-event batch: **28.7 KB → 2.1 KB on the wire, 13.5x**. Bodies under 1 KiB go uncompressed: gzip's 18 bytes of framing make a single-event batch bigger rather than smaller, and the CPU is spent either way. **The ingest body cap applies to the UNCOMPRESSED body**, so compression buys throughput and not headroom — `BatchSize` still governs how large a body gets. Scoped to the batch route; the consent write, the remote-config GET and the crash upload are untouched (the first is under the threshold anyway, the last is already-compressed multipart to a different service). Turning this off should rarely be needed — see the fallback below. |
| `SpoolEnabled` | `true` | Durable offline spool: persist undeliverable events and re-send them on the next launch — see [Offline durability](#offline-durability). No-op on WebGL. |
| `SpoolMaxEvents` | `2000` | Hard cap on spooled events; oldest evicted first (FIFO). |
| `SpoolMaxBytes` | 1 MiB | Hard cap on the summed UTF-8 size of the spooled envelopes; oldest evicted first. Bounds the **backlog** — the file holding it may reach about 1.5x encoded live-record bytes plus header and 4 KiB (quoted sealed records can double their wire bytes, making the pre-compaction bound about 3x wire bytes plus framing), because an evicted entry's bytes are reclaimed by a later compaction rather than by rewriting the whole file on the append that evicted it (see [Offline durability](#offline-durability)). |
| `SpoolDir` | `Application.persistentDataPath` | Where the spool file lives (injectable for tests). The file name carries a hash of the (workspace, app, environment) scope. The crash spool file lives in this same directory. |
| `HangWatchdogEnabled` / `HangWatchdogThreshold` | `false` / `TimeSpan.FromSeconds(5)` | Android only: report RECOVERED main-thread stalls the OS never killed the app for (the exit-info lane cannot see those). Same dark posture. **Decided at `Initialize`** — the watchdog starts only if crash reporting is enabled at that moment, so an app opted out then, that later calls `SetCrashReportingEnabled(true)`, gets no watchdog until the next `Initialize`. **These reports are NON-fatal** — the session survived — so they ride `CrashSampleEvery` like any other. The counter is SHARED across every non-fatal source on the client, not per stall type: `ShouldEmitNonFatal()` increments one client-wide counter, so at the `10` default it is every tenth non-fatal REPORT of any kind that ships. A client that has already sent nine managed non-fatals emits its very first recovered stall; a client whose non-fatals are all stalls emits the tenth. Do not read the default as "one stall in ten" when sizing watchdog visibility. |
| `UserId` / `AnonymousId` (length) | ≤ 512 UTF-8 bytes | A host-supplied identifier over the limit is **rejected, never truncated** (truncating could collide distinct identities). An over-long `UserId` drops to empty and the actor falls back to the anonymous id; an over-long `AnonymousId` is discarded in favour of the persisted or a fresh one. A legacy oversized PERSISTED anonymous id self-heals to a fresh UUID and the spool it gated is purged — so that installation is detached from its server-side history and loses anything the old id had spooled. |
| `ConsentKindEmissionEnabled` | `true` | Send the consent decision's `kind` on the receipt. **Set `false` when the ingest deployment's consent-kind mode is still `off`** — an unknown field there is a terminal `400`, which permanently drops the receipt, denials included. |
| `CrashSpoolEnabled` | `true` | Durable crash-report spool: persist every captured report at capture time and re-send on the next launch — see [Crash capture](#crash-capture). Runs only when `SpoolEnabled` (the master durability switch) is also `true` and a `CrashKey` is configured. No-op on WebGL. |
| `CrashSpoolMaxReports` | `50` | Hard cap on spooled crash reports; oldest evicted first. |
| `CrashSpoolMaxBytes` | 512 KiB | Hard cap on the summed UTF-8 size of the spooled crash-report bodies; oldest evicted first. A single report larger than the whole cap is rejected outright. |
| `Platform` | auto-detected | Fresh analytics and crash reports use `windows`, `macos`, `linux`, `android`, `ios`, `tvos`, `web`, `ps4`, `ps5`, `xbox`, `switch` or `other`. Existing aliases (`Windows`, `WIN32`, `OSX`, `html5`, `browser`, `steamdeck`) fold to their canonical values. Empty or unrecognised wire values become `other`; an explicitly configured unrecognised value is reported once through `Logger` as `platform_unmapped`, without repeating the raw input. An event's non-empty platform overrides the configured platform. Native detection covers desktop/server, Windows Store, embedded Linux, TV and console families; an unmappable runtime uses `other`. Existing retained crash payloads keep their original bytes. |
| `AppVersion` | `Application.version` | Explicit nonempty values override the Unity application version. |
| `AppBuild` | unset | Maps to app-first `app_build`. |
| `AnonymousId` | auto-generated | See below. |
| `AllowInsecurePrivateNetwork` | `false` | Allows plain-http endpoints on private network ranges (local dev). |

### Request compression fallback

> **New in `v0.7.1-alpha`.** `ShardPilotRequestCompression` was absent from `v0.7.0-alpha`.

`RequestCompressionEnabled` is on by default, and the reason that is safe in an
SDK you point at your own deployment is that the client discovers a server or
edge that cannot carry the coding and stops using it — it never silently drops
batches.

Two failure shapes, because they need different evidence:

- **The server refuses the coding.** Ingest answers `400` with detail code
  `unsupported_content_encoding` (the deployment predates gzip acceptance) or
  `invalid_content_encoding` (the bytes arrived unreadable). Unambiguous:
  compression latches off for the session and the same batch is re-sent
  uncompressed. The match is on those detail codes, never on the bare `400` —
  an ordinary validation failure must not switch your transport.
- **The request never reaches the server (WebGL).** A browser preflights
  `Content-Encoding`; it is not CORS-safelisted. An edge whose
  `Access-Control-Allow-Headers` omits it fails the request as a **network
  error** — no status, no detail code, nothing to match on. So the first
  compressed publish of a session is probationary: a transport error there
  also triggers one uncompressed retry, and compression latches off only if
  that retry **succeeds**, which is the evidence that the coding rather than
  the network was the problem. Once a compressed publish has succeeded the
  coding is proven for that endpoint and later transport errors cost no extra
  request.

ShardPilot's own edge allows the header, so this matters for self-hosted or
proxied ingest. Set `RequestCompressionEnabled = false` to skip even the one
probing attempt.

### Authentication (dual-mode)

Two ingest credential kinds are supported; supply **exactly one** for ingest. If both are set, the Mode-B JWT wins for ingest (the `sp_` key is never sent there). **Consent picks its credential per receipt**, and since `v0.7.0-alpha` not always the JWT: a receipt keyed to a `UserId` rides the JWT; an ANON-keyed one is sent with the publishable key — *except* a GRANT for the client's current anonymous id when a `TokenProvider` is configured and no `UserId` is set, which the JWT vouches for better and so rides it. Note the direction: supplying an `PublishableKey` is what switches anon-keyed receipts — denials, and anything for a superseded anonymous id — onto the publishable key, so supply it only if that key is scoped to record them.

**Before the credential choice there is a PARKING gate**, and it is the reason "no `PublishableKey` means everything rides the JWT" is not the whole story. A retained receipt parks — kept, cap-counted, and skipped by dispatch — when no credential *this* session holds can vouch for its actor:

- an ANON-keyed receipt in a Mode-B session with no `PublishableKey` when a `UserId` is configured or the receipt names a different anonymous id (the current JWT cannot vouch for that earlier actor);
- a `user_verified` receipt whose actor is not the session's current `UserId`, or a session with no `TokenProvider` at all;
- any receipt of a kind this build does not recognize.
- a receipt for a different workspace, app or environment.

A parked receipt is retained. Actor-related parking needs a credential that can vouch for the receipt's actor; an unknown kind needs an SDK version that understands it. A foreign-scope receipt remains in its source outbox under the normal cap rules and is not dispatched. This SDK has no automatic cross-scope discovery or migration: a relaunch with a matching tenant configuration reads a different key and does not find or send that receipt. Changing authentication alone does not release an unknown kind or a foreign scope.

Only receipts for the current workspace, app and environment back that scope's pending grant, receipt ordering and dispatch gate. Foreign receipts keep their metadata and cannot block the current scope's analytics batch as pending grants. The SDK still uses eligible same-actor denial evidence across scopes and advances shared decision ordering from all retained evidence.

If an outbox contains malformed content, maintenance preserves its original bytes, including after an intact receipt is acknowledged or terminally rejected. A fresh explicit consent decision can replace the damaged record while retaining individually valid pending receipts. A store read failure first requires a successful reread so replacement cannot overwrite unread evidence.

- **Mode A — publishable key.** `PublishableKey` must start with `sp_` and is sent verbatim as `Authorization: Bearer <key>`. Safe to embed client-side; never expires.
- **Mode B — per-tenant JWT.** `TokenProvider` is an async delegate the SDK calls to obtain a per-tenant HS256 JWT (the host fetches/refreshes it from its **own** backend; the SDK never holds the signing secret). The SDK invokes `TokenProvider` on the initializing host’s main thread; the delegate must return a Task promptly and do its I/O asynchronously. Its asynchronous answer is cached and refreshed proactively a `TokenRefreshLead` (default 60s) before `ExpiresAt` (or on a `401` if no `ExpiresAt` is returned). A `401` is **retryable** in Mode B (drop the stale token, fetch a fresh one, retry the batch), and only a single refresh is ever in flight.

The existing 30-second provider deadline includes time waiting for the main-thread
host pump. An expired queued call never invokes the provider later. Retirement
abandons queued and running provider waits and signals cooperative cancellation;
a late answer cannot revive a retired producing host or its successor. One caller
cancelling its wait still leaves a shared refresh available to other callers.
The SDK cannot preempt synchronous user code inside a provider. Other callback
and public-task completion threading remains unchanged in this cut.

```csharp
ShardPilotSdk.Initialize(new ShardPilotConfig {
    TokenProvider = async ct => new ShardPilotTokenResult(
        await myBackend.MintShardPilotTokenAsync(ct),
        DateTimeOffset.UtcNow.AddMinutes(15)), // ExpiresAt enables proactive refresh
    WorkspaceKey = "workspace_example", AppKey = "app_example", EnvironmentKey = "develop",
});
```

> **Remote config is the exception.** Remote config is a separate endpoint authenticated by the `sp_` publishable key; the Mode-B ingest JWT (scoped `analytics:ingest`) cannot authenticate it. So if you enable remote config, experiments or targeting attributes while using Mode B, you must **also** supply a valid `sp_` `PublishableKey` — the one case where both credentials are required (ingest uses the JWT; remote config uses the `sp_` key).

`anonymous_id` is generated, persisted, and sent on every event in **both** modes; the SDK never rotates it mid-session.

### Three distinct services

The SDK talks to three different base URLs — do not conflate them:

- **`IngestUrl`** is the **analytics ingest** base URL. Event batches (`POST /v1/events:batch`) and consent receipts (`POST /v1/consent`) go here, authenticated by `PublishableKey`/the Mode-B JWT.
- **`CrashUrl`** is the **crash ingest** origin. Supply an absolute origin without a route path, query or fragment; the SDK appends `/api/v1/crashes/ingest` for JSON reports. HTTPS is required except for loopback addresses or, when `AllowInsecurePrivateNetwork` is enabled, private-network addresses. Requests use the `crash:write` `CrashKey`. A crash is **not** wrapped as an analytics event. Supply `CrashUrl` explicitly when `CrashKey` is set; an absent crash URL is a configuration error. The same endpoint-validation rules as `IngestUrl` apply.
- **`AppUrl`** is the **remote-config and experiments** base URL. Remote-config fetches (`GET /config/v1/...`), experiment assignments and explicit application requests go here with the publishable key. Pointing it at `IngestUrl` is wrong.

Local test sinks remain supported deliberately: the shared origin validation
retains its loopback and explicitly enabled private-network HTTP exceptions.
The packaged smoke sample uses reserved example origins; replace them with your
own test sinks before running it.

## Wire contract

New analytics intake accepts at most 2,048 UTF-8 bytes per complete event JSON
object (including metadata; equality is allowed). An oversized event returns
`false`, increments `Dropped` once, and sets `Snapshot().LastError` to
`event_too_large` with the byte count; typed producers also return that code
through `out error`. The schema-version `Track` overload returns the same code
in `Error`. The event never enters the queue or spool. The admitted JSON is
reused for sending and persistence, including after nested caller data changes.
Server rejection records and Flush/Close rejection counts remain separate.
Retired schema-1 experiment facts follow ingest's schema-version refusal instead.

Custom `Track` and public `Enqueue` names trim leading and trailing ASCII space,
tab, carriage return, line feed, vertical tab and form feed. Empty names return
`false` with `event_name_required`; reserved names return `false` with
`reserved_event_name` in `Snapshot().LastError`. Each refusal counts one dropped
event without queueing, spooling, sending or counting a failed batch. Existing
initialization and consent gates retain their precedence.

The reserved names are `experiment_exposure`, `experiment_outcome`,
`governed_action_result`, `app_runtime_ping`, `monitor_evaluation_recorded`,
`llm_usage`, `support_assistant_feedback`, `ad_impression_revenue`,
`session_started` and `session_ended`, including each with one `app.` prefix.
The comparison is case-sensitive; other characters are preserved. Use the
typed ad and session methods for the reserved events they produce. Typed and
internal producers retain their existing validation and consent rules.

The ordinary app-first event envelope, consistent across the ShardPilot SDKs
(sealed experiment facts use the [schema-v2 contract below](#sealed-experiment-facts)):

- `POST {IngestUrl}/v1/events:batch`, `Content-Type: application/json`, `Authorization: Bearer <sp_ key OR per-tenant JWT>` (dual-mode).
- App-first envelopes carrying `workspace_key`, `app_key`, `environment_key` (plus `event_id`, `session_id`, `session_sequence`, `event_name`, `source`, `app_version`, `app_build`, `anonymous_id`, etc.).
- Idempotency by UUIDv7 `event_id`; default batch `25`, max `100`.
- `Enqueue` trims caller-supplied event IDs and sends valid UUIDv7 IDs in
  lowercase without changing the caller's event object. An omitted ID is
  generated as UUIDv7; other versions, including UUIDv4 and UUIDv5, are refused
  locally. The accepted ID is preserved across retries and spool replay.
- JSON is hand-rolled (`ShardPilotJson` writer + a minimal `ShardPilotJsonParser` for remote-config payloads) — no third-party JSON dependency.

Crash reports ride a **separate** wire contract on a dedicated crash endpoint, separate from analytics events: `POST {CrashUrl}/api/v1/crashes/ingest`, `Authorization: Bearer <crash:write key>`, body = the crash report JSON (top-level `crash_id`, `occurred_at`, `app{…}`, the component `component` slug, `platform`, `os{…}`, `device`, `exception{…}`, pre-symbolicated `threads[]/frames[]`, `breadcrumbs[]`, and the hashed-at-ingest `anonymous_id`/`session_id`). They are **not** wrapped as `mobile_crash` analytics events on `/v1/events:batch`.

The SDK does **not** emit banned legacy envelope fields (`project_id`, `game_id`, `env`, `event_ts_server`, `event_seq_session`, or a top-level `build_version`). It emits canonical wire event names while keeping public helper names stable:

| Helper / usage | Wire `event_name` |
|---|---|
| `SessionStart()` | `app.session_started` |
| screen views (via `Track`) | `app.screen_view` |
| `TrackLevelStart(levelId, attempt)` | `level_start` |
| `TrackLevelComplete(levelId, attempt, durationMs[, score])` | `level_complete` |
| `TrackLevelFail(levelId, attempt, durationMs[, failReason])` | `level_fail` |
| `TrackAdImpressionRevenue(args)` | `ad_impression_revenue` |

Supply ad revenue as named fields on `ShardPilotAdImpressionRevenueArgs`:

```csharp
var impression = new ShardPilotAdImpressionRevenueArgs
{
    ImpressionId = "impression-42",
    Network = "admob",
    RevenueMicros = 4200,
    Currency = "USD",
    Placement = "main-menu"
};
var accepted = ShardPilotSdk.TrackAdImpressionRevenue(impression);
```

`ImpressionId`, `Network`, `RevenueMicros` and `Currency` are required;
zero revenue is valid. `RevenuePrecision`, `AdUnit`, `AdFormat` and `Placement`
are optional and omitted when empty. The client also offers
`TrackAdImpressionRevenue(args, out string error)` for local refusal details.
The value is copied at the call, and the canonical event fields and consent
gates are unchanged. Positional overloads have been removed.

## Diagnostics

`ShardPilotConfig.LogLevel` sets the minimum severity: `Debug`, `Info`,
`Warn` (the default), `Error`, or `Off`. `Off` silences both custom and platform
sinks without clearing rejection history or changing delivery/counters.
Without `Logger`, warnings use `Debug.LogWarning`, errors use `Debug.LogError`,
and lower levels use `Debug.Log`. Failed batches are errors; rejected events
are warnings; other per-event outcomes are info.

Set `Logger` to replace the platform sink. The SDK formats and redacts the
message before calling the level-aware method:

```csharp
sealed class GameLogger : IShardPilotLogger
{
    public void Log(ShardPilotLogLevel level, string message)
        => System.Console.WriteLine($"{level}: {message}");
}
```

This API is available in current source. Migrate custom sinks from
`Printf(string, params object[])` to `Log(ShardPilotLogLevel, string)`; no alias
remains. Log callbacks may run on SDK workers, so custom sinks must be thread-safe.
Set `LogLevel = ShardPilotLogLevel.Info` to observe non-rejection verdicts.

## Batch verdicts

A `409` response with `error.code = schema_revision_mismatch` is a terminal
batch refusal and is never retried. The SDK declares no schema revision; inspect
the receiving service's compatibility policy if this diagnostic appears.

**`0.14.0-alpha`:** read `client.Snapshot().LastEventIssue` for the most recent non-accepted
per-event verdict, including suppression, observation, rejection and duplicate
outcomes. It is `status` or `status:code`, with credentials redacted, and is
available even with logging off. It starts empty for each client; accepted-only
batches leave the previous value intact, and earlier snapshots keep their value.
For example, an ingest suppression can report `suppressed_ad_revenue_consent`
despite a successful flush. This diagnostic history does not change delivery,
retry decisions or flush success, and it contains no event ID or server message.

A `202` batch response is a transport completion, not confirmation that every event was accepted. The SDK retains each rejected event's ID, status, code, and message in an in-memory rejection ring (default 64 entries, configurable, oldest evicted first), while the rejection counter remains cumulative. A configured logger or observer handles diagnostics; otherwise the default channel warns for the first ten rejected events, then once per previously unseen code, retaining at most 64 code keys to bound both memory and session output. Logging limits never suppress the ring. Terminal rejections leave the retry spool and are not resent; accepted siblings settle once. Where a flush result is available, inspect its rejected count and retained entries even when transport completion succeeded. Use the public ring accessor for automatic publishes too. The ring is diagnostic history for this client instance, not durable storage or a retry queue; restarting the client clears it.

A pending flush reports rejections only for events admitted before its call; later
events cannot add to its count or evict its retained details. A pending close also
includes its owned application drain and final session-end admission, starting
when the public close begins. Each result retains at most
`RejectionCapacity` entries independently of the global history, while its
`Rejected` count includes its own entries that were evicted.

SDK logs and retained rejection/statistics diagnostics redact configured
publishable and crash keys. Batch and consent diagnostics also redact the bearer
owned by that operation, including late responses after token rotation.
Redaction covers the fully formatted message and stored diagnostic fields;
bounded warning-code history keeps hashes. Retry decisions and rejection
counters use the original transport verdicts.

In Unity, set `ShardPilotConfig.RejectionCapacity` (non-positive values use 64),
read `client.LastRejections()` or `ShardPilotSdk.LastRejections()`, and inspect
`ShardPilotResult.Rejected` / `Rejections` after `FlushAsync` or `CloseAsync`.
The result counts rejections observed while that operation was pending; its
entry snapshot is bounded by the same capacity and may therefore contain fewer
entries than the count. A later empty flush does not repeat historical rejections.
Snapshots are immutable and ordered oldest first. `Snapshot().Rejected` counts
all rejected events for the client. Without `Logger`, warnings use
`Debug.LogWarning`; the first-ten/64-code rule emits at most 73 warnings per
client session. A throwing custom logger cannot erase records or turn a parsed
202 into a retry. The level filter applies to these warnings too.

See the [batch verdict contract](https://docs.shardpilot.com/api/ingest/#batch-verdicts)
and its dated 2026-09-11 example: a 3374-byte event exceeded that test workspace's
configured 2048-byte limit, producing one rejected event while its sibling was
accepted. The deployment's configured limit applies; the SDK does not retry an
unchanged `event_too_large` event.

## Identity & consent

**Preview:** regional consent preparation validates replies while every decision remains strict and unused.

`ShardPilotSdk.PrepareConsentPolicy(context, callback)` is available before
`Initialize`. Call it on Unity's main thread. Supply a
`ShardPilotConsentPolicyContext` with `AppUrl`,
`WorkspaceKey`, `AppKey`, `EnvironmentKey`, `AppVersion`, `Locale` and `Platform`.
It posts an identifier-free version 3 request to `{AppUrl}/api/cp/v1/consent/policy`.
The request freezes those values, uses a two-second deadline and refuses redirects.
Use an HTTPS origin; HTTP is accepted only for loopback hosts.

Optional `Store` is exactly `steam`, `apple`, `google_play` or `standalone`;
`StoreRegion` must be null. Optional `AgeBand` has only `Vocabulary` and `Band`.
`Advisory = true` asks for the advisory part; false and absent options are omitted.
Scope keys and app version are bounded to 64 UTF-8 bytes, locale to 35,
age-band members to 32 and the origin to 256. App version permits only ASCII
letters, digits, `.`, `_`, `+`, `/` and `-`. Platform aliases use the event
vocabulary, with unknown spellings sent as `other`.

The host receives one immutable local `STRICT_OPT_IN` decision: crash capture off,
server analytics denied, minimised child rules and explicit grant required.
`PlanUsed` is false; null `OperationBlocks` means unknown, with source `none`.
Every response in `0.14.0-alpha` takes this strict path; received policies are not adopted.

Replies are byte-bounded and checked for duplicate and unknown fields, required
values, container shapes, scope and expiry. A valid unsigned reply returns
`Reason = "plan_unsigned"`; known resolver refusal reasons remain distinct.
A requested, fully validated advisory is exposed through nullable `decision.Advisory`
as detached, immutable reference data, including its matrix version. Its nullable
`Estimate` is informational; it never changes the strict decision or grants consent.
Absent, refused, invalid or fenced replies carry no advisory. References are never
cached or reused across requests. `Supports("consent_policy_advisory")` detects this
preview surface; it does not promise non-objection recording or analytics admission.
Available signals omit reasons; unavailable signals carry a known reason.
Refusals cannot carry advisory data. A known advisory row may have no estimate;
a resolved country without its own row may use `OTHER`.
A non-objection declaration is separate host data: `eligible_adult`, four explicit
true attestations, advisory matrix/row/jurisdiction and notice provenance. Its closed
shape rejects caller-supplied actor, scope and receipt fields. Capturing this data
grants no consent or analytics permission and performs no recording request.
Use `client.RecordNonObjection(record, callback)` or the facade
equivalent. Pass an `IDictionary<string, object>` with exactly these host fields:

| Field or nested path | Value |
| --- | --- |
| `advisory` | Map with `matrix`, `row_id` and two-letter uppercase `jurisdiction` from the validated advisory. |
| `advisory.matrix` | Map with `docs_commit`, `file_sha256` and `date` copied from its matrix. |
| `age_evidence_class` | `"eligible_adult"`. |
| `attestation` | Map with `notice_provided`, `free_simple_objection_provided`, `did_not_object` and `eligible`, each explicitly `true`. |
| `notice_version`, `notice_locale`, `policy_version` | Identifiers for the notice actually presented and its locale; never notice text. |

Do not infer these attestations from an advisory estimate or fabricate matrix provenance.
These APIs submit one closed declaration to the ingest consent endpoint, using
SDK-owned scope, actor, decision time and idempotency fields. A configured token
provider takes precedence, with no publishable fallback after a failed mint. Only
a provider-backed user identifier uses `user_verified`; other records use the anonymous actor.
The callback receives a `ShardPilotResult` once within `HttpTimeout`, including retry
classification and applicable `RetryAfter`. It may run off the Unity host thread.
Receipts retain their exact body, decision time and idempotency key for paced retries.
Restored or expired work stays parked until a fresh matching host attestation;
healing a failed storage write alone never activates it. Capacity protects denial
receipts and durable grant backing. Without durable storage, shutdown retires basis
work with a `shutdown_undurable` diagnostic while preserving denial receipts.
A valid current recording ACK opens future ordinary analytics in the current process,
with `ConsentState` and stored consent still `Unknown`. Before ACK, events are refused.
Experiments, ad revenue and remote-config targeting require an explicit grant.
`Supports("analytics_non_objection")` detects this admission path.
Current eligibility is rechecked against the captured identity, scope and recording generation.
The latest host call supersedes any pending recording, whose callback completes
with `non_objection_superseded`.

Basis-created events remain memory-only after a later explicit grant. A new recording,
identity or scope change, or denial closes that episode; a later ACK cannot revive it.
A current episode may drain within the existing close bound. Undelivered memory-only
work does not survive a restart.

`InvalidateConsentPolicy()` fences pending replies when the host discards a
consent screen. Changing request context also fences earlier replies; the latest
request for the same context supersedes older requests. Preparation never initializes
the SDK, persists a policy, creates an identity or grants consent. Apply the host's
explicit choice separately through the existing consent API.

In current source, consent requests use `workspace_key`,
`app_key` and `environment_key` with the configured literal keys. The three
retired `*_id` wire roots are absent; actor/kind, decision provenance and
retry/idempotency behavior are unchanged. This wire change is included in `v0.12.0-alpha`.

- **Anonymous id:** when `AnonymousId` is omitted, `Initialize` loads or creates a stable UUIDv7 at PlayerPrefs key `shardpilot.anonymous_id` (file fallback of the same name under `Application.persistentDataPath` when PlayerPrefs throws). It is stamped on every event as `anonymous_id`, exposed via `ShardPilotClient.AnonymousId`, and carried in `X-ShardPilot-Anonymous-Id` for remote config only with granted analytics consent. Supplying `AnonymousId` skips load-and-create and leaves any persisted value untouched. Assignment rejects ASCII control characters (including CR/LF/NUL/HTAB and DEL) and leading/trailing spaces with `ShardPilotConfigException`, before replacing the previous value; identities are never trimmed into another identity. When `AnonymousId` is omitted, a stored value that fails these checks or the existing 512-byte limit is replaced with a fresh UUIDv7 and rewritten when the store is writable. Explicit unsafe settings still throw; valid stored identities are reused literally.
- **Consent (`SetConsent(bool analyticsGranted)`, or `SetConsent(ShardPilotConsentDecision)` for the age-gate-forced `DeniedForcedMinor` band) — CONSENT-FIRST:** tri-state, persisted at PlayerPrefs key `shardpilot.consent.analytics` (same file fallback). An explicit **`Granted`**, or the current recorded non-objection basis described above, opens ordinary analytics. `Unknown` without that current basis (never decided, or an unreadable persisted decision) refuses every event at enqueue exactly like `Denied` — the event is dropped (`Track` returns `false`), nothing is queued or spooled, and **zero analytics bytes leave the device**: no event batches while undecided without that basis. Ordinary consent receipts require explicit decisions; non-objection recording is the separate host-declared path above. From `v0.7.0-alpha` an explicit decision's receipt rides a **durable outbox** — persisted, retried until acknowledged and delivered in decision order — so a receipt survives an app that terminates or stays offline before the consent endpoint answers; it is no longer fire-and-forget. The refusal reason is surfaced as a distinct code via `ShardPilotStats.LastError`: `consent_unknown` vs `consent_denied`. `Denied` additionally clears the pending queue and the crash breadcrumb trail, discards an in-flight batch as dropped, purges the offline spools, and **suppresses crash capture** (the `anonymous_id` is the diagnostics-consent/erasure subject) — denied/pre-denial activity never appears in a later crash report or batch. A grant opens the pipeline for FUTURE events only; events refused before it are gone by design. A consent-state read failure fails closed end to end: it resolves to `Unknown` (no analytics batch traffic) and the offline spool is purged. Consent never rides the event envelope; receipts go to `POST {IngestUrl}/v1/consent` through that same durable outbox. Server-side, a publishable `PublishableKey` (Mode A) may record **denials only**: a denial receipt is accepted for the key's own scope, while a **grant** receipt is rejected `403` with detail code `consent_grant_requires_verified_credential` and dropped by policy (local consent state is unaffected) — grants are recorded server-side only through a trusted backend credential, so a server-side denial keeps that actor's events suppressed (`suppressed_no_consent`) until such a grant lands — for an ordinary denial, calling `SetConsent(true)` on the device may re-open the **local** pipeline after purge completes but never restores server-side consent. A forced-minor denial cannot be reversed by an ordinary grant. To reverse an ordinary denial, record the grant **from your backend**: post the consent receipt for the actor the SDK's own receipt was keyed on. In Mode A that is **always `anonymous_id`**, even when `UserId` is configured (v0.7's canonical keying), so a grant posted for `user_id` updates a different actor and leaves the anonymous denial as the server's last word. In Mode B it is the JWT's bound subject when one is configured — but a Mode-B client with no valid `UserId` records ANON-keyed receipts too, so restore those against `anonymous_id` as well to the same `POST {IngestUrl}/v1/consent` with a consent-write-capable **service** credential (a server-side ingest token — never ship one in a client build). Client-key consent writes consume ingest budget whether accepted or rejected. The **remote-config fetch** remains available while consent is `Unknown` or either denied flavor, using an identifier-free route with no targeting attributes or identified cache validator. Granted requests may carry the anonymous ID and, when opted in, targeting attributes. The dispatcher rechecks consent before creating the request; see [Remote config](#remote-config). Call from the Unity main thread.
- **Consent result and forced-minor restriction:** every client and facade setter returns `ShardPilotResult`. Refused decisions have `Success = false` and `Error`; applied decisions have `Success = true`, no refusal error, and any unfinished work in `Warnings`. Persistence warnings do not reopen analytics after denial. A grant from ordinary `Denied` is refused with `spool_purge_failed` until the owed purge succeeds. Only recorded per-plane purge debt is retried: completed denial sweeps are not repeated during a grant, and event-only debt preserves the independent crash backlog. A shared grant refused or held by one scope records scope debt before returning. A persisted marker keeps that scope gated across restart until its own grant receipt is durable, while other scopes retain their grants. A failed marker write is logged and cannot guarantee the refusal across a process exit. An ordinary grant from `DeniedForcedMinor` always fails with `consent_forced_minor`, even after storage recovers; an ordinary denial succeeds without changing that state or creating a replacement receipt. The restriction is stored independently of the latest shared consent decision and persists across restart after receipt acknowledgment and marker retirement. Replaced clients consult it before accepting ordinary decisions. An unreadable or malformed restriction refuses grants with `consent_restriction_unreadable` until storage is readable again; analytics remains closed. With an invalid or overlong configured actor, denial succeeds with `consent_actor_invalid` and no receipt, while grant fails with that code. The restriction also applies when startup, shared-state refresh or healed storage restores a grant: analytics stays `Unknown` while stored denials retain their state. An omitted `AnonymousId` remains omitted through repeated normalization and allows the SDK to resolve its usual anonymous actor; an explicit blank value is invalid. Inspect `ConsentState` for the current decision; before initialization it is `Unknown`.

- **Optional notice provenance:** both `ShardPilotSdk` and `ShardPilotClient` accept `SetConsent(bool analyticsGranted, ShardPilotConsentNotice? notice = null)` and `SetConsent(ShardPilotConsentDecision decision, ShardPilotConsentNotice? notice = null)`. Supply `new ShardPilotConsentNotice { NoticeVersion = "dialog-v2", NoticeLocale = "en-US", PolicyVersion = "privacy-v3" }` to record the identifiers shown for that decision. Omission keeps the existing behavior; a supplied empty struct is malformed. Version identifiers accept 1–64 ASCII characters from `A-Z`, `a-z`, `0-9`, `.`, `_`, `+`, `/`, `-`. Locale accepts 2–35 ASCII bytes of BCP 47 syntax, with duplicate variants and extension singletons refused; registry validity belongs to the server. A valid tuple is copied before return and retained unchanged in direct, propagated and late-scope receipts, retries and restart completion. The receipt sends all three top-level `notice_version`, `notice_locale`, `policy_version` fields or none; notice text is never accepted. A malformed grant fails with `Error = "consent_notice_invalid"` before applying or minting a receipt. A malformed ordinary or forced-minor denial still applies, omits the whole tuple and adds `consent_notice_invalid` to `Warnings`. Every simultaneous purge, state-persist, outbox-persist and actor warning remains in that collection. The forced-minor ordinary-denial no-op preserves its original receipt and tuple; notice cannot reverse an age restriction.

- **Crash-reporting opt-out (`SetCrashReportingEnabled(bool)` / `IsCrashReportingEnabled(out string reason)`):** the crash plane's own persisted setting (PlayerPrefs key `shardpilot.crash.enabled`), INDEPENDENT of analytics consent. Crash diagnostics run on a legitimate-interest basis — ON by default with no first-run decision. Disabling stops **collection as well as sending**: captures refuse before any report is prepared or persisted (`LastError = crash_disabled`), breadcrumbs are neither recorded nor retained (disabling clears the ring), and the pending backlog stops re-sending. The setting fails **closed**: an unreadable or malformed (non-boolean) persisted value disables the plane (`settings_read_failed`) instead of silently defaulting to sending; an absent record is the default (enabled), and an unreadable analytics-**consent** record also fails the crash plane closed (it may have carried a denial, whose crash-spool wipe is then applied) — that hold outranks a later `SetCrashReportingEnabled(true)` and lifts only on the next explicit `SetConsent` decision. The decision is process-wide immediately: it propagates to every live client (the persisted key is global; consent grants propagate the same way), and a disable synchronizes with any in-flight dispatch handoff before returning. A failed persist returns `false` (`LastError = crash_persist_failed`) while the in-memory decision still applies for the session — and a failed ENABLE is never readable by a later construction, which therefore stays fail-closed. Call from the Unity main thread.

## Offline durability

The bounded in-memory analytics queue is backed by a durable on-disk spool.
These rules cover grant-admitted envelopes, including sealed experiment facts.
Non-objection events remain memory-only, including after a later grant; overflow
cannot spill them to disk and shutdown cannot make them durable.
Raw experiment applications are memory-only until sealing and admission; a
pause/quit snapshot does not journal them (see [Experiments](#experiments-dark-by-default)).

On backgrounding, the dispatcher finishes its pause-subscriber notifications
before persisting registered clients. Accepted events enqueued during those
callbacks are included in that final snapshot, subject to the normal consent,
spool capacity and storage-failure limits. Resume does not take a snapshot.

- **What is spooled.** (a) A batch whose publish failed **transiently** (connection error, timeout, `408`, `429`, `5xx`) is persisted as insurance, together with a snapshot of the queued remnant — the normal in-memory retry continues unchanged; (b) the pending remnant (queued events plus the worker-held batch) is persisted when the app pauses or quits, and by `CloseAsync` when the final drain could not deliver — the persist completes before the returned close task resolves, even when the close times out mid-drain, so an awaited `CloseAsync` is a durable shutdown barrier in every outcome. A **permanently rejected** batch (non-retryable `4xx`) is never spooled, and a permanent rejection of a previously spooled batch removes it — it would fail forever.
- **Queue overflow spills to disk.** When the bounded in-memory queue is full (a sustained outage, typically), new events are appended to the spool instead of being dropped — **accepted** on the tracking call and written to disk by a background writer (see *Acceptance, not durability* below) — and are delivered on the **next launch** through the normal restore path (`ShardPilotStats.SpooledOverflow` counts them). Overflow never evicts: once the spool itself is full, further overflow events are dropped and the oldest backlog is preserved. Without a spool (disabled, WebGL), a full queue drops as before.
- **Resend on the next launch.** When the launch starts from a persisted grant, the spool is loaded at `Initialize` and its events are sent ahead of new events, chunked to `BatchSize`, through the normal publish path (consent checks included). Restored entries publish in **isolated batches**, never mixed with fresh events (which follow one flush cycle later): if a restored envelope is permanently rejected, only its own batch is dropped. Entries are removed only once the ingest service **acknowledged** the batch (`2xx`); a transient failure keeps them for the next attempt or launch.
- **De-duplicated re-sends.** The spool stores the original envelope JSON and preserves its stable UUIDv7 `event_id`, allowing ingest to de-duplicate a delivery whose acknowledgment was lost. Valid envelopes replay byte for byte. On load, compatibility handling normalizes old platform values and drops obsolete `session_start`/`session_end` envelopes; their unregistered names would reject the entire restored batch, including valid siblings. The retained backlog is reconciled to disk.
- **Bounded analytics retry age.** Analytics expires seven days after SDK intake or 15 minutes after the first attempt whose delivery is uncertain, matching ingest's default deduplication retention. Offline time before an attempt does not consume the shorter window. A proven pre-send failure keeps the event eligible; an unknown transport failure is ambiguous, and later failures never renew an earlier ambiguous attempt. Eligibility is checked at restore, handoff and the owned transport's final submission gate. Host `TimestampUtc` does not set or renew either local clock. Expiry records the original ID with `code=spool_expired`, increments `ShardPilotStats.SpoolExpired` and the dropped total, and retains eligible siblings. Missing, invalid or future clocks expire conservatively, including records from older spool formats. Delivery remains best effort within these limits. Consent receipts, crash reports and remote config retain their separate retry policies.
- **Durable attempt fence.** Before sending stored analytics, the SDK persists any new attempt clock without evicting backlog members. A refused write prevents submission and retains the event for a later attempt. The owned transport defers this clock until its final pre-send gate, so a dispatcher wait cannot leave a preliminary attempt clock on disk. Durable I/O and later frame scheduling are followed by fresh age and consent checks. Metadata writes run off the frame thread on platforms with a spool.
- **Consent-first: only a persisted GRANT loads the spool.** A construction that starts from anything else — `Unknown` (absent record), `Denied`, or an **unreadable** consent record — **purges** the spool at init instead of loading or holding it, sweeping every scope's file in the directory (the consent key is global while spool files are per-scope): a spool without an affirmative grant behind it cannot be proven to have been written under one (a pre-consent-first install spooled while `Unknown` was open). Nothing is ever appended outside a grant either. `SetConsent(false)` at runtime wipes the spool too. The denial sweeps every (workspace, app, environment) scope's spool file in the active spool directory and propagates eagerly to every other live client — the consent decision is global — touching only the SDK's own canonical spool files; files under a different custom `SpoolDir` than the active one are out of reach, so keep `SpoolDir` stable. The wipe fails closed: if the file cannot be removed, the spool is disabled for the rest of the session and a persisted “wipe owed” marker (recording that exact file) keeps it disabled — nothing is loaded or sent — until a later launch removes the file for good.
- **Caps.** `SpoolMaxEvents` (default `2000`) and `SpoolMaxBytes` (default 1 MiB), oldest-first (FIFO) eviction, enforced on append and on load — except overflow appends, which are rejected at the caps instead of evicting. A single event larger than the byte cap on its own is rejected up front and never evicts the existing backlog; a failed disk write does not lose the events — they stay in the backlog and the writer retries with backoff until the disk recovers. The caps bound the **backlog**: an evicted entry leaves it immediately, while its bytes stay in the file as a dead record until a later compaction reclaims them, so the file may reach about 1.5x the live envelope bytes plus bounded clock framing and 4 KiB. Clock framing reserves at most 43 bytes per live record before that multiplier; it does not count against the envelope backlog cap. That trade is deliberate — rewriting the whole file on the append that evicts puts an O(spool size) cost on the calling thread, which at queue overflow is the game thread. A relaunch re-derives the same backlog: the load path reads every record and trims oldest-first, and the dead records are by construction the oldest.
- **Appends are appends.** The spool file is a header line followed by one newline-terminated record per line: SDK intake milliseconds, a tab, first ambiguous-attempt milliseconds (zero before an attempt), a tab, and unchanged envelope JSON, and spooling an event appends exactly that record — it does not read or rewrite what is already there. At a full spool a single overflow append measures **0.32 ms against 2.65 ms** for the whole-file rewrite it replaced, and no longer grows with the backlog in the recorded append-only measurement; moving the write off the calling thread took the same call to **0.057 ms** p95. A crash mid-append leaves a final line with no terminator, which the load path drops while keeping every complete record; the serializer escapes newlines inside strings, so a raw newline in the file can only be a record terminator. Version 2 files are discarded. The pre-existing v1 reader retains envelopes with missing clocks, which expire at the replay gate. The quoted latency measurements describe the earlier v2 implementation, not a new v3 benchmark.
- **Acceptance, not durability.** `Track` returning `true` for a spooled event means the event was **accepted into the backlog** — validated, serializable, within the caps, not refused — and will be published from memory or from disk. It does not mean the bytes are on disk yet: the write happens on a background writer thread, so a hard kill in that window loses events a synchronous write would have saved. Every ordinary path closes the window before it matters — `OnApplicationPause`, `OnApplicationQuit`, `CloseAsync` and `PersistPendingEvents()` all write on the **calling** thread and return once the bytes are down, so an awaited `CloseAsync` is still a durable shutdown barrier. A failed write is retried by the writer with backoff (250 ms doubling to a 30 s ceiling) instead of dropping the event, and an idle writer sleeps rather than polling. On WebGL there is no spool at all, so none of this applies.
- **Robust and private.** Rewrites are atomic (temp file, then replace, in the same directory); an unparseable or truncated spool file is silently discarded and the SDK starts clean — the spool never throws into game code. The file lives in the app-private `Application.persistentDataPath` (override with `SpoolDir`), holds envelope JSON and local retry clocks — never auth tokens or headers — and is scoped per (workspace, app, environment), so two configurations never share a spool. ONE live client per scope owns the file: initializing a newer client for the same scope transfers file ownership to it, and the replaced client (which keeps delivering from memory) can no longer rewrite the file. `ShardPilotSdk.Initialize` hands the file over as one atomic step on re-initialization — it validates the new config first (an invalid config throws and leaves the current client running), then persists the previous client's remnant, constructs the replacement (which loads and re-sends the undelivered remnant), and swaps — briefly blocking concurrent SDK calls (one spool load, typically tens of milliseconds) while the replacement takes ownership, before closing the old client in the background. A consumer that needs an extra safety point can call `ShardPilotClient.PersistPendingEvents()` directly (it never throws and does not clear the queue).
- **WebGL.** The spool is a no-op on WebGL — browser persistence is not a real synchronous filesystem — so behavior there stays in-memory only.

A replaced client's close carries the final owning spool's persistence outcome.
Events deliberately discarded by that owner's consent gate or experiment
withdrawal do not produce `spool_persist_failed`. An actual append refusal or
failed writer cycle still produces that final, non-retryable storage result.

`ShardPilotClient` implements `IDisposable`. Leaving a `using` block attempts
synchronous persistence of its pending remnant, then starts the existing bounded
close without waiting for network delivery. Storage can still refuse the write,
and WebGL has no synchronous event spool. Await `CloseAsync` when you need its
result or completed retirement; `Dispose` does not report either. Repeated
`Dispose` calls reuse the retained close, including the timeout chosen by an
earlier `CloseAsync`. Disposing the currently attached client detaches the
facade and crash hooks; disposing an older client leaves its successor attached.

Internal host-lifetime retirement fences new intake before draining owned
experiment applications and persists each final session tail. Its weak registry
also covers clients displaced from per-spool ownership. Starting a new lifetime
keeps old references fenced. The Editor adapter invokes these primitives on play
exit and before script reload.
Retirement cancels discarded publish tasks and completes callback operations without
waiting on network work. Late sends to a retired host complete on their caller.

Owner certification is **NOT RUN**: in a disposable supported Unity Editor project,
run `ShardPilotEditorLifecycleTests` with the package test assembly enabled. Record
the Editor version and XML for actual script reload and two plays with domain and
scene reload disabled. Native API bindings do not implement Unity's frame scheduler.

Optional Event fields and host API inputs/outputs carry nullable reference
annotations. For example, check `ShardPilotSdk.Client` before using it and treat
an absent experiment variant as the default experience. Tracking-call property maps accept
both nullable-value and existing non-null-value dictionaries without warnings.
Validation, event bytes and the required third argument of the schema-version `Track` overload are unchanged.

Each client retains one `CloseAsync` task, including its completed result.
The first call owns the timeout; later calls return that same task. The facade
also retains its close task while detached and never reattaches a closed client
or reinstalls its crash hooks. A successful `Initialize` starts a new facade
close-task owner; an invalid initialization leaves the prior task intact.
At the deadline an already-observed terminal rejection from that close's
events remains terminal unless consent or remnant persistence failed.

Case-distinct journal paths remain independent on case-sensitive platforms. Repeated authoritative drops of already-absent assignments preserve journal capacity; unreadable records keep their durable drop work.

On Unix, case aliases share a journal owner only when the filesystem confirms the same location. This rare check uses an empty private temporary file and removes it before sharing; an unavailable check withholds alias authority.

The Unix alias check uses portable file operations because the Unity 2022.3 BCL has no ResolveLinkTarget API.

## Crash capture

In current source, fresh JSON crash reports and Android
tombstone metadata use `app.key` with the configured `AppKey`; `app.id` is
absent. Crash/native/debug identities and raw dump bytes are unchanged.
This selector change is included in `v0.12.0-alpha`.

`Initialize(config)` with `config.CrashHooksEnabled = true` (the default) installs `Application.logMessageReceivedThreaded` and `AppDomain.CurrentDomain.UnhandledException` hooks. A crash is shipped to the **dedicated crash endpoint**, separate from analytics events: `POST {CrashUrl}/api/v1/crashes/ingest` with the `crash:write` `CrashKey`, body = the crash report JSON — **not** a `mobile_crash` analytics event. It carries the configured component `component` slug, the exception type/reason, a single pre-symbolicated thread of `function`/`file`/`line` frames (no native modules), the last 50 sanitized analytics breadcrumbs, OS/device context, and the hashed-at-ingest `anonymous_id`/`session_id`. The crash path deliberately excludes minidumps, screenshots, network/provider payloads, file contents, prompts, completions, tokens, and raw PII.

**Crash capture requires `CrashKey`** — without it there is no credential for the crash endpoint and `CaptureCrash` is a safe no-op. **Fatal crashes are never sampled** (a terminating crash is always sent, consistent across the ShardPilot SDKs); only non-fatal reports are sampled, deterministically every `CrashSampleEvery`-th call per client instance (default every 10th as of `v0.7.0-alpha`, matching the Go and Defold SDKs; `v0.6.0-alpha` and earlier defaulted to `1` — the first N−1 non-fatals of each client instance are always dropped, and a re-`Initialize` starts a fresh count). The crash hooks classify an `AppDomain.UnhandledException` with `IsTerminating == true` as fatal.

Captured JSON reports carry `fatal`; admitted non-fatals also carry
`non_fatal_sample_one_in` with the admitting N when it is in the wire range
1..1,000,000. Larger configured rates still govern sampling but are omitted on
the wire (unknown). Tombstone metadata carries `fatal: true` and no rate. These
SDK-stamped fields are serialized before persistence, so replay keeps the
capture-time values; old spooled bodies replay unchanged with unknown annotations.
Caller context/metadata cannot override them. The sampling default remains 10.

**Crash reporting is a legitimate-interest plane with a persisted opt-out** — see [Identity & consent](#identity--consent). With crash credentials configured it starts enabled; `SetCrashReportingEnabled(false)` stops collection and sending and clears breadcrumbs. Analytics consent `Denied` and `DeniedForcedMinor` also suppress crash capture. An unreadable or malformed persisted setting fails **closed**.

Public `CaptureCrash(onSent)` notifications run once on the producing Unity
host thread, outside SDK locks, on a later host pump. Retiring that host or its
producing lifetime discards an undelivered notification. Internal fatal-send,
AEI and tombstone durability settlement still progresses on its original thread
without a host pump; no additional wait, close owner or deadline is introduced.

### Android next-launch native crashes and ANRs

**`0.14.0-alpha`:** `NativeCrashExitInfoEnabled` and `AnrExitInfoEnabled` are separate opt-ins,
both **false** by default. On Android API 30+, the next SDK initialization
reads the app's exit history and reports only the enabled reason. Configure
`CrashUrl` and `CrashKey`, keep `CrashHooksEnabled = true`, and set the desired
flags on your `ShardPilotConfig` before calling `Initialize`:

```csharp
config.NativeCrashExitInfoEnabled = true;
config.AnrExitInfoEnabled = true;
```

Enable either flag independently. Native reports upload the raw Android
binary tombstone stream when available (API 31+), or metadata only without
one; ANR reports describe system-killed ANRs. Other platforms ignore these
flags. In-session stall reporting uses the separate `HangWatchdogEnabled` flag.

Crash opt-out, denied analytics consent and `DeniedForcedMinor` continue to
refuse these reports. For minors, leave the flags off, omit `CrashKey`, and
set `CrashHooksEnabled = false` before initialization; apply the forced-minor
consent decision through the existing age-gate flow.

### Crash durability (next-launch re-send)

A crash report is captured at the worst possible moment to deliver it: the process may be dying, or there may be no network. Every accepted report is therefore persisted to a **durable crash spool** at capture time and re-sent on the next launch until the crash ingest service acknowledges it:

On the crash endpoint, a `400` or `404` whose JSON `error.code` is exactly `kind_not_enabled` is a retryable refusal. The original report stays pending, with its captured ID, kind and timestamps unchanged; replay honors `Retry-After` and the existing crash backoff, retention and privacy gates. Android native reports likewise keep their cursor pending for another pass. A bare, malformed or differently coded `404`, and other terminal `4xx` responses, still settle the report. This exception does not change analytics or consent response classification.

## Remote config

Set `AppUrl` and `RemoteConfigEnabled = true`; an address alone does not enable fetches.

```csharp
bool started = ShardPilotSdk.FetchRemoteConfig(result => {
    if (result.Success) {
        var values = result.Values;
    }
});
```

Both `FetchRemoteConfig` methods now return `bool`: true admits a fetch; false means no fetch started. `FetchExperimentAssignment` uses the same admission meaning. A live initialized owner still receives a queued refusal callback when a fetch is refused. Before `Initialize`, without a captured host, or after owner/host retirement, no public callback runs. A response settled before ordinary `CloseAsync` keeps its queued notification and delivers its own pre-close outcome once while the host stays live, subject to the same privacy and publication fences. Results from synchronous, worker, cache and refusal paths notify once on the captured Unity host thread, outside SDK locks. Internal cache settlement and experiment declaration cleanup progress without frames. The host must keep pumping for public notification delivery.

Every fetch uses `GET {AppUrl}/config/v1/{workspace_key}/{environment_key}`. Set `WorkspaceKey` and `EnvironmentKey` to the workspace and environment **keys**, rather than database row IDs. With `Granted` consent and an available anonymous ID, the SDK adds `X-ShardPilot-Anonymous-Id` with that literal identity; it never appends an identity path segment. Without analytics consent (`Unknown`, `Denied`, or `DeniedForcedMinor`), no user/device identifier or targeting attributes ride in its URL, headers, or body. The main-thread UnityWebRequest dispatcher rechecks consent immediately before creating the request; a queued request loses its subject header and attributes after a denial, including a denial followed by a new grant. The same selection binds the matching cache validator. The route uses the `sp_` publishable `PublishableKey` as Bearer authorization (Mode-B ingest JWTs cannot authenticate remote config, so Mode-B users must also set `PublishableKey`). The endpoint answers `{"version": <number>, "values": {…}}`; `result.Values` is the `values` map and `result.Version` the published version (a body whose `values` member is absent or not a keyed object is treated as malformed rather than served).

Responses are cached on disk at Application.persistentDataPath/shardpilot.remote-config.json as `{scope, etag, body, fetched_at}`; subsequent fetches send `If-None-Match` only from the same consent scope. Identifier-free requests never reuse an identified response or its validator, including when consent changes while a request is queued. One fetch = one of these outcomes:

- `200` with a usable body — fresh values are served and the cache is overwritten (body + ETag).
- `304 Not Modified` — the cached snapshot is served (`result.FromCache == true`) and the record's freshness stamp is renewed: the endpoint confirmed the body as current, so a same-scope record written while the request was in flight can no longer outrank it.
- A **transient** failure (connection error, timeout, `408`, `429`, `5xx`, malformed body) — while the scope is open, the cached snapshot is served with `FromCache == true` and `Error` carrying the reason; with no usable cache or a terminally closed scope the fetch fails.
- `401`/`403` fails **closed** — the cache is not served, so a revoked or wrong key never keeps supplying config (the cache file itself is left untouched).
- Any **other** status (e.g. a `404` for a removed environment, an unexpected redirect) is a **permanent** failure: retrying cannot help, so the fetch fails rather than serving stale values as a healthy `Success`; the cache record is left untouched.

The cache is scoped to the `(workspace_id, environment_id, client_id, AppUrl)` tuple, with an empty `client_id` for identifier-free requests; any other scope — or a record whose body no longer parses (its ETag would provoke a `304` with no body to recover from) — is treated as a miss and overwritten by the next successful fetch. Responses to concurrent fetches may return out of order; an older response never overwrites a newer configuration. A terminal response keeps that instance's scope closed: cached getters return null, and transient, local retry-window or fenced-out callbacks supply no values. Only a winning valid 200 or 304 reopens it. Closure survives switching scopes away and back; another scope stays independent. The body and ETag remain available for revalidation or a new client/session, whose closure state starts empty. Outside a closed scope, callbacks retain their own fetch outcomes. If the cache write fails, the freshest configuration is kept in memory for offline fallback and the stale on-disk record the fetch superseded is cleared (a fresher record persisted by a sibling instance is left in place), so a restart starts from defaults rather than from rolled-back values. `ShardPilotSdk.RemoteConfig.GetCachedValues()` reads the freshest cached values without touching the network. This is a config fetch only — experiment assignment and exposure facts live on their own surface (below), and there are no stats here.

In current source, a positive `Retry-After` on `429` or `5xx` also opens an in-memory retry window for that consent scope, shared across SDK reinitialization and sibling instances in the process regardless of cache path. Expired scopes are pruned on subsequent fetches. Explicit fetches inside it make no HTTP request and return the eligible cache or failure with the opening status. The scope is selected immediately before dispatch, so a queued consent downgrade uses the identifier-free window. Only a live, current-scope, unfenced response can extend the deadline; a shorter hint or local cached answer cannot renew it. The existing shared parser now accepts delta-seconds and HTTP dates, rounds dates up, and caps hints at 24 hours. Missing, zero, past-date or invalid hints create no server-directed window. These windows neither add an automatic retry loop nor change the other SDK planes’ local backoff policies.

**Targeting attributes (opt-in, consent-gated).** `ShardPilotSdk.SetRemoteConfigAttributes(attributes)` stores targeting attributes for the fetch to carry (defensive copy; `null`/empty clears), and `ShardPilotConfig.RemoteConfigAttributesEnabled = true` (default `false` — dark; requires `AppUrl`) opts the fetch into sending them — but **only while analytics consent is `Granted` at fetch time**: under `Unknown` or either denied flavor the request omits identifiers and attributes. With the opt-in off, a granted request still carries its anonymous ID but no attributes. Attributes are normalized per fetch with the experiment plane's own vocabulary and bounds (`geo`, `app_version`, `device_type`, `install_date`, `user_segment`, `custom_attribute_<1-64 chars>`; values trimmed, ≤512 UTF-8 bytes, ≤64 pairs, ordinal-sorted; out-of-vocabulary names dropped, never sent), with both query sides percent-escaped. The ETag cache distinguishes identifier-free and identified requests; attributes are not part of the scope, so the last-known-good snapshot reflects the last served body regardless of attribute changes between fetches (an honest v1 limit).

## Experiments (dark by default)

**Explicit application availability:** this flow was introduced in `0.11.0-alpha`;
the examples below use the `0.14.0-alpha` scope API. Install its tag after
publication, or use UPM's local-package flow for the candidate. Before enabling it
against a deployment, the owner must verify the assignment and application
routes, publishable-key permission, and sealed-fact ingest support.
This guide makes no claim about current production flags or availability.

Opt in at initialization with `ShardPilotConfig.ExperimentsEnabled = true`
(default `false`), `AppUrl`, and an `sp_` `PublishableKey`, including when
ordinary analytics uses a Mode-B JWT. Applications require `Source = Client`
(the default), granted analytics consent, and a usable `client_id` assignment
with serving pins. With the opt-in off there is no assignment fetch,
revalidation, application request or experiment fact capture. Keep the game's
control experience as the fallback when an assignment or application is refused.

After initialization, a successful consent grant and `SessionStart()`, fetch
the assignment. Only apply it when the game actually uses that treatment:

```csharp
ShardPilotSdk.FetchExperimentAssignment("checkout_flow",
    ShardPilotExperimentAttributes.WithAgeBand(ShardPilotExperimentAgeBand.Adult,
        new Dictionary<string, string> { { "geo", "US" } }),
    result =>
    {
        if (!result.Ok || !result.Assigned) return; // keep the control experience
        var variant = ShardPilotSdk.ApplyExperimentVariant("checkout_flow");
        if (variant == null) return; // local application was refused
        // Use variant for this interaction. Reading result.VariantKey alone is not an application.
    });
```

In the later outcome handler, after an explicit application in this session:

```csharp
bool captured = ShardPilotSdk.TrackExperimentOutcome("checkout_flow", "revenue_cents", 999);
// captured means local capture, not CP acceptance or analytics delivery.
```

- **Declared age:** the game determines the player's band; this SDK does not
  verify age or accept a date of birth. `WithAgeBand` copies optional attributes
  and adds `age_band`: `Unknown` (`unknown`), `UnderThreshold`
  (`under_threshold`), or `Adult` (`adult`); undefined enum values throw
  `ArgumentOutOfRangeException`. Declare `Adult` only when the game has
  established its applicable threshold. Undeclared and non-adult players are
  ineligible for client-id experiments. An authoritative `age_ineligible`
  answer clears the assignment and stops its automatic retry. A non-adult
  declaration suppresses serving immediately; when no answer is applied it
  withdraws applications as the age refusal would. Earlier adult requests
  cannot reinstall that withdrawn assignment. A later `Adult` declaration
  likewise fences every earlier fetch of the experiment, including older adult
  and undeclared ones, for any subject id, since a declaration covers the
  install: an older non-adult answer or its subject re-mint retry cannot
  withdraw it. A replaced client's declaration fences nothing. Consent remains
  a separate gate.
  Age withdrawal covers that experiment's captured applications and facts,
  including retained earlier fact keys, while preserving unrelated facts.
  A declaration keeps its original known facts across an automatic subject
  remint. Outstanding work lives in a journal beside the assignment cache,
  independently of cache rewrites and identity-store writes. Unknown journal
  or assignment input defers the affected work without confirming absence.
  New withdrawals identify later re-admissions by sealed event IDs, so equal
  timestamps or a backward clock correction do not withdraw their new facts.
  While an earlier withdrawal remains unresolved, each new fact waits for its
  identity exemption to persist before entering analytics; a failed write
  retains the sealed occurrence for retry. Earlier emissions keep their
  withdrawal alive through enqueue and the late purge, including across a
  client replacement. Failed located purges retry before their intent retires.
  A batch already on the wire may arrive. Journal admission must succeed for
  crash-safe retry; unavailable journal storage leaves work pending in memory.
  An absent assignment is not proof of a durable fact purge. After re-admission, the game
  must explicitly apply again to record a new occurrence.
- **Age attribute compatibility:** the existing dictionary fetch overload also accepts `age_band`. The legacy `custom_attribute_age_band` spelling remains supported, and either spelling can refuse eligibility even when the other says `adult`. Both are retained within the 64-attribute limit. Only exact lowercase `adult`, `unknown`, and `under_threshold` are valid; other supplied values (including empty, null, padded, or mixed-case values) are sent as `unknown`, never omitted or promoted to adult. Automatic revalidation retains the declaration of an assignment already confirmed on this launch; a new explicit fetch without attributes supplies no declaration.
- **Restart eligibility:** on every launch, restored client-id assignments remain unavailable until a fresh host fetch with an age declaration receives a successful assigned or authoritative not-assigned answer. Stored adult attributes do not authorize serving or automatic revalidation. Restored sealed exposure/outcome facts stay in the retry spool until that experiment’s fresh decision: `age_ineligible` withdraws them; a kill switch preserves prior facts. Transient, malformed and auth failures do not confirm eligibility, and another experiment’s decision does not release them. Ordinary analytics continues to drain; synthetic assignments retain their existing consent gates. An otherwise successful drain returns retryable `pending` while facts remain quarantined. An age fetch never grants analytics consent. Once close begins, new experiment fetches are refused before recording an age declaration. Non-adult declarations accepted before close still end unanswered and withdraw before teardown.
- **Fetch:** `GET {AppUrl}/api/cp/v1/runtime/experiments/assignment?app_key&environment_key&experiment_key&subject_key&<attributes>` with the `sp_` key as Bearer. Targeting is 100% server-evaluated; the client's only obligation is supplying attributes (`geo`, `app_version`, `device_type`, `install_date`, `user_segment`, `custom_attribute_<name>`; values trimmed, ≤512 bytes, ≤64 attributes, sorted). The subject is a dedicated persisted `spcid_` installation id (`shardpilot.spcid`) — SDK-managed with no host override path, NOT the anonymous id, minted lazily at first need, re-minted only on storage loss/corruption.
- **Classification (per fetch):** `200` installs (assigned) or drops the cached assignment (not-assigned shapes — `reason` absent, `kill_switch`, `targeting_unmatched`, `age_ineligible`, or future reasons/assignment units reported as `unknown`); transient failures (offline, timeout, `408`, `429`, `5xx` incl. the kill-state `503`) serve an eligible last-known-good with `FromCache = true` after startup has been decided; `401`/`403` fail **closed** (record retained, revalidation halts until a later authorized outcome or re-init) — except the exact real-subjects sentinel `403`, which drops the record and stops sibling instances serving it too; `404` and non-grammar `400`s drop permanently; the subject-grammar `400` re-mints the subject once per process and retries with the same attribute set. Out-of-order assignments and ordinary per-key removals are fenced per (scope, experiment). A late `401`/`403` closes the whole experiment lane even after a newer answer for that experiment. Responses for a replaced SDK client, previous installation identity, or an auth state already superseded by recovery remain discarded.
- **Response validation:** a not-assigned version may be absent; when present
  it must be a positive int64 JSON integer. A present invalid `subject_fact_key`
  on a `client_id` assignment makes the response malformed and keeps the prior
  cache. Synthetic assignments ignore that echo. Future nonblank string reasons or units
  clear the serving cache without creating an exposure. Wrong types and empty
  or whitespace-only vocabulary tokens remain malformed.
- **Revalidation:** every cached assignment re-fetches every 300 s ± 10 % jitter (`Retry-After` honored on `429`/`5xx`, exponential backoff on transients) — the SDK-side kill-switch reach. An offline client keeps its variant indefinitely; the cache is a latency/offline device, never an assignment authority (no local re-bucketing, ever).
- **Consent (granted-only plane):** under `Unknown` or either `Denied` flavor, no assignment request leaves the device, nothing is minted, getters return `null`, and the cadence does not run — a forced-minor session produces zero experiment traffic on both planes. This is deliberately stricter than remote config.
- **Reads and migration:** fetching an assignment, reading `result.VariantKey`,
  `GetExperimentVariant`, and `GetExperimentPayload` never record an exposure.
  Replace a getter used to select the actual treatment with
  `ApplyExperimentVariant` or `ApplyExperimentPayload`. Both share one
  occurrence per (experiment, version, subject) per analytics session, even
  when both are called. Both return `null` on refusal, but the payload method
  also returns `null` when the optional payload is absent after the application
  was recorded. That `null` is ambiguous: decide control versus treatment from
  `ApplyExperimentVariant` or the assignment result after explicit application;
  use `ApplyExperimentPayload` only to read data as a defensive copy. An absent
  payload must not switch an already applied treatment to control.
  `TrackExperimentExposure` captures an additional
  application with a new occurrence ID on every successful call; use it only
  for an additional treatment use, not as a companion to the first Apply call.
- **Outcome units:** outcomes require an explicit application in the current session. Values must be finite integers in `[-9007199254740991, 9007199254740991]`; fractional values, NaN, infinity and larger magnitudes return `false` and record no outcome. Choose a scale and unit for each outcome key: for example, `revenue_cents = 999` represents USD 9.99. The SDK sends that integer unchanged; it does not scale or round a fractional input.
- **One live client per cache directory:** keep one live `ShardPilotClient`
  per `SpoolDir`. A successor replaces the old client's automatic experiment
  lane: old getters and Apply methods return `null`; fetch and Track methods
  refuse with `experiments_replaced`. Already captured applications may drain
  from the old client with their captured pins and identity, subject to consent,
  age withdrawal and the close deadline. Replacement is not delivery proof.
  Close the old client after handoff, as `ShardPilotSdk.Initialize` does.

### Sealed experiment facts

The worker sends captured applications to
`POST {AppUrl}/api/cp/v1/runtime/experiments/exposures` or
`POST {AppUrl}/api/cp/v1/runtime/experiments/outcomes` with the `sp_` key.
It captures the assignment's version, variant, serving revision/kill gate/time,
targeting attributes, application time and occurrence ID before returning to
gameplay. Outcomes use the last explicit application in that analytics session
and get their own occurrence ID. Return values confirm local capture,
**not CP acceptance or analytics delivery**.

CP returns `{ fact, seal }`. The SDK validates the fact's structure and binding
to the captured request; analytics verifies the seal's authenticity. Only a
matching response enters the consent-first analytics queue. Do not fabricate
these reserved events with `Track` or `Enqueue`, or turn a failed apply into an
unsigned event. The schema-v2 envelope carries `workspace_key`, `app_key`,
`environment_key`, `schema_version = 2`, `source = client`, `attestation_seal`,
`anonymous_id` and the captured `session_id`. Formatted sealed envelopes are quoted only in the disk record so literal
newlines remain intact on replay. Storage quoting can require up to twice their
wire bytes even after compaction; `SpoolMaxBytes` still limits envelope bytes.
Compaction measures encoded live records so quoting overhead does not trigger
repeated full-file rewrites.
CP owns the signed fact fields:
`cpx1_` event ID, timestamp, and props including `assignment_unit = client_id`,
`assignment_key = sfk1_…`, opaque `attestation` (currently `client_attested`), experiment/version
and variant (plus outcome key/value for outcomes). The raw `spcid_` goes to CP
as the assignment subject; it never goes to analytics, and `user_id` is omitted.
Additive fact and props members are retained with their original JSON bytes
through persistence and replay. Required fields, request binding, unique members
and the size budget still apply. Attestation must be nonblank; its bytes are
preserved. Signed response decoding retains its 64 KiB budget; the completed
analytics envelope, including client metadata, must fit 2,048 UTF-8 bytes before
it enters the analytics queue. Client metadata names
are reserved in the returned fact to prevent duplicate fields in the assembled envelope.
Omitted actor/request identities, retired scope IDs and server-derived identity
fields are refused in both responses and restored sealed envelopes.
Ordinary gameplay events keep their UUIDv7 IDs and existing app-first envelope.

Apply transport failures, `408`, `429` and `5xx` retry the same captured request
and occurrence ID. Malformed or mismatched success bodies do not enqueue a fact
and are retried. Queue pressure after sealing retries admission without another
CP apply. An immutable sealed envelope that exceeds the metadata-inclusive
budget is discarded with `event_too_large` and counted once in
`Snapshot().ExperimentExposureDrops` or `ExperimentOutcomeDrops` under that code,
separately from `Dropped`, so later captures
can advance. `401`/`403` block the experiment lane until a fresh authorized fetch;
other terminal `4xx` (except `408`/`429`) discard that application. An apply
`409 not_assigned` with `reason = age_ineligible` withdraws its affected facts.
Do not call Apply again merely to retry delivery.

**Durability:** unsealed applications are memory-only. Persistence snapshots
do not send apply HTTP or journal raw application requests. Once admitted, a
sealed envelope follows the ordinary spool's persistence, capacity, consent and
retry-age rules and preserves its fact, seal and ID on replay. An awaited flush
waits for an unadmitted application like any queued event and answers `pending`
if it is still unadmitted at the deadline; `CloseAsync` offers a
bounded drain, not a guarantee that an offline application will be sealed before
exit. The `experiment_applications_not_persisted` diagnostic names this limit.
WebGL has no durable disk spool.

**On-device records:** `shardpilot.spcid` in the identity store; the scoped,
bounded assignment cache shardpilot.experiments.json under `SpoolDir` (default
`Application.persistentDataPath`) with `fact_key_history`; the `.condemned`
marker for whole-scope cache suppression; and the bounded `.withdrawals.*`
journal for per-key assignment drops and fact removals. Unfinished frames
survive flag-off replacement and credential or URL rotation of the owning spool.
Without a resolvable cache path, retained work is memory-only and cannot survive process exit.
Legacy unsigned experiment spool entries are not replayed as sealed facts, and
an assignment lacking usable serving pins requires a fresh fetch before Apply.

## Project layout

```text
Runtime/
  ShardPilotSdk.cs           Static entrypoint (Initialize/Track/SessionStart/FetchRemoteConfig/Flush/Close)
  ShardPilotClient.cs        Client, bounded queue, worker (thread vs WebGL coroutine)
  ShardPilotConfig.cs        Config, defaults, endpoint validation, platform detection
  ShardPilotTokenManager.cs  Mode-B JWT cache + proactive refresh + 401-retry
  ShardPilotEvent.cs         App-first event model
  ShardPilotTransport.cs     UnityWebRequest batch/consent transport
  ShardPilotConsent.cs       Consent-first tri-state consent + receipt body
  ShardPilotCrashSettings.cs Persisted crash-reporting opt-out (fail-closed)
  ShardPilotSpool.cs         Durable offline event spool (resend on next launch)
  ShardPilotCrashSpool.cs    Durable crash-report spool (write-ahead at capture, resend on next launch)
  ShardPilotCrash.cs         Crash hooks + sanitized payload
  ShardPilotRemoteConfig.cs  Remote-config fetch + ETag disk cache
  ShardPilotJson.cs          Hand-rolled JSON writer
  ShardPilotJsonParser.cs    Minimal JSON reader (remote config)
  ShardPilotPersistence.cs   PlayerPrefs + file fallback
  ShardPilotIds.cs           UUIDv7 generation
  ShardPilotDispatcher.cs    Main-thread coroutine dispatcher
  ShardPilotStats.cs         Internal counters
Samples~/BasicTelemetry/     Minimal init + session + track quickstart
Samples~/EngineSmoke/        Init + track + flush + handled-crash sample scene
```

The worker has two implementations selected at compile time: a background `Thread` on standard platforms, and a `ShardPilotDispatcher` coroutine under `#if UNITY_WEBGL && !UNITY_EDITOR` (WebGL has no threads).

## Build & test

Public snapshots contain the runtime, editor integration, samples and package documentation.
Contributor tooling and its instructions are excluded from the snapshot.

Public package workflows validate manifests and committed asset metadata and scan
repository history for secrets on GitHub-hosted runners. Tagged releases require
a clean mainline commit and matching package, tag and changelog versions.
Tagged releases apply these changelog checks:

- Version headings use `## [version]`; only tag names carry the `v` prefix.
- Every level 2 heading is a canonical bracketed version or exactly `[Unreleased]`.
- At most one level 1 document title is allowed; fenced code is content.
- At least one versioned `##` section exists.
- The first `##` section is Unreleased or a version.
- An optional Unreleased section is unique and first.
- Unreleased contains only blank lines or headings, with no pending entries.
- Version labels are unique.
- The first version matches `package.json` and the `v<version>` tag.
- The selected version section has at least one nonblank, non-heading line.

Release notes include that version section up to the next versioned heading,
preserving its subsections and excluding Unreleased and older versions.
Level 2 delimits sections; level 3 and below are free-form content and cannot end a section.
Both public workflows use `.github/scripts/changelog-check.py`. CI permits
pending Unreleased entries; release mode additionally requires them to be empty
and checks the tag before writing notes.

Source runners use a package-qualified test-support symbol so unrelated consumer
defines do not enable runtime test registries.

## Compatibility

- Unity `2022.3`+ (`package.json` `unity` field; CI test images `2022.3.70f1` and `6000.3.20f1`).
- C# / .NET as provided by the Unity Editor; runtime has no third-party dependencies.
- Wire-compatible with ShardPilot ingest; the event envelope is consistent across the ShardPilot SDKs.

## License

Apache-2.0 (ODSOFT LLC). See [LICENSE](LICENSE).
