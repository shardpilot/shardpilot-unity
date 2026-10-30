---
name: shardpilot-unity-integration
description: >-
  Use when integrating the ShardPilot Unity SDK (com.shardpilot.unity) into a
  Unity project or debugging an existing integration — install, credentials,
  init, the consent-first analytics contract, crash reporting, remote config,
  explicit experiment applications, offline durability, and verification.
---

# ShardPilot Unity SDK integration

How to integrate `com.shardpilot.unity` correctly, including the parts that
are easy to get wrong: the consent-first contract, the three separate service
endpoints, and how to verify the integration actually delivers events. The
[README](../../../README.md) is the full reference; this guide is the
integration path with the contract rules stated up front.

## What the SDK does today

- **Consent-first analytics**: app-first events (`Track`, `SessionStart`)
  batched to `POST {IngestUrl}/v1/events:batch` over UnityWebRequest, gated
  behind an explicit player consent grant.
- **Durable offline spool**: admitted envelopes persist to disk and re-send
  within the spool/retry limits, de-duplicated by stable `event_id` (UUIDv7 for
  ordinary gameplay; CP's `cpx1_` for sealed experiment facts).
- **Sanitized crash reporting**: managed crashes in-process (Unity-logged exceptions and
  unhandled CLR exceptions) captured automatically and shipped to a dedicated
  crash endpoint (`POST {CrashUrl}/api/v1/crashes/ingest`) with their own
  durable spool. Legitimate-interest plane with a persisted opt-out.
- **Remote config**: an ETag-cached config fetch
  (`GET {AppUrl}/config/v1/{workspace_key}/{environment_key}`), carrying
  `X-ShardPilot-Anonymous-Id` only with granted analytics consent. Config fetch only —
  no experiment assignment, no exposure events.
- **Experiments (opt-in):** a separate assignment fetch and explicit
  application flow through CP; only returned sealed facts reach analytics.
  See [Experiments](#experiments-explicit-application) for the explicit application flow in
  `0.11.0-alpha`, consent/age gates and migration from getters.

Use a verified dev/test deployment for integration checks; endpoint defaults
and SDK capabilities do not establish production availability.
Crash capture is **managed-only** — a hard native crash (e.g. a segfault in
native code) is not captured. The public API is unstable before v1. See
[Known limitations](#known-limitations).

## Install

After `v0.13.0-alpha` is published, add the package as a UPM git dependency in the project's
Packages/manifest.json:

```json
{
  "dependencies": {
    "com.shardpilot.unity": "https://github.com/shardpilot/shardpilot-unity.git#v0.13.0-alpha"
  }
}
```

- Package name: `com.shardpilot.unity`. Requires Unity `2022.3` or later.
- The `#v…` URL fragment pins the release tag; the owner creates release tags.
  The candidate version is `0.13.0-alpha`; use a local package until its tag is published.
  CI matches this pin to `package.json`, not to the contents or existence of
  a tag. **`v0.11.0-alpha` includes the explicit application/sealed-fact
  flow.** Unmarked API references below describe `0.13.0-alpha`.
  Consent-policy preparation is **preview**: every reply returns the local
  strict fallback with `PlanUsed = false`; received policies are not adopted.
- For local development, add the package folder through Unity Package
  Manager's local-package flow instead.
- `v0.7.1-alpha` includes `RequestCompressionEnabled`, absent from `v0.7.0-alpha`.

## What `v0.7.0-alpha` changed in the contract

Collected here rather than scattered, because these are the facts an
integration written against `v0.6.0-alpha` gets wrong:

- **Consent picks a credential PER RECEIPT.** A `UserId`-keyed receipt rides
  the JWT; an anon-keyed one is sent with the publishable `PublishableKey` — except a
  GRANT for the client's current anonymous id under a `TokenProvider` with no
  `UserId`, which rides the JWT. Supplying an `PublishableKey` is what switches
  anon-keyed receipts onto the publishable key, so supply it only if that key is
  scoped to record them. And a PARKING gate runs BEFORE the credential choice:
  an anon-keyed receipt in a Mode-B session with a configured `UserId` and no
  `PublishableKey`, a `user_verified` receipt the current session cannot vouch for, and
  any unrecognized kind are all retained but NOT dispatched in that session. The
  first two release when a launch configures a credential that can vouch for the
  actor; an unrecognized kind stays inert until an SDK version that knows it.
- **`ConsentKindEmissionEnabled` defaults true.** Against an ingest deployment
  whose consent-kind mode is still `off`, the extra `kind` field is a terminal
  `400` that permanently drops the receipt, denials included. Set it `false`
  until that deployment migrates.
- **Engine requests do not declare a schema revision.** No configuration property or default digest is exported.

- **Experiment assignment was introduced behind an opt-in.** Current
  applications use the explicit application flow below. Server enablement must be verified for
  the deployment; a capability or config flag does not establish availability.
- **Non-fatal crash sampling defaults to 10.** An integration moving up from
  `v0.6.0-alpha` that emits fewer than ten non-fatals per client instance now
  reports none of them.

## Credentials

The SDK talks to **three separate services** with **separate credentials**.
Never hardcode real keys in source control or scene assets — inject them from
your normal secret-delivery path and keep placeholders in the repo.

1. **Analytics ingest** (`IngestUrl`, default `https://ingest.shardpilot.com`)
   — dual-mode, supply exactly one:
   - **Mode A — publishable key**: `PublishableKey`, must start with `sp_`, sent
     verbatim as `Authorization: Bearer`. Designed to be client-embeddable
     (it is still a credential — keep real values out of the repo).
   - **Mode B — per-tenant ingest JWT**: `TokenProvider`, an async
     `Func<CancellationToken, Task<ShardPilotTokenResult>>` your game
     implements to fetch/refresh a per-tenant HS256 JWT **from your own
     backend**. The SDK never holds the signing secret — only the returned
     JWT string, cached and refreshed off the game thread (`TokenRefreshLead`
     before `ExpiresAt`, default 60s, and on a 401). If both are set, the JWT
     wins for ingest.
2. **Crash ingest** (`CrashUrl` + `CrashKey`) — a **separate service
   and a separate key**. `CrashKey` is a `crash:write`-scope Bearer, a
   different scope from the analytics key/JWT; without it crash capture is a
   safe no-op. Supply `CrashUrl` separately whenever `CrashKey` is set;
   an absent URL is a configuration error, never an ingest fallback.
3. **Remote config and experiment CP routes** (`AppUrl`) — a separate service that
   authenticates **only with the `sp_` publishable key**. The Mode-B ingest
   JWT cannot authenticate it, so a Mode-B game that enables remote config or
   experiments must ALSO set a valid `PublishableKey`; config validation
   enforces this.

## Initialize

The following examples use the `0.13.0-alpha` scope API:
`WorkspaceKey`, `AppKey`, `EnvironmentKey`. Use a local package until its
tag is published. The earlier `v0.11.0-alpha` tag uses
`*Id` settings; the new source does not retain them as aliases.

Exact signature (static facade, `ShardPilot` namespace):

```csharp
public static ShardPilotClient ShardPilotSdk.Initialize(
    ShardPilotConfig config)
```

- **Must run on the Unity main thread** — it resolves the persisted anonymous
  id and consent state through PlayerPrefs and prepares the main-thread web
  request dispatcher. `Track` afterwards is thread-safe.
- Crash hooks are installed automatically by default
  (`config.CrashHooksEnabled = true`); set that property to `false` to keep the crash hooks out and
  call `ShardPilotSdk.Client.CaptureCrash(...)` yourself.
- The config is validated first: an invalid config throws
  `ShardPilotConfigException` and leaves a previously initialized client
  fully running. Required: one ingest credential mode, `WorkspaceKey`,
  `AppKey`, `EnvironmentKey`. These key-named properties take literal
  workspace, app and environment keys for analytics envelopes.
- Re-initialization is one atomic handoff (old client's pending events are
  persisted and re-owned by the new client; the old client closes in the
  background).

```csharp
using ShardPilot;
using UnityEngine;

ShardPilotSdk.Initialize(new ShardPilotConfig
{
    PublishableKey = "<SP-PUBLISHABLE-KEY>",   // Mode A; real value must start with sp_
    WorkspaceKey = "<WORKSPACE-KEY>",
    AppKey = "<APP-KEY>",
    EnvironmentKey = "develop",
    AppVersion = Application.version,

    // Crash reporting (recommended) — separate service, separate key:
    CrashUrl = "<CRASH-INGEST-BASE-URL>", // explicit crash service URL; no ingest fallback
    CrashKey = "<CRASH-WRITE-KEY>",         // without it, crash capture is a no-op
    CrashComponent = "game-client",               // component slug on crash reports

    // Remote config (optional) — separate service, sp_-key auth:
    AppUrl = "<REMOTE-CONFIG-BASE-URL>",
    RemoteConfigEnabled = true,
});
```

Mode B instead of `PublishableKey`:

```csharp
ShardPilotSdk.Initialize(new ShardPilotConfig
{
    TokenProvider = async ct => new ShardPilotTokenResult(
        await myBackend.MintShardPilotIngestJwtAsync(ct), // your backend mints it
        DateTimeOffset.UtcNow.AddMinutes(15)),            // ExpiresAt enables proactive refresh
    WorkspaceKey = "<WORKSPACE-KEY>", AppKey = "<APP-KEY>", EnvironmentKey = "production",
});
```

Useful defaults (all overridable on `ShardPilotConfig`): `BatchSize` 25 (max
100), `BufferSize` 1000, `FlushInterval` 15s (how long a PARTIAL batch waits
before publishing — not a heartbeat, an empty batch publishes nothing; a full
`BatchSize` publishes immediately and `Flush` on demand, and retry, consent and
crash pacing all run on their own clocks), `HttpTimeout` 2s, `SpoolEnabled`
true (`SpoolMaxEvents` 2000 / `SpoolMaxBytes` 1 MiB), `CrashSpoolEnabled`
true (`CrashSpoolMaxReports` 50 / `CrashSpoolMaxBytes` 512 KiB),
`RequestCompressionEnabled` true (new in `v0.7.1-alpha`). For local
plain-http endpoints on private ranges set
`AllowInsecurePrivateNetwork = true` (https is otherwise required outside
loopback).

**Batch bodies are gzip-compressed by default.** A batch is the same envelope
keys repeated per event, so it compresses hard — a 100-event batch measures
28.7 KB down to 2.1 KB on the wire (13.5x). Bodies under 1 KiB are sent
uncompressed, because gzip's framing makes a single-event batch bigger rather
than smaller. Three things follow when you integrate:

- **The ingest body cap applies to the UNCOMPRESSED body.** Compression buys
  throughput, not headroom — keep sizing against `BatchSize` exactly as
  before.
- **You do not need to coordinate the rollout.** A deployment that cannot read
  the coding answers `400` with detail code `unsupported_content_encoding`,
  and the client latches compression off for the session and re-sends the same
  batch uncompressed. The cost is one round-trip, not lost events.
- **WebGL behind a self-hosted edge is the one case worth knowing about.** A
  browser preflights `Content-Encoding` (it is not CORS-safelisted), and an
  edge that does not list it fails the request as a *network* error with no
  status to match on. The SDK handles this: the first compressed publish of a
  session is probationary and falls back to uncompressed on a transport error,
  latching only if that retry succeeds. ShardPilot's own edge allows the
  header. Set `RequestCompressionEnabled = false` to skip even the probe.

## The consent-first contract (as implemented in this SDK)

Analytics starts **gated**,
and the gate is real — do not "fix" silently dropped events by re-sending
them around the gate.

- **Consent state, persisted**: `ShardPilotConsentState { Unknown = 0,
  Granted = 1, Denied = 2, DeniedForcedMinor = 3 }`, readable via
  `ShardPilotSdk.ConsentState` (`Unknown` before initialization). Every client
  and facade `SetConsent` overload returns `ShardPilotResult`: `Success` says
  whether the decision applied, `Error` names a refusal, and `Warnings` names
  unfinished persistence, receipt or purge work. Call from your consent
  dialog's handler on the Unity main thread. Inspect warnings before assuming
  the decision or receipt survives a process exit.
- **Optional per-decision notice:** both bool and enum setters on the facade
  and client accept `ShardPilotConsentNotice? notice = null`, with public string
  fields `NoticeVersion`, `NoticeLocale`, `PolicyVersion`. Supply identifiers,
  never notice text. Omission keeps the existing behavior; an empty supplied
  struct is malformed. Invalid grant provenance refuses with
  `consent_notice_invalid`; invalid denial provenance omits all three wire
  fields while denial applies and that warning joins every other applicable
  warning. Valid tuples are copied before return and survive scope propagation,
  retries and restart completion unchanged. An ordinary denial after a
  forced-minor decision preserves that decision's original receipt and tuple.
- **Unknown = drop at enqueue.** Until an explicit `SetConsent(true)`,
  `Track`/`SessionStart` refuse at enqueue exactly like `Denied`: the call
  returns `false`, nothing is queued, nothing is spooled, and **zero
  analytics bytes leave the device**. The refusal reason is surfaced as a
  distinct code via `ShardPilotSdk.Client.Snapshot().LastError`:
  `consent_unknown` ("ask the player") vs `consent_denied` ("the player said
  no"). A grant opens the pipeline for FUTURE events only — events refused
  before it are gone by design; do not queue events app-side to replay after
  the grant.
- **Grant-only spool with fail-closed wipe markers.** Only an `Initialize`
  that starts from a persisted GRANT loads the offline spool. Any
  non-granted start — `Unknown`, `Denied`, or an unreadable consent record —
  **purges** the spool instead, sweeping every (workspace, app, environment)
  scope's file in the spool directory. If a wipe cannot complete, a
  persisted "wipe owed" marker keeps the spool disabled (nothing loaded,
  nothing appended) until a later launch removes the file for good —
  unproven events can never ride a later grant.
- **Denial is a full stop.** `SetConsent(false)` clears the queue, discards
  the in-flight batch, wipes the spools (same owed-wipe markers), clears
  crash breadcrumbs, and also suppresses crash capture (the anonymous id is
  the consent/erasure subject). A denial whose persist fails still applies
  process-wide (fail-closed shadow) so a re-init cannot resurrect the stale
  grant.
- **Per-scope consent receipts, sequenced before the batch.** An explicit
  receipt in `0.13.0-alpha` uses `workspace_key`, `app_key`
  and `environment_key` without retired `*_id` wire aliases.
  Actor/kind, provenance and idempotency are unchanged. An explicit
  in-session decision posts a small consent receipt to the analytics ingest
  service on a dedicated consent-recording call — since `v0.7.0-alpha` through
  a DURABLE OUTBOX (persisted, retried until acknowledged, delivered in
  decision order PER ACTOR — see the parking gate below), not fire-and-forget (separate
  from the events batch; carries the scope, the decision, the original
  `decided_at`, and an idempotency key). Each (workspace, app, environment)
  scope records its own receipt, including clients registered after the
  decision. On a grant, the batch publish path hands the still-pending
  receipt to the HTTP sender BEFORE the event batch within the same flush
  cycle — handoff ordering only; the batch never waits on the receipt's
  network acknowledgment. Since `v0.7.0-alpha` receipts are NOT in-memory:
  the outbox above persists them and retries until acknowledged, so an app
  that dies or stays offline before delivery still ships the receipt on a
  later launch. (Pre-v0.7 they were fire-and-forget and a lost receipt was
  really lost — do not carry that assumption into a v0.7 integration.)
- **Server-side consent is a separate ledger.** A publishable (Mode A) key
  can record **denials only** — the service rejects a grant receipt posted
  with it. Grants are recorded server-side through a consent-write-capable
  backend credential (Mode-B path or your backend). So on a
  strict-consent-enforcing workspace, `SetConsent(true)` on the device
  may re-open the LOCAL pipeline after an ordinary denial and a completed
  purge, but the server keeps suppressing that actor's
  events until a grant lands from your backend. If accepted events never
  appear in ShardPilot, check this first.
- **Forced-minor consent exists since `v0.7.0-alpha`.** Record it with
  `ShardPilotSdk.SetConsent(ShardPilotConsentDecision.DeniedForcedMinor)` — or
  the `(decision, out string error)` overload for the refusal reason.
  `SetConsent(bool)` cannot create the state, and it gates like a denial.
  An ordinary grant is refused with `consent_forced_minor`, even after purge
  recovery. An ordinary denial succeeds without changing the state or minting
  a reasonless replacement receipt; the original receipt keeps its delivery
  lifecycle. This restriction survives reload, receipt acknowledgment and marker
  retirement independently of the latest shared decision. Replaced clients
  consult it before accepting ordinary decisions. An unreadable or malformed
  restriction refuses grants with `consent_restriction_unreadable` until storage
  is readable again. An ordinary backend grant cannot reverse the restriction.
  Before `v0.7.0-alpha` this SDK was boolean-only and the state existed on the
  Defold SDK alone.
- **Purge and actor failures:** an applied denial succeeds and reports an
  unfinished purge as `spool_purge_failed` in `Warnings`. The additional
  `out error` diagnostic names the first warning, with durability before actor
  validation; applied decisions keep `Error` null. An ordinary-denied
  grant is refused with that `Error` until the purge succeeds. Grants retry only
  recorded per-plane debt: completed sweeps stay completed, and event-only debt
  preserves the independent crash backlog. Persistence
  barriers retry the debt without changing consent. A shared grant that a scope
  refuses or holds records scope debt before returning. A persisted marker
  prevents restart from reopening that scope without its own durable grant
  receipt; other scopes retain their grants. Failed marker writes are logged
  and cannot guarantee the refusal across process exit. An invalid or overlong
  configured actor can deny locally with warning `consent_actor_invalid` but
  creates no receipt; a grant fails with that error instead of switching to a
  replacement actor. Startup, registration refresh and storage healing enforce
  the same restriction: a restored grant remains locally `Unknown`, and a
  stored denial keeps its state. Leave `AnonymousId` unset to use the usual
  resolved anonymous actor; repeated normalization preserves that omission,
  while an explicitly blank value remains invalid.

## Sending analytics events

```csharp
// Call from the player's consent handler. Every overload returns the same
// application result, with durability notices separate from refusal errors.
var consent = ShardPilotSdk.SetConsent(ShardPilotConsentDecision.Granted);
if (!consent.Success)
{
    Debug.LogWarning("ShardPilot: consent refused (" + consent.Error + ").");
    return;
}
foreach (var warning in consent.Warnings)
    Debug.LogWarning("ShardPilot: consent applied with warning (" + warning + ").");

ShardPilotSdk.SessionStart();        // emits wire event app.session_started
bool queued = ShardPilotSdk.Track("app.screen_view",
    new Dictionary<string, object> { {"screen_name", "main_menu"} });

await ShardPilotSdk.FlushAsync();    // Task<ShardPilotResult>; default timeout 10s
```

`CloseAsync` is NOT shown here on purpose. A `consent_pending` result is
RETRYABLE and the client deliberately stays alive for it — the durable outbox
still owes a receipt write — so a close is a barrier with its own contract,
not a teardown call to append to this list. See below.

**Where that barrier runs — and whether to write one at all.** The SDK installs
its own lifecycle host and persists pending events and consent receipts
SYNCHRONOUSLY from Unity's quit and pause callbacks, so an integration owes no
shutdown code for admitted analytics durability — with one limit: `PersistPendingEvents` has
nowhere to write when the event spool is off (`SpoolEnabled = false`) or
unavailable (WebGL, where it is a documented no-op), so those configurations
still lose the in-memory event tail on a reported quit. Flush before quitting
there. Never put `CloseAsync` in `OnApplicationQuit`:
that callback returns `void`, so the first `await` hands control back to Unity
and the continuation may never run.

A barrier buys only the final NETWORK flush, and it belongs in
`Application.wantsToQuit` (the one hook that can CANCEL a quit). Four things it
must handle, each a way to be worse than no barrier:

1. RE-ENTRANCY — a second quit during the barrier must be vetoed, not allowed
   to start a second one. The facade detaches the client synchronously, so a
   concurrent `CloseAsync` returns non-retryable `not initialized`; a handler
   reading that as success quits mid-close. Track in-progress separately from
   complete.
2. PER-ATTEMPT TIMEOUT — argument-less `CloseAsync()` uses the SDK's 10-second
   default, so a wall-clock budget checked only between calls bounds nothing.
   Pass the remaining budget to each call.
3. THE EDITOR — `Application.Quit()` is a no-op there; re-issue with
   `UnityEditor.EditorApplication.isPlaying = false` under `#if UNITY_EDITOR`,
   or the veto leaves play mode running.
4. GIVING UP — `consent_pending` means the receipt write has not landed, and a
   store refusing writes keeps refusing them. Bound the barrier and LOG the
   pending reason; do not quit reporting a success that did not happen.

This persistence contract covers admitted analytics; raw experiment
applications remain memory-only until sealing and admission (see Experiments).
An exit Unity REPORTS snapshots eligible admitted events through the SDK's hook
with or without a barrier. A termination raising no callback (OS task-kill, SIGKILL,
hard crash) runs that hook no more than it runs yours — what is spooled
survives, the in-memory tail does not.

- `Track(string eventName, IDictionary<string, object> props = null)` and
  `SessionStart(string sessionId = null, IDictionary<string, object> props =
  null)` return `bool`: `true` = accepted into the pipeline, `false` =
  refused (consent gate, closed client, invalid event, not initialized).
  `Track` is thread-safe.
- Typed progression verbs, the same `bool` contract, plus an overload with
  `out string error` on the client that names the refusal:
  `TrackLevelStart(levelId, attempt[, props])` → `level_start`;
  `TrackLevelComplete(levelId, attempt, durationMs[, score][, props])` →
  `level_complete`; `TrackLevelFail(levelId, attempt, durationMs[, failReason][, props])`
  → `level_fail`. They validate the canonical schema's bounds (`level_id`
  non-empty, `attempt` 1–65535, `duration_ms` and `score` 0–4294967295) and
  refuse with `level_id_required` / `invalid_attempt` / `invalid_duration` /
  `invalid_score` before anything is queued; extras in `props` ride along and
  the typed keys win, while an absent `score` or empty `failReason` erases an
  extra spelling that key. A client whose `Source` is not `Client` is refused
  with `source_not_client` — these schemas pin `source` to `client`.
- Typed ad verb, same contract:
  `TrackAdImpressionRevenue(ShardPilotAdImpressionRevenueArgs args)` uses named
  `ImpressionId`, `Network`, `RevenueMicros`, `Currency` fields (required), and
  `RevenuePrecision`, `AdUnit`, `AdFormat`, `Placement` (optional). Construct the
  value with an object initializer; positional overloads are removed. The
  client also accepts `(args, out string error)` for local refusal details.
  Zero revenue is valid; currency is upper-cased and optional fields are
  omitted when empty. Beyond the SDK's own consent gate the ingest applies the
  workspace's `ad_revenue` posture and can suppress the event
  per event inside an accepted batch with `suppressed_ad_revenue_consent` —
  this SDK cannot see that grant before sending.
  Read `client.Snapshot().LastEventIssue` for the latest non-accepted `status` or
  `status:code`, including that suppression, even with logging off. The value
  starts empty and survives accepted-only batches; it is diagnostic history,
  not a delivery guarantee for a particular impression. Suppression counts
  under neither `Accepted` nor `Rejected`. An Info-level logger can also report
  the per-event verdict. Consent (`consent_denied` / `consent_unknown`), the
  session attach and queueing are `Track`'s; a closed client answers `shutdown`.
- Events ride the app-first envelope (`workspace_key`, `app_key`,
  `environment_key`, `event_id`, `session_id`, `session_sequence`,
  `event_name`, `anonymous_id`, `app_version`, ...) to
  `POST {IngestUrl}/v1/events:batch`, batched (default 25, max 100), drained
  by one worker every `FlushInterval`.
- Idempotency: the UUIDv7 `event_id` is stamped once at enqueue and never
  regenerated, so re-sends de-duplicate server-side — at-least-once from the
  SDK, exactly-once after ingest.
- The anonymous id is generated and persisted on first `Initialize` and
  stamped on every event; it never rotates mid-session.

## Remote config

Enabled only when `RemoteConfigEnabled = true` and `AppUrl` is set (plus an `sp_` `PublishableKey` — see
Credentials). Exact API:

```csharp
ShardPilotSdk.FetchRemoteConfig(result =>
{
    if (result.Success)
    {
        IReadOnlyDictionary<string, object> values = result.Values;
        long? version = result.Version;     // published config version, when present
        bool fromCache = result.FromCache;  // served from the ETag disk cache
    }
});

// Freshest cached values without touching the network (null when none):
var cached = ShardPilotSdk.RemoteConfig.GetCachedValues();
```

- One fetch = one `GET {AppUrl}/config/v1/{workspace_key}/{environment_key}`
  without user/device identifiers under `Unknown`, `Denied` or
  `DeniedForcedMinor` consent. Set `WorkspaceKey` and `EnvironmentKey` to the
  workspace and environment keys for this route. Granted consent adds
  `X-ShardPilot-Anonymous-Id` (the literal anonymous ID); the path stays two segments.
  Setting `AnonymousId` refuses ASCII controls and leading/trailing spaces with
  `ShardPilotConfigException`; never trim or escape unsafe values into identities.
  With no explicit `AnonymousId`, a corrupt stored value is replaced with a fresh
  UUIDv7 and rewritten when storage is writable; valid stored values stay literal.
  The route uses
  `Authorization: Bearer` with the `sp_` key. The callback runs on the Unity
  main thread. The dispatcher rechecks consent before creating the request;
  a queued denial strips identifiers and attributes, even if followed by a grant.
- **ETag-cached**: responses persist to disk
  (Application.persistentDataPath/shardpilot.remote-config.json); later
  fetches revalidate with `If-None-Match` from the matching identity scope.
  Identifier-free requests never reuse an identified response or its ETag.
  A previous identified cache file can remain on disk after denial until
  replaced or removed. `304` serves the matching cached snapshot
  with `FromCache == true`. A transient failure (connection error, timeout,
  408, 429, 5xx, malformed body) serves the cache with `FromCache == true`
  and `Error` set. `401`/`403` fail **closed** — no cache serving. Any other
  status (e.g. 404) is a permanent failure — no stale values masquerading as
  a healthy fetch.
- Config delivery remains available without granted analytics consent;
  identifiers and targeting attributes require a grant.
- **Targeting attributes also require an explicit opt-in** (dark by
  default): `ShardPilotConfig.RemoteConfigAttributesEnabled =
  true` (default `false`; requires `AppUrl`) plus
  `ShardPilotSdk.SetRemoteConfigAttributes(attributes)` (defensive copy;
  `null`/empty clears) appends the stored attributes to the fetch URL as
  query parameters ONLY while analytics consent is `Granted` at fetch time —
  normalized per fetch with the experiment vocabulary (`geo`, `app_version`,
  `device_type`, `install_date`, `user_segment`, `custom_attribute_<name>`;
  trimmed, ≤512 bytes, ≤64 pairs, sorted; out-of-vocabulary names dropped).
  Under `Unknown` or either denied flavor, the request omits identifiers
  and attributes. With the opt-in off, a granted request still carries its
  client ID but no attributes. The ETag cache distinguishes identifier-free
  and identified requests; attributes are not part of the scope, so the
  last-known-good snapshot reflects the last served body regardless of
  attribute changes within that scope.
- Config fetch only: no experiment assignment, no exposure events, no stats.

## Experiments: explicit application

**The explicit application flow is included in `0.11.0-alpha`.** The owner
publishes its tag after the version-bump merge. Verify that tag and the target deployment's assignment,
application and sealed-fact ingest support before enablement.
`Supports("experiments_assignment")`
and `Supports("experiments_age_band")` describe the consumer and age helper;
neither promises server readiness or distinguishes the explicit application flow.

Set `ExperimentsEnabled = true`, `AppUrl`, an `sp_` `PublishableKey`, and
`Source = Client` (the default) before `Initialize`. The flag defaults false.
Analytics consent must be granted and a client-id assignment must have usable
serving pins. The game determines age eligibility: use
`ShardPilotExperimentAttributes.WithAgeBand(ShardPilotExperimentAgeBand.Adult, attributes)`
only after establishing the applicable threshold; `Unknown`, `UnderThreshold`
and an absent declaration are ineligible for client-id experiments. The helper
is not age verification and cannot override a forced-minor consent refusal.

After initialization, a successful consent grant and `SessionStart()`:

```csharp
ShardPilotSdk.FetchExperimentAssignment("checkout_flow",
    ShardPilotExperimentAttributes.WithAgeBand(ShardPilotExperimentAgeBand.Adult),
    result =>
    {
        if (!result.Ok || !result.Assigned) return; // retain the control experience
        var variant = ShardPilotSdk.ApplyExperimentVariant("checkout_flow");
        if (variant == null) return; // do not apply a refused treatment
        // Use variant here; the game owns the treatment implementation.
    });
```

In the later outcome handler, after an explicit application in this session:

```csharp
bool captured = ShardPilotSdk.TrackExperimentOutcome("checkout_flow", "revenue_cents", 999);
// This is local capture, not CP acceptance or analytics delivery.
```

- **Migration from reads:** fetches, `result.VariantKey`, `GetExperimentVariant`
  and `GetExperimentPayload` never record an exposure. Use
  `ApplyExperimentVariant` or `ApplyExperimentPayload` at the treatment-use
  point instead of the old getter. Both share one occurrence per
  (experiment, version, subject) per analytics session. Both return `null` on
  refusal, but the payload method also returns `null` when the optional payload
  is absent after the application was recorded. That `null` is ambiguous:
  decide control versus treatment from `ApplyExperimentVariant` or the
  assignment result after explicit application; use `ApplyExperimentPayload`
  only to read data as a defensive copy. An absent payload must not switch an
  already applied treatment to control.
- **Additional use and outcomes:** `TrackExperimentExposure` captures another
  application with a fresh occurrence ID per successful call. Do not pair it
  with the initial Apply call to record that same treatment use twice.
  `TrackExperimentOutcome` requires the last explicit application in this
  session, captures its pins, and creates a new occurrence per call. Values
  must be finite integers in `[-9007199254740991, 9007199254740991]`; there is no
  rounding or scaling. `revenue_cents = 999` means USD 9.99.
- **Service boundaries:** the worker posts captured requests to
  `/api/cp/v1/runtime/experiments/exposures` or
  `/api/cp/v1/runtime/experiments/outcomes` on `AppUrl`, with the publishable
  key. CP returns `{ fact, seal }`; the SDK checks structure and request
  binding, while analytics authenticates the seal. A successful Apply/Track
  return means local capture, **not CP acceptance or analytics delivery**.
- **Analytics wire:** only matching sealed facts enter the consent-first
  pipeline. Their envelope uses `schema_version = 2`, `source = client`,
  `workspace_key`, `app_key`, `environment_key`, `attestation_seal`,
  `anonymous_id`, and the captured `session_id`. CP supplies the signed fact's
  `cpx1_` event ID and props (`assignment_unit = client_id`,
  `assignment_key = sfk1_…`, `attestation = client_attested`, experiment/version,
  variant, and outcome fields when applicable). Raw `spcid_` goes only to CP;
  analytics omits it and `user_id`. Ordinary gameplay retains UUIDv7 IDs.
  `Track`/`Enqueue` cannot be used to fabricate reserved experiment events.
- **Retry and revocation:** transient apply failures, `408`, `429`, `5xx`, and
  malformed/mismatched success bodies retry the same captured request and ID.
  Queue pressure after sealing retries admission without reapplying. Auth
  failures latch the lane until a fresh authorized fetch; terminal apply
  failures discard the affected application. A consent epoch change revokes
  pending dispatch and late seals. An age refusal withdraws the experiment's
  applications and retained fact keys while preserving unrelated facts.
- **Lifecycle and storage:** unsealed applications are memory-only; pause/quit
  persistence does not send apply HTTP or journal raw requests. Admitted sealed
  facts follow the analytics spool's limits and preserve their fact, seal and
  ID on replay. A replaced client refuses new applications but may drain
  captured ones subject to consent, age withdrawal and a bounded close budget.
  Complete crash-safe withdrawal is **not guaranteed** for retired-scope
  compound storage failures: see [#181](https://github.com/shardpilot/shardpilot-unity/issues/181).
  An absent assignment is not proof of a durable purge. Legacy unsigned
  experiment spool entries do not become sealed facts on upgrade; cached
  assignments without usable serving pins require a new fetch before Apply.

Use the [full reference](../../../README.md#experiments-dark-by-default) to check
assignment classification, age-attribute compatibility and on-device records.

## Crash reporting

Version `0.13.0-alpha` emits `app.key` (the configured `AppKey`)
for fresh JSON crashes and tombstone metadata, without an `app.id` alias.
Crash/native/debug identities and dump bytes stay
unchanged.

- **Auto-hooked at `Initialize`** (default `config.CrashHooksEnabled = true`):
  `Application.logMessageReceivedThreaded` (logged exceptions → non-fatal)
  and `AppDomain.CurrentDomain.UnhandledException` (`IsTerminating` →
  fatal). Requires `CrashKey`; without it every capture is a safe no-op
  (`IsCrashReportingEnabled` reason: `not_configured`).
- **One internal lane is NOT sanitized client-side.** If the internal,
  default-disabled native exit-info lane is enabled on Android API 31+, a
  `REASON_CRASH_NATIVE` record carrying a tombstone
  stream ships the OS's RAW AOSP `Tombstone` protobuf bytes unmodified to
  `POST {CrashUrl}/api/v1/crashes/ingest-tombstone` (capped at the
  server's 4 MiB limit); the SERVER decodes, allowlist-strips and symbolicates
  it. This is not a public configuration control. Review privacy before any
  SDK enablement — it is the one place a crash carries a raw OS payload rather than a shape this
  SDK built. It is also OUTSIDE the durable crash spool — the lane is ack-gated
  and no
  spool ever holds tombstone bytes. Retry comes from the OS instead: a
  retryable failure leaves the `ApplicationExitInfo` record unsettled, so the
  next launch re-reads and re-ships it while Android still retains it. A
  permanent reject settles. So durability here is bounded by the OS's
  retention of that record rather than by this SDK's spool.
- Reports go to `POST {CrashUrl}/api/v1/crashes/ingest` as a sanitized
  crash-report JSON body — **not** as analytics events. Sanitized means:
  exception type/reason, managed frames, up to 50 breadcrumbs, OS/device
  context, component `component` slug; no minidumps, screenshots, raw payloads,
  or raw PII; `anonymous_id`/`session_id` are hashed at ingest.
- **Legitimate-interest with a persisted opt-out, independent of analytics
  consent**: ON by default with no first-run decision — analytics consent
  `Unknown` does not block crash reporting. The player opts out via
  `ShardPilotSdk.SetCrashReportingEnabled(false)` (its own persisted
  setting); check status with
  `ShardPilotSdk.IsCrashReportingEnabled(out string disabledReason)` —
  reasons: `not_configured`, `consent_denied`, `opt_out`,
  `settings_read_failed`, `not_initialized`. Two deliberate couplings to
  analytics consent: an explicit analytics **denial** suppresses crash
  capture too, and an **unreadable** analytics-consent record holds the
  crash plane closed until the next explicit `SetConsent` decision.
- **Fail-closed everywhere**: an unreadable or malformed persisted crash
  setting disables the plane (`settings_read_failed`) instead of defaulting
  to sending; disabling stops **collection** as well as sending and clears
  the breadcrumb ring; a failed opt-out persist still applies process-wide.
- **Sampling**: fatal crashes are NEVER sampled. Non-fatal (handled/logged)
  reports are sampled 1-in-`CrashSampleEvery` per client instance; zero
  suppresses all non-fatals but preserves fatal capture. There is
  ONE counter shared by every non-fatal source (managed non-fatals, ANR
  watchdog stalls), so the rate is one in N of the client's non-fatal reports
  in aggregate, never one in N of any single kind.
  The pinned tag defaults to `10` (the cross-SDK canonical shared with the Go
  and Defold SDKs); `v0.6.0-alpha` and earlier defaulted to `1`, sending every
  non-fatal. **Set it explicitly if you depend on the rate** — an integration
  moving up from `v0.6.0-alpha` that emits fewer than 10 non-fatals per client
  instance will now report none of them.
- **Durable** when `SpoolEnabled` and `CrashSpoolEnabled` are true (the
  defaults): every accepted report is persisted at capture time
  (write-ahead, before the send — for a fatal crash "later" does not exist)
  and re-sent on the next launch until acknowledged, oldest first,
  de-duplicated by the stable `crash_id`; a server `Retry-After` persists
  across relaunch; backlog retention is 7 days.
- Manual capture: `ShardPilotSdk.Client.CaptureCrash(ShardPilotCrashEvent
  crashEvent, Action<ShardPilotResult> onSent = null)` returns `false` when
  refused (no key, disabled, closed, invalid, sampled away).

## Offline durability expectations

What "offline-safe" means here — set expectations, don't fight them:

- These rules cover admitted analytics envelopes. A raw experiment application
  is memory-only until sealing and admission; a quit snapshot does not journal
  or dispatch it. See [Experiments](#experiments-explicit-application).
- A batch that fails **transiently** (connection error, timeout, 429, 5xx)
  is persisted and retried; the pending remnant is persisted on app
  pause/quit and by `CloseAsync` (an awaited `CloseAsync` is a durable
  shutdown barrier). A **permanent** rejection (non-retryable 4xx) is
  dropped, never spooled.
- When the in-memory queue (default 1000) is full, new events spill straight
  to the spool and ship on the **next launch** (`Stats.SpooledOverflow`
  counts them) — they are not silently lost, but they are not sent this
  session either.
- Spooled events are removed only after the ingest service acknowledges
  them; re-sends are byte-identical and de-duplicated by `event_id`.
- Caps: `SpoolMaxEvents` 2000 / `SpoolMaxBytes` 1 MiB, oldest-first
  eviction. Spool files are per-(workspace, app, environment) scope, under
  the app-private `Application.persistentDataPath`, and hold only envelope
  JSON — never credentials.
- The spool obeys the consent contract: loaded only under a persisted grant,
  purged otherwise, fail-closed wipe markers when a purge cannot complete.
- **WebGL**: both spools are no-ops (no real synchronous filesystem);
  behavior there is in-memory only.

## Verify your integration

Do not claim the integration works because `Initialize` didn't throw. Verify
against the SDK's observable result surface — return values,
`ShardPilotSdk.Client.Snapshot()` (a `ShardPilotStats` snapshot), and
`FlushAsync`'s `ShardPilotResult`:

1. **Point at a reachable ingest** — your dev/staging ShardPilot stack or a
   local sink; `AllowInsecurePrivateNetwork = true` for plain-http private
   addresses. Default endpoints do not prove deployment availability.
2. **Prove the consent gate first (negative check — fresh install state
   only).** Consent persists across launches, so first read
   `ShardPilotSdk.ConsentState`: if it already reads `Granted` from an
   earlier run, skip this step (or clear the stored decision / test on a
   fresh profile). On a fresh state, after `Initialize` and BEFORE any
   `SetConsent`: `Track("test")` must return `false` and
   `ShardPilotSdk.Client.Snapshot().LastError` must be `consent_unknown`.
   If it returns `true`, you are not talking to the consent-first SDK you
   think you are.
3. **Grant and emit.** `SetConsent(true)`, then `SessionStart()` and
   `Track("integration.test_event", ...)` — both must return `true`.
4. **Flush and check the result.**
   `var result = await ShardPilotSdk.FlushAsync();` — `result.Success` must
   be `true`. On failure, `result.Error` carries the transport reason and
   `result.Retryable` whether the SDK will retry.
5. **Confirm server acceptance in stats.** `var stats =
   ShardPilotSdk.Client.Snapshot();` — expect `Enqueued >= 2`,
   `Accepted >= 2`, `Rejected == 0`, `FailedBatches == 0`.
   `Accepted`/`Rejected`/`Duplicates` are parsed from the ingest service's
   batch response, so a non-zero `Accepted` proves the server accepted the
   events — not merely that bytes left the device. Note `LastError` is a
   latch of the most recent refusal/failure, not a health flag: after the
   step-2 negative check it still reads `consent_unknown` even though
   delivery then succeeded — only a NEW refusal or failure overwrites it.
   (On a strict-consent-enforcing workspace, remember server-side grants:
   see the consent section.)
6. **Crash plane (when configured).** Set `CrashSampleEvery = 1` for
   the test, `Debug.LogException(new InvalidOperationException("smoke"))`
   inside a try/catch, `await ShardPilotSdk.FlushAsync()`, then expect
   `Snapshot().CrashesSent >= 1` (and `CrashesSpooled >= 1` when the crash
   spool is on). `IsCrashReportingEnabled(out var reason)` must be `true`
   beforehand — if not, `reason` tells you why.
7. **Scripted end-to-end pass**: the repo's `Samples~/EngineSmoke` sample
   runs this
   sequence (init → consent → session → event → flush → handled crash →
   flush) against a configurable sink.

## Known limitations

Stated plainly so you can plan around them; do not paper over these in
integration code:

- **Android OS exit-info lanes are internal and default-disabled.** They
  are not host configuration controls. Historical server acceptance observations
  are not a current deployment guarantee: a permanent
  rejection consumes an OS record rather than deferring it. A metadata-only
  JSON record and a raw tombstone take different routes. Owner observation:
  verify current server acceptance before any SDK enablement of these lanes.
- **The public hang watchdog** uses `HangWatchdogEnabled` and
  `HangWatchdogThreshold` (`TimeSpan`); both it and the internal OS lanes
  require the automatic hooks.
- **The Android lanes are decided at `Initialize`, and an opt-out CONSUMES the
  backlog.** `HangWatchdogEnabled` is read once and the watchdog only starts if
  crash reporting is also enabled at that moment, so an app opted out at
  `Initialize` that later calls `SetCrashReportingEnabled(true)` gets no
  watchdog until the next `Initialize`. The next-launch
  `ApplicationExitInfo` pass is worse than deferred under that opt-out (or a
  consent denial): it ships nothing and **settles the backlog permanently** —
  suppression is consume-without-reporting, so those exit records are gone
  rather than waiting. Only a read FAILURE holds without settling.
- **The Android lanes need the crash hooks installed.** The next-launch
  `ApplicationExitInfo` pass (ANR and native-crash alike) and the stall
  watchdog start from internal automatic hook installation, so setting
  `config.CrashHooksEnabled = false` before `Initialize(config)`
  leaves every one of them dead regardless of individual source settings.
- **`.meta` files ship for every importable asset** since `v0.7.0-alpha`
  (package validation verifies every importable path and matching meta,
  with no orphans), so a
  UPM git install keeps stable GUIDs. Earlier tags shipped only two and Unity
  generated fresh GUIDs per machine.
- **No engine-real CI leg.** CI proves the contract via headless EditMode
  tests (Runtime compiled against a UnityEngine shim, NUnitLite). The run script
  verifies that its isolated shards cover the whole discovered suite.
  The real Unity Editor EditMode/PlayMode jobs are license-gated and skipped
  in public CI. Validate in your own Editor/project — the EngineSmoke sample
  is the fastest path.
- **Managed crash hooks only.** No in-process native signal handling
  (segfaults, native plugin crashes) or symbol upload. Hook-captured frames are
  managed, pre-symbolicated `function`/`file`/`line`; internal Android native
  exit-info replay is a separate, default-disabled lane.
- **Consent receipts ride a DURABLE OUTBOX** since `v0.7.0-alpha`: persisted,
  retried until acknowledged, delivered in decision order PER ACTOR (dispatch
  skips receipts parked for an actor this session cannot vouch for, so a newer
  receipt for the current actor can land before an older parked one), so an app
  killed or offline before delivery does not lose the decision. Earlier tags
  held them in memory and lost them. `CloseAsync` reports `consent_pending`
  while a write is still owed and the client stays alive for the retry — see
  the shutdown example.
- **`denied_forced_minor` exists since `v0.7.0-alpha`** — the age-gate
  band forcing decision; see the consent section for how it gates.
- **WebGL**: no spools (in-memory only), and the worker runs as a
  frame-ticked coroutine.
- **Release pin**: this guide targets `v0.13.0-alpha`, including request compression. Use a local package until that tag is published.
