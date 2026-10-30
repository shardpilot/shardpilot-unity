# Security Policy

The ShardPilot Unity SDK is pre-v1 software: the API is unstable and may
change before v1. Assess it against your own production-readiness bar before
using it with production secrets or production player data.

## Reporting a vulnerability

Email **security@shardpilot.com**. That address is published on
ShardPilot's own security page and does not depend on this repository's
settings or on you already having a contact here. You may also use this
repository's private security-advisory flow if it is enabled for you.

Please do not open a public issue for a suspected vulnerability.

Include the SDK version (`package.json` `version`), the Unity version and
target platform, and the smallest reproduction you have.

## What this SDK handles

Each client holds **one to three credentials** depending on what you enable — see below.
A process that constructs several clients for different scopes holds that many
sets, so "three" is a per-client ceiling and not a process-wide one. Each client
writes up to two spools on disk **per (workspace, app, environment) scope**,
and sends analytics and crash reports to
the endpoints you configure.

Clients on different scopes normally do not share a spool, so a process running
more than one has that many pairs — "normally" because the filename carries only
a **short scope hash**, and it fails in two different ways. The hash is the
first eight bytes of the SHA-256 of `workspace + "\n" + app + "\n" +
environment` — so first, sixty-four bits is a collision-resistance question
rather than a guarantee; and second, the parts are joined by a delimiter with
**no escaping or length prefix**. Construction trims leading and trailing
whitespace, so the alias needs an **embedded** newline rather than a trailing
one: a workspace id of `a\nb` with app `c` hashes the same text as workspace
`a` with app `b\nc`, exactly, and the two configurations then share both spool
filenames. That half needs no luck at all — it is a property of the encoding,
reachable deliberately by any deployment whose identifiers can carry a
newline — and the same is true across `SpoolDir`
values: the scoped filenames are created under whatever directory is configured,
so two clients on the SAME scope with different directories, or one client whose
directory changes between launches, leave files under each path. Nothing
migrates or sweeps the old ones. Neither spool is unconditional. On **WebGL neither is created in a player build**: browser
persistence is not a synchronous filesystem, and the spool is a documented
no-op there. The guard is `#if UNITY_WEBGL && !UNITY_EDITOR`, so with the WebGL
target selected **in the Editor** both symbols are defined, the early return
does not happen, and real filesystem spools are created — Editor sessions
against a WebGL target write to disk, whatever the shipped build does. Elsewhere the **event spool** exists while `SpoolEnabled` is on
(the default), and the **crash spool** additionally requires `CrashSpoolEnabled`
(also on by default) *and* a non-empty `CrashKey` — without that key a
stored report could never be sent, so none is written. Either can also fail to
open, in which case the client runs without it.

**Turning a spool off stops new writes; it does not delete what is already
there.** The factories simply return `null`, so files written by an earlier
enabled launch stay on disk under the same directory, and a deployment that
flips `SpoolEnabled` or `CrashSpoolEnabled` off — or drops `CrashKey` —
inherits whatever the previous build left. (A consent *denial* is different: it
wipes prior-launch spool files whether or not the spool is enabled now.) The
directory is not emptied for a second reason either: the experiments cache
resolves from `SpoolDir` on its own and is not gated on that switch.

The credentials, and when each is present:

- `ShardPilotConfig.PublishableKey` — the publishable ingest key. Required whenever
  `TokenProvider` is **absent** (the ordinary analytics configuration), and
  required alongside `TokenProvider` when any of `RemoteConfigEnabled`,
  `ExperimentsEnabled` or `RemoteConfigAttributesEnabled` is true. `AppUrl`
  supplies an address only: a token-only configuration may keep that address
  without the publishable key when all three switches are false. Consent
  receipts still go out there, resolving the provider's JWT;
- `TokenProvider`'s JWT — fetched at runtime, cached after its first return,
  and **never embedded in the build**. It is of course transmitted: it is the
  Bearer credential on Mode-B analytics and consent dispatch. This paragraph is
  about what an attacker can extract from your shipped binary, not about what
  leaves the device — for that, see the egress section. **Its lifetime is whatever your provider gives it, and the SDK
  neither requires nor verifies a bound:** `new ShardPilotTokenResult(token)`
  without an expiry sets `ExpiresAt` to `DateTimeOffset.MaxValue`, and the
  manager then reuses that token for the whole process until a 401 forces a
  refetch. If you want a short-lived credential, return an expiry;
- `ShardPilotConfig.CrashKey` — a static `crash:write` Bearer, required for
  crash capture, and neither of the two above.

So crash capture plus remote config plus `TokenProvider` — a combination the
SDK explicitly supports — holds all three at once. That is the case a rotation
review is most likely to under-count, and rotating the analytics credential
still leaves the crash one in place.

Whether any of them is **embedded** depends on how you supply it: `PublishableKey` and `CrashKey` can be
literals compiled into the game, or delivered at runtime, and `TokenProvider`
fetches its JWT at runtime instead of shipping one at all — with whatever
lifetime your provider gives it, per the note above. So the
rotation point is real — rotating the analytics credential leaves the crash one
in place — while binary-extraction exposure applies only to the parts you chose
to compile in.

**What leaves the device is not only analytics and crash reports.** The
outbound paths, derived from the sources rather than recalled:

- `{IngestUrl}/v1/events:batch` — the analytics batches;
- `{CrashUrl}/api/v1/crashes/ingest` — crash reports;
- `{CrashUrl}/api/v1/crashes/ingest-tombstone` — see the raw-payload note below. **Only
  for tombstones of at most 4 MiB** — the cap is inclusive, so a payload
  exactly at it is uploaded. Above it the protobuf is not uploaded at all and
  a metadata-only JSON report goes to the crash endpoint instead;
- `{AppUrl}/api/cp/v1/runtime/experiments/assignment` — **a GET whose
  QUERY STRING carries the data**: `app_key`, `environment_key`,
  `experiment_key`, `subject_key` and every normalized targeting attribute. The
  subject identifier and host-supplied attribute values therefore reach proxy,
  gateway, cache and access logs along the way, which is the same exposure this
  document calls out for granted remote-config requests — there the client ID
  is in the path, here the subject is in the query, and ordinary HTTP
  infrastructure can retain both. **Granted analytics consent only:** with consent
  unknown, denied or forced-minor the consumer returns before minting a
  subject, and re-checks before dispatch, so nothing leaves the device;
- `{AppUrl}/config/v1/{workspace_key}/{environment_key}` — under `Unknown`,
  `Denied` or `DeniedForcedMinor` consent, config remains available without a
  user/device identifier or targeting attributes in the URL, headers or body.
  Configure the workspace and environment **keys** for this route. With
  **Granted** consent, the SDK appends the anonymous/client ID as a PATH
  segment. Normalized targeting attributes are also appended as QUERY
  parameters when `RemoteConfigAttributesEnabled` is true (default **false**).
  Those granted-request identifiers and attributes can reach proxy, gateway,
  cache and access logs. Turning the attribute flag off omits only attributes;
  the client ID still travels with granted consent. The dispatcher rechecks
  consent before creating the request and removes both after a queued denial,
  including a denial followed by a new grant. Identifier-free requests use a
  separate cache scope and never send an identified response's ETag.
  Omitting `ShardPilotConfig.AnonymousId` makes the SDK **mint and keep its
  own** anonymous-ID record; supplying one skips `LoadOrCreate`. A supplied
  value can still persist: a consent decision writes it into the outbox and
  decision record, and a **granted** config fetch embeds it in the cache scope.
  Denial does not erase a previously identified cache file; it remains on disk
  until replaced or removed, although identifier-free fetches cannot reuse it;
- `{IngestUrl}/v1/consent` — a consent **receipt** whenever the host records an analytics
  consent decision. `BuildWireBody` sends `workspace_id`, `app_id`,
  `environment_id`, `actor_identifier`, `categories` — an **object**, whose
  `analytics` boolean carries the decision itself — `decided_at`,
  `idempotency_key`, an optional `reason`, and — while
  `ConsentKindEmissionEnabled` is at its default `true` — an actor `kind` that
  distinguishes an anonymous actor from a verified user. One of those is worth
  pulling out: `kind` is an identity classification rather
  than a transport detail, so turning the setting off omits it from the
  **request** and not from the stored receipt — see the outbox entry in the
  storage section.

The **analytics batch** and the **two crash endpoints** are the data plane —
the telemetry you configured the SDK to send. The **experiment assignment**,
**remote config** and **consent receipt** are configuration and consent calls
that can also carry player-linked identifiers under the conditions above.
Remote config omits those identifiers without granted consent. A deployment
pointing these calls at different hosts should account for each path.

No count here on purpose: an earlier revision said "the last four", which was
a claim about list ORDER that broke the moment the list changed and grouped
the tombstone upload with those calls, where it does not belong.

**Crash collection does not wait for a consent decision.** With `CrashKey`
configured, a fresh install has crash reporting **on**: the stored opt-out
record is absent, which `ShardPilotCrashSettings.Load` reads as enabled, and
both crash lanes gate on `IsDeniedState` — a *denied* decision, not an unknown
one. So on first run, before the player has answered anything, both crash
endpoints may send, the raw tombstone lane included. Analytics batches and
assignment requests do not behave this way; if your consent flow assumes
nothing leaves the device until the player answers, that assumption does not
hold for the crash plane.

**The decision has to be made before the client is constructed, not after.** The
constructor registers with the dispatcher and calls
`StartDispatchAfterRegistration` — the crash re-send pass included — before it
returns, and `ShardPilotSdk.Initialize` then installs the crash hooks that start
the ANR lane. A `SetCrashReportingEnabled(false)` on the next line of host code
can therefore arrive after a report spooled by an earlier unknown-consent
session, or an Android exit-info record, has already gone out. **Two levers hold it, and both are set before or at
initialization.** The first is `CrashKey`: construct the client without one
until the player has answered, then reinitialize with it. The second is
`config.CrashHooksEnabled = false`, set before `Initialize(config)` and
bounded by the two limits described below.

The spool flags are not a second option, and it is worth saying why, because
they read like one. `CrashSpoolEnabled` controls **durability, not sending** —
`CaptureCrash` hands the report to the sender whether or not a spool exists, so
turning the spool off removes the retry, not the send. The internal exit-info
switches govern only their OS lanes and are not public configuration controls;
`HangWatchdogEnabled` is a separate public source control. Disabling individual
sources does not close every path while `CrashKey` is set.

**The second lever is the configuration field `CrashHooksEnabled`:** set it
before initialization to skip internal automatic hook installation. Because the
ANR lane is created inside those hooks, this suppresses automatic managed
capture and every hook-owned ANR source at once.

```csharp
config.CrashHooksEnabled = false;
ShardPilotSdk.Initialize(config);
```

Two limits matter before you rely on it: it does not stop an
explicit `CaptureCrash` call your own code makes, and it does not stop the
re-send of a report a previous launch already spooled — the client is
constructed, and starts dispatching, before automatic hook installation is
considered.
On a fresh install with no prior crash spool, and with no explicit capture of
your own, it holds.

**And the crash spool retains one of them.** When the internal ANR exit-info
lane produces a report carrying a trace, the thread dump is serialized into the spooled record
as `raw_text` — size-bounded but otherwise unfiltered — so the unsanitized
payload described below is not only in flight, it is at rest in the crash spool
until the report drains.

**Two internal, default-disabled paths can send unsanitized OS data.** If
the native exit-info lane is enabled inside the SDK on Android API 31+, a
tombstone found in the OS's exit-info stream is uploaded
as the **raw AOSP `Tombstone` protobuf, byte-for-byte unmodified** — the SDK
never decodes, strips or truncates it, because the server owns parsing and
allowlist projection. That payload is materially richer than the JSON crash
lane's fields (which is why "sends crash reports" understates it), and it is
one of two paths on which unfiltered operating-system data leaves the device —
and only for tombstones of at most 4 MiB, since one ABOVE the cap is not
uploaded at all.

The other is the **ANR thread dump**: if the internal ANR exit-info lane is
enabled, an Android exit record carrying a trace stream has that trace copied
into the event as
`raw_text`, and the crash sanitizer applies only a **size bound** to it — no
filtering. A thread dump names classes, methods and, depending on the app,
whatever is on the stack.

**How to read the field lists below.** They are a **dated derivation, not a
promise**: each was produced from the serializer as it stands at `0fc142d`, and
nothing keeps them in step with later changes. Beside each is the command that
produced it. Run it — the output shows both the names and their nesting, which a
prose list cannot — and if it disagrees with the text, trust the command: the
code moved and this document did not.

```
# the consent receipt as SENT
git show 0fc142d:Runtime/ShardPilotConsentOutbox.cs \
  | awk '/string BuildWireBody/,/^        }$/' | grep -E "builder\\.Append\\(['\"]"

# the consent receipt as STORED - note the three the wire never carries
git show 0fc142d:Runtime/ShardPilotConsentOutbox.cs \
  | awk '/static string Serialize\(List/,/^        }$/' | grep -E "builder\\.Append\\(['\"]"

# the experiments cache: record root, then one entry
git show 0fc142d:Runtime/ShardPilotExperiments.cs \
  | awk '/static string SerializeRecord/,/^        }$/' | grep -E "builder\\.Append\\(['\"]"
git show 0fc142d:Runtime/ShardPilotExperiments.cs \
  | awk '/static void AppendEntryObject/,/^        }$/' | grep -E "builder\\.Append\\(['\"]"

# the ANR era line
git show 0fc142d:Runtime/ShardPilotAnrEraStore.cs \
  | awk '/string SerializeEraLine/,/^        }$/' | grep -E "builder\\.Append\\(['\"]"

# the remote-config cache - `body` is the complete server response
git show 0fc142d:Runtime/ShardPilotRemoteConfig.cs \
  | awk '/static string SerializeCache/,/^        }$/' | grep -E "builder\\.Append\\(['\"]"

# the consent decision record, and the denial marker that delegates to it
git show 0fc142d:Runtime/ShardPilotConsent.cs \
  | awk '/string SerializeDecision/,/^        }$/' | grep -E "builder\\.Append\\(['\"]"

# the Android exit-info cursor - colon-separated, not JSON
git show 0fc142d:Runtime/ShardPilotAnrExitInfo.cs \
  | awk '/public string Serialize\(\)/,/^        }$/'

# the population, part 1: records that go through a serializer
grep -rnE 'static (string|void) (Serialize|Append)[A-Za-z]*\(' Runtime/*.cs

# the population, part 2: records written DIRECTLY through the key-value store
grep -rnE '(store|_store|Store)\.Set\(' Runtime/*.cs | grep -v '^\s*//'

# the population, part 3: bytes that reach disk without the store at all -
# the one-shot File.* helpers AND the stream forms, which are two searches
# because they were two omissions
grep -rnE 'File\.(WriteAllText|WriteAllBytes|AppendAllText|Move|Replace)\(' Runtime/*.cs
grep -rnE 'new (FileStream|StreamWriter)\(' Runtime/*.cs
```

These searches identify records from their serializers, key-value writes and
file writes. Part 1 also includes the wire-only serializers in
`ShardPilotJson.cs` — batches, crash ingest, envelope and tombstone metadata —
which belong to the egress section rather than the storage inventory.

**All three parts are needed to cover the storage mechanisms.** Part 1 finds
records with serializers; the consent downgrade flag, exit-info suppression
stamp, crash `Retry-After` seconds and sweep-owed markers have none. Part 2
finds key-value store writes; the `.condemned` and `.dropped` sidecars instead
use inline records written with `File.WriteAllText`.

**Two blind spots remain in both commands, named here rather than discovered
later.** Neither shows a value that is a JSON **key**: the experiments cache
stores each experiment key as the dynamic property name inside `entries`, so the keys themselves are retained even though no field is named for
them. And neither says which store a record lands in — that is
`IsFileRoutedKey` (`Runtime/ShardPilotPersistence.cs:216-224`), and it is the
authority over any grouping in this document.

Two mistakes this document made before those commands existed are why they are
here: `analytics` was listed as a top-level receipt field when it sits inside
`categories`, and `scope` was listed on each cache entry when it is written once
at the record root. Both came from searching for quoted key names, which shows
names and hides levels. The commands print the appends in order, so the braces
are visible.

**What `SpoolDir` moves** — and note that an atomic rewrite interrupted
after its temporary file is written leaves a complete copy behind as
`<event-spool>.tmp`, `<crash-spool>.tmp` or `<era-file>.tmp`, so the directory
can hold player data under names the list below does not mention — the event
spool, the crash spool, the
experiments assignment cache (shardpilot.experiments.json) **with its
two sidecars, which do not share a schema**: `.condemned` is a whole-scope
marker holding `scope` and `as_of_ticks` and naming no experiment, while only
`.dropped` entries carry `experiment_key` — the cache is itself an
identity-bearing record. Per entry, `AppendEntryObject` serializes
`assignment_key`, `variant_key`, `version`, `assignment_unit`, both fetch
timestamps (`fetched_at`, `fetched_at_ticks`), `subject_key`,
`subject_fact_key`, the normalized `attributes` as `name`/`value` pairs, and
**the `variant_payload`**, which is complete only to a
point: `AppendValue` writes `null` for anything reached at nesting depth 16 or
deeper, so a payload nested past that is persisted truncated rather than
whole. The experiment key is retained too, as the
dynamic property NAME under `entries` —
no field is serialized for it, so a field list cannot show it and the commands
above will not print it; `scope` is written once at the record root
by `SerializeRecord`, not on each entry — **and it is not structural metadata**:
`BuildScope` composes it from workspace, app, environment, the subject ID, the
remote-config base URL and a **12-hex-character SHA-256 prefix of your
`PublishableKey`**. A credential fingerprint is therefore persisted in this cache — not
the key, but a stable derivative that **distinguishes** which key wrote the file
across rotations — six bytes of a SHA-256, so it identifies a key only as far as
collision resistance goes, and two keys sharing that prefix share the cache
scope — in a directory the consumer may point anywhere — arbitrary
server-supplied content whose shape this SDK does not constrain, so what lands
in a custom `SpoolDir` is whatever your experiment configuration puts
there. A cache an earlier version wrote can still hold an `owed` section, each
capture a full copy of an assignment; it is ignored and the next cache write
drops it. A record-level
`fact_key_history` section holds, per experiment key, the record subject's
earlier `subject_fact_key` values: the keys of versions a republish replaced or
of entries a drop deleted, kept only while a queued, spooled or owed fact still
carries one, so that an `age_ineligible` refusal withdraws that fact too.
Alongside it, the ANR era snapshot
(`shardpilot.aei-era.*`). **The era snapshot is itself a consent record**, not
just crash attribution. `SerializeEraLine` writes `era_id`, `started_at_ms`,
`ended_at_ms`, `anonymous_id`, `app_version`, `build_id`, `consent` and
`crash_enabled`. It does **not** record a crash opt-out: an
entry is created only while the crash plane is open **and** `SpoolEnabled` is
on, and always with that flag set, so an opted-out launch produces no entry
rather than an entry describing the opt-out. An inventory naming only the cache leaves the two
sidecars behind, and their schemas differ as described above: `.condemned`
carries `scope` and `as_of_ticks`, `.dropped` entries carry `experiment_key`.

Everything in this group inherits the custom location's sharing, backup and
access properties, and all of it is player-linked: assignments and subject
keys in the cache, consent state and the anonymous ID in the era snapshot.

**What stays at `Application.persistentDataPath` whatever you set** — the
remote-config cache (shardpilot.remote-config.json, the full
`{scope, etag, fetched_at, body}` record, where **`body` is the complete server
response verbatim**. Its `scope` is workspace, environment,
client ID and base URL. The client component is empty for identifier-free
requests; an earlier identified record can remain on disk after denial but
cannot be reused by those requests. The first three components are
`Uri.EscapeDataString`-escaped before joining, and the base URL is appended in
**plaintext normalized form**,
so it is readable as-is by anything that opens the file — whatever your remote configuration contains, this SDK does
not constrain it) and the file-routed key-value
store, which holds the **consent outbox** (workspace / app / environment,
and the fields `Serialize` writes per receipt:
`idempotency_key`, `workspace_id`, `app_id`, `environment_id`,
`actor_identifier`, `kind`, `analytics`, `decided_at`, `reason`, plus three
written **only to the file, never to the wire** — `anonymous_id` when the entry
has one, `superseded` once it is superseded, and `decision_seq` when set. Those
three are the reason this list matters: **the stored receipt holds more than
the request that carries it**, so a review that reconstructs retention from the
egress inventory alone will miss them. Two more worth naming:
`actor_identifier` is the anonymous ID for an anonymous actor and
**`ShardPilotConfig.UserId` for a verified one**, so a verified receipt
persists the identifier you supplied — and, via `anonymous_id`, can persist
both; and `kind` is kept even when `ConsentKindEmissionEnabled` is off, because
that setting gates the wire body and not the file), the withheld-grant
obligation markers (each retaining `idempotency_key`, `decided_at`, an optional
`decision_seq` and `receipt_durable`, and surviving independently of any outbox
receipt), the consent denial-marker witness slot (the serialized denial state,
anonymous ID, decision timestamp and sequence), **the per-actor owed
denial-witness records** (`shardpilot.consent.owed-denial-witness.*` — one per
actor, carrying the serialized denial state, anonymous ID, decision timestamp
and sequence; distinct from the single witness slot above), the experiments
condemned-facts purge markers (each storing **every affected event ID**,
newline-separated, so the marker links this record to the individual events it
is waiting to delete), and the experiments subject id. That group
contains player identity and consent decisions: redirecting the spools moves
the telemetry and leaves the consent records where they were.

**A third location, and it is not a file set.** Records the SDK does not
file-route go through Unity's `PlayerPrefs`: the fail-closed deletion markers
(written when deleting a spool file or listing its directory fails — the marker
**value is the exact failed path**, so a custom `SpoolDir` appears
verbatim here and outlives the file it names), the Android exit-info cursors
(`shardpilot.aei_cursor.*` — not opaque positions but
`timestampMs:reason:pid:definingUid`, so each retains the **OS process ID and
defining UID** of the exiting process; `IsFileRoutedKey` does not list this
prefix, so they stay in the platform store) and the exit-info suppression
stamps beside them, the consent **downgrade-pending** flags (one global, one per
scope), the crash `Retry-After` seconds, the sweep-owed markers whose **value is
a directory path**, the spool suppression markers
(`shardpilot.spool.suppressed.*`, up to eight **event IDs** each, persisted when
a provisional append cannot be undone after a close race — they can outlive the
spool file itself), the stable anonymous ID **when the SDK minted it** — supply
`ShardPilotConfig.AnonymousId` yourself and `LoadOrCreate` never runs, so the
SDK keeps no anonymous-ID *record* of its own. That is not the same as not
persisting the value: a consent decision copies it into the outbox receipt as
both `actor_identifier` and `anonymous_id`, and a granted remote-config fetch
embeds it in the cached `scope` (identifier-free fetches use an empty client
component). A supplied ID can still land on disk, in those records
rather than this one — the
analytics-consent decision — which is not only a state: `ShardPilotConsent.Save`
stores the decision-time `anonymous_id`, `decided_at` and `decision_seq` with it,
so the primary consent record is itself identity-bearing — the crash-reporting
opt-out, the Android exit-info
cursors and several fail-closed markers. `PlayerPrefs` is the platform's own
store — on most platforms a registry key or a plist rather than a file in
either directory above — so `SpoolDir` does not move it.

**But not only there — and not always.** When `PlayerPrefs` **throws**, the SDK
*may* fall back to the same persistent-data file store rather than lose the
record. That is the only trigger: a `Save()` that returns normally without
persisting is indistinguishable from success, so `Set` returns before the
fallback is reached and **no second copy is written**. The undetectable case
loses the record rather than duplicating it. For the **consent decision and the crash opt-out** it deliberately does
not: if the previous value cannot be read, the write is reported failed instead
of being written somewhere that could later win over the truth. And a `Save()`
that returns normally without actually persisting is undetectable from inside
the SDK, so neither store is guaranteed in that case. So the SDK-minted anonymous ID, the
consent decision and the opt-out can exist as `PlayerPrefs` entries, as
persistent-data files, or both, and an erasure or backup review that covers
only the platform store can miss a surviving copy. Which happens is a runtime
condition, not a configuration choice: there is no setting that selects it.

So there are three storage **classes**: the directory `SpoolDir`
controls, the persistent-data files it does not, and `PlayerPrefs`. Classes, not
locations — with `SpoolDir` left empty the first two are the **same
physical directory**, `Application.persistentDataPath`, and only setting it
splits them. An access or backup review has to walk two paths in the default
configuration and three once the setting is used.

**Changing `SpoolDir` does not move what is already written.** The
resolver simply selects the new directory; there is no migration, and the
consent-erasure sweeps cannot reach files under a directory the SDK is no
longer configured with. Event, crash, experiment and era files written before
the change stay at the old path and need their own backup, access and deletion
handling. "Moves" below means *where new writes go*, not *relocation*.

Those spools, when they exist, go to `Application.persistentDataPath` **by
default only**. Set
`ShardPilotConfig.SpoolDir` and new writes go there instead — and
inherit that location's sharing, backup and access properties rather than the
persistent data path's. If you point it somewhere synced, backed up or
world-readable, the spooled events and crash reports go with it. It does not go looking through
your files — but "reads nothing of yours" is too strong for a directory you
choose: if that directory already contains a file under one of the SDK's
reserved spool names, `TryCreate` calls `LoadFromDisk` on that exact path, and
a consent sweep enumerates and deletes every canonical matching filename in it.
Point `SpoolDir` at a directory the SDK owns, not one it shares with
application data. It never persists a service token — service tokens
are server-side credentials and must not be shipped in a game build.

## Supported versions

Pre-v1, only the latest tag is supported. Fixes land on the next tag rather
than being backported.
