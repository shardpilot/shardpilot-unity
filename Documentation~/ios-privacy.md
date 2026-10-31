# iOS privacy source and release observation

Refs #100. This package contributes
`Runtime/Plugins/iOS/PrivacyInfo.xcprivacy`, with an iOS-only `PluginImporter`.
The source guard checks that contribution. Unity export, final archive inclusion
and Xcode privacy-report verification are **NOT RUN** by that guard; #100 remains
open for the owner's release observation. A package version alone does not prove
that its released tag includes this source change: record the source commit too.

[Unity documents package privacy manifests](https://docs.unity3d.com/2022.3/Documentation/Manual/apple-privacy-manifest-policy.html)
and their consolidation into
[UnityFramework](https://docs.unity3d.com/2022.3/Documentation/Manual/StructureOfXcodeProject.html).
Automatic manifest support starts at [2022.3.18f1](https://activation.unity3d.com/releases/editor/whats-new/2022.3.18f1) on the 2022.3 line. Use the
supported release-test Editors below; earlier patches require an upgrade for
this integration. Both an ordinary Unity iOS player export and Unity as a
Library must retain the contribution when the framework reaches the host app.
The package supplies source/importer settings; it does not replace the host's
or another SDK's manifest. The owner must observe Unity's actual merge.

## Declared source inventory

The shipped capabilities include optional host calls and flags; their omission
from one run does not remove them from this source inventory. Every listed type
is declared linked because the requests can carry stable installation or host
user identifiers. Tracking is false, with no tracking domains: the SDK's
first-party analytics/configuration and diagnostics do not implement cross-company
tracking. This contribution is not a statement about the host or other SDKs.

The category/purpose mapping is a source assessment for the owner's release test.
Purposes below use Apple's manifest categories: A = analytics, F = app
functionality, P = product personalization. The full keys are in the manifest.

| Data type suffix | SDK source / fields | Purposes |
| --- | --- | --- |
| UserID | `ShardPilotEvent.UserId`, consent and identified configuration requests | A, F, P |
| DeviceID | Persisted anonymous installation ID and crash actor identity | A, F, P |
| CoarseLocation | Host-supplied event `Country` and targeting `geo`; no GPS collection | A, P |
| ProductInteraction | Session, progression, explicit analytics and experiment facts | A, P |
| AdvertisingData | Host-called `TrackAdImpressionRevenue` and impression metadata | A |
| CrashData | Managed exceptions, sanitized messages and stacks | A, F |
| OtherDiagnosticData | App/build, platform, OS and device context | A, F |
| OtherDataTypes | Consent/age-band state and supplied segment classifications | A, F, P |

The serializer is `Runtime/ShardPilotJson.cs`; event fields are in
`ShardPilotEvent.cs`, with ad/progression producers in their client partials.
Custom event/context values and targeting attributes are host input. The host
must classify any additional payload, including contact, purchase or sensitive
content, under its applicable data categories. `OtherDataTypes` does not replace
those specific categories. See Apple's
[data-type and purpose definitions](https://developer.apple.com/documentation/bundleresources/describing-data-use-in-privacy-manifests).

| Required-reason category | Reason | Source use |
| --- | --- | --- |
| UserDefaults | CA92.1 | App-local SDK settings via `PlayerPrefs` in `ShardPilotPersistence.cs` |
| SystemBootTime | 35F9.1 | `Stopwatch` intervals for session boundaries, delivery deadlines and spool pacing |
| FileTimestamp | C617.1 | Sandbox file metadata underlying SDK-owned persistence/spool file I/O |

Only elapsed in-app intervals are eligible for transmission from the monotonic
clock; boot time itself is not a payload. Android ANR/tombstone collectors are
gated off on iOS and are outside this inventory. Source contains no native dependency
bundle, disk-space probe or active-keyboard inventory. The owner must repeat the
API/dependency inventory for the actual IL2CPP/toolchain output. Reason meanings
come from Apple's [required-reason API reference](https://developer.apple.com/documentation/bundleresources/app-privacy-configuration/nsprivacyaccessedapitypes/nsprivacyaccessedapitype).

The preview pre-initialization consent-policy API sends host-supplied scope, app version,
locale, platform and optional store/age-band declarations without a user or device
identifier. They do not initialize persistence or change consent. Every returned
decision remains local and strict; no received policy enables collection.
Response validation checks ambiguity, scope and expiry without authenticating a
plan. Context invalidation changes only in-memory reply eligibility.

## Owner release test — NOT RUN

1. Pin the SDK commit and release tag. Record SDK version, Unity Editor patch,
   Xcode version, build configuration and iOS target. Use synthetic data.
2. With Unity 2022.3.70f1 and 6000.3.20f1, import the package in a minimal iOS
   host and verify the asset's iOS importer is enabled. Export a regular player
   and a Unity-as-a-Library project for each Editor. Record each result separately.
3. Inspect the generated UnityFramework/PrivacyInfo.xcprivacy: the SDK's eight
   data entries, purposes, linkage and three API reasons must survive the merge.
   Check an existing host contribution survives too. In an integration, other
   SDKs may add declarations; do not overwrite them with this source file.
4. Build/archive both routes in Xcode. Inspect the delivered app's
   Frameworks/UnityFramework.framework/PrivacyInfo.xcprivacy, including when
   UnityFramework is embedded by a native host. Record archive and manifest
   hashes. A source file or generated Xcode project alone is insufficient.
5. Generate the Xcode privacy report and verify the SDK contribution appears.
   Validate all bundled manifests, inventory native required-reason calls and
   dependencies, and reconcile host-supplied custom data with its declarations.
6. Attach export/archive/report receipts to #100 for the named SDK release.
   Until those observations pass, report archive coverage as NOT RUN; this source
   PR does not establish App Store acceptance or application compliance.

Apple describes the expected
[manifest bundle locations](https://developer.apple.com/documentation/bundleresources/adding-a-privacy-manifest-to-your-app-or-third-party-sdk).
