# Changelog

## [0.18.0-alpha]

### Added

- `ShardPilotConsentPolicyContext.AgeAdsRules` (off by default) asks the consent policy service for its version 1 age and ads rules, and `decision.AgeAdsRules` returns them as typed, read-only data. A request that asks for the rules sends no age band. A reply with a malformed or unknown-version rules object is `invalid_response` and carries no rules. Without the option the request and the decision are unchanged. `Supports("consent_policy_age_ads_rules")` detects it.
- `decision.AgeAdsRules.PersonalisedAdsMode` says how adults choose personalised ads: `consent_window`, `opt_out` or `opt_in`. A missing or unknown mode makes the reply `invalid_response` with no rules.
- `CrashSessionMaxReports` caps the non-fatal crash reports one client admits (default 100; below 1 falls back to 100). It counts after sampling and never applies to fatal crashes.

### Changed

- A live crash send now honours a stored crash `Retry-After` window, as re-sends already did: the capture stays in the crash spool and the re-send pass delivers it after the window.
- `CrashSampleEvery` now defaults to `1`, so every admitted non-fatal report is sent with `non_fatal_sample_one_in` = 1. The previous default was `10`; set `CrashSampleEvery = 10` to keep it.

### Fixed

- A `429` crash response with error code `crash_monthly_quota_fatal_reserve` now holds only non-fatal crash reports for its `Retry-After`: fatal reports are still sent at once, live and from the crash spool, and held non-fatal reports are sent after the window. Any other `429` keeps holding every crash report as before.
- A consent-policy preparation whose request stalls in the transport now settles on the first main-thread frame at or after its deadline, with the strict `deadline_exceeded` fallback, instead of waiting for the transport's whole-second timeout. A late reply cannot replace that decision.
