# Changelog

## [0.19.0-alpha]

### Added

- `decision.AgeAdsRules.TeenAds` returns the rule for admitted players aged 13 to 15 in the European Economic Area: `DigitalConsentAge` (13 to 16) and `BelowDigitalConsentMode` (`no_consent_only`). It is null outside the European Economic Area.

### Changed

- Version 1 age and ads rules now require the `teen_ads` member, either null or the closed teen rule. A rules object makes the reply `invalid_response` with no rules if `teen_ads` is missing, has an unsupported age or mode, or comes with a consent window that is not required or a mode other than `consent_window`. Version 0.18.0-alpha rejects every rules object that carries `teen_ads`, so use this version with rules that include it.
