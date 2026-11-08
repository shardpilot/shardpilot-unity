# Changelog

## [0.16.3-alpha]

### Fixed

- When the SDK closes with consent writes still pending, `CloseAsync` now publishes `ConsentWritesUnpersistedAtShutdown` and `LastError` together before it completes. A race between the worker and its deadline counts the failure only once.

### Changed

- The offline consent-policy fixtures now carry the public disclaimer in `basis.notice` for resolved and fallback responses, including the advisory case.
- The public package changelog now lists the changes included in each release.
