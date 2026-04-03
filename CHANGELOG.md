# Changelog

All notable changes to this project will be documented in this file.

## v0.4.0

### Added
- Runtime RPC blacklist with configurable TTL and automatic pruning.
- Support for separate full and incremental snapshot archive paths.
- Replacement incremental discovery when the originally selected incremental disappears.
- Live download-speed enforcement during real transfers.
- Visible progress bar for Python-based downloads.
- Unified version filtering under `--version`, including wildcard patterns.

### Changed
- Reworked the tool into a cleaner, more maintainable operator-oriented implementation.
- Aligned the CLI with validator conventions by promoting `--snapshots`, `--maximum-local-snapshot-age`, and `-u` / `--url`.
- Unified exclusion handling under `--blacklist`.
- Simplified `--sort-order` to `latency` and `slots_diff`.
- Replaced retry-count behavior with time-budget-based search flow closer to validator bootstrap behavior.
- Preserved reusable local full snapshots as the recovery base when compatible.
- Proactively searched for a matching incremental after downloading a full snapshot.
- Preferred the freshest compatible replacement incremental during recovery.
- Improved bootstrap-readiness logging for standalone full snapshots and full-plus-incremental recovery.

### Fixed
- Avoid exiting successfully with a full snapshot that is still too old and still requires a compatible incremental.
- Better handling when incrementals disappear after long full-snapshot downloads.
- Better replacement incremental selection and retry behavior.
- Replacement incremental rescans now respect the configured `--with-private-rpc` setting.
