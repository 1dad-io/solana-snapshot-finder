# Changelog

All notable changes to this project will be documented in this file.

## v0.4.2

### Fixed
- Keep the search in incremental-only recovery mode after a full snapshot has already been downloaded locally for the active base slot, instead of downloading newer full snapshots during the same search flow.
- Ignore `.part` files when detecting the latest local full snapshot so incomplete downloads are not treated as valid local recovery bases.
- Stop treating `--maximum-local-snapshot-age` as a hard filter for older remote full snapshots during discovery when those full snapshots can still be paired with a recent enough incremental snapshot.
- Activate incremental-only recovery only for a reusable local full snapshot or for a full snapshot downloaded during the current search flow, instead of getting stuck on stale non-reusable local full snapshots found on disk.

### Changed
- Align `.gitignore` and `.dockerignore` with current snapshot archive formats by removing `*.tar` and adding `*.lz4`.

## v0.4.1

### Fixed
- Avoid re-downloading the same full snapshot across later candidates after the full archive is already present locally; reuse the local full snapshot and keep retrying only compatible incrementals.

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
