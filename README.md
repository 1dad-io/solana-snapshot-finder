# solana-snapshot-finder

Fast snapshot discovery and download for Solana validators.

`solana-snapshot-finder` discovers snapshot sources from validator RPC endpoints, filters them by freshness and latency, measures real download speed, and downloads the best matching full and incremental snapshot archives for your node.

It is built for operators who want more control than the validator's built-in snapshot fetch path, especially during bootstrap, recovery, or when storing full and incremental archives in separate directories.

## Highlights

- Dynamic snapshot source discovery from cluster RPC nodes
- Filtering by snapshot age, validator version, and latency
- Real transfer-speed probing before choosing a source
- Separate full and incremental archive directories
- Backward compatibility with the legacy `--snapshot-path` flag
- `snapshot.json`, `snapshot-finder.log`, and runtime `blacklist.json` output for debugging and automation

## How it works

```mermaid
flowchart TD
    A[Get current slot] --> B[Fetch cluster nodes]
    B --> C[Resolve RPC endpoints]
    C --> D[Probe full and incremental snapshot redirects]
    D --> E[Filter by age, version, latency, and archive rules]
    E --> F[Sort candidates]
    F --> G[Measure download speed on top candidates]
    G --> H{Suitable source found?}
    H -- No --> I[Retry / expand search]
    H -- Yes --> J[Download full and or incremental archives]
    J --> K[Write snapshot.json and logs]
```

## Snapshot paths

The CLI follows current validator conventions:

- `--snapshots` is the primary path flag
- `--snapshot-path` is kept as a supported alias for backward compatibility
- `--full-snapshot-archive-path` defaults to `--snapshots` if not set
- `--incremental-snapshot-archive-path` defaults to `--snapshots` if not set

You can keep a single snapshots directory, or split full and incremental archives across different paths.

## Selection behavior

The tool prefers the freshest valid source that also satisfies your latency and download-speed requirements.

When a local full snapshot already exists, the tool checks whether it is still fresh enough to reuse. If it is reusable, the tool treats that full snapshot as the recovery base and searches only for compatible incremental snapshots built on the same full snapshot slot. If no compatible incremental is currently available, the tool keeps the reusable local full snapshot and exits cleanly by default. If `--allow-full-snapshot-fallback` is enabled, it may fall back to full snapshot discovery instead. If no reusable local full snapshot exists, the tool downloads a full snapshot and then proactively searches for a matching recent incremental snapshot, even if the originally selected candidate did not include one.

Incomplete downloads use the `.part` suffix until the transfer completes successfully.

Failing RPC snapshot sources can also be written to a runtime `blacklist.json` under `--snapshots`. This blacklist helps retries avoid hammering the same broken source, and entries are automatically pruned after the configured TTL.

`--maximum-local-snapshot-age` is the primary validator-aligned flag for deciding whether a local full snapshot is still reusable. `--max-snapshot-age` is kept as a legacy compatibility alias.

## Requirements

- Python 3.10+
- `wget`
- Network access to Solana validator RPC endpoints

Python dependencies are listed in `requirements.txt`.

## Installation

```bash
git clone https://github.com/1dad-io/solana-snapshot-finder.git
cd solana-snapshot-finder
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Ubuntu packages:

```bash
sudo apt-get update
sudo apt-get install -y python3-venv wget git
```

## Quick start

Store everything in a validator-style local snapshots directory:

```bash
python3 snapshot-finder.py   --snapshots snapshots
```

Store full and incremental archives separately:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --full-snapshot-archive-path snapshots/full   --incremental-snapshot-archive-path snapshots/incremental
```

Use the legacy path flag:

```bash
python3 snapshot-finder.py   --snapshot-path snapshots
```

## Common examples

Prefer newer snapshots and stricter latency:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --maximum-local-snapshot-age 800   --max-latency 60
```

Require faster download sources:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --min-download-speed 120   --measurement-time 10
```

Keep failing RPCs in the runtime blacklist for one hour:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --runtime-blacklist-ttl 3600
```

Allow fallback to full snapshot discovery when no compatible incremental exists for a reusable local full snapshot:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --allow-full-snapshot-fallback
```

Use a specific RPC to fetch cluster data:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --rpc-address https://api.mainnet-beta.solana.com
```

Include private RPC guesses derived from gossip:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --with-private-rpc
```

Restrict results to a validator version:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --version 2.2.14
```

Match a major/minor version series:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --wildcard-version 2.2
```

Search for a specific slot:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --slot 381234567
```

Exclude problematic endpoints or archives:

```bash
python3 snapshot-finder.py   --snapshots snapshots   --ip-blacklist 203.0.113.10:8899,198.51.100.5:8899   --blacklist 381234567,some_archive_hash
```

Dedicated mount-point layout:

```bash
python3 snapshot-finder.py   --snapshots /mnt/ledger/snapshots   --full-snapshot-archive-path /mnt/snapshots/full   --incremental-snapshot-archive-path /mnt/snapshots/incremental
```

## Docker

Build locally:

```bash
docker build -t solana-snapshot-finder .
```

Run with a single snapshots directory:

```bash
docker run --rm -it   -v /mnt/snap:/snapshots   --user "$(id -u):$(id -g)"   solana-snapshot-finder
```

Run with separate full and incremental archive paths:

```bash
docker run --rm -it   -v /mnt/snap:/snapshots   -v /mnt/full:/mnt/full   -v /mnt/inc:/mnt/inc   --user "$(id -u):$(id -g)"   solana-snapshot-finder   --snapshots /snapshots   --full-snapshot-archive-path /mnt/full   --incremental-snapshot-archive-path /mnt/inc
```

The provided Dockerfile uses:
- `python:3.12-slim`
- non-root runtime user
- default entrypoint: `python snapshot-finder.py`
- default command: `--snapshots /snapshots`

## CLI reference

### Path options

- `--snapshots` — primary snapshots directory
- `--snapshot-path` — backward-compatible alias for `--snapshots`
- `--full-snapshot-archive-path` — directory for full snapshot archives; defaults to `--snapshots`
- `--incremental-snapshot-archive-path` — directory for incremental snapshot archives; defaults to `--snapshots`

### Cluster and RPC options

- `--rpc-address` — RPC endpoint used for cluster discovery and current-slot lookup
- `--with-private-rpc` — also probe likely private RPC addresses derived from gossip data
- `--internal-rpc-nodes` — comma-separated list of additional RPC endpoints to include directly

### Snapshot selection options

- `--slot` — target a specific full snapshot slot
- `--maximum-local-snapshot-age` — primary flag for deciding whether a local full snapshot is still reusable
- `--max-snapshot-age` — legacy alias for `--maximum-local-snapshot-age`
- `--version` — require an exact validator version match
- `--wildcard-version` — match a validator major/minor series such as `2.2`
- `--sort-order` — candidate sort mode: `latency`, `latency_ms`, or `slots_diff`

### Network and speed options

- `--max-latency` — maximum acceptable RPC latency in milliseconds
- `--min-download-speed` — minimum measured download speed in MB/s
- `--max-download-speed` — upper bound for acceptable measured download speed in MB/s
- `--measurement-time` — number of seconds used for the download speed probe

### Retry, concurrency, and runtime blacklist options

- `--threads-count` — number of worker threads used for RPC probing
- `--num-of-retries` — number of attempts before the tool gives up
- `--sleep` — delay in seconds between retry attempts
- `--runtime-blacklist-ttl` — keep failing RPC snapshot sources in `blacklist.json` for this many seconds before auto-pruning them; use `0` to disable the persistent runtime blacklist
- `--allow-full-snapshot-fallback` — when no compatible incremental exists for a reusable local full snapshot, fall back to full snapshot discovery instead of exiting cleanly with the local full only

### Exclusion and logging options

- `--ip-blacklist` — comma-separated list of `ip:port` RPC endpoints to ignore
- `--blacklist` — comma-separated list of snapshot slots, hashes, or identifiers to ignore
- `--verbose` — enable more detailed console logging

Use `python3 snapshot-finder.py --help` for the full argument list.

## Output

The tool writes:
- `snapshot-finder.log` in `--snapshots`
- `snapshot.json` in `--snapshots`
- `blacklist.json` in `--snapshots` when runtime blacklisting is enabled
- full snapshot archives in `--full-snapshot-archive-path`
- incremental snapshot archives in `--incremental-snapshot-archive-path`

## Notes

- Make sure the validator uses a compatible `--maximum-local-snapshot-age` threshold, otherwise validator may still decide to fetch a newer incremental snapshot after the tool finishes.
- The speed check is a short real download probe, not a theoretical estimate.
- A local full snapshot is reused only when it is still fresh enough.
- If a reusable local full snapshot exists, the tool switches to incremental-only recovery and searches only for compatible incrementals built on that full snapshot slot.
- If no compatible incremental is found for a reusable local full snapshot, the default behavior is to keep that local full snapshot and exit cleanly.
- `--allow-full-snapshot-fallback` makes the tool fall back to full snapshot discovery in that case.
- If an incremental snapshot disappears because the full download took too long, the tool can retry discovery of a fresh compatible incremental, prefer the newest compatible incremental slot, and try the next-best compatible source if the best replacement fails transiently.
- After a full snapshot is downloaded successfully, the tool proactively searches for a compatible recent incremental snapshot instead of leaving that work to validator.
- Failing RPC snapshot sources can be persisted in `blacklist.json` and auto-pruned after `--runtime-blacklist-ttl` seconds.
- Transient replacement download failures such as rate limiting can cause the tool to blacklist that replacement source temporarily and try the next compatible replacement incremental.
- If no suitable candidate is found, the tool retries and can expand the search by enabling private RPC probing.

## Troubleshooting

### `wget` not found

Install `wget` and retry.

### Existing partial downloads

Incomplete downloads are left with a `.part` suffix. Remove stale `.part` files manually if they were left behind after an interrupted run.

### Runtime blacklist entries

Failing RPC snapshot sources can be stored in `blacklist.json` under `--snapshots`. Entries are pruned automatically after `--runtime-blacklist-ttl` seconds, or immediately if you delete `blacklist.json`.

## License

See the repository license file.
