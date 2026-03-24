"""Solana snapshot finder.

A cleaner, more maintainable rewrite of the original script with the same
operator-oriented workflow:
- discover RPC endpoints
- inspect full/incremental snapshot availability
- filter by freshness, latency, and version
- measure download speed on top candidates
- download the best snapshot archives

This version also supports storing full and incremental snapshots in separate
paths via --full-snapshot-archive-path and --incremental-snapshot-archive-path,
with --snapshots as the primary directory flag.
"""

from __future__ import annotations

import argparse
import glob
import ipaddress
import json
import logging
import math
import os
import shutil
import socket
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Iterable, Optional
from urllib.parse import urlparse

import requests
from multiprocessing.dummy import Pool as ThreadPool
from requests import ConnectionError, ConnectTimeout, HTTPError, ReadTimeout, Timeout
from tqdm import tqdm

DEFAULT_HEADERS = {"Content-Type": "application/json"}
DEFAULT_RPC_ADDRESS = "https://api.mainnet-beta.solana.com"
DEFAULT_SPEED_TEST_LIMIT = 15
DEFAULT_RUNTIME_BLACKLIST_TTL_SEC = 3600
DEFAULT_RUNTIME_BLACKLIST_FILENAME = "blacklist.json"


class DownloadError(RuntimeError):
    def __init__(self, message: str, *, url: str, returncode: Optional[int] = None) -> None:
        super().__init__(message)
        self.url = url
        self.returncode = returncode


@dataclass(slots=True)
class Config:
    threads_count: int
    rpc_address: str
    specific_slot: int
    specific_version: Optional[str]
    wildcard_version: Optional[str]
    max_snapshot_age_in_slots: int
    min_download_speed_mb: int
    max_download_speed_mb: Optional[int]
    max_latency_ms: int
    with_private_rpc: bool
    measurement_time_sec: int
    snapshots_path: Path
    full_snapshot_archive_path: Path
    incremental_snapshot_archive_path: Path
    num_of_retries: int
    sleep_before_retry_sec: int
    sort_order: str
    ip_blacklist: set[str]
    snapshot_blacklist: set[str]
    internal_rpc_nodes: list[str]
    verbose: bool
    runtime_blacklist_ttl_sec: int = DEFAULT_RUNTIME_BLACKLIST_TTL_SEC
    runtime_blacklist_filename: str = DEFAULT_RUNTIME_BLACKLIST_FILENAME
    speed_test_limit: int = DEFAULT_SPEED_TEST_LIMIT


@dataclass(slots=True)
class SnapshotFile:
    kind: str
    filename: str
    snapshot_slot: int
    base_slot: Optional[int] = None
    full_slot: Optional[int] = None
    relative_path: Optional[str] = None


@dataclass(slots=True)
class SnapshotCandidate:
    snapshot_address: str
    slots_diff: int
    latency_ms: float
    files_to_download: list[str]


@dataclass(slots=True)
class AttemptStats:
    discarded_by_archive_type: int = 0
    discarded_by_latency: int = 0
    discarded_by_slot: int = 0
    discarded_by_version: int = 0
    discarded_by_unknown_error: int = 0
    discarded_by_timeout: int = 0


@dataclass(slots=True)
class ScanState:
    current_slot: int = 0
    unsuitable_servers: set[str] = field(default_factory=set)
    local_full_snapshot_path: Optional[Path] = None
    local_full_snapshot_slot: Optional[int] = None
    local_full_snapshot_is_usable: bool = False
    runtime_blacklist: set[str] = field(default_factory=set)
    candidates: list[SnapshotCandidate] = field(default_factory=list)
    stats: AttemptStats = field(default_factory=AttemptStats)


class SnapshotFinder:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.wget_path = shutil.which("wget")
        self._append_lock = Lock()
        self._pbar_lock = Lock()
        self._blacklist_lock = Lock()
        self._pbar: Optional[tqdm] = None

        if self.wget_path is None:
            raise RuntimeError("The wget utility was not found in the system, it is required")

    def run(self) -> int:
        self._ensure_paths()

        self.logger.info("Version: 0.4.0")
        self.logger.info("https://github.com/1dad-io/solana-snapshot-finder")
        self.logger.info(
            "Configuration:\n"
            f"rpc_address={self.config.rpc_address}\n"
            f"max_snapshot_age_in_slots={self.config.max_snapshot_age_in_slots}\n"
            f"min_download_speed_mb={self.config.min_download_speed_mb}\n"
            f"max_download_speed_mb={self.config.max_download_speed_mb}\n"
            f"snapshots_path={self.config.snapshots_path}\n"
            f"full_snapshot_archive_path={self.config.full_snapshot_archive_path}\n"
            f"incremental_snapshot_archive_path={self.config.incremental_snapshot_archive_path}\n"
            f"threads_count={self.config.threads_count}\n"
            f"num_of_retries={self.config.num_of_retries}\n"
            f"with_private_rpc={self.config.with_private_rpc}\n"
            f"sort_order={self.config.sort_order}\n"
            f"runtime_blacklist_ttl_sec={self.config.runtime_blacklist_ttl_sec}"
        )

        with_private_rpc = self.config.with_private_rpc

        for attempt in range(1, self.config.num_of_retries + 1):
            state = ScanState()
            state.current_slot = self.config.specific_slot or self.get_current_slot()

            if self.config.specific_slot:
                state.current_slot = self.config.specific_slot

            self.logger.info(
                "Attempt number: %s. Total attempts: %s",
                attempt,
                self.config.num_of_retries,
            )

            if state.current_slot is None:
                if attempt < self.config.num_of_retries:
                    time.sleep(self.config.sleep_before_retry_sec)
                    continue
                self.logger.error("Could not determine current slot")
                return 1

            self._load_runtime_blacklist(state)
            self._load_local_full_snapshot(state)
            result = self._scan_and_download(state, with_private_rpc=with_private_rpc)
            if result == 0:
                self.logger.info("Done")
                return 0

            with_private_rpc = True
            if attempt < self.config.num_of_retries:
                self.logger.info("Now trying with flag --with-private-rpc")
                self.logger.info(
                    "Sleeping %s seconds before next try",
                    self.config.sleep_before_retry_sec,
                )
                time.sleep(self.config.sleep_before_retry_sec)

        self.logger.error("Could not find a suitable snapshot --> exit")
        return 1

    def _scan_and_download(self, state: ScanState, *, with_private_rpc: bool) -> int:
        rpc_nodes = sorted(set(self.get_all_rpc_ips(state, with_private_rpc=with_private_rpc)))
        self.logger.info(
            "RPC servers in total: %s | Current slot number: %s",
            len(rpc_nodes),
            state.current_slot,
        )

        if not rpc_nodes:
            self.logger.error("No RPC nodes available for scanning")
            return 1

        self.logger.info("Searching information about snapshots on all found RPCs")
        with tqdm(total=len(rpc_nodes)) as progress_bar:
            self._pbar = progress_bar
            pool = ThreadPool(self.config.threads_count)
            try:
                pool.map(lambda rpc: self.inspect_rpc_node(state, rpc), rpc_nodes)
            finally:
                pool.close()
                pool.join()
                self._pbar = None

        self.logger.info("Found suitable RPCs: %s", len(state.candidates))
        self.logger.info(
            "Discard summary: discarded_by_archive_type=%s | discarded_by_latency=%s | "
            "discarded_by_slot=%s | discarded_by_version=%s | discarded_by_timeout=%s | "
            "discarded_by_unknown_error=%s",
            state.stats.discarded_by_archive_type,
            state.stats.discarded_by_latency,
            state.stats.discarded_by_slot,
            state.stats.discarded_by_version,
            state.stats.discarded_by_timeout,
            state.stats.discarded_by_unknown_error,
        )

        if not state.candidates:
            self.logger.info(
                "No snapshot nodes were found matching the given parameters: max_snapshot_age=%s",
                self.config.max_snapshot_age_in_slots,
            )
            return 1

        candidates = sorted(state.candidates, key=lambda item: getattr(item, self.config.sort_order))
        self._write_snapshot_json(rpc_nodes, candidates, state)
        return self._select_and_download_candidate(candidates, state)

    def _select_and_download_candidate(self, candidates: list[SnapshotCandidate], state: ScanState) -> int:
        for index, candidate in enumerate(candidates[: self.config.speed_test_limit], start=1):
            if self._is_blacklisted(candidate):
                self.logger.info("%s/%s BLACKLISTED --> %s", index, len(candidates), candidate)
                continue

            if candidate.snapshot_address in state.unsuitable_servers:
                self.logger.info(
                    "RPC node already in unsuitable list --> skip %s",
                    candidate.snapshot_address,
                )
                continue

            self.logger.info("%s/%s checking the speed %s", index, len(candidates), candidate)
            try:
                speed_bytes_per_second = self.measure_speed(
                    url=candidate.snapshot_address,
                    measure_time=self.config.measurement_time_sec,
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "Speed check failed for %s: %s. Adding it to runtime blacklist",
                    candidate.snapshot_address,
                    exc,
                )
                state.unsuitable_servers.add(candidate.snapshot_address)
                self._add_to_runtime_blacklist(candidate.snapshot_address, reason="speed_check_failed")
                continue
            speed_human = convert_size(speed_bytes_per_second)

            if speed_bytes_per_second < self.config.min_download_speed_mb * 1e6:
                self.logger.info("Too slow: candidate=%s speed=%s", candidate, speed_human)
                state.unsuitable_servers.add(candidate.snapshot_address)
                continue

            self.logger.info("Suitable snapshot server found: candidate=%s speed=%s", candidate, speed_human)
            try:
                self._download_candidate_files(candidate, state)
                return 0
            except DownloadError as exc:
                self.logger.warning(
                    "Download failed for %s: %s. Adding it to runtime blacklist and trying the next candidate",
                    candidate.snapshot_address,
                    exc,
                )
                state.unsuitable_servers.add(candidate.snapshot_address)
                self._add_to_runtime_blacklist(candidate.snapshot_address, reason="download_failed")
                continue

        self.logger.error(
            "No snapshot nodes were found matching the given parameters: min_download_speed_mb=%s",
            self.config.min_download_speed_mb,
        )
        return 1


    def _find_replacement_incremental_candidate(
        self,
        *,
        full_slot: int,
        state: ScanState,
        tried_urls: set[str],
    ) -> Optional[tuple[str, Path]]:
        retry_state = ScanState(
            current_slot=state.current_slot,
            unsuitable_servers=set(state.unsuitable_servers),
        )
        self._load_local_full_snapshot(retry_state)
        candidates = self._rescan_candidates(retry_state)
        if not candidates:
            return None

        for candidate in candidates:
            for relative_path in candidate.files_to_download:
                try:
                    file_info = parse_snapshot_filename(relative_path)
                except ValueError:
                    continue

                if file_info.kind != "incremental" or file_info.base_slot != full_slot:
                    continue

                download_url = self._build_download_url(candidate.snapshot_address, relative_path)
                if download_url in tried_urls:
                    continue
                if not self._snapshot_file_still_available(download_url):
                    continue
                return download_url, self.get_download_dir(file_info)

        return None

    def _rescan_candidates(self, state: ScanState) -> list[SnapshotCandidate]:
        rpc_nodes = sorted(set(self.get_all_rpc_ips(state, with_private_rpc=True)))
        if not rpc_nodes:
            return []

        self.logger.info(
            "Re-scanning %s RPC servers for a fresh replacement incremental snapshot",
            len(rpc_nodes),
        )

        with tqdm(total=len(rpc_nodes)) as progress_bar:
            self._pbar = progress_bar
            pool = ThreadPool(self.config.threads_count)
            try:
                pool.map(lambda rpc: self.inspect_rpc_node(state, rpc), rpc_nodes)
            finally:
                pool.close()
                pool.join()
                self._pbar = None

        return sorted(state.candidates, key=lambda item: getattr(item, self.config.sort_order))

    def _download_replacement_incremental(
        self,
        *,
        full_slot: int,
        state: ScanState,
        tried_urls: set[str],
    ) -> bool:
        replacement = self._find_replacement_incremental_candidate(
            full_slot=full_slot,
            state=state,
            tried_urls=tried_urls,
        )
        if replacement is None:
            return False

        download_url, target_dir = replacement
        tried_urls.add(download_url)
        self.logger.info(
            "Found replacement incremental snapshot for full slot %s: %s",
            full_slot,
            download_url,
        )
        self.download(download_url, target_dir)
        return True


    def _download_candidate_files(self, candidate: SnapshotCandidate, state: ScanState) -> None:
        full_downloaded_in_this_run = False
        active_full_slot = state.local_full_snapshot_slot if state.local_full_snapshot_is_usable else None
        tried_incremental_urls: set[str] = set()

        for relative_path in candidate.files_to_download:
            file_info = parse_snapshot_filename(relative_path)

            if (
                file_info.kind == "full"
                and state.local_full_snapshot_is_usable
                and file_info.full_slot == state.local_full_snapshot_slot
            ):
                self.logger.info("Skipping download of existing reusable full snapshot %s", relative_path)
                active_full_slot = state.local_full_snapshot_slot
                continue

            download_url = self._build_download_url(candidate.snapshot_address, relative_path)
            target_dir = self.get_download_dir(file_info)

            if file_info.kind == "incremental":
                tried_incremental_urls.add(download_url)
                if active_full_slot is None:
                    self.logger.warning(
                        "Skipping incremental snapshot because no matching full snapshot is available yet: %s",
                        download_url,
                    )
                    continue
                if file_info.base_slot != active_full_slot:
                    self.logger.warning(
                        "Skipping incremental snapshot with base slot %s because the active full snapshot slot is %s: %s",
                        file_info.base_slot,
                        active_full_slot,
                        download_url,
                    )
                    continue
                if not self._snapshot_file_still_available(download_url):
                    self.logger.warning(
                        "Incremental snapshot disappeared before download; retrying search for a newer compatible incremental for full slot %s",
                        active_full_slot,
                    )
                    try:
                        if self._download_replacement_incremental(
                            full_slot=active_full_slot,
                            state=state,
                            tried_urls=tried_incremental_urls,
                        ):
                            continue
                    except DownloadError as exc:
                        self.logger.warning(
                            "Replacement incremental snapshot download failed: %s (%s)",
                            exc.url,
                            exc,
                        )
                    self.logger.warning(
                        "No compatible replacement incremental snapshot was found; keeping the available full snapshot and skipping %s",
                        download_url,
                    )
                    continue

            self.logger.info("Downloading %s to %s", download_url, target_dir)

            try:
                self.download(download_url, target_dir)
            except DownloadError as exc:
                if file_info.kind == "incremental" and (full_downloaded_in_this_run or state.local_full_snapshot_is_usable):
                    self.logger.warning(
                        "Incremental snapshot became unavailable during download; retrying search for a newer compatible incremental for full slot %s",
                        active_full_slot,
                    )
                    try:
                        if active_full_slot is not None and self._download_replacement_incremental(
                            full_slot=active_full_slot,
                            state=state,
                            tried_urls=tried_incremental_urls,
                        ):
                            continue
                    except DownloadError as replacement_exc:
                        self.logger.warning(
                            "Replacement incremental snapshot download failed: %s (%s)",
                            replacement_exc.url,
                            replacement_exc,
                        )
                    self.logger.warning(
                        "No compatible replacement incremental snapshot was found; keeping the available full snapshot and skipping %s (%s)",
                        exc.url,
                        exc,
                    )
                    continue
                raise

            if file_info.kind == "full":
                full_downloaded_in_this_run = True
                active_full_slot = file_info.full_slot

    def _snapshot_file_still_available(self, download_url: str) -> bool:
        response = self.do_request(url=download_url, method="head", timeout=3, stats=None)
        if not isinstance(response, requests.Response):
            return False
        if response.status_code >= 400:
            return False
        return True



    @property
    def runtime_blacklist_path(self) -> Path:
        return self.config.snapshots_path / self.config.runtime_blacklist_filename

    def _read_runtime_blacklist_entries(self) -> dict[str, dict]:
        if self.config.runtime_blacklist_ttl_sec <= 0:
            return {}

        path = self.runtime_blacklist_path
        if not path.exists():
            return {}

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Could not read runtime blacklist %s: %s", path, exc)
            return {}

        if not isinstance(payload, dict):
            return {}

        entries = payload.get("entries", payload)
        if not isinstance(entries, dict):
            return {}

        now = int(time.time())
        changed = False
        pruned: dict[str, dict] = {}

        for rpc_address, metadata in entries.items():
            if not isinstance(metadata, dict):
                changed = True
                continue

            added_at = metadata.get("added_at")
            if not isinstance(added_at, (int, float)):
                changed = True
                continue

            if now - int(added_at) >= self.config.runtime_blacklist_ttl_sec:
                changed = True
                continue

            pruned[rpc_address] = {
                "added_at": int(added_at),
                "reason": str(metadata.get("reason", "runtime_failure")),
            }

        if changed:
            self._write_runtime_blacklist_entries(pruned)

        return pruned

    def _write_runtime_blacklist_entries(self, entries: dict[str, dict]) -> None:
        if self.config.runtime_blacklist_ttl_sec <= 0:
            return

        payload = {
            "ttl_seconds": self.config.runtime_blacklist_ttl_sec,
            "entries": dict(sorted(entries.items())),
        }
        self.runtime_blacklist_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _load_runtime_blacklist(self, state: ScanState) -> None:
        entries = self._read_runtime_blacklist_entries()
        state.runtime_blacklist = set(entries.keys())
        if state.runtime_blacklist:
            self.logger.info(
                "Loaded %s runtime-blacklisted RPC endpoint(s) from %s",
                len(state.runtime_blacklist),
                self.runtime_blacklist_path,
            )

    def _add_to_runtime_blacklist(self, rpc_address: str, *, reason: str) -> None:
        if self.config.runtime_blacklist_ttl_sec <= 0:
            return

        with self._blacklist_lock:
            entries = self._read_runtime_blacklist_entries()
            entries[rpc_address] = {
                "added_at": int(time.time()),
                "reason": reason,
            }
            self._write_runtime_blacklist_entries(entries)

    def inspect_rpc_node(self, state: ScanState, rpc_address: str) -> None:
        self._progress_update(1)

        if rpc_address in state.runtime_blacklist:
            self.logger.debug("Skipping runtime-blacklisted RPC %s", rpc_address)
            return

        try:
            incremental_response = self.do_request(
                url=f"http://{rpc_address}/incremental-snapshot.tar.bz2",
                method="head",
                timeout=1,
                stats=state.stats,
            )
            if self._response_exceeds_latency(incremental_response):
                state.stats.discarded_by_latency += 1
                return

            if self.is_redirect_response(incremental_response):
                incremental_path = incremental_response.headers["location"]
                incremental_file = parse_snapshot_filename(incremental_path)
                if incremental_path.endswith("tar"):
                    state.stats.discarded_by_archive_type += 1
                    return

                slots_diff = state.current_slot - incremental_file.snapshot_slot
                if not self._is_slot_diff_acceptable(slots_diff, state.stats):
                    return

                if (
                    state.local_full_snapshot_is_usable
                    and state.local_full_snapshot_slot == incremental_file.base_slot
                ):
                    self._append_candidate(
                        state,
                        SnapshotCandidate(
                            snapshot_address=rpc_address,
                            slots_diff=slots_diff,
                            latency_ms=incremental_response.elapsed.total_seconds() * 1000,
                            files_to_download=[incremental_path],
                        ),
                    )
                    return

                full_response = self.do_request(
                    url=f"http://{rpc_address}/snapshot.tar.bz2",
                    method="head",
                    timeout=1,
                    stats=state.stats,
                )
                if self.is_redirect_response(full_response):
                    self._append_candidate(
                        state,
                        SnapshotCandidate(
                            snapshot_address=rpc_address,
                            slots_diff=slots_diff,
                            latency_ms=min(
                                incremental_response.elapsed.total_seconds() * 1000,
                                full_response.elapsed.total_seconds() * 1000,
                            ),
                            files_to_download=[full_response.headers["location"], incremental_path],
                        ),
                    )
                    return

            full_response = self.do_request(
                url=f"http://{rpc_address}/snapshot.tar.bz2",
                method="head",
                timeout=1,
                stats=state.stats,
            )
            if not self.is_redirect_response(full_response):
                return

            full_path = full_response.headers["location"]
            if full_path.endswith("tar"):
                state.stats.discarded_by_archive_type += 1
                return

            full_file = parse_snapshot_filename(full_path)
            slots_diff = state.current_slot - full_file.snapshot_slot
            if not self._is_slot_diff_acceptable(slots_diff, state.stats):
                return

            latency_ms = full_response.elapsed.total_seconds() * 1000
            if latency_ms > self.config.max_latency_ms:
                state.stats.discarded_by_latency += 1
                return

            self._append_candidate(
                state,
                SnapshotCandidate(
                    snapshot_address=rpc_address,
                    slots_diff=slots_diff,
                    latency_ms=latency_ms,
                    files_to_download=[full_path],
                ),
            )
        except Exception as exc:  # noqa: BLE001
            state.stats.discarded_by_unknown_error += 1
            self.logger.debug("Unexpected error while inspecting %s: %s", rpc_address, exc)

    def _append_candidate(self, state: ScanState, candidate: SnapshotCandidate) -> None:
        with self._append_lock:
            state.candidates.append(candidate)

    def _progress_update(self, amount: int) -> None:
        if self._pbar is None:
            return
        with self._pbar_lock:
            self._pbar.update(amount)

    def _write_snapshot_json(
        self,
        rpc_nodes: Iterable[str],
        candidates: list[SnapshotCandidate],
        state: ScanState,
    ) -> None:
        payload = {
            "last_update_at": time.time(),
            "last_update_slot": state.current_slot,
            "total_rpc_nodes": len(list(rpc_nodes)),
            "rpc_nodes_with_actual_snapshot": len(candidates),
            "rpc_nodes": [
                {
                    "snapshot_address": candidate.snapshot_address,
                    "slots_diff": candidate.slots_diff,
                    "latency": candidate.latency_ms,
                    "files_to_download": candidate.files_to_download,
                }
                for candidate in candidates
            ],
        }
        output_path = self.config.snapshots_path / "snapshot.json"
        output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        self.logger.info("All data is saved to json file - %s", output_path)

    def _load_local_full_snapshot(self, state: ScanState) -> None:
        latest = self._find_latest_local_full_snapshot()
        state.local_full_snapshot_path = None
        state.local_full_snapshot_slot = None
        state.local_full_snapshot_is_usable = False

        if latest is None:
            self.logger.info(
                "Cannot find any full local snapshots in %s --> the search will be carried out on full snapshots",
                self.config.full_snapshot_archive_path,
            )
            return

        state.local_full_snapshot_path = latest[0]
        state.local_full_snapshot_slot = latest[1]
        local_full_age = state.current_slot - latest[1]
        state.local_full_snapshot_is_usable = 0 <= local_full_age <= self.config.max_snapshot_age_in_slots
        self.logger.info(
            "Found full local snapshot %s | local_full_snapshot_slot=%s | local_full_age=%s | reusable=%s",
            state.local_full_snapshot_path,
            state.local_full_snapshot_slot,
            local_full_age,
            state.local_full_snapshot_is_usable,
        )

    def _find_latest_local_full_snapshot(self) -> Optional[tuple[Path, int]]:
        latest: Optional[tuple[Path, int]] = None
        for path_string in glob.glob(str(self.config.full_snapshot_archive_path / "snapshot-*tar*")):
            path = Path(path_string)
            try:
                file_info = parse_snapshot_filename(path.name)
            except ValueError:
                continue
            if file_info.kind != "full" or file_info.full_slot is None:
                continue
            if latest is None or file_info.full_slot > latest[1]:
                latest = (path, file_info.full_slot)
        return latest

    def get_all_rpc_ips(self, state: ScanState, *, with_private_rpc: bool) -> list[str]:
        payload = '{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}'
        response = self.do_request(
            url=self.config.rpc_address,
            method="post",
            data=payload,
            timeout=25,
            stats=state.stats,
        )
        if not isinstance(response, requests.Response):
            self.logger.error("Cannot get RPC addresses: %s", response)
            return []

        try:
            nodes = response.json()["result"]
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Cannot parse cluster node list: %s", exc)
            return []

        rpc_ips: set[str] = set()
        for node in nodes:
            version = node.get("version")
            if self._version_is_excluded(version):
                state.stats.discarded_by_version += 1
                continue

            rpc_endpoint = node.get("rpc")
            if rpc_endpoint:
                rpc_ips.add(rpc_endpoint)
                continue

            if with_private_rpc and node.get("gossip"):
                gossip_ip = node["gossip"].split(":")[0]
                rpc_ips.add(f"{gossip_ip}:8899")

        for node in self.config.internal_rpc_nodes:
            rpc_ips.update(self._resolve_internal_rpc_node(node))

        runtime_blacklist = set(self._read_runtime_blacklist_entries().keys())
        rpc_ips.difference_update(self.config.ip_blacklist)
        rpc_ips.difference_update(runtime_blacklist)
        return sorted(rpc_ips)

    def _version_is_excluded(self, version: Optional[str]) -> bool:
        if self.config.wildcard_version and version and self.config.wildcard_version not in version:
            return True
        if self.config.specific_version and version and version != self.config.specific_version:
            return True
        return False

    def _resolve_internal_rpc_node(self, node: str) -> set[str]:
        if not node:
            return set()
        if ":" in node:
            host, port = node.rsplit(":", 1)
        else:
            host, port = node, "8899"
        return {f"{resolved_ip}:{port}" for resolved_ip in resolve_domain(host, self.logger)}

    def get_current_slot(self) -> Optional[int]:
        payload = '{"jsonrpc":"2.0","id":1, "method":"getSlot"}'
        response = self.do_request(
            url=self.config.rpc_address,
            method="post",
            data=payload,
            timeout=25,
            stats=AttemptStats(),
        )
        if not isinstance(response, requests.Response):
            self.logger.error("Cannot get current slot: %s", response)
            return None

        try:
            body = response.json()
            return body.get("result")
        except Exception as exc:  # noqa: BLE001
            self.logger.error("Cannot parse current slot response: %s", exc)
            return None

    def measure_speed(self, url: str, measure_time: int) -> float:
        response = requests.get(
            f"http://{url}/snapshot.tar.bz2",
            stream=True,
            timeout=measure_time + 2,
        )
        response.raise_for_status()

        start_time = time.monotonic_ns()
        last_time = start_time
        loaded_since_last_sample = 0
        samples: list[float] = []

        for chunk in response.iter_content(chunk_size=81920):
            current_time = time.monotonic_ns()
            elapsed_total = (current_time - start_time) / 1_000_000_000
            if elapsed_total >= measure_time:
                break

            loaded_since_last_sample += len(chunk)
            delta = (current_time - last_time) / 1_000_000_000
            if delta > 1:
                samples.append(loaded_since_last_sample / delta)
                last_time = current_time
                loaded_since_last_sample = 0

        if loaded_since_last_sample > 0:
            tail_elapsed = (time.monotonic_ns() - last_time) / 1_000_000_000
            if tail_elapsed > 0:
                samples.append(loaded_since_last_sample / tail_elapsed)

        return statistics.median(samples) if samples else 0.0

    def do_request(
        self,
        *,
        url: str,
        method: str = "get",
        data: str = "",
        timeout: int = 3,
        headers: Optional[dict[str, str]] = None,
        stats: Optional[AttemptStats] = None,
    ):
        request_headers = headers or DEFAULT_HEADERS
        try:
            if method.lower() == "get":
                return requests.get(url, headers=request_headers, timeout=(timeout, timeout))
            if method.lower() == "post":
                return requests.post(url, headers=request_headers, data=data, timeout=(timeout, timeout))
            if method.lower() == "head":
                return requests.head(url, headers=request_headers, timeout=(timeout, timeout))
            raise ValueError(f"Unsupported request method: {method}")
        except (ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError) as exc:
            if stats is not None:
                stats.discarded_by_timeout += 1
            return f"error in do_request(): {exc}"
        except Exception as exc:  # noqa: BLE001
            if stats is not None:
                stats.discarded_by_unknown_error += 1
            return f"error in do_request(): {exc}"

    def is_redirect_response(self, response) -> bool:
        return isinstance(response, requests.Response) and "location" in response.headers

    def _response_exceeds_latency(self, response) -> bool:
        return self.is_redirect_response(response) and response.elapsed.total_seconds() * 1000 > self.config.max_latency_ms

    def _is_slot_diff_acceptable(self, slots_diff: int, stats: AttemptStats) -> bool:
        if slots_diff < -100:
            stats.discarded_by_slot += 1
            return False
        if slots_diff > self.config.max_snapshot_age_in_slots:
            stats.discarded_by_slot += 1
            return False
        return True

    def _is_blacklisted(self, candidate: SnapshotCandidate) -> bool:
        if not self.config.snapshot_blacklist:
            return False
        files_repr = " ".join(candidate.files_to_download)
        return any(item in files_repr for item in self.config.snapshot_blacklist)

    def _build_download_url(self, snapshot_address: str, relative_path: str) -> str:
        return f"http://{snapshot_address}{relative_path}"

    def get_download_dir(self, file_info: SnapshotFile) -> Path:
        if file_info.kind == "incremental":
            return self.config.incremental_snapshot_archive_path
        return self.config.full_snapshot_archive_path

    def download(self, url: str, target_dir: Path) -> None:
        filename = os.path.basename(urlparse(url).path)
        temp_path = target_dir / f"{filename}.part"
        final_path = target_dir / filename

        command = [self.wget_path, "--progress=dot:giga", "--trust-server-names"]
        if self.config.max_download_speed_mb is not None:
            command.append(f"--limit-rate={self.config.max_download_speed_mb}M")
        command.extend([url, f"-O{temp_path}"])

        result = subprocess.run(command, stdout=subprocess.PIPE, universal_newlines=True)
        if result.returncode != 0:
            raise DownloadError(
                f"wget exited with non-zero code {result.returncode}",
                url=url,
                returncode=result.returncode,
            )

        self.logger.info("Rename the downloaded file %s --> %s", temp_path, final_path)
        os.rename(temp_path, final_path)

    def _ensure_paths(self) -> None:
        for path in {self.config.full_snapshot_archive_path, self.config.incremental_snapshot_archive_path}:
            path.mkdir(parents=True, exist_ok=True)
            try:
                test_file = path / "write_perm_test"
                test_file.write_text("ok", encoding="utf-8")
                test_file.unlink()
            except OSError as exc:
                raise RuntimeError(f"Check path and permissions: {path}: {exc}") from exc


def parse_snapshot_filename(path: str) -> SnapshotFile:
    filename = os.path.basename(urlparse(path).path)
    parts = filename.split("-")

    if filename.startswith("snapshot-") and len(parts) >= 3:
        slot = int(parts[1])
        return SnapshotFile(
            kind="full",
            filename=filename,
            snapshot_slot=slot,
            full_slot=slot,
            relative_path=path,
        )

    if filename.startswith("incremental-snapshot-") and len(parts) >= 5:
        base_slot = int(parts[2])
        snapshot_slot = int(parts[3])
        return SnapshotFile(
            kind="incremental",
            filename=filename,
            snapshot_slot=snapshot_slot,
            base_slot=base_slot,
            relative_path=path,
        )

    raise ValueError(f"Unsupported snapshot filename format: {filename}")


def convert_size(size_bytes: float) -> str:
    if size_bytes == 0:
        return "0B"
    size_names = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    index = int(math.floor(math.log(size_bytes, 1024)))
    power = math.pow(1024, index)
    size = round(size_bytes / power, 2)
    return f"{size} {size_names[index]}"


def resolve_domain(domain: str, logger: logging.Logger) -> list[str]:
    try:
        ipaddress.ip_address(domain)
        return [domain]
    except ValueError:
        pass

    try:
        addrinfo = socket.getaddrinfo(domain, None)
        ips = sorted({info[4][0] for info in addrinfo if info[0] == socket.AF_INET})
        if ips:
            return ips
        return [socket.gethostbyname(domain)]
    except socket.gaierror as exc:
        logger.warning("Could not resolve %s: %s", domain, exc)
        return [domain]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to resolve domain %s: %s", domain, exc)
        return [domain]


def normalize_directory(path: str) -> Path:
    return Path(path.rstrip("/")).expanduser().resolve()


def parse_csv_set(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def parse_csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Solana snapshot finder")
    parser.add_argument(
        "-t",
        "--threads-count",
        default=1000,
        type=int,
        help="Number of concurrent threads used to inspect RPC nodes",
    )
    parser.add_argument(
        "-r",
        "--rpc-address",
        default=DEFAULT_RPC_ADDRESS,
        type=str,
        help="RPC address from which the current slot number will be taken",
    )
    parser.add_argument("--slot", default=0, type=int, help="Search for a snapshot with a specific slot")
    parser.add_argument("--version", default=None, help="Search for a snapshot from a specific validator version")
    parser.add_argument(
        "--wildcard-version",
        default=None,
        help="Search for snapshots matching a major/minor version, for example 2.2",
    )
    parser.add_argument(
        "--max-snapshot-age",
        default=2500,
        type=int,
        help="Maximum age of a candidate snapshot in slots",
    )
    parser.add_argument(
        "--min-download-speed",
        default=60,
        type=int,
        help="Minimum average download speed in megabytes per second",
    )
    parser.add_argument(
        "--max-download-speed",
        type=int,
        help="Optional bandwidth limit in megabytes per second for wget",
    )
    parser.add_argument(
        "--max-latency",
        default=100,
        type=int,
        help="Maximum HEAD request latency in milliseconds",
    )
    parser.add_argument(
        "--with-private-rpc",
        action="store_true",
        help="Allow scanning derived private RPC endpoints from gossip addresses",
    )
    parser.add_argument(
        "--measurement-time",
        default=7,
        type=int,
        help="Duration in seconds used for download speed measurement",
    )
    parser.add_argument(
        "--snapshots",
        "--snapshot-path",
        dest="snapshots",
        default=".",
        type=str,
        help="Primary snapshots directory; kept compatible with --snapshot-path",
    )
    parser.add_argument(
        "--full-snapshot-archive-path",
        default=None,
        type=str,
        help="Directory where full snapshots will be stored; defaults to --snapshots",
    )
    parser.add_argument(
        "--incremental-snapshot-archive-path",
        default=None,
        type=str,
        help="Directory where incremental snapshots will be stored; defaults to --snapshots",
    )
    parser.add_argument(
        "--runtime-blacklist-ttl",
        default=DEFAULT_RUNTIME_BLACKLIST_TTL_SEC,
        type=int,
        help="Keep failing RPC snapshot sources in blacklist.json for this many seconds; use 0 to disable it",
    )
    parser.add_argument("--num-of-retries", default=5, type=int, help="Maximum number of attempts")
    parser.add_argument("--sleep", default=7, type=int, help="Delay in seconds before the next retry")
    parser.add_argument(
        "--sort-order",
        default="latency",
        choices=["latency_ms", "slots_diff", "latency"],
        help="Sort priority for discovered candidates",
    )
    parser.add_argument(
        "-ipb",
        "--ip-blacklist",
        default="",
        type=str,
        help="Comma-separated list of RPC endpoints to exclude",
    )
    parser.add_argument(
        "-b",
        "--blacklist",
        default="",
        type=str,
        help="Comma-separated list of snapshot slots or hashes to exclude",
    )
    parser.add_argument(
        "--internal-rpc-nodes",
        default="",
        type=str,
        help="Comma-separated list of internal RPC nodes to include",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    return parser


def build_config(args: argparse.Namespace) -> Config:
    snapshots_path = normalize_directory(args.snapshots)
    full_snapshot_archive_path = (
        normalize_directory(args.full_snapshot_archive_path)
        if args.full_snapshot_archive_path
        else snapshots_path
    )
    incremental_snapshot_archive_path = (
        normalize_directory(args.incremental_snapshot_archive_path)
        if args.incremental_snapshot_archive_path
        else snapshots_path
    )
    sort_order = "latency_ms" if args.sort_order == "latency" else args.sort_order

    return Config(
        threads_count=args.threads_count,
        rpc_address=args.rpc_address,
        specific_slot=int(args.slot),
        specific_version=args.version,
        wildcard_version=args.wildcard_version,
        max_snapshot_age_in_slots=args.max_snapshot_age if not args.slot else 0,
        min_download_speed_mb=args.min_download_speed,
        max_download_speed_mb=args.max_download_speed,
        max_latency_ms=args.max_latency,
        with_private_rpc=args.with_private_rpc,
        measurement_time_sec=args.measurement_time,
        snapshots_path=snapshots_path,
        full_snapshot_archive_path=full_snapshot_archive_path,
        incremental_snapshot_archive_path=incremental_snapshot_archive_path,
        num_of_retries=args.num_of_retries,
        sleep_before_retry_sec=args.sleep,
        sort_order=sort_order,
        ip_blacklist=parse_csv_set(args.ip_blacklist),
        snapshot_blacklist=parse_csv_set(args.blacklist),
        internal_rpc_nodes=parse_csv_list(args.internal_rpc_nodes),
        verbose=args.verbose,
        runtime_blacklist_ttl_sec=args.runtime_blacklist_ttl,
        runtime_blacklist_filename=DEFAULT_RUNTIME_BLACKLIST_FILENAME,
    )


def configure_logging(config: Config) -> None:
    log_file = config.snapshots_path / "snapshot-finder.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.basicConfig(
        level=logging.DEBUG if config.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    config = build_config(args)
    configure_logging(config)

    try:
        finder = SnapshotFinder(config)
        return finder.run()
    except KeyboardInterrupt:
        print("\nKeyboardInterrupt - Ctrl+C", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).error("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
