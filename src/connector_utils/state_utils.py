"""Shared state helpers for BEA download and transform nodes.

Two concerns live here:

- **TTL-based download state** — `ttl_filter_pending` / `mark_downloaded`.
  Download nodes store `{key: iso_timestamp}` per table/indicator. Keys
  older than `BEA_DOWNLOAD_TTL_DAYS` are considered stale and get refetched.

- **Dynamic stale-dataset cutoff** — `stale_cutoff_year`. Transform nodes
  drop datasets whose latest observation year is older than
  `current_year - BEA_STALE_CUTOFF_YEARS` (default 3).
"""

import os
from datetime import datetime, timezone, timedelta

from subsets_utils import load_state, save_state


DEFAULT_DOWNLOAD_TTL_DAYS = 7
DEFAULT_STALE_CUTOFF_YEARS = 3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def download_ttl_days() -> int:
    return int(os.environ.get("BEA_DOWNLOAD_TTL_DAYS", DEFAULT_DOWNLOAD_TTL_DAYS))


def load_download_state(asset: str) -> dict[str, str]:
    """Load the `{key: iso_timestamp}` download state.

    Back-compat: the old state format stored `{"completed": [list, of, keys]}`.
    Those keys are migrated in-memory to epoch-0 (always considered stale) so
    they get refetched on the next run, honouring the TTL going forward.
    """
    state = load_state(asset)
    downloaded = dict(state.get("downloaded", {}))
    legacy = state.get("completed", [])
    if legacy:
        for k in legacy:
            downloaded.setdefault(k, "1970-01-01T00:00:00+00:00")
    return downloaded


def save_download_state(asset: str, downloaded: dict[str, str]) -> None:
    save_state(asset, {"downloaded": downloaded})


def ttl_filter_pending(asset: str, keys: list[str]) -> tuple[list[str], dict[str, str]]:
    """Return `(keys_to_fetch, downloaded_state)` honouring the TTL.

    Keys whose last-download timestamp is older than `BEA_DOWNLOAD_TTL_DAYS`
    are returned as pending; the caller should refetch them and then call
    `mark_downloaded` to update the state after each successful fetch.
    """
    downloaded = load_download_state(asset)
    ttl = timedelta(days=download_ttl_days())
    cutoff = datetime.now(timezone.utc) - ttl

    pending: list[str] = []
    for k in keys:
        ts = _parse_iso(downloaded.get(k, ""))
        if ts is None or ts < cutoff:
            pending.append(k)
    return pending, downloaded


def mark_downloaded(asset: str, downloaded: dict[str, str], key: str) -> None:
    """Record a successful download for `key` and persist state."""
    downloaded[key] = _now_iso()
    save_download_state(asset, downloaded)


def stale_cutoff_year() -> str:
    """Year string (YYYY) below which transforms should drop datasets."""
    years = int(os.environ.get("BEA_STALE_CUTOFF_YEARS", DEFAULT_STALE_CUTOFF_YEARS))
    return str(datetime.now(timezone.utc).year - years)
