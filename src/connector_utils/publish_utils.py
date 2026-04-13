"""Publishing helpers shared across BEA transform nodes.

Centralises the BEA-wide metadata fields (license, source) and the
`data_hash`-based "skip if unchanged" check that every transform uses.
"""

import pyarrow as pa

from subsets_utils import data_hash, load_state, save_state


BEA_LICENSE = "Public Domain (U.S. Government Work, 17 U.S.C. § 105)"
BEA_SOURCE = "U.S. Bureau of Economic Analysis"

# Published metadata is serialized as JSON into the Delta table description,
# which Delta Lake caps at ~4KB. Leave headroom for title/description/etc.
_MAX_METADATA_CHARS = 3800


def with_bea_fields(metadata: dict) -> dict:
    """Attach the connector-wide license / source fields to a metadata dict."""
    return {**metadata, "license": BEA_LICENSE, "source": BEA_SOURCE}


def _hash_state_key(dataset_id: str) -> str:
    return f"hash:{dataset_id}"


def is_unchanged(table: pa.Table, dataset_id: str) -> bool:
    """True if the dataset's data_hash matches the stored hash."""
    h = data_hash(table)
    return load_state(_hash_state_key(dataset_id)).get("hash") == h


def record_hash(table: pa.Table, dataset_id: str) -> None:
    save_state(_hash_state_key(dataset_id), {"hash": data_hash(table)})


def truncate_column_descriptions(
    column_descriptions: dict[str, str],
    fixed_fields: dict,
) -> dict[str, str]:
    """Truncate description *text* to keep the serialized metadata under the
    Delta 4KB table-description cap. Prefers trimming long descriptions over
    dropping columns outright.
    """
    import json

    def size_with(desc: dict) -> int:
        return len(json.dumps({**fixed_fields, "column_descriptions": desc}))

    if size_with(column_descriptions) <= _MAX_METADATA_CHARS:
        return column_descriptions

    # Progressively shrink the longest descriptions until we fit.
    out = {k: v for k, v in column_descriptions.items()}
    while size_with(out) > _MAX_METADATA_CHARS:
        longest = max(out.items(), key=lambda kv: len(kv[1]))
        k, v = longest
        if len(v) <= 20:
            # Everything is already short; drop the largest key.
            out.pop(k)
        else:
            out[k] = v[: max(20, len(v) // 2)].rstrip() + "…"
        if not out:
            break
    return out
