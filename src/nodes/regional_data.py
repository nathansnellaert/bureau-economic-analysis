"""Ingest Regional economic data from BEA API (state-level)."""

from subsets_utils import load_raw_json, save_raw_json
from connector_utils.bea_client import get_regional_data
from connector_utils.state_utils import ttl_filter_pending, mark_downloaded


STATE_ASSET = "regional_data"


def run():
    """Fetch all Regional table+linecode combos for state-level data.

    Each combo is a single key (`{table}_{line_code}`) in the TTL state.
    """
    catalog = load_raw_json('regional_catalog')
    print(f"  Found {len(catalog)} Regional table+linecode combinations")

    by_key = {f"{e['table_name']}_{e['line_code']}": e for e in catalog}
    all_keys = list(by_key.keys())

    pending_keys, downloaded = ttl_filter_pending(STATE_ASSET, all_keys)

    if not pending_keys:
        print("  All regional data up to date (within TTL)")
        return

    print(f"  Fetching {len(pending_keys)} combinations (TTL expired or never fetched)...")

    for i, key in enumerate(pending_keys, 1):
        entry = by_key[key]
        table_name = entry['table_name']
        line_code = entry['line_code']
        print(f"    [{i}/{len(pending_keys)}] {key}: {entry.get('line_desc', '')[:50]}...")

        try:
            records = get_regional_data(table_name, line_code, geo_fips='STATE', year='ALL')
        except ValueError as e:
            print(f"      Warning: {e}")
            records = []

        data = {
            "table_name": table_name,
            "line_code": line_code,
            "line_desc": entry.get('line_desc', ''),
            "table_desc": entry.get('table_desc', ''),
            "records": records,
        }

        save_raw_json(data, f"regional/{table_name}/{line_code}")
        mark_downloaded(STATE_ASSET, downloaded, key)

    print(f"  Completed fetching {len(pending_keys)} combinations")


from nodes.regional_tables import run as regional_tables_run

NODES = {
    run: [regional_tables_run],
}


if __name__ == "__main__":
    run()
