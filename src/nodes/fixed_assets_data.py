"""Ingest all Fixed Assets tables from BEA API."""

from subsets_utils import load_raw_json, save_raw_json
from connector_utils.bea_client import get_fixed_assets_data
from connector_utils.state_utils import ttl_filter_pending, mark_downloaded


STATE_ASSET = "fixed_assets_data"


def run():
    """Fetch all Fixed Assets tables, honouring the per-table download TTL."""
    tables = load_raw_json('fixed_assets_tables')
    print(f"  Found {len(tables)} Fixed Assets tables")

    all_keys = [t['TableName'] for t in tables]
    pending_keys, downloaded = ttl_filter_pending(STATE_ASSET, all_keys)

    if not pending_keys:
        print("  All tables up to date (within TTL)")
        return

    pending_set = set(pending_keys)
    pending = [t for t in tables if t['TableName'] in pending_set]
    print(f"  Fetching {len(pending)} tables (TTL expired or never fetched)...")

    for i, table in enumerate(pending, 1):
        table_name = table['TableName']
        desc = table.get('Description', '')
        print(f"    [{i}/{len(pending)}] {table_name}: {desc[:60]}...")

        try:
            annual = get_fixed_assets_data(table_name)
        except ValueError as e:
            print(f"      Warning: {e}")
            annual = []

        data = {
            "table_name": table_name,
            "description": desc,
            "annual": annual,
        }

        save_raw_json(data, f"fixed_assets/{table_name}")
        mark_downloaded(STATE_ASSET, downloaded, table_name)

    print(f"  Completed fetching {len(pending)} tables")


from nodes.fixed_assets_tables import run as fixed_assets_tables_run

NODES = {
    run: [fixed_assets_tables_run],
}


if __name__ == "__main__":
    run()
