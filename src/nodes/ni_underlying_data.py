"""Ingest NIUnderlyingDetail tables from BEA API."""

from subsets_utils import load_raw_json, save_raw_json
from connector_utils.bea_client import get_ni_underlying_data
from connector_utils.state_utils import ttl_filter_pending, mark_downloaded


STATE_ASSET = "ni_underlying_data"


def run():
    """Fetch NIUnderlyingDetail tables, honouring the per-table download TTL."""
    tables = load_raw_json('ni_underlying_tables')
    print(f"  Found {len(tables)} NIUnderlyingDetail tables")

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
        print(f"    [{i}/{len(pending)}] {table_name}...")

        def fetch(freq: str):
            try:
                return get_ni_underlying_data(table_name, frequency=freq, year='X')
            except ValueError:
                return []

        data = {
            "table_name": table_name,
            "description": table.get('Description', ''),
            "annual": fetch('A'),
            "quarterly": fetch('Q'),
            "monthly": fetch('M'),
        }

        save_raw_json(data, f"ni_underlying/{table_name}")
        mark_downloaded(STATE_ASSET, downloaded, table_name)

    print(f"  Completed fetching {len(pending)} tables")


from nodes.ni_underlying_tables import run as ni_underlying_tables_run

NODES = {
    run: [ni_underlying_tables_run],
}


if __name__ == "__main__":
    run()
