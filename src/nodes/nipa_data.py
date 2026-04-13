"""Ingest all NIPA tables dynamically from BEA catalog."""

from subsets_utils import save_raw_json
from connector_utils.bea_client import get_parameter_values, get_nipa_data
from connector_utils.state_utils import ttl_filter_pending, mark_downloaded


STATE_ASSET = "nipa_data"


def run():
    """Fetch NIPA tables, honouring the per-table download TTL."""
    tables = get_parameter_values('NIPA', 'TableName')
    print(f"  Found {len(tables)} NIPA tables")

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

        data = {
            "table_name": table_name,
            "description": table['Description'],
            "annual": get_nipa_data(table_name, frequency='A', year='X'),
            "quarterly": get_nipa_data(table_name, frequency='Q', year='X'),
            "monthly": get_nipa_data(table_name, frequency='M', year='X'),
        }

        save_raw_json(data, f"nipa/{table_name}")
        mark_downloaded(STATE_ASSET, downloaded, table_name)

    print(f"  Completed fetching {len(pending)} tables")


from nodes.nipa_tables import run as nipa_tables_run

NODES = {
    run: [nipa_tables_run],
}


if __name__ == "__main__":
    run()
