"""Ingest all GDP by Industry tables from BEA API."""

from subsets_utils import load_raw_json, save_raw_json
from connector_utils.bea_client import get_gdp_industry_data
from connector_utils.state_utils import ttl_filter_pending, mark_downloaded


STATE_ASSET = "gdp_industry_data"


def run():
    """Fetch all GDP by Industry tables, honouring the per-table download TTL."""
    tables = load_raw_json('gdp_industry_tables')
    print(f"  Found {len(tables)} GDP by Industry tables")

    all_keys = [t['Key'] for t in tables]
    pending_keys, downloaded = ttl_filter_pending(STATE_ASSET, all_keys)

    if not pending_keys:
        print("  All tables up to date (within TTL)")
        return

    pending_set = set(pending_keys)
    pending = [t for t in tables if t['Key'] in pending_set]
    print(f"  Fetching {len(pending)} tables (TTL expired or never fetched)...")

    for i, table in enumerate(pending, 1):
        table_id = table['Key']
        desc = table.get('Desc', '')
        print(f"    [{i}/{len(pending)}] Table {table_id}: {desc}...")

        # Detect available frequencies from description: (A), (Q), (A) (Q)
        has_annual = '(A)' in desc
        has_quarterly = '(Q)' in desc

        annual = []
        quarterly = []

        if has_annual:
            try:
                annual = get_gdp_industry_data(table_id, frequency='A')
            except ValueError:
                print(f"      Warning: annual data unavailable for table {table_id}")

        if has_quarterly:
            try:
                quarterly = get_gdp_industry_data(table_id, frequency='Q')
            except ValueError:
                print(f"      Warning: quarterly data unavailable for table {table_id}")

        data = {
            "table_id": table_id,
            "description": desc,
            "annual": annual,
            "quarterly": quarterly,
        }

        save_raw_json(data, f"gdp_industry/{table_id}")
        mark_downloaded(STATE_ASSET, downloaded, table_id)

    print(f"  Completed fetching {len(pending)} tables")


from nodes.gdp_industry_tables import run as gdp_industry_tables_run

NODES = {
    run: [gdp_industry_tables_run],
}


if __name__ == "__main__":
    run()
