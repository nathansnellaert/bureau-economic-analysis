"""Ingest all NIPA tables dynamically from BEA catalog."""

from subsets_utils import save_raw_json, load_state, save_state
from utils.bea_client import get_parameter_values, get_nipa_data


def run():
    """Fetch all NIPA tables dynamically from catalog."""
    # Get catalog of all tables
    tables = get_parameter_values('NIPA', 'TableName')
    print(f"  Found {len(tables)} NIPA tables")

    state = load_state("nipa_data")
    completed = set(state.get("completed", []))

    pending = [t for t in tables if t['TableName'] not in completed]

    if not pending:
        print("  All tables up to date")
        return

    print(f"  Fetching {len(pending)} tables...")

    for i, table in enumerate(pending, 1):
        table_name = table['TableName']
        print(f"    [{i}/{len(pending)}] {table_name}...")

        data = {
            "table_name": table_name,
            "description": table['Description'],
            "annual": get_nipa_data(table_name, frequency='A', year='X'),
            "quarterly": get_nipa_data(table_name, frequency='Q', year='X'),
        }

        # Save each table immediately
        save_raw_json(data, f"nipa/{table_name}")

        # Update state after each save
        completed.add(table_name)
        save_state("nipa_data", {"completed": list(completed)})

    print(f"  Completed fetching {len(pending)} tables")
