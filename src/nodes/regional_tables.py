"""Fetch Regional data catalog from BEA API."""

from connector_utils.bea_client import get_parameter_values, get_parameter_values_filtered
from subsets_utils import save_raw_json, load_state, save_state


# State-level table families to fetch
TARGET_TABLES = [
    'SAGDP1', 'SAGDP2N', 'SAGDP3', 'SAGDP4', 'SAGDP5N', 'SAGDP6N',
    'SAGDP7N', 'SAGDP8N', 'SAGDP9N', 'SAGDP10N', 'SAGDP11N',
    'SAINC1', 'SAINC4', 'SAINC5N', 'SAINC7N', 'SAINC30',
    'SAINC35', 'SAINC40', 'SAINC50', 'SAINC51',
    'SARPP',
    'SAPCE1', 'SAPCE2', 'SAPCE3', 'SAPCE4',
]


def run():
    """Fetch Regional table catalog and line codes for each target table."""
    print("  Fetching Regional table catalog...")

    all_tables = get_parameter_values('Regional', 'TableName')
    print(f"  Found {len(all_tables)} total Regional tables")
    save_raw_json(all_tables, "regional_tables")

    # Build lookup
    table_lookup = {t['Key']: t for t in all_tables}

    # For each target table, fetch available line codes
    state = load_state("regional_tables_linecodes")
    completed = set(state.get("completed", []))

    catalog = []
    for table_name in TARGET_TABLES:
        if table_name not in table_lookup:
            print(f"    {table_name}: not found in catalog, skipping")
            continue

        table_info = table_lookup[table_name]

        if table_name in completed:
            # Load existing line codes
            try:
                from subsets_utils import load_raw_json
                line_codes = load_raw_json(f"regional_line_codes/{table_name}")
            except FileNotFoundError:
                line_codes = []
        else:
            print(f"    {table_name}: fetching line codes...")
            try:
                line_codes = get_parameter_values_filtered(
                    'Regional', 'LineCode', TableName=table_name
                )
            except ValueError:
                print(f"      Warning: could not fetch line codes for {table_name}")
                line_codes = []

            save_raw_json(line_codes, f"regional_line_codes/{table_name}")
            completed.add(table_name)
            save_state("regional_tables_linecodes", {"completed": list(completed)})

        for lc in line_codes:
            catalog.append({
                'table_name': table_name,
                'table_desc': table_info.get('Desc', ''),
                'line_code': lc.get('Key', ''),
                'line_desc': lc.get('Desc', ''),
            })

    save_raw_json(catalog, "regional_catalog")
    print(f"  Regional catalog: {len(catalog)} table+linecode combinations")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
