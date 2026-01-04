
from utils.bea_client import get_parameter_values
from subsets_utils import save_raw_json


def run():
    """Fetch NIPA table catalog and save raw JSON"""
    print("  Fetching NIPA table catalog...")

    tables = get_parameter_values('NIPA', 'TableName')

    print(f"  Found {len(tables):,} tables")

    save_raw_json(tables, "nipa_tables")
