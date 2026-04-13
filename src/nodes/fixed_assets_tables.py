"""Fetch Fixed Assets table catalog from BEA API."""

from connector_utils.bea_client import get_parameter_values
from subsets_utils import save_raw_json


def run():
    """Fetch Fixed Assets table catalog."""
    print("  Fetching Fixed Assets table catalog...")

    tables = get_parameter_values('FixedAssets', 'TableName')
    print(f"  Found {len(tables)} tables")
    save_raw_json(tables, "fixed_assets_tables")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
