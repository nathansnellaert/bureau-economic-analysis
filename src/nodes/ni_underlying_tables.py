"""Fetch NIUnderlyingDetail table catalog from BEA API."""

from connector_utils.bea_client import get_parameter_values
from subsets_utils import save_raw_json


def run():
    """Fetch NIUnderlyingDetail table catalog and save raw JSON."""
    print("  Fetching NIUnderlyingDetail table catalog...")

    tables = get_parameter_values('NIUnderlyingDetail', 'TableName')

    print(f"  Found {len(tables):,} tables")

    save_raw_json(tables, "ni_underlying_tables")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
