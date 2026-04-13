"""Fetch GDP by Industry table catalog from BEA API."""

from connector_utils.bea_client import get_parameter_values
from subsets_utils import save_raw_json


def run():
    """Fetch GDP by Industry table catalog and industry codes."""
    print("  Fetching GDP by Industry table catalog...")

    tables = get_parameter_values('GDPbyIndustry', 'TableID')
    print(f"  Found {len(tables)} tables")
    save_raw_json(tables, "gdp_industry_tables")

    industries = get_parameter_values('GDPbyIndustry', 'Industry')
    print(f"  Found {len(industries)} industry codes")
    save_raw_json(industries, "gdp_industry_industries")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
