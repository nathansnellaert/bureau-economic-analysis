"""Fetch IIP (International Investment Position) parameter catalog from BEA API."""

from connector_utils.bea_client import get_parameter_values
from subsets_utils import save_raw_json


def run():
    """Fetch IIP parameter catalogs (TypeOfInvestment, Component)."""
    print("  Fetching IIP TypeOfInvestment catalog...")
    types = get_parameter_values('IIP', 'TypeOfInvestment')
    print(f"  Found {len(types)} TypeOfInvestment values")
    save_raw_json(types, "iip_types")

    print("  Fetching IIP Component catalog...")
    components = get_parameter_values('IIP', 'Component')
    print(f"  Found {len(components)} Component values")
    save_raw_json(components, "iip_components")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
