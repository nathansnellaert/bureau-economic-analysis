"""Fetch ITA (International Transactions) catalog from BEA API."""

from connector_utils.bea_client import get_parameter_values
from subsets_utils import save_raw_json


def run():
    """Fetch ITA indicator and area catalogs."""
    print("  Fetching ITA indicator catalog...")

    indicators = get_parameter_values('ITA', 'Indicator')
    # Filter out TSI_ time series IDs (pre-built aggregates)
    indicators = [i for i in indicators if not i['Key'].startswith('TSI_')]
    print(f"  Found {len(indicators)} indicators")
    save_raw_json(indicators, "ita_indicators")

    areas = get_parameter_values('ITA', 'AreaOrCountry')
    print(f"  Found {len(areas)} areas/countries")
    save_raw_json(areas, "ita_areas")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
