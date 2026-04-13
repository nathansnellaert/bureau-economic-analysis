"""Ingest IIP (International Investment Position) data from BEA API.

One raw file per TypeOfInvestment, holding annual and quarterly series for
every Component. The transform node splits these into per-component datasets.
"""

from subsets_utils import load_raw_json, save_raw_json
from connector_utils.bea_client import get_iip_data
from connector_utils.state_utils import ttl_filter_pending, mark_downloaded


STATE_ASSET = "iip_data"

FREQUENCIES = {
    'annual': 'A',
    'quarterly_nsa': 'QNSA',
}


def run():
    types = load_raw_json('iip_types')
    print(f"  Found {len(types)} IIP TypeOfInvestment values")

    all_keys = [t['Key'] for t in types]
    pending_keys, downloaded = ttl_filter_pending(STATE_ASSET, all_keys)

    if not pending_keys:
        print("  All IIP types up to date (within TTL)")
        return

    pending_set = set(pending_keys)
    pending = [t for t in types if t['Key'] in pending_set]
    print(f"  Fetching {len(pending)} TypeOfInvestment values (TTL expired or never fetched)...")

    for i, type_info in enumerate(pending, 1):
        type_code = type_info['Key']
        desc = type_info.get('Desc', '')
        print(f"    [{i}/{len(pending)}] {type_code}: {desc[:50]}...")

        data = {"type_of_investment": type_code, "description": desc}

        for freq_name, freq_code in FREQUENCIES.items():
            try:
                records = get_iip_data(
                    type_of_investment=type_code,
                    component='All',
                    frequency=freq_code,
                    year='ALL',
                )
                data[freq_name] = records
            except ValueError as e:
                print(f"      Warning: {freq_name} unavailable: {e}")
                data[freq_name] = []

        save_raw_json(data, f"iip/{type_code}")
        mark_downloaded(STATE_ASSET, downloaded, type_code)

    print(f"  Completed fetching {len(pending)} TypeOfInvestment values")


from nodes.iip_tables import run as iip_tables_run

NODES = {
    run: [iip_tables_run],
}


if __name__ == "__main__":
    run()
