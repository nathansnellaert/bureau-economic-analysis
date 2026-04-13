"""Ingest ITA (International Transactions) data from BEA API."""

from subsets_utils import load_raw_json, save_raw_json
from connector_utils.bea_client import get_ita_data
from connector_utils.state_utils import ttl_filter_pending, mark_downloaded


STATE_ASSET = "ita_data"

FREQUENCIES = {
    'annual': 'A',
    'quarterly_sa': 'QSA',
    'quarterly_nsa': 'QNSA',
}


def run():
    """Fetch all ITA indicators, honouring the per-indicator download TTL."""
    indicators = load_raw_json('ita_indicators')
    print(f"  Found {len(indicators)} ITA indicators")

    all_keys = [i['Key'] for i in indicators]
    pending_keys, downloaded = ttl_filter_pending(STATE_ASSET, all_keys)

    if not pending_keys:
        print("  All indicators up to date (within TTL)")
        return

    pending_set = set(pending_keys)
    pending = [i for i in indicators if i['Key'] in pending_set]
    print(f"  Fetching {len(pending)} indicators (TTL expired or never fetched)...")

    for i, indicator in enumerate(pending, 1):
        code = indicator['Key']
        desc = indicator.get('Desc', '')
        print(f"    [{i}/{len(pending)}] {code}: {desc[:50]}...")

        data = {"indicator": code, "description": desc}

        for freq_name, freq_code in FREQUENCIES.items():
            try:
                records = get_ita_data(code, frequency=freq_code)
                data[freq_name] = records
            except ValueError:
                data[freq_name] = []

        save_raw_json(data, f"ita/{code}")
        mark_downloaded(STATE_ASSET, downloaded, code)

    print(f"  Completed fetching {len(pending)} indicators")


from nodes.ita_tables import run as ita_tables_run

NODES = {
    run: [ita_tables_run],
}


if __name__ == "__main__":
    run()
