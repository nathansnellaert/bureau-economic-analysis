"""
Transform BEA ITA (International Transactions) data into datasets.

Dataset naming: bea_ita_{indicator_slug}_{frequency}
Long format: one row per date, single value column (AllCountries aggregate).
"""

import pyarrow as pa

from subsets_utils import load_raw_json, merge, publish, validate
from connector_utils.transform_utils import slugify, parse_value, normalize_date, detect_frequency
from connector_utils.publish_utils import with_bea_fields, is_unchanged, record_hash
from connector_utils.state_utils import stale_cutoff_year


FREQUENCIES = {
    'annual': 'A',
    'quarterly_sa': 'QSA',
    'quarterly_nsa': 'QNSA',
}


def transform_indicator_frequency(records: list[dict], frequency: str) -> pa.Table | None:
    """Transform ITA records for a single frequency into a table."""
    if not records:
        return None

    freq_code = FREQUENCIES[frequency]
    rows = []

    for r in records:
        if r.get('Frequency', '') != freq_code:
            continue

        time_period = r.get('TimePeriod', '')
        if not time_period:
            continue

        detected = detect_frequency(time_period)
        date = normalize_date(time_period, detected)
        value = parse_value(r.get('DataValue'))

        rows.append({'date': date, 'value': value})

    if not rows:
        return None

    # Deduplicate by date (take last)
    seen = {}
    for row in rows:
        seen[row['date']] = row

    rows = [seen[d] for d in sorted(seen.keys())]

    schema = pa.schema([
        pa.field('date', pa.string(), nullable=False),
        pa.field('value', pa.float64(), nullable=True),
    ])

    return pa.Table.from_pylist(rows, schema=schema)


def test(table: pa.Table) -> None:
    """Validate ITA dataset output."""
    validate(table, {
        'columns': {'date': 'string', 'value': 'double'},
        'not_null': ['date'],
        'unique': ['date'],
        'min_rows': 1,
    })


def make_dataset_id(indicator_code: str, description: str, frequency: str) -> str:
    """Generate semantic dataset ID."""
    subject = slugify(description) if description else slugify(indicator_code)
    return f"bea_ita_{subject}_{frequency}"


def make_metadata(dataset_id: str, indicator_code: str, description: str, frequency: str) -> dict:
    """Generate metadata for an ITA dataset."""
    freq_label = frequency.replace('_', ' ').title()

    return with_bea_fields({
        'id': dataset_id,
        'title': f"BEA ITA - {description} ({freq_label})",
        'description': f"{description}. Source: BEA International Transactions Accounts, indicator {indicator_code}.",
        'column_descriptions': {
            'date': 'Period of observation',
            'value': description,
        },
    })


def run():
    """Transform all ITA indicators into frequency-split datasets."""
    indicators = load_raw_json('ita_indicators')
    print(f"Processing {len(indicators)} ITA indicators...")

    datasets_created = 0
    datasets_unchanged = 0
    skipped_stale: list[str] = []
    cutoff_year = stale_cutoff_year()

    for indicator_info in indicators:
        code = indicator_info['Key']
        desc = indicator_info.get('Desc', '')

        try:
            data = load_raw_json(f'ita/{code}')
        except FileNotFoundError:
            continue

        for frequency in FREQUENCIES:
            records = data.get(frequency, [])
            table = transform_indicator_frequency(records, frequency)

            if table is None or len(table) == 0:
                continue

            dates = table.column('date').to_pylist()
            max_date = max(dates) if dates else ''
            if max_date[:4] < cutoff_year:
                stale_id = make_dataset_id(code, desc, frequency)
                skipped_stale.append(f"{stale_id} (max_date={max_date})")
                continue

            dataset_id = make_dataset_id(code, desc, frequency)

            if is_unchanged(table, dataset_id):
                datasets_unchanged += 1
                continue

            print(f"  {dataset_id}: {len(table)} rows")

            test(table)
            merge(table, dataset_id, key="date")

            metadata = make_metadata(dataset_id, code, desc, frequency)
            publish(dataset_id, metadata)
            record_hash(table, dataset_id)

            datasets_created += 1

    print(
        f"\nComplete: {datasets_created} ITA datasets created, "
        f"{datasets_unchanged} unchanged, "
        f"{len(skipped_stale)} stale (max_date < {cutoff_year})"
    )
    if skipped_stale:
        print(f"  (Stale ITA count: {len(skipped_stale)} — first 20 shown)")
        for s in skipped_stale[:20]:
            print(f"    - {s}")


from nodes.ita_data import run as ita_data_run

NODES = {
    run: [ita_data_run],
}


if __name__ == '__main__':
    run()
