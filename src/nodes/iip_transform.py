"""
Transform BEA IIP (International Investment Position) data into datasets.

One dataset per (TypeOfInvestment, Component, frequency) combination, long
format: date + value. This mirrors the ITA connector shape.
"""

from collections import defaultdict

import pyarrow as pa

from subsets_utils import load_raw_json, merge, publish, validate
from connector_utils.transform_utils import slugify, parse_value, normalize_date, detect_frequency
from connector_utils.publish_utils import with_bea_fields, is_unchanged, record_hash
from connector_utils.state_utils import stale_cutoff_year


def _record_component(r: dict) -> str:
    """Best-effort extraction of the Component label from a raw IIP record."""
    for key in ('Component', 'ComponentName', 'Desc', 'TimeSeriesDescription'):
        v = r.get(key)
        if v:
            return str(v)
    return 'unknown'


def _split_by_component(records: list[dict]) -> dict[str, list[dict]]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        buckets[_record_component(r)].append(r)
    return buckets


def transform_component_records(records: list[dict]) -> pa.Table | None:
    if not records:
        return None

    rows: list[dict] = []
    seen_dates: set[str] = set()
    for r in records:
        tp = r.get('TimePeriod', '')
        if not tp:
            continue
        freq = detect_frequency(tp)
        date = normalize_date(tp, freq) or tp
        if date in seen_dates:
            # Duplicate within the same component — last wins.
            for row in rows:
                if row['date'] == date:
                    row['value'] = parse_value(r.get('DataValue'))
                    break
            continue
        seen_dates.add(date)
        rows.append({'date': date, 'value': parse_value(r.get('DataValue'))})

    if not rows:
        return None

    rows.sort(key=lambda r: r['date'])
    schema = pa.schema([
        pa.field('date', pa.string(), nullable=False),
        pa.field('value', pa.float64(), nullable=True),
    ])
    return pa.Table.from_pylist(rows, schema=schema)


def test(table: pa.Table) -> None:
    validate(table, {
        'columns': {'date': 'string', 'value': 'double'},
        'not_null': ['date'],
        'unique': ['date'],
        'min_rows': 1,
    })


def make_dataset_id(type_code: str, component_label: str, frequency: str) -> str:
    type_slug = slugify(type_code) or 'unknown'
    comp_slug = slugify(component_label) or 'all'
    return f"bea_iip_{type_slug}_{comp_slug}_{frequency}"


def make_metadata(
    dataset_id: str,
    type_code: str,
    type_desc: str,
    component_label: str,
    frequency: str,
) -> dict:
    freq_label = frequency.replace('_', ' ').title()
    title_type = type_desc or type_code
    return with_bea_fields({
        'id': dataset_id,
        'title': f"BEA IIP - {title_type} / {component_label} ({freq_label})",
        'description': (
            f"International Investment Position: {title_type} — {component_label}. "
            f"Source: BEA IIP, TypeOfInvestment={type_code}."
        ),
        'column_descriptions': {
            'date': 'Period of observation',
            'value': f"{title_type}: {component_label}",
        },
    })


def run():
    types = load_raw_json('iip_types')
    print(f"Processing {len(types)} IIP TypeOfInvestment values...")

    datasets_created = 0
    datasets_unchanged = 0
    skipped_stale: list[str] = []
    cutoff_year = stale_cutoff_year()

    for type_info in types:
        type_code = type_info['Key']
        type_desc = type_info.get('Desc', '')

        try:
            data = load_raw_json(f'iip/{type_code}')
        except FileNotFoundError:
            continue

        for freq_name in ('annual', 'quarterly_nsa'):
            records = data.get(freq_name, [])
            if not records:
                continue

            for component_label, comp_records in _split_by_component(records).items():
                table = transform_component_records(comp_records)
                if table is None or len(table) == 0:
                    continue

                dates = table.column('date').to_pylist()
                max_date = max(dates) if dates else ''
                dataset_id = make_dataset_id(type_code, component_label, freq_name)

                if max_date[:4] < cutoff_year:
                    skipped_stale.append(f"{dataset_id} (max_date={max_date})")
                    continue

                if is_unchanged(table, dataset_id):
                    datasets_unchanged += 1
                    continue

                print(f"  {dataset_id}: {len(table)} rows")
                test(table)
                merge(table, dataset_id, key="date")

                metadata = make_metadata(
                    dataset_id, type_code, type_desc, component_label, freq_name
                )
                publish(dataset_id, metadata)
                record_hash(table, dataset_id)

                datasets_created += 1

    print(
        f"\nComplete: {datasets_created} IIP datasets created, "
        f"{datasets_unchanged} unchanged, "
        f"{len(skipped_stale)} stale (max_date < {cutoff_year})"
    )
    if skipped_stale:
        print(f"  (Stale IIP count: {len(skipped_stale)} — first 20 shown)")
        for s in skipped_stale[:20]:
            print(f"    - {s}")


from nodes.iip_data import run as iip_data_run

NODES = {
    run: [iip_data_run],
}


if __name__ == '__main__':
    run()
