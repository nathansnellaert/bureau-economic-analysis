"""
Transform BEA Regional data into datasets.

Dataset naming: bea_regional_{table_family}_{line_description_slug}
Long format: date + geo_fips + geo_name + value
"""

import pyarrow as pa

from subsets_utils import load_raw_json, merge, publish, validate
from connector_utils.transform_utils import slugify, parse_value
from connector_utils.publish_utils import with_bea_fields, is_unchanged, record_hash
from connector_utils.state_utils import stale_cutoff_year


def transform_regional(records: list[dict]) -> pa.Table | None:
    """Transform Regional records into a long-format PyArrow table.

    All key columns (`date`, `geo_fips`) are cast to str explicitly and rows
    with null/empty keys are dropped. `value` is parsed as float; `geo_name`
    is coerced to str (or None).
    """
    if not records:
        return None

    rows = []
    for r in records:
        raw_date = r.get('TimePeriod')
        raw_fips = r.get('GeoFips')
        if raw_date is None or raw_fips is None:
            continue

        date = str(raw_date).strip()
        geo_fips = str(raw_fips).strip()
        if not date or not geo_fips:
            continue

        raw_name = r.get('GeoName')
        geo_name = str(raw_name).strip() if raw_name is not None else None

        value = parse_value(r.get('DataValue'))

        rows.append({
            'date': date,
            'geo_fips': geo_fips,
            'geo_name': geo_name,
            'value': value,
        })

    if not rows:
        return None

    # Deduplicate by (date, geo_fips) - take last
    seen: dict[tuple[str, str], dict] = {}
    for row in rows:
        seen[(row['date'], row['geo_fips'])] = row

    rows = sorted(seen.values(), key=lambda r: (r['date'], r['geo_fips']))

    schema = pa.schema([
        pa.field('date', pa.string(), nullable=False),
        pa.field('geo_fips', pa.string(), nullable=False),
        pa.field('geo_name', pa.string(), nullable=True),
        pa.field('value', pa.float64(), nullable=True),
    ])

    return pa.Table.from_pylist(rows, schema=schema)


def test(table: pa.Table) -> None:
    """Validate Regional dataset output."""
    validate(table, {
        'columns': {'date': 'string', 'geo_fips': 'string', 'geo_name': 'string', 'value': 'double'},
        'not_null': ['date', 'geo_fips'],
        'min_rows': 1,
    })


def make_dataset_id(table_name: str, line_desc: str) -> str:
    """Generate semantic dataset ID."""
    # Extract table family (e.g., SAGDP from SAGDP1)
    family = table_name.rstrip('0123456789N').lower()
    line_slug = slugify(line_desc) if line_desc else 'unknown'
    return f"bea_regional_{family}_{line_slug}"


def make_metadata(dataset_id: str, table_name: str, table_desc: str, line_desc: str) -> dict:
    """Generate metadata for a Regional dataset."""
    return with_bea_fields({
        'id': dataset_id,
        'title': f"BEA Regional - {line_desc[:80]}",
        'description': f"{table_desc}: {line_desc}. Source: BEA Regional Table {table_name}.",
        'column_descriptions': {
            'date': 'Year of observation',
            'geo_fips': 'FIPS code for geographic area',
            'geo_name': 'Name of geographic area (state)',
            'value': line_desc,
        },
    })


def run():
    """Transform all Regional data into long-format datasets."""
    catalog = load_raw_json('regional_catalog')
    print(f"Processing {len(catalog)} Regional table+linecode combinations...")

    datasets_created = 0
    datasets_unchanged = 0
    skipped_stale: list[str] = []
    cutoff_year = stale_cutoff_year()

    for entry in catalog:
        table_name = entry['table_name']
        line_code = entry['line_code']
        line_desc = entry.get('line_desc', '')
        table_desc = entry.get('table_desc', '')

        try:
            data = load_raw_json(f"regional/{table_name}/{line_code}")
        except FileNotFoundError:
            continue

        records = data.get('records', [])
        table = transform_regional(records)

        if table is None or len(table) == 0:
            continue

        dates = table.column('date').to_pylist()
        max_date = max(dates) if dates else ''
        if max_date[:4] < cutoff_year:
            stale_id = make_dataset_id(table_name, line_desc)
            skipped_stale.append(f"{stale_id} (max_date={max_date})")
            continue

        dataset_id = make_dataset_id(table_name, line_desc)

        if is_unchanged(table, dataset_id):
            datasets_unchanged += 1
            continue

        n_geos = len(set(table.column('geo_fips').to_pylist()))
        n_dates = len(set(table.column('date').to_pylist()))
        print(f"  {dataset_id}: {len(table)} rows ({n_dates} dates x {n_geos} geos)")

        test(table)
        merge(table, dataset_id, key=["date", "geo_fips"])

        metadata = make_metadata(dataset_id, table_name, table_desc, line_desc)
        publish(dataset_id, metadata)
        record_hash(table, dataset_id)

        datasets_created += 1

    print(
        f"\nComplete: {datasets_created} Regional datasets created, "
        f"{datasets_unchanged} unchanged, "
        f"{len(skipped_stale)} stale (max_date < {cutoff_year})"
    )
    if skipped_stale:
        print("  Stale datasets dropped:")
        for s in skipped_stale:
            print(f"    - {s}")


from nodes.regional_data import run as regional_data_run

NODES = {
    run: [regional_data_run],
}


if __name__ == '__main__':
    run()
