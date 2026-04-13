"""
Transform BEA GDP by Industry tables into semantically named datasets.

Dataset naming: bea_gdp_industry_{subject}_{frequency}
Industries become columns (wide format), dates as rows.
"""

from collections import defaultdict

import pyarrow as pa

from subsets_utils import load_raw_json, merge, publish, validate
from connector_utils.transform_utils import slugify, parse_value
from connector_utils.publish_utils import (
    with_bea_fields,
    is_unchanged,
    record_hash,
    truncate_column_descriptions,
)
from connector_utils.state_utils import stale_cutoff_year


QUARTER_MAP = {'I': 'Q1', 'II': 'Q2', 'III': 'Q3', 'IV': 'Q4'}


def make_date(record: dict, frequency: str) -> str | None:
    """Extract date from GDPbyIndustry record (uses Year/Quarter, not TimePeriod)."""
    year = record.get('Year', '')
    if not year:
        return None
    if frequency == 'annual':
        return year
    quarter = record.get('Quarter', '')
    mapped = QUARTER_MAP.get(quarter)
    if not mapped:
        return None
    return f"{year}-{mapped}"


def transform_table_frequency(
    records: list[dict], frequency: str
) -> tuple[pa.Table, dict[str, str]] | None:
    """Transform records for a single frequency into a wide PyArrow table."""
    if not records:
        return None

    freq_code = 'A' if frequency == 'annual' else 'Q'

    date_rows = defaultdict(dict)
    column_order = []
    seen_columns = set()
    col_to_desc: dict[str, str] = {}

    for r in records:
        if r.get('Frequency', '') != freq_code:
            continue

        date = make_date(r, frequency)
        industry = r.get('IndustrYDescription', '')
        value = parse_value(r.get('DataValue'))

        if not date or not industry:
            continue

        col_name = slugify(industry)
        if not col_name:
            continue

        if col_name not in seen_columns:
            column_order.append(col_name)
            seen_columns.add(col_name)
            col_to_desc[col_name] = industry

        date_rows[date][col_name] = value

    if not date_rows:
        return None

    rows = []
    for date in sorted(date_rows.keys()):
        row = {'date': date}
        for col in column_order:
            row[col] = date_rows[date].get(col)
        rows.append(row)

    schema_fields = [pa.field('date', pa.string(), nullable=False)]
    for col in column_order:
        schema_fields.append(pa.field(col, pa.float64(), nullable=True))

    return pa.Table.from_pylist(rows, schema=pa.schema(schema_fields)), col_to_desc


def test(table: pa.Table, frequency: str) -> None:
    """Validate GDP by Industry dataset output."""
    columns = {f.name: str(f.type) for f in table.schema}
    schema_dict = {col: ('string' if col == 'date' else 'double') for col in columns}

    validate(table, {
        'columns': schema_dict,
        'not_null': ['date'],
        'unique': ['date'],
        'min_rows': 1,
    })


def make_dataset_id(table_id: str, description: str, frequency: str) -> str:
    """Generate semantic dataset ID from table description."""
    subject = slugify(description) if description else f"table_{table_id}"
    return f"bea_gdp_industry_{subject}_{frequency}"


def make_metadata(
    dataset_id: str,
    table_id: str,
    description: str,
    frequency: str,
    columns: list[str],
    col_to_desc: dict[str, str],
) -> dict:
    """Generate metadata for a GDP by Industry dataset."""
    freq_label = frequency.title()

    column_descriptions = {
        'date': f"{'Year' if frequency == 'annual' else 'Quarter'} of observation"
    }
    for col in columns:
        column_descriptions[col] = col_to_desc.get(col, col.replace('_', ' ').title())

    fixed = {
        'id': dataset_id,
        'title': f"BEA GDP by Industry - {description} ({freq_label})",
        'description': f"{description}. Source: BEA GDP by Industry Table {table_id}.",
    }
    column_descriptions = truncate_column_descriptions(column_descriptions, fixed)
    return with_bea_fields({**fixed, 'column_descriptions': column_descriptions})


def run():
    """Transform all GDP by Industry tables into frequency-split datasets."""
    tables = load_raw_json('gdp_industry_tables')
    print(f"Processing {len(tables)} GDP by Industry tables...")

    datasets_created = 0
    datasets_unchanged = 0
    skipped_stale: list[str] = []
    cutoff_year = stale_cutoff_year()

    for table_info in tables:
        table_id = table_info['Key']
        description = table_info.get('Desc', '')

        try:
            data = load_raw_json(f'gdp_industry/{table_id}')
        except FileNotFoundError:
            continue

        all_records = data.get('annual', []) + data.get('quarterly', [])

        for frequency in ['annual', 'quarterly']:
            result = transform_table_frequency(all_records, frequency)
            if result is None:
                continue
            table, col_to_desc = result

            if len(table) == 0:
                continue

            dates = table.column('date').to_pylist()
            max_date = max(dates) if dates else ''
            if max_date[:4] < cutoff_year:
                stale_id = make_dataset_id(table_id, description, frequency)
                skipped_stale.append(f"{stale_id} (max_date={max_date})")
                continue

            dataset_id = make_dataset_id(table_id, description, frequency)
            columns = [f.name for f in table.schema if f.name != 'date']

            if is_unchanged(table, dataset_id):
                print(f"  {dataset_id}: unchanged, skipping")
                datasets_unchanged += 1
                continue

            print(f"  {dataset_id}: {len(table)} rows, {len(columns)} columns")

            test(table, frequency)
            merge(table, dataset_id, key="date")

            metadata = make_metadata(
                dataset_id, table_id, description, frequency, columns, col_to_desc
            )
            publish(dataset_id, metadata)
            record_hash(table, dataset_id)

            datasets_created += 1

    print(
        f"\nComplete: {datasets_created} GDP by Industry datasets created, "
        f"{datasets_unchanged} unchanged, "
        f"{len(skipped_stale)} stale (max_date < {cutoff_year})"
    )
    if skipped_stale:
        print("  Stale datasets dropped:")
        for s in skipped_stale:
            print(f"    - {s}")


from nodes.gdp_industry_data import run as gdp_industry_data_run

NODES = {
    run: [gdp_industry_data_run],
}


if __name__ == '__main__':
    run()
