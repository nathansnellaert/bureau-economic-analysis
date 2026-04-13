"""
Transform BEA Fixed Assets tables into datasets.

Dataset naming: bea_fixed_assets_{subject}_annual
Same wide format as NIPA - line items as columns, dates as rows.
Annual data only.
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


def transform_table(records: list[dict]) -> tuple[pa.Table, dict[str, str]] | None:
    """Transform records into a wide PyArrow table + column description map."""
    if not records:
        return None

    date_rows = defaultdict(dict)
    column_order = []
    seen_columns = set()
    col_to_desc: dict[str, str] = {}

    for r in records:
        date = r.get('TimePeriod', '')
        line_desc = r.get('LineDescription', '')
        value = parse_value(r.get('DataValue'))

        if not date or not line_desc:
            continue

        col_name = slugify(line_desc)
        if not col_name:
            continue

        if col_name not in seen_columns:
            column_order.append(col_name)
            seen_columns.add(col_name)
            col_to_desc[col_name] = line_desc

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


def test(table: pa.Table) -> None:
    """Validate Fixed Assets dataset output."""
    columns = {f.name: str(f.type) for f in table.schema}
    schema_dict = {col: ('string' if col == 'date' else 'double') for col in columns}

    validate(table, {
        'columns': schema_dict,
        'not_null': ['date'],
        'unique': ['date'],
        'min_rows': 1,
    })


def make_dataset_id(table_name: str, description: str) -> str:
    """Generate semantic dataset ID."""
    subject = slugify(description) if description else table_name.lower()
    return f"bea_fixed_assets_{subject}_annual"


def make_metadata(
    dataset_id: str,
    table_name: str,
    description: str,
    columns: list[str],
    col_to_desc: dict[str, str],
) -> dict:
    """Generate metadata for a Fixed Assets dataset."""
    column_descriptions = {'date': 'Year of observation'}
    for col in columns:
        column_descriptions[col] = col_to_desc.get(col, col.replace('_', ' ').title())

    fixed = {
        'id': dataset_id,
        'title': f"BEA Fixed Assets - {description[:80]}",
        'description': f"{description}. Source: BEA Fixed Assets Table {table_name}.",
    }
    column_descriptions = truncate_column_descriptions(column_descriptions, fixed)
    return with_bea_fields({**fixed, 'column_descriptions': column_descriptions})


def run():
    """Transform all Fixed Assets tables into datasets."""
    tables = load_raw_json('fixed_assets_tables')
    print(f"Processing {len(tables)} Fixed Assets tables...")

    datasets_created = 0
    datasets_unchanged = 0
    skipped_stale: list[str] = []
    cutoff_year = stale_cutoff_year()

    for table_info in tables:
        table_name = table_info['TableName']
        description = table_info.get('Description', '')

        try:
            data = load_raw_json(f'fixed_assets/{table_name}')
        except FileNotFoundError:
            continue

        result = transform_table(data.get('annual', []))
        if result is None:
            continue
        table, col_to_desc = result

        if len(table) == 0:
            continue

        dates = table.column('date').to_pylist()
        max_date = max(dates) if dates else ''
        if max_date[:4] < cutoff_year:
            stale_id = make_dataset_id(table_name, description)
            skipped_stale.append(f"{stale_id} (max_date={max_date})")
            continue

        dataset_id = make_dataset_id(table_name, description)
        columns = [f.name for f in table.schema if f.name != 'date']

        if is_unchanged(table, dataset_id):
            print(f"  {dataset_id}: unchanged, skipping")
            datasets_unchanged += 1
            continue

        print(f"  {dataset_id}: {len(table)} rows, {len(columns)} columns")

        test(table)
        merge(table, dataset_id, key="date")

        metadata = make_metadata(dataset_id, table_name, description, columns, col_to_desc)
        publish(dataset_id, metadata)
        record_hash(table, dataset_id)

        datasets_created += 1

    print(
        f"\nComplete: {datasets_created} Fixed Assets datasets created, "
        f"{datasets_unchanged} unchanged, "
        f"{len(skipped_stale)} stale (max_date < {cutoff_year})"
    )
    if skipped_stale:
        print("  Stale datasets dropped:")
        for s in skipped_stale:
            print(f"    - {s}")


from nodes.fixed_assets_data import run as fixed_assets_data_run

NODES = {
    run: [fixed_assets_data_run],
}


if __name__ == '__main__':
    run()
