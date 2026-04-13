"""
Transform BEA NIUnderlyingDetail tables into wide datasets by frequency.

Dataset naming: bea_ni_underlying_{table_slug}_{frequency}
Same wide format as NIPA (line items as columns, dates as rows).
"""

import re
from collections import defaultdict

import pyarrow as pa

from subsets_utils import load_raw_json, merge, publish, validate
from connector_utils.transform_utils import slugify, parse_value, normalize_date, detect_frequency
from connector_utils.publish_utils import (
    with_bea_fields,
    is_unchanged,
    record_hash,
    truncate_column_descriptions,
)
from connector_utils.state_utils import stale_cutoff_year


def transform_table_frequency(
    records: list[dict], frequency: str
) -> tuple[pa.Table, dict[str, str]] | None:
    if not records:
        return None

    date_rows = defaultdict(dict)
    column_order: list[str] = []
    seen_columns: set[str] = set()
    col_to_desc: dict[str, str] = {}

    for r in records:
        time_period = r.get('TimePeriod', '')
        if detect_frequency(time_period) != frequency:
            continue

        date = normalize_date(time_period, frequency)
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


def test(table: pa.Table, frequency: str) -> None:
    columns = {f.name: str(f.type) for f in table.schema}
    schema_dict = {col: ('string' if col == 'date' else 'double') for col in columns}

    validate(table, {
        'columns': schema_dict,
        'not_null': ['date'],
        'unique': ['date'],
        'min_rows': 1,
    })

    dates = table.column('date').to_pylist()
    if frequency == 'annual':
        pattern = r'^\d{4}$'
    elif frequency == 'quarterly':
        pattern = r'^\d{4}-Q[1-4]$'
    else:
        pattern = r'^\d{4}-\d{2}$'
    for d in dates:
        assert re.match(pattern, d), f"Invalid {frequency} date format: {d}"


def _extract_table_subject(table_name: str, description: str) -> str:
    """Derive a short slug from the table number or description.

    NIUnderlyingDetail table names look like `U00100`, `U70100D` etc. The
    description typically starts with `Table 1.1U. Something (A) (Q)`.
    """
    match = re.search(r'Table (\S+)\. (.+?) \([AQM]', description or '')
    if match:
        title = match.group(2).strip()
        return slugify(title) or table_name.lower()
    return slugify(description) if description else table_name.lower()


def make_dataset_id(table_name: str, description: str, frequency: str) -> str:
    subject = _extract_table_subject(table_name, description)
    return f"bea_ni_underlying_{subject}_{frequency}"


def make_metadata(
    dataset_id: str,
    table_name: str,
    description: str,
    frequency: str,
    columns: list[str],
    col_to_desc: dict[str, str],
) -> dict:
    freq_label = frequency.title()
    date_col_label = {'annual': 'Year', 'quarterly': 'Quarter', 'monthly': 'Month'}[frequency]

    column_descriptions = {'date': f"{date_col_label} of observation"}
    for col in columns:
        column_descriptions[col] = col_to_desc.get(col, col.replace('_', ' ').title())

    clean_desc = (description or '').strip() or table_name
    fixed = {
        'id': dataset_id,
        'title': f"BEA NI Underlying Detail - {clean_desc[:80]} ({freq_label})",
        'description': f"{clean_desc}. Source: BEA NIUnderlyingDetail Table {table_name}.",
    }
    column_descriptions = truncate_column_descriptions(column_descriptions, fixed)
    return with_bea_fields({**fixed, 'column_descriptions': column_descriptions})


def run():
    catalog = load_raw_json('ni_underlying_tables')
    print(f"Processing {len(catalog)} NIUnderlyingDetail tables...")

    # Collision handling: multiple tables may produce the same base ID.
    id_counts: dict[str, list[str]] = defaultdict(list)
    for t in catalog:
        base = _extract_table_subject(t['TableName'], t.get('Description', ''))
        id_counts[base].append(t['TableName'])
    suffix_map: dict[str, str] = {}
    for base, table_names in id_counts.items():
        if len(table_names) > 1:
            for i, tn in enumerate(sorted(table_names), 1):
                suffix_map[tn] = str(i)

    datasets_created = 0
    datasets_unchanged = 0
    datasets_no_raw = 0
    skipped_stale: list[str] = []
    cutoff_year = stale_cutoff_year()

    for table_info in catalog:
        table_name = table_info['TableName']
        description = table_info.get('Description', '')

        try:
            data = load_raw_json(f'ni_underlying/{table_name}')
        except FileNotFoundError:
            datasets_no_raw += 1
            continue

        all_records = (
            data.get('annual', []) + data.get('quarterly', []) + data.get('monthly', [])
        )

        for frequency in ('annual', 'quarterly', 'monthly'):
            result = transform_table_frequency(all_records, frequency)
            if result is None:
                continue
            table, col_to_desc = result
            if len(table) == 0:
                continue

            dates = table.column('date').to_pylist()
            max_date = max(dates) if dates else ''
            base_id = make_dataset_id(table_name, description, frequency)
            suffix = suffix_map.get(table_name, '')
            dataset_id = f"{base_id}_{suffix}" if suffix else base_id

            if max_date[:4] < cutoff_year:
                skipped_stale.append(f"{dataset_id} (max_date={max_date})")
                continue

            if is_unchanged(table, dataset_id):
                datasets_unchanged += 1
                continue

            columns = [f.name for f in table.schema if f.name != 'date']
            print(f"  {dataset_id}: {len(table)} rows, {len(columns)} columns")

            test(table, frequency)
            merge(table, dataset_id, key="date")

            metadata = make_metadata(
                dataset_id, table_name, description, frequency, columns, col_to_desc
            )
            publish(dataset_id, metadata)
            record_hash(table, dataset_id)

            datasets_created += 1

    print(
        f"\nComplete: {datasets_created} NIUnderlyingDetail datasets created, "
        f"{datasets_unchanged} unchanged, "
        f"{datasets_no_raw} tables missing raw data, "
        f"{len(skipped_stale)} stale (max_date < {cutoff_year})"
    )
    if skipped_stale:
        print("  Stale datasets dropped:")
        for s in skipped_stale:
            print(f"    - {s}")


from nodes.ni_underlying_data import run as ni_underlying_data_run

NODES = {
    run: [ni_underlying_data_run],
}


if __name__ == '__main__':
    run()
