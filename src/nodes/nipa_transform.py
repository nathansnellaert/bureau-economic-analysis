"""
Transform BEA NIPA tables into semantically named datasets split by frequency.

Dataset naming: bea_{subject}_{measurement}_{frequency}
Examples:
- bea_gdp_current_dollars_quarterly
- bea_gdp_percent_change_quarterly
- bea_pce_chained_dollars_monthly
"""

import re
from collections import defaultdict

import pyarrow as pa

from subsets_utils import load_raw_json, upload_data, validate

def slugify(text: str) -> str:
    """Convert LineDescription to snake_case column name."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', '_', text.strip())
    return text

def parse_value(value_str: str) -> float | None:
    """Parse BEA data value to float, handling commas and special values."""
    if value_str is None:
        return None

    cleaned = str(value_str).replace(',', '').strip()

    if not cleaned or cleaned in ('', '...', '----', 'n.a.'):
        return None

    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None

def normalize_date(time_period: str, frequency: str) -> str:
    """Normalize TimePeriod to ISO 8601 format."""
    if frequency == 'quarterly':
        match = re.match(r'^(\d{4})Q([1-4])$', time_period)
        if match:
            return f"{match.group(1)}-Q{match.group(2)}"
    elif frequency == 'monthly':
        match = re.match(r'^(\d{4})M(\d{2})$', time_period)
        if match:
            return f"{match.group(1)}-{match.group(2)}"

    return time_period

def detect_frequency(time_period: str) -> str:
    """Detect frequency from TimePeriod format."""
    if re.match(r'^\d{4}Q[1-4]$', time_period):
        return 'quarterly'
    elif re.match(r'^\d{4}M\d{2}$', time_period):
        return 'monthly'
    else:
        return 'annual'

def extract_semantic_name(table_name: str, description: str) -> tuple[str, str]:
    """
    Extract semantic subject and measurement from table description.

    Returns:
        (subject, measurement) tuple
        subject: gdp, pce, govt, trade, investment, income, etc.
        measurement: current_dollars, chained_dollars, percent_change, price_index, etc.
    """
    # Extract title from 'Table X.X.X[A-D]. Title (A) (Q)'
    # The letter suffix (A, B, C, D) indicates methodology variants
    match = re.search(r'Table (\d+)\.(\d+)\.?\d*([A-Z])?\. (.+?) \([AQM]', description)
    if not match:
        return table_name.lower(), 'data'

    section = match.group(1)
    subsection = match.group(2)
    variant = match.group(3)  # A, B, C, D or None
    title = match.group(4)

    # Determine subject from section.subsection
    # Each subsection represents a distinct breakdown/view of the data
    subject_map = {
        # Section 1: GDP and related aggregates
        ('1', '1'): 'gdp',
        ('1', '2'): 'gdp_by_product',
        ('1', '3'): 'value_added_by_sector',
        ('1', '4'): 'gdp_purchases_final_sales',
        ('1', '5'): 'gdp_expanded',
        ('1', '6'): 'domestic_purchases',
        ('1', '7'): 'gdp_gnp_nnp',
        ('1', '8'): 'gdp_command_basis',
        ('1', '9'): 'net_value_added',
        ('1', '10'): 'domestic_income',
        ('1', '11'): 'domestic_income_shares',
        ('1', '12'): 'national_income',
        ('1', '13'): 'national_income_by_sector',
        ('1', '14'): 'corporate_value_added',
        ('1', '15'): 'corporate_profit_per_unit',
        ('1', '16'): 'private_enterprise_income',
        ('1', '17'): 'gdp_gdi_aggregates',
        # Section 2: Personal income and outlays
        ('2', '1'): 'personal_income',
        ('2', '2'): 'personal_income_disposition',
        ('2', '3'): 'pce_by_function',
        ('2', '4'): 'pce_by_type',
        ('2', '5'): 'pce_bridges',
        ('2', '6'): 'personal_income_monthly',
        ('2', '7'): 'wages_monthly',
        ('2', '8'): 'pce_supplemental',
        # Section 3: Government
        ('3', '1'): 'govt_receipts_expenditures',
        ('3', '2'): 'federal_govt',
        ('3', '3'): 'state_local_govt',
        ('3', '4'): 'govt_social_benefits',
        ('3', '5'): 'govt_taxes',
        ('3', '6'): 'govt_contributions',
        ('3', '7'): 'govt_taxes_receipts',
        ('3', '8'): 'govt_subsidies',
        ('3', '9'): 'govt_consumption',
        ('3', '10'): 'govt_consumption_output',
        ('3', '11'): 'defense_spending',
        ('3', '12'): 'govt_output',
        ('3', '13'): 'govt_employment',
        ('3', '14'): 'govt_compensation',
        ('3', '15'): 'govt_investment_detail',
        ('3', '16'): 'govt_receipts_detail',
        ('3', '17'): 'govt_spending_function',
        ('3', '18'): 'federal_govt_detail',
        ('3', '19'): 'state_local_detail',
        ('3', '20'): 'social_insurance',
        ('3', '21'): 'social_insurance_funds',
        # Section 4: Foreign transactions
        ('4', '1'): 'trade_balance',
        ('4', '2'): 'exports_imports',
        ('4', '3'): 'trade_detail',
        # Section 5: Saving and investment
        ('5', '1'): 'saving_investment',
        ('5', '2'): 'private_investment_type',
        ('5', '3'): 'private_fixed_investment',
        ('5', '4'): 'nonresidential_investment',
        ('5', '5'): 'residential_investment',
        ('5', '6'): 'investment_equipment',
        ('5', '7'): 'inventories',
        ('5', '8'): 'inventories_detail',
        ('5', '9'): 'govt_fixed_investment',
        ('5', '10'): 'capital_stock',
        ('5', '11'): 'capital_consumption',
        # Section 6: Income and employment by industry
        ('6', '1'): 'income_by_industry',
        ('6', '2'): 'compensation_by_industry',
        ('6', '3'): 'wages_by_industry',
        ('6', '4'): 'supplements_by_industry',
        ('6', '5'): 'employment_by_industry',
        ('6', '6'): 'hours_by_industry',
        ('6', '7'): 'labor_productivity',
        ('6', '8'): 'labor_costs',
        ('6', '9'): 'proprietors_income',
        ('6', '10'): 'rental_income',
        ('6', '11'): 'corporate_profits_by_industry',
        ('6', '12'): 'net_interest',
        ('6', '13'): 'taxes_by_industry',
        ('6', '14'): 'capital_consumption_by_industry',
        ('6', '15'): 'undistributed_profits',
        ('6', '16'): 'corporate_profits',
        ('6', '17'): 'corporate_profits_detail',
        ('6', '18'): 'profits_iva_ccadj',
        ('6', '19'): 'profits_financial_nonfinancial',
        ('6', '20'): 'profits_receipts',
        ('6', '21'): 'profits_taxes',
        ('6', '22'): 'profits_after_tax',
        # Section 7: Supplemental tables
        ('7', '1'): 'motor_vehicle_output',
        ('7', '2'): 'auto_output',
        ('7', '3'): 'farm_sector',
        ('7', '4'): 'housing',
        ('7', '5'): 'housing_output',
        ('7', '6'): 'nonprofit_institutions',
        ('7', '7'): 'food_services',
        ('7', '8'): 'petroleum',
        ('7', '9'): 'energy',
        ('7', '10'): 'cpi_pce_comparison',
        ('7', '11'): 'implicit_deflators',
        ('7', '12'): 'real_pce_detail',
        ('7', '13'): 'pce_by_function_detail',
        ('7', '14'): 'farm_income',
        ('7', '15'): 'farm_income_detail',
        ('7', '16'): 'govt_social_insurance',
        ('7', '17'): 'employer_contributions',
        ('7', '18'): 'contributions_detail',
        ('7', '20'): 'pensions_defined_benefit',
        ('7', '21'): 'pensions_defined_contribution',
        ('7', '22'): 'pensions_federal',
        ('7', '23'): 'pensions_state_local',
        ('7', '24'): 'pensions_private',
        ('7', '25'): 'pensions_ira_keogh',
        # Section 8: Not seasonally adjusted
        ('8', '1'): 'gdp_nsa',
        ('8', '3'): 'federal_govt_nsa',
        ('8', '4'): 'state_local_govt_nsa',
    }

    subject = subject_map.get((section, subsection))
    if not subject:
        # Fallback: use section name with subsection
        section_names = {
            '1': 'gdp', '2': 'personal', '3': 'govt', '4': 'trade',
            '5': 'investment', '6': 'industry', '7': 'supplemental', '8': 'nsa'
        }
        base = section_names.get(section, f's{section}')
        subject = f'{base}_{subsection}'

    # Determine measurement type from title
    # Order matters - more specific patterns first
    title_lower = title.lower()
    if 'contributions to percent change in real' in title_lower:
        measurement = 'contributions_real'
    elif 'contributions to percent change in' in title_lower and 'price' in title_lower:
        measurement = 'contributions_price'
    elif 'contributions to percent change' in title_lower:
        measurement = 'contributions'
    elif 'percent change from quarter one year ago' in title_lower:
        measurement = 'percent_change_yoy'
    elif 'percent change from preceding period in prices' in title_lower:
        measurement = 'price_percent_change'
    elif 'percent change' in title_lower:
        measurement = 'percent_change'
    elif 'quantity indexes' in title_lower or 'quantity index' in title_lower:
        measurement = 'quantity_index'
    elif 'price indexes' in title_lower or 'price index' in title_lower:
        measurement = 'price_index'
    elif 'implicit price deflator' in title_lower:
        measurement = 'deflator'
    elif 'chained dollars' in title_lower:
        measurement = 'chained_dollars'
    elif 'percentage shares' in title_lower:
        measurement = 'shares'
    elif 'current dollars' in title_lower:
        measurement = 'current_dollars'
    elif 'relation of' in title_lower:
        measurement = 'relation'
    elif 'transactions of' in title_lower:
        measurement = 'transactions'
    else:
        measurement = 'level'

    # Append variant suffix (A, B, C, D) to subject if present
    # These represent different data scopes or methodologies
    if variant:
        subject = f"{subject}_{variant.lower()}"

    return subject, measurement

def transform_table_frequency(records: list[dict], frequency: str) -> pa.Table | None:
    """Transform records for a single frequency into a wide PyArrow table."""
    if not records:
        return None

    date_rows = defaultdict(dict)
    column_order = []
    seen_columns = set()

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

    schema = pa.schema(schema_fields)
    return pa.Table.from_pylist(rows, schema=schema)

def make_dataset_id(table_name: str, description: str, frequency: str, suffix: str = "") -> str:
    """Generate semantic dataset ID."""
    subject, measurement = extract_semantic_name(table_name, description)

    base_id = f"bea_{subject}_{measurement}_{frequency}"
    if suffix:
        base_id = f"{base_id}_{suffix}"

    return base_id

def make_metadata(dataset_id: str, table_name: str, description: str, frequency: str, columns: list[str]) -> dict:
    """Generate metadata for a dataset."""
    freq_label = frequency.title()

    # Extract clean table title from description
    match = re.search(r'Table (\d+\.\d+\.?\d*[A-Z]?)\. (.+?) \([AQM]', description)
    if match:
        table_ref = match.group(1)
        table_title = match.group(2).strip()
    else:
        table_ref = table_name
        table_title = description

    # Only include column descriptions for first N columns to stay under 4000 char limit
    max_columns = min(30, len(columns))
    column_descriptions = {
        'date': f"{'Year' if frequency == 'annual' else 'Quarter' if frequency == 'quarterly' else 'Month'} of observation"
    }

    for col in columns[:max_columns]:
        column_descriptions[col] = col.replace('_', ' ').title()

    subject, measurement = extract_semantic_name(table_name, description)
    title = f"BEA {subject.replace('_', ' ').title()} - {measurement.replace('_', ' ').title()} ({freq_label})"

    return {
        'id': dataset_id,
        'title': title,
        'description': f"{table_title}. Source: BEA NIPA Table {table_ref}.",
        'column_descriptions': column_descriptions,
    }

def load_nipa_tables() -> list[dict]:
    """Load the NIPA table catalog."""
    return load_raw_json('nipa_tables')

def load_table_data(table_name: str) -> dict | None:
    """Load raw data for a single NIPA table."""
    try:
        return load_raw_json(f'nipa/{table_name}')
    except FileNotFoundError:
        return None

def test(table: pa.Table, frequency: str) -> None:
    """
    Validate NIPA dataset output.

    Args:
        table: PyArrow table to validate
        frequency: 'annual', 'quarterly', or 'monthly'
    """
    # Get all column names
    columns = {f.name: str(f.type) for f in table.schema}

    # Build schema validation dict
    schema_dict = {}
    for col_name, col_type in columns.items():
        if col_name == 'date':
            schema_dict[col_name] = 'string'
        else:
            schema_dict[col_name] = 'double'

    # Basic schema validation
    validate(table, {
        'columns': schema_dict,
        'not_null': ['date'],
        'unique': ['date'],
        'min_rows': 1,
    })

    # Validate date format based on frequency
    dates = table.column('date').to_pylist()

    if frequency == 'annual':
        pattern = r'^\d{4}$'
        for d in dates:
            assert re.match(pattern, d), f"Invalid annual date format: {d}"
    elif frequency == 'quarterly':
        pattern = r'^\d{4}-Q[1-4]$'
        for d in dates:
            assert re.match(pattern, d), f"Invalid quarterly date format: {d}"
    elif frequency == 'monthly':
        pattern = r'^\d{4}-\d{2}$'
        for d in dates:
            assert re.match(pattern, d), f"Invalid monthly date format: {d}"

    # Validate dates are sorted
    assert dates == sorted(dates), "Dates should be sorted ascending"

    # Validate we have at least one data column
    data_columns = [c for c in columns.keys() if c != 'date']
    assert len(data_columns) >= 1, "Table must have at least one data column"

def run():
    """Transform all NIPA tables into frequency-split datasets."""
    catalog = load_nipa_tables()
    print(f"Processing {len(catalog)} NIPA tables...")

    # First pass: detect collisions by counting how many tables map to each base ID
    base_id_counts = defaultdict(list)
    for table_info in catalog:
        table_name = table_info['TableName']
        description = table_info['Description']
        subject, measurement = extract_semantic_name(table_name, description)
        base_key = f"{subject}_{measurement}"
        base_id_counts[base_key].append(table_name)

    # Build suffix map for tables with collisions
    suffix_map = {}
    for base_key, table_names in base_id_counts.items():
        if len(table_names) > 1:
            # Multiple tables share the same base ID - add numbered suffixes
            for i, tn in enumerate(sorted(table_names), 1):
                suffix_map[tn] = str(i)

    datasets_created = 0
    datasets_skipped = 0

    for table_info in catalog:
        table_name = table_info['TableName']
        description = table_info['Description']

        data = load_table_data(table_name)
        if not data:
            print(f"  {table_name}: No raw data found, skipping")
            datasets_skipped += 1
            continue

        all_records = data.get('annual', []) + data.get('quarterly', []) + data.get('monthly', [])

        for frequency in ['annual', 'quarterly', 'monthly']:
            table = transform_table_frequency(all_records, frequency)

            if table is None or len(table) == 0:
                continue

            # Skip datasets with no recent data (before 2024)
            dates = table.column('date').to_pylist()
            max_date = max(dates) if dates else ''
            if max_date < '2024':
                print(f"  {table_name} ({frequency}): Skipping - last data from {max_date}")
                continue

            suffix = suffix_map.get(table_name, "")
            dataset_id = make_dataset_id(table_name, description, frequency, suffix)
            columns = [f.name for f in table.schema if f.name != 'date']

            print(f"  {dataset_id}: {len(table)} rows, {len(columns)} columns")

            test(table, frequency)

            upload_data(table, dataset_id, mode='overwrite')

            datasets_created += 1

    print(f"\nComplete: {datasets_created} datasets created, {datasets_skipped} tables skipped")

from nodes.nipa_data import run as nipa_data_run

NODES = {
    run: [nipa_data_run],
}

if __name__ == '__main__':
    run()
