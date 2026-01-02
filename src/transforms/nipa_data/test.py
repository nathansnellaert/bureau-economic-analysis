"""Validation for NIPA data transforms."""

import re
import pyarrow as pa
from subsets_utils import validate


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
