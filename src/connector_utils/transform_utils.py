"""Shared transform utilities for BEA datasets."""

import re


def slugify(text: str) -> str:
    """Convert text to snake_case column name."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', '_', text.strip())
    return text


def parse_value(value_str: str) -> float | None:
    """Parse BEA data value to float, handling commas and special values."""
    if value_str is None:
        return None

    cleaned = str(value_str).replace(',', '').strip()

    if not cleaned or cleaned in ('', '...', '----', 'n.a.', '(NA)'):
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
