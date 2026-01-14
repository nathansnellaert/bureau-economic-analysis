"""BEA (Bureau of Economic Analysis) API client with rate limiting."""
import os

from ratelimit import limits, sleep_and_retry
from subsets_utils import get

BASE_URL = "https://apps.bea.gov/api/data"


def get_api_key():
    """Get BEA API key from environment."""
    return os.environ['BEA_API_KEY']


@sleep_and_retry
@limits(calls=1, period=2)  # BEA rate limit is ~100 requests/minute, need 2s between calls
def rate_limited_get(params):
    """Make a rate-limited GET request to BEA API."""
    params['UserID'] = get_api_key()
    params['ResultFormat'] = 'JSON'
    response = get(BASE_URL, params=params, timeout=120.0)
    return response


def get_dataset_list():
    """
    Get list of all available datasets.

    Returns:
        List of dataset metadata dicts
    """
    response = rate_limited_get({'method': 'GETDATASETLIST'})
    response.raise_for_status()
    data = response.json()
    return data.get('BEAAPI', {}).get('Results', {}).get('Dataset', [])


def get_parameter_list(dataset_name):
    """
    Get parameters for a specific dataset.

    Args:
        dataset_name: The dataset name (e.g., 'NIPA', 'Regional')

    Returns:
        List of parameter metadata dicts
    """
    response = rate_limited_get({
        'method': 'GETPARAMETERLIST',
        'DatasetName': dataset_name
    })
    response.raise_for_status()
    data = response.json()
    return data.get('BEAAPI', {}).get('Results', {}).get('Parameter', [])


def get_parameter_values(dataset_name, parameter_name):
    """
    Get valid values for a specific parameter.

    Args:
        dataset_name: The dataset name
        parameter_name: The parameter name

    Returns:
        List of parameter value dicts
    """
    response = rate_limited_get({
        'method': 'GETPARAMETERVALUES',
        'DatasetName': dataset_name,
        'ParameterName': parameter_name
    })
    response.raise_for_status()
    data = response.json()
    return data.get('BEAAPI', {}).get('Results', {}).get('ParamValue', [])


def get_data(dataset_name, **params):
    """
    Get data from a specific dataset.

    Args:
        dataset_name: The dataset name (e.g., 'NIPA', 'Regional')
        **params: Additional parameters for the query

    Returns:
        List of data records
    """
    query_params = {
        'method': 'GETDATA',
        'DatasetName': dataset_name,
    }
    query_params.update(params)

    response = rate_limited_get(query_params)
    response.raise_for_status()
    data = response.json()

    # Check for errors
    results = data.get('BEAAPI', {}).get('Results', {})
    if 'Error' in results:
        error = results['Error']
        raise ValueError(f"BEA API Error: {error.get('APIErrorDescription', error)}")

    return results.get('Data', [])


def get_nipa_data(table_name, frequency='A', year='X'):
    """
    Get NIPA (National Income and Product Accounts) data.

    Args:
        table_name: The NIPA table name (e.g., 'T10101' for GDP)
        frequency: A (Annual), Q (Quarterly), M (Monthly)
        year: Year or 'X' for all years

    Returns:
        List of data records
    """
    return get_data(
        'NIPA',
        TableName=table_name,
        Frequency=frequency,
        Year=year
    )


def get_regional_data(table_name, line_code, geo_fips='STATE', year='LAST5'):
    """
    Get Regional economic data.

    Args:
        table_name: The table name (e.g., 'CAGDP1' for GDP by state)
        line_code: The line code for specific series
        geo_fips: Geographic level (STATE, COUNTY, MSA, etc.)
        year: Year range

    Returns:
        List of data records
    """
    return get_data(
        'Regional',
        TableName=table_name,
        LineCode=line_code,
        GeoFips=geo_fips,
        Year=year
    )
