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

    beaapi = data.get('BEAAPI', {})

    # Check for top-level errors (e.g., ITA returns errors here)
    if 'Error' in beaapi:
        error = beaapi['Error']
        raise ValueError(f"BEA API Error: {error.get('APIErrorDescription', error)}")

    # Extract results - some datasets wrap in a list (e.g., GDPbyIndustry)
    results = beaapi.get('Results', {})
    if isinstance(results, list) and len(results) > 0:
        results = results[0]

    if isinstance(results, dict) and 'Error' in results:
        error = results['Error']
        raise ValueError(f"BEA API Error: {error.get('APIErrorDescription', error)}")

    result_data = results.get('Data', []) if isinstance(results, dict) else []

    if isinstance(result_data, dict):
        return []

    return result_data


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


def get_gdp_industry_data(table_id, frequency='A', industry='ALL', year='ALL'):
    """
    Get GDP by Industry data.

    Args:
        table_id: The table ID (integer 1-42, 208-209)
        frequency: A (Annual), Q (Quarterly)
        industry: Industry code(s) or 'ALL'
        year: Year(s) or 'ALL'

    Returns:
        List of data records
    """
    return get_data(
        'GDPbyIndustry',
        TableID=table_id,
        Frequency=frequency,
        Industry=industry,
        Year=year
    )


def get_fixed_assets_data(table_name, year='ALL'):
    """
    Get Fixed Assets data (annual only).

    Args:
        table_name: The table name (e.g., 'FAAt101')
        year: Year(s) or 'ALL'

    Returns:
        List of data records
    """
    return get_data(
        'FixedAssets',
        TableName=table_name,
        Year=year
    )


def get_ita_data(indicator, area_or_country='AllCountries', frequency='A', year='ALL'):
    """
    Get International Transactions data.

    Args:
        indicator: The indicator code (e.g., 'BalGds')
        area_or_country: Country/area code or 'AllCountries'
        frequency: A (Annual), QSA (Quarterly SA), QNSA (Quarterly NSA)
        year: Year(s) or 'ALL'

    Returns:
        List of data records
    """
    return get_data(
        'ITA',
        Indicator=indicator,
        AreaOrCountry=area_or_country,
        Frequency=frequency,
        Year=year
    )


def get_ni_underlying_data(table_name, frequency='A', year='X'):
    """
    Get NI Underlying Detail data.

    Same API shape as NIPA (TableName / Frequency / Year), different dataset name.
    """
    return get_data(
        'NIUnderlyingDetail',
        TableName=table_name,
        Frequency=frequency,
        Year=year,
    )


def get_iip_data(type_of_investment='All', component='All', frequency='A', year='ALL'):
    """
    Get International Investment Position data.

    Parameters:
        type_of_investment: TypeOfInvestment code or 'All'
        component: Component code (Assets, Liabilities, etc.) or 'All'
        frequency: A (Annual) or QNSA (Quarterly NSA)
        year: Year(s) or 'ALL'
    """
    return get_data(
        'IIP',
        TypeOfInvestment=type_of_investment,
        Component=component,
        Frequency=frequency,
        Year=year,
    )


def get_parameter_values_filtered(dataset_name, target_parameter, **filter_params):
    """
    Get valid parameter values filtered by other parameter values.

    Args:
        dataset_name: The dataset name (e.g., 'Regional')
        target_parameter: The parameter to get values for
        **filter_params: Filter parameters (e.g., TableName='SAGDP1')

    Returns:
        List of parameter value dicts
    """
    query_params = {
        'method': 'GETPARAMETERVALUESFILTERED',
        'DatasetName': dataset_name,
        'TargetParameter': target_parameter,
    }
    query_params.update(filter_params)

    response = rate_limited_get(query_params)
    response.raise_for_status()
    data = response.json()
    return data.get('BEAAPI', {}).get('Results', {}).get('ParamValue', [])
