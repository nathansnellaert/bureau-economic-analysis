import os
import pyarrow as pa
from utils import get_client, load_state, save_state

def process_parameters(datasets_table):
    """
    Fetch parameters for each dataset using GetParameterList method.
    This discovers what parameters each dataset requires/accepts.
    """
    api_key = os.environ['BEA_API_KEY']
    base_url = "https://apps.bea.gov/api/data"
    
    client = get_client()
    
    # Convert datasets table to list of dicts
    datasets = datasets_table.to_pylist()
    
    parameters = []
    
    for dataset in datasets:
        dataset_name = dataset['dataset_name']
        
        # Skip obsolete datasets mentioned in the docs
        if dataset_name in ['RegionalData', 'RegionalIncome', 'RegionalProduct']:
            continue
        
        # GetParameterList for each dataset
        response = client.get(
            base_url,
            params={
                'UserID': api_key,
                'method': 'GetParameterList',
                'DatasetName': dataset_name,
                'ResultFormat': 'JSON'
            }
        )
        response.raise_for_status()
        
        result = response.json()
        
        # Extract parameters from the API response
        if 'BEAAPI' in result and 'Results' in result['BEAAPI']:
            param_list = result['BEAAPI']['Results'].get('Parameter', [])
            
            # Ensure it's a list
            if not isinstance(param_list, list):
                param_list = [param_list]
            
            for param in param_list:
                parameters.append({
                    'dataset_name': dataset_name,
                    'parameter_name': param.get('ParameterName', ''),
                    'parameter_data_type': param.get('ParameterDataType', ''),
                    'parameter_description': param.get('ParameterDescription', ''),
                    'parameter_is_required': param.get('ParameterIsRequiredFlag', '') == '1',
                    'parameter_default_value': param.get('ParameterDefaultValue', ''),
                    'multiple_accepted_flag': param.get('MultipleAcceptedFlag', '') == '1',
                    'all_value': param.get('AllValue', '')
                })
    
    return pa.Table.from_pylist(parameters)