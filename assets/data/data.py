import os
import pyarrow as pa
from utils import get_client, load_state, save_state
import time

def process_data(datasets_table, parameters_table):
    """
    Fetch actual data for each dataset using GetData method.
    Uses discovered parameters to construct appropriate requests.
    """
    api_key = os.environ['BEA_API_KEY']
    base_url = "https://apps.bea.gov/api/data"
    
    client = get_client()
    
    # Convert tables to dictionaries for easier lookup
    datasets = datasets_table.to_pylist()
    parameters = parameters_table.to_pylist()
    
    # Group parameters by dataset
    params_by_dataset = {}
    for param in parameters:
        dataset_name = param['dataset_name']
        if dataset_name not in params_by_dataset:
            params_by_dataset[dataset_name] = []
        params_by_dataset[dataset_name].append(param)
    
    # Load state to track what we've already fetched
    state = load_state('data')
    if not state:
        state = {}
    
    all_data = []
    
    for dataset in datasets:
        dataset_name = dataset['dataset_name']
        
        # Skip obsolete datasets
        if dataset_name in ['RegionalData', 'RegionalIncome', 'RegionalProduct']:
            continue
        
        # Skip if no parameters found (shouldn't happen, but be safe)
        if dataset_name not in params_by_dataset:
            continue
        
        dataset_params = params_by_dataset[dataset_name]
        
        # Build request parameters for GetData
        request_params = {
            'UserID': api_key,
            'method': 'GetData',
            'DatasetName': dataset_name,
            'ResultFormat': 'JSON'
        }
        
        # Add required parameters with appropriate "ALL" values
        for param in dataset_params:
            if param['parameter_is_required']:
                param_name = param['parameter_name']
                
                # Skip UserID and method as they're already added
                if param_name in ['UserID', 'method', 'DatasetName']:
                    continue
                
                # Use AllValue if available, otherwise use default
                if param['all_value']:
                    request_params[param_name] = param['all_value']
                elif param['parameter_default_value']:
                    request_params[param_name] = param['parameter_default_value']
                else:
                    # For some datasets, we need to make educated guesses
                    # This will be refined based on actual API responses
                    if param_name == 'Year':
                        request_params[param_name] = 'ALL'
                    elif param_name == 'TableName':
                        # We'll need to discover valid table names
                        # For now, skip this dataset
                        continue
        
        # Check if we have all required parameters
        missing_required = False
        for param in dataset_params:
            if param['parameter_is_required'] and param['parameter_name'] not in request_params:
                # Can't fetch this dataset without knowing valid values
                missing_required = True
                break
        
        if missing_required:
            continue
        
        try:
            # Rate limiting - max 100 requests per minute
            time.sleep(0.6)  # 60 seconds / 100 requests = 0.6 seconds per request
            
            response = client.get(base_url, params=request_params)
            response.raise_for_status()
            
            result = response.json()
            
            # Extract data from the API response
            if 'BEAAPI' in result and 'Results' in result['BEAAPI']:
                data_points = result['BEAAPI']['Results'].get('Data', [])
                
                # Ensure it's a list
                if not isinstance(data_points, list):
                    data_points = [data_points]
                
                # Add dataset name to each data point
                for point in data_points:
                    point['dataset_name'] = dataset_name
                    all_data.append(point)
                
                # Update state
                state[dataset_name] = {
                    'last_updated': time.strftime('%Y-%m-%d %H:%M:%S')
                }
        
        except Exception as e:
            # Log error but continue with other datasets
            print(f"Error fetching data for {dataset_name}: {str(e)}")
            continue
    
    # Save state
    save_state('data', state)
    
    # Return all collected data as a table
    if all_data:
        return pa.Table.from_pylist(all_data)
    else:
        # Return empty table with expected schema
        return pa.Table.from_pylist([{'dataset_name': '', 'message': 'No data fetched'}])