import os
import pyarrow as pa
from utils import get_client, load_state, save_state

def process_datasets():
    """
    Fetch all available BEA datasets using GetDataSetList method.
    This is the entry point for discovering what data BEA offers.
    """
    api_key = os.environ['BEA_API_KEY']
    base_url = "https://apps.bea.gov/api/data"
    
    client = get_client()
    
    # GetDataSetList - discover all available datasets
    response = client.get(
        base_url,
        params={
            'UserID': api_key,
            'method': 'GetDataSetList',
            'ResultFormat': 'JSON'
        }
    )
    response.raise_for_status()
    
    result = response.json()
    
    # Extract datasets from the API response
    datasets = []
    if 'BEAAPI' in result and 'Results' in result['BEAAPI']:
        dataset_list = result['BEAAPI']['Results'].get('Dataset', [])
        
        # Ensure it's a list
        if not isinstance(dataset_list, list):
            dataset_list = [dataset_list]
        
        for dataset in dataset_list:
            datasets.append({
                'dataset_name': dataset.get('DatasetName', ''),
                'dataset_description': dataset.get('DatasetDescription', '')
            })
    
    return pa.Table.from_pylist(datasets)