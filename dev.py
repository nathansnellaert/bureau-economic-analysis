import os

# Set environment variables for this run
os.environ['CONNECTOR_NAME'] = 'bureau-economic-analysis'
os.environ['RUN_ID'] = 'local-dev'
os.environ['ENABLE_HTTP_CACHE'] = 'true'
os.environ['CACHE_REQUESTS'] = 'false'
os.environ['WRITE_SNAPSHOT'] = 'false'
os.environ['STORAGE_BACKEND'] = 'local'
os.environ['DATA_DIR'] = 'data'

# You'll need to set your BEA API key here or as an environment variable
# Get one at: https://apps.bea.gov/api/signup/
# os.environ['BEA_API_KEY'] = 'YOUR-36-CHARACTER-KEY'

# Test individual assets
from utils import validate_environment, upload_data
from assets.datasets.datasets import process_datasets
from assets.parameters.parameters import process_parameters

# Validate we have the API key
try:
    env = validate_environment(['BEA_API_KEY'])
    has_api_key = True
except Exception as e:
    print(f"WARNING: {e}")
    print("Get an API key from: https://apps.bea.gov/api/signup/")
    has_api_key = False

if has_api_key:
    print("Testing BEA connector...")
    print("=" * 50)
    
    # Step 1: Get available datasets
    print("\n1. Fetching available datasets...")
    datasets = process_datasets()
    print(f"Found {len(datasets)} datasets")
    print("\nDatasets:")
    for row in datasets.to_pylist()[:5]:  # Show first 5
        print(f"  - {row['dataset_name']}: {row['dataset_description'][:50]}...")
    
    # Step 2: Get parameters for each dataset
    print("\n2. Fetching parameters for each dataset...")
    parameters = process_parameters(datasets)
    print(f"Found {len(parameters)} total parameters across all datasets")
    
    # Group by dataset to show summary
    params_by_dataset = {}
    for row in parameters.to_pylist():
        ds = row['dataset_name']
        if ds not in params_by_dataset:
            params_by_dataset[ds] = []
        params_by_dataset[ds].append(row)
    
    print("\nParameters by dataset:")
    for ds, params in list(params_by_dataset.items())[:3]:  # Show first 3 datasets
        required = [p for p in params if p['parameter_is_required']]
        print(f"  - {ds}: {len(params)} parameters ({len(required)} required)")
        for p in required:
            print(f"      * {p['parameter_name']} ({p['parameter_data_type']})")
    
    print("\n" + "=" * 50)
    print("Basic structure verified! To test full data retrieval, uncomment the code below.")
    
    # Uncomment to test data retrieval (will make many API calls)
    # from assets.data.data import process_data
    # print("\n3. Fetching actual data...")
    # data = process_data(datasets, parameters)
    # print(f"Retrieved {len(data)} data points")
    # if len(data) > 0:
    #     print("\nSample data point:")
    #     print(data.to_pylist()[0])
else:
    print("\nPlease set your BEA_API_KEY environment variable to test the connector")