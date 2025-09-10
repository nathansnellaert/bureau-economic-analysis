import os
os.environ['CONNECTOR_NAME'] = 'bea'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.datasets.datasets import process_datasets
from assets.parameters.parameters import process_parameters
from assets.data.data import process_data

def main():
    env = validate_environment(['BEA_API_KEY'])
    
    # Process and upload in DAG order matching API discovery flow
    datasets = process_datasets()
    upload_data(datasets, "bea_datasets")
    
    parameters = process_parameters(datasets)
    upload_data(parameters, "bea_parameters")
    
    data = process_data(datasets, parameters)
    upload_data(data, "bea_data")

if __name__ == "__main__":
    main()