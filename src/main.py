import argparse

from subsets_utils import validate_environment
from ingest import nipa_tables as ingest_nipa_tables
from ingest import nipa_data as ingest_nipa_data
from transforms.nipa_data import main as transform_nipa_data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment(['BEA_API_KEY'])

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        print("\n--- Ingesting NIPA table catalog ---")
        ingest_nipa_tables.run()
        print("\n--- Ingesting NIPA data ---")
        ingest_nipa_data.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")
        print("\n--- Transforming NIPA data ---")
        transform_nipa_data.run()


if __name__ == "__main__":
    main()
