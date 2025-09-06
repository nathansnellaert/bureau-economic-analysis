import pyarrow as pa
from datetime import datetime
from utils import get, load_state, save_state
import json
import os

API_BASE_URL = "https://apps.bea.gov/api/data"
API_KEY = os.environ['BEA_API_KEY']

REGIONAL_TABLES = ["CAINC1", "CAEMP25N", "SQINC1"]

def fetch_regional_table(table_name: str, year: str = "ALL"):
    params = {
        "UserID": API_KEY,
        "method": "GetData",
        "datasetname": "Regional",
        "TableName": table_name,
        "Frequency": "A",
        "Year": year,
        "GeoFIPS": "STATE",
        "LineCode": "1",
        "ResultFormat": "JSON"
    }
    
    response = get(API_BASE_URL, params=params)
    data = response.json()
    
    if "BEAAPI" in data and "Results" in data["BEAAPI"]:
        if "Error" in data["BEAAPI"]["Results"]:
            error = data["BEAAPI"]["Results"]["Error"]
            error_msg = error.get("@APIErrorDescription", "Unknown API error")
            error_code = error.get("@APIErrorCode", "Unknown")
            raise Exception(f"BEA API Error {error_code}: {error_msg}")
        if "Data" in data["BEAAPI"]["Results"]:
            return data["BEAAPI"]["Results"]["Data"]
    return []

def clean_numeric_value(value):
    if value is None or value == "" or value in ["n/a", "NA", "(NA)", "(NM)"]:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except:
        return None

def process_regional():
    state = load_state("regional")
    
    all_records = []
    
    for table in REGIONAL_TABLES:
        try:
            data = fetch_regional_table(table)
            
            for item in data:
                record = {
                    "dataset": "Regional",
                    "table_name": table,
                    "frequency": "A",
                    "geo_fips": item.get("GeoFips", ""),
                    "geo_name": item.get("GeoName", ""),
                    "code": item.get("Code", ""),
                    "time_period": item.get("TimePeriod", ""),
                    "description": item.get("Description", ""),
                    "unit": item.get("CL_UNIT", item.get("UNIT_MULT", "units")),
                    "value": clean_numeric_value(item.get("DataValue"))
                }
                
                period = str(item.get("TimePeriod", ""))
                if period.isdigit() and len(period) == 4:
                    record["year"] = int(period)
                else:
                    record["year"] = None
                
                all_records.append(record)
                
        except Exception as e:
            print(f"Error fetching {table}: {e}")
            continue
    
    if all_records:
        save_state("regional", {"last_updated": datetime.now().isoformat()})
        return pa.Table.from_pylist(all_records)
    
    return pa.Table.from_pylist([])