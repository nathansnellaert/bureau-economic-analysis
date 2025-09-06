import pyarrow as pa
from datetime import datetime
from utils import get, load_state, save_state
import json
import os

API_BASE_URL = "https://apps.bea.gov/api/data"
API_KEY = os.environ['BEA_API_KEY']

NIPA_TABLES = ["T10101", "T10105", "T20100", "T20301", "T20305"]

def fetch_nipa_table(table_name: str, frequency: str, year: str = "ALL"):
    params = {
        "UserID": API_KEY,
        "method": "GetData",
        "datasetname": "NIPA",
        "TableName": table_name,
        "Frequency": frequency,
        "Year": year,
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

def process_nipa():
    state = load_state("nipa")
    
    all_records = []
    
    for table in NIPA_TABLES:
        for frequency in ["Q", "A"]:
            try:
                data = fetch_nipa_table(table, frequency)
                
                for item in data:
                    record = {
                        "dataset": "NIPA",
                        "table_name": table,
                        "frequency": frequency,
                        "series_code": item.get("SeriesCode", ""),
                        "line_number": item.get("LineNumber", ""),
                        "line_description": item.get("LineDescription", ""),
                        "time_period": item.get("TimePeriod", ""),
                        "metric_name": item.get("LineDescription", ""),
                        "unit": item.get("CL_UNIT", item.get("UNIT_MULT", "units")),
                        "value": clean_numeric_value(item.get("DataValue"))
                    }
                    
                    period = str(item.get("TimePeriod", ""))
                    if len(period) == 4 and period.isdigit():
                        record["year"] = int(period)
                        record["quarter"] = None
                        record["month"] = None
                    elif "Q" in period:
                        parts = period.split("Q")
                        if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                            record["year"] = int(parts[0])
                            record["quarter"] = int(parts[1])
                            record["month"] = None
                    elif len(period) == 6 and period.isdigit():
                        record["year"] = int(period[:4])
                        record["quarter"] = None
                        record["month"] = int(period[4:])
                    else:
                        record["year"] = None
                        record["quarter"] = None
                        record["month"] = None
                    
                    all_records.append(record)
                    
            except Exception as e:
                print(f"Error fetching {table} ({frequency}): {e}")
                continue
    
    if all_records:
        save_state("nipa", {"last_updated": datetime.now().isoformat()})
        return pa.Table.from_pylist(all_records)
    
    return pa.Table.from_pylist([])