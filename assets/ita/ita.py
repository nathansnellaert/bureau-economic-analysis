import pyarrow as pa
from datetime import datetime
from utils import get, load_state, save_state
import json
import os

API_BASE_URL = "https://apps.bea.gov/api/data"
API_KEY = os.environ['BEA_API_KEY']

ITA_TABLES = ["Balance", "Trade"]

def fetch_ita_table(table_name: str, frequency: str, year: str = "ALL"):
    params = {
        "UserID": API_KEY,
        "method": "GetData",
        "datasetname": "ITA",
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

def process_ita():
    state = load_state("ita")
    
    all_records = []
    
    for table in ITA_TABLES:
        for frequency in ["Q", "A"]:
            try:
                data = fetch_ita_table(table, frequency)
                
                for item in data:
                    record = {
                        "dataset": "ITA",
                        "table_name": table,
                        "frequency": frequency,
                        "indicator_code": item.get("IndicatorCode", ""),
                        "indicator_name": item.get("Indicator", ""),
                        "time_period": item.get("TimePeriod", ""),
                        "account": item.get("Account", ""),
                        "series_name": item.get("SeriesName", ""),
                        "unit": item.get("CL_UNIT", "millions_of_dollars"),
                        "value": clean_numeric_value(item.get("DataValue"))
                    }
                    
                    period = str(item.get("TimePeriod", ""))
                    if len(period) == 4:
                        record["year"] = int(period)
                        record["quarter"] = None
                    elif "Q" in period:
                        parts = period.split("Q")
                        record["year"] = int(parts[0])
                        record["quarter"] = int(parts[1])
                    else:
                        record["year"] = None
                        record["quarter"] = None
                    
                    all_records.append(record)
                    
            except Exception as e:
                print(f"Error fetching {table} ({frequency}): {e}")
                continue
    
    if all_records:
        save_state("ita", {"last_updated": datetime.now().isoformat()})
        return pa.Table.from_pylist(all_records)
    
    return pa.Table.from_pylist([])