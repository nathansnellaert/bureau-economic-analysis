import pyarrow as pa
from datetime import datetime
from utils import get, load_state, save_state
import json
import os

API_BASE_URL = "https://apps.bea.gov/api/data"
API_KEY = os.environ['BEA_API_KEY']

GDP_INDUSTRY_TABLES = ["VAL", "QTY"]

def fetch_gdp_industry_table(table_name: str, frequency: str, year: str = "ALL"):
    params = {
        "UserID": API_KEY,
        "method": "GetData",
        "datasetname": "GDPbyIndustry",
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

def process_gdp_by_industry():
    state = load_state("gdp_by_industry")
    
    all_records = []
    
    for table in GDP_INDUSTRY_TABLES:
        for frequency in ["Q", "A"]:
            try:
                data = fetch_gdp_industry_table(table, frequency)
                
                for item in data:
                    record = {
                        "dataset": "GDPbyIndustry",
                        "table_name": table,
                        "frequency": frequency,
                        "industry_id": item.get("IndustryID", ""),
                        "industry": item.get("Industry", ""),
                        "time_period": item.get("TimePeriod", ""),
                        "table_id": item.get("TableID", ""),
                        "naics_code": item.get("NAICS", ""),
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
        save_state("gdp_by_industry", {"last_updated": datetime.now().isoformat()})
        return pa.Table.from_pylist(all_records)
    
    return pa.Table.from_pylist([])