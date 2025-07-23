import os
import pandas as pd
from datetime import datetime, timedelta
from meteostat import Point, Hourly
import json


# Create path to save raw data
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "..", "data", "raw")

# Coordinates for Copenhagen
location = Point(55.67, 12.56)

# Date range: current month
end_month = datetime.now()
start_month = end_month.replace(day=1)
filename_month = f"weather_{start_month.date()}_{end_month.date()}.json"

# Date range: Last 24 hours
end_daily = datetime.now()
start_daily = end_daily - timedelta(days=1)
filename_daily = f"weather_{start_daily.date()}_{end_daily.date()}.json"


def extract_weather_data(location: Point, start: datetime, end: datetime, filename: str):
    data = Hourly(location, start, end)
    df = data.fetch()
    if df.empty: 
        print("The DataFrame is empty")
        return None
    else:
        output_path = os.path.join(RAW_DATA_DIR, filename)
        df.to_json(output_path, orient="records", lines=True, date_format="iso")
        print(f"File saved at: {output_path}")
        return output_path
    

#extract_weather_monthly = extract_weather_data(location, start_month, end_month, filename_month)
extract_weather_daily = extract_weather_data(location, start_daily, end_daily, filename_daily)