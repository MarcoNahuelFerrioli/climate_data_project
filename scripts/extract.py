import os
import pandas as pd
from datetime import datetime, timedelta
from meteostat import Point, Hourly
import json
import logging

def extract_weather_data(location: Point, start: datetime, end: datetime, filename: str, raw_data_dir: str):
    data = Hourly(location, start, end)
    df = data.fetch()
    if df.empty: 
        logging.info("The DataFrame is empty")
        return None
    else:
        df.reset_index(inplace=True) 
        
        if not os.path.exists(raw_data_dir):
            os.makedirs(raw_data_dir, exist_ok=True)
        output_path = os.path.join(raw_data_dir, filename)
        df.to_json(output_path, orient="records", lines=True, date_format="iso")
        logging.info(f"File saved at: {output_path}")
        return output_path