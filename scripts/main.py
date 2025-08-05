#import libraries
import os
from pyspark.sql import SparkSession
import logging
import pandas as pd
import json
from meteostat import Point, Hourly
from datetime import datetime, timedelta
from extract import extract_weather_data
from load import load_df_into_mongodb
from transform import ( 
    change_data_type,
    create_dataframe, 
    change_name_column, 
    weather_condition_codes, 
    add_climate_station, 
    drop_duplicate_rows, 
    validate_ranges,
    )
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docker", ".env")

load_dotenv(dotenv_path)

# Create path to save raw data
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "raw"))

# Coordinates for Copenhague
location = Point(55.6759, 12.5655)
city = "Copenhague"

# Date range: current month
end_month = datetime.now()
start_month = end_month.replace(day=1)
filename_month = f"weather_{start_month.date()}_{end_month.date()}.json"

# Date range: Last 24 hours
end = datetime.now()
start = end - timedelta(days=1)
filename = f"weather_{start.date()}_{end.date()}.json"

if __name__ == "__main__":
    
    spark_master_url = os.getenv("SPARK_MASTER", "spark://spark_master:7077")

    #Build SparkSession
    spark = SparkSession.builder.appName("climate_project").master(spark_master_url).config("spark.jars.packages", os.getenv("SPARK_MONGO_CONNECTOR")).getOrCreate()

    #Extract data from meteostat
    output_path = extract_weather_data(location, start, end, filename, RAW_DATA_DIR)
    print(output_path)

    #Create df of Spark
    df = create_dataframe(spark, output_path)

    #Drop duplicate rows in the dataframe
    df = drop_duplicate_rows(df, "time")
    
    #Change data type of df
    df = change_data_type(df)

    #Validate ranges
    df = validate_ranges(df)

    #Replace ID to description in coco (weather condition code)
    df = weather_condition_codes(df)

    #Add climate station name into df
    df = add_climate_station(df, city)


    #Change name column
    df = change_name_column(df)

    #Load df into MongoDB data
    load_df_into_mongodb(df)

    #Stop SparkSession
    spark.stop()