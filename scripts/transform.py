#import libraries
from pyspark.sql.functions import col, to_timestamp, lit, when
from pyspark.sql import SparkSession
import os
import logging


#Create a dataframe from a JSON file
def create_dataframe(spark: SparkSession, path: str):
    logging.info("Creating the DataFrame")
    if not os.path.exists(path):
        logging.warning("File not found")
        return None
    df_raw_json = spark.read.json(path)
    if df_raw_json.rdd.isEmpty():
        logging.warning("df_raw_json is empty")
        return None
    logging.info("DataFrame created successfully")
    return df_raw_json

#Validate if DataFrame in not empty and None:
def validate_df(df, function_name):
    if df is None:
        logging.warning(f"The DataFrame is None. It is not possible to continue with the {function_name} function.")
        return False
    if df.rdd.isEmpty():
        logging.warning(f"The DataFrame is empty. It is not possible to continue with the {function_name} function.")
        return False
    return True


#Change data type to match with MongoDB data types
def change_data_type(df):
    if not validate_df(df, "change_data_type"):
        return None

    #Change time column data type to timestamp:
    df = df.withColumn("time", to_timestamp(col("time"), "dd/MM/yyyy HH:mm"))

    types = {
        "temp": "float",
        "dewp": "float",
        "rhum": "integer",
        "prcp": "float",
        "snow": "integer",
        "wdir": "integer",
        "wspd": "float",
        "wpgt": "float",
        "pres": "float",
        "tsun": "integer",
        "coco": "integer"
    }

    for column, dType in types.items():
        if column in df.columns:
            df = df.withColumn(column, col(column).cast(dType))
        else:
            logging.warning(f"{column} not found in df")

    logging.info("Data Types changed") 
    return df

def change_name_column(df):
        if not validate_df(df, "change_name_column"):
            return None

        logging.info("Starting to rename columns")
        renames = {
            "time": "timestamp",
            "temp": "temperature",
            "dewp": "dewPoint",
            "rhum": "relativeHumidity",
            "prcp": "precipitation",
            "snow": "snowDepth",
            "wdir": "windDirection",
            "wspd": "windSpeed",
            "wpgt": "peakWindGust",
            "pres": "pressure",
            "tsun": "sunshine",
            "coco": "weatherCondition"
        }

        for old, new in renames.items():
            df = df.withColumnRenamed(old, new)
        
        logging.info(f"{len(renames)} columns renamed successfully")
        return df


def weather_condition_codes(df):
    if not validate_df(df, "weather_condition_codes"):
        return None
    weather_codes = {
        1: "clear",
        2: "fair",
        3: "cloudy",
        4: "overcast",
        5: "fog",
        6: "freezing fog",
        7: "light rain",
        8: "rain",
        9: "heavy rain",
        10: "freezing rain",
        11: "heavy freezing rain",
        12: "sleet",
        13: "heavy sleet",
        14: "light snowfall",
        15: "snowfall",
        16: "heavy snowfall",
        17: "rain shower",
        18:"heavy rain shower",
        19: "sleet shower",
        20: "heavy sleet shower",
        21: "snow shower",
        22: "heavy snow shower",
        23: "lightning",
        24: "hail",
        25: "thunderstorm",
        26: "heavy thunderstorm",
        27: "storm"
    }

    df = df.replace(weather_codes, subset=["coco"])
    logging.info("Weather codes replaced successfully in coco column")
    return df

def add_climate_station(df, city):
    if not validate_df(df, "add_climate_station"):
        return None
    df = df.withColumn("climateStation", lit(city))
    return df


def drop_duplicate_rows(df, column):
    if not validate_df(df, "drop_duplicate_rows"):
        return None
    
    logging.info("Check if there are duplicate rows")
    duplicate = df.groupBy(column).count().filter("count > 1")
    if duplicate.rdd.isEmpty():
        logging.info(f"There aren't duplicate in {column} column")
        return df
    else: 
        logging.warning(f"There are duplicate rows in {column}")
        for row in duplicate.collect():
            logging.warning(f"Duplicate value: '{row[column]}', count: {row['count']}")
        logging.info("Dropping duplicates rows")
        df = df.dropDuplicates([column])
        logging.info("Duplicate rows dropped")
        return df

def validate_ranges(df):
    if not validate_df(df, "validate_ranges"):
        return None
    logging.info("Validating data ranges")

    checks = {
        "temperature": (-50, 60),
        "dewPoint": (-50, 60),
        "relativeHumidity": (0, 100),
        "precipitation": (0, None),  # solo mayor o igual a 0
        "snowDepth": (0, None),
        "windSpeed": (0, None),
        "pressure": (870, 1085)
    }

    #Iterate over each column and validate its values across all rows
    for column, (min_val, max_val) in checks.items():
        #Check if columns is in df
        if column in df.columns:
            #Check if column is null
            condition = col(column).isNotNull()
            #Check if min_val is not None(without limit)
            if min_val is not None:
                #Validate min_val condition
                condition &= col(column) >= min_val
            #Check if max_val is not None (without limit)
            if max_val is not None:
                #Validate max_val condition
                condition &= col(column) <= max_val
            #Load the rows that are out of expected range
            invalid = df.filter(~condition)
            count_invalid = invalid.count()

            #If condition is false, assign None (null) to the column value
            df = df.withColumn(column, when(condition, col(column)).otherwise(None))

            #Print in console the rows that are out of expected range
            if count_invalid > 0:
                logging.warning(f"{count_invalid} rows in '{column}' are out of expected range")
            else:
                logging.info(f"All values in '{column}' are within expected range")
        else:
            logging.warning(f"Column '{column}' not found in DataFrame")

    return df  
