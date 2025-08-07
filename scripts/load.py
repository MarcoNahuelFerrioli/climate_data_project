import logging
from transform import validate_df
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docker", ".env")
load_dotenv(dotenv_path)

# Load df into MongoDB database
def load_df_into_mongodb(df, collection):

    #Transform df spark to df pandas
    df_pandas = df.toPandas()
    if df_pandas.empty:
        logging.warning("DataFrame is empty")
        return
    #Convert df_pandas to a dict
    data = df_pandas.to_dict(orient="records")
    #Insert into collection
    collection.insert_many(data)
    logging.info("Data inserted into MongoDB database successfully")


