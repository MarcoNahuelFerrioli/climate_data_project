import logging
from transform import validate_df
import os
from dotenv import load_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docker", ".env")

load_dotenv(dotenv_path)

#Load df into MongoDB database
def load_df_into_mongodb(df):
    if not validate_df(df, "load_df"):
        return None

    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_INITDB_DATABASE", "climate_db")

    if not mongo_uri:
        logging.error("MongoDB URI is not set in the environment variables.")
        return None    
    
    df.write.format("mongodb") \
        .mode("append") \
        .option("uri", mongo_uri) \
        .option("database", mongo_db) \
        .option("collection", "weather_data") \
        .save()
    logging.info("DataFrame loaded successfully into MongoDB")