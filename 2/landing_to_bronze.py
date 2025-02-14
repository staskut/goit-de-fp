import requests
import os
import logging
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LandingToBronze")

# FTP paths
FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
TABLES = ["athlete_bio", "athlete_event_results"]
LANDING_ZONE = "landing/"
BRONZE_ZONE = "bronze/"

# Initialize Spark
spark = SparkSession.builder \
    .appName("LandingToBronze") \
    .getOrCreate()

def download_data(table_name):
    """Downloads CSV file from the FTP server."""
    url = f"{FTP_BASE_URL}{table_name}.csv"
    local_path = os.path.join(LANDING_ZONE, f"{table_name}.csv")

    logger.info(f"Downloading {url}...")
    response = requests.get(url)

    if response.status_code == 200:
        os.makedirs(LANDING_ZONE, exist_ok=True)
        with open(local_path, 'wb') as file:
            file.write(response.content)
        logger.info(f"Successfully downloaded {table_name}.csv")
    else:
        logger.error(f"Failed to download {table_name}.csv, status: {response.status_code}")
        exit(1)

def convert_csv_to_parquet(table_name):
    """Reads CSV with Spark and writes it as Parquet in the Bronze layer."""
    csv_path = os.path.join(LANDING_ZONE, f"{table_name}.csv")
    bronze_path = os.path.join(BRONZE_ZONE, table_name)

    logger.info(f"Processing {table_name}...")
    df = spark.read.option("header", "true").csv(csv_path)

    os.makedirs(BRONZE_ZONE, exist_ok=True)
    df.write.mode("overwrite").parquet(bronze_path)
    logger.info(f"{table_name} written to {bronze_path}")
    df.show()

if __name__ == "__main__":
    for table in TABLES:
        download_data(table)
        convert_csv_to_parquet(table)

    logger.info("All tables processed successfully!")
