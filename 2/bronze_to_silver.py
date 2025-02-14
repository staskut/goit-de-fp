from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import re
import os

# Initialize Spark Session
spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

# Define Text Cleaning Function
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,\.\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

# Read Bronze Data
bronze_path = "bronze"
silver_path = "silver"
os.makedirs(silver_path, exist_ok=True)

tables = ["athlete_bio", "athlete_event_results"]

for table in tables:
    print(f"Processing {table}...")

    # Load data from Bronze
    df = spark.read.parquet(f"{bronze_path}/{table}")

    # Clean text columns
    for col_name in df.columns:
        if df.schema[col_name].dataType == StringType():
            df = df.withColumn(col_name, clean_text_udf(col(col_name)))

    # Remove duplicates
    df = df.dropDuplicates()

    # Save to Silver
    df.write.mode("overwrite").parquet(f"{silver_path}/{table}")
    print(f"{table} saved to Silver Zone")

    df.show()

print("Processing complete!")
