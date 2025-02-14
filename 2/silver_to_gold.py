from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

# Load Silver Data
df_bio = spark.read.parquet("silver/athlete_bio")
df_results = spark.read.parquet("silver/athlete_event_results")

# Convert height & weight to numerical type if needed
df_bio = df_bio.withColumn("height", col("height").cast("double")) \
    .withColumn("weight", col("weight").cast("double")) \
    .drop("country_noc")

# Join Datasets
df_joined = df_results.join(df_bio, on="athlete_id", how="inner")

# Compute Averages
df_aggregated = df_joined.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight")
) \
    .withColumn("timestamp", current_timestamp())

# Save to Gold Layer
df_aggregated.write.mode("overwrite").parquet("gold/avg_stats")

df_aggregated.show()

print("Data successfully saved to gold layer!")

# Stop Spark Session
spark.stop()