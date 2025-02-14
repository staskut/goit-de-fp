from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import avg, current_timestamp
from config import mysql_credentials, kafka_credentials

jdbc_url = mysql_credentials["jdbc_url"]
jdbc_user = mysql_credentials["jdbc_user"]
jdbc_password = mysql_credentials["jdbc_password"]
jdbc_table_bio = mysql_credentials["jdbc_table_bio"]
jdbc_table_results = mysql_credentials["jdbc_table_results"]

kafka_bootstrap_servers = kafka_credentials["bootstrap_servers"][0]
kafka_topic_results = kafka_credentials["athlete_results_topic"]
kafka_topic_aggregates = kafka_credentials["athlete_aggregates_topic"]
kafka_security_protocol = kafka_credentials["security_protocol"]
kafka_sasl_mechanism = kafka_credentials["sasl_mechanism"]
kafka_sasl_username = kafka_credentials["username"]
kafka_sasl_password = kafka_credentials["password"]

spark = SparkSession.builder \
    .appName("JDBCToKafka") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .config("spark.kafka.security.protocol", kafka_security_protocol) \
    .config("spark.kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .config("spark.kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_sasl_username}" password="{kafka_sasl_password}";') \
    .getOrCreate()

# 1. Зчитати дані фізичних показників атлетів за допомогою Spark з MySQL таблиці olympic_dataset.athlete_bio
df_bio = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=jdbc_table_bio,
    user=jdbc_user,
    password=jdbc_password
).load()

# 2. Відфільтрувати дані, де показники зросту та ваги є порожніми або не є числами.
df_filtered = df_bio.filter(
    (col("height").isNotNull()) & (col("weight").isNotNull()) &
    (col("height") > 0) & (col("weight") > 0)
).withColumnRenamed("country_noc", "bio_country_noc")

# 3. Зчитати дані з mysql таблиці athlete_event_results і записати в кафка топік athlete_event_results.
df_results = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable=jdbc_table_results,
    user=jdbc_user,
    password=jdbc_password
).load()

df_results_json = df_results.withColumn("key", col("athlete_id").cast(StringType())) \
    .withColumn("value", to_json(struct([col(c) for c in df_results.columns])))

df_results_json.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", kafka_topic_results) \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_sasl_username}" password="{kafka_sasl_password}";') \
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    .save()

# Зчитати дані з результатами змагань з Kafka-топіку athlete_event_results.
df_kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_results) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .option("kafka.security.protocol", kafka_security_protocol) \
    .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_sasl_username}" password="{kafka_sasl_password}";') \
    .load()

schema_results = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True)
])

df_kafka_parsed = df_kafka_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), schema_results)) \
    .select("data.*")

# 4. Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id.

df_joined = df_kafka_parsed.join(df_filtered, on="athlete_id", how="inner")

# 5. Знайти середній зріст і вагу атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc).

df_aggregated = df_joined.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight")) \
    .withColumn("timestamp", current_timestamp())

# 6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
#
# а) вихідний кафка-топік,
def write_to_sinks(batch_df, batch_id):
    batch_df.selectExpr("CAST(sport AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_topic_aggregates) \
        .option("kafka.security.protocol", kafka_security_protocol) \
        .option("kafka.sasl.mechanism", kafka_sasl_mechanism) \
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_sasl_username}" password="{kafka_sasl_password}";') \
        .save()

    #     6. Зробіть стрим даних (за допомогою функції forEachBatch) у:
    #       b) базу даних.
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url.replace("olympic_dataset", "staskut")) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "athlete_aggregates") \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

query = df_aggregated.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_sinks) \
    .start()

query.awaitTermination()

