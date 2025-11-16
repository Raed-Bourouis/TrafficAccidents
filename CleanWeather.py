from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower
from pyspark.sql.types import DoubleType
from dotenv import load_dotenv
import os

load_dotenv("hadoop.env")

HDFS_URL = os.getenv("HDFS_URL")
HDFS_USER = os.getenv("HDFS_USER")
HDFS_DIR = os.getenv("HDFS_DIR")
# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("CleanWeatherData") \
    .getOrCreate()

# -----------------------------
# HDFS Paths (replace localhost:9000 if needed)
# -----------------------------
WEATHER_BRONZE = f"hdfs://localhost:9000/home/bigdata/weather/*/*.csv"
SILVER_WEATHER = f"hdfs://localhost:9000/home/bigdata/silver/weather/"

# -----------------------------
# Read raw weather CSV from HDFS
# -----------------------------
weather_df = spark.read.option("header", True).csv(WEATHER_BRONZE)

# -----------------------------
# Clean / normalize
# -----------------------------
weather_clean = (
    weather_df
    .withColumn("city", lower(col("city")))
    .withColumn("timestamp", to_timestamp("timestamp"))
    .withColumn("temperature", col("temperature").cast(DoubleType()))
    .withColumn("precipitation", col("precipitation").cast(DoubleType()))
    .withColumn("visibility", col("visibility").cast(DoubleType()))
    .withColumn("wind_speed", col("wind_speed").cast(DoubleType()))
    .withColumn("latitude", col("latitude").cast(DoubleType()))
    .withColumn("longitude", col("longitude").cast(DoubleType()))
)

# Remove duplicates
weather_clean = weather_clean.dropDuplicates(["city", "timestamp"])

# Fill missing numeric values
weather_clean = weather_clean.fillna({
    "temperature": 0.0,
    "precipitation": 0.0,
    "visibility": 0.0,
    "wind_speed": 0.0
})

# -----------------------------
# Write cleaned Parquet to HDFS, partitioned by city
# -----------------------------
weather_clean.write.mode("overwrite").partitionBy("city").parquet(SILVER_WEATHER)

print("✅ Weather cleaning complete →", SILVER_WEATHER)

spark.stop()
