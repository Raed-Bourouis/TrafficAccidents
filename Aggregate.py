from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, date_format, count, avg
from pyspark.sql.types import DoubleType

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("JoinWeatherTraffic") \
    .getOrCreate()

# -----------------------------
# HDFS Paths
# -----------------------------
SILVER_WEATHER = "hdfs://localhost:9000/home/bigdata/silver/weather/"
SILVER_TRAFFIC = "hdfs://localhost:9000/home/bigdata/silver/traffic/"
GOLD_PATH = "hdfs://localhost:9000/home/bigdata/gold/weather_traffic/"

# -----------------------------
# Load cleaned weather
# -----------------------------
weather_df = spark.read.parquet(SILVER_WEATHER)

# Extract hour and date for aggregation
weather_df = weather_df.withColumn("hour", hour(col("timestamp"))) \
                       .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))

# -----------------------------
# Load cleaned traffic
# -----------------------------
traffic_df = spark.read.parquet(SILVER_TRAFFIC)

# Extract hour and date from startTime
traffic_df = traffic_df.withColumn("hour", hour(col("startTime"))) \
                       .withColumn("date", date_format(col("startTime"), "yyyy-MM-dd"))

# -----------------------------
# Aggregate traffic by city + date + hour
# -----------------------------
traffic_agg = (
    traffic_df.groupBy("city", "date", "hour")
    .agg(
        count("id").alias("incident_count"),
        avg("length").alias("avg_length"),
        avg("delay").alias("avg_delay")
    )
)

# -----------------------------
# Aggregate weather by city + date + hour
# -----------------------------
weather_agg = (
    weather_df.groupBy("city", "date", "hour")
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("precipitation").alias("avg_precipitation"),
        avg("visibility").alias("avg_visibility"),
        avg("wind_speed").alias("avg_wind_speed")
    )
)

# -----------------------------
# Join weather + traffic on city + date + hour
# -----------------------------
gold_df = weather_agg.join(
    traffic_agg,
    on=["city", "date", "hour"],
    how="left"  # keep weather even if no traffic
)

# Fill null traffic metrics with 0
gold_df = gold_df.fillna({
    "incident_count": 0,
    "avg_length": 0.0,
    "avg_delay": 0.0
})

# -----------------------------
# Write gold dataset to HDFS
# -----------------------------
gold_df.write.mode("overwrite").partitionBy("city").parquet(GOLD_PATH)

print("✅ Gold dataset created →", GOLD_PATH)

spark.stop()
