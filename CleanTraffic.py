from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, explode, when
from pyspark.sql.types import DoubleType, IntegerType

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("CleanTrafficData") \
    .getOrCreate()

# -----------------------------
# HDFS Paths
# -----------------------------
TRAFFIC_BRONZE = "hdfs://localhost:9000/home/bigdata/tomtraffic/*.json"
SILVER_TRAFFIC = "hdfs://localhost:9000/home/bigdata/silver/traffic/"

# -----------------------------
# Read raw traffic JSON (multiLine=True)
# -----------------------------
traffic_raw = spark.read.option("multiLine", True).json(TRAFFIC_BRONZE)

# Explode arrays if needed (depends on your JSON structure)
traffic_df = traffic_raw.select(
    "id",
    "iconCategory",
    "description",
    "startTime",
    "endTime",
    "from",
    "to",
    "length",
    "delay",
    "magnitudeOfDelay",
    "roadNumbers",
    "bbox_city"
)

# -----------------------------
# Clean / normalize
# -----------------------------
traffic_clean = (
    traffic_df
    .withColumn("city", lower(col("bbox_city")))
    .withColumn("iconCategory", col("iconCategory").cast(IntegerType()))
    .withColumn("length", col("length").cast(DoubleType()))
    .withColumn("delay", col("delay").cast(DoubleType()))
    .withColumn("magnitudeOfDelay", col("magnitudeOfDelay").cast(IntegerType()))
    .withColumn("startTime", to_timestamp("startTime"))
    .withColumn("endTime", to_timestamp("endTime"))
    .withColumn("description",
                when(col("description").isNull(), "")
                .otherwise(col("description").cast("string")))
)

# Remove duplicates by id + startTime
traffic_clean = traffic_clean.dropDuplicates(["id", "startTime"])

# Fill null city
traffic_clean = traffic_clean.fillna({"city": "unknown"})

# -----------------------------
# Write cleaned Parquet to HDFS, partitioned by city
# -----------------------------
traffic_clean.write.mode("overwrite").partitionBy("city").parquet(SILVER_TRAFFIC)

print("✅ Traffic cleaning complete →", SILVER_TRAFFIC)

spark.stop()
