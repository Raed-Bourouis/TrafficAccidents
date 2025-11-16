from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, avg, sum as spark_sum
import matplotlib.pyplot as plt
import seaborn as sns

spark = SparkSession.builder.appName("WeatherTrafficAnalysis").getOrCreate()

GOLD_PATH = "hdfs://localhost:9000/home/bigdata/gold/weather_traffic/"

# Read all partitions (all cities)
gold_df = spark.read.parquet(GOLD_PATH)

# Inspect
gold_df.printSchema()
gold_df.show(5)


#Total Accidents
print("Total Accidents")
total_accidents = gold_df.agg(spark_sum("incident_count").alias("total_accidents")).collect()[0]["total_accidents"]
print("Total accidents:", total_accidents)

#Accidents Per City
print("Accidents Per City")
acc_per_city = gold_df.groupBy("city").agg(spark_sum("incident_count").alias("accidents"))
acc_per_city.show()

# Average precipitation during accidents per city
print("Average precipitation during accidents per city")
avg_precip_city = gold_df.groupBy("city").agg({"avg_precipitation": "avg"})
avg_precip_city.show()


# Accidents per hour
print("Accidents per hour")
acc_per_hour = gold_df.groupBy("hour").agg(spark_sum("incident_count").alias("accidents"))
acc_per_hour.show()


acc_per_city_pd = acc_per_city.toPandas()
acc_per_hour_pd = acc_per_hour.toPandas()
avg_precip_city_pd = avg_precip_city.toPandas()




# Total accidents per city
sns.barplot(data=acc_per_city_pd, x="city", y="accidents")
plt.xticks(rotation=45)
plt.title("Total Accidents per City")
plt.show()

# Accidents by hour of day
sns.lineplot(data=acc_per_hour_pd, x="hour", y="accidents", marker="o")
plt.title("Accidents by Hour")
plt.show()

# Average precipitation by city
sns.barplot(data=avg_precip_city_pd, x="city", y="avg_precipitation")
plt.xticks(rotation=45)
plt.title("Average Precipitation per City")
plt.show()
