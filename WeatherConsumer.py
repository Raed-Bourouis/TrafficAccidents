from kafka import KafkaConsumer
from hdfs import InsecureClient
import pandas as pd
from datetime import datetime
import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv("hadoop.env")
load_dotenv("kafka.env")


HDFS_URL = os.getenv("HDFS_URL") 
HDFS_USER =os.getenv("HDFS_USER") 
HDFS_DIR = os.getenv("HDFS_DIR")
KAFKA_BROKER=os.getenv("KAFKA_BROKER")

client = InsecureClient(HDFS_URL, user=HDFS_USER)

TOPIC="weather_stream"

# Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=10000  # exits if no new message for 10 sec
)



local_file = "weather_buffer.csv"
buffer = []

def WeatherConsumer(BATCH_SIZE: int = 10):

    for message in consumer:
        data = message.value

        # Append to buffer
        buffer.append({
            "timestamp": data.get("timestamp"),
            "temperature": data.get("temperature"),
            "precipitation": data.get("precipitation"),
            "visibility": data.get("visibility"),
            "wind_speed": data.get("wind_speed"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude")
        })

        if len(buffer) >= BATCH_SIZE:
            # Convert to CSV
            df = pd.DataFrame(buffer)
            df.to_csv(local_file, index=False)

            # Generate unique HDFS file name
            hdfs_file = os.path.join(HDFS_DIR, f"weather_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv")

            # Write CSV to HDFS
            client.upload(hdfs_file, local_file, overwrite=True)
            print(f"[INFO] Uploaded {len(buffer)} records to HDFS: {hdfs_file}")

            # Clear buffer
            buffer = []

