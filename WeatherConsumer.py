from kafka import KafkaConsumer
from hdfs import InsecureClient
import pandas as pd
from datetime import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv("hadoop.env")
load_dotenv("kafka.env")

HDFS_URL = os.getenv("HDFS_URL")
HDFS_USER = os.getenv("HDFS_USER")
HDFS_DIR = os.getenv("HDFS_DIR")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

client = InsecureClient(HDFS_URL, user=HDFS_USER)

TOPIC = "weather_stream"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=10000
)

# Local staging file
local_file = "weather_buffer.csv"


def WeatherConsumer(BATCH_SIZE: int = 5):

    # ➜ buffer per city
    buffers = {}

    for message in consumer:
        data = message.value
        print("[RECEIVED]", data)

        city = data.get("city", "Unknown")
        if city not in buffers:
            buffers[city] = []

        # Append to this city's buffer
        buffers[city].append({
            "city": city,
            "timestamp": data.get("timestamp"),
            "temperature": data.get("temperature"),
            "precipitation": data.get("precipitation"),
            "visibility": data.get("visibility"),
            "wind_speed": data.get("wind_speed"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude")
        })

        # If this city reached BATCH_SIZE → flush
        if len(buffers[city]) >= BATCH_SIZE:

            df = pd.DataFrame(buffers[city])
            df.to_csv(local_file, index=False)

            # HDFS folder for this city
            city_path = f"{HDFS_DIR}/weather/{city}"
            try:
                client.makedirs(city_path)
            except:
                pass  # folder exists

            # File name
            hdfs_file = os.path.join(
                city_path,
                f"weather_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
            )

            # Upload to HDFS
            client.upload(hdfs_file, local_file, overwrite=True)

            print(f"[INFO] Uploaded {len(buffers[city])} records to HDFS → {hdfs_file}")

            # Clear only this city's buffer
            buffers[city] = []


WeatherConsumer()
