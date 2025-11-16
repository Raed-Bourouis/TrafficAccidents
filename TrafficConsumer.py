from kafka import KafkaConsumer
from hdfs import InsecureClient
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
TOPIC = "traffic-accidents"
BATCH_SIZE = 10
LOCAL_FILE = "accidents_buffer.json"

client = InsecureClient(HDFS_URL, user=HDFS_USER)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=10000
)

buffer = []

def TrafficAccidentConsumer(batch_size=BATCH_SIZE):
    global buffer

    for message in consumer:
        data = message.value
        print("[TRAFFIC]", data)

        buffer.append(data)

        if len(buffer) >= batch_size:
            # write buffer
            with open(LOCAL_FILE, "w", encoding="utf-8") as f:
                json.dump(buffer, f, ensure_ascii=False, indent=2)

            # HDFS file name
            hdfs_file = os.path.join(
                HDFS_DIR,
                f"tomtraffic/traffic_incidents_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            )

            client.upload(hdfs_file, LOCAL_FILE, overwrite=True)
            print(f"[INFO] Uploaded {len(buffer)} traffic rows → {hdfs_file}")

            buffer = []


if __name__ == "__main__":
    print("🚀 Traffic Accidents Consumer (Multi-city) started")
    try:
        TrafficAccidentConsumer()
    except KeyboardInterrupt:
        if buffer:
            with open(LOCAL_FILE, "w", encoding="utf-8") as f:
                json.dump(buffer, f, ensure_ascii=False, indent=2)

            hdfs_file = os.path.join(
                HDFS_DIR,
                f"tomtraffic/traffic_incidents_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            )

            client.upload(hdfs_file, LOCAL_FILE, overwrite=True)
            print(f"[INFO] Uploaded remaining {len(buffer)} traffic rows → {hdfs_file}")

        print("🛑 Consumer stopped.")
