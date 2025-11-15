from kafka import KafkaProducer
import requests
import json
import time
import os
from dotenv import load_dotenv


load_dotenv("kafka.env")


KAFKA_BROKER = os.getenv("KAFKA_BROKER")


TOPIC = "weather_stream"

HOURLY_FIELDS = [
    "temperature_2m",
    "precipitation",
    "windspeed_10m",
    "visibility"
]

WeatherProducer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

WEATHER_API=os.getenv("WEATHER_API")

def Produce(LAT: float=51.3, LON: float=-0.5, frequency:int=60 ):
    try:
        # Build the Open-Meteo API URL
        fields_str = ",".join(HOURLY_FIELDS)
        url = (
            f"{WEATHER_API}latitude={LAT}&longitude={LON}&hourly={fields_str}"
        )

        response = requests.get(url).json()

        # Extract latest weather sample
        record = {
            "timestamp": response["hourly"]["time"][0],
            "temperature": response["hourly"]["temperature_2m"][0],
            "precipitation": response["hourly"]["precipitation"][0],
            "visibility": response["hourly"]["visibility"][0],
            "wind_speed": response["hourly"]["windspeed_10m"][0],
            "latitude": LAT,
            "longitude": LON
        }

        # Send to Kafka
        WeatherProducer.send(TOPIC, record)
        WeatherProducer.flush()

        print("[SENT]", record)

        time.sleep(frequency)  # Send every 60 seconds

    except Exception as e:
        print("[ERROR]", e)
        time.sleep(10)
        
        
while True:
    Produce()