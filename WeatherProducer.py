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

WEATHER_API = os.getenv("WEATHER_API")


def Produce(LAT: float = 51.3, LON: float = -0.5):
    try:
        fields_str = ",".join(HOURLY_FIELDS)

        url = f"{WEATHER_API}latitude={LAT}&longitude={LON}&hourly={fields_str}"

        res = requests.get(url)
        if res.status_code != 200:
            print("[API ERROR]", res.text)
            return

        response = res.json()

        # Ensure expected fields exist
        hourly = response.get("hourly", {})
        if "time" not in hourly or len(hourly["time"]) == 0:
            print("[NO DATA] Hourly field missing")
            return

        # Always take latest sample → index -1
        idx = -1

        record = {
            "timestamp": hourly["time"][idx],
            "temperature": hourly["temperature_2m"][idx],
            "precipitation": hourly["precipitation"][idx],
            "visibility": hourly.get("visibility", [None])[idx],
            "wind_speed": hourly["windspeed_10m"][idx],
            "latitude": LAT,
            "longitude": LON
        }

        WeatherProducer.send(TOPIC, record)
        WeatherProducer.flush()

        print("[SENT]", record)

    except Exception as e:
        print("[ERROR]", e)



FRENCH_CITIES = {
    "Paris":         (2.20, 48.80, 2.48, 48.91),
    "Marseille":     (5.30, 43.23, 5.47, 43.38),
    "Lyon":          (4.77, 45.70, 4.95, 45.83),
    "Toulouse":      (1.35, 43.50, 1.53, 43.67),
    "Nice":          (7.20, 43.66, 7.31, 43.74),
    "Nantes":        (-1.63, 47.17, -1.51, 47.27),
    "Strasbourg":    (7.67, 48.53, 7.80, 48.63),
    "Montpellier":   (3.80, 43.56, 3.94, 43.66),
    "Bordeaux":      (-0.61, 44.78, -0.53, 44.88),
    "Lille":         (3.01, 50.59, 3.14, 50.67),
    "Rennes":        (-1.73, 48.06, -1.63, 48.16),
    "Reims":         (4.00, 49.20, 4.10, 49.30),
    "Saint-Étienne": (4.33, 45.40, 4.42, 45.49),
    "Toulon":        (5.88, 43.10, 5.96, 43.17),
    "Grenoble":      (5.67, 45.13, 5.76, 45.22),
}


def ProduceAllCities(frequency=30):
    for city, bbox in FRENCH_CITIES.items():
        # Use city center (midpoint)
        minLon, minLat, maxLon, maxLat = bbox
        lat = (minLat + maxLat) / 2
        lon = (minLon + maxLon) / 2

        try:
            fields_str = ",".join(HOURLY_FIELDS)
            url = f"{WEATHER_API}latitude={lat}&longitude={lon}&hourly={fields_str}"

            response = requests.get(url).json()
            hourly = response["hourly"]

            idx = -1  # latest hour

            record = {
                "city": city,
                "timestamp": hourly["time"][idx],
                "temperature": hourly["temperature_2m"][idx],
                "precipitation": hourly["precipitation"][idx],
                "visibility": hourly.get("visibility", [None])[idx],
                "wind_speed": hourly["windspeed_10m"][idx],
                "latitude": lat,
                "longitude": lon
            }

            WeatherProducer.send(TOPIC, record)
            print(f"[SENT {city}]", record)

        except Exception as e:
            print(f"[ERROR {city}]", e)

    time.sleep(frequency)


while True:
    ProduceAllCities()
