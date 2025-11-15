import time
import json
import requests
from kafka import KafkaProducer
import logging
import os
from dotenv import load_dotenv
import csv


load_dotenv("kafka.env")
load_dotenv("traffic.env")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# ------------------------------------------------------
#                SETTINGS
# ------------------------------------------------------
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
TOPIC = "traffic-accidents"
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
CATEGORY_FILTER = "0,1,3,11,14"
POLL_INTERVAL = 10

# BBOX Tunisia example — change as needed


class TomTomKafkaProducer:
    def __init__(self, bootstrap=KAFKA_BROKER):
        self.sent_ids = set()
        self.total_sent = 0
        self.total_errors = 0
        self.start_time = time.time()

        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap],
            acks="all",
            # compression_type="gzip",
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=30,
        )

    # ------------------------------------------------------
    #           CALLBACKS
    # ------------------------------------------------------
    def on_success(self, record_metadata, accident_id):
        self.sent_ids.add(accident_id)
        self.total_sent += 1
        logging.info(
            f"✓ Sent (partition={record_metadata.partition}, offset={record_metadata.offset})"
        )

    def on_error(self, exc):
        self.total_errors += 1
        logging.error(f"✗ Kafka error: {exc}")

    # ------------------------------------------------------
    #          SEND FUNCTION (dedup + partition)
    # ------------------------------------------------------
    def send_incident(self, incident):
        props = incident.get("properties", {})
        accident_id = props.get("id")

        if not accident_id:
            logging.warning("Skipping incident without ID")
            return

        # ✔ Deduplication
        if accident_id in self.sent_ids:
            logging.info(f"Duplicate skipped: {accident_id}")
            return

        # Minimal message
        msg = {
            "id": accident_id,
            "iconCategory": props.get("iconCategory"),
            "description": props.get("events")[0].get("description"),
            "startTime": props.get("startTime"),
            "endTime": props.get("endTime"),
            "from": props.get("from"),
            "to": props.get("to"),
            "length": props.get("length"),
            "delay": props.get("delay"),
            "magnitudeOfDelay": props.get("magnitudeOfDelay"),
            "roadNumbers": props.get("roadNumbers"),
        }

        # ✔ Use accident_id as Kafka key → consistent partitioning
        future = self.producer.send(TOPIC, key=str(accident_id), value=msg)
        future.add_callback(self.on_success, accident_id=accident_id)
        future.add_errback(self.on_error)

    # ------------------------------------------------------
    #          TOMTOM FETCH
    # ------------------------------------------------------

    def fetch_from_tomtom(
        self,
        minLon: int = -0.5,
        minLat: int = 51.3,
        maxLon: int = 0.3,
        maxLat: int = 51.7,
    ):
        url = (
            "https://api.tomtom.com/traffic/services/5/incidentDetails"
            f"?key={TOMTOM_API_KEY}"
            f"&bbox={minLon},{minLat},{maxLon},{maxLat}"
            f"&categoryFilter={CATEGORY_FILTER}"
            "&fields={incidents{type,properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity}}}"
        )

        resp = requests.get(url)
        resp.raise_for_status()

        data = resp.json()
        incidents = data.get("incidents", [])

        logging.info(f"Retrieved {len(incidents)} incidents.")
        return incidents

    # ------------------------------------------------------
    #         MAIN LOOP
    # ------------------------------------------------------
    def run(self):
        logging.info("🚀 Starting TomTom → Kafka producer...")

        while True:
            cities = {
                "Paris": (2.224, 48.815, 2.470, 48.902),
                "Lyon": (4.78, 45.70, 4.90, 45.80),
                "Marseille": (5.32, 43.24, 5.47, 43.35),
            }
            for city, citybbox in cities.items():
                try:
                    incidents = self.fetch_from_tomtom(citybbox[0],citybbox[1],citybbox[2],citybbox[3])

                    # Write the raw incidents list to a JSON file for debugging/archival.
                    try:
                        with open("incidents.json", "a", encoding="utf-8") as _f:
                            json.dump(incidents, _f, ensure_ascii=False, indent=2)
                        logging.info("Wrote incidents to incidents.json")
                    except Exception as _e:
                        logging.error(f"Failed to write incidents.json: {_e}")

                    for inc in incidents:
                        self.send_incident(inc)

                    self.producer.flush()

                    # Print real-time stats
                    sent = self.total_sent
                    errors = self.total_errors
                    rate = sent / max(1, (time.time() - self.start_time))
                    logging.info(
                        f"📊 Stats — total sent={sent}, total errors={errors}, rate={rate:.2f} msg/s"
                    )

                except Exception as e:
                    logging.error(f"Loop error: {e}")

            time.sleep(POLL_INTERVAL)


# ------------------------------------------------------
#                   RUN
# ------------------------------------------------------
if __name__ == "__main__":
    prod = TomTomKafkaProducer()
    prod.run()
