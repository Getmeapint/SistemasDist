import gpxpy
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from threading import Thread

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # or your Kafka broker address
KAFKA_TOPIC = "runner-events"

GPX_FILE_PATH = "trail_route.gpx"
ATHLETES = [
    {"name": "John Doe", "gender": "male"},
    {"name": "Jane Smith", "gender": "female"},
    {"name": "Alice Johnson", "gender": "female"},
    {"name": "Bob Brown", "gender": "male"}
]
SPEED_VARIATION = (6, 12)  # km/h

# --- Kafka connection with retries ---
def wait_for_kafka(bootstrap_servers, retries=10, delay=2):
    for i in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready, retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise Exception("Unable to connect to Kafka after retries")

# Initialize Kafka producer
producer = wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

# --- GPX parsing ---
def read_gpx(file_path):
    with open(file_path, 'r') as gpx_file:
        gpx = gpxpy.parse(gpx_file)
    return gpx

# --- Athlete simulation ---
def simulate_athlete(athlete, points, speed_kmh):
    athlete_name = athlete["name"]
    athlete_gender = athlete["gender"]
    speed_mps = speed_kmh / 3.6

    for i in range(len(points) - 1):
        start = points[i]
        end = points[i + 1]
        distance = start.distance_3d(end)
        duration = distance / speed_mps

        for t in range(int(duration)):
            fraction = t / duration
            lat = start.latitude + fraction * (end.latitude - start.latitude)
            lon = start.longitude + fraction * (end.longitude - start.longitude)
            ele = start.elevation + fraction * (end.elevation - start.elevation)

            event = {
                "athlete": athlete_name,
                "gender": athlete_gender,
                "location": {"latitude": lat, "longitude": lon},
                "elevation": ele,
                "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "event": "running"
            }

            # Send event to Kafka
            try:
                producer.send(KAFKA_TOPIC, event)
                producer.flush()
                print(f"Sent to Kafka: {event}")
            except Exception as e:
                print(f"Error sending event to Kafka: {e}")

            time.sleep(1)

# --- Multiple athletes simulation ---
def simulate_multiple_athletes():
    gpx = read_gpx(GPX_FILE_PATH)
    points = []
    for track in gpx.tracks:
        for segment in track.segments:
            points.extend(segment.points)

    threads = []
    for athlete in ATHLETES:
        speed_kmh = random.uniform(*SPEED_VARIATION)
        thread = Thread(target=simulate_athlete, args=(athlete, points, speed_kmh))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    simulate_multiple_athletes()
