import gpxpy
import time
import random
import glob
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from threading import Thread

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # or your Kafka broker address
BASE_TOPIC_PREFIX = "race-"   # Each race will have its own topic

GPX_FILES_FOLDER = "./gpx/"

ATHLETES = [
    {"name": "John Doe", "gender": "male"},
    {"name": "Jane Smith", "gender": "female"},
    {"name": "Alice Johnson", "gender": "female"},
    {"name": "Bob Brown", "gender": "male"},
    {"name": "Charlie Davis", "gender": "male"},
    {"name": "David Evans", "gender": "male"},
    {"name": "Eva Green", "gender": "female"},
    {"name": "Frank Harris", "gender": "male"},
    {"name": "Grace Lee", "gender": "female"},
    {"name": "Hannah Martin", "gender": "female"},
    {"name": "Isaac Newton", "gender": "male"},
    {"name": "Jackie O'Neill", "gender": "female"}
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

# --- Trails loading with GPX parsing ---
def load_trails(folder=GPX_FILES_FOLDER):
    races = {}  # map: topic_name → list of GPX points
    for file in glob.glob(f"{folder}/*.gpx"):
        with open(file, 'r') as f:
            gpx = gpxpy.parse(f)
            points = []
            for track in gpx.tracks:
                for segment in track.segments:
                    points.extend(segment.points)
            # Generate topic name from file name
            race_name = os.path.splitext(os.path.basename(file))[0]
            topic_name = BASE_TOPIC_PREFIX + race_name
            races[topic_name] = points
    print(f"Loaded {len(races)} races: {list(races.keys())}")
    return races

# --- Athlete simulation ---
def simulate_athlete(athlete, race_topic, points, speed_kmh):
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
                "race": race_topic,  # NEW: race identifier
                "location": {"latitude": lat, "longitude": lon},
                "elevation": ele,
                "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "event": "running"
            }

            try:
                producer.send(race_topic, event)
                producer.flush()
                print(f"[{race_topic}] Sent {athlete_name} @ ({lat:.5f}, {lon:.5f})")
            except Exception as e:
                print(f"Error sending event to Kafka: {e}")

            time.sleep(1)

# --- Multiple athletes simulation ---
def simulate_multiple_athletes():
    races = load_trails(GPX_FILES_FOLDER)
    if not races:
        print("No GPX races found!")
        return

    threads = []
    for athlete in ATHLETES:
        # Randomly assign athlete to a race
        race_topic = random.choice(list(races.keys()))
        points = races[race_topic]
        speed_kmh = random.uniform(*SPEED_VARIATION)

        thread = Thread(target=simulate_athlete, args=(athlete, race_topic, points, speed_kmh))
        threads.append(thread)
        thread.start()
        time.sleep(0.5)

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    simulate_multiple_athletes()
