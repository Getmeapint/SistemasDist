import gpxpy
import time
import random
import glob
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from threading import Thread

KAFKA_BOOTSTRAP_SERVERS = "kafka-service:9092"

BASE_TOPIC_PREFIX = "race-"   

GPX_FILES_FOLDER = os.environ.get("GPX_FILES_FOLDER", "./gpx/")
print(f"Using GPX files folder: {GPX_FILES_FOLDER}")

ATHLETES = [
    {"name": "John Doe", "gender": "male"},
    {"name": "Jane Smith", "gender": "female"},
    {"name": "Alice Johnson", "gender": "female"},
    {"name": "Bob Brown", "gender": "male"},
    {"name": "Charlie Davis", "gender": "male"},
    {"name": "David Evans", "gender": "male"}
]
SPEED_VARIATION = (6, 12) 

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

_producer = None
def get_producer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
    """Return a singleton KafkaProducer, creating it on first use."""
    global _producer
    if _producer is None:
        _producer = wait_for_kafka(bootstrap_servers)
    return _producer

def flush_producer():
    """Flush the global producer if it exists. Safe to call at shutdown."""
    global _producer
    if _producer is not None:
        try:
            _producer.flush()
        except Exception as e:
            print("Error flushing producer:", e)

def load_trails(folder=GPX_FILES_FOLDER):
    races = {}  
    for file in glob.glob(f"{folder}/*.gpx"):
        with open(file, 'r') as f:
            gpx = gpxpy.parse(f)
            points = []
            for track in gpx.tracks:
                for segment in track.segments:
                    points.extend(segment.points)
            race_name = os.path.splitext(os.path.basename(file))[0]
            topic_name = BASE_TOPIC_PREFIX + race_name
            races[topic_name] = points
    print(f"Loaded {len(races)} races: {list(races.keys())}")
    return races

def simulate_athlete(athlete, race_topic, points, speed_kmh):
    athlete_name = athlete["name"]
    athlete_gender = athlete["gender"]
    speed_mps = speed_kmh / 3.6

    producer = get_producer()

    for i in range(len(points) - 1):
        start = points[i]
        end = points[i + 1]
        distance = start.distance_3d(end)
        duration = distance / speed_mps

        steps = max(1, int(duration))

        for t in range(steps):
            fraction = t / steps
            lat = start.latitude + fraction * (end.latitude - start.latitude)
            lon = start.longitude + fraction * (end.longitude - start.longitude)
            ele = (start.elevation or 0) + fraction * ((end.elevation or 0) - (start.elevation or 0))

            event = {
                "athlete": athlete_name,
                "gender": athlete_gender,
                "race": race_topic,
                "location": {"latitude": lat, "longitude": lon},
                "elevation": ele,
                "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "event": "running"
            }

            try:
                producer.send(race_topic, event)
                print(f"[{race_topic}] Sent {athlete_name} @ ({lat:.5f}, {lon:.5f})")
            except Exception as e:
                print(f"Error sending event to Kafka: {e}")

            time.sleep(1)

def simulate_multiple_athletes():
    races = load_trails(GPX_FILES_FOLDER)
    if not races:
        print("No GPX races found!")
        return

    fixed_race_env = os.environ.get("FIXED_RACE")
    fixed_participants_env = os.environ.get("FIXED_PARTICIPANTS")

    threads = []

    if fixed_race_env:
        if fixed_race_env.startswith(BASE_TOPIC_PREFIX):
            race_topic = fixed_race_env
        else:
            race_topic = BASE_TOPIC_PREFIX + fixed_race_env

        if race_topic not in races:
            print(f"Fixed race '{race_topic}' not found among loaded races: {list(races.keys())}")
            return

        if fixed_participants_env:
            try:
                num = int(fixed_participants_env)
            except ValueError:
                print(f"Invalid FIXED_PARTICIPANTS value: {fixed_participants_env}")
                return
            if num <= 0:
                print("FIXED_PARTICIPANTS must be > 0")
                return
        else:
            num = 3

        print(f"Starting fixed-assignment: race={race_topic}, participants={num}")

        participants = []
        for i in range(num):
            base = ATHLETES[i % len(ATHLETES)].copy()
            if i >= len(ATHLETES):
                base["name"] = f"{base['name']} #{i//len(ATHLETES)+1}"
            participants.append(base)

        points = races[race_topic]
        for athlete in participants:
            speed_kmh = random.uniform(*SPEED_VARIATION)
            thread = Thread(target=simulate_athlete, args=(athlete, race_topic, points, speed_kmh))
            threads.append(thread)
            thread.start()
            time.sleep(0.5)

    else:
        for athlete in ATHLETES:
            race_topic = random.choice(list(races.keys()))
            points = races[race_topic]
            speed_kmh = random.uniform(*SPEED_VARIATION)

            thread = Thread(target=simulate_athlete, args=(athlete, race_topic, points, speed_kmh))
            threads.append(thread)
            thread.start()
            time.sleep(0.5)

    for thread in threads:
        thread.join()

    try:
        flush_producer()
    except Exception:
        pass

if __name__ == "__main__":
    simulate_multiple_athletes()
