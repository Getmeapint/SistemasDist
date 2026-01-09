import gpxpy
import time
import random
import glob
import os
import pika
from pika.exceptions import AMQPConnectionError
import json
from threading import Thread

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq-cluster.rabbitmq-system.svc.cluster.local")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "grupo5")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "z1x2c3v4b5n6")
RABBITMQ_VHOST = os.environ.get("RABBITMQ_VHOST", "/grupo5")


BASE_TOPIC_PREFIX = "race-"   

GPX_FILES_FOLDER = os.environ.get("GPX_FILES_FOLDER", "./gpx/")
print(f"Using GPX files folder: {GPX_FILES_FOLDER}")

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
    {"name": "Ian Nelson", "gender": "male"},
    {"name": "Jackie O'Connor", "gender": "female"},
    {"name": "Kevin Parker", "gender": "male"},
    {"name": "Laura Quinn", "gender": "female"}
]
SPEED_VARIATION = (6, 12) 

def wait_for_rabbitmq(retries=10, delay=2):
    print(f"Connecting to RabbitMQ at {RABBITMQ_HOST}:{RABBITMQ_PORT} as {RABBITMQ_USER}...")
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    for i in range(retries):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    virtual_host=RABBITMQ_VHOST,
                    credentials=credentials,
                    connection_attempts=1,
                    retry_delay=1
                )
            )
            channel = connection.channel()
            print("Connected to RabbitMQ!")
            connection.close()
            return True
        except AMQPConnectionError:
            print(f"RabbitMQ not ready, retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)
    raise Exception("Unable to connect to RabbitMQ after retries")

def get_channel():
    """Create a new RabbitMQ channel."""
    print(f"Opening RabbitMQ channel to {RABBITMQ_HOST}:{RABBITMQ_PORT} as {RABBITMQ_USER}")
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials
        )
    )
    channel = connection.channel()
    return connection, channel

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

    connection, channel = get_channel()
    
    # Declare the exchange2
    channel.exchange_declare(exchange=race_topic, exchange_type='fanout', durable=True)

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
                channel.basic_publish(
                    exchange=race_topic,
                    routing_key='',
                    body=json.dumps(event)
                )
                print(f"[{race_topic}] Sent {athlete_name} @ ({lat:.5f}, {lon:.5f})")
            except Exception as e:
                print(f"Error sending event to RabbitMQ: {e}")

            time.sleep(1)
    
    connection.close()

def simulate_multiple_athletes():
    wait_for_rabbitmq()
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

if __name__ == "__main__":
    simulate_multiple_athletes()