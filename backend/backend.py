from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from aiokafka import AIOKafkaConsumer
import socket
from typing import Optional, List

KAFKA_BOOTSTRAP_SERVERS = "kafka-service:9092"
DEFAULT_TOPIC = "runner-events"  # fallback if no race is specified

app = FastAPI()

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Utility to wait for Kafka readiness ---
async def wait_for_kafka(host="kafka-service", port=9092, timeout=60):
    for _ in range(timeout):
        try:
            with socket.create_connection((host, port), timeout=1):
                print("Kafka is ready!")
                return
        except OSError:
            await asyncio.sleep(1)
    raise TimeoutError("Kafka not ready after 60 seconds")

# --- Create a consumer for given topics ---
async def consume_topics(topics: List[str], websocket: WebSocket):
    await wait_for_kafka()
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="backend-consumer-group",
        auto_offset_reset="latest"  # Only new events, real-time style
    )
    await consumer.start()
    print(f"Started consumer for {topics}")

    try:
        async for msg in consumer:
            try:
                event = json.loads(msg.value.decode("utf-8"))
                print(f"Consumed from Kafka topic {msg.topic}: {event}")
            except Exception as e:
                print("Failed to decode Kafka message:", e)
                continue

            # Forward to WebSocket
            try:
                await websocket.send_json(event)
            except Exception as e:
                print(f"WebSocket send error: {e}")
                break
    finally:
        await consumer.stop()
        print(f"Stopped consumer for {topics}")

# --- WebSocket endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, race: Optional[str] = Query(None)):
    await websocket.accept()
    race_topic = race or DEFAULT_TOPIC
    topics = [race_topic]
    print(f"WebSocket connected. Subscribing to: {topics}")

    # Launch dedicated consumer task for this socket
    consumer_task = asyncio.create_task(consume_topics(topics, websocket))

    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        print(f"Client disconnected from {race_topic}")
    finally:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass



# import time
# import json
# from kafka import KafkaConsumer
# from kafka.errors import NoBrokersAvailable
# import os
# import sys

# # --- Configuration ---
# KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-service:9092")
# KAFKA_TOPIC_PATTERN = os.environ.get("KAFKA_TOPIC_PATTERN", "race-")  # prefix match
# RETRIES = int(os.environ.get("KAFKA_RETRIES", "10"))
# RETRY_DELAY = int(os.environ.get("KAFKA_RETRY_DELAY", "3"))  # seconds

# def wait_for_kafka(bootstrap_servers, retries=10, delay=3):
#     """Try to connect to Kafka with retry logic."""
#     for i in range(retries):
#         try:
#             consumer = KafkaConsumer(
#                 bootstrap_servers=bootstrap_servers,
#                 auto_offset_reset="earliest",
#                 enable_auto_commit=True,
#                 value_deserializer=lambda v: json.loads(v.decode("utf-8")),
#                 group_id="console-logger"
#             )
#             print("✅ Connected to Kafka!")
#             return consumer
#         except NoBrokersAvailable:
#             print(f"⚠️ Kafka not ready, retrying in {delay}s... ({i+1}/{retries})")
#             time.sleep(delay)
#     print("❌ Unable to connect to Kafka after retries.")
#     sys.exit(1)

# def main():
#     consumer = wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS, RETRIES, RETRY_DELAY)

#     # Subscribe to all topics starting with the prefix
#     print(f"📡 Subscribing to topics starting with '{KAFKA_TOPIC_PATTERN}'")
#     consumer.subscribe(pattern=f"^{KAFKA_TOPIC_PATTERN}.*$")

#     try:
#         for message in consumer:
#             print("📥 Received event:")
#             print(json.dumps(message.value, indent=2))
#             print("-" * 60)
#     except KeyboardInterrupt:
#         print("🛑 Stopping consumer...")
#     finally:
#         consumer.close()

# if __name__ == "__main__":
#     main()



# import json
# import time
# import socket
# from threading import Thread
# from typing import List, Dict
# from kafka import KafkaConsumer
# from kafka.errors import NoBrokersAvailable

# from fastapi import FastAPI, WebSocket, WebSocketDisconnect
# from fastapi.middleware.cors import CORSMiddleware

# # ----------------------------
# # Configuration
# # ----------------------------
# KAFKA_BOOTSTRAP_SERVERS = "kafka-service:9092"
# DEFAULT_TOPIC = "race-default"
# KAFKA_GROUP_ID = "websocket-consumer"
# KAFKA_TOPIC_PREFIX = "race-"  # subscribe to all topics starting with this prefix
# RETRIES = 10
# RETRY_DELAY = 3  # seconds

# # ----------------------------
# # FastAPI app
# # ----------------------------
# app = FastAPI(title="Kafka WebSocket API (sync)")

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # ----------------------------
# # Store connected WebSockets
# # ----------------------------
# connections: List[WebSocket] = []

# # ----------------------------
# # Utility: Wait for Kafka
# # ----------------------------
# def wait_for_kafka(host="kafka-service", port=9092, retries=RETRIES, delay=RETRY_DELAY):
#     """Wait until Kafka is reachable."""
#     for i in range(retries):
#         try:
#             with socket.create_connection((host, port), timeout=2):
#                 print(f"✅ Kafka is ready at {host}:{port}")
#                 return
#         except OSError:
#             print(f"⏳ Waiting for Kafka ({i+1}/{retries})...")
#             time.sleep(delay)
#     raise TimeoutError(f"❌ Kafka not ready after {retries*delay} seconds")


# # ----------------------------
# # Kafka consumer thread
# # ----------------------------
# def kafka_consumer_thread(topics: List[str]):
#     """Consume Kafka messages and broadcast to all WebSocket clients."""
#     wait_for_kafka()
#     consumer = KafkaConsumer(
#         *topics,
#         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
#         group_id=KAFKA_GROUP_ID,
#         auto_offset_reset="earliest",
#         value_deserializer=lambda v: json.loads(v.decode("utf-8")),
#     )
#     print(f"📡 Kafka consumer started for topics: {topics}")

#     try:
#         for message in consumer:
#             event = message.value
#             print(f"📥 Received Kafka message: {event}")
#             disconnected = []
#             for ws in connections:
#                 try:
#                     ws.send_json(event)  # synchronous send
#                 except Exception as e:
#                     print(f"⚠️ WebSocket disconnected: {e}")
#                     disconnected.append(ws)
#             for ws in disconnected:
#                 connections.remove(ws)
#     except KeyboardInterrupt:
#         print("🛑 Kafka consumer stopping...")
#     finally:
#         consumer.close()


# # ----------------------------
# # Start Kafka consumer in background
# # ----------------------------
# def start_consumer_for_default_topic():
#     Thread(target=kafka_consumer_thread, args=([DEFAULT_TOPIC],), daemon=True).start()


# # ----------------------------
# # WebSocket endpoint
# # ----------------------------
# @app.websocket("/ws")
# def websocket_endpoint(websocket: WebSocket):
#     websocket.accept()
#     connections.append(websocket)
#     print(f"🌐 WebSocket connected, total connections: {len(connections)}")
#     try:
#         while True:
#             time.sleep(1)  # keep connection alive
#     except WebSocketDisconnect:
#         connections.remove(websocket)
#         print("❌ WebSocket client disconnected")
