from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from aiokafka import AIOKafkaConsumer
import socket
from typing import Optional

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "runner-events"

app = FastAPI()

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

connections = []  # Active WebSocket clients
consumer_task: Optional[asyncio.Task] = None  # Background Kafka consumer task

# --- Utility to wait for Kafka ---
async def wait_for_kafka(host="kafka", port=9092, timeout=60):
    for _ in range(timeout):
        try:
            with socket.create_connection((host, port), timeout=1):
                print("Kafka is ready!")
                return
        except OSError:
            await asyncio.sleep(1)
    raise TimeoutError("Kafka not ready after 60 seconds")

# --- Kafka consumer task ---
async def consume_kafka():
    await wait_for_kafka()  # Wait until Kafka is ready

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="fastapi-broadcast-group",
        auto_offset_reset="earliest"
    )
    await consumer.start()
    print("Kafka consumer started.")
    try:
        async for msg in consumer:
            try:
                event = json.loads(msg.value.decode("utf-8"))
                print("Received event:", event)
            except Exception as e:
                print("Failed to decode Kafka message:", e)
                continue

            disconnected = []
            for ws in connections:
                try:
                    await ws.send_json(event)
                except Exception as e:
                    print("WebSocket send error:", e)
                    disconnected.append(ws)

            for ws in disconnected:
                connections.remove(ws)
                print("Removed disconnected client")
    finally:
        await consumer.stop()
        print("Kafka consumer stopped")

# --- FastAPI startup/shutdown events ---
@app.on_event("startup")
async def startup_event():
    global consumer_task
    consumer_task = asyncio.create_task(consume_kafka())

@app.on_event("shutdown")
async def shutdown_event():
    global consumer_task
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            print("Kafka consumer task cancelled")

# --- WebSocket endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.append(websocket)
    print(f"WebSocket client connected, total: {len(connections)}")
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        connections.remove(websocket)
        print("WebSocket client disconnected")
