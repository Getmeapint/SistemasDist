from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from aiokafka import AIOKafkaConsumer
import socket
from contextlib import asynccontextmanager


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

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.append(websocket)
    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        connections.remove(websocket)
        print("WebSocket client disconnected")

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
    try:
        async for msg in consumer:
            event = json.loads(msg.value.decode("utf-8"))
            disconnected = []
            for ws in connections:
                try:
                    await ws.send_json(event)
                except:
                    disconnected.append(ws)
            for ws in disconnected:
                connections.remove(ws)
    finally:
        await consumer.stop()

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_task = asyncio.create_task(consume_kafka())
    yield
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass
