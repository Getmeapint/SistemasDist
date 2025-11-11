from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from aiokafka import AIOKafkaConsumer
import socket
from typing import Optional, List

KAFKA_BOOTSTRAP_SERVERS = "kafka-service:9092"
DEFAULT_TOPIC = "runner-events"  

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def wait_for_kafka(host="kafka-service", port=9092, timeout=60):
    for _ in range(timeout):
        try:
            with socket.create_connection((host, port), timeout=1):
                print("Kafka is ready!")
                return
        except OSError:
            await asyncio.sleep(1)
    raise TimeoutError("Kafka not ready after 60 seconds")

async def consume_topics(topics: List[str], websocket: WebSocket):
    await wait_for_kafka()
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="backend-consumer-group",
        auto_offset_reset="latest"  
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

            try:
                await websocket.send_json(event)
            except Exception as e:
                print(f"WebSocket send error: {e}")
                break
    finally:
        await consumer.stop()
        print(f"Stopped consumer for {topics}")

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, race: Optional[str] = Query(None)):
    await websocket.accept()
    race_topic = race or DEFAULT_TOPIC
    topics = [race_topic]
    print(f"WebSocket connected. Subscribing to: {topics}")

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

