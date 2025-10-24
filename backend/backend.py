from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import aio_pika
import json

RABBITMQ_URL = "amqp://guest:guest@rabbitmq/"  # Your RabbitMQ container URL
QUEUE_NAME = "events"


app = FastAPI()

# Allow CORS for the frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# List to store active WebSocket connections
connections = []

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connections.append(websocket)
    try:
        while True:
            # Keep the connection alive
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        # Remove the connection when the client disconnects
        connections.remove(websocket)
        print("WebSocket client disconnected")

# @app.post("/events")
# async def receive_event(event: dict):
#     # Send the event to all active WebSocket connections
#     disconnected_connections = []
#     print(f"Broadcasting event: {event}")
#     for connection in connections:
#         try:
#             await connection.send_json(event)
#         except RuntimeError as e:
#             # Handle cases where the connection is already closed
#             print(f"Error sending to WebSocket: {e}")
#             disconnected_connections.append(connection)

#     # Remove disconnected WebSocket connections
#     for connection in disconnected_connections:
#         connections.remove(connection)

#     return {"status": "event sent"}

async def consume_rabbitmq():
    while True:
        try:
            connection = await aio_pika.connect_robust(RABBITMQ_URL)
            channel = await connection.channel()
            queue = await channel.declare_queue(QUEUE_NAME, durable=True)
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        event = json.loads(message.body)
                        disconnected = []
                        for ws in connections:
                            try:
                                await ws.send_json(event)
                            except:
                                disconnected.append(ws)
                        for ws in disconnected:
                            connections.remove(ws)
        except Exception as e:
            print(f"RabbitMQ connection error: {e}, retrying in 5s...")
            await asyncio.sleep(5)

# Start RabbitMQ consumer on app startup
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume_rabbitmq())