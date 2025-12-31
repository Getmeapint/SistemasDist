from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter
import asyncio
import json
import aio_pika
import socket
import os
import aiohttp
from typing import Optional, List, Dict, Set

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "guest")
RABBITMQ_URL = f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
DEFAULT_TOPIC = "runner-events"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Expose /metrics for Prometheus
Instrumentator().instrument(app).expose(app, endpoint="/metrics")


@app.get("/")
async def root():
    return {"status": "Backend is running", "websocket": "/ws"}


async def wait_for_rabbitmq(host=None, port=None, timeout=60):
    h = host or RABBITMQ_HOST
    p = port or RABBITMQ_PORT
    for _ in range(timeout):
        try:
            with socket.create_connection((h, p), timeout=1):
                print("RabbitMQ is ready!")
                return
        except OSError:
            await asyncio.sleep(1)
    raise TimeoutError("RabbitMQ not ready after 60 seconds")


# Global connection/channel/queue
_rabbit_connection: aio_pika.RobustConnection = None
_rabbit_channel: aio_pika.RobustChannel = None
_backend_queue: aio_pika.Queue = None

# subscribers: topic -> set of websocket objects
SUBSCRIBERS: Dict[str, Set[WebSocket]] = {}

# Custom metrics
messages_consumed = Counter(
    "backend_messages_consumed_total",
    "Messages consumed from RabbitMQ",
    ["race"],
)
ws_messages_sent = Counter(
    "backend_ws_messages_sent_total",
    "Messages sent over WebSocket",
    ["race"],
)
ws_connections = Counter(
    "backend_ws_connections_total",
    "WebSocket connections opened",
    ["race"],
)
ws_disconnections = Counter(
    "backend_ws_disconnections_total",
    "WebSocket disconnections",
    ["race"],
)


async def discover_and_bind_exchanges():
    """Discover exchanges via RabbitMQ management API and bind backend queue to them."""
    mgmt_url = f"http://{RABBITMQ_HOST}:15672/api/exchanges/"
    auth = aiohttp.BasicAuth(RABBITMQ_USER, RABBITMQ_PASSWORD)

    try:
        async with aiohttp.ClientSession(auth=auth) as session:
            async with session.get(mgmt_url) as resp:
                if resp.status != 200:
                    print(f"Failed to query exchanges: {resp.status}")
                    return
                data = await resp.json()

        for ex in data:
            name = ex.get("name", "")
            # bind user-created fanout/topic/direct exchanges (ignore amq.* and empty name)
            if not name or name.startswith("amq."):
                continue
            try:
                exchange = await _rabbit_channel.declare_exchange(name, aio_pika.ExchangeType.FANOUT, durable=True)
                await _backend_queue.bind(exchange)
                print(f"Bound backend queue to exchange {name}")
            except Exception as e:
                print(f"Failed to bind to exchange {name}: {e}")
    except Exception as e:
        print(f"Error discovering exchanges: {e}")


async def rabbit_startup():
    global _rabbit_connection, _rabbit_channel, _backend_queue
    await wait_for_rabbitmq()
    _rabbit_connection = await aio_pika.connect_robust(RABBITMQ_URL)
    _rabbit_channel = await _rabbit_connection.channel()

    # declare a durable backend queue that will be bound to exchanges
    _backend_queue = await _rabbit_channel.declare_queue("backend-queue", durable=True)

    # initial discovery and bind
    await discover_and_bind_exchanges()

    # periodically rescan exchanges to bind new ones
    async def periodic_rescan():
        while True:
            try:
                await discover_and_bind_exchanges()
            except Exception:
                pass
            await asyncio.sleep(10)

    asyncio.create_task(periodic_rescan())

    # start consuming from the backend queue
    await _backend_queue.consume(on_message)


async def rabbit_shutdown():
    global _rabbit_connection
    try:
        if _rabbit_connection:
            await _rabbit_connection.close()
    except Exception:
        pass


async def on_message(message: aio_pika.IncomingMessage):
    async with message.process():
        try:
            event = json.loads(message.body.decode("utf-8"))
        except Exception as e:
            print("Failed to decode RabbitMQ message:", e)
            return

        # determine topic/race
        topic = event.get("race") or DEFAULT_TOPIC
        print(f"Backend consumed message for {topic}: {event}")
        try:
            messages_consumed.labels(topic).inc()
        except Exception:
            pass

        # broadcast to subscribers of that topic and to subscribers of 'all' (if any)
        recipients = set()
        if topic in SUBSCRIBERS:
            recipients.update(SUBSCRIBERS[topic])
        if "all" in SUBSCRIBERS:
            recipients.update(SUBSCRIBERS["all"])

        dead_ws = []
        sent_count = 0
        for ws in list(recipients):
            try:
                await ws.send_json(event)
                sent_count += 1
            except Exception as e:
                print(f"Failed to send to websocket: {e}, marking as dead")
                dead_ws.append(ws)

        if sent_count:
            try:
                ws_messages_sent.labels(topic).inc(sent_count)
            except Exception:
                pass

        # remove dead websockets from all subscriber lists
        for ws in dead_ws:
            for key in list(SUBSCRIBERS.keys()):
                if ws in SUBSCRIBERS[key]:
                    SUBSCRIBERS[key].discard(ws)
                    if not SUBSCRIBERS[key]:
                        del SUBSCRIBERS[key]


@app.on_event("startup")
async def on_startup():
    asyncio.create_task(rabbit_startup())


@app.on_event("shutdown")
async def on_shutdown():
    await rabbit_shutdown()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, race: Optional[str] = Query(None)):
    await websocket.accept()
    race_topic = race or DEFAULT_TOPIC
    print(f"WebSocket connected. Subscribing to: {race_topic}")
    try:
        ws_connections.labels(race_topic if race_topic != "all" else "all").inc()
    except Exception:
        pass

    # register websocket
    key = race_topic if race_topic != "all" else "all"
    SUBSCRIBERS.setdefault(key, set()).add(websocket)

    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        print(f"Client disconnected from {race_topic}")
    finally:
        # unregister websocket
        if key in SUBSCRIBERS and websocket in SUBSCRIBERS[key]:
            SUBSCRIBERS[key].remove(websocket)
            if not SUBSCRIBERS[key]:
                del SUBSCRIBERS[key]
        try:
            ws_disconnections.labels(key).inc()
        except Exception:
            pass
