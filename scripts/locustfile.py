"""
Locust WebSocket load test.

Run:
  pip install locust websocket-client
  WS_TARGET=ws://10.2.15.161:30015 locust -f scripts/locustfile.py

Then open http://localhost:8089, set users/spawn rate, and start.
"""
import os
import random
import time
from locust import User, task, between
from websocket import create_connection


WS_TARGET = os.getenv("WS_TARGET", "ws://127.0.0.1:30015")
RACES = [
    "race-trail_route_1",
    "race-trail_route_2",
    "race-trail_route_3",
    "race-trail_route_4",
]


class WebSocketUser(User):
    wait_time = between(1, 3)

    @task
    def connect_and_listen(self):
        race = random.choice(RACES)
        url = f"{WS_TARGET}/ws?race={race}"
        start = time.time()
        ws = None
        try:
            ws = create_connection(url, timeout=10)
            # Listen quietly for a few messages / seconds
            end_time = time.time() + 5
            while time.time() < end_time:
                try:
                    ws.recv()
                except Exception:
                    break
            self.environment.events.request.fire(
                request_type="WS",
                name=f"connect {race}",
                response_time=(time.time() - start) * 1000,
                response_length=0,
                exception=None,
            )
        except Exception as e:
            self.environment.events.request.fire(
                request_type="WS",
                name=f"connect {race}",
                response_time=(time.time() - start) * 1000,
                response_length=0,
                exception=e,
            )
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
