import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from backend.backend import app, connections

# --- WebSocket Test ---
# def test_websocket_connection():
#     client = TestClient(app)
    
#     with client.websocket_connect("/ws") as websocket:
#         # After connection, connections list should have 1 item
#         assert len(connections) == 1
        
#         # Send a message and get echo
#         websocket.send_text("hello")
#         assert websocket.receive_text() == "Echo: hello"
    
#     # After context exit, connection should be removed
#     assert len(connections) == 0


