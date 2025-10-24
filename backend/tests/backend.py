import pytest
import asyncio
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from app import app, connections

client = TestClient(app)

@pytest.mark.asyncio
async def test_websocket_connection():
    """
    Test that a WebSocket can connect and stay alive.
    """
    uri = "ws://testserver/ws"
    async with client.websocket_connect("/ws") as websocket:
        # The connection should be added to the connections list
        assert websocket in connections
        # Wait a bit to simulate activity
        await asyncio.sleep(0.1)
        # Disconnect
    # After disconnecting, websocket should be removed
    assert len(connections) == 0

@pytest.mark.asyncio
async def test_rabbitmq_consume():
    """
    Test that consume_rabbitmq processes a message and sends to WebSockets.
    RabbitMQ is mocked.
    """
    from app import consume_rabbitmq

    # Mock a WebSocket
    class MockWS:
        def __init__(self):
            self.sent = []
        async def send_json(self, data):
            self.sent.append(data)

    ws = MockWS()
    connections.append(ws)

    # Mock aio_pika connect_robust
    with patch("aio_pika.connect_robust") as mock_connect:
        mock_conn = AsyncMock()
        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.body = b'{"athlete": "John Doe", "event": "running"}'
        mock_queue.iterator.return_value.__aenter__.return_value.__aiter__.return_value = [mock_message]
        mock_channel.declare_queue.return_value = mock_queue
        mock_conn.channel.return_value = mock_channel
        mock_connect.return_value = mock_conn

        # Run consume_rabbitmq for one iteration
        task = asyncio.create_task(consume_rabbitmq())
        await asyncio.sleep(0.1)  # let the consumer run
        task.cancel()  # stop infinite loop

    # Check that WebSocket received the message
    assert ws.sent[0]["athlete"] == "John Doe"
    connections.remove(ws)
