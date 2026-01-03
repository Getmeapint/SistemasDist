import importlib
import sys
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from prometheus_client import REGISTRY


def _clear_registry():
    for collector in list(REGISTRY._collector_to_names.keys()):
        try:
            REGISTRY.unregister(collector)
        except KeyError:
            pass


def _reload_backend():
    _clear_registry()
    if "backend.backend" in sys.modules:
        del sys.modules["backend.backend"]
    import backend.backend as mod
    return importlib.reload(mod)


def test_root_endpoint():
    mod = _reload_backend()
    client = TestClient(mod.app)
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data.get("status") == "Backend is running"


def test_rabbitmq_url_builds_with_vhost(monkeypatch):
    monkeypatch.setenv("RABBITMQ_HOST", "rabbit-host")
    monkeypatch.setenv("RABBITMQ_PORT", "1234")
    monkeypatch.setenv("RABBITMQ_USER", "user1")
    monkeypatch.setenv("RABBITMQ_PASSWORD", "pass1")
    monkeypatch.setenv("RABBITMQ_VHOST", "myvhost")

    mod = _reload_backend()
    assert mod.RABBITMQ_URL == "amqp://user1:pass1@rabbit-host:1234/myvhost"


@patch("backend.backend.aio_pika.connect_robust", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_connect_with_backoff_success(mock_connect):
    fake_conn = AsyncMock()
    fake_chan = AsyncMock()
    mock_connect.return_value = fake_conn
    fake_conn.channel.return_value = fake_chan

    mod = _reload_backend()
    conn, chan = await mod.connect_rabbit_with_backoff(max_attempts=2, base_delay=0, cool_off=0)
    assert conn is fake_conn
    assert chan is fake_chan


@patch("backend.backend.aio_pika.connect_robust", new_callable=AsyncMock)
@pytest.mark.asyncio
async def test_connect_with_backoff_eventual_cooloff(mock_connect):
    mock_connect.side_effect = [Exception("boom"), Exception("boom2"), AsyncMock()]
    mod = _reload_backend()
    # base_delay=0 and cool_off=0 to make test fast
    conn, chan = await mod.connect_rabbit_with_backoff(max_attempts=2, base_delay=0, cool_off=0)
    assert conn is not None
    assert chan is not None


@patch("backend.backend._backend_queue")
@patch("backend.backend._rabbit_channel")
@patch("backend.backend.aiohttp.ClientSession")
@pytest.mark.asyncio
async def test_discover_and_bind_exchanges_binds_non_amq(mock_session, mock_channel, mock_queue):
    # Mock management API response
    fake_resp = AsyncMock()
    fake_resp.status = 200
    fake_resp.json.return_value = [
        {"name": "amq.direct"},
        {"name": ""},
        {"name": "custom-ex"},
    ]
    mock_get_ctx = AsyncMock()
    mock_get_ctx.__aenter__.return_value = fake_resp
    mock_session.return_value.__aenter__.return_value.get.return_value = mock_get_ctx

    # Mock declare and bind
    mock_exchange = AsyncMock()
    mock_channel.declare_exchange.return_value = mock_exchange

    mod = _reload_backend()
    await mod.discover_and_bind_exchanges()

    mock_channel.declare_exchange.assert_called_once()
    mock_queue.bind.assert_called_once_with(mock_exchange)


@patch("backend.backend.messages_consumed")
@patch("backend.backend.json.loads")
@pytest.mark.asyncio
async def test_on_message_parses_and_counts(mock_json_loads, mock_counter):
    mod = _reload_backend()
    mock_json_loads.return_value = {"race": "r1"}

    class FakeMessage:
        def __init__(self):
            self.body = b'{"race":"r1"}'

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def process(self):
            return self

    msg = FakeMessage()
    await mod.on_message(msg)
    mock_counter.labels.assert_called_with("r1")
