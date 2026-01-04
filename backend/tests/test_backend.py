import sys
import pytest
import prometheus_client
import prometheus_client.metrics as prom_metrics
from prometheus_client import CollectorRegistry
import prometheus_client.registry as prom_registry
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch


def _clear_registry():
    # Replace the global registry with a fresh one to avoid duplicate metric errors on reload.
    new_reg = CollectorRegistry()
    prom_registry.REGISTRY = new_reg
    prometheus_client.REGISTRY = new_reg
    prometheus_client.metrics.REGISTRY = new_reg

    class DummyCounter:
        def __init__(self, *args, **kwargs):
            pass

        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

    prometheus_client.Counter = DummyCounter
    prom_metrics.Counter = DummyCounter


def _reload_backend():
    _clear_registry()
    if "backend.backend" in sys.modules:
        del sys.modules["backend.backend"]
    import backend.backend as mod
    return mod


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


@pytest.mark.asyncio
async def test_discover_and_bind_exchanges_binds_non_amq():
    mod = _reload_backend()

    class DummyResp:
        status = 200

        async def json(self):
            return [
                {"name": "amq.direct"},
                {"name": ""},
                {"name": "custom-ex"},
            ]

    class DummyGetCtx:
        async def __aenter__(self):
            return DummyResp()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url):
            return DummyGetCtx()

    mock_exchange = AsyncMock()
    mod.aiohttp.ClientSession = DummySession
    mod._rabbit_channel = AsyncMock()
    mod._rabbit_channel.declare_exchange.return_value = mock_exchange
    mod._backend_queue = AsyncMock()

    await mod.discover_and_bind_exchanges()

    mod._rabbit_channel.declare_exchange.assert_called_once()
    mod._backend_queue.bind.assert_called_once_with(mock_exchange)


@pytest.mark.asyncio
async def test_on_message_parses_and_counts():
    mod = _reload_backend()
    mod.messages_consumed = MagicMock()

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
    mod.messages_consumed.labels.assert_called_with("r1")
