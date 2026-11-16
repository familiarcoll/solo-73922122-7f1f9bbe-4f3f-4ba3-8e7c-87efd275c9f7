"""共享测试夹具：进程内 SCPI 仪器 + 临时 SQLite + FastAPI TestClient。

注意：starlette TestClient 在独立线程的事件循环（portal）中运行应用，
所有异步辅助调用都必须经 anyio.start_blocking_portal 提交到同一线程，
禁止在测试线程自建事件循环直连，以免跨 loop 混用 asyncio 传输对象。
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest
from anyio.from_thread import start_blocking_portal

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@pytest.fixture(scope="session")
def unused_tcp_port():
    return _free_port()


@pytest.fixture
def instrument_portal(unused_tcp_port):
    """仪器服务器所在的事件循环与 portal（测试内异步 SCPI 调用复用它）。"""
    with start_blocking_portal() as portal:
        from instrument.scpi_device import create_server

        server, state = portal.call(
            create_server, "127.0.0.1", unused_tcp_port)
        try:
            yield portal, unused_tcp_port, state
        finally:
            portal.call(server.close)
            portal.call(server.wait_closed)


@pytest.fixture
def api_components(tmp_path, instrument_portal):
    """暴露底层组件，供需要模拟重启的测试使用（不启动 HTTP 应用）。"""
    portal, port, _ = instrument_portal
    db_path = tmp_path / "test-collector.db"

    from app.config import Settings
    from app.coordinator import Coordinator
    from app.database import Database
    from app.scpi_client import ScpiClient, ScpiConfig

    settings = Settings(
        instrument_host="127.0.0.1",
        instrument_port=port,
        instrument_timeout=5.0,
        db_path=str(db_path),
        scan_timeout=3.0,
        resync_interval=0.5,
        opc_wait_timeout=5.0,
    )
    db = Database(settings.db_path)
    scpi = ScpiClient(ScpiConfig(
        host=settings.instrument_host,
        port=settings.instrument_port,
        timeout=settings.instrument_timeout,
    ))
    coordinator = Coordinator(
        db, scpi,
        scan_timeout=settings.scan_timeout,
        resync_interval=settings.resync_interval,
        opc_wait_timeout=settings.opc_wait_timeout,
    )
    return portal, settings, db, scpi, coordinator


@pytest.fixture
def api_client(api_components):
    from fastapi.testclient import TestClient

    from app.main import create_app

    portal, settings, db, scpi, coordinator = api_components
    app = create_app(settings, db, coordinator)

    with TestClient(app) as client:
        yield client, app, settings.instrument_port, scpi, portal
