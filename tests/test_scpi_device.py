"""SCPI 演示仪器的协议级测试（进程内 TCP 服务器）。"""

from __future__ import annotations

import asyncio

import pytest

from instrument.scpi_device import InstrumentState, create_server


@pytest.fixture
async def endpoint(unused_tcp_port):
    server, state = await create_server("127.0.0.1", unused_tcp_port)
    try:
        yield unused_tcp_port, state
    finally:
        server.close()
        await server.wait_closed()


async def _send(port: int, cmd: str) -> str:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write((cmd + "\n").encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout=5)
        return line.decode().strip()
    finally:
        writer.close()
        await writer.wait_closed()


async def _cmd(port: int, cmd: str) -> None:
    """发送无响应命令（ROUT:SCAN / *RST / *CLS / SYST:DELAY）。"""
    _, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write((cmd + "\n").encode())
        await writer.drain()
        await asyncio.sleep(0.05)
    finally:
        writer.close()
        await writer.wait_closed()


async def _send_multi(port: int, cmd: str, count: int) -> list[str]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write((cmd + "\n").encode())
        await writer.drain()
        lines = []
        for _ in range(count):
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            lines.append(line.decode().strip())
        return lines
    finally:
        writer.close()
        await writer.wait_closed()


async def test_idn_and_scan_table_validation(endpoint):
    port, _ = endpoint
    idn = await _send(port, "*IDN?")
    assert idn.startswith("LAB,DEMO-SCANNER")

    err = await _send(port, "ROUT:SCAN 999")
    assert err.startswith("-222")
    err = await _send(port, "ROUT:SCAN 101,101")
    assert err.startswith("-221")

    await _cmd(port, "ROUT:SCAN 101,102,103,104")
    assert await _send(port, "ROUT:SCAN?") == "101,102,103,104"


async def test_read_returns_bare_values_in_scan_order(endpoint):
    port, _ = endpoint
    await _cmd(port, "ROUT:SCAN 104,102")
    values = await _send_multi(port, "READ?", 2)
    # 裸读数、无批次号，且顺序跟随扫描表
    assert all("B" not in v for v in values)
    floats = [float(v) for v in values]
    assert len(floats) == 2
    assert floats[0] > floats[1]  # 通道 104 基准电压高于 102


async def test_scan_table_is_shared_across_connections(endpoint):
    port, _ = endpoint
    await _cmd(port, "ROUT:SCAN 103")
    # 另一条连接读到同一份扫描表
    assert await _send(port, "ROUT:SCAN?") == "103"


async def test_concurrent_read_rejected_while_scanning(endpoint):
    port, state = endpoint
    await _cmd(port, "SYST:DELAY 0.3")
    await _cmd(port, "ROUT:SCAN 101,102,103,104")
    reader1, writer1 = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer1.write(b"READ?\n")
        await writer1.drain()
        await asyncio.sleep(0.2)  # 等扫描启动
        err = await _send(port, "READ?")
        assert err.startswith("-410")
        lines = [await asyncio.wait_for(reader1.readline(), timeout=5)
                 for _ in range(4)]
        assert len(lines) == 4
    finally:
        writer1.close()
        await writer1.wait_closed()
    assert not state.scanning


async def test_abort_stops_scan_and_produces_error(endpoint):
    port, state = endpoint
    await _cmd(port, "SYST:DELAY 0.3")
    await _cmd(port, "ROUT:SCAN 101,102,103,104")
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"READ?\n")
        await writer.drain()
        await asyncio.sleep(0.2)
        assert await _send(port, "ABORt") == "1"
        line = await asyncio.wait_for(reader.readline(), timeout=5)
        assert line.decode().strip().startswith("-410")
    finally:
        writer.close()
        await writer.wait_closed()
    assert not state.scanning
    # 中止后仪器恢复可用
    await _cmd(port, "SYST:DELAY 0.02")
    values = await _send_multi(port, "READ?", 4)
    assert len(values) == 4


async def test_client_disconnect_does_not_stop_scan(endpoint):
    port, state = endpoint
    await _cmd(port, "SYST:DELAY 0.25")
    await _cmd(port, "ROUT:SCAN 101,102")
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"READ?\n")
    await writer.drain()
    await asyncio.sleep(0.1)
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass
    # 扫描仍在运行：并发 READ? 被拒
    err = await _send(port, "READ?")
    assert err.startswith("-410")
    # 等扫描自然结束，响应进入迟到缓冲，*OPC? 可用且 *CLS 能清
    await _send(port, "*OPC?")
    assert len(state.late_buffer) >= 1
    await _cmd(port, "*CLS")
    assert state.late_buffer == []


async def test_rst_clears_table_and_late_responses(endpoint):
    port, state = endpoint
    await _cmd(port, "ROUT:SCAN 101")
    state.late_buffer.append("+1.000000")
    await _cmd(port, "*RST")
    assert state.scan_table == ()
    assert state.late_buffer == []
    err = await _send(port, "READ?")
    assert err.startswith("-221")


async def test_syst_disc_closes_but_listener_survives(endpoint):
    port, _ = endpoint
    assert await _send(port, "*IDN?") == await _send(port, "*IDN?")
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"SYST:DISC\n")
    await writer.drain()
    try:
        line = await asyncio.wait_for(reader.readline(), timeout=3)
        assert line.decode().strip() == "1"
        eof = await asyncio.wait_for(reader.readline(), timeout=3)
        assert eof == b""
    finally:
        writer.close()
    # 监听仍在，新连接可用
    assert (await _send(port, "*IDN?")).startswith("LAB")
