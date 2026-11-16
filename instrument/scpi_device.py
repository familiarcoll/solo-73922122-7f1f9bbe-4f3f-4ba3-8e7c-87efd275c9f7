"""本地 TCP SCPI 演示仪器（共享扫描表 / 共享扫描状态机）。

线协议：命令以 LF（或 CRLF）结尾，响应逐行返回，以 LF 结尾。
以 "-" 开头的响应行为 SCPI 风格错误，例如：-410,"Query INTERRUPTED: ..."

支持的命令（大小写不敏感）：
  *IDN?                         识别串
  *RST                          停机并复位扫描表、清迟到响应
  *CLS                          清迟到响应缓冲（不影响进行中的扫描）
  *OPC?                         空闲立即返回 1；扫描中则等扫描结束后返回 1
  ROUT:SCAN <ch>[,<ch>...]      设置全机共享扫描表（通道 101~104 的子集）
  ROUT:SCAN?                    查询扫描表
  READ?                         空闲时启动扫描并按扫描顺序返回裸电压读数（每通道一行，
                                无批次号）；扫描进行中则返回 -410 错误
  ABORt                         立即请求停止当前扫描，返回 1
  SYST:DELAY <秒>               设置每通道驻留（慢响应演示），默认 0.05s
  SYST:DELAY?                   查询驻留
  SYST:DISC                     返回 1 后断开所有客户端连接（不停止扫描，监听保持）

关键行为：
- 扫描表与扫描状态全机共享，与具体连接无关；
- 发起 READ? 的客户端断开不会终止扫描，其最终响应进入全机迟到响应缓冲，
  等待 *CLS / *RST 清除；
- 被 ABORt 中止的扫描不产出读数，READ? 只得到错误行。
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

logger = logging.getLogger("scpi-instrument")

VALID_CHANNELS = (101, 102, 103, 104)
DEFAULT_DELAY = 0.05


class InstrumentError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message

    def scpi_line(self) -> str:
        return f'-{self.code},"{self.message}"'


class InstrumentState:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.scan_table: tuple[int, ...] = ()
        self.scanning: bool = False
        self.scan_done = asyncio.Event()
        self.scan_done.set()  # 初始即空闲
        self.abort_event = asyncio.Event()
        self.aborted: bool = False
        self.delay: float = DEFAULT_DELAY
        self.scan_count: int = 0
        self.late_buffer: list[str] = []
        self.connections: set["Connection"] = set()
        self.server: asyncio.AbstractServer | None = None

    # ---- 仪器语义 -----------------------------------------------------

    def voltage_for(self, channel: int) -> float:
        """确定性读数：同一批次同通道稳定，不同扫描轮次略有差异。"""
        base = 0.1 + (channel - 100) * 0.25
        drift = 0.001 * (self.scan_count % 7)
        return base + drift


class Connection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.write_lock = asyncio.Lock()
        self.alive = True
        peer = writer.get_extra_info("peername")
        self.peer = f"{peer[0]}:{peer[1]}" if peer else "?"

    async def send(self, line: str, state: InstrumentState) -> bool:
        if not self.alive:
            state.late_buffer.append(line)
            return False
        try:
            async with self.write_lock:
                self.writer.write((line + "\n").encode())
                await self.writer.drain()
            return True
        except (ConnectionError, BrokenPipeError, RuntimeError):
            self.alive = False
            state.late_buffer.append(line)
            logger.warning("连接 %s 已断开，响应转入迟到缓冲: %s", self.peer, line)
            return False


def _parse_channels(arg: str) -> tuple[int, ...]:
    if not arg.strip():
        raise InstrumentError(221, "Settings conflict: empty channel list")
    try:
        channels = tuple(int(x.strip()) for x in arg.split(","))
    except ValueError:
        raise InstrumentError(222, "Data out of range: channel must be 101..104")
    if not channels:
        raise InstrumentError(221, "Settings conflict: empty channel list")
    if any(ch not in VALID_CHANNELS for ch in channels):
        raise InstrumentError(222, "Data out of range: channel must be 101..104")
    if len(set(channels)) != len(channels):
        raise InstrumentError(221, "Settings conflict: duplicate channels")
    return channels


async def _run_scan(state: InstrumentState, table: tuple[int, ...]) -> None:
    state.scanning = True
    state.scan_done.clear()
    state.aborted = False
    state.scan_count += 1
    readings: list[str] = []
    try:
        for ch in table:
            # 驻留期间可被 ABORt 立即打断
            try:
                await asyncio.wait_for(state.abort_event.wait(), timeout=state.delay)
                state.aborted = True
                break
            except asyncio.TimeoutError:
                pass
            readings.append(f"{state.voltage_for(ch):+.6f}")
        if state.aborted:
            logger.info("扫描在通道 %s 前被 ABORt 中止，丢弃 %d 条部分读数",
                        ch, len(readings))
        else:
            state.last_result = readings  # type: ignore[attr-defined]
            logger.info("扫描完成: %s -> %s", table, readings)
    finally:
        state.scanning = False
        state.scan_done.set()


async def _handle_command(line: str, conn: Connection, state: InstrumentState) -> bool:
    """处理一行命令。返回 False 表示关闭连接。"""
    raw = line.strip()
    if not raw:
        return True
    head, _, arg = raw.partition(" ")
    cmd = head.upper()
    arg = arg.strip()

    try:
        if cmd == "*IDN?":
            await conn.send("LAB,DEMO-SCANNER-1,0001,SCPI-1.0", state)

        elif cmd == "*RST":
            state.abort_event.set()
            await state.scan_done.wait()
            async with state.lock:
                state.scan_table = ()
                state.late_buffer.clear()
                state.abort_event.clear()
            logger.info("仪器复位 *RST")

        elif cmd == "*CLS":
            state.late_buffer.clear()
            logger.info("迟到响应已清除 *CLS")

        elif cmd == "*OPC?":
            if state.scanning:
                await state.scan_done.wait()
            await conn.send("1", state)

        elif cmd == "ROUT:SCAN":
            channels = _parse_channels(arg)
            async with state.lock:
                if state.scanning:
                    raise InstrumentError(221, "Settings conflict: scan in progress")
                state.scan_table = channels
            logger.info("扫描表设置为 %s", channels)

        elif cmd == "ROUT:SCAN?":
            await conn.send(",".join(str(c) for c in state.scan_table), state)

        elif cmd == "READ?":
            async with state.lock:
                if state.scanning:
                    raise InstrumentError(410, "Query INTERRUPTED: scan already running")
                if not state.scan_table:
                    raise InstrumentError(221, "Settings conflict: scan table is empty")
                table = state.scan_table
                state.abort_event.clear()
            # 直接驱动扫描状态机：_run_scan 在首个 await 前置 scanning=True，
            # 并发 READ? 因此能被稳定拒绝；ABORt 不经锁，可随时打断。
            await _run_scan(state, table)
            if state.aborted:
                await conn.send('-410,"Query INTERRUPTED: scan aborted"', state)
            else:
                for reading in getattr(state, "last_result", []):
                    ok = await conn.send(reading, state)
                    if not ok:
                        break  # 剩余行已由 send 转入迟到缓冲

        elif cmd == "ABORT":
            state.abort_event.set()
            logger.info("收到 ABORt，请求中止扫描")
            await conn.send("1", state)

        elif cmd == "SYST:DELAY":
            try:
                value = float(arg)
                if value < 0 or value > 60:
                    raise ValueError
            except ValueError:
                raise InstrumentError(222, "Data out of range: delay in seconds [0,60]")
            state.delay = value
            logger.info("每通道驻留设置为 %.3fs", value)

        elif cmd == "SYST:DELAY?":
            await conn.send(f"{state.delay:.3f}", state)

        elif cmd == "SYST:DISC":
            await conn.send("1", state)
            logger.warning("演示断连：关闭全部 %d 个客户端连接（监听保持）",
                           len(state.connections))
            for other in list(state.connections):
                if other is not conn:
                    other.alive = False
                    try:
                        other.writer.close()
                    except Exception:  # noqa: BLE001
                        pass
            conn.alive = False
            try:
                conn.writer.close()
            except Exception:  # noqa: BLE001
                pass
            return False

        else:
            raise InstrumentError(113, f"Undefined header: {cmd}")

    except InstrumentError as exc:
        await conn.send(exc.scpi_line(), state)

    return True


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                        state: InstrumentState) -> None:
    conn = Connection(reader, writer)
    state.connections.add(conn)
    logger.info("客户端接入 %s（当前连接数 %d）", conn.peer, len(state.connections))
    try:
        while True:
            data = await reader.readline()
            if not data:
                break
            keep_open = await _handle_command(data.decode(errors="replace"), conn, state)
            if not keep_open:
                break
    except (ConnectionError, BrokenPipeError, asyncio.IncompleteReadError):
        pass
    finally:
        state.connections.discard(conn)
        conn.alive = False
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass
        logger.info("客户端离开 %s（当前连接数 %d）", conn.peer,
                    len(state.connections))


async def create_server(host: str, port: int,
                        state: InstrumentState | None = None
                        ) -> tuple[asyncio.AbstractServer, InstrumentState]:
    state = state or InstrumentState()
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, state), host, port
    )
    state.server = server
    return server, state


async def serve(host: str = "0.0.0.0", port: int = 5025) -> None:
    server, _state = await create_server(host, port)
    logger.info("SCPI 演示仪器监听 %s:%d", host, port)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - Windows
            pass
    async with server:
        await stop.wait()
    _state.abort_event.set()
    logger.info("SCPI 演示仪器已停止")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    host = os.environ.get("INSTRUMENT_HOST", "0.0.0.0")
    port = int(os.environ.get("INSTRUMENT_PORT", "5025"))
    asyncio.run(serve(host, port))


if __name__ == "__main__":
    main()
