"""SCPI 仪器 TCP 客户端（带行缓冲、迟到响应清理、慢响应/断连容忍）。

- 每次命令调用使用独立连接，天然支持“客户端断开不终止仪器扫描”：
  连接断开时仪器继续扫描，响应留在仪器迟到缓冲，由 clear_late_responses 清理。
- readline 支持超时，避免被慢响应或断连永久挂住。
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

logger = logging.getLogger("scpi-client")


class ScpiError(Exception):
    def __init__(self, code: int | None, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass
class ScpiConfig:
    host: str = "instrument"
    port: int = 5025
    timeout: float = 10.0
    connect_timeout: float = 5.0


class ScpiClient:
    def __init__(self, config: ScpiConfig):
        self.config = config

    async def _open(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.config.host, self.config.port),
                timeout=self.config.connect_timeout,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            raise ScpiError(None, f"无法连接仪器 {self.config.host}:{self.config.port}: {exc}") from exc
        return reader, writer

    async def command(self, cmd: str, *, expect_response: bool = True,
                      timeout: float | None = None) -> list[str]:
        """发送一条命令，返回响应行（已去掉行尾）。expect_response=False 时返回空列表。"""
        reader, writer = await self._open()
        to = timeout if timeout is not None else self.config.timeout
        try:
            writer.write((cmd + "\n").encode())
            await asyncio.wait_for(writer.drain(), timeout=to)
            if not expect_response:
                return []
            lines: list[str] = []
            while True:
                try:
                    data = await asyncio.wait_for(reader.readline(), timeout=to)
                except asyncio.TimeoutError as exc:
                    raise ScpiError(None, f"仪器响应超时（{to}s）: {cmd}") from exc
                if not data:
                    raise ScpiError(None, f"仪器连接提前断开: {cmd}")
                line = data.decode(errors="replace").strip()
                if line:
                    lines.append(line)
                # 所有命令均为单响应或定长响应；读到一行即可返回。
                break
            self._raise_if_error(lines[0], cmd)
            return lines
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass

    @staticmethod
    def _raise_if_error(line: str, cmd: str) -> None:
        if line.startswith("-"):
            body = line[1:]
            code_str, _, msg = body.partition(",")
            try:
                code = int(code_str)
            except ValueError:
                code = None
            message = msg.strip('"') if msg else body
            raise ScpiError(code, f"仪器拒绝 {cmd}: {message}")

    # ---- 业务语义封装 -------------------------------------------------

    async def idn(self) -> str:
        return (await self.command("*IDN?"))[0]

    async def reset(self) -> None:
        # *RST 无响应
        await self.command("*RST", expect_response=False)

    async def set_scan_table(self, channels: list[int]) -> None:
        # ROUT:SCAN 正常无响应；先在客户端预检，非法参数不产生连接污染
        if not channels or any(ch not in (101, 102, 103, 104) for ch in channels):
            raise ScpiError(222, f"非法通道列表: {channels}")
        if len(set(channels)) != len(channels):
            raise ScpiError(221, f"通道重复: {channels}")
        await self.command(
            f"ROUT:SCAN {','.join(str(c) for c in channels)}",
            expect_response=False)

    async def read(self, expected_count: int, timeout: float) -> list[float]:
        """触发 READ? 并读取 expected_count 条裸读数（无批次号）。

        扫描中被 ABORt 时仪器返回错误行，抛 ScpiError。
        连接在读取中断开会抛 ScpiError，仪器侧扫描仍继续。
        """
        reader, writer = await self._open()
        values: list[float] = []
        try:
            writer.write(b"READ?\n")
            await asyncio.wait_for(writer.drain(), timeout=timeout)
            while len(values) < expected_count:
                try:
                    data = await asyncio.wait_for(reader.readline(), timeout=timeout)
                except asyncio.TimeoutError as exc:
                    raise ScpiError(None,
                                    f"READ? 超时，仅收到 {len(values)}/{expected_count} 条") from exc
                if not data:
                    raise ScpiError(None,
                                    f"READ? 连接断开，仅收到 {len(values)}/{expected_count} 条")
                line = data.decode(errors="replace").strip()
                if not line:
                    continue
                if line.startswith("-"):
                    self._raise_if_error(line, "READ?")
                try:
                    values.append(float(line))
                except ValueError as exc:
                    raise ScpiError(None, f"无法解析读数 {line!r}") from exc
            # 严格校验：若仪器还吐出额外行（迟到响应未清干净），快速探测并报错，
            # 由上层走停机确认 + *CLS 流程，避免污染下一批。
            try:
                extra = await asyncio.wait_for(reader.readline(), timeout=0.5)
            except asyncio.TimeoutError:
                extra = b""
            if extra.strip():
                raise ScpiError(
                    None,
                    f"READ? 返回超出预期的额外响应 {extra.decode().strip()!r}，"
                    "仪器迟到响应未清干净")
            return values
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass

    async def abort(self) -> None:
        # ABORt 本身必须快速受理，给较短超时；失败由上层重试。
        await self.command("ABORt", timeout=min(self.config.timeout, 5.0))

    async def opc(self, timeout: float = 30.0) -> None:
        """阻塞到仪器空闲（*OPC? 在扫描结束后才回 1）。"""
        await self.command("*OPC?", timeout=timeout)

    async def clear_late_responses(self) -> None:
        """*CLS 清掉仪器侧迟到响应缓冲（无响应行）。"""
        try:
            await self.command("*CLS", expect_response=False)
        except ScpiError as exc:
            logger.warning("*CLS 失败: %s", exc)

    async def set_delay(self, seconds: float) -> None:
        # SYST:DELAY 无响应
        await self.command(f"SYST:DELAY {seconds:.3f}", expect_response=False)

    async def disconnect_clients(self) -> None:
        """演示用：要求仪器断开所有客户端连接（不影响监听与扫描）。"""
        reader, writer = await self._open()
        try:
            writer.write(b"SYST:DISC\n")
            await asyncio.wait_for(writer.drain(), timeout=5.0)
            # 仪器回 1 后即断连，读不到 EOF 也算成功
            try:
                await asyncio.wait_for(reader.readline(), timeout=2.0)
            except (asyncio.TimeoutError, OSError):
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass
