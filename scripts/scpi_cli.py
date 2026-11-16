#!/usr/bin/env python
"""极简 SCPI 命令行辅助：python -m scripts.scpi_cli HOST PORT 'CMD'。

无参数时从命令行读取多条命令，以空行结束。仅用于本地演示与冒烟测试。
"""

from __future__ import annotations

import asyncio
import sys


async def run(host: str, port: int, commands: list[str]) -> int:
    reader, writer = await asyncio.open_connection(host, port)
    try:
        for cmd in commands:
            writer.write((cmd + "\n").encode())
            await writer.drain()
            # 查询类命令（以 ? 结尾）或 ABORt/SYST:DISC 有响应
            if cmd.strip().endswith("?") or cmd.strip().upper().startswith(
                    ("ABORT", "SYST:DISC")):
                line = await asyncio.wait_for(reader.readline(), timeout=10)
                print(line.decode().strip())
            else:
                await asyncio.sleep(0.15)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except OSError:
            pass
    return 0


def main() -> None:
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5025
    commands = sys.argv[3:] or [line.strip() for line in sys.stdin if line.strip()]
    asyncio.run(run(host, port, commands))


if __name__ == "__main__":
    main()
