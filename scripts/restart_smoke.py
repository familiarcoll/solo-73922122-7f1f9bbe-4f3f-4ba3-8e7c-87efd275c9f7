"""进程内重启接续冒烟脚本（环境禁止 kill 信号，用两段 lifespan 模拟重启）。

针对当前正在运行的仪器 127.0.0.1:5025，使用独立临时 DB，
先后启动两个 AppState/Coordinator，验证 INTERRUPTED 标记与 resume 接续。
"""

from __future__ import annotations

import asyncio
import os
import tempfile

from app.config import Settings
from app.database import Database
from app.coordinator import Coordinator
from app.scpi_client import ScpiClient, ScpiConfig


async def main() -> None:
    db_path = os.path.join(tempfile.mkdtemp(), "restart-smoke.db")
    settings = Settings(
        instrument_host="127.0.0.1", instrument_port=5025,
        instrument_timeout=5.0, db_path=db_path,
        scan_timeout=3.0, resync_interval=0.5, opc_wait_timeout=5.0)

    def build():
        db = Database(settings.db_path)
        scpi = ScpiClient(ScpiConfig(
            host=settings.instrument_host, port=settings.instrument_port,
            timeout=settings.instrument_timeout))
        coord = Coordinator(db, scpi, settings.scan_timeout,
                            settings.resync_interval, settings.opc_wait_timeout)
        return db, scpi, coord

    async def wait_for(db, bid, targets):
        for _ in range(100):
            row = await db.get_batch(bid)
            if row and row["status"] in targets:
                return row
            await asyncio.sleep(0.1)
        raise RuntimeError(f"{bid} 未到达 {targets}")

    # ---- 第一个“进程” ----
    db1, scpi1, coord1 = build()
    await scpi1.set_delay(0.4)
    await db1.connect()
    await coord1.startup()
    await db1.create_batch("B-DONE", [101, 102])
    await coord1.enqueue("B-DONE")
    await wait_for(db1, "B-DONE", {"COMPLETED"})
    await db1.create_batch("B-PENDING", [103, 104])
    await coord1.enqueue("B-PENDING")
    await wait_for(db1, "B-PENDING", {"QUEUED", "RUNNING"})
    await coord1.shutdown()
    await db1.close()
    print("[1] 第一个实例停止：B-DONE 已完成，B-PENDING 未完成")

    # ---- 第二个“进程”：同一 DB 文件重启 ----
    db2, scpi2, coord2 = build()
    await db2.connect()
    interrupted = await coord2.startup()
    assert "B-PENDING" in interrupted
    row = await db2.get_batch("B-PENDING")
    assert row["status"] == "INTERRUPTED", row["status"]
    kept = await db2.get_readings_for_batch("B-DONE")
    assert len(kept) == 2
    print(f"[2] 重启恢复：未完成批次 -> {row['status']}；历史读数保留 {len(kept)} 条")

    await scpi2.set_delay(0.05)
    assert await coord2.resume("B-PENDING") is True
    row = await wait_for(db2, "B-PENDING", {"COMPLETED"})
    readings = await db2.get_readings_for_batch("B-PENDING")
    assert [r["channel"] for r in readings] == [103, 104]
    assert row["attempts"] == 2
    print(f"[3] 接续成功：status={row['status']} attempts={row['attempts']} "
          f"channels={[r['channel'] for r in readings]}")

    await coord2.shutdown()
    await db2.close()
    print("OK: 重启中断标记 + 接续采集 + 历史读数保留 全部验证通过")


if __name__ == "__main__":
    asyncio.run(main())
