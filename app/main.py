"""FastAPI 采集服务入口。

接口一览（/docs 可交互演示）：
  POST /batches                 按通道子集（101~104）提交采集批次
  GET  /batches                 批次列表与状态
  GET  /batches/{batch_id}      单批次状态
  POST /batches/{batch_id}/cancel   取消（立即受理）
  POST /batches/{batch_id}/resume   重启后接续 INTERRUPTED 批次
  GET  /batches/{batch_id}/readings 该批次各通道完整读数
  GET  /channels/{channel}/readings 某通道跨批次完整读数
  GET  /health                  服务与仪器同步状态
  GET  /                         演示入口页
"""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from .config import Settings, settings
from .coordinator import Coordinator
from .database import Database
from .schemas import (
    BatchCreateRequest,
    BatchInfo,
    BatchReadings,
    BatchStatus,
    CancelResult,
    HealthOut,
    ReadingOut,
    VALID_CHANNELS,
)
from .scpi_client import ScpiClient, ScpiConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("api")


def create_app(app_settings: Settings, db: Database,
               coordinator: Coordinator) -> FastAPI:
    """应用工厂：依赖注入，便于测试替换仪器地址与数据库路径。"""

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await db.connect()
        interrupted = await coordinator.startup()
        logger.info("采集服务启动，DB=%s，仪器=%s:%d，重启中断批次 %d 个",
                    app_settings.db_path, app_settings.instrument_host,
                    app_settings.instrument_port, len(interrupted))
        try:
            yield
        finally:
            await coordinator.shutdown()
            await db.close()

    application = FastAPI(
        title="共享扫描仪电压采集 API",
        version="1.0.0",
        description=(
            "实验室共享扫描仪批次采集服务。批次串行执行，扫描表全机共享，"
            "支持取消、3 秒超时、断连重同步与重启接续。"
        ),
        lifespan=lifespan,
    )

    def get_db() -> Database:
        return db

    def _batch_info(row: dict) -> BatchInfo:
        return BatchInfo(
            id=row["id"],
            channels=row["channels"],
            status=BatchStatus(row["status"]),
            error=row.get("error"),
            created_at=row["created_at"],
            started_at=row.get("started_at"),
            finished_at=row.get("finished_at"),
            attempts=row.get("attempts", 1),
        )

    @application.post("/batches", response_model=BatchInfo, status_code=201,
                      tags=["批次"])
    async def create_batch(payload: BatchCreateRequest,
                           _db: Database = Depends(get_db)) -> BatchInfo:
        """提交一个采集批次（通道子集 101~104），返回排队状态。"""
        batch_id = f"B{uuid.uuid4().hex[:12]}"
        row = await db.create_batch(batch_id, payload.channels)
        await coordinator.enqueue(batch_id)
        logger.info("批次 %s 已入队，通道 %s", batch_id, payload.channels)
        return _batch_info(row)

    @application.get("/batches", response_model=list[BatchInfo], tags=["批次"])
    async def list_batches(_db: Database = Depends(get_db)) -> list[BatchInfo]:
        return [_batch_info(r) for r in await db.list_batches()]

    @application.get("/batches/{batch_id}", response_model=BatchInfo,
                     tags=["批次"])
    async def get_batch(batch_id: str,
                        _db: Database = Depends(get_db)) -> BatchInfo:
        row = await db.get_batch(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        return _batch_info(row)

    @application.post("/batches/{batch_id}/cancel", response_model=CancelResult,
                      tags=["批次"])
    async def cancel_batch(batch_id: str,
                           _db: Database = Depends(get_db)) -> CancelResult:
        result = await coordinator.request_cancel(batch_id)
        if not result.get("found", False):
            raise HTTPException(status_code=404, detail="批次不存在")
        return CancelResult(
            id=batch_id,
            accepted=bool(result["accepted"]),
            status=result.get("status", BatchStatus.FAILED),
            message=result["message"],
        )

    @application.post("/batches/{batch_id}/resume", response_model=BatchInfo,
                      tags=["批次"])
    async def resume_batch(batch_id: str,
                           _db: Database = Depends(get_db)) -> BatchInfo:
        """重启恢复后接续：仅 INTERRUPTED 批次可重新入队，已完成读数保留。"""
        ok = await coordinator.resume(batch_id)
        if not ok:
            raise HTTPException(
                status_code=409,
                detail="仅 INTERRUPTED 状态的批次可以接续（或批次不存在）",
            )
        row = await db.get_batch(batch_id)
        return _batch_info(row)  # type: ignore[arg-type]

    @application.get("/batches/{batch_id}/readings",
                     response_model=BatchReadings, tags=["读数"])
    async def batch_readings(batch_id: str,
                             _db: Database = Depends(get_db)) -> BatchReadings:
        """该批次各通道的完整读数（无读数时 readings 为空并提示当前状态）。"""
        row = await db.get_batch(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="批次不存在")
        rows = await db.get_readings_for_batch(batch_id)
        return BatchReadings(
            batch_id=batch_id,
            status=BatchStatus(row["status"]),
            channels=row["channels"],
            readings=[ReadingOut(**r) for r in rows],
        )

    @application.get("/channels/{channel}/readings",
                     response_model=list[ReadingOut], tags=["读数"])
    async def channel_readings(channel: int,
                               _db: Database = Depends(get_db)
                               ) -> list[ReadingOut]:
        """某一通道跨所有已完成批次的完整读数。"""
        if channel not in VALID_CHANNELS:
            raise HTTPException(status_code=422,
                                detail="通道号必须是 101、102、103 或 104")
        rows = await db.get_readings_for_channel(channel)
        return [ReadingOut(**r) for r in rows]

    @application.get("/health", response_model=HealthOut, tags=["运维"])
    async def health(_db: Database = Depends(get_db)) -> HealthOut:
        h = coordinator.health()
        return HealthOut(
            status="ok",
            instrument_synced=h["synced"],
            queue_depth=h["queue_depth"],
            unfinished_batches=await db.count_unfinished(),
        )

    @application.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def index(request: Request) -> str:
        return _INDEX_HTML

    return application


class AppState:
    """默认（uvicorn / docker）装配。"""

    def __init__(self, app_settings: Settings) -> None:
        self.db = Database(app_settings.db_path)
        self.client = ScpiClient(ScpiConfig(
            host=app_settings.instrument_host,
            port=app_settings.instrument_port,
            timeout=app_settings.instrument_timeout,
        ))
        self.coordinator = Coordinator(
            self.db, self.client,
            scan_timeout=app_settings.scan_timeout,
            resync_interval=app_settings.resync_interval,
            opc_wait_timeout=app_settings.opc_wait_timeout,
        )


state = AppState(settings)
app = create_app(settings, state.db, state.coordinator)


_INDEX_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<title>共享扫描仪电压采集 API</title>
<style>
body{font-family:system-ui,sans-serif;max-width:860px;margin:2rem auto;padding:0 1rem;line-height:1.6}
code{background:#f4f4f4;padding:.1rem .35rem;border-radius:4px}
.card{border:1px solid #ddd;border-radius:8px;padding:1rem 1.25rem;margin:1rem 0}
a.btn{display:inline-block;background:#1261a0;color:#fff;padding:.5rem 1rem;border-radius:6px;text-decoration:none}
</style></head><body>
<h1>共享扫描仪电压采集 API</h1>
<p>FastAPI + SQLite 批次采集服务，配合本地 TCP SCPI 演示仪器使用。</p>
<div class="card">
<h3>快速演示</h3>
<ol>
<li>打开 <a class="btn" href="/docs">Swagger UI（/docs）</a></li>
<li><code>POST /batches</code> 提交 <code>{"channels":[101,103,104]}</code></li>
<li>用 <code>GET /batches/{id}</code> 轮询状态，完成后 <code>GET /batches/{id}/readings</code> 取读数</li>
<li>用 <code>POST /batches/{id}/cancel</code> 体验取消；仪器慢响应可在仪器容器执行
<code>SYST:DELAY 1</code>，断连演示执行 <code>SYST:DISC</code></li>
</ol>
</div>
<div class="card">
<h3>状态机</h3>
<p><code>QUEUED → RUNNING → COMPLETED</code>；取消路径
<code>QUEUED/RUNNING → CANCELLING → CANCELLED</code>；超时与断连为
<code>FAILED</code>；服务重启时未完成批次为 <code>INTERRUPTED</code>，
可 <code>POST /batches/{id}/resume</code> 接续，已完成读数保留。</p>
</div>
<p>健康检查：<code>GET /health</code>；通道历史读数：
<code>GET /channels/101/readings</code></p>
</body></html>"""
