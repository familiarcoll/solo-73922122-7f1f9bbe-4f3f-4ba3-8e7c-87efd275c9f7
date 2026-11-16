"""采集协调器：批次串行状态机 + 仪器同步。

设计要点
--------
1. 单 worker 协程 + 仪器锁语义：任何时刻最多一个批次在配置扫描表 / 取数，
   并发批次在队列中排队，绝不交叉配置或取数。
2. 取消立即受理：排队批次直接置 CANCELLED（worker 弹出后跳过，不触发仪器）；
   执行中批次先置 CANCELLING 并经独立连接发 ABORt，确认停机并清迟到响应后置
   CANCELLED，读数不落库。
3. 扫描满 SCAN_TIMEOUT（默认 3 秒）即超时：取消 READ?、ABORt、确认停机、
   清迟到响应，读数不落库，然后才允许下一批。
4. 仪器不可达 / 连接断开：批次 FAILED 且读数不保存；worker 进入重同步循环
   （ABORt → *OPC? 确认停止 → *RST 清扫描表与迟到响应），期间状态查询照常，
   新批次可入队但不会触发仪器。
5. 服务重启：启动时把 QUEUED/RUNNING/CANCELLING 批次统一标记 INTERRUPTED，
   完成仪器重同步后，可经 /batches/{id}/resume 重新入队接续，已完成读数保留。
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any, Optional

from .database import Database
from .schemas import BatchStatus, TERMINAL_STATUSES
from .scpi_client import ScpiClient, ScpiError

logger = logging.getLogger("coordinator")


class Coordinator:
    def __init__(self, db: Database, client: ScpiClient,
                 scan_timeout: float, resync_interval: float,
                 opc_wait_timeout: float):
        self.db = db
        self.client = client
        self.scan_timeout = scan_timeout
        self.resync_interval = resync_interval
        self.opc_wait_timeout = opc_wait_timeout

        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._worker_task: Optional[asyncio.Task[None]] = None
        self._resync_task: Optional[asyncio.Task[None]] = None
        self._cancel_events: dict[str, asyncio.Event] = {}
        self._instrument_lock = asyncio.Lock()

        self.synced = False
        self.sync_event = asyncio.Event()
        self._stopping = False
        self._current_batch: Optional[str] = None
        self._last_resync_reason: Optional[str] = None

    # ---- 生命周期 -----------------------------------------------------

    async def startup(self) -> list[str]:
        interrupted = await self.db.mark_unfinished_interrupted()
        if interrupted:
            logger.warning("重启恢复：%d 个未完成批次标记为 INTERRUPTED: %s",
                           len(interrupted), interrupted)
        self._worker_task = asyncio.create_task(self._worker(), name="batch-worker")
        self._resync_task = asyncio.create_task(
            self._resync_loop("服务启动"), name="instrument-resync"
        )
        return interrupted

    async def shutdown(self) -> None:
        self._stopping = True
        self.sync_event.set()  # 解除 worker 等待
        for task in (self._worker_task, self._resync_task):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._worker_task = None
        self._resync_task = None

    # ---- 入队 / 取消 / 接续 ------------------------------------------

    async def enqueue(self, batch_id: str) -> None:
        self._cancel_events[batch_id] = asyncio.Event()
        await self._queue.put(batch_id)

    async def request_cancel(self, batch_id: str) -> dict[str, Any]:
        batch = await self.db.get_batch(batch_id)
        if batch is None:
            return {"found": False, "accepted": False, "message": "批次不存在"}
        status = BatchStatus(batch["status"])
        if status in TERMINAL_STATUSES:
            return {
                "found": True, "accepted": False, "status": status,
                "message": f"批次已处于终态 {status.value}，不可取消",
            }
        if status == BatchStatus.QUEUED:
            # 排队中：立即取消，worker 弹出后跳过，绝不触发仪器
            await self.db.update_status(batch_id, BatchStatus.CANCELLED, finished=True)
            self._event_for(batch_id).set()
            logger.info("批次 %s 排队中取消，不再触发仪器", batch_id)
            return {"found": True, "accepted": True, "status": BatchStatus.CANCELLED,
                    "message": "取消已受理：批次尚未执行，已移出队列"}
        if status == BatchStatus.RUNNING:
            await self.db.update_status(batch_id, BatchStatus.CANCELLING)
            event = self._event_for(batch_id)
            event.set()
            # 独立连接立即发 ABORt，不阻塞取消响应
            asyncio.create_task(self._abort_for_cancel(batch_id))
            logger.info("批次 %s 执行中取消已受理（CANCELLING）", batch_id)
            return {"found": True, "accepted": True, "status": BatchStatus.CANCELLING,
                    "message": "取消已受理：正在停止仪器扫描"}
        # CANCELLING 幂等
        self._event_for(batch_id).set()
        return {"found": True, "accepted": True, "status": BatchStatus.CANCELLING,
                "message": "取消已在处理中"}

    async def resume(self, batch_id: str) -> bool:
        batch = await self.db.get_batch(batch_id)
        if batch is None or BatchStatus(batch["status"]) != BatchStatus.INTERRUPTED:
            return False
        await self.db.requeue_interrupted(batch_id)
        self._cancel_events[batch_id] = asyncio.Event()
        await self._queue.put(batch_id)
        logger.info("中断批次 %s 重新入队接续采集（attempts+1，已有读数保留）", batch_id)
        return True

    def _event_for(self, batch_id: str) -> asyncio.Event:
        event = self._cancel_events.get(batch_id)
        if event is None:
            event = asyncio.Event()
            self._cancel_events[batch_id] = event
        return event

    async def _abort_for_cancel(self, batch_id: str) -> None:
        try:
            await self.client.abort()
        except ScpiError as exc:
            logger.warning("批次 %s 取消 ABORt 发送失败（将由停机确认重试）: %s",
                           batch_id, exc)

    # ---- worker 主循环 -----------------------------------------------

    async def _worker(self) -> None:
        logger.info("批次 worker 启动，等待仪器同步…")
        while not self._stopping:
            try:
                await self.sync_event.wait()
                if self._stopping:
                    break
                batch_id = await self._queue.get()
                batch = await self.db.get_batch(batch_id)
                if batch is None:
                    continue
                if BatchStatus(batch["status"]) != BatchStatus.QUEUED:
                    logger.info("批次 %s 状态为 %s，跳过（排队期间已取消）",
                                batch_id, batch["status"])
                    continue
                self._current_batch = batch_id
                await self._run_batch(batch_id, batch["channels"])
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - worker 永不退出
                logger.exception("worker 处理批次时发生未预期异常，触发仪器重同步")
                with contextlib.suppress(Exception):
                    if self._current_batch:
                        cur = await self.db.get_batch(self._current_batch)
                        if cur and BatchStatus(cur["status"]) not in TERMINAL_STATUSES:
                            await self.db.update_status(
                                self._current_batch, BatchStatus.FAILED,
                                error="内部异常，触发仪器重同步", finished=True)
                self._begin_resync("worker 异常")
            finally:
                self._current_batch = None

    # ---- 单批次执行 ---------------------------------------------------

    async def _run_batch(self, batch_id: str, channels: list[int]) -> None:
        event = self._event_for(batch_id)
        try:
            await self.db.update_status(batch_id, BatchStatus.RUNNING, started=True)
            logger.info("批次 %s 开始执行，通道 %s", batch_id, channels)

            async with self._instrument_lock:
                await self.client.set_scan_table(channels)
                if event.is_set():
                    await self._abort_and_drain("执行前取消")
                    await self._finish_cancelled(batch_id)
                    return

                read_task = asyncio.create_task(
                    self.client.read(len(channels), timeout=self.scan_timeout + 5.0),
                    name=f"read-{batch_id}",
                )
                try:
                    values = await asyncio.wait_for(
                        asyncio.shield(read_task), timeout=self.scan_timeout
                    )
                except asyncio.TimeoutError:
                    read_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError, ScpiError):
                        await read_task
                    reason = f"扫描超过 {self.scan_timeout:.0f} 秒超时"
                    logger.error("批次 %s %s，读数不保存", batch_id, reason)
                    await self._abort_and_drain(reason)
                    if event.is_set():
                        await self._finish_cancelled(batch_id)
                    else:
                        await self.db.update_status(
                            batch_id, BatchStatus.FAILED,
                            error=f"{reason}，已确认仪器停止并清除迟到响应",
                            finished=True)
                    return
                except ScpiError as exc:
                    with contextlib.suppress(asyncio.CancelledError):
                        await read_task
                    if event.is_set():
                        # ABORt 先于取数完成生效：仪器回 aborted 错误属取消预期
                        await self._abort_and_drain("执行中取消")
                        await self._finish_cancelled(batch_id)
                        return
                    logger.error("批次 %s 仪器取数失败: %s", batch_id, exc)
                    await self._abort_and_drain(f"取数失败: {exc.message}")
                    await self.db.update_status(
                        batch_id, BatchStatus.FAILED,
                        error=f"仪器取数失败: {exc.message}", finished=True)
                    raise

                if event.is_set():
                    # 扫描已完成但取消信号先到：丢弃读数，仍确认停机并清理
                    await self._abort_and_drain("完成瞬间取消")
                    await self._finish_cancelled(batch_id)
                    return

                await self.db.save_readings(batch_id, channels, values)
                await self.db.update_status(
                    batch_id, BatchStatus.COMPLETED, finished=True)
                logger.info("批次 %s 完成，保存 %d 条读数: %s",
                            batch_id, len(values), values)
        except ScpiError as exc:
            if event.is_set():
                # 取消路径上的通信异常（含停机确认失败）：按取消收口，不判失败
                with contextlib.suppress(ScpiError):
                    await self._abort_and_drain("取消收口")
                await self._finish_cancelled(batch_id)
                return
            logger.error("批次 %s 执行失败: %s", batch_id, exc)
            cur = await self.db.get_batch(batch_id)
            if cur and BatchStatus(cur["status"]) not in TERMINAL_STATUSES:
                await self.db.update_status(
                    batch_id, BatchStatus.FAILED,
                    error=f"仪器通信失败: {exc.message}", finished=True)
            self._begin_resync(f"批次 {batch_id} 通信失败: {exc.message}")
        finally:
            self._cancel_events.pop(batch_id, None)

    async def _finish_cancelled(self, batch_id: str) -> None:
        await self.db.update_status(batch_id, BatchStatus.CANCELLED, finished=True)
        logger.info("批次 %s 已取消，读数未保存", batch_id)

    # ---- 仪器停止确认 / 迟到响应清理 / 重同步 ------------------------

    async def _abort_and_drain(self, reason: str) -> None:
        """ABORt → *OPC? 确认停止 → *CLS 清迟到响应。三次短重试。"""
        last_exc: Optional[ScpiError] = None
        for attempt in range(1, 4):
            try:
                await self.client.abort()
                await self.client.opc(timeout=self.opc_wait_timeout)
                await self.client.clear_late_responses()
                logger.info("已确认仪器停止并清除迟到响应（%s，第 %d 次尝试）",
                            reason, attempt)
                return
            except ScpiError as exc:
                last_exc = exc
                logger.warning("停机确认第 %d 次失败（%s）: %s", attempt, reason, exc)
                await asyncio.sleep(0.2 * attempt)
        raise ScpiError(None, f"无法确认仪器停止: {last_exc.message if last_exc else ''}")

    def _begin_resync(self, reason: str) -> None:
        self.synced = False
        self.sync_event.clear()
        self._last_resync_reason = reason
        if self._resync_task is None or self._resync_task.done():
            self._resync_task = asyncio.create_task(
                self._resync_loop(reason), name="instrument-resync")
        else:
            logger.info("重同步已在进行中，原因追加: %s", reason)

    async def _resync_loop(self, reason: str) -> None:
        logger.warning("开始仪器重同步: %s", reason)
        backoff = self.resync_interval
        while not self._stopping:
            try:
                # 1) 尽力发 ABORt（连不上也没关系，继续重试）
                with contextlib.suppress(ScpiError):
                    await self.client.abort()
                # 2) *OPC? 确认仪器已经停止
                await self.client.opc(timeout=self.opc_wait_timeout)
                # 3) *RST：停机 + 清扫描表 + 清迟到响应
                await self.client.reset()
                self.synced = True
                self.sync_event.set()
                self._last_resync_reason = None
                logger.warning("仪器重同步完成，排队批次可以继续")
                return
            except ScpiError as exc:
                logger.warning("重同步未完成（%s）: %s，%.1fs 后重试",
                               reason, exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 5.0)
            except asyncio.CancelledError:
                raise

    # ---- 观测 ---------------------------------------------------------

    def health(self) -> dict[str, Any]:
        return {
            "synced": self.synced,
            "queue_depth": self._queue.qsize(),
            "current_batch": self._current_batch,
            "resync_reason": self._last_resync_reason,
        }
