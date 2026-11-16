"""SQLite 持久层（aiosqlite，WAL 模式，自动提交 + 显式发布事务）。

一致性设计
----------
1. 连接使用自动提交（isolation_level=None）：单语句写入各自原子生效，
   不存在“隐式事务残留被下一次 commit 顺带提交”的残件风险。
2. 所有写操作经 `_write_lock` 串行：全库共享一条连接时，阻止其他任务的
   语句滑入本任务尚未提交的显式事务（complete_batch 的多语句发布事务
   尤其关键），同时保证取消与完成登记的 CAS 判断互斥、先提交者胜。
3. 状态迁移一律条件更新（CAS）：UPDATE ... WHERE id=? AND status IN (...)，
   调用方据 rowcount 判断自己是否赢得迁移权，杜绝无条件覆盖他人已提交的
   终态（“取消已受理却又完成”/“完成已提交却被取消覆盖”）。
4. 发布点 complete_batch：完成登记与完整读数在同一事务生效；取消先获准
   则整体回滚、读数全部丢弃；写入失败同样整体回滚，不留可见残件。
   重发（重启接续）时先清除同批旧读数，保证每个所选通道只交付本次成功
   扫描的一条读数。
5. 读数查询与批次状态联立过滤：只有 COMPLETED 批次的读数对外可见，
   批次与通道查询都供不出未完成或已取消的读数。
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import aiosqlite

from .schemas import BatchStatus

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id          TEXT PRIMARY KEY,
    channels    TEXT NOT NULL,
    status      TEXT NOT NULL,
    error       TEXT,
    attempts    INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL,
    started_at  TEXT,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS readings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL REFERENCES batches(id),
    channel     INTEGER NOT NULL,
    position    INTEGER NOT NULL,
    value       REAL NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_channel ON readings(channel);
CREATE INDEX IF NOT EXISTS idx_readings_batch ON readings(batch_id);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(value) if value else None


class Database:
    def __init__(self, path: str):
        self.path = path
        self._db: Optional[aiosqlite.Connection] = None
        # 进程内写者锁：串行所有写操作，保护显式事务不被其他任务的语句穿插
        self._write_lock = asyncio.Lock()

    async def connect(self) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        # 自动提交：单语句各自原子；多语句发布事务用显式 BEGIN/COMMIT
        self._db = await aiosqlite.connect(self.path, isolation_level=None)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.execute("PRAGMA busy_timeout=5000")
        await self._db.executescript(SCHEMA)

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        assert self._db is not None, "Database.connect() 尚未调用"
        return self._db

    # ---- 批次 ---------------------------------------------------------

    async def create_batch(self, batch_id: str, channels: list[int]) -> dict[str, Any]:
        now = utcnow()
        async with self._write_lock:
            await self.db.execute(
                "INSERT INTO batches (id, channels, status, attempts, created_at) "
                "VALUES (?, ?, ?, 1, ?)",
                (batch_id, json.dumps(channels), BatchStatus.QUEUED.value, _iso(now)),
            )
        return await self.get_batch(batch_id)  # type: ignore[return-value]

    async def get_batch(self, batch_id: str) -> Optional[dict[str, Any]]:
        async with self.db.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)) as cur:
            row = await cur.fetchone()
        return self._row_to_dict(row) if row else None

    async def list_batches(self) -> list[dict[str, Any]]:
        async with self.db.execute("SELECT * FROM batches ORDER BY created_at DESC") as cur:
            rows = await cur.fetchall()
        return [self._row_to_dict(r) for r in rows]

    async def try_transition(
        self,
        batch_id: str,
        from_states: Iterable[BatchStatus],
        to: BatchStatus,
        *,
        error: Optional[str] = None,
        started: bool = False,
        finished: bool = False,
    ) -> bool:
        """条件状态迁移（CAS）：仅当前状态属于 from_states 时迁移成功。

        返回 True 表示本次调用赢得了迁移权；返回 False 表示其他迁移已先
        提交（调用方应重读状态后如实响应，不得覆盖）。
        """
        states = list(from_states)
        sets = ["status = ?"]
        params: list[Any] = [to.value]
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if started:
            sets.append("started_at = ?")
            params.append(_iso(utcnow()))
        if finished:
            sets.append("finished_at = ?")
            params.append(_iso(utcnow()))
        placeholders = ",".join("?" for _ in states)
        params.append(batch_id)
        params.extend(s.value for s in states)
        async with self._write_lock:
            cur = await self.db.execute(
                f"UPDATE batches SET {', '.join(sets)} "
                f"WHERE id = ? AND status IN ({placeholders})",
                params,
            )
            return cur.rowcount == 1

    async def mark_unfinished_interrupted(self) -> list[str]:
        """服务重启恢复：所有未完成批次标记为 INTERRUPTED，保留已完成读数。"""
        unfinished = (BatchStatus.QUEUED, BatchStatus.RUNNING, BatchStatus.CANCELLING)
        placeholders = ",".join("?" for _ in unfinished)
        values = tuple(s.value for s in unfinished)
        async with self._write_lock:
            async with self.db.execute(
                f"SELECT id FROM batches WHERE status IN ({placeholders})", values
            ) as cur:
                rows = await cur.fetchall()
            ids = [r["id"] for r in rows]
            if ids:
                await self.db.execute(
                    f"UPDATE batches SET status = ?, finished_at = ? "
                    f"WHERE status IN ({placeholders})",
                    (BatchStatus.INTERRUPTED.value, _iso(utcnow()), *values),
                )
        return ids

    async def requeue_interrupted(self, batch_id: str) -> None:
        async with self._write_lock:
            await self.db.execute(
                "UPDATE batches SET status = ?, error = NULL, attempts = attempts + 1, "
                "finished_at = NULL WHERE id = ? AND status = ?",
                (BatchStatus.QUEUED.value, batch_id, BatchStatus.INTERRUPTED.value),
            )

    async def count_unfinished(self) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) AS n FROM batches WHERE status = ?",
            (BatchStatus.QUEUED.value,),
        ) as cur:
            row = await cur.fetchone()
        return int(row["n"])

    # ---- 读数发布（完成登记与完整读数同一事务） ------------------------

    async def complete_batch(self, batch_id: str, channels: list[int],
                             values: list[float]) -> bool:
        """发布点：完成登记 + 完整读数在同一事务生效。

        返回 True  —— 本批次赢得发布权：读数已落库且状态为 COMPLETED。
        返回 False —— 取消已先获准：事务整体回滚，本次读数全部丢弃，
                      调用方按取消收口，不得再保存任何读数。
        异常       —— 写入失败整体回滚，不留可见残件。
        """
        now = _iso(utcnow())
        rows = [(batch_id, ch, idx, val, now)
                for idx, (ch, val) in enumerate(zip(channels, values))]
        async with self._write_lock:
            try:
                await self.db.execute("BEGIN IMMEDIATE")
                cur = await self.db.execute(
                    "UPDATE batches SET status = ?, finished_at = ? "
                    "WHERE id = ? AND status = ?",
                    (BatchStatus.COMPLETED.value, now, batch_id,
                     BatchStatus.RUNNING.value),
                )
                if cur.rowcount != 1:
                    # 取消已先获准：不再发布
                    await self.db.execute("ROLLBACK")
                    return False
                # 同批旧读数（如旧版本留下的残件）一并清除：
                # 每个所选通道只交付本次成功扫描的一条读数
                await self.db.execute(
                    "DELETE FROM readings WHERE batch_id = ?", (batch_id,))
                await self.db.executemany(
                    "INSERT INTO readings "
                    "(batch_id, channel, position, value, recorded_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    rows,
                )
                await self.db.execute("COMMIT")
                return True
            except Exception:
                with contextlib.suppress(Exception):
                    await self.db.execute("ROLLBACK")
                raise

    # ---- 读数查询（与批次状态联立：仅 COMPLETED 批次对外可见） ---------

    async def get_readings_for_batch(self, batch_id: str) -> list[dict[str, Any]]:
        async with self.db.execute(
            "SELECT r.* FROM readings r JOIN batches b ON b.id = r.batch_id "
            "WHERE r.batch_id = ? AND b.status = ? ORDER BY r.position",
            (batch_id, BatchStatus.COMPLETED.value),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_readings_for_channel(self, channel: int) -> list[dict[str, Any]]:
        async with self.db.execute(
            "SELECT r.* FROM readings r JOIN batches b ON b.id = r.batch_id "
            "WHERE r.channel = ? AND b.status = ? ORDER BY r.recorded_at, r.id",
            (channel, BatchStatus.COMPLETED.value),
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ---- 映射 ---------------------------------------------------------

    @staticmethod
    def _row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
        data = dict(row)
        data["channels"] = json.loads(data["channels"])
        data["created_at"] = _parse_dt(data["created_at"])
        data["started_at"] = _parse_dt(data["started_at"])
        data["finished_at"] = _parse_dt(data["finished_at"])
        return data
