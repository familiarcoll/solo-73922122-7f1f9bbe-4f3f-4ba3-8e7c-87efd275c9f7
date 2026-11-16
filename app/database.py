"""SQLite 持久层（aiosqlite，WAL 模式）。

一个 Database 实例持有一条专用连接；FastAPI 请求与后台 worker 各持实例，
依靠 WAL + 单写者串行（worker 是唯一写状态机者，请求侧仅做取消标记）保证一致。
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

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

    async def connect(self) -> None:
        directory = os.path.dirname(self.path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.execute("PRAGMA busy_timeout=5000")
        await self._db.executescript(SCHEMA)
        await self._db.commit()

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
        await self.db.execute(
            "INSERT INTO batches (id, channels, status, attempts, created_at) "
            "VALUES (?, ?, ?, 1, ?)",
            (batch_id, json.dumps(channels), BatchStatus.QUEUED.value, _iso(now)),
        )
        await self.db.commit()
        return await self.get_batch(batch_id)  # type: ignore[return-value]

    async def get_batch(self, batch_id: str) -> Optional[dict[str, Any]]:
        async with self.db.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)) as cur:
            row = await cur.fetchone()
        return self._row_to_dict(row) if row else None

    async def list_batches(self) -> list[dict[str, Any]]:
        async with self.db.execute("SELECT * FROM batches ORDER BY created_at DESC") as cur:
            rows = await cur.fetchall()
        return [self._row_to_dict(r) for r in rows]

    async def update_status(
        self,
        batch_id: str,
        status: BatchStatus,
        *,
        error: Optional[str] = None,
        started: bool = False,
        finished: bool = False,
    ) -> None:
        sets = ["status = ?"]
        params: list[Any] = [status.value]
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if started:
            sets.append("started_at = ?")
            params.append(_iso(utcnow()))
        if finished:
            sets.append("finished_at = ?")
            params.append(_iso(utcnow()))
        params.append(batch_id)
        await self.db.execute(
            f"UPDATE batches SET {', '.join(sets)} WHERE id = ?", params
        )
        await self.db.commit()

    async def mark_unfinished_interrupted(self) -> list[str]:
        """服务重启恢复：所有未完成批次标记为 INTERRUPTED，保留已完成读数。"""
        placeholders = ",".join("?" for _ in (
            BatchStatus.QUEUED, BatchStatus.RUNNING, BatchStatus.CANCELLING))
        async with self.db.execute(
            f"SELECT id FROM batches WHERE status IN ({placeholders})",
            tuple(s.value for s in (
                BatchStatus.QUEUED, BatchStatus.RUNNING, BatchStatus.CANCELLING)),
        ) as cur:
            rows = await cur.fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            await self.db.execute(
                f"UPDATE batches SET status = ?, finished_at = ? "
                f"WHERE status IN ({placeholders})",
                (BatchStatus.INTERRUPTED.value, _iso(utcnow()),
                 *(s.value for s in (
                     BatchStatus.QUEUED, BatchStatus.RUNNING, BatchStatus.CANCELLING))),
            )
            await self.db.commit()
        return ids

    async def requeue_interrupted(self, batch_id: str) -> None:
        await self.db.execute(
            "UPDATE batches SET status = ?, error = NULL, attempts = attempts + 1, "
            "finished_at = NULL WHERE id = ? AND status = ?",
            (BatchStatus.QUEUED.value, batch_id, BatchStatus.INTERRUPTED.value),
        )
        await self.db.commit()

    async def count_unfinished(self) -> int:
        async with self.db.execute(
            "SELECT COUNT(*) AS n FROM batches WHERE status = ?",
            (BatchStatus.QUEUED.value,),
        ) as cur:
            row = await cur.fetchone()
        return int(row["n"])

    # ---- 读数 ---------------------------------------------------------

    async def save_readings(self, batch_id: str, channels: list[int],
                            values: list[float]) -> None:
        now = _iso(utcnow())
        await self.db.executemany(
            "INSERT INTO readings (batch_id, channel, position, value, recorded_at) "
            "VALUES (?, ?, ?, ?, ?)",
            [(batch_id, ch, idx, val, now)
             for idx, (ch, val) in enumerate(zip(channels, values))],
        )
        await self.db.commit()

    async def get_readings_for_batch(self, batch_id: str) -> list[dict[str, Any]]:
        async with self.db.execute(
            "SELECT * FROM readings WHERE batch_id = ? ORDER BY position", (batch_id,)
        ) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_readings_for_channel(self, channel: int) -> list[dict[str, Any]]:
        async with self.db.execute(
            "SELECT r.* FROM readings r JOIN batches b ON b.id = r.batch_id "
            "WHERE r.channel = ? ORDER BY r.recorded_at, r.id", (channel,)
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
