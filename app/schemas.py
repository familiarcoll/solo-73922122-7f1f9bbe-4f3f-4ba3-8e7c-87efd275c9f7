"""批次状态常量与 Pydantic 模型。"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator

VALID_CHANNELS = (101, 102, 103, 104)


class BatchStatus(str, Enum):
    QUEUED = "QUEUED"            # 已受理，排队等待
    RUNNING = "RUNNING"          # 仪器扫描执行中
    CANCELLING = "CANCELLING"    # 取消已受理，正在停止仪器
    COMPLETED = "COMPLETED"      # 读数已落库
    CANCELLED = "CANCELLED"      # 已取消（排队中或执行中），无读数
    FAILED = "FAILED"            # 超时 / 仪器错误 / 断连，读数未保存
    INTERRUPTED = "INTERRUPTED"  # 服务重启时未完成


TERMINAL_STATUSES = {
    BatchStatus.COMPLETED,
    BatchStatus.CANCELLED,
    BatchStatus.FAILED,
    BatchStatus.INTERRUPTED,
}


class BatchCreateRequest(BaseModel):
    channels: list[int] = Field(
        ..., min_length=1, description="通道子集，取值 101~104，不可重复"
    )

    @field_validator("channels")
    @classmethod
    def _validate_channels(cls, value: list[int]) -> list[int]:
        if any(ch not in VALID_CHANNELS for ch in value):
            raise ValueError("通道号必须是 101、102、103、104 的子集")
        if len(set(value)) != len(value):
            raise ValueError("通道不可重复")
        return value


class BatchInfo(BaseModel):
    id: str
    channels: list[int]
    status: BatchStatus
    error: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    attempts: int = 1


class ReadingOut(BaseModel):
    channel: int
    position: int
    value: float
    recorded_at: datetime


class BatchReadings(BaseModel):
    batch_id: str
    status: BatchStatus
    channels: list[int]
    readings: list[ReadingOut]


class CancelResult(BaseModel):
    id: str
    accepted: bool
    status: BatchStatus
    message: str


class HealthOut(BaseModel):
    status: str
    instrument_synced: bool
    queue_depth: int
    unfinished_batches: int
