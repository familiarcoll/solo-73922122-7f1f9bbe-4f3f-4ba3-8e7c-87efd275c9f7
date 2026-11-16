"""运行配置（全部可通过环境变量覆盖）。"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    instrument_host: str = _env("INSTRUMENT_HOST", "instrument")
    instrument_port: int = int(_env("INSTRUMENT_PORT", "5025"))
    instrument_timeout: float = float(_env("INSTRUMENT_TIMEOUT", "10"))
    db_path: str = _env("DB_PATH", "/data/collector.db")
    scan_timeout: float = float(_env("SCAN_TIMEOUT_SECONDS", "3.0"))
    # 重启恢复 / 断连后仪器重同步的重试参数
    resync_interval: float = float(_env("RESYNC_INTERVAL_SECONDS", "1.0"))
    opc_wait_timeout: float = float(_env("OPC_WAIT_TIMEOUT_SECONDS", "8.0"))


settings = Settings()
