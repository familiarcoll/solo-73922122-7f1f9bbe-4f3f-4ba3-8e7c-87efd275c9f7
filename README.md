# 共享扫描仪电压采集系统

实验室共享扫描仪的批次采集服务：FastAPI + SQLite 采集 API，配套本地 TCP SCPI
演示仪器，Docker Compose 一键启动两端。

## 快速启动

```bash
docker compose up --build
```

- 采集 API 与交互文档：<http://localhost:8000/docs>（Swagger UI）
- 演示入口页：<http://localhost:8000/>
- 仪器 SCPI 直连端口（主机）：`localhost:5026`（容器内 5025）
- SQLite 数据持久化在 Docker 卷 `collector-data`（`/data/collector.db`）

不用 Docker 时的本地启动方式：

```bash
pip install -r requirements.txt
# 终端 1：仪器
python -m instrument.scpi_device
# 终端 2：API（默认连接 instrument:5025，本地覆盖主机）
INSTRUMENT_HOST=127.0.0.1 DB_PATH=./data/collector.db \
  uvicorn app.main:app --reload
```

## 核心接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches` | 按通道子集（101~104）提交批次，入队串行执行 |
| GET | `/batches` / `/batches/{id}` | 批次列表 / 单批次状态 |
| POST | `/batches/{id}/cancel` | 取消，立即受理；已完成返回不可取消 |
| POST | `/batches/{id}/resume` | 重启后接续 INTERRUPTED 批次，已有读数保留 |
| GET | `/batches/{id}/readings` | 该批次各通道完整读数（扫描顺序） |
| GET | `/channels/{101..104}/readings` | 某通道跨批次完整读数 |
| GET | `/health` | 服务与仪器同步状态、队列深度 |

批次状态机：`QUEUED → RUNNING → COMPLETED`；取消为
`QUEUED/RUNNING → CANCELLING → CANCELLED`；超时/通信故障为 `FAILED`；
服务重启时未完成批次标记 `INTERRUPTED`。

## 关键语义（与需求对应）

- **串行不交叉**：单 worker 加仪器锁，任何时刻只有一个批次配置 `ROUT:SCAN`
  或执行 `READ?`，并发批次排队，绝不交叉配置或取数。
- **共享扫描表 / 裸读数**：仪器扫描表全机共享；`READ?` 按扫描表顺序返回
  不带批次号的电压读数，由 API 侧按批次绑定通道落库，避免迟到读数串批。
- **取消立即受理**：排队批次直接 `CANCELLED` 且不再触发仪器；执行中批次先
  `CANCELLING`，经独立连接发 `ABORt`，**确认仪器停止（`*OPC?`）并清掉迟到
  响应（`*CLS`）后**才置 `CANCELLED` 并允许下一批，读数不落库。
- **3 秒扫描超时**：扫描满 `SCAN_TIMEOUT_SECONDS`（默认 3s）未取齐读数即
  超时失败，取消取数、`ABORt`、确认停机、清迟到响应，读数不保存。
- **断连不终止扫描**：仪器侧客户端断开只把最终响应转入迟到缓冲，扫描继续；
  API 侧重连后通过 `ABORt → *OPC? → *RST` 重同步，期间状态查询照常可用。
- **重启接续**：启动时把所有未完成批次标 `INTERRUPTED`，完成仪器重同步后
  可 `resume` 重新入队；已完成批次的读数原样保留。

## 演示仪器 SCPI 命令

| 命令 | 说明 |
| --- | --- |
| `*IDN?` | 识别串 |
| `ROUT:SCAN 101,103` | 设置全机共享扫描表（101~104 子集） |
| `ROUT:SCAN?` | 查询扫描表 |
| `READ?` | 启动扫描，按扫描顺序逐行返回裸电压；扫描中再发返回 -410 |
| `ABORt` | 立即中止当前扫描，返回 1 |
| `*OPC?` | 扫描结束后才回 1（可用于确认停机） |
| `*RST` / `*CLS` | 复位（停机+清表+清迟到响应）/ 仅清迟到响应 |
| `SYST:DELAY 1.0` | 每通道驻留秒数，制造慢响应（0~60） |
| `SYST:DISC` | 回 1 后断开全部客户端连接，监听与扫描保持（断连演示） |

主机直连示例（容器外执行，端口映射为 5026）：

```bash
# 慢响应：让后续扫描每通道停 1 秒（用于演示 3 秒超时）
python -m scripts.scpi_cli localhost 5026 'SYST:DELAY 1.0'
# 断连演示
python -m scripts.scpi_cli localhost 5026 'SYST:DISC'
# 查询类命令直接打印响应
python -m scripts.scpi_cli localhost 5026 '*IDN?' 'ROUT:SCAN?'
```

（如本机装有 `nc`，等价写法：`(printf 'SYST:DELAY 1.0\n'; sleep 0.2) | nc localhost 5026`）

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

17 项测试覆盖：SCPI 协议（共享扫描表、并发 READ? 拒绝、ABORt、客户端断连
不终止扫描、迟到缓冲、`SYST:DISC`）与 API 端到端（批次生命周期、通道历史、
并发不交叉、排队/执行中取消、3 秒超时、断连重同步恢复、重启中断与接续）。

## 目录结构

```
app/                 FastAPI 采集服务（main/coordinator/database/scpi_client）
instrument/          本地 TCP SCPI 演示仪器
tests/               协议测试与端到端集成测试
Dockerfile.api / Dockerfile.instrument / docker-compose.yml
```
