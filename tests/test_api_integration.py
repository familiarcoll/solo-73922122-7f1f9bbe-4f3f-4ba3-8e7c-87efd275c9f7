"""采集 API 端到端测试（真实 TCP 仪器 + 真实 SQLite + ASGI 测试客户端）。

所有异步 SCPI 辅助调用经 portal.call 提交到应用所在事件循环。
"""

from __future__ import annotations

import time


def _wait_status(client, batch_id, targets, timeout=10.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = client.get(f"/batches/{batch_id}").json()
        if last["status"] in targets:
            return last
        time.sleep(0.05)
    raise AssertionError(
        f"批次 {batch_id} 未在 {timeout}s 内到达 {targets}，最后状态 {last}")


def test_health_and_docs(api_client):
    client, _app, _port, _scpi, _portal = api_client
    # 启动重同步在后台进行，轮询等待首次同步完成
    deadline = time.time() + 10
    health = None
    while time.time() < deadline:
        health = client.get("/health").json()
        if health["instrument_synced"]:
            break
        time.sleep(0.05)
    assert health is not None and health["status"] == "ok"
    assert health["instrument_synced"] is True
    assert client.get("/docs").status_code == 200
    assert client.get("/").status_code == 200


def test_validation_rejects_bad_channels(api_client):
    client, *_ = api_client
    assert client.post("/batches", json={"channels": [999]}).status_code == 422
    assert client.post("/batches", json={"channels": [101, 101]}).status_code == 422
    assert client.post("/batches", json={"channels": []}).status_code == 422


def test_batch_lifecycle_and_channel_history(api_client):
    client, _app, _port, _scpi, _portal = api_client
    resp = client.post("/batches", json={"channels": [104, 102, 101]})
    assert resp.status_code == 201
    batch = resp.json()
    assert batch["status"] == "QUEUED"

    final = _wait_status(client, batch["id"], {"COMPLETED"})
    assert final["error"] is None

    readings = client.get(f"/batches/{batch['id']}/readings").json()
    assert readings["status"] == "COMPLETED"
    assert [r["channel"] for r in readings["readings"]] == [104, 102, 101]
    assert all(isinstance(r["value"], float) for r in readings["readings"])
    # 读数为裸电压，不含批次号
    assert all("B" not in str(r["value"]) for r in readings["readings"])

    # 通道完整读数（跨批次）
    history = client.get("/channels/102/readings").json()
    assert len(history) >= 1
    assert all(r["channel"] == 102 for r in history)
    assert client.get("/channels/999/readings").status_code == 422

    # 已完成批次不可取消
    cancel = client.post(f"/batches/{batch['id']}/cancel").json()
    assert cancel["accepted"] is False
    assert "不可取消" in cancel["message"]


def test_concurrent_batches_do_not_interleave(api_client):
    client, _app, _port, _scpi, _portal = api_client
    ids = []
    for chans in ([101], [102], [103], [104], [101, 102, 103]):
        resp = client.post("/batches", json={"channels": chans})
        assert resp.status_code == 201
        ids.append((resp.json()["id"], chans))

    for batch_id, chans in ids:
        _wait_status(client, batch_id, {"COMPLETED"})
        readings = client.get(f"/batches/{batch_id}/readings").json()
        assert [r["channel"] for r in readings["readings"]] == chans
        assert len(readings["readings"]) == len(chans)


def test_cancel_queued_batch_never_touches_instrument(api_client):
    client, _app, _port, scpi, portal = api_client
    portal.call(scpi.set_delay, 0.4)

    first = client.post("/batches", json={"channels": [101, 102]}).json()
    second = client.post("/batches", json={"channels": [103]}).json()
    _wait_status(client, first["id"], {"RUNNING"})

    # second 仍在排队，取消应立即受理且不会触发仪器
    result = client.post(f"/batches/{second['id']}/cancel").json()
    assert result["accepted"] is True
    assert result["status"] == "CANCELLED"
    assert client.get(f"/batches/{second['id']}").json()["status"] == "CANCELLED"
    assert client.get(
        f"/batches/{second['id']}/readings").json()["readings"] == []

    _wait_status(client, first["id"], {"COMPLETED"})
    assert client.get(
        f"/batches/{second['id']}").json()["status"] == "CANCELLED"

    portal.call(scpi.set_delay, 0.02)


def test_cancel_running_batch_discards_readings(api_client):
    client, _app, _port, scpi, portal = api_client
    portal.call(scpi.set_delay, 0.5)

    batch = client.post("/batches",
                        json={"channels": [101, 102, 103, 104]}).json()
    _wait_status(client, batch["id"], {"RUNNING"})

    result = client.post(f"/batches/{batch['id']}/cancel").json()
    assert result["accepted"] is True
    assert result["status"] == "CANCELLING"

    final = _wait_status(client, batch["id"], {"CANCELLED"})
    assert final["status"] == "CANCELLED"
    assert client.get(
        f"/batches/{batch['id']}/readings").json()["readings"] == []

    # 下一批可正常执行——证明已确认仪器停止并清掉迟到响应
    nxt = client.post("/batches", json={"channels": [101]}).json()
    _wait_status(client, nxt["id"], {"COMPLETED"})
    nxt_readings = client.get(f"/batches/{nxt['id']}/readings").json()
    assert len(nxt_readings["readings"]) == 1

    portal.call(scpi.set_delay, 0.02)


def test_scan_timeout_saves_nothing_and_next_batch_runs(api_client):
    client, _app, _port, scpi, portal = api_client
    # 4 通道 × 1.2s = 4.8s，超过 3 秒扫描超时
    portal.call(scpi.set_delay, 1.2)

    batch = client.post("/batches",
                        json={"channels": [101, 102, 103, 104]}).json()
    final = _wait_status(client, batch["id"], {"FAILED"}, timeout=8.0)
    assert "超时" in (final["error"] or "")
    assert client.get(
        f"/batches/{batch['id']}/readings").json()["readings"] == []

    # 仪器已被 ABORt 并清迟到响应：恢复正常驻留后下一批成功
    portal.call(scpi.set_delay, 0.02)
    nxt = client.post("/batches", json={"channels": [102, 103]}).json()
    _wait_status(client, nxt["id"], {"COMPLETED"}, timeout=8.0)
    nxt_readings = client.get(f"/batches/{nxt['id']}/readings").json()
    assert len(nxt_readings["readings"]) == 2


def test_instrument_disconnect_triggers_resync_then_recovers(api_client):
    client, _app, _port, scpi, portal = api_client
    # 演示断连：仪器关闭所有客户端连接，监听保持
    portal.call(scpi.disconnect_clients)

    # 提交批次：worker 重同步期间状态查询仍可用，批次先排队
    batch = client.post("/batches", json={"channels": [101, 104]}).json()
    assert client.get("/health").status_code == 200  # 状态查询不受影响

    final = _wait_status(client, batch["id"], {"COMPLETED"}, timeout=20.0)
    assert final["status"] == "COMPLETED"
    readings = client.get(f"/batches/{batch['id']}/readings").json()
    assert [r["channel"] for r in readings["readings"]] == [101, 104]
    assert client.get("/health").json()["instrument_synced"] is True


async def _wait_db(db, batch_id, targets, timeout=10.0):
    import asyncio

    deadline = time.time() + timeout
    while time.time() < deadline:
        row = await db.get_batch(batch_id)
        if row and row["status"] in targets:
            return row
        await asyncio.sleep(0.05)
    raise AssertionError(f"批次 {batch_id} 未到达 {targets}")


def test_restart_marks_interrupted_and_resume_keeps_completed(api_components):
    """模拟服务重启：同一 DB 文件上先后两个 Coordinator 实例。"""
    from app.main import AppState

    portal, settings, db, scpi, coordinator = api_components

    async def scenario():
        await scpi.set_delay(0.3)
        await db.connect()
        await coordinator.startup()
        try:
            # 1) 完成一个批次，落库读数
            await db.create_batch("B-DONE", [101, 102])
            await coordinator.enqueue("B-DONE")
            await _wait_db(db, "B-DONE", {"COMPLETED"})
            kept = await db.get_readings_for_batch("B-DONE")
            assert len(kept) == 2

            # 2) 再造一个未完成批次（慢驻留保证仍在 QUEUED/RUNNING）
            await db.create_batch("B-PENDING", [103])
            await coordinator.enqueue("B-PENDING")
            await _wait_db(db, "B-PENDING", {"QUEUED", "RUNNING"})
        finally:
            # 服务停止
            await coordinator.shutdown()
            await db.close()

        # 3) 全新进程式装配：同一 DB 文件、同一仪器
        new_state = AppState(settings)
        await new_state.db.connect()
        interrupted = await new_state.coordinator.startup()
        try:
            assert "B-PENDING" in interrupted
            row = await new_state.db.get_batch("B-PENDING")
            assert row["status"] == "INTERRUPTED"

            # 已完成批次读数原样保留
            kept = await new_state.db.get_readings_for_batch("B-DONE")
            assert len(kept) == 2

            # 4) 接续采集
            assert await new_state.coordinator.resume("B-PENDING") is True
            row = await _wait_db(new_state.db, "B-PENDING", {"COMPLETED"})
            resumed = await new_state.db.get_readings_for_batch("B-PENDING")
            assert [r["channel"] for r in resumed] == [103]
            assert row["attempts"] == 2
        finally:
            await new_state.coordinator.shutdown()
            await new_state.db.close()
            await scpi.set_delay(0.02)

    portal.call(scenario)
