"""采集 API 端到端测试（真实 TCP 仪器 + 真实 SQLite + ASGI 测试客户端）。

所有异步 SCPI 辅助调用经 portal.call 提交到应用所在事件循环。
"""

from __future__ import annotations

import asyncio
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


def test_cancel_winning_publish_race_discards_readings(api_components):
    """取数结束、保存前取消先获准：批次不得完成，读数不得落库也不得被查询到。"""
    from app.schemas import BatchStatus

    portal, settings, db, scpi, coordinator = api_components

    async def scenario():
        await scpi.set_delay(0.05)
        await db.connect()
        await coordinator.startup()
        try:
            original_complete = db.complete_batch
            cancel_outcome = {}

            async def cancel_wins(batch_id, channels, values):
                # 模拟“取数结束、保存前取消”的临界交错：取消先提交
                cancel_outcome.update(await coordinator.request_cancel(batch_id))
                return await original_complete(batch_id, channels, values)

            db.complete_batch = cancel_wins
            try:
                await db.create_batch("B-RACE-CANCEL", [101, 102])
                await coordinator.enqueue("B-RACE-CANCEL")
                row = await _wait_db(db, "B-RACE-CANCEL", {"CANCELLED"})
            finally:
                db.complete_batch = original_complete

            # 取消返回已受理后，批次必须停在 CANCELLED，不得随后完成
            assert cancel_outcome["accepted"] is True
            assert cancel_outcome["status"] == BatchStatus.CANCELLING
            assert row["status"] == "CANCELLED"
            # 不再发布：批次读数与通道历史都供不出该批读数
            assert await db.get_readings_for_batch("B-RACE-CANCEL") == []
            assert await db.get_readings_for_channel(101) == []
            assert await db.get_readings_for_channel(102) == []
        finally:
            await coordinator.shutdown()
            await db.close()

    portal.call(scenario)


def test_publish_winning_race_makes_cancel_refused(api_components):
    """完成（含完整读数）先提交：取消必须被告知不可取消，读数完整可查。"""
    from app.schemas import BatchStatus

    portal, settings, db, scpi, coordinator = api_components

    async def scenario():
        await scpi.set_delay(0.05)
        await db.connect()
        await coordinator.startup()
        try:
            original_complete = db.complete_batch
            cancel_outcome = {}
            cancel_done = asyncio.Event()

            async def publish_wins(batch_id, channels, values):
                published = await original_complete(batch_id, channels, values)
                assert published is True
                # 完成已提交，紧随其后的取消必须被拒
                cancel_outcome.update(await coordinator.request_cancel(batch_id))
                cancel_done.set()
                return published

            db.complete_batch = publish_wins
            try:
                await db.create_batch("B-RACE-DONE", [103, 104])
                await coordinator.enqueue("B-RACE-DONE")
                await _wait_db(db, "B-RACE-DONE", {"COMPLETED"})
                # 完成登记先于包装器里的取消调用，等取消结论落地再断言
                await asyncio.wait_for(cancel_done.wait(), timeout=5.0)
            finally:
                db.complete_batch = original_complete

            assert cancel_outcome["accepted"] is False
            assert cancel_outcome["status"] == BatchStatus.COMPLETED
            assert "不可取消" in cancel_outcome["message"]
            readings = await db.get_readings_for_batch("B-RACE-DONE")
            assert [r["channel"] for r in readings] == [103, 104]
            assert len(await db.get_readings_for_channel(103)) == 1
        finally:
            await coordinator.shutdown()
            await db.close()

    portal.call(scenario)


def test_queued_cancel_racing_worker_start_never_runs(api_components):
    """worker 刚取出批次、尚未置 RUNNING 时取消先提交：批次不得执行。"""
    from app.schemas import BatchStatus

    portal, settings, db, scpi, coordinator = api_components

    async def scenario():
        await db.connect()
        await coordinator.startup()
        try:
            original_transition = db.try_transition
            injected = False

            async def cancel_slips_in(batch_id, from_states, to, **kwargs):
                nonlocal injected
                if not injected and to == BatchStatus.RUNNING:
                    injected = True
                    # 取消抢在 worker 置 RUNNING 前提交
                    assert await original_transition(
                        batch_id, {BatchStatus.QUEUED}, BatchStatus.CANCELLED,
                        finished=True)
                return await original_transition(
                    batch_id, from_states, to, **kwargs)

            db.try_transition = cancel_slips_in
            try:
                await db.create_batch("B-RACE-QUEUED", [101])
                await coordinator.enqueue("B-RACE-QUEUED")
                row = await _wait_db(db, "B-RACE-QUEUED", {"CANCELLED"})
            finally:
                db.try_transition = original_transition

            assert injected  # 确实走到了临界交错
            assert row["started_at"] is None  # 从未进入执行
            assert await db.get_readings_for_batch("B-RACE-QUEUED") == []
        finally:
            await coordinator.shutdown()
            await db.close()

    portal.call(scenario)


def test_restart_resume_clears_remnants_and_publishes_once(api_components):
    """旧版残件现场（读数已落库、批次未完成）重启后：

    - 中断/未完成批次的残件读数，批次与通道查询都不可见；
    - 接续重扫后每个所选通道恰好一条本次读数（残件被清除而非叠加）；
    - 已完成批次及其读数保持不变。
    """
    from app.main import AppState
    from app.schemas import BatchStatus

    portal, settings, db, scpi, coordinator = api_components

    async def scenario():
        await scpi.set_delay(0.05)
        await db.connect()
        await coordinator.startup()
        try:
            # 正常完成一个批次作对照
            await db.create_batch("B-OK", [101])
            await coordinator.enqueue("B-OK")
            await _wait_db(db, "B-OK", {"COMPLETED"})

            # 构造旧版崩溃现场：批次 RUNNING，两条读数已落库但未登记完成
            await db.create_batch("B-LEGACY", [101, 102])
            assert await db.try_transition(
                "B-LEGACY", {BatchStatus.QUEUED}, BatchStatus.RUNNING,
                started=True)
            await db.db.execute(
                "INSERT INTO readings "
                "(batch_id, channel, position, value, recorded_at) VALUES "
                "('B-LEGACY', 101, 0, 9.999, '2000-01-01T00:00:00+00:00'), "
                "('B-LEGACY', 102, 1, 8.888, '2000-01-01T00:00:00+00:00')")

            # 未完成批次的残件读数：批次与通道查询都不可见
            assert await db.get_readings_for_batch("B-LEGACY") == []
            ch101 = await db.get_readings_for_channel(101)
            assert [r["batch_id"] for r in ch101] == ["B-OK"]
        finally:
            await coordinator.shutdown()
            await db.close()

        # 模拟重启：同一 DB 文件全新装配
        new_state = AppState(settings)
        await new_state.db.connect()
        interrupted = await new_state.coordinator.startup()
        try:
            assert "B-LEGACY" in interrupted
            # 中断状态下残件仍不可见
            assert await new_state.db.get_readings_for_batch("B-LEGACY") == []
            ch101 = await new_state.db.get_readings_for_channel(101)
            assert [r["batch_id"] for r in ch101] == ["B-OK"]

            # 接续：重扫后每通道恰好一条本次读数，残件被清除而非叠加
            assert await new_state.coordinator.resume("B-LEGACY") is True
            await _wait_db(new_state.db, "B-LEGACY", {"COMPLETED"})
            readings = await new_state.db.get_readings_for_batch("B-LEGACY")
            assert [r["channel"] for r in readings] == [101, 102]
            assert len(readings) == 2
            assert all(r["value"] not in (9.999, 8.888) for r in readings)
            # 通道 101 历史恰为两条（B-OK 与 B-LEGACY 各一），残件未叠加
            ch101 = await new_state.db.get_readings_for_channel(101)
            assert sorted(r["batch_id"] for r in ch101) == ["B-LEGACY", "B-OK"]
            # 已完成批次保持不变
            assert len(await new_state.db.get_readings_for_batch("B-OK")) == 1
        finally:
            await new_state.coordinator.shutdown()
            await new_state.db.close()

    portal.call(scenario)


def test_cancel_and_status_do_not_wait_for_scan(api_client):
    """取消受理与状态查询不等待仪器扫描结束。"""
    client, _app, _port, scpi, portal = api_client
    # 3 通道 × 0.8s ≈ 2.4s 扫描（低于 3 秒超时）
    portal.call(scpi.set_delay, 0.8)

    batch = client.post("/batches",
                        json={"channels": [101, 102, 103]}).json()
    _wait_status(client, batch["id"], {"RUNNING"})

    t0 = time.monotonic()
    status = client.get(f"/batches/{batch['id']}").json()
    assert time.monotonic() - t0 < 1.5
    assert status["status"] == "RUNNING"

    t0 = time.monotonic()
    result = client.post(f"/batches/{batch['id']}/cancel").json()
    cancel_elapsed = time.monotonic() - t0
    assert result["accepted"] is True
    # 扫描还需约 2 秒才结束，受理必须远早于此
    assert cancel_elapsed < 1.5

    _wait_status(client, batch["id"], {"CANCELLED"})
    assert client.get(
        f"/batches/{batch['id']}/readings").json()["readings"] == []
    portal.call(scpi.set_delay, 0.02)
