"""模拟上游服务：可产生慢请求、可被手动置为故障模式。

业务接口:
  GET  /slow?seconds=N            延迟 N 秒后返回（慢请求）
  GET  /echo                      回显请求信息
  GET  /flaky                     按当前模式返回 200 或 500
  GET  /                          健康信息（含实例 ID，实例 ID 每次进程启动都不同）

管理/核对接口（仅用于测试/演练，不经过网关）:
  GET  /control/state                     查看当前模式、在途请求与实例 ID
  POST /control/mode?mode=...             切换模式: healthy | slow | error | hang
  GET  /control/verify?ids=a,b,c          核对请求是否仍在执行：
        {"instance_id": ..., "active": [...], "done": [...], "unknown": [...]}
        active  仍在本实例上执行
        done    已在本实例上执行结束
        unknown 本实例从未收到过（含尚在网络途中）
  POST /control/verify_fail?enabled=true  测试钩子：让核对接口返回 503，
                                          但 / 健康端点与业务接口保持正常，
                                          用于验证“探测成功不抹去待核状态”

网关转发请求会带上 X-Gateway-Request-Id 头；本服务据此登记请求生命周期。
"""
from __future__ import annotations

import asyncio
import time
import uuid

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

# 节点运行模式（演示用的全局状态）
_mode = "healthy"
_inflight = 0
_started = time.time()
_node_id = "unknown"
# 每次进程启动生成新实例 ID：网关据此识别“上游已重启，旧请求必然结束”
_instance_id = uuid.uuid4().hex

# 网关请求 ID -> 开始时间 / 结束时间（单事件循环内字典操作是原子的）
_active: dict[str, float] = {}
_done: dict[str, float] = {}
_DONE_TTL = 600.0
# 测试钩子：核对接口故障，但健康/业务链路正常
_verify_fail = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    import os

    global _node_id
    _node_id = os.environ.get("NODE_ID", "unknown")
    yield


app = FastAPI(title="mock-upstream", version="1.1.0", lifespan=lifespan)


def _prune_done(now: float) -> None:
    if len(_done) < 10000:
        return
    stale = [rid for rid, ended in _done.items() if now - ended > _DONE_TTL]
    for rid in stale:
        _done.pop(rid, None)


@app.middleware("http")
async def track_request_lifecycle(request: Request, call_next):
    """登记网关带来的请求 ID：处理期间为 active，处理结束移入 done。"""
    rid = request.headers.get("x-gateway-request-id")
    if rid:
        _active[rid] = time.time()
    try:
        return await call_next(request)
    finally:
        if rid:
            _active.pop(rid, None)
            now = time.time()
            _done[rid] = now
            _prune_done(now)


@app.get("/")
async def root() -> JSONResponse:
    """健康端点：如实反映节点模式。

    error -> 503（探测据此判定节点不健康）；hang -> 延迟（探测超时判定）；
    其余 -> 200。注意控制面状态请查 /control/state，该接口不受模式影响。
    """
    if _mode == "error":
        return JSONResponse(
            status_code=503,
            content={"node": _node_id, "instance_id": _instance_id, "mode": _mode,
                     "inflight": _inflight, "error": "upstream in error mode"},
        )
    if _mode == "hang":
        await asyncio.sleep(120)
    return JSONResponse(
        status_code=200,
        content={
            "node": _node_id,
            "instance_id": _instance_id,
            "mode": _mode,
            "inflight": _inflight,
            "uptime": round(time.time() - _started, 2),
        },
    )


@app.get("/echo")
async def echo(request: Request) -> dict:
    return {
        "node": _node_id,
        "path": "/echo",
        "query_params": dict(request.query_params),
        "headers": {k: v for k, v in request.headers.items()},
    }


@app.get("/slow")
async def slow(seconds: float = Query(2.0, ge=0, le=60)) -> JSONResponse:
    """慢请求：用于验证排空期间在途请求不被截断。"""
    global _inflight
    _inflight += 1
    started = time.time()
    try:
        await asyncio.sleep(seconds)
        if _mode == "error":
            return JSONResponse(
                status_code=500,
                content={"node": _node_id, "error": "upstream in error mode"},
            )
        return JSONResponse(
            status_code=200,
            content={
                "node": _node_id,
                "path": "/slow",
                "slept": round(time.time() - started, 3),
                "requested": seconds,
            },
        )
    finally:
        _inflight -= 1


@app.get("/flaky")
async def flaky() -> JSONResponse:
    """按模式返回：error -> 500；hang -> 挂起（由网关超时兜底）；其余 -> 200。"""
    if _mode == "error":
        return JSONResponse(
            status_code=500,
            content={"node": _node_id, "error": "upstream in error mode"},
        )
    if _mode == "hang":
        # 挂足够久，让网关的请求超时先触发
        await asyncio.sleep(120)
    return JSONResponse(status_code=200, content={"node": _node_id, "ok": True})


@app.get("/control/state")
async def control_state() -> dict:
    return {
        "node": _node_id,
        "instance_id": _instance_id,
        "mode": _mode,
        "inflight": _inflight,
        "active_request_ids": sorted(_active),
        "done_count": len(_done),
        "verify_fail": _verify_fail,
    }


@app.get("/control/verify")
async def control_verify(ids: str = Query("")) -> JSONResponse:
    """核对网关请求是否仍可能在本上游执行。"""
    if _verify_fail:
        return JSONResponse(
            status_code=503,
            content={"node": _node_id, "error": "verify unavailable (test hook)"},
        )
    now = time.time()
    _prune_done(now)
    requested = [rid for rid in ids.split(",") if rid]
    active = [rid for rid in requested if rid in _active]
    done = [rid for rid in requested if rid in _done]
    unknown = [rid for rid in requested if rid not in _active and rid not in _done]
    return JSONResponse(
        status_code=200,
        content={
            "node": _node_id,
            "instance_id": _instance_id,
            "checked_at": now,
            "active": active,
            "done": done,
            "unknown": unknown,
        },
    )


@app.post("/control/mode")
async def set_mode(mode: str = Query(..., pattern="^(healthy|slow|error|hang)$")) -> dict:
    global _mode
    old = _mode
    _mode = mode
    return {"node": _node_id, "previous_mode": old, "mode": _mode}


@app.post("/control/verify_fail")
async def set_verify_fail(enabled: bool = True) -> dict:
    global _verify_fail
    old = _verify_fail
    _verify_fail = enabled
    return {"node": _node_id, "previous": old, "verify_fail": _verify_fail}
