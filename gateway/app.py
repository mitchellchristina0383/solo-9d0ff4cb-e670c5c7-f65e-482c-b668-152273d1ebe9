"""HTTP 入口网关（FastAPI + httpx）。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志，
    也绝不抹去「待核」状态；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 区分两种“未结束”：
      1. inflight              —— 网关仍在等待响应的请求；
      2. unconfirmed/uncertain —— 网关已结束等待（读超时、响应读取错误、
         任务取消），但上游可能仍在执行该请求。
    只有 inflight 归零 **且** 上游确认这些待核请求都已不在执行，
    排空才会返回 drained=true（可停机回执）。
  * 上游不可达时保持待核，稍后恢复可继续确认；周期探测成功不改变待核状态；
  * 排空等待超时只返回“未确认”（202 / drained=false），绝不虚报完成。

上游确认协议（本仓库的 mock upstream 已实现）：
  GET /control/verify?ids=id1,id2
      -> {"instance_id": "...", "active": [...], "done": [...], "unknown": [...]}
  active 表示仍在执行（最高优先级）；done 表示已结束；unknown 表示该实例
  从未登记过，经过观察窗口后视为不会被执行。实例 ID 仅用于观测排障；
  探测成功与上游重启本身都不会清除待核状态。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))
# 向上游核对“待核请求是否仍在执行”的单次超时
VERIFY_TIMEOUT = float(os.environ.get("VERIFY_TIMEOUT", "1.5"))
# 上游不可达 / 仍有请求执行时的重试间隔
VERIFY_RETRY_INTERVAL = float(os.environ.get("VERIFY_RETRY_INTERVAL", "0.5"))
# 上游明确返回 unknown（从未登记过）时，距请求尝试超过该窗口才判定其未执行，
# 避免“请求尚在网络途中”被误判为已完成。
VERIFY_UNKNOWN_GRACE = float(os.environ.get("VERIFY_UNKNOWN_GRACE", "2.0"))

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


def _may_still_be_executing(exc: BaseException) -> bool:
    """异常发生时，请求是否仍可能在上游执行。

    * 连接未建立（连接错误 / 连接超时 / 取连接池超时 / 协议不支持）：
      请求根本没有发出，上游不可能执行；
    * 写超时：请求未完整送达，上游无法形成一次完整请求，也不会执行；
    * 读超时、响应读取错误、协议错误、其他传输错误：请求已送达上游，
      只是网关没等到（或没读完）响应，保守视为“仍可能执行”；
    * 任务取消：调用方另行保守处理。
    """
    if isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.PoolTimeout,
            httpx.UnsupportedProtocol,
            httpx.WriteTimeout,
        ),
    ):
        return False
    return True


# ----------------------------- 节点状态 -----------------------------

@dataclass
class UncertainRequest:
    """网关已不再等待、但上游可能仍在执行的请求。"""

    request_id: str
    path: str
    reason: str
    created_at: float
    # 发起请求时观测到的上游实例 ID，仅作排障参考；是否结束以核对结果为准
    instance: Optional[str] = None

    def snapshot(self, now: Optional[float] = None) -> dict:
        now = time.time() if now is None else now
        return {
            "request_id": self.request_id,
            "path": self.path,
            "reason": self.reason,
            "age_seconds": round(max(now - self.created_at, 0.0), 2),
            "instance": self.instance,
        }


@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    # 网关仍在等待响应的请求数
    inflight: int = 0
    # 网关已结束等待、但可能仍在上游执行的请求（按请求 ID 去重）
    uncertain: dict[str, UncertainRequest] = field(default_factory=dict)
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)
    # 最近观测到的上游实例 ID（探测 / 核对均可更新，但不会因此清除待核集合）
    upstream_instance: Optional[str] = None
    last_instance_seen_at: Optional[float] = None
    # 排空核对状态
    drain_started_at: Optional[float] = None
    drain_verified: bool = False
    last_drain_confirmed_at: Optional[float] = None
    last_verify_at: Optional[float] = None
    last_verify_ok: Optional[bool] = None
    last_verify_detail: str = "尚未核对"

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def snapshot(self) -> dict:
        now = time.time()
        unconfirmed = len(self.uncertain)
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "unconfirmed": unconfirmed,
            "pending_verification": unconfirmed > 0,
            "outstanding": self.inflight + unconfirmed,
            "drain_verified": self.drain_verified,
            "safe_to_stop": (
                self.draining
                and self.drain_verified
                and self.inflight == 0
                and unconfirmed == 0
            ),
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
            "upstream_instance": self.upstream_instance,
            "last_instance_seen_at": self.last_instance_seen_at,
            "drain_started_at": self.drain_started_at,
            "last_drain_confirmed_at": self.last_drain_confirmed_at,
            "last_verify_at": self.last_verify_at,
            "last_verify_ok": self.last_verify_ok,
            "last_verify_detail": self.last_verify_detail,
            "unconfirmed_requests": [
                rec.snapshot(now) for rec in list(self.uncertain.values())
            ],
        }


@dataclass
class DrainResult:
    drained: bool
    inflight: int
    unconfirmed: int
    reason: str  # confirmed | inflight | unconfirmed


class Gateway:
    def __init__(self, nodes: list[NodeState]) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        # 一把条件变量保护所有节点状态；请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(self) -> Optional[NodeState]:
        """轮询选取健康且未排空的节点，并立即登记在途计数。"""
        async with self.cond:
            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.can_serve():
                    self._rr = (idx + 1) % n
                    node.inflight += 1
                    node.rr_hits += 1
                    return node
            return None

    async def finish_request(
        self,
        node: NodeState,
        *,
        ok: bool,
        request_id: Optional[str] = None,
        path: str = "",
        reason: Optional[str] = None,
        request_instance: Optional[str] = None,
    ) -> None:
        """归还网关在途计数；若请求可能仍在上游执行，同时登记为待核。

        计数与待核登记在同一把锁内完成，排空等待者看到的永远是
        “inflight > 0”或“uncertain 中已有该请求”两者之一，不存在空窗。
        """
        async with self.cond:
            if node.inflight > 0:
                node.inflight -= 1
            if ok:
                node.success_count += 1
            else:
                node.failure_count += 1
            if request_id and reason:
                node.uncertain[request_id] = UncertainRequest(
                    request_id=request_id,
                    path=path,
                    reason=reason,
                    created_at=time.time(),
                    instance=request_instance,
                )
                # 新的“可能仍在执行”证据使此前的排空确认失效
                node.drain_verified = False
                node.last_change_at = time.time()
            # 在途归零 / 待核变化都可能让 drain 等待者满足条件
            self.cond.notify_all()

    async def settle_request(
        self,
        node: NodeState,
        *,
        ok: bool,
        request_id: str,
        path: str,
        reason: Optional[str] = None,
        request_instance: Optional[str] = None,
    ) -> None:
        """取消安全地完成一次请求结算。

        用独立任务 + shield，确保即使调用方任务已被取消，计数归还与待核登记
        也会跑完，不会出现“在途已清零但待核尚未登记”的窗口。
        """
        finisher = asyncio.ensure_future(
            self.finish_request(
                node,
                ok=ok,
                request_id=request_id,
                path=path,
                reason=reason,
                request_instance=request_instance,
            )
        )
        try:
            await asyncio.shield(finisher)
        except asyncio.CancelledError:
            # 结算任务不受外层取消影响，等它完成后再让取消继续传播
            try:
                await finisher
            except asyncio.CancelledError:
                pass
            raise

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining / uncertain。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。

        探测成功只置 healthy=True，绝不解除 draining，也绝不清除 uncertain。
        同时记录上游实例 ID，供后续核对判断“上游是否已重启”。
        """
        assert self.client is not None
        instance: Optional[str] = None
        try:
            resp = await self.client.get(
                f"{node.base_url}/", timeout=PROBE_TIMEOUT
            )
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
            try:
                payload = resp.json()
                if isinstance(payload, dict):
                    raw_instance = payload.get("instance_id")
                    if isinstance(raw_instance, str) and raw_instance:
                        instance = raw_instance
            except ValueError:
                pass
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining / node.uncertain / drain_verified
            if instance:
                node.upstream_instance = instance
                node.last_instance_seen_at = time.time()
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()
            self.cond.notify_all()

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def try_confirm_uncertain(
        self, node: NodeState, timeout: float
    ) -> bool:
        """向上游核对待核请求；返回 True 表示该节点已无待核请求。

        上游不可达 / 核对接口失败时保留全部待核记录并返回 False。
        只有明确证据才会移除记录：
          * 上游报告该请求仍在执行 -> 保留（最高优先级，任何信号不得推翻）；
          * 上游报告该请求已结束 -> 移除；
          * 上游不认识该请求，且距登记已超过观察窗口 -> 判定其不会被执行
            （覆盖上游重启后旧记录丢失、请求从未送达两种情况）。
        注意：探测成功与实例 ID 更新本身都不会清空待核集合；上游重启后
        新实例若报告某请求仍在执行，也必须继续等待。
        """
        async with self.cond:
            ids = sorted(node.uncertain)
        if not ids:
            return True
        if self.client is None:
            return False

        try:
            resp = await self.client.get(
                f"{node.base_url}/control/verify",
                params={"ids": ",".join(ids)},
                timeout=max(timeout, 0.05),
            )
            if resp.status_code != 200:
                raise httpx.HTTPStatusError(
                    f"verify HTTP {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )
            data = resp.json()
            if not isinstance(data, dict):
                raise ValueError("verify 响应不是 JSON 对象")

            reported_instance = data.get("instance_id")
            if not isinstance(reported_instance, str) or not reported_instance:
                reported_instance = None
            # 严格校验字段类型：任何非法结构都按“核对失败”处理，保留待核
            parsed: dict[str, set[str]] = {}
            for key in ("active", "done"):
                raw = data.get(key)
                if raw is None:
                    raw = []
                if not isinstance(raw, list) or not all(
                    isinstance(item, str) for item in raw
                ):
                    raise ValueError(f"verify 响应字段 {key} 非法")
                parsed[key] = set(raw)
            active, done = parsed["active"], parsed["done"]
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            async with self.cond:
                node.last_verify_at = time.time()
                node.last_verify_ok = False
                node.last_verify_detail = (
                    f"上游暂不可达，维持待核: {type(exc).__name__}: {str(exc)[:100]}"
                )
                node.last_change_at = time.time()
                self.cond.notify_all()
            return False

        now = time.time()

        async with self.cond:
            if reported_instance:
                node.upstream_instance = reported_instance
                node.last_instance_seen_at = now

            removed_done = 0
            removed_unknown = 0
            kept_active = 0
            kept_unknown = 0
            for rid in ids:
                rec = node.uncertain.get(rid)
                if rec is None:
                    continue
                # 1) 上游报告仍在执行 —— 最高优先级，任何其他信号都不能推翻
                if rid in active:
                    kept_active += 1
                    continue
                # 2) 上游报告已执行结束
                if rid in done:
                    node.uncertain.pop(rid, None)
                    removed_done += 1
                    continue
                # 3) 上游不认识该 ID（含请求尚未送达上游、上游重启后旧记录丢失）。
                #    在观察窗口内容忍“可能尚在途中 / 即将被登记”，继续待核。
                if now - rec.created_at < VERIFY_UNKNOWN_GRACE:
                    kept_unknown += 1
                    continue
                # 4) 超过观察窗口仍从未被登记，判定上游不会执行该请求
                node.uncertain.pop(rid, None)
                removed_unknown += 1

            node.last_verify_at = now
            node.last_verify_ok = True
            node.last_verify_detail = (
                f"待核 {len(ids)}；仍在执行 {kept_active}，"
                f"尚未登记 {kept_unknown}；已结束 {removed_done}，"
                f"确认未执行 {removed_unknown}"
            )
            node.last_change_at = now
            self.cond.notify_all()
            return not node.uncertain

    async def drain(
        self, node: NodeState, timeout: Optional[float]
    ) -> DrainResult:
        """置排空标志并等待“网关在途 + 上游待核”全部确认结束。

        只有 inflight==0 且 uncertain 为空才返回 drained=True；
        等待超时或上游不可达时返回 drained=False，待核状态保留，
        之后可再次调用 drain 继续确认。
        """
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            was_draining = node.draining
            node.draining = True
            # 记录本次排空开始时间；重复调用同一次排空不重置
            if not was_draining:
                node.drain_started_at = time.time()
            # 每次发起排空都必须重新核对，不能沿用旧回执
            node.drain_verified = False
            node.last_change_at = time.time()
            self.cond.notify_all()

        while True:
            # 1) 先等网关自己的在途请求结束
            async with self.cond:
                while node.inflight > 0:
                    if deadline is None:
                        await self.cond.wait()
                        continue
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        return DrainResult(
                            False, node.inflight, len(node.uncertain), "inflight"
                        )
                    try:
                        await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                    except asyncio.TimeoutError:
                        pass
                if not node.uncertain:
                    node.drain_verified = True
                    node.last_drain_confirmed_at = time.time()
                    return DrainResult(True, 0, 0, "confirmed")

            # 2) 网关已无在途，但仍有请求可能在上游执行：向上游核对
            if deadline is None:
                verify_budget = VERIFY_TIMEOUT
            else:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    async with self.cond:
                        return DrainResult(
                            False, node.inflight, len(node.uncertain), "unconfirmed"
                        )
                verify_budget = min(VERIFY_TIMEOUT, remaining)

            confirmed = await self.try_confirm_uncertain(node, verify_budget)

            async with self.cond:
                if confirmed and node.inflight == 0 and not node.uncertain:
                    node.drain_verified = True
                    node.last_drain_confirmed_at = time.time()
                    return DrainResult(True, 0, 0, "confirmed")
                pending = len(node.uncertain)
                inflight = node.inflight

            if deadline is not None and deadline - loop.time() <= 0:
                return DrainResult(False, inflight, pending, "unconfirmed")

            # 上游不可达 / 仍有请求执行：稍后重试；期间若上游恢复即可继续确认。
            # 休眠不超过剩余预算，避免明显越过调用方给定的等待上限。
            if deadline is None:
                await asyncio.sleep(VERIFY_RETRY_INTERVAL)
            else:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return DrainResult(False, inflight, pending, "unconfirmed")
                await asyncio.sleep(min(VERIFY_RETRY_INTERVAL, remaining))

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。

        注意：不清理待核请求——它们仍可能在上游执行，下一次排空仍需核对。
        探测成功后也不代表这些旧请求已经结束。
        """
        async with self.cond:
            node.draining = False
            node.drain_verified = False
            node.drain_started_at = None
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
            self.cond.notify_all()
        await self.probe_one(node)


# ----------------------------- 应用生命周期 -----------------------------

gateway = Gateway([NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)])


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass


app = FastAPI(
    title="http-entry-gateway",
    version="1.1.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游；排空时区分网关在途与"
        "上游可能仍在执行的请求，向上游确认后才给出可停机回执。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET",
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
        ],
        "semantics": {
            "inflight": "网关仍在等待响应的请求",
            "unconfirmed": "网关已超时/取消，但上游可能仍在上游执行的请求",
            "safe_to_stop": "仅在排空且上游确认无待核请求时为 true",
        },
    }


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    node = await gateway.pick_node()
    if node is None:
        return JSONResponse(
            status_code=503,
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
            },
        )

    target_url = f"{node.base_url}/{path}"
    request_id = uuid.uuid4().hex
    # 记录发起时的上游实例，供核对时识别“上游是否已重启”
    request_instance = node.upstream_instance
    ok = False
    response_received = False
    uncertain_reason: Optional[str] = None

    try:
        try:
            upstream_resp = await gateway.client.request(
                "GET",
                target_url,
                params=request.query_params,
                headers={"X-Gateway-Request-Id": request_id},
                timeout=REQUEST_TIMEOUT,
            )
            response_received = True
        except httpx.TimeoutException as exc:
            if _may_still_be_executing(exc):
                uncertain_reason = (
                    f"网关等待响应超时({type(exc).__name__})，上游可能仍在执行"
                )
            await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            return JSONResponse(
                status_code=504,
                content={
                    "error": "upstream_timeout",
                    "node": node.name,
                    "request_id": request_id,
                    "unconfirmed": uncertain_reason is not None,
                },
            )
        except httpx.RequestError as exc:
            if _may_still_be_executing(exc):
                uncertain_reason = (
                    f"网关连接异常({type(exc).__name__})，上游可能仍在执行"
                )
            await gateway.mark_unhealthy(
                node, f"{type(exc).__name__}: {str(exc)[:100]}"
            )
            return JSONResponse(
                status_code=502,
                content={
                    "error": "upstream_unreachable",
                    "node": node.name,
                    "request_id": request_id,
                    "unconfirmed": uncertain_reason is not None,
                },
            )
        except asyncio.CancelledError:
            # 客户端断开或任务被取消：响应尚未取回，上游可能仍在执行
            if not response_received:
                uncertain_reason = "网关任务被取消，上游可能仍在执行"
            raise

        # 5xx 视为上游故障：如实透传状态码并降级，绝不虚报成功
        if upstream_resp.status_code >= 500:
            await gateway.mark_unhealthy(
                node, f"上游返回 HTTP {upstream_resp.status_code}"
            )

        ok = upstream_resp.status_code < 500
        headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Gateway"] = "http-entry-gateway"
        headers["X-Upstream-Node"] = node.name
        headers["X-Gateway-Request-Id"] = request_id
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=headers,
            media_type=upstream_resp.headers.get("content-type"),
        )
    finally:
        # 无论成功、失败还是取消，在途计数都必须准确归还；
        # 可能仍在上游执行时，与计数归还在同一把锁内登记待核。
        await gateway.settle_request(
            node,
            ok=ok,
            request_id=request_id,
            path=path,
            reason=uncertain_reason,
            request_instance=request_instance,
        )


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "verify_timeout": VERIFY_TIMEOUT,
            "verify_retry_interval": VERIFY_RETRY_INTERVAL,
            "verify_unknown_grace": VERIFY_UNKNOWN_GRACE,
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求，并等待可停机条件成立。

    可停机条件 = 网关在途请求为 0 **且** 上游确认所有“网关已超时/取消”的
    请求都不再执行。只有条件成立才返回 200 + drained=true + safe_to_stop=true；
    超时、上游不可达或仍有待核请求时返回 202 + drained=false，
    节点继续保持排空，稍后可再次调用本接口继续确认。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    result = await gateway.drain(node, timeout)
    async with gateway.cond:
        snap = node.snapshot()

    if result.drained:
        detail = "上游已确认不再处理排空前的请求，可以停机"
    elif result.reason == "inflight":
        detail = (
            f"网关仍有 {result.inflight} 个在途请求未结束，"
            f"另有 {result.unconfirmed} 个待核请求；暂不可停机"
        )
    else:
        detail = (
            f"仍有 {result.unconfirmed} 个请求可能在上游执行或无法确认"
            f"（{snap['last_verify_detail']}）；暂不可停机，可再次排空以继续确认"
        )

    body = {
        "node": name,
        "draining": True,
        "drained": result.drained,
        "safe_to_stop": result.drained,
        "confirmed": result.drained,
        "inflight": result.inflight,
        "unconfirmed": result.unconfirmed,
        "pending_verification": result.unconfirmed > 0,
        "drain_verified": snap["drain_verified"],
        "upstream_instance": snap["upstream_instance"],
        "last_verify_at": snap["last_verify_at"],
        "last_verify_ok": snap["last_verify_ok"],
        "last_verify_detail": snap["last_verify_detail"],
        "unconfirmed_requests": snap["unconfirmed_requests"],
        "detail": detail,
    }
    return JSONResponse(status_code=200 if result.drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "inflight": snap["inflight"],
        "unconfirmed": snap["unconfirmed"],
        "pending_verification": snap["pending_verification"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准（待核请求仍会保留）",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变排空标志，也不会清除待核状态）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}
