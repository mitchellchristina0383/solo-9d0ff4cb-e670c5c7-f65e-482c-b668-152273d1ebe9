#!/usr/bin/env python3
"""网关状态机单测（不启动 HTTP 服务，使用假上游客户端）。

覆盖端到端不易稳定复现的边界：
  * 异常分类：连接未建立/写超时不算“可能仍在执行”，读超时算；
  * 读超时请求从 inflight 移入 unconfirmed；
  * 上游报告 active 时排空绝不放行（即使实例 ID 已变化）；
  * 上游报告 done 后才放行；
  * 核对接口不可达时保留待核；
  * 探测成功不清除待核；
  * 上游从未登记时需经过观察窗口才判定为未执行。
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import sys
import time

import httpx

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("UPSTREAMS", "node1=http://127.0.0.1:1")
os.environ["VERIFY_UNKNOWN_GRACE"] = "0.3"
os.environ["VERIFY_RETRY_INTERVAL"] = "0.05"
os.environ["VERIFY_TIMEOUT"] = "0.3"

spec = importlib.util.spec_from_file_location(
    "gw_app", os.path.join(ROOT, "gateway", "app.py")
)
gw_app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = gw_app  # dataclass 需要模块已注册
spec.loader.exec_module(gw_app)

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


class FakeResponse:
    def __init__(self, status_code: int = 200, payload=None, request=None):
        self.status_code = status_code
        self._payload = payload
        self.request = request

    def json(self):
        return self._payload


class FakeClient:
    """按 URL 返回预设结果的假 httpx.AsyncClient。"""

    def __init__(self):
        self.verify_payload = {"instance_id": "inst-1", "active": [], "done": [], "unknown": []}
        self.verify_exc = None
        self.probe_payload = {"instance_id": "inst-1"}
        self.verify_calls = 0

    async def get(self, url, params=None, timeout=None):
        if url.endswith("/control/verify"):
            self.verify_calls += 1
            if self.verify_exc is not None:
                raise self.verify_exc
            return FakeResponse(200, self.verify_payload, request=object())
        if url.endswith("/"):
            return FakeResponse(200, self.probe_payload, request=object())
        return FakeResponse(404, {}, request=object())


async def new_gateway():
    gw = gw_app.Gateway([gw_app.NodeState("node1", "http://127.0.0.1:1")])
    gw.client = FakeClient()
    gw.nodes[0].healthy = True
    return gw


async def make_uncertain(gw, rid: str = "rid-1"):
    node = gw.nodes[0]
    async with gw.cond:
        node.inflight += 1
    await gw.finish_request(
        node,
        ok=False,
        request_id=rid,
        path="slow",
        reason="网关等待响应超时(ReadTimeout)，上游可能仍在执行",
        request_instance="inst-1",
    )
    return node


async def test_exception_classification():
    print("== 异常分类 ==")
    check("连接错误不算仍可能执行",
          gw_app._may_still_be_executing(httpx.ConnectError("x")) is False)
    check("连接超时不算仍可能执行",
          gw_app._may_still_be_executing(httpx.ConnectTimeout("x")) is False)
    check("写超时不算仍可能执行",
          gw_app._may_still_be_executing(httpx.WriteTimeout("x")) is False)
    check("读超时视为仍可能执行",
          gw_app._may_still_be_executing(httpx.ReadTimeout("x")) is True)
    check("传输错误视为仍可能执行",
          gw_app._may_still_be_executing(httpx.RemoteProtocolError("x")) is True)


async def test_uncertain_and_verify():
    print("== 读超时进入待核，active 优先 ==")
    gw = await new_gateway()
    node = await make_uncertain(gw)
    check("网关在途归零", node.inflight == 0)
    check("待核记录已登记", list(node.uncertain) == ["rid-1"])
    check("待核时不 safe_to_stop", node.snapshot()["safe_to_stop"] is False)

    # 上游报告仍在执行：即使实例 ID 变化，也不得放行
    gw.client.verify_payload = {
        "instance_id": "inst-2",
        "active": ["rid-1"],
        "done": [],
        "unknown": [],
    }
    result = await gw.drain(node, timeout=0.4)
    check("active 时排空未确认", result.drained is False)
    check("active 时待核保留", len(node.uncertain) == 1)
    check("active 时 safe_to_stop=false", node.snapshot()["safe_to_stop"] is False)
    check("verify 被重试", gw.client.verify_calls >= 1, str(gw.client.verify_calls))

    # 上游报告已结束 -> 放行
    gw.client.verify_payload = {
        "instance_id": "inst-2",
        "active": [],
        "done": ["rid-1"],
        "unknown": [],
    }
    result = await gw.drain(node, timeout=1.0)
    check("done 后确认完成", result.drained is True and result.reason == "confirmed")
    check("done 后待核清空", node.uncertain == {})
    check("确认后可停机", node.snapshot()["safe_to_stop"] is True)


async def test_verify_unreachable_keeps_pending():
    print("== 核对不可达维持待核 ==")
    gw = await new_gateway()
    node = await make_uncertain(gw, "rid-2")
    gw.client.verify_exc = httpx.ConnectError("refused")
    result = await gw.drain(node, timeout=0.4)
    check("核对不可达时排空未确认", result.drained is False)
    check("核对不可达时保留待核", len(node.uncertain) == 1)
    check("记录核对失败原因", node.last_verify_ok is False and node.last_verify_detail)
    check("不可达时不 safe_to_stop", node.snapshot()["safe_to_stop"] is False)

    # 探测成功不得清除待核
    gw.client.verify_exc = None
    await gw.probe_one(node)
    check("探测成功但待核仍在", len(node.uncertain) == 1)
    check("探测成功但健康位已更新", node.healthy is True)
    check("探测不改 drain_verified", node.drain_verified is False)

    # 核对恢复且上游报告 done -> 可继续确认
    gw.client.verify_payload = {
        "instance_id": "inst-1", "active": [], "done": ["rid-2"], "unknown": []
    }
    result = await gw.drain(node, timeout=1.0)
    check("上游恢复后可继续确认", result.drained is True)


async def test_unknown_grace():
    print("== 上游从未登记的观察窗口 ==")
    gw = await new_gateway()
    node = await make_uncertain(gw, "rid-3")
    gw.client.verify_payload = {
        "instance_id": "inst-1", "active": [], "done": [], "unknown": ["rid-3"]
    }
    result = await gw.drain(node, timeout=0.15)
    check("观察窗口内不确认", result.drained is False and len(node.uncertain) == 1)
    # 等过观察窗口 + 放宽等待
    await asyncio.sleep(0.35)
    result = await gw.drain(node, timeout=1.0)
    check("超过观察窗口后确认未执行", result.drained is True and not node.uncertain)


async def test_drain_waits_done_not_uncertain():
    print("== 网关在途与待核均需结束 ==")
    gw = await new_gateway()
    node = gw.nodes[0]
    async with gw.cond:
        node.inflight = 1
    task = asyncio.create_task(gw.drain(node, timeout=2.0))
    await asyncio.sleep(0.15)
    check("在途未结束时排空等待", not task.done())
    await gw.finish_request(
        node,
        ok=False,
        request_id="rid-4",
        path="slow",
        reason="网关任务被取消，上游可能仍在执行",
    )
    # 上游尚未结束
    gw.client.verify_payload = {
        "instance_id": "inst-1", "active": ["rid-4"], "done": [], "unknown": []
    }
    await asyncio.sleep(0.2)
    check("在途归零但待核仍在时不放行", not task.done())
    # 上游结束
    gw.client.verify_payload = {
        "instance_id": "inst-1", "active": [], "done": ["rid-4"], "unknown": []
    }
    result = await asyncio.wait_for(task, timeout=2.0)
    check("两项都结束后才确认", result.drained is True)


async def test_cancelled_request_is_settled():
    print("== 请求被取消也会结算 ==")
    gw = await new_gateway()
    node = gw.nodes[0]
    async with gw.cond:
        node.inflight = 1

    async def cancelled_proxy():
        try:
            await asyncio.sleep(10)
        finally:
            await gw.settle_request(
                node,
                ok=False,
                request_id="rid-cancelled",
                path="slow",
                reason="网关任务被取消，上游可能仍在执行",
            )

    task = asyncio.create_task(cancelled_proxy())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await asyncio.sleep(0)
    check("取消后在途归零", node.inflight == 0)
    check("取消后登记待核", list(node.uncertain) == ["rid-cancelled"])
    check("取消后不得给出停机回执", node.snapshot()["safe_to_stop"] is False)


async def main() -> int:
    await test_exception_classification()
    await test_uncertain_and_verify()
    await test_verify_unreachable_keeps_pending()
    await test_unknown_grace()
    await test_drain_waits_done_not_uncertain()
    await test_cancelled_request_is_settled()
    print(f"\n==== 结果: {passed} passed, {failed} failed ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
