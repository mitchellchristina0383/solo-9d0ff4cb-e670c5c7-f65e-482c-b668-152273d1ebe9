#!/usr/bin/env python3
"""网关本地自测（不依赖 docker，使用 stdlib + 三个 uvicorn 子进程）。

覆盖关键场景：
  1.  轮询分发
  2.  状态 API
  3.  空闲节点排空 -> 立即 drained=true
  4.  排空后新请求不进入该节点
  5.  有在途慢请求时排空 -> 等在途结束才完成，且原响应可取回
  6.  全部不可接流 -> 503
  7.  上游 5xx -> 不虚报成功、节点降级、在途归零
  8.  探测成功不解除排空（交错核心）
  9.  恢复不绕过故障探测（交错核心）
  10. 仅 GET：POST 转发路径返回 405
  11. /docs 可打开
  12. 排空等待超时 -> 202，在途数准确
  13. 上游挂起 -> 网关请求超时 504，不虚报成功、在途归零
  14. 多并发在途请求排空
  15. 未知节点 404
  16. 读超时请求不再计入 inflight，但进入“待核”；探测成功不抹去待核，
      上游报告完成后才给出可停机回执；其他健康节点继续接流
  17. 待核期间上游不可达 -> 维持待核；上游恢复后可继续确认并给出回执
  18. 上游重启后旧请求经观察窗口确认不再执行
  19. 调用方先放弃等待时，请求按网关自身超时结算并进入待核，
      不得给出可停机回执（任务取消路径由 state_test 覆盖）
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable

GW_PORT = 19080
U1_PORT = 19081
U2_PORT = 19082

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


def http(method: str, url: str, timeout: float = 20.0, data: bytes | None = None):
    """返回 (status, headers, body_text)。4xx/5xx 不抛异常。"""
    req = urllib.request.Request(url, method=method, data=data)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read().decode()


def jget(url: str, timeout: float = 20.0):
    status, _, body = http("GET", url, timeout)
    return status, json.loads(body)


def wait_ready(url: str, timeout: float = 25.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, _, _ = http("GET", url, timeout=2)
            if status == 200:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def spawn(module_dir: str, port: int, extra_env: dict | None = None):
    # -I 隔离模式会忽略 PYTHONPATH 且不含 cwd，故用 runpy 包装器显式注入依赖与应用目录
    launcher = (
        "import sys,runpy,os;"
        f"sys.path[:0]=[{PYLIBS!r}, os.getcwd()];"
        "sys.argv=['uvicorn']+sys.argv[1:];"
        "runpy.run_module('uvicorn', run_name='__main__')"
    )
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.update(extra_env or {})
    proc = subprocess.Popen(
        [PYTHON, "-I", "-c", launcher, "app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=module_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return proc


def kill(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=8)
        except Exception:
            proc.kill()


def node_status(node: str) -> dict:
    status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
    assert status == 200
    return next(n for n in body["nodes"] if n["name"] == node)


def set_mode(port: int, mode: str) -> None:
    status, _, body = http("POST", f"http://127.0.0.1:{port}/control/mode?mode={mode}")
    assert status == 200, body


def main() -> int:
    procs = []
    try:
        print("== 启动两个上游和网关 ==")
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        u2 = spawn(os.path.join(ROOT, "upstream"), U2_PORT, {"NODE_ID": "node2"})
        procs = [u1, u2]
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/")
        assert wait_ready(f"http://127.0.0.1:{U2_PORT}/")

        gw_env = {
            "UPSTREAMS": f"node1=http://127.0.0.1:{U1_PORT},node2=http://127.0.0.1:{U2_PORT}",
            "PROBE_INTERVAL": "1",
            "PROBE_TIMEOUT": "1",
            "REQUEST_TIMEOUT": "15",
            "DRAIN_DEFAULT_TIMEOUT": "10",
        }
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs"), "网关启动失败"
        time.sleep(1.2)  # 等首轮探测结果稳定

        # 1. 轮询分发
        print("== 1. 轮询分发 ==")
        seen = []
        for _ in range(4):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            assert status == 200, body
            seen.append(body["node"])
        check("4 个请求在两节点间交替", seen == ["node1", "node2", "node1", "node2"], str(seen))
        st = node_status("node1")
        check("rr_hits 各 2 次", st["rr_hits"] == 2, str(st))
        check("在途计数已归零", st["inflight"] == 0, str(st))

        # 2. 状态 API 内容
        print("== 2. 状态 API ==")
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/admin/status")
        check("两节点均健康", all(n["healthy"] for n in body["nodes"]))
        check("两节点均未排空", all(not n["draining"] for n in body["nodes"]))
        check("eligible 含两节点", sorted(body["eligible_nodes"]) == ["node1", "node2"])

        # 3. 空闲节点排空立即完成
        print("== 3. 空闲节点排空 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5")
        b = json.loads(body)
        check("排空返回 200", status == 200, body)
        check("drained=true", b.get("drained") is True, body)
        check("inflight=0", b.get("inflight") == 0, body)
        st = node_status("node1")
        check("状态显示 draining", st["draining"] is True and st["eligible"] is False)

        # 4. 排空后新请求不进入 node1
        print("== 4. 排空后新请求只去 node2 ==")
        nodes = set()
        for _ in range(3):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            nodes.add(body["node"])
        check("新请求全部落在 node2", nodes == {"node2"}, str(nodes))

        # 5. 在途慢请求 + 排空交错：排空等在途结束，原响应可取回
        print("== 5. 在途慢请求期间排空 node2 ==")
        result = {}

        def slow_call():
            result["status"], _, result["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20
            )

        t = threading.Thread(target=slow_call)
        t.start()
        # 等到请求确实进入 node2
        inflight_seen = 0
        deadline = time.time() + 5
        while time.time() < deadline:
            st2 = node_status("node2")
            inflight_seen = max(inflight_seen, st2["inflight"])
            if st2["inflight"] == 1:
                break
            time.sleep(0.1)
        check("慢请求已进入 node2 且 inflight=1", inflight_seen == 1, str(st2))

        drain_started = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=10"
        )
        drain_elapsed = time.time() - drain_started
        b = json.loads(body)
        t.join(timeout=20)
        check("排空请求被在途请求阻塞约 3s", drain_elapsed >= 2.5, f"{drain_elapsed:.2f}s")
        check("排空最终成功 drained=true", status == 200 and b.get("drained") is True, body)
        check("慢请求原响应成功取回 200", result.get("status") == 200, str(result))
        check("响应来自 node2 且内容完整",
              json.loads(result["body"]).get("node") == "node2"
              and json.loads(result["body"]).get("slept", 0) >= 2.5,
              result.get("body"))
        check("结束后两节点在途均为 0",
              all(node_status(n)["inflight"] == 0 for n in ("node1", "node2")))

        # 6. 两节点都排空 -> 503
        print("== 6. 无可用节点返回 503 ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("返回 503", status == 503, body)
        check("503 原因可解释", json.loads(body).get("error") == "no_eligible_upstream", body)

        # 7. 上游 5xx：不虚报成功 + 节点降级 + 在途归零
        print("== 7. 上游故障 (5xx) ==")
        # 先恢复 node1 以便发请求（node2 仍排空）
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 等周期性探测先发现故障
        st = node_status("node1")
        check("探测已将 node1 标记为不健康", st["healthy"] is False, str(st))
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("无健康节点时返回 503 而非虚假 200", status == 503, body)
        # 临时恢复健康标志位才能真正打到 5xx（模拟请求路径上的失败降级）
        set_mode(U1_PORT, "healthy")
        time.sleep(1.3)
        st = node_status("node1")
        check("探测恢复后 node1 健康", st["healthy"] is True, str(st))
        set_mode(U1_PORT, "error")  # 不等待探测，让请求先撞上 5xx
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky")
        check("5xx 状态如实返回", status == 500, f"status={status} body={body}")
        time.sleep(0.3)
        st = node_status("node1")
        check("请求失败后节点即时降级", st["healthy"] is False, str(st))
        check("失败后在途计数仍归零", st["inflight"] == 0, str(st))

        # 8. 交错核心：探测成功不得解除排空
        print("== 8. 探测成功不解除排空 ==")
        set_mode(U1_PORT, "healthy")
        # node1 当前未排空，先排空，再让周期性探测持续成功
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        assert status == 200
        time.sleep(2.5)  # 跨越至少 2 轮探测，上游为 healthy，探测必然成功
        st = node_status("node1")
        check("多轮探测成功后排空标志仍在", st["draining"] is True, str(st))
        check("探测成功只更新健康位", st["healthy"] is True and st["eligible"] is False, str(st))

        # 9. 交错核心：恢复不绕过故障探测
        print("== 9. 恢复不绕过故障探测 ==")
        set_mode(U1_PORT, "error")
        time.sleep(1.3)  # 探测确认故障
        st_before = node_status("node1")
        check("恢复前 node1 不健康", st_before["healthy"] is False, str(st_before))
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        b = json.loads(body)
        check("resume 清除排空但节点仍不可接流",
              status == 200 and b["draining"] is False and b["eligible"] is False, body)
        st = node_status("node1")
        check("draining=False 且 healthy=False",
              st["draining"] is False and st["healthy"] is False, str(st))
        # 故障节点上请求不会被分配：node2 仍排空 -> 503
        status, _, _ = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("故障节点不会被轮询选中 (503)", status == 503)
        # 上游恢复后经探测自动回到可接流
        set_mode(U1_PORT, "healthy")
        time.sleep(1.5)
        st = node_status("node1")
        check("探测恢复后节点重新 eligible", st["eligible"] is True, str(st))
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
        check("流量恢复进入 node1", status == 200 and body["node"] == "node1")

        # 10. 仅 GET 转发
        print("== 10. 非 GET 方法被拒 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/proxy/echo", data=b"x")
        check("POST 转发路径返回 405", status == 405, f"status={status}")

        # 11. /docs 可打开
        print("== 11. /docs 与 openapi.json ==")
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/docs")
        check("/docs 返回 200", status == 200 and "swagger" in body.lower())
        status, body = jget(f"http://127.0.0.1:{GW_PORT}/openapi.json")
        paths = body["paths"]
        check("openapi 含管理与转发接口",
              "/admin/nodes/{name}/drain" in paths and "/proxy/{path}" in paths)

        # 12. 排空等待超时：202 + 在途准确
        print("== 12. 排空超时返回 202 ==")
        result12 = {}

        def slow_call12():
            result12["status"], _, result12["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=4", timeout=15
            )

        t12 = threading.Thread(target=slow_call12)
        t12.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b = json.loads(body)
        check("短超时排空返回 202", status == 202, body)
        check("drained=false 且 inflight=1", b.get("drained") is False and b.get("inflight") == 1, body)
        t12.join(timeout=15)
        check("在途慢请求仍拿到完整 200", result12.get("status") == 200, str(result12))
        time.sleep(0.3)
        # 在途归零后再次排空（节点已在排空中）应立即完成
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3")
        b = json.loads(body)
        check("在途归零后排空立即完成", status == 200 and b["drained"] is True, body)

        # 13. 请求超时：重启网关为短超时，上游 hang（探测与业务请求都会挂住）
        print("== 13. 上游挂起 -> 网关 504 超时 ==")
        kill(gw)
        procs.remove(gw)
        short_env = dict(gw_env)
        short_env["REQUEST_TIMEOUT"] = "1"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, short_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        set_mode(U1_PORT, "healthy")
        # 等首轮探测把 node1 标记为健康可接流（网关重启后状态全清）
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 经探测恢复可接流", node_status("node1")["eligible"] is True)
        # 切 hang 后立即发请求：赶在下一轮周期探测摘掉它之前验证网关自身的请求超时
        set_mode(U1_PORT, "hang")
        t0 = time.time()
        status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
        elapsed = time.time() - t0
        if status == 503:
            # 探测抢先把节点摘掉，则手动恢复后重试一次，确保覆盖请求超时路径
            set_mode(U1_PORT, "healthy")
            deadline = time.time() + 6
            while time.time() < deadline and not node_status("node1")["eligible"]:
                time.sleep(0.2)
            set_mode(U1_PORT, "hang")
            t0 = time.time()
            status, _, body = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/flaky", timeout=8)
            elapsed = time.time() - t0
        check("挂起请求在约 1s 内返回 504", status == 504 and elapsed < 4,
              f"status={status} elapsed={elapsed:.1f}s body={body}")
        check("未虚报成功", status != 200)
        time.sleep(0.3)
        st = node_status("node1")
        check("超时后在途计数准确归零", st["inflight"] == 0, str(st))
        check("超时后节点被标记不健康", st["healthy"] is False, str(st))
        set_mode(U1_PORT, "healthy")

        # 14. 多个并发在途请求：排空须等全部结束，在途计数全程准确
        print("== 14. 多并发在途请求排空 ==")
        # 恢复正常请求超时并重启网关（清空场景 13 的短超时与节点状态）
        kill(gw)
        procs.remove(gw)
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, gw_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        check("重启后 node1 可接流", node_status("node1")["eligible"] is True)
        # 先排空 node2，把流量集中到 node1
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        assert status == 200
        results14 = []

        def slow_call14(idx: int):
            s, _, b = http("GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=3", timeout=20)
            results14.append((idx, s, b))

        threads = [threading.Thread(target=slow_call14, args=(i,)) for i in range(4)]
        for th in threads:
            th.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] < 4:
            time.sleep(0.1)
        st = node_status("node1")
        check("4 个在途请求计数准确", st["inflight"] == 4, str(st))
        t0 = time.time()
        status, _, body = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=15"
        )
        elapsed = time.time() - t0
        b = json.loads(body)
        for th in threads:
            th.join(timeout=20)
        check("排空阻塞到全部 4 个在途请求结束", elapsed >= 2.5 and b.get("drained") is True,
              f"elapsed={elapsed:.1f}s body={body}")
        check("4 个慢请求全部取回完整 200",
              len(results14) == 4 and all(s == 200 for _, s, _ in results14),
              str(results14))
        check("全部结束后在途归零", node_status("node1")["inflight"] == 0)

        # 15. 未知节点管理接口返回 404
        print("== 15. 未知节点 404 ==")
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/drain")
        check("排空未知节点返回 404", status == 404, body)
        status, _, body = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/ghost/resume")
        check("恢复未知节点返回 404", status == 404, body)

        # 16. 网关读超时 -> 在途归零但登记待核；探测成功不抹去待核；
        #     上游确认完成后才给可停机回执；期间其他健康节点继续接流。
        print("== 16. 读超时进入待核，确认后才可停机 ==")
        kill(gw)
        procs.remove(gw)
        # 短请求超时：/slow?seconds=6 会在 1s 时被网关放弃，上游仍继续执行
        verify_env = dict(gw_env)
        verify_env["REQUEST_TIMEOUT"] = "1"
        verify_env["VERIFY_RETRY_INTERVAL"] = "0.2"
        # 缩短“上游从未登记”的观察窗口，让上游重启场景可在测试时长内确认
        verify_env["VERIFY_UNKNOWN_GRACE"] = "0.5"
        gw = spawn(os.path.join(ROOT, "gateway"), GW_PORT, verify_env)
        procs.append(gw)
        assert wait_ready(f"http://127.0.0.1:{GW_PORT}/docs")
        deadline = time.time() + 8
        while time.time() < deadline and not (
            node_status("node1")["eligible"] and node_status("node2")["eligible"]
        ):
            time.sleep(0.2)
        check("重启后两节点均可接流",
              node_status("node1")["eligible"] and node_status("node2")["eligible"])

        set_mode(U1_PORT, "healthy")
        # 先排空 node2，把慢请求固定到 node1
        status, _, body16 = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5"
        )
        assert status == 200, body16

        result16 = {}

        def slow_call16():
            result16["status"], _, result16["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=6", timeout=15
            )

        t16 = threading.Thread(target=slow_call16)
        t16.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        check("慢请求已进入 node1", node_status("node1")["inflight"] == 1)
        t16.join(timeout=15)
        check("网关在 1s 读超时后返回 504", result16.get("status") == 504, str(result16))
        body16 = json.loads(result16["body"])
        check("504 响应声明请求未被确认结束",
              body16.get("unconfirmed") is True and body16.get("request_id"), str(body16))

        st = node_status("node1")
        check("超时后网关在途数归零", st["inflight"] == 0, str(st))
        check("超时请求进入待核", st["unconfirmed"] == 1, str(st))
        check("待核时不得给出可停机回执", st["safe_to_stop"] is False, str(st))

        # 探测持续成功，但上游仍在执行该慢请求 -> 必须保持待核
        for _ in range(3):
            http("POST", f"http://127.0.0.1:{GW_PORT}/admin/probe")
            time.sleep(0.4)
        st = node_status("node1")
        check("多轮探测成功后待核仍在", st["unconfirmed"] == 1 and st["healthy"] is True, str(st))
        check("探测成功不抹去待核状态", st["drain_verified"] is False, str(st))

        status, _, body16 = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b16 = json.loads(body16)
        check("上游仍在执行时排空超时返回 202", status == 202, body16)
        check("未确认时 drained/safe_to_stop 均为 false",
              b16.get("drained") is False and b16.get("safe_to_stop") is False
              and b16.get("pending_verification") is True, body16)

        # 其他健康节点继续接流：node2 已排空，故先恢复 node2
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/resume")
        assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node2")["eligible"]:
            time.sleep(0.2)
        check("node2 恢复后可接流", node_status("node2")["eligible"] is True)
        seen16 = set()
        for _ in range(2):
            status, body = jget(f"http://127.0.0.1:{GW_PORT}/proxy/echo")
            check("其他节点接流成功", status == 200, str(body))
            seen16.add(body["node"])
        check("流量全部落在健康且未排空的 node2", seen16 == {"node2"}, str(seen16))

        # 上游慢请求结束后再次排空：应确认完成并给回执
        deadline = time.time() + 15
        confirmed = None
        while time.time() < deadline:
            status, _, body16 = http(
                "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=3"
            )
            confirmed = (status, json.loads(body16))
            if status == 200:
                break
            time.sleep(0.3)
        status, b16 = confirmed
        check("上游执行结束后排空返回 200", status == 200, str(b16))
        check("回执确认可停机",
              b16.get("drained") is True and b16.get("safe_to_stop") is True
              and b16.get("unconfirmed") == 0, str(b16))
        check("确认信息包含待核明细", isinstance(b16.get("unconfirmed_requests"), list), str(b16))

        # 17. 待核期间上游不可达 -> 维持待核；恢复后继续确认
        print("== 17. 上游不可达维持待核，恢复后可继续确认 ==")
        # 恢复 node1 接流，制造一个新的待核请求后立即让上游“不可达”
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        # 排空 node2，确保请求进入 node1
        http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")

        result17 = {}

        def slow_call17():
            result17["status"], _, result17["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=5", timeout=15
            )

        t17 = threading.Thread(target=slow_call17)
        t17.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        t17.join(timeout=15)
        check("新一轮读超时请求进入待核",
              node_status("node1")["unconfirmed"] == 1, str(node_status("node1")))

        # 只让核对接口故障：健康探测与业务接口仍正常 -> 验证“探测转好不能抹去待核”
        status, _, _ = http("POST", f"http://127.0.0.1:{U1_PORT}/control/verify_fail?enabled=true")
        assert status == 200
        status, _, body17 = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b17 = json.loads(body17)
        check("核对接口不可达时排空返回 202", status == 202, body17)
        check("上游不可达不抹去待核",
              b17.get("unconfirmed") == 1 and b17.get("last_verify_ok") is False, body17)
        check("不可达时仍不给停机回执", b17.get("safe_to_stop") is False, body17)
        st = node_status("node1")
        check("周期探测仍成功但待核仍在",
              st["healthy"] is True and st["unconfirmed"] == 1, str(st))

        # 上游核对恢复（慢请求此时已执行结束）-> 再次排空应确认完成
        http("POST", f"http://127.0.0.1:{U1_PORT}/control/verify_fail?enabled=false")
        status, _, body17 = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5"
        )
        b17 = json.loads(body17)
        check("上游恢复后可继续确认并给回执",
              status == 200 and b17.get("drained") is True
              and b17.get("unconfirmed") == 0, body17)
        check("状态中待核已清零", node_status("node1")["unconfirmed"] == 0)

        # 18. 上游重启：旧请求随旧实例消失，超过观察窗口后由“上游从未登记”确认
        print("== 18. 上游重启后旧请求经观察窗口确认 ==")
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        result18 = {}

        def slow_call18():
            result18["status"], _, result18["body"] = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=30", timeout=15
            )

        t18 = threading.Thread(target=slow_call18)
        t18.start()
        deadline = time.time() + 5
        while time.time() < deadline and node_status("node1")["inflight"] != 1:
            time.sleep(0.1)
        t18.join(timeout=15)
        check("超时请求再次进入待核",
              node_status("node1")["unconfirmed"] == 1, str(node_status("node1")))
        status, _, body18 = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b18 = json.loads(body18)
        check("重启前待核请求阻止回执",
              status == 202 and b18.get("safe_to_stop") is False, body18)
        # 重启 node1 上游：旧实例上的 30s 慢请求随之消失
        kill(u1)
        procs.remove(u1)
        u1 = spawn(os.path.join(ROOT, "upstream"), U1_PORT, {"NODE_ID": "node1"})
        procs.insert(0, u1)
        assert wait_ready(f"http://127.0.0.1:{U1_PORT}/"), "上游 node1 重启失败"
        status, _, body18 = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=5"
        )
        b18 = json.loads(body18)
        check("重启后旧请求不再执行，待核得到确认，可停机",
              status == 200 and b18.get("drained") is True
              and b18.get("unconfirmed") == 0, b18)

        # 19. 客户端中途断开：网关取消转发，但上游可能仍在执行 -> 进入待核
        print("== 19. 客户端断开进入待核 ==")
        status, _, _ = http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/resume")
        assert status == 200
        deadline = time.time() + 8
        while time.time() < deadline and not node_status("node1")["eligible"]:
            time.sleep(0.2)
        http("POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node2/drain?timeout=5")
        # 用 1.2s 客户端超时请求 8s 慢接口：网关已把请求交给上游，客户端先放弃。
        # 网关自身在该场景下可能先到 1s 请求超时（返回 504），客户端是否读到
        # 响应不影响结论：关键是不能虚报成功，且请求必须进入待核。
        try:
            status, _, body19 = http(
                "GET", f"http://127.0.0.1:{GW_PORT}/proxy/slow?seconds=8", timeout=1.2
            )
        except Exception:
            status = 0
        check("客户端放弃时未虚报成功", status in (0, 504), str(status))
        deadline = time.time() + 6
        while time.time() < deadline and node_status("node1")["unconfirmed"] != 1:
            time.sleep(0.2)
        st = node_status("node1")
        check("客户端断开后网关在途归零", st["inflight"] == 0, str(st))
        check("客户端断开请求进入待核", st["unconfirmed"] == 1, str(st))
        check("客户端断开时不得给停机回执", st["safe_to_stop"] is False, str(st))
        status, _, body19 = http(
            "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=1"
        )
        b19 = json.loads(body19)
        check("客户端断开后立即排空只返回未确认",
              status == 202 and b19.get("drained") is False, body19)
        # 上游慢请求结束后可确认
        deadline = time.time() + 20
        confirmed19 = None
        while time.time() < deadline:
            status, _, body19 = http(
                "POST", f"http://127.0.0.1:{GW_PORT}/admin/nodes/node1/drain?timeout=10"
            )
            confirmed19 = (status, json.loads(body19))
            if status == 200:
                break
            time.sleep(0.5)
        check("上游执行结束后客户端断开请求也得到确认",
              confirmed19[0] == 200 and confirmed19[1].get("safe_to_stop") is True,
              str(confirmed19[1]))

        print(f"\n==== 结果: {passed} passed, {failed} failed ====")
        return 1 if failed else 0

    finally:
        for p in procs:
            kill(p)


if __name__ == "__main__":
    sys.exit(main())
