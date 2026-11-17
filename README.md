# HTTP 入口网关（支持优雅排空 / 健康探测 / 轮询转发）

滚动更新场景下的入口网关：运维人员先排空待停机实例，使新请求不再进入，
同时保证在途长请求完整取回原响应，全部结束后才报告排空完成。

**1.1 起的关键语义**：排空回执不再只依赖网关本地的在途计数。网关读超时 /
客户端断开时，请求可能仍在上游执行，这些请求会被记为**待核（unconfirmed）**；
只有网关在途清零 **且** 上游确认这些请求已不再执行，才返回可停机回执。

技术栈：**FastAPI + httpx**，由 **Docker Compose** 启动 1 个网关 + 2 个可模拟
慢请求 / 故障的上游。

## 目录结构

```
.
├── docker-compose.yml      # 编排：gateway + upstream1 + upstream2
├── gateway/
│   ├── app.py              # 网关：仅转发 GET、轮询、探测、排空/恢复/状态
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py              # 模拟上游：slow / flaky / 故障模式 + 请求核对接口
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    ├── self_test.py        # 本地端到端自测（19 组场景 / 86 项断言）
    └── state_test.py       # 状态机单测（31 项断言，不启动 HTTP 服务）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 交互文档:    http://localhost:18080/docs
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |
| `VERIFY_TIMEOUT` | `1.5` | 单次向上游核对待核请求的超时（秒） |
| `VERIFY_RETRY_INTERVAL` | `0.5` | 待核未确认时的重试间隔（秒） |
| `VERIFY_UNKNOWN_GRACE` | `2.0` | 上游报告“从未登记”时，判定其未执行的观察窗口（秒） |

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 轮询转发到健康且未排空的节点，透传查询串与响应
                       # 无可用节点 -> 503；上游超时 -> 504；连不上 -> 502
                       # 非 GET 方法 -> 405
```

管理：

```
GET  /admin/status                     # 健康/排空/在途/待核/计数/最近探测与核对
POST /admin/nodes/{name}/drain?timeout=# 排空：立即停接新流，等待“在途清零 + 上游确认”
                                       #   全部确认 -> 200 drained=true, safe_to_stop=true
                                       #   超时/上游不可达/仍有待核 -> 202 drained=false
POST /admin/nodes/{name}/resume        # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                      # 手动触发一轮探测（不清除待核）
```

状态字段说明：

| 字段 | 含义 |
| --- | --- |
| `inflight` | 网关仍在等待响应的请求数 |
| `unconfirmed` | 网关已超时/取消，但上游可能仍在执行的请求数 |
| `pending_verification` | `unconfirmed > 0` |
| `outstanding` | `inflight + unconfirmed` |
| `safe_to_stop` | 仅在排空中（`draining=true`）且 `inflight=0`、`unconfirmed=0` 时为 true |
| `last_verify_ok/detail` | 最近一次上游核对结果；不可达时会给出原因 |
| `unconfirmed_requests` | 待核请求明细（request_id / path / 原因 / 已等待秒数） |

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state                # 查看模式、实例 ID、在途与请求登记
GET  /control/verify?ids=a,b       # 核对请求：active / done / unknown
POST /control/mode?mode=           # healthy | slow | error | hang
POST /control/verify_fail?enabled= # 测试钩子：只让核对接口故障
GET  /slow?seconds=N               # 慢请求（验证在途不被截断）
GET  /flaky                        # 按模式返回 200 / 500 / 挂起
GET  /                             # 健康端点（error->503, hang->挂起，含实例 ID）
```

## 典型演练

```bash
# 1. 发一个慢请求（10 秒），在另一终端立刻排空 node2
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
# 现象：drain 阻塞约 10 秒，慢请求拿到完整 200，随后 drained=true

# 2. 读超时场景：网关先放弃等待，但上游仍在执行
curl -X POST "http://localhost:18081/control/mode?mode=healthy"
curl "http://localhost:18080/proxy/slow?seconds=30"   # REQUEST_TIMEOUT 到点 -> 504
curl http://localhost:18080/admin/status
# 现象：inflight=0，但 unconfirmed=1、safe_to_stop=false
curl -X POST "http://localhost:18080/admin/nodes/node1/drain?timeout=5"
# 现象：202 drained=false、pending_verification=true（不会误报可停机）
# 30 秒后上游慢请求结束，再次排空 -> 200 drained=true

# 3. 上游不可达时维持待核，恢复后继续确认
curl -X POST "http://localhost:18081/control/verify_fail?enabled=true"
curl -X POST "http://localhost:18080/admin/nodes/node1/drain?timeout=5"
# 现象：202、last_verify_ok=false；周期探测仍成功也不会抹去待核
curl -X POST "http://localhost:18081/control/verify_fail?enabled=false"
curl -X POST "http://localhost:18080/admin/nodes/node1/drain?timeout=5"  # 确认后 200

# 4. 让 node1 进入故障模式，观察探测自动摘除
curl -X POST "http://localhost:18081/control/mode?mode=error"
sleep 3
curl http://localhost:18080/admin/status      # node1 healthy=false

# 5. 故障未消除时恢复 node1：不会绕过探测，节点仍不可接流
curl -X POST http://localhost:18080/admin/nodes/node1/resume
# eligible=false；上游修好后经周期探测自动恢复接流
```

## 关键设计与并发正确性

- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **三种请求状态**：`inflight`（网关在等响应）、`unconfirmed`（网关已放弃等待，
  上游可能仍在执行）、结束。读超时、响应读取错误、任务取消进入 `unconfirmed`；
  连接未建立、连接超时、写超时说明请求未完整送达，**不会**被登记为待核。
- **可停机回执**：`drain` 仅在 `inflight==0` 且 `unconfirmed` 为空时返回
  `drained=true / safe_to_stop=true`（HTTP 200）。网关在途归零但仍有待核时，
  回执为 202 + `drained=false`，绝不提前放行。
- **上游确认**：网关为每个转发请求生成 `X-Gateway-Request-Id`，排空时调用上游
  `GET /control/verify?ids=...`：
  - `active`：仍在执行 → 保留待核（**最高优先级**，即使上游实例已变化也不放行）；
  - `done`：已结束 → 移除；
  - `unknown` 且超过 `VERIFY_UNKNOWN_GRACE`：判定请求从未被上游接收 → 移除；
  - 核对接口不可达 / 非 200 / 响应非法：**保留全部待核**，返回 `last_verify_ok=false`。
- **探测不抹待核**：周期探测只更新健康位与实例 ID，不触碰 `draining`、
  `unconfirmed`、`drain_verified`；上游恢复健康后仍需显式核对才能给回执。
- **可恢复**：排空等待超时后节点保持排空与待核，再次调用 drain 可继续确认；
  上游恢复（慢请求结束或进程重启）后即可完成确认。
- **原子登记**：选节点与 `inflight += 1` 在同一把 `asyncio.Condition` 锁内完成；
  请求结束时 `inflight -= 1` 与待核登记也在同一锁内完成，排空等待者不会看到
  “在途已清零、待核尚未登记”的空窗。
- **取消也结算**：转发被取消时用独立任务 + `asyncio.shield` 完成计数归还与待核
  登记，保证状态准确。
- **在途慢请求不被截断**：排空只影响新请求分配，已取得响应的慢请求照常透传；
  其他健康且未排空的节点继续接流。
- **不虚报成功**：5xx 如实透传并即时降级；超时 504、连接错误 502，均不返回
  虚假 200。504/502 响应体里的 `unconfirmed` 字段说明该请求是否进入了待核。
- **无节点可接流**：所有节点都不健康或排空中时，`GET /proxy/*` 返回 503。

### 已知边界

- `unknown` 观察窗口（默认 2s）是一个工程折中：上游从未登记且超过窗口的请求
  会被判定为“不会执行”。若网络延迟显著大于该窗口，可调大
  `VERIFY_UNKNOWN_GRACE`。
- 上游需实现 `/control/verify` 核对接口；未实现时网关始终无法确认，排空只会
  持续返回“未确认”（安全侧，但无法给出可停机回执）。

## 自测

```bash
# 无需 docker；需本地具备 fastapi/httpx/uvicorn（可 pip install 到独立目录）
PYLIBS=/path/to/site-packages python3 tests/self_test.py   # 端到端，19 组 / 86 项
python3 tests/state_test.py                                 # 状态机单测，31 项
```

端到端测试覆盖：轮询、状态 API、空闲/在途/并发排空、503/504/502、探测不解除
排空、恢复不绕过探测、仅 GET、/docs、排空超时 202、**读超时进入待核且探测
成功不抹去、上游仍在执行时不给回执、上游不可达维持待核、上游恢复后继续确认、
上游重启后确认、调用方提前放弃（由网关超时结算）进入待核**、未知节点 404 等；
状态机单测额外覆盖任务取消、异常分类、上游字段异常等边界。
