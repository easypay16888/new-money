# Private Algo WebSocket Recovery & Diagnostics V1

## 审计范围与现场证据

基线：`2fe6f813bb88ee9ae4946a383480f809aa0f9240`。此补丁不修改策略、风险参数、CAA ownership、LIVE gates 或账本补录架构。

2026-10-08 现场只读检查发现：四路 transport、processing、freshness 和完整对账已经健康，Governor 仍为 HALT，原因是 `auto recovery circuit breaker`。没有执行 resume 或服务器升级。

旧版日志显示 private-algo 在 UTC 10/7 18:43:55、19:53:24、21:04:01、21:09:46 出现 `ConnectionClosedError`，没有收到 close frame，且最近 Pong 分别约为 9.9、1.5、4.4、10.0 秒前。这些证据只能证明连接丢失，不能证明具体是 OKX 服务端、网络中间设备或其他原因。重连累计次数不是连续失败时长；旧 trading incident 直到交易恢复 NORMAL 才关闭，造成连接已经恢复后仍展示长期断线的误导。

代码审计确认另外两个可复现缺陷，但不把它们推断成上述现场断线的唯一原因：

- subscribe ACK 期限从 `_connected_at` 计算，错误地扣掉 login 消耗的时间。
- `run()` 在重连前无期限 `queue.join()`；旧 handler 挂起时无法开始下一次连接。恢复任务里也有无期限 join。

## 生命周期与所有阻塞门禁

|阶段/路径|结果与恢复条件|
|---|---|
|CONNECTING：TCP/TLS/WS 失败或超时|`connected=false`，BACKOFF 后继续尝试；明确 `open_timeout=8s`|
|AUTHENTICATING：登录失败/超时|`login_ok=false`，固定安全错误码；协议/认证失败保持人工处理规则|
|SUBSCRIBING：发送失败、ACK 超时、错误 ACK、订阅拒绝/撤销|transport 不健康；ACK 必须匹配请求的 channel/instType/instId|
|ACTIVE：收到 close frame 或连接异常|保存真实关闭代码、原因、方向；无 frame 时明确 unavailable|
|ACTIVE：text ping 后无 pong|独立 `ws_heartbeat_timeout`；保留 10s idle / 5s pong 时限|
|服务端 notice 64008|`ws_server_maintenance`，有序断开并重新认证订阅|
|queue 满、handler 异常、worker 意外退出|HALT，`processing_unsafe=true`；队列有界，已接收 private 事件不在重连时丢弃|
|RECONCILING：业务队列未空闲、代际变化或完整对账失败|`reconciliation_required=true`，禁止开仓；只有同一代际、transport 健康且业务空闲的完整对账可以清除|
|run task 意外结束或重连超期没有下一次尝试|立即/周期 liveness fence，CRITICAL；安全或人工原因，不进入 transient 白名单|
|BACKOFF|1/2/4/8…秒加 jitter，包含 jitter 后最多 30s；没有最大重试次数，也不自动恢复交易状态|

登录和订阅各自有完整 `REQUEST_TIMEOUT_SECONDS`（默认 8s）。例如 login 7s + ACK 2s 通过；login 9s 或 subscribe 9s 都失败。subscribe deadline 从发送订阅时重新计时，发送过程也纳入期限。

private worker 跨连接保持有序处理。private 重连不等待 queue drain，也不取消已接收的 handler/交易写；full reconciliation 的既有 idle/generation 门禁继续生效。恢复任务的 join 有限时，超时只取消 join 等待者，完整对账仍不能把忙碌 worker 标成安全。公开市场的旧队列仍先排空，排空等待也有界，超时 fail closed。

所有 successful queue.get 均被 try/finally 覆盖，task_done 在观测 metrics 之前执行。worker 意外退出可独立重启，但 processing unsafe 与 HALT 不因重启而解除；必须对账及原有人工/恢复门禁。

run task 的 done callback 只在 runtime 仍运行时 fence，正常 shutdown 的取消不告警。reconnect watchdog 阈值为 `2 * WS_BACKOFF_MAX_SECONDS + WS_CONNECT_TIMEOUT_SECONDS`（默认 68s）；它不能重新启动交易、放宽健康门禁或绕过 circuit breaker。

### Reconnect progress 修复

stall age 使用 `last_reconnect_progress_at`，而非上一段健康 session 的建连时间。断线检测、退避安排、开始建连、连接建立、login 完成、subscribe 发送、ACK 完成都刷新锚点；status 读取不会刷新它。未到期 backoff，以及 CONNECTING / AUTHENTICATING / SUBSCRIBING 的独立期限都免于 stall 判定（1 秒调度容差只用于此 watchdog，不延长协议期限）。超过期限后仍必须无进展超过 68 秒才判 stalled。

ACK 完成后当前 `reason_code=healthy`，历史根因单独保留为 `last_failure_reason_code`，关闭代码、原因与断线时间继续可查。worker/run 的真实故障仍 fence，故障码不会因 status 读取消失。

该修复新增 8 项测试；完整本地验证 **735 passed / 0 skipped / 0 warnings**，真实 PostgreSQL 执行，Ruff / mypy PASS。一小时健康 session 后普通 close 的 watchdog 采样不会使 transient HALT 变成不可自动恢复；真实无进展 stalled 仍为 SAFETY_OR_MANUAL。

## /status 与日志

下面是协议 fixture 示例，不是新版本服务器已部署的证明：

```json
{
  "name": "private-algo",
  "phase": "SUBSCRIBING",
  "connected": true,
  "login_ok": true,
  "subscriptions_ok": false,
  "transport_healthy": false,
  "reconciliation_required": true,
  "run_task_alive": true,
  "worker_task_alive": true,
  "reason_code": "ws_subscription_timeout",
  "close_code": null,
  "close_reason": "",
  "next_retry_in": null
}
```

额外输出：login/subscribe 起止时刻、session 代际、最近 RX/ping/pong 年龄、RTT、队列/handler 年龄、最近连接尝试与断线、失败阶段、backoff、连续失败、worker 异常类型、任务存活。`*_at` 握手及重试计时字段是 monotonic seconds，附 `time_basis`，不可当成 Unix 时间；日志本身有 UTC timestamp。

session-ended 结构化日志包括 socket、异常类型、close code/reason/side、phase、session age、RX/pong age、ping pending、login、ACK 数量、queue、reconciliation required、重连数和 backoff。原始异常、认证消息、UID、credentials、完整 URL 不写日志。close reason 去除凭证、URL 和控制字符，长度有界。

每个 socket 开始运行时只输出 host/port/标准路径末段；明确配置 8443 会 WARNING，保留用户配置，绝不偷偷重写。

## Bark 与 incident

private 每个 socket 有两个独立 incident：`ws:transport:<name>` 与 `ws:recovery:<name>`。

- transport 恢复即可结束断线 incident，即使 Governor 仍 HALT。
- 若对账未完成，只发送“⚠️ WebSocket 已重连，等待安全对账”；持续超过现有 infra delay 才发“🚨 WebSocket 重连后对账未完成”。
- 完整健康后才发送“✅ WebSocket 已恢复”，包含 Transport/Login/Subscription/Worker/Reconciliation 的当前结果。
- 风控/circuit breaker incident 独立保留，连接恢复不等于恢复交易。
- 异常期间阶段/重连数变化更新 incident 证据；保留 delay、聚合及 delivery audit，不按每次重连刷手机通知。
- liveness CRITICAL 使用稳定 transition identity，重复采样不会刷屏。

断线内容包含 socket、当前及故障阶段、machine reason、真实 close code/reason/side、最近 Pong、连续失败、总重连和下一次重试。没有 close frame 时写“不可用”，不写“系统异常”或伪造 1006。

## 官方协议依据

- [OKX Algo orders channel](https://app.okx.com/docs-v5/en/#order-book-trading-algo-trading-ws-algo-orders-channel)：`/ws/v5/business`、required login、`orders-algo`、`instType=ANY` 均正确；仅订单更新推送，没有初始业务快照。无 algo 事件不能判 stale。
- [OKX WebSocket Connect / Notification](https://app.okx.com/docs-v5/en/#overview-websocket)：保留应用层 text ping/pong，识别 notice 64008，不依赖业务事件活跃度。
- [OKX 8443 discontinuation](https://www.okx.com/en-eu/help/okx-websocket-port-8443-discontinuation-announcement)：标准 443 已可用，8443 于 2026-10-31 停用；PAPER/LIVE 默认端点不含 :8443。

## Metrics

新增稳定 socket label 的：

- `quant_ws_connect_attempts_total`
- `quant_ws_connect_failures_total{socket,reason_class}`
- `quant_ws_consecutive_failures`
- `quant_ws_session_duration_seconds`
- `quant_ws_reconnect_backoff_seconds`
- `quant_ws_last_connect_attempt_age_seconds`
- `quant_ws_worker_alive`

reason_class 只用固定 machine code；close reason、UID、order ID 不进入 label。既有 queue/handler/ping 指标保留定义。

## 验收边界

测试覆盖独立握手期限、真实本地 close 1000/1001/无 frame、连续失败、虚拟一小时重连、worker crash/restart、队列记账、代际/空闲对账门禁、Bark 与凭证隐藏。Mock/local server 的通过不能替代 OKX 网络 soak。

新增 53 项测试，全部旧测试保留。本地完整 `uv run pytest -q -W error` 为 **727 passed / 0 skipped / 0 warnings**；真实 PostgreSQL 集成使用独立 `quant_live_acceptance_test` 数据库执行，既有 lease/binding/CAA/ledger 回归通过。Ruff PASS，mypy PASS（34 source files）。GitHub Actions 的结果须按补丁实际 commit 独立核验，不以本地结果代替。

Existing unrelated composite fill identity P1 remains unresolved：当前 seen_trade_ids 与 fills unique reference 仍以全局 tradeId 去重，OKX tradeId 的唯一性范围是 instId。本次不改它；需独立审计与 commit 后，才能继续判断 Micro-Live readiness。

生产 Demo 的 circuit breaker、未知协议错误、LIVE 与 EMERGENCY 的人工恢复规则均未扩大。此补丁只可以进入 controlled Demo acceptance，不声明 READY FOR LIVE。
