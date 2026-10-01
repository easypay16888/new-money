# OKX Demo Quant Trading System

Python 3.12+ 的 OKX USDT 永续合约模拟交易基础工程。默认 `MODE=PAPER`，无 API 凭据时只采集公开数据并维持 `HALT`。交易链路固定为策略、融合、独立风控、执行。任何未确认的状态均禁止新开仓。

## 架构

`OKX WS → MarketData → Features → Regime → Strategies → Meta → Risk → Execution → OKX Demo`。私有 WS 与 REST 对账回写订单、仓位及风控状态。PostgreSQL 保存事件审计，Redis 保存最新行情。

## 安装与配置

复制 `.env.example` 为 `.env`，只在本机填入 **Demo Trading API Key**、Secret 和 Passphrase。API 权限：Read=Yes、Trade=Yes、Withdraw=No，建议绑定 IP。请勿提交 `.env`。默认服务区域为 OKX 全球站；其他区域需按账户所属站点的官方文档设置 REST 和 WebSocket URL。

运行 USDT 永续前，在 OKX **模拟交易**账户中选择「合约模式」（API `acctLv=2`）及单向持仓（`net_mode`），并确认有可用 USDT。首次切换账户模式须在 OKX 网站或 App 完成。系统使用 USDT 权益和可用保证金计算风险；现货模式或无可用 USDT 时保持 `HALT`。

```sh
cp .env.example .env
docker compose up --build
```

本机未安装 Docker Compose 时，可用已有 Redis 启动本地 Demo 试运行；密钥仍从 `.env` 读取：

```sh
uv sync --extra dev
REDIS_URL=redis://127.0.0.1:6379/15 \
DATABASE_URL=sqlite+aiosqlite:///./data/demo-trading-usdt.db \
MODE=PAPER uv run uvicorn app.main:app --host 127.0.0.1 --port 8000
```

此本机方案使用 SQLite，适合初步 Demo 连通性和交易链路观察；长期 Demo soak 应使用 PostgreSQL 和受监督的服务进程。
本机服务与 Compose 共用 Demo 账户及 `8000` 端口，切换部署方式时先正常停止当前实例，避免两个交易引擎同时运行。

`http://127.0.0.1:8000/status` 可查看连接、风控、阻断 symbol、emergency 目标仓位与保护单状态。初次启动回填 5 个周期最近的已收盘 K 线，再读取账户、仓位、普通挂单和条件单；对账、Redis 或行情不健康时保持 `HALT`。恢复本系统未完成 entry 时先读取交易所 `accFillSz`，核对已成交仓位及保护单，再撤销剩余 entry；撤单未确认也不会跳过保护核对。保护单必须匹配标的、方向、覆盖数量、触发价、`reduceOnly`、有效状态和失败码。保护不足或无法重新确认时进入 EMERGENCY，持久化目标仓位零并尝试只减仓平仓。外部风险增加订单会触发 HALT 和告警。

## 测试

```sh
uv sync --extra dev
uv run pytest
uv run ruff check app tests
uv run mypy app
```

CI 在每次 push 和 pull request 执行上述三个检查。

## Paper Trade 与风险控制

默认 1x，单笔风险 0.25%，最大 0.35%，总持仓风险 1%，当日亏损 1.5%，周回撤 4%，保证金使用 25%，最多 3 仓。系统支持手动 `POST /system/halt`，进入 HALT 会阻止新 entry 并主动撤销本系统未完成 entry；保护性 SL/TP 与必要的 reduce-only exit 保留。订单记录创建时间和信号过期时间；到期时请求撤单，在交易所确认前 symbol 保持阻断。持续组合风控会检查日损失、周回撤、保证金使用率和维持保证金比率；滚动七日权益峰值由数据库 `MAX` 聚合计算，不受事件读取条数限制。EMERGENCY 持久化目标仓位，按交易所实际仓位继续 reduce-only 减仓，HTTP 或查询超时后先对账，不把提交数量计为已完成数量。Cancel All After 每 20 秒刷新为 60 秒倒计时，作为进程失联保险丝。正常停机先 HALT、确认 entry 已撤及持仓保护有效；若保留普通 reduce-only 挂单，调用 `timeOut=0` 停用倒计时后才停止 heartbeat。若停用失败，停机失败且 heartbeat 继续运行；崩溃时不主动停用倒计时。

## Backtest

已实现按 15m 已收盘 K 线逐步处理的初版事件回测：信号在下一根开盘成交，止损与止盈同根触发时优先止损，并计算手续费、滑点、价差、资金费率和部分成交比例。仓库累积至少 201 根 15m K 线后运行：

```sh
uv run python -m app.cli backtest --symbol BTC-USDT-SWAP --equity 10000
uv run python -m app.cli walk-forward --bars 1000 --train 500 --validation 200 --out-of-sample 100
uv run python -m app.cli walk-forward --run --symbol BTC-USDT-SWAP --train 500 --validation 200 --out-of-sample 100
```

Live 与回测共用 `DecisionPipeline`。回测只使用决策时已收盘的 15m 主周期、1H/4H 趋势确认及 5m 入场确认；数据仓库缺少所需确认周期时不会生成依赖它们的信号。新收到的 OI、资金费率、标记价和指数价按交易所时间戳写入 `market_derivatives`；回测在每个 15m 决策时刻只读取当时已知的事件，重算 OI 变化、资金费率和 mark/index premium。旧数据库不会自动补齐历史衍生品事件；缺失字段保持空值。滚动窗口 `--run` 对每折分别计算训练、验证和互不重叠的样本外指标，使用固定参数，尚未实现参数优化。回测结果保存在 `backtest_runs`，不能直接作为实盘收益估计。

## Monitoring 与日复盘

`http://127.0.0.1:3300` 是 Grafana 本地仪表板，Prometheus 在 `:9090` 抓取 `/metrics`。面板显示权益、当日盈亏、持仓数、风控状态和连接重试。默认 Grafana 管理员账号及密码均为 `admin`，仅绑定本机。日报每日按 UTC 日期生成到 `daily_reports`，包含权益变化、日内最大回撤、手续费；仅当全部成交提供 `fillPnl` 时汇总已实现盈亏，胜率和盈亏比因缺少可靠的完整交易配对仍显示 `null`。`/performance` 当前只显示权益与当日权益变化。

### Bark 主动通知

Bark 默认关闭。需要推送时，先在本机 `.env` 设置以下值，然后正常重启 Demo 服务：

```dotenv
BARK_ENABLED=true
BARK_SERVER=https://api.day.app
BARK_DEVICE_KEY=...
BARK_GROUP=OKX Quant
BARK_TIMEOUT_SECONDS=5
BARK_HEARTBEAT_HOURS=6
BARK_DEDUP_SECONDS=60
BARK_HEARTBEAT_ENABLED=false
BARK_NOTIFY_ENTRY_SUBMITTED=false
BARK_NOTIFY_SYSTEM_STOPPING=false
BARK_NOTIFY_FAST_RECOVERY=false
BARK_INFRA_ALERT_DELAY_SECONDS=60
BARK_INCIDENT_MERGE_WINDOW_SECONDS=300
BARK_INCIDENT_RETRY_INITIAL_SECONDS=30
BARK_INCIDENT_RETRY_MAX_SECONDS=300
BARK_TRADE_NOTIFICATIONS=true
BARK_RISK_NOTIFICATIONS=true
BARK_DAILY_REPORT=true
WEBHOOK_NOTIFICATIONS_VERBOSE=true
```

`BARK_DEVICE_KEY` 是 secret，只能放在本机环境变量或未跟踪的 `.env` 中；不要写入代码、日志或提交 GitHub。缺少密钥或 Bark 配置无效时，运行时跳过 Bark 通道并记录不含密钥的错误，交易系统继续运行。推送使用 Bark 的 HTTP POST JSON API。可选 `BARK_SOUND`、`BARK_CRITICAL_SOUND`（默认 `alarm`）和 `BARK_CRITICAL_VOLUME`（默认 `5`）。

交易运行时只把事件放入有界队列，由 dispatcher 分发给 Console、Bark 和 Webhook 各自独立的优先级队列与 worker。单个渠道超时或重试不会阻塞其他渠道；失败重试延时放在待发送队列中，不占用发送 worker，后来的 CRITICAL 会优先发送。队列满时先丢弃低优先级事件，优先保留 CRITICAL；同一 `dedup_key` 的重复事件默认 60 秒内抑制，恢复和明确的风控状态转换仍会发送。Bark 只有 HTTP 2xx 且 JSON 明确返回 `code=200` 或 `code=0` 才算成功；HTML、空响应及缺少成功码都会重试。Bark 超时、服务错误、DNS 故障及通知审计数据库故障不会触发 HALT，也不会阻塞下单、对账、EMERGENCY 或安全停机。发送失败最多重试 3 次，间隔 1、2、5 秒；安全停机完成后最多等待通知队列 3 秒。`notification_events` 按渠道分别记录状态、次数和错误类型，不记录设备密钥或完整 Bark URL。

`NotificationPolicy` 按渠道筛选事件：Console 保留详细事件，Webhook 默认保留详细事件（设 `WEBHOOK_NOTIFICATIONS_VERBOSE=false` 可使用同样的低噪音规则），Bark 只接收重要事件。Bark 默认接收真实成交、保护单确认、平仓、系统启动及停机、日报和严重风控事件；心跳、entry 提交、计划停机的 Stopping、普通信号和单次基础设施抖动默认静默。可按需单独开启上述配置。成交与保护单各推一次，按对应订单或保护单 ID 去重；不会伪造缺失的单笔净盈亏或 R。

Bark 的标题和正文统一使用简体中文，例如“🟢 量化系统已启动”“🛡 BTC 止损保护已生效”“🚨 紧急风控已触发”。`NotificationEvent.event_code` 提供稳定机器标识，路由、事故分类与 CRITICAL 判断优先读取 code、原始 reason 和 metadata；旧英文事件在统一入口兼容识别。`localize_bark_event` 仅在 Bark HTTP 发送边界生成展示副本，Console、Webhook 和内部审计仍保留原始事件。中文化不会修改事件 ID、去重 key、incident key、priority、category 或机器状态；HALT、EMERGENCY、NORMAL、PAPER 等保留英文。未知原因使用经过凭据及地址脱敏的 `reason_code` 提示。

`IncidentManager` 按 key 独立跟踪基础设施、安全 HALT、EMERGENCY 和自动恢复熔断事故。WebSocket、Redis、CAA 与对账异常合并为一个基础设施事故；持续不足 60 秒且恢复的异常只写入内部记录，持续超过阈值才推告警。安全事故独立且立即通知。`BARK_INCIDENT_MERGE_WINDOW_SECONDS` 只把再次发生的事故关联到同一历史系列，不延长新事故的 60 秒告警阈值。

Bark 的渠道内重试耗尽后，事故仍标记为未送达，由独立通知 worker 按 30、60、120、240、300 秒的封顶退避重投同一 `incident_id`；成功回执才将 `notified_open` 或 `notified_resolved` 置为 true。若事故结束前始终未成功发送开场告警，通知恢复后只发送一条注明延迟及持续时间的 retrospective 摘要；曾发生的 EMERGENCY 保留 CRITICAL 级别。组件恢复按事故聚合，不逐条推 Bark；独立组件事故可以结束并说明当前风控仍被暂停，而“交易系统已恢复”须等到风险状态真正 NORMAL。`system_events` 保留开场、失败、重试、恢复和 retrospective 生命周期；Prometheus 增加送达失败、事故重试与 retrospective 指标。通知重试只重发 `NotificationEvent`，不调用交易、风控或恢复操作。日报复用现有 `daily_reports`，Grafana 继续负责完整历史监控。

### External Watchdog

`app.watchdog` 是独立于交易应用的进程。Docker Compose 的 `watchdog` 服务只获配 Bark 与 WATCHDOG 环境变量，不接收 OKX API 密钥，不暴露端口；它只对 `WATCHDOG_STATUS_URL` 执行 `GET /status`，不会调用交易控制接口。`depends_on: app` 只控制启动顺序，app 停止后 watchdog 仍继续运行。Watchdog 的 Bark 分组使用 `WATCHDOG_BARK_GROUP`，未设置时回退到 `BARK_GROUP`。Watchdog 不要求交易应用开启 `BARK_ENABLED`，但需要本机或 Compose 环境提供同一 `BARK_DEVICE_KEY` 才能推送到手机。

```dotenv
WATCHDOG_ENABLED=true
WATCHDOG_STATUS_URL=http://app:8000/status
WATCHDOG_INTERVAL_SECONDS=60
WATCHDOG_FAILURE_THRESHOLD=3
WATCHDOG_RECOVERY_THRESHOLD=2
WATCHDOG_STARTUP_GRACE_SECONDS=120
WATCHDOG_UNHEALTHY_ALERT_SECONDS=60
WATCHDOG_BARK_GROUP=OKX Quant Watchdog
```

本机独立运行时，把 `WATCHDOG_STATUS_URL` 改为 `http://127.0.0.1:8000/status`，然后在另一个受监督的进程中执行 `uv run python -m app.watchdog`。启动前 120 秒只检查不告警；之后连续 3 次不可达或返回错误状态才通知 App Offline。HTTP 可达但 `running=false`、未同步或 WebSocket 不新鲜属于 App Unhealthy，Bark 要在异常持续达到 `WATCHDOG_UNHEALTHY_ALERT_SECONDS` 后才推送。连续 2 次健康后，仅当先前的告警真正送达 Bark 才推送恢复。风险状态 HALT/EMERGENCY 本身不代表进程离线。Watchdog 不发送常规心跳，不参与交易安全决策。

Compose 对 watchdog 使用 `stop_signal: SIGINT` 和 10 秒停机宽限期，触发 Python asyncio 的正常取消流程，完成有界通知 drain 后退出。app 的安全停机仍使用自己的交易恢复与保护检查。

Watchdog 将“告警已生成”“发送中”“Bark 已确认”分别记录。Bark 渠道内重试耗尽或队列丢弃后，复用 `BARK_INCIDENT_RETRY_INITIAL_SECONDS=30`、`BARK_INCIDENT_RETRY_MAX_SECONDS=300`，按 30、60、120、240、300 秒封顶退避；到期且下次 `/status` 检查仍异常时，重投同一 outage ID 和事件 ID。Console 或 Webhook 成功不会确认手机送达。离线通知成功后保持静默，连续两次健康后推一次“✅ 交易程序已恢复”。若整个离线期间从未成功送达 Bark，恢复后手机保持静默，Console 和日志保留故障及恢复记录；队列中的过时离线通知也会停止发送。已发出的单次 HTTP 请求无法撤回，若它在恢复检查后才确认成功，则补一条恢复消息以保留上下文。

通知及 Watchdog 重试状态保存在内存中，重启不会恢复待送达历史；Bark 服务确认成功也不代表 iPhone 已展示消息。Watchdog 的重投时刻还受 `WATCHDOG_INTERVAL_SECONDS` 检查间隔影响。它始终只读取状态并发送通知，没有交易控制权限。

## 独立服务器 Demo 部署

`docker-compose.server.yml` 用独立的 `new-money-demo` Compose 项目运行 app、watchdog、Redis、Prometheus 和 Grafana。它固定 `MODE=PAPER`、`LIVE_TRADING_ENABLED=false`，保留 SQLite 状态数据库；不会连接服务器上其他应用的 Redis 或数据库。app、Prometheus、Grafana 默认只绑定服务器回环地址的 `18000`、`18090`、`13300` 端口，启动前确认这些端口空闲。Watchdog 仅获配通知与状态检查变量，不接收 OKX 凭据。Docker 构建通过 `.dockerignore` 排除 `.env`、数据库和本机虚拟环境。

迁移前先构建镜像、测试服务器的 OKX Demo REST/WS 连通性，并确认其他机器人不会操作同一 Demo 账户。然后在旧实例调用 `POST /system/stop`，必须得到 HTTP 200 和 `running=false` 才停止其进程监督器。若安全停机失败，保留原实例处理仓位，不启动第二个交易引擎。使用 SQLite backup API 导出 `data/demo-trading-usdt.db`，验证完整性，通过 SSH 传输数据库和未跟踪的 `.env`；数据库与 `.env` 权限设为 `600`，数据目录设为 `700`。保留旧实例停机后的数据库备份，避免遗漏订单、止损、Emergency 目标和审计历史。

在服务器部署目录的 `.env` 填入 Demo 凭据、Bark 配置和独立的 `GRAFANA_ADMIN_PASSWORD`，不要提交任何真实密钥或密码。设置 `DEPLOY_REVISION` 为实际部署的 Git commit，然后运行：

```bash
docker compose -f docker-compose.server.yml config --quiet
docker compose -f docker-compose.server.yml build app
docker compose -f docker-compose.server.yml up -d
curl http://127.0.0.1:18000/status
```

确认 `running=true`、`synchronized=true`、四路 WS fresh、CAA 持续成功，且无未确认订单或 Emergency 目标。启动时所有既有恢复门禁仍适用；只有通过健康检查才进入 NORMAL。Redis 行情缓存由启动 preload 和 WS 重新建立；Prometheus 与 Grafana 使用各自持久卷。迁移 SQLite 不会导入另一个监控部署的历史时序数据。

本机可通过 SSH 隧道查看服务器状态和 Grafana；远端 SSH host 使用自己的配置别名：

```bash
ssh -N -L 18000:127.0.0.1:18000 -L 13300:127.0.0.1:13300 your-server
```

隧道开启后访问 `http://127.0.0.1:18000/status`、`http://127.0.0.1:18000/positions` 和 `http://127.0.0.1:13300`。更新或回滚也必须先通过 `POST /system/stop` 安全停机，再停止容器；不得在两个主机同时运行同一账户的 app。通知和监控服务故障不参与交易恢复决策。

## 切换到 LIVE

**真实资金交易存在损失风险。当前版本尚未完成全部验收，不得启用 LIVE。** 配置层要求 `MODE=LIVE`、`LIVE_TRADING_ENABLED=true` 和 `CONFIRM_LIVE_ACCOUNT_ID` 三项同时存在；启动时还核对账户 UID。LIVE 控制 API 需要额外配置 `API_TOKEN` 并以 Bearer token 调用。Compose 的 `MODE` 从 `.env` 读取，默认始终为 PAPER。上线前必须完成账户模式、止损单、故障注入和全链路人工验收。

当前安全修复已经通过本地自动化检查，但尚未经过 OKX Demo 的真实网络故障注入、断线重连、部分成交、保护单触发及停机演练，当前仍 **NOT READY FOR LIVE**。停机如果撤单、保护确认或 CAA 停用失败会返回错误并保持风控任务运行，需排障后重试。

## 排障

### OKX 止损状态与对账快照

有效保护单的 `failCode` 兼容 OKX 返回的空值与成功码 `"0"`（数值 `0` 同样兼容）；非零或异常失败码仍视为无效。交易对、方向、数量、触发价、live 状态与 reduce-only 的全部校验仍需通过。REST 与 WS 共用这一判定，避免将正常止损误判为失败并触发 Emergency。

交易所的挂单快照与 WS 成交更新并非原子读取。已经同步的运行实例发现 owned order 的挂单状态不一致时，会读取该订单详情：核验 client/order ID、交易对、方向、reduce-only、原始数量、累计成交数量与已成交/已取消的最终状态，再审计更新并重新读取账户、仓位、挂单和保护单一次。所有现有对账和风险门禁继续执行；未知订单、未确认状态、查询失败或第二次快照仍不一致，仍阻断交易，不重试下单。

通知层读取最近一次**完成**的对账结果，对账进行中不会被误报为 OKX 不可用；交易安全门禁继续在对账执行期间保持未确认状态。通知侧超过 `max(60 秒, 2 × 对账间隔, 4 × 请求超时)` 没有新完成结果时仍判为异常，避免掩盖卡住的对账。

基础设施组件已恢复但系统仍因独立安全原因停留在 EMERGENCY 时，本次组件事故单独结束；如已推送组件告警，发送“交易基础设施已恢复”，并明确风控仍为 EMERGENCY、新开仓仍被阻断。该消息不会宣称交易恢复。之后再次异常会新建事件，从复发时间重新计算持续时间；风险事件继续独立跟踪，风险恢复仍须通过原有安全门禁。

部分成交与撤单竞态可能留下 symbol 阻断。完整对账通过后，只有交易所订单详情确认本系统最新 entry 已处于最终状态，client/order ID、标的、方向、reduce-only、原始数量与累计成交数量均匹配、本地累计成交量一致、账本净仓位与实际仓位一致、无未完成 entry/风险增加挂单/待处理 Emergency 目标，并且现有持仓仍有有效止损覆盖时，才释放该记账阻断。释放前写入验证审计；查询失败、审计失败或验证期间保护状态变化会保留阻断。**释放 symbol 阻断不解除 HALT/EMERGENCY**；恢复新开仓仍需人工 `POST /system/resume` 通过数据库、Redis、CAA、WS、完整对账和风险限制的全部检查。

`HALT` 时先查看 `/status` 的原因，确认 Demo 凭据、账户模式、区域 API 地址、Redis 与 PostgreSQL 连通性以及行情时效。订单超时会查询同一个 `clOrdId`，未确认状态不会自动重试。

### PAPER 模式受控自动恢复

PAPER 模式默认开启自动恢复。只有 `reconciliation failed`、`WebSocket disconnected or stale`、`Redis unavailable`、`dead man switch unavailable` 属于自动恢复白名单。进入 HALT 后至少等待 30 秒，每隔 30 秒重新完整对账并检查数据库、Redis 读写、CAA、全部 WebSocket、组合风险、未确认 entry 及 Emergency 目标；连续 3 次通过后再次完整对账和检查，才允许恢复 NORMAL。任一次失败会清零计数。`/status.auto_recovery` 显示启用状态、是否符合资格、当前连续通过次数和熔断状态。

仓位或订单不一致、外部风险订单、意外条件单、保护单异常、保证金风险、人工 HALT、未知原因及认证/权限错误均须人工处理；安全性 HALT 不会被后续瞬态故障覆盖。EMERGENCY 永不自动恢复。`MODE=LIVE` 时自动恢复始终关闭，即使设置了 `AUTO_RECOVERY_ENABLED=true`。人工 `POST /system/resume` 保留完整对账与同一套健康检查，不会强制跳过安全门。

一小时内最多自动恢复 3 次；观察期内同类故障复发也计入熔断预算。达到预算后保持 HALT，触发熔断并发送一次高优先级通知。自动恢复后观察 60 秒，稳定期未再次 HALT 才推送 `Auto Recovery Completed`。可通过 `.env.example` 中的 `AUTO_RECOVERY_*` 项调整 PAPER 模式的检查间隔、连续次数、最短 HALT 时间、观察期及熔断阈值。Prometheus 提供尝试、成功、失败检查和熔断计数。独立 Watchdog 仍只读取 `/status`，不参与恢复。

OKX 对账故障日志记录失败的接口操作、无查询参数的路径、数字错误码、HTTP 状态和是否可重试；不记录交易所返回的原始消息、请求头、凭据或完整 URL。仅网络故障、HTTP 429/指定临时服务错误及明确的 OKX 限流/超时代码可进入瞬态恢复。交易写请求不会因本功能自动重试。OKX 限流码 `50011` 与超时码 `50004` 的含义见 [OKX V5 文档](https://www.okx.com/docs-v5/en/)和 [OKX API FAQ](https://www.okx.com/en-us/help/api-faq)。

接口依据：[OKX V5 官方文档](https://www.okx.com/docs-v5/en/)。Demo REST 请求使用 `x-simulated-trading: 1`；K 线通过 business WS；Cancel All After 为 `POST /api/v5/trade/cancel-all-after`。
