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
```

`BARK_DEVICE_KEY` 是 secret，只能放在本机环境变量或未跟踪的 `.env` 中；不要写入代码、日志或提交 GitHub。缺少密钥或 Bark 配置无效时，运行时跳过 Bark 通道并记录不含密钥的错误，交易系统继续运行。推送使用 Bark 的 HTTP POST JSON API。可选 `BARK_SOUND`、`BARK_CRITICAL_SOUND`（默认 `alarm`）和 `BARK_CRITICAL_VOLUME`（默认 `5`）。

交易运行时只把事件放入有界队列，由独立 worker 发送到 Console、Bark 或现有 Webhook。队列满时先丢弃低优先级事件，优先保留 CRITICAL；同一 `dedup_key` 的重复事件默认 60 秒内抑制，恢复和明确的风控状态转换仍会发送。Bark 超时、服务错误、DNS 故障及通知审计数据库故障不会触发 HALT，也不会阻塞下单、对账、EMERGENCY 或安全停机。发送失败最多重试 3 次，间隔 1、2、5 秒；安全停机完成后最多等待通知队列 3 秒。`notification_events` 只记录状态、次数和错误类型，不记录设备密钥或完整 Bark URL。

推送涵盖启动完成、正常停机、HALT/EMERGENCY、关键基础设施异常与恢复、entry 提交及成交、保护单确认、仓位平仓、每日 UTC 报告。默认每 6 小时发送 PASSIVE 心跳，风险状态非 NORMAL 时显示警告和原因。日报复用现有 `daily_reports`，缺少可靠成交配对的胜率、单笔净盈亏和 R 不会被编造。Prometheus 暴露发送、失败、丢弃、队列长度和延迟指标；Grafana 继续负责历史监控。

## 切换到 LIVE

**真实资金交易存在损失风险。当前版本尚未完成全部验收，不得启用 LIVE。** 配置层要求 `MODE=LIVE`、`LIVE_TRADING_ENABLED=true` 和 `CONFIRM_LIVE_ACCOUNT_ID` 三项同时存在；启动时还核对账户 UID。LIVE 控制 API 需要额外配置 `API_TOKEN` 并以 Bearer token 调用。Compose 的 `MODE` 从 `.env` 读取，默认始终为 PAPER。上线前必须完成账户模式、止损单、故障注入和全链路人工验收。

当前安全修复已经通过本地自动化检查，但尚未经过 OKX Demo 的真实网络故障注入、断线重连、部分成交、保护单触发及停机演练，当前仍 **NOT READY FOR LIVE**。停机如果撤单、保护确认或 CAA 停用失败会返回错误并保持风控任务运行，需排障后重试。

## 排障

`HALT` 时先查看 `/status` 的原因，确认 Demo 凭据、账户模式、区域 API 地址、Redis 与 PostgreSQL 连通性以及行情时效。订单超时会查询同一个 `clOrdId`，未确认状态不会自动重试。

接口依据：[OKX V5 官方文档](https://www.okx.com/docs-v5/en/)。Demo REST 请求使用 `x-simulated-trading: 1`；K 线通过 business WS；Cancel All After 为 `POST /api/v5/trade/cancel-all-after`。
