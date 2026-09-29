# OKX Demo Quant Trading System

Python 3.12+ 的 OKX USDT 永续合约模拟交易基础工程。默认 `MODE=PAPER`，无 API 凭据时只采集公开数据并维持 `HALT`。交易链路固定为策略、融合、独立风控、执行。任何未确认的状态均禁止新开仓。

## 架构

`OKX WS → MarketData → Features → Regime → Strategies → Meta → Risk → Execution → OKX Demo`。私有 WS 与 REST 对账回写订单、仓位及风控状态。PostgreSQL 保存事件审计，Redis 保存最新行情。

## 安装与配置

复制 `.env.example` 为 `.env`，只在本机填入 **Demo Trading API Key**、Secret 和 Passphrase。API 权限：Read=Yes、Trade=Yes、Withdraw=No，建议绑定 IP。请勿提交 `.env`。默认服务区域为 OKX 全球站；其他区域需按账户所属站点的官方文档设置 REST 和 WebSocket URL。

```sh
cp .env.example .env
docker compose up --build
```

`http://127.0.0.1:8000/status` 可查看连接和风控状态。初次启动回填 5 个周期最近的已收盘 K 线，再读取账户、仓位、普通挂单和条件单；对账、Redis 或行情不健康时保持 `HALT`。已有仓位只有在本地订单审计、交易所仓位和附带保护单一致时才恢复；已有普通挂单或任何不匹配状态会保持 `HALT`。

## 测试

```sh
uv sync --extra dev
uv run pytest
uv run ruff check app tests
uv run mypy app
```

## Paper Trade 与风险控制

默认 1x，单笔风险 0.25%，最大 0.35%，总持仓风险 1%，当日亏损 1.5%，周回撤 4%，保证金使用 25%，最多 3 仓。系统支持手动 `POST /system/halt`，恢复需 `POST /system/resume` 并通过对账与健康检查。Cancel All After 每 20 秒刷新为 60 秒倒计时。

## Backtest

已实现按 15m 已收盘 K 线逐步处理的初版事件回测：信号在下一根开盘成交，止损与止盈同根触发时优先止损，并计算手续费、滑点、价差、资金费率和部分成交比例。仓库累积至少 201 根 15m K 线后运行：

```sh
uv run python -m app.cli backtest --symbol BTC-USDT-SWAP --equity 10000
uv run python -m app.cli walk-forward --bars 1000 --train 500 --validation 200 --out-of-sample 100
uv run python -m app.cli walk-forward --run --symbol BTC-USDT-SWAP --train 500 --validation 200 --out-of-sample 100
```

滚动窗口 `--run` 对每折分别计算训练、验证和互不重叠的样本外指标，使用固定参数，尚未实现参数优化和完整多周期回测。回测结果保存在 `backtest_runs`，不能直接作为实盘收益估计。

## Monitoring 与日复盘

`http://127.0.0.1:3000` 是 Grafana 本地仪表板，Prometheus 在 `:9090` 抓取 `/metrics`。面板显示权益、当日盈亏、持仓数、风控状态和连接重试。默认 Grafana 管理员账号及密码均为 `admin`，仅绑定本机。日报每日按 UTC 日期生成到 `daily_reports`，包含权益变化、日内最大回撤、手续费；仅当全部成交提供 `fillPnl` 时汇总已实现盈亏，胜率和盈亏比因缺少可靠的完整交易配对仍显示 `null`。`/performance` 当前只显示权益与当日权益变化。

## 切换到 LIVE

**真实资金交易存在损失风险。当前版本尚未完成全部验收，不得启用 LIVE。** 配置层要求 `MODE=LIVE`、`LIVE_TRADING_ENABLED=true` 和 `CONFIRM_LIVE_ACCOUNT_ID` 三项同时存在；启动时还核对账户 UID。LIVE 控制 API 需要额外配置 `API_TOKEN` 并以 Bearer token 调用。Compose 的 `MODE` 从 `.env` 读取，默认始终为 PAPER。上线前必须完成账户模式、止损单、故障注入和全链路人工验收。

## 排障

`HALT` 时先查看 `/status` 的原因，确认 Demo 凭据、账户模式、区域 API 地址、Redis 与 PostgreSQL 连通性以及行情时效。订单超时会查询同一个 `clOrdId`，未确认状态不会自动重试。

接口依据：[OKX V5 官方文档](https://www.okx.com/docs-v5/en/)。Demo REST 请求使用 `x-simulated-trading: 1`；K 线通过 business WS；Cancel All After 为 `POST /api/v5/trade/cancel-all-after`。
