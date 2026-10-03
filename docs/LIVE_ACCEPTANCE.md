# LIVE 验收说明

代码加固和自动化测试不能替代 OKX 现场验收。默认 `MODE=PAPER`、`LIVE_TRADING_ENABLED=false`；服务器 Demo compose 继续使用 SQLite，不启用 LIVE。

## 代码门禁

- LIVE 账本仅支持 `postgresql+asyncpg://`；账户 UID、权限（仅 read_only + trade）、IP 绑定和账户模式核验通过后才获取单写 lease。
- `LIVE_LEASE_DATABASE_URL` 是必须显式配置的 secret DSN。**同一 OKX UID 的所有实例必须使用同一个协调 PostgreSQL 数据库**，即使各自账本数据库不同。不同协调数据库的 advisory locks 不互斥，不能提供全局单写保证。
- 协调连接必须直连 PostgreSQL 或使用保持会话的连接方式；禁止 PgBouncer transaction/statement pooling。lease 使用独立 NullPool 会话，非等待式 `pg_try_advisory_lock`，固定 namespace + UID 的 SHA-256 前 64 位稳定有符号整数。锁键、UID 和 digest 不出现在状态接口或日志中。
- lease 持有后才原子绑定账本、恢复本地状态和对账；LIVE 始终 HALT，等待人工 `/system/resume`。绑定采用 PostgreSQL 事务 advisory lock + 常量唯一索引，一个账本最多一个 binding，仅存 mode / account_digest。
- 初始化失败或安全关闭释放会话锁。连接断开、实际锁丢失或有界探测失败会锁死当前 lease，禁止自动重新获取；同步 fencing 禁止 entry、Governor 至少 HALT、CRITICAL 通知 `LIVE writer lease lost`，归类 SAFETY_OR_MANUAL。
- 写入前验证身份；新开仓还须有效 lease 与 NORMAL。有效身份的只减仓、保护单及撤单不依赖 lease。账户级 CAA refresh/disable 必须持有当前 lease；丢锁后停止所有未来 CAA 写入（不采用 one-shot final refresh），包括直接 client 调用。dead-man 循环退出，不把 ownership loss 误报为 CAA unavailable；需要停用 CAA 才能安全保留普通 reduce-only exit 时，丢锁实例拒绝停机、记录 `CAA disable skipped: LIVE writer lease not owned`，不会自动重抢 lease，也不会关闭新 owner 的 CAA。凭据/endpoint 变化永久撤销缓存授权，必须重新核验，运行中的账户 UID 不可切换；认证 401/403 撤销身份授权；临时 config GET 故障暂停开仓，保留先前验证的 Emergency 减仓权限。未知写结果继续查询同一个 clOrdId，绝不盲重试。
- `API_TOKEN`、`STATUS_API_TOKEN` trim 后均至少 32 字符且互不相同。状态令牌只能 GET `/status`；`/health` 保持原来的匿名行为。watchdog 只接收状态令牌与 Bark 配置，不接收交易凭据、控制令牌或数据库 DSN。
- `/status.live_writer_lease` 只报告 required/held 布尔值。preflight 在 PAPER 标记 LIVE_SINGLE_WRITER 为 UNVERIFIED，观察 LIVE 时必须 held；报告永远不会自动批准 LIVE。

## 保证金安全余量

原有硬上限 `max_margin_usage=0.25` 和 risk_per_trade 定义保持不变。新增 `margin_usage_target=0.20`（必须 < 硬上限且 <= 0.22）只用于保守下单量上限。

定义：E=权益，M=已有组合保证金，O=已有 open_risk，P=已有组合名义敞口，L=原有杠杆，t=20%，d=按原规则取整后的 stop distance / entry，c=双边 max(maker_fee,taker_fee) + 双边 slippage_bps / 10000。

```text
E_stress = E - O - P*c
N_buffer = max(0, (t*E_stress - M) / (1/L + t*(d+c)))
N_available = available_balance / (1/L + d+c)
N = max(0, min(original risk sizing, original leverage cap,
               original 25% margin cap, N_buffer, N_available))
contracts = floor(N / per_contract / lotSz) * lotSz
```

该公式要求 `(M + N/L) / (E_stress - N*(d+c)) <= t`，预留止损损失与双边费用/滑点。已有仓位保证金和 open_risk 已按组合聚合。低于 minSz 则不交易；非有限数字、异常 instrument 参数及极端止损距离 fail closed。费用较高只会减量。

这不是交易所清算模型：跳空、资金费、保证金规则变化及在途请求仍须依靠已有保护、持续风控、CAA 和对账。PostgreSQL 锁无法撤回丢锁前已发出的 OKX HTTP 请求，现场演练必须检查这类在途订单。

## PostgreSQL 自动化验收

`TEST_POSTGRES_URL` 必须指向隔离的一次性测试服务：asyncpg、用户 `quant_test`、数据库 `quant_live_acceptance_test`、loopback 主机。未配置直接失败，不能 silent skip。测试清理仅允许该命名的数据库表；跨账本测试另创建并删除自己独占的一次性第二测试数据库，绝不使用生产账本。

```sh
# DSN 由隔离环境注入，不在命令或日志中展示生产密码。
uv run pytest
uv run ruff check app tests
uv run mypy app
```

CI 使用独立 PostgreSQL 16 service，覆盖锁争用、释放、连接中断、实际锁移除、不同 UID 绑定竞争、同 UID 幂等绑定、Demo/未知历史拒绝及 LIVE 恢复门禁。LIVE HTTP 测试使用固定假凭据与 MockTransport，不发送真实交易。

## 受控 Demo 现场演练（必须人工填写）

自动化测试通过不构成下面任何项目的 PASS。所有项目先确认测试账户、单实例、仓位/挂单/保护和退出计划，按现场控制流程在 Demo 执行；本轮仅提供记录模板，不主动制造交易或故障。单写争用/丢锁演练使用隔离 PostgreSQL 和假 LIVE HTTP，LIVE 真实账户验收需要后续人工授权。

| 编号 | 场景 | 时间戳 | commit SHA | 初始仓位 | orders | protection | 最终仓位 | Governor | reconciliation | Bark | 人工结论/证据 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | partial fill + restart | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 2 | protected open position + restart | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 3 | CAA expiry | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 4 | Redis outage | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 5 | private WS outage | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 6 | reconciliation REST outage | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 7 | safe shutdown with protected position | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 8 | Emergency reduce-only flatten | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 9 | lease contention | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |
| 10 | lease loss | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | 待填 | UNVERIFIED |

每个场景保留脱敏日志、交易所订单/保护证据、最终对账和通知记录。验收后由操作者签名，记录是否满足预期；失败必须保持 HALT/EMERGENCY，不能通过重启、放宽硬上限或扩大自动恢复范围绕过。lease 丢失后人工确认仅一个实例，重启获取 lease、完整对账、人工 resume；EMERGENCY 和 LIVE 都不自动恢复 NORMAL。
