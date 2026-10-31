# Polymarket BTC GTD Bot — 架构分析文档

## 项目概览

这是一个**异步交易机器人**，在 Polymarket 的 BTC 15分钟 UP/DOWN 预测市场上自动执行马丁格尔（Martingale）加仓策略。

### 核心特性

- **回合制交易**：每个 15 分钟为一个独立交易回合
- **马丁格尔策略**：最多 4 层加仓，最大风险暴露 $15/回合（T1 用 taker 市价入场）
- **GateKeeper 机制**：限制 BTC 价格漂移，防止追涨杀跌
- **GTD 订单**：Good-Till-Date 订单，自动过期
- **实盘交易**：DRY_RUN 已移除，全部走真实 CLOB
- **部分成交处理**：`size_filled` 累加，`filled` 仅在全量成交后翻转
- **GTD 过期监控**：`GtdExpiryWatcher` 后台任务主动检测过期订单
- **UserWS 接入**：CLOB 用户 WebSocket 实时接收成交/取消事件

---

## 项目结构

```
polygtd/
├── docs/                    # 文档目录
├── src/
│   ├── main.py              # 程序入口，协调整体流程
│   ├── config.py            # 配置管理（TOML + .env）
│   │
│   ├── market/             # 市场数据层
│   │   ├── pm_client.py      # Polymarket Gamma + CLOB 公共 API
│   │   ├── models.py         # 数据模型（MarketInfo / Side / OpenPosition）
│   │   ├── market_service.py # 市场发现 + 价格获取
│   │   ├── window_context.py # 窗口生命周期状态容器
│   │   ├── window_manager.py # 窗口生命周期管理器（15-min 边界轮询）
│   │   ├── market_ws.py     # Polymarket 市场行情 WebSocket
│   │   └── user_ws.py       # 用户订单 WebSocket
│   │
│   ├── feeds/              # 数据源
│   │   └── binance_spot.py    # Binance BTC/USDT 现货（WS 实时流 + REST K线回溯）
│   │
│   ├── strategy/           # 策略层
│   │   ├── engine.py       # 策略引擎（决策核心）
│   │   ├── gatekeeper.py   # 价格漂移门控
│   │   └── round_state.py  # 回合状态/持仓管理
│   │
│   ├── execution/          # 执行层
│   │   ├── clob_client.py  # CLOB REST API 封装（py-clob-client-v2）
│   │   ├── order_manager.py # 订单管理器（序列化，无 DRY_RUN）
│   │   ├── tier_watcher.py # 单笔 GTD 生命周期/成交对账
│   │   ├── gtd_expiry_watcher.py # GTD 过期检测
│   │   └── heartbeat.py    # 心跳保活（10秒窗口）
│   │
│   └── persistence/        # 持久化层
│       └── db.py          # SQLite 数据库
│
├── config.toml            # 公开配置
└── .env                  # 敏感配置（API密钥）
```

---

## 核心工作原理

### 回合生命周期

每个 15 分钟回合的执行流程如下：

```
┌─────────────────────────────────────────────────────────────────┐
│  回合开始 (WindowManager 每500ms检查边界触发)                       │
├─────────────────────────────────────────────────────────────────┤
│  Step 1: 市场发现                                                │
│  ├── Gamma API 查询 slug = "btc-updown-15m-{timestamp}"        │
│  ├── 获取 condition_id、UP/DOWN token_id、tick_size             │
│  └── 订阅 Polymarket 市场 WebSocket                              │
├─────────────────────────────────────────────────────────────────┤
│  Step 2: 等待 Baseline 窗口 (300秒)                            │
│  ├── 收集 [round_start - 300s, round_start) 的 Binance 价格   │
│  └── 计算中位数作为基准价格                                      │
├─────────────────────────────────────────────────────────────────┤
│  Step 3: 策略评估循环 (每价格tick触发)                          │
│  ├── GateKeeper: 检查 BTC 漂移 ≤ 0.15%                        │
│  ├── T1 入场: mid ≤ 0.44 时以 ≤0.40 的 FAK 市价买单吃单          │
│  ├── TN 加仓: 相对 T1 实际成交均价跌 20/40/60% 时加仓          │
│  └── 止盈: 达到 tp_target（20%, 逐层 −3%）时预挂 GTD SELL      │
├─────────────────────────────────────────────────────────────────┤
│  回合结束                                                        │
│  ├── 取消所有未成交订单                                          │
│  └── 重置状态，进入下一回合                                      │
└─────────────────────────────────────────────────────────────────┘
```

### 马丁格尔策略层级

| 层级 | 名义价值 | 触发条件 | 说明 |
|------|---------|---------|------|
| T1   | $1      | mid ≤ 0.40×1.10 且 GateKeeper PASS | 初始入场（taker 市价买，限价 ≤0.40） |
| T2   | $2      | 相对 T1 成交均价下跌 20% | 第一次加仓（maker GTD） |
| T3   | $4      | 相对 T1 成交均价下跌 40% | 第二次加仓（maker GTD） |
| T4   | $8      | 相对 T1 成交均价下跌 60% | 第三次加仓（maker GTD） |
| TP   | 全部    | 盈利 20% − N×3%（N=已成交加仓层数） | 止盈离场 |

**最大风险暴露：** $1 + $2 + $4 + $8 = $15/回合

T1 使用 taker 市价单的原因：$1 名义在 0.40 附近只有 2.5 股，低于 CLOB
对挂单（GTD）的 5 股最小限制；市价买单按 USDC 金额下单，可以直接吃单。
T2–T4 的金额在触发价上都能买到 ≥5 股，因此继续用 maker GTD。

止盈同理：T1 单独持仓（约 2.5 股）无法挂 GTD SELL，改为用 FAK 市价
卖出（限价 = mid − `SELL_SLIPPAGE_PCT`）；加仓后总股数 ≥5 时恢复 GTD 止盈。

### T4 入场闸门

T4 是最大的一层（8/15 USD），触发价只有 T1 成交均价的 40%。只看"跌了
60%"无法区分"市场过度反应"和"BTC 已经趋势性离开行权价"，所以 T4 在
通用加仓条件之外还要过三道闸（2026-10-31）：

1. **时间**：窗口剩余 < `T4_MIN_REMAINING_S`（默认 240s）不补，300s 的
   GTD 和止盈都来不及走完；0 = 关闭。
2. **行权价距离**：15 分钟代币本质上是"BTC 收盘在开盘价哪一侧"的概率。
   对 DOWN 仓位只有 BTC *高于*开盘价才算不利（UP 对称）：
   `z = 不利方向距离 / (σ_1m × √剩余分钟)`，只有 `z ≤ T4_MAX_STRIKE_Z`
   （默认 2.0）才补。σ_1m 用 60 秒振幅/2 估计（快速插针抬高 σ，缓慢
   趋势则维持下限），样本不足时用 `T4_STRIKE_SIGMA_PCT_PER_MIN`。
3. **不接飞刀**：mid 跌破 T4 触发价超过 `T4_MAX_GAP_BELOW_TRIGGER_PCT`
   （默认 5%）时不挂单；等价格回到预挂区，避免在下跌途中接单。
   0 = 关闭（旧行为）。

三道闸都**失败开放**（fail-open）：输入缺失（无 baseline / 窗口时间）时
不阻断，避免新的风控条件把补仓永久卡死；BTC 行情新鲜度由
`BTC_STALE_MAX_AGE_S` 负责。闸门在 drift 门之前执行，因此 T4 的
`T4_DRIFT_BYPASS_MIN_REMAINING_S` 时间豁免也不能绕过它们。每次决策的
z、剩余时间、σ 等指标写进 `engine.trigger_tN` / `engine.tN_skip_t4_*`
日志，用于后续校准阈值。

这些事件还会由 `src/infra/t4_journal.py` 镜像到
`<data dir>/t4_journal.jsonl`（`T4_JOURNAL_ENABLED` 开关，路径可用
`T4_JOURNAL_PATH` 覆盖）：T4 决策、成交、止盈/止损出场、回合结算各一行
JSON，方便长期积累后回看。注意 `/app/data` 需要持久卷才能跨部署保留。

### T4 后止损

T4 成交代表本轮已经打满计划仓位，之后继续下跌不再是马丁格尔可以摊薄的
"回调"。`_check_stop_loss` 在 T4 有成交后启用：

1. **第一段**：`mid ≤ T4成交均价 × (1 − STOP_LOSS_T4_DROP_PCT)` 且持续
   `STOP_LOSS_CONFIRM_S` 秒 → 用 FAK 市价卖出 `STOP_LOSS_STAGE1_SELL_PCT`（默认 60%）。
2. **第二段**：在第一段触发线基础上再跌 `STOP_LOSS_STAGE2_EXTRA_DROP_PCT`，
   或剩余 ≤`STOP_LOSS_TIME_EXIT_S` 且 mid ≤`STOP_LOSS_FLOOR`，或最后
   `STOP_LOSS_FINAL_EXIT_S` 秒且 mid <`STOP_LOSS_FINAL_MID` → 清掉剩余。

卖出前会撤销本回合所有未成交挂单（包括 TP GTD），避免股份被占用；成交
按 taker 计费。止损只会在 T4 有成交后触发，killswitch 激活时不会自动卖。

### GateKeeper（漂移门控）

GateKeeper 是防止追涨杀跌的关键机制，确保所有交易操作都在 BTC 价格相对稳定的条件下执行。

**计算公式：**
```
drift = |BTC_now - BTC_baseline| / BTC_baseline
允许下单: drift ≤ 0.0015 (0.15%)
```

**检查场景：**
- `can_enter()`: 入场时 drift + 价格双重检查
- `can_double_down()`: 加仓时 drift 检查
- `can_act()`: 止盈时 drift 检查

---

## 数据流架构

```
                    Binance                           Polymarket CLOB
                   BTC/USDT                               API
                       │                                    │
                       ▼                                    ▼
              ┌────────────────┐                 ┌─────────────────────┐
              │ binance_feed   │                 │    MarketWS         │
              │ - cache        │ ──────────────▶│ - price_cache       │
              │ - ring_buffer  │                 │   (last_trade_price)│
              └────────────────┘                 └──────────┬──────────┘
                       │                                   │
                       │  baseline 计算                    │
                       │                                   │
                       ▼                                   ▼
              ┌────────────────────────────────────────────────────┐
              │              StrategyEngine                         │
              │  - 入场判断 (_check_t1_entry)                     │
              │  - 加仓判断 (_check_average_down)                  │
              │  - 止盈判断 (_check_take_profit)                   │
              │  - GateKeeper drift 检查                          │
              └─────────────────────┬────────────────────────────┘
                                    │
                                    ▼
                           ┌────────────────┐
                           │ ActionRouter   │
                           └───────┬────────┘
                                   │
        ┌──────────────────────────┬┴───────────────────────┐
        ▼                          ▼                        ▼
┌───────────────┐         ┌──────────────┐        ┌──────────────┐
│ OrderManager   │ ───────▶│ CLOBClient   │        │  UserWS      │
│ (序列化订单)   │         │ (REST API)   │        │ (订单事件)   │
└───────────────┘         └──────────────┘        └──────────────┘
        │
        ▼
   ┌─────────┐
   │ SQLite  │
   │  (持久化) │
   └─────────┘
```

---

## 关键模块详解

### 1. WindowManager (window_manager.py)

**职责：** 跟踪 15 分钟窗口边界，管理窗口生命周期。

**核心逻辑：**
- 内部每 500ms 检查一次当前时间是否跨越 15 分钟边界
- 计算公式：`round_start = floor(now / 900) * 900`
- 检测到边界切换时调用 `tick()` 发现新窗口市场，并触发回调 `on_new_round(round_start_ts)`
- 窗口切换时：记录跨窗口 BTC 偏移、检测新窗口、更新价格

### 2. BinanceFeed (binance_spot.py)

**职责：** 订阅 Binance BTC/USDT 实时成交数据。

**关键组件：**
- `BinanceCache`: 最新成交价缓存
- `RingBuffer`: 时间边界 FIFO 缓冲区（默认保留 1200 秒）
- `compute_baseline()`: 计算基准窗口的中位数价格

### 3. MarketService / PolymarketClient (market_service.py / pm_client.py)

**职责：** 通过 Gamma REST API 发现 Polymarket 市场。

**Slug 格式：** `btc-updown-15m-{round_start_ts}`

**校验：** 只接受窗口包含当前时间的市场；按 Gamma `outcomes` 字段
映射 UP/DOWN token 顺序；同时拉取 `tick-size`。

**重试策略：** WindowManager 在窗口内每 5 秒重试 discovery，直到拿到
当前回合的市场（不再等到下一个 15 分钟边界）。

### 4. MarketWS (market_ws.py)

**职责：** 订阅 Polymarket CLOB 市场行情 WebSocket。

**订阅数据：**
- `best_bid_ask`: 买卖盘口
- `last_trade_price`: 最新成交价
- `tick_size_change`: 最小价格变动

### 5. StrategyEngine (engine.py)

**职责：** 策略决策核心，评估入场、加仓、止盈条件。

**Action 类型：**
```python
ActionOpenT1   # T1 入场
ActionOpenTN   # T2-T4 加仓
ActionTakeProfit  # 止盈
ActionCancelTier # 取消某层订单
```

**评估流程：**
1. 检查 GateKeeper drift 状态
2. drift 超限则禁止所有新订单
3. 未持仓则检查 T1 入场条件
4. 已持仓则检查加仓和止盈条件

**Tier 状态机（部分成交处理）：**
- `placed=True` → 订单已发出（可能部分成交）
- `filled=True` → 完全成交（`size_filled >= 99.5% * size_expected`）
- `placed=False` → 未下或已取消/过期
- **互斥**：每个 tier 只触发一次，直到完成或过期

### 6. GateKeeper (gatekeeper.py)

**职责：** BTC 价格漂移门控，确保交易在价格稳定时执行。

**状态快照：**
```python
@dataclass
class GateState:
    binance_baseline: float   # 回合开始基准价
    binance_now: float        # 当前价格
    drift: float             # 漂移率
    within_limit: bool       # 是否在允许范围内
```

### 7. RoundState / Position (round_state.py)

**职责：** 管理回合状态和持仓信息。

**Position 结构：**
- 4 个 TierSnapshot (T1-T4)
- 持仓统计：total_qty、total_cost、avg_cost
- 止盈目标：avg_cost * (1 + TP_PROFIT_PCT)

### 8. OrderManager (order_manager.py)

**职责：** 订单执行的唯一入口，保证线程安全。

**关键特性：**
- 所有写操作通过 `asyncio.Lock` 序列化
- 内存中维护订单映射：`client_order_id` ↔ `server_order_id`
- `reconcile()`: 与 CLOB API 同步订单状态
- **DRY_RUN 已移除**：所有订单直接发送到真实 CLOB

**订单标签格式：** `{round_start_ts}-{tier_name}-{uuid8}`

### 8b. GtdExpiryWatcher (gtd_expiry_watcher.py)

**职责：** 后台任务主动检测 GTD 订单过期。

**工作原理：**
- 每 2 秒轮询一次所有在飞 GTD 订单的 `expires_at`
- 如果 `now >= expires_at` 且订单未完成，触发 `on_order_expired`
- 调用 `position.mark_expired()` 重置 `tier.placed` 标志
- 通知 Engine 重新评估是否应该再次触发该层

**为什么需要它：** Polymarket 服务端会在 GTD 到期时取消订单，但事件可能延迟到达或 WS 连接断开。Watcher 确保引擎永远知道哪些订单已经失效。

### 9. CLOBClient (clob_client.py)

**职责：** 封装 py-clob-client-v2 的 REST API 调用。

**主要方法：**
- `place_gtd_buy()` / `place_gtd_sell()`: GTD 订单
- `cancel_order()` / `cancel_orders()`: 取消订单
- `get_balance()`: 查询余额
- `post_heartbeat()`: 心跳保活

### 10. HeartbeatLoop (heartbeat.py)

**职责：** 定时发送心跳，防止 GTD 订单被取消。

**规则：** Polymarket 要求每 10 秒至少一次心跳

**实现：** 每 5 秒发送一次（安全边际）

### 11. DB (db.py)

**职责：** SQLite 持久化存储。

**表结构：**
- `rounds`: 回合记录
- `orders`: 订单记录
- `fills`: 成交记录

---

## 配置管理

### config.toml (公开配置)

```toml
[general]
ROUND_PERIOD_S = 900          # 15分钟
WINDOW_PRE_OPEN_S = 300

[binance]
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
BUFFER_RETENTION_S = 1200

[baseline]
WINDOW_S = 300                # 基准窗口
MIN_SAMPLES = 30

[log]
LEVEL = "INFO"
HEARTBEAT_S = 5
```

### .env (敏感配置)

```bash
# Polymarket API 凭证
API_KEY=xxx
API_SECRET=xxx
API_PASSPHRASE=xxx
WALLET_ADDRESS=xxx

# 策略参数（四层马丁，最大暴露 1+2+4+8 = 15 USD）
TIER1_NOTIONAL=1.0
TIER2_NOTIONAL=2.0
TIER3_NOTIONAL=4.0
TIER4_NOTIONAL=8.0
ENTRY_PRICE_CAP=0.40
TIER2_DROP_PCT=0.20
TIER3_DROP_PCT=0.40
TIER4_DROP_PCT=0.60
TP_PROFIT_PCT=0.20
TP_DECREMENT_PCT=0.03
TP_MIN_PCT=0.05
BASELINE_DRIFT_LIMIT=0.0015
GTD_LIFETIME_S=300
```

---

## 技术亮点

### 1. 纯异步架构

- 所有 I/O 操作使用 `asyncio` + `websockets`
- 指数退避重连：1s → 2s → 4s → ... → 30s 上限
- 非阻塞设计，支持高并发行情处理

### 2. 状态机设计

- `RoundState` 管理每个回合的完整生命周期
- `Position` + `TierSnapshot` 精确追踪 4 层马丁格尔持仓
- 清晰的 `Action` 类型定义，便于扩展

### 3. 数据持久化

- SQLite + WAL 模式，提升并发性能
- 记录 rounds、orders、fills 三个核心表
- 支持重启后恢复未完成订单状态

### 4. 实盘模式（无 DRY_RUN）

代码中已移除 DRY_RUN：所有订单直接发送到真实 CLOB。凭证缺失时
CLOBClient 保持未配置状态，订单操作会失败而不是模拟成交。

### 5. 序列化订单操作

```python
async with self._lock:  # asyncio.Lock 保证并发安全
    await self._clob.place_gtd_buy(...)
```

---

## 潜在风险点

### 1. 马丁格尔策略风险

虽然有 4 层加仓保护，但在极端单边行情下可能面临较大回撤。建议：
- 设置每日最大亏损限额
- 监控连续亏损次数

### 2. GateKeeper 基准计算风险

如果基准价格计算出现偏差，可能导致策略失效。建议：
- 验证 `MIN_SAMPLES = 30` 是否足够
- 监控基准窗口内价格波动率

### 3. WebSocket 断连风险

虽然有重连机制，但断连期间可能错过关键行情。建议：
- 监控连接状态
- 考虑添加本地价格告警

### 4. UserWS 已集成

`user_ws.py` 已在主流程中启动，订单 UPDATE / TRADE 事件会实时同步到
OrderManager 与策略引擎；TierWatcher 作为 REST 兜底对账。

---

## 运行指南

### 前置条件

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置环境变量（项目根目录 .env 会被自动读取）
cp .env.example .env
# 编辑 .env 填入 Polymarket API 凭证

# 3. 按需调整 config.toml / .env
```

### 启动命令

```bash
# 实盘启动（无 DRY_RUN）
python -m src.main
```

### 日志输出示例

```
# 回合切换
INFO | round.transition prev_round_start=1720575000 new_round_start=1720575900

# 市场发现
INFO | gamma.market.resolved slug=btc-updown-15m-1720575900

# 基准就绪
INFO | round.baseline_ready baseline=67500.0 samples=45

# T1 入场
INFO | engine.trigger_t1 side=UP token=0x1234... last_trade=0.38

# 订单成交
INFO | engine.tier_filled tier=1 fill_price=0.38 avg_cost=0.38

# 止盈
INFO | engine.trigger_tp last_trade=0.46 tp_target=0.456 qty=10.5
```

---

## 扩展建议

1. **UserWS 已集成**: 订单 UPDATE / TRADE 事件闭环 + TierWatcher REST 兜底
2. **Dashboard**: 添加 Web UI 展示实时状态
3. **风险控制**: 添加每日亏损限额、连续亏损监控
4. **通知**: 支持 Telegram/Discord 告警
5. **回测框架**: 基于历史数据验证策略效果

---

*文档生成时间: 2026-07-10*
