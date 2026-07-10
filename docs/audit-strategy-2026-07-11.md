# 入场 / 补仓 / 止盈 全面审计 v2

- 审计人: code review (静态 + 动态)
- 日期: 2026-07-11
- 范围: `src/strategy/*`, `src/execution/*`, `src/main.py`, `src/config.py`
- 上一版: `docs/audit-strategy-2026-07-10.md` (v1, 已对照逐项复查)

## 严重度图例

- 🔴 **Blocker** — 阻止策略按设计运行或直接造成**资金风险** (多扣款 / 多发单 / 数据错乱)
- 🟠 **High** — 在常见路径中会触发明显偏差，需要修复但短期有兜底
- 🟡 **Medium** — 边界场景下会偏离设计意图
- 🟢 **Low** — 日志 / 一致性 / 可维护性

---

## TL;DR (优先处理顺序)

| P  | 项          | 简介                                               | 改动量 |
|----|-------------|--------------------------------------------------|-------|
| P0 | **🔴 B-1 (本轮新发现)** | `place_buy` 整段被复制粘贴，**每笔 BUY 会被下 2 次单** | 小 |
| P0 | 🔴 B-2      | 第一笔 GTD BUY 第一次 fill 后，`place_buy` 第二份不认识的 server_order_id 在 `_by_server` 没插入，被误认未跟踪；`place_buy` 调用方拿不到"第二批"的 server_id 会丢仓位追踪，但同金额**两次扣款** | 同上修 |
| P0 | 🟠 H-1     | T1 entry 候选 sid 后还没拿到 server_id 之前，因为 `_eval_inflight` 释放，新 tick 在落 `_round.position.entry_token` 前还能再次进入，导致**同 T1 多单** | 中 |
| P0 | 🟠 H-2     | `place_tp_with_fallback` `await asyncio.sleep(75)` **仍然阻塞** router 协程 75–90s，导致 RS / Binance tick 全部堆积；`_tp_runner` 已经 fire-and-forget，但 fallback 内部 sleep 仍占事件循环 | 小 |
| P0 | 🟠 H-3     | T2/T3/T4 的 `trigger_price` 用 `baseline = pos.baseline_price = T1 实成交均价`，**T1 滑点会线性放大到 N 个档位**，违反 martingale 设计意图 | 中 |
| P1 | 🟡 M-1     | T1 时间门 `T1_ENTRY_WINDOW_S=300` 是从 `round_start_ts` 计算，但 `round_start_ts` 在 `_on_new_round` 里被设定**有时差 (秒级)**，且 vs `window_start` 来自 slug timestamp 二者可能存在 UTC/polymarket 平台误差 | 小 |
| P1 | 🟠 H-4     | `T2_DROP_PCT / T3_DROP_PCT / T4_DROP_PCT` (0.20/0.40/0.60) 设计上呈 0.20 等差，但**实际策略永远只用 baseline 的相对位置**触发，没有显式"按 cost-basis 累积的预期跌幅" | 中 |
| P2 | 🟡 M-2 ~ M-7 | 见下文                                            | 小   |

---

## 1. Blocker

### 🔴 B-1 (新): `OrderManager.place_buy` 含整段重复代码，**每一次 BUY 都会向 CLOB 下 2 次单**

- **路径**: `src/execution/order_manager.py:153-257`
- **症状** (确定): lines **175–201** 与 lines **203–221** **几乎 100% 重复**。两次都做了:
  1. `adjusted_price = await self._adjust_buy_price(token_id, price)` — 走 PM orderbook 的 REST
  2. `result = await self._clob.place_gtd_buy(...)` — **真正 create_and_post_order 到 CLOB**
  3. `if not result.success: return None`
  4. `server_id = result.order_id`

  最终只有**第二批**的 `result` 进入 `TrackedOrder`，而第一批下单单已经在 CLOB 上挂着、可能已经 fill，但 engine 没有它的 server_order_id。

- **影响** (🔴 资金级):
  - **资金风险**: 每一笔 BUY 实际消耗 2× 预算。T1 $2 → 实际 $4，T3 $4 → 实际 $8。一轮 4 档理论 max $16，实际可能花到 $32，全部超过 `total_max_notional`。
  - **仓位追踪**: 第一张单 fill → `on_maker_fill` 走 `_make_maker_fill_callback` → `by_server_id(...)` 返回 `None` (`_by_server` 没插)，被 `om.untracked` log 静默丢弃。`record_fill` 不会被调用，`_total_qty` 不增。第二张单的 fill 才进入 engine。
  - **结算误判**: 如果第一张以更优价格 fill (snap 后可能更好)，用户实际成本被记成"第二批"的价格，TP 触发基于的 `_avg_cost` 偏高。
  - **下单失败链路**: 第一份 GTD place 成功、第二份失败时 `_by_server` 完全空，但 CLOB 上有未追踪的活单 → 等到 GTD 过期/有人吃单 → 用户花了钱，bot 完全无感。

- **修复方向**:
  - 删掉 lines **203–221** 的复制块即可（一行 git 命令级别修复）。
  - 加单测: mock 一个 `_FakeCLOB.place_gtd_buy` 计数，断言调用次数 = 1。

---

### 🔴 B-2: v1 B-2 / B-3 / H-3 修复**未真正生效**于生产路径

承接上版审计。本轮逐项复查：

- **B-2 (TP fill state)**: `Position._tp_snapshots` 已经存在，`record_tp_fill` 在 `engine.on_order_filled` 末尾被调用 (engine.py:210) → **OK**。但前提是 `_fire_fill_callback` 已经把 TP 卖出股的 fill 喂进 `on_order_filled`。检查发现：
  - `place_gtd_sell` 在 `place_gtd_sell` 内 (clob_client.py:508-510) 调 `_fire_fill_callback`，OK。
  - `place_fak_sell` (clob_client.py:652) 同样 OK，但 FAK 的 fill_id 是空的，`FillDeduper` 走纯 bookmark 路径，仍有效。
  - 然而 **TP 卖单的 server_order_id 必须先在 `pos.register_tp(...)`** 注册 → 路由在 `main.py:218-232` 调 → **OK**。
  - 风险点: 若 `place_tp_with_fallback` 内部 sleep 期间又触发 fill，**回调路径使用的还是原 GTC server_id**；若 CLOB 因为 GTD 到期 (`gtd_poll_timeout`) 后被 cancel，**fill signal 可能丢失** (audit v1 M-6/M-9)。

- **B-3 (deduper)**: `FillDeduper` 已经 single-source，OK；但 `place_gtd_sell` 仍然两处 fire fill (`place_gtd_buy` 仅一处，是 OK 的)；deduper 的 `bookmark[id]` 单调递增，跨 WS+polling+FAK 安全。

- **H-3 (lifetime)**: `_CC.GTD_MIN_SEC + 1 = 181` 已经统一用在 BUY/SELL/TP，OK。但 `TP_GTD_LIFETIME_S = 90` 通过 `effective_lifetime = max(90, 181) = 181` 提升，**意味着 TP 默认也变 181s**，原意是 90s，**OK 但与文档注释 (`TP_GTD_LIFETIME_S is short on purpose`) 不一致**。

---

### 🔴 B-3 (本轮新): `tp_runner` 创建后**没有 reference hold**，可能被 GC

- **路径**: `src/main.py:280` `asyncio.create_task(_tp_runner())` — 没有把 task 保存到任何 list/dict。
- **症状**: `asyncio.create_task` 的 task 对象如果没有 strong reference，可能在创建它的协程 yield 前就被 GC 掉，导致 `_tp_runner` 不被执行（极小概率，但 Python 3.11 之前确实存在）。PEP 3156 也提醒"建议保留 reference"。
- **影响**: 在压力 / GC 触发期，`_tp_runner` 没起来，**整个 TP 流程就丢了**：GTD 单已挂 (CLOB 上挂着)，但 engine 不 wait fallback，也不 register_tp，**完全没法退出**。
- **修复方向**:
  - 加 `state._bg_tasks: set[asyncio.Task]`，`asyncio.create_task(_tp_runner())` 后立即 `state._bg_tasks.add(t); t.add_done_callback(state._bg_tasks.discard)`，与 `gtd_expiry_watcher._bg_tasks` 同样模式。
  - 单测: 模拟 GC，并断言 fill_event 仍能进 TP snapshot。

---

## 2. High

### 🟠 H-1 (v1 H-6 partial): T1 entry 在 `_eval_inflight` 释放 → `_round.position.entry_token` 写完之间存在窗口

- **路径**:
  - `src/strategy/engine.py:317-322` (`_eval_inflight` 仅在 `_evaluate` 期间 True)
  - `src/strategy/engine.py:416-421` (`await self._handler(ActionOpenT1(...))` ← 这里让出事件循环)
  - `src/main.py:121-124` (router 写 `position.entry_token = action.token`)
- **症状**: `_evaluate` 的 `_eval_inflight = False` 在 `finally` 里即会被 clear；下一次 `_evaluate` 在 router 设置 `entry_token` 之前就进入 `if pos.entry_token is None: check T1`，**重复触发 T1**。
- **影响**: 同帧下两张 GTD BUY 在盘口；**资金 double-spend** (`place_buy` 自带的双发问题还会再放大 2x = 4x 总单)。
- **修复方向**:
  - 把 `entry_token` 的写入从 router 移到 engine 内 _evaluate 调 handler 之前 (engine.py:407 附近)。
  - 或在 `_check_t1_entry` 拿到 `_handler` 之后立刻 `self._round.position.entry_token = token; entry_side = side` (拿锁语义)，再 `await handler`。
  - 或换更可靠的并发原语：把 `StrategyEngine._evaluate` 整体替换为一个 actor 模式 (`asyncio.Queue` + single consumer task)。

---

### 🟠 H-2: `place_tp_with_fallback` 内 `await asyncio.sleep(wait_s)` 仍占事件循环

- **路径**: `src/execution/order_manager.py:438-439`
- **症状**: v1 H-1 fix 已经把外层 router 的 `await place_tp_with_fallback(...)` 替换为 `asyncio.create_task(_tp_runner())`。但**`place_tp_with_fallback` 函数内部仍然 `await asyncio.sleep(wait_s)` (最多 166s)**，这只是事件循环级别不影响其他 task 调度 (asyncio sleep 不阻塞 thread) **—— 这一点 v1 修正确保了 fire-and-forget 后主循环继续跑**。所以这一项**从 Blocker 降级到 Low**。
- **修复方向**:
  - 用 `asyncio.create_task` 取代内部 sleep + sequential fallback；使 TP 变真正并行 (GTD 到期后才起 FAK 而不是先阻塞)。
  - 或直接合到 `_tp_runner`，把 sleep 提到 task 顶层。

---

### 🟠 H-3 (新): **T2/T3/T4 trigger_price = baseline * (1 - drop%) 但 baseline = T1 实成交均价**

- **路径**:
  - `src/strategy/round_state.py:394-396` (`baseline_price = tier.avg_fill_price or price` —— 在 T1 fill 后写入 `baseline_set=True`)
  - `src/strategy/round_state.py:61-67` (`trigger_price = baseline * (1 - drop_pct)`)
- **症状**: 设计意图是"baseline = T1 设计价 / 窗口开盘价"，但代码实际把 baseline 锚定到**T1 真实成交均价**。考虑:
  - T1 滑点好 (例如 bid 0.395 vs cap 0.40，fill 0.40 才进) → baseline = 0.40 → T2 trigger = 0.40 × 0.80 = 0.32。但**用户期望的 baseline 是 0.40，T2 trigger 0.32 是对的** —— 这个 case OK。
  - T1 滑点差 (例如设置买 0.40，实成 0.45 — 错位 fill) → baseline = 0.45 → T2 trigger = 0.36。**但下一档也要看到 0.36 才补，深于用户的"0.40 - 0.20 = 0.32"**。整个 martingale **under-triggered**，仓位风险更低但命中率也更低。
  - 反之，T1 实成价 < 0.40 (大单 slip 后反弹) → baseline < 0.40，T2 trigger < 0.32。**提前触发**，暴露过大。
- **影响**: 偏向保守 / 激进不对称，**取决于 T1 fill 滑点的方向**。
- **修复方向**:
  - 引入 `baseline_target_price`: round start 时的 token mid (e.g. window_open_up)，与 `baseline_price` (T1 实成均价) **分别用作不同语义**:
    - `_check_average_down` 改用 `baseline_target_price`；
    - `Position.avg_cost` 仍按 T1 实成均价 (影响 TP)。
  - 或: 在 `_check_t1_entry` 决策时记录 `desired_entry_price = bid_price`，把它作为 baseline 锚点 (无视 fill 后实际 avg)。

---

### 🟠 H-4 (新): `TIER2_DROP_PCT / TIER3_DROP_PCT / TIER4_DROP_PCT` 0.20/0.40/0.60 **与 `TIER*_NOTIONAL` 的几何匹配性问题**

- **路径**:
  - `src/config.py:103-106` (`TIER1_NOTIONAL=2 / TIER2=2 / TIER3=4 / TIER4=8`)
  - `src/config.py:118-120` (`TIER2_DROP_PCT=0.20 / TIER3=0.40 / TIER4=0.60`)
- **症状**: 
  - 用户文档化意图: martingale 的"gain back all losses on small move"需要 trigger_pct 满足 `TIER_N+1_NOTIONAL ≈ TIER_N_NOTIONAL / (1 / drop_step_loss_factor)`。
  - 实际: 各 tier trigger 间距是 0.20/0.40/0.60，即 **触发门槛本身是 absolute difference**，而非与 notional 的反比关系。
  - 复合: T3 notional = 4，T3 trigger -40% from baseline；T4 notional = 8，T4 trigger -60% from baseline。**T4 满档约 $8 在 60% drop 区** —— 如果 token mid 真跌到 0.10，T4 fill size = 8/0.10 = **80 shares**，而 CLOB 5-share minimum 平易超过，但 price impact $8 × 0.10 = $0.80 是 nominal 风险敞口。**整轮最大 nominal = T1..T4 total × 0.10 mid = $16 × 0.10 = $1.60**，理论上远小。
  - 但若盘口真的 drop 60%，**实际盘口可能非常浅** (0.10 区间挂单薄)，GTD BUY 大概率不 fill → expiry → 循环过期。
- **影响**: martingale 设计的"每次补仓覆盖到 avg_cost"数学关系 (T4 fill @ avg ≈ 1/(1+Σ loss))，**在这里没有显式校核**。给定 drop 0.20/0.40/0.60 和 notional 2/2/4/8，**T2+T3+T4 fill 后 avg_cost 公式可以推，但代码未实现 assertion** (例如 `gatekeeper.can_double_down` 只看 trigger hit，不检查 notional 平衡性)。
- **修复方向**:
  - 加单测: 模拟 T1@0.40, T2@0.32, T3@0.24, T4@0.16 → 算 avg_cost = (0.40·5 + 0.32·6.25 + 0.24·16.67 + 0.16·50) / total_shares ≈ 实际填的 avg 与 TP 目标差距。
  - config: 把 drop_pct 改成 **true martingale reverse** — `drop_pct_n = 1 - (sum_prev_notional / (sum_prev_notional + TIER_N_NOTIONAL))`。例如 T1 2, T2 2 → T2 drop = 1 - (2/(2+2)) = 50%。

---

### 🟠 H-5 (新): `gatekeeper.can_double_down` `next_tier_drop_pct` 计算重复

- **路径**:
  - `src/strategy/gatekeeper.py:110-117` (`can_double_down` 内部计算 `next_trigger`)
  - `src/strategy/engine.py:486-488` (`_check_average_down` 调用前不重复计算但用 `TierSnapshot._next_tier_drop`)
- **症状**: 同一公式 (`baseline × (1 - drop)`) 在 gatekeeper / engine / round_state 三个地方都各算一遍 (v1 M-7 的延续)。
- **影响**: 如果 `tier_drop_pcts` 配置变更，3 处都要同步；现状下不变，但维护脆弱。
- **修复方向**: 抽 `compute_next_trigger(tier_index, baseline)` helper (`tier_drop_pcts[tier]` × baseline)，三处共用。

---

### 🟠 H-6 (新): `place_buy` bug 加上后，每 tick 250ms 评估 × 多源 tick 触发**叠加效应**

- **路径**: `src/strategy/loop.py` (250ms tick) + `src/main.py:533-539` (midpoint poll 不再触发 evaluate，但 BinanceFeed 不调 on_binance_tick，看 `binance_spot.py`)
- **症状**: v1 修 B-1 fix 用 `DecisionLoop` 单驱；后续 main.py 在 midpoint poll 注释说"loop is now solely driven by DecisionLoop"，OK。但 `DecisionLoop._tick` 内部如果 `_handler` await 慢，**250ms 之后新一 tick 进来即被 `_inflight.locked()` drop** —— OK。
- **影响**: 单线程 drop-on-reentry 是稳定设计，不再叠加。这条本质上是**验证 B-1 修复后的反推**: 一旦 B-1 修了，H-6 自然消失。

---

### 🟠 H-7 (新): `cancel_market` 把 BUY 也清掉，但 _avg_cost 中有 partial fill 残留

- **路径**: `src/execution/order_manager.py:621-647` (`cancel_market`) — 把 `placed` + `live` 全 cancel
- **症状**: 在 `cancel_market` 之前若有 partial fill 已经 `record_fill` 进 `_total_cost`，partial fill 已"花出去的钱"**没有 unwind**：用户钱被扣了，但 `record_fill` 累计后 `_avg_cost` 与已扣款不再关联 (`_by_server` 里 order 状态变 cancelled)。
- **影响**: 新的 round 开始后，`pos = Position()` 全新，新一轮 `_total_cost = 0`，**上一轮的残仓/残钱就这么丢了**。未平仓部分由 settlement 走 Polymarket resolution，但**bot 内账本不再反映这部分**。
- **修复方向**:
  - DB persistence 加 `partial_fill_residual` 账目；新 round 显式 reset，但日志保留上轮没收尾的部分。
  - 或: 在 `_on_new_round` 调用 `state.engine.on_round_end` 之前先 reconcile：以 `pm_client.get_balance(token)` 调合最新 token balance vs `_total_qty`，差额记为 `round_end_orphan`。

---

## 3. Medium

### 🟡 M-1: `_on_new_round` 把 `set_binance_baseline` 调 2 次 (immediate seed + sampled)

- **路径**: `src/main.py:436-442` (immediate) + `src/main.py:470-472` (sampled at 5min)
- **症状**: 即时 seed 与 5min sampled 都设 baseline，最终值以 sampled 为准；engine 用后者。OK 但日志与两侧 set 出 2 倍。
- **影响**: 低。
- **修复方向**: 仅保留 sampled，初始 seed 直接给 `GateKeeper` 一个 placeholder 让 gate 至少 parse。

---

### 🟡 M-2: `cancel_market` 遍历 `placed` + `live`，但 FAK `place_sell` 写 `status="filled"` 不被 cancel；OK 但 BUYs should race against cancel

- 与 H-7 同一根因，独立列条说明 FAK path 不受影响。

---

### 🟡 M-3: TP-GTD-LIFETIME = 90 但 CLOB clamp = 181，意味着**TP 实际活 181s**

- **路径**: `src/execution/clob_client.py:289` (clamp) + `src/execution/order_manager.py:172` (BUY 也 clamp = 181) + `src/execution/order_manager.py:293-294` (TP 也 = 181)
- **症状**: comment 上写 "TP_GTD_LIFETIME_S is short on purpose"，**实际生效都是 181**，文档偏离实现。
- **修复方向**: 把 `TP_GTD_LIFETIME_S` 注释改 = 180，或 **完全统一 BUY/SELL 都用 180** (改动一处 config)，减弱模块特化带来的认知负担。

---

### 🟡 M-4: `_eval_inflight` drop-on-reentry + `DecisionLoop._inflight` 双重锁

- **路径**:
  - `src/strategy/loop.py:50,94-98` (`DecisionLoop._inflight`)
  - `src/strategy/engine.py:100,317-322` (`StrategyEngine._eval_inflight`)
- **症状**: 两层 in-flight 守卫，**互不感知**。如果 `DecisionLoop._inflight` 持锁但 `engine._eval_inflight` False (不可能，因为 evaluate 是 loop 直调)，会混乱；实际两层都 True 时行为仍正确。
- **影响**: 低。
- **修复方向**: 删掉其中一层 (`engine._eval_inflight` 已不被需要 — DecisionLoop 是唯一驱动器)。

---

### 🟡 M-5: `bind_server_order_id` 取 `self._round.position.tiers[tier - 1]`，但 `engine.py:286` 没校验 `tier in 1..4`

- **路径**: `src/strategy/engine.py:286`
- **症状**: 如果 caller 传 `tier=0` 或 `tier=5`，会 IndexError。可读性 OK 但 missing guard。
- **修复方向**: 加 `if not (1 <= tier <= 4): log.error; return`。

---

### 🟡 M-6: `_make_fill_callback` 把 `apply_fill_event` (累加) 与 `mark_filled` (状态) 都调一边

- **路径**: `src/main.py:316-329`
- **症状**: `apply_fill_event` 用 `max(prior, new)` 单调比较；`mark_filled` 再看 `size_matched >= size * 0.995`。两个函数都基于同一个填量，**两次重复 if 判断**。
- **影响**: 可读性 + 微小 CPU。
- **修复方向**: 把 `mark_filled` 内部直接调 `apply_fill_event` (合并到一个语义函数 `ingest_fill`)。

---

### 🟡 M-7: `tier_remaining < tier.notional * 0.5` 跳过二次补单 — 与 `tier_is_saturated` 重叠

- **路径**: `src/strategy/engine.py:491-497` (`tier_remaining < tier.notional * 0.5`)
- **症状**: 同一个保护阈值 (`TIER_NOTIONAL_MAX_MULT = 1.1`)，两个 if 串行判断。删 0.5 threshold 也安全 (v1 M-3 已识别)。
- **修复方向**: 删 `tier_remaining < tier.notional * 0.5` 检查，统一用 `tier_is_saturated`。

---

## 4. Low

### 🟢 L-1: `_fire_fill_callback` 第一参数 `token_id` 未被 `om.apply_fill_event` 使用

- **路径**: `src/execution/clob_client.py:737-774` (`_fire_fill_callback`)
- **症状**: `token_id` 被获取但仅在 `_deduper.submit` 内部签名使用 (外层无消费)；router / engine 用 `asset_id` 走另一条路。冗余字段。
- **修复方向**: deduper 不需要 token_id（bookmark 只按 order_id），把该参数去除。

---

### 🟢 L-2: `record_tp_fill` 中 `if sold <= 0: ... return True;` 返回 True 但仍算"applied"

- **路径**: `src/strategy/round_state.py:311-316`
- **症状**: 当 `sold` (卖家 size) > 当前持仓时 (defensive)，直接返回 True 而没 reduce，但日志没记录"over-sell"。
- **修复方向**: 加 `log.warning("position.tp_oversell_defensive_noop")`，便于 trace。

---

### 🟢 L-3: `TrackedOrder.size_matched` 是 instance 字段，但 dedup 还在 CLOBClient 内独立存

- **路径**: `src/execution/clob_client.py:64-150` (`FillDeduper` 内的 `_bookmark`)
- **症状**: 同一事实（matched size）存了两份。TrackedOrder 里 `size_matched` 在 `apply_fill_event` 才更新；而 `_bookmark` (CLOBClient 内部) 仅 consumer 字段。**一致性**靠调用路径保持 (如果多源调用漏一个就会错)。
- **修复方向**: 删 deduper 内的 bookmark，改用 `TrackedOrder.size_matched`。这迫使所有 fill-event 必须先确认有 tracked order → **更安全**。

---

### 🟢 L-4: `cancel_market` 的 `client_ids` 返回，但调用方 (`_on_new_round`) 未使用返回值

- **路径**: `src/execution/order_manager.py:621` vs `src/main.py:409`
- **症状**: 一致性 OK，但 dead return value hint。
- **修复方向**: 调用方加 `if state.order_manager: await state.order_manager.cancel_market(prev)` + 接收返回值 (或加 `_ =` 显式 ignore)。

---

### 🟢 L-5: `Position` 内 `is_empty` 在 sold_out 后仍然 False

- **路径**: `src/strategy/round_state.py:206-208`
- **症状**: TP FAK 清完仓后 `tiers[].filled` 还是 True (record_fill 只在 BUY tier 调)。
- **修复方向**: 在 `record_tp_fill` 调用末，把任意 BUY-tier 的 state 不动，但**新增 `pos.sold_out: bool` 标志**；`is_empty` 重新定义为 `not any(t.placed for t in tiers) and self._total_qty == 0`。

---

### 🟢 L-6: `engine.on_binance_tick / on_token_tick` 现在**没有 caller** (v1 B-1 修后)

- **路径**: `src/strategy/engine.py:135-141`
- **症状**: DecisionLoop 不调 `on_binance_tick` / `on_token_tick`，直接 `_evaluate`。
- **影响**: 死方法；删之；或在 comment 中标注 "kept for back-compat with custom driver"。

---

## 5. 修复优先级

| P  | 项           | 改动量 | 关联依赖 |
|----|-------------|------|---------|
| P0 | **B-1 (新 Blocker)** | 极小 | 删除重复块，加 1 个 mock 单测 |
| P0 | **B-3 (新 Blocker)** | 极小 | 保留 task reference |
| P0 | H-1         | 小    | 写 entry_token 时机提前到 engine |
| P1 | H-3 (新)    | 中    | 引入 baseline_target_price |
| P1 | H-4 (新)    | 中    | drop_pct 公式与 notional 匹配性分析 |
| P1 | H-7 (新)    | 中    | round_end reconcile DB |
| P2 | H-5, M-1~M-7, L-1~L-6 | 小 | 各自独立可批改 |

---

## 6. 关键路径速查表 (2026-07-11)

| 文件 | 行数 | 角色 |
|------|------|------|
| `src/strategy/engine.py` | 586 | 决策核心 (T1/T2-T4/TP) |
| `src/strategy/round_state.py` | 500 | 状态机 (Position/TierSnapshot/TpSnapshot/RoundState) |
| `src/strategy/gatekeeper.py` | 119 | BTC drift gate + 补仓旁路 |
| `src/strategy/loop.py` | 99 | 250ms 驱动循环 |
| `src/execution/order_manager.py` | ~735 | 下单 / 取消 (含**复制粘贴 bug**) |
| `src/execution/clob_client.py` | 783 | GTD BUY / GTD SELL / FAK SELL + FillDeduper |
| `src/execution/gtd_expiry_watcher.py` | 141 | GTD 到期清扫 |
| `src/execution/heartbeat.py` | 111 | 5s 心跳 |
| `src/main.py` | 912 | ActionRouter + Wiring |
| `src/config.py` | 209 | 阈值 / martingale 配置 |
| `src/clob_l1_auth.py` | 184 | L1 wallet 认证 |

---

## 7. 必须新加的单测 (建议在修复前先写)

1. **test_b1_v2_buy_single_calling**: mock `_FakeCLOB.place_gtd_buy` (count=0)，断言调用 1 次。
2. **test_b3_tp_runner_not_gc**: 在 `_tp_runner()` 主动 `del locals()` 模拟 GC，断言 `pos.register_tp` 仍被调用。
3. **test_h1_concurrent_evaluate_emit_one_T1**: 模拟两个 DecisionLoop tick 同时 access engine (锁释放后立即再 trigger)，断言 `engine._handler` 被调 1 次。
4. **test_h3_baseline_uses_target_not_fill_price**: T1 实成 0.45 时，断言 T2 trigger = 0.40 * 0.80 = 0.32 (按设计)。
5. **test_h7_round_end_orphan**: partial fill 50% 收到 cancel_market 后，断言 DB 有 `partial_fill_residual` 记录。

---

## 8. 总结

v1 (2026-07-10) 审计罗列的 B-1 / B-2 / B-3 / B-4 / H-1 / H-3 / H-4 等已**部分修复并单测通过** (tests/test_audit_fixes_2026_07_10.py)。

本轮 (v2) 新发现的**核心**问题：

1. **B-1 复制粘贴 bug** —— 一次性可能导致 2× / 4× (叠加 H-1) 的扣款，必须第一时间修复。
2. **B-3 GC 风险** —— 中等概率事件，但发生时 TP 全路径失效。
3. **H-3 baseline 锚点语义偏差** —— 设计意图 vs 实现偏 1 档，**修改 baseline_target_price 即可**。
4. **H-4 martingale 数学关系缺失 assertion** —— 加单测可暴露。

代码整体结构清晰，分层 (strategy/execution/market/feeds/persistence/infra) 健康，deduper / expiry_watcher / heartbeat 都是好设计。**P0 修复后即可投产**，P1 应在首周内完成。
