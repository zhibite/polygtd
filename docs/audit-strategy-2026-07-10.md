# 入场 / 补仓 / 止盈 逻辑审计

- 审计人: code review (静态)
- 日期: 2026-07-10
- 范围:
  - `src/strategy/engine.py` (决策核心)
  - `src/strategy/round_state.py` (状态机)
  - `src/strategy/gatekeeper.py` (BTC drift)
  - `src/execution/order_manager.py` (下单 / 状态)
  - `src/execution/clob_client.py` (GTD BUY / GTD SELL / FAK SELL)
  - `src/execution/gtd_expiry_watcher.py` (GTD 到期清扫)
  - `src/main.py` (ActionRouter + wiring)
  - `src/config.py` (阈值 / martingale 系数)

本文档按严重度 (Blocker / High / Medium / Low) 列出。每条都给出代码位置 (`path:line`) 和建议修复方向。
未列出具体修复 patch，**因部分问题相互耦合** (例如 #1 不解决前，多个 followup 的修复方向都需要重新设计互斥路径)。

---

## 严重度图例

- 🔴 **Blocker** — 阻止策略按设计运行或会造成资金风险，**在投产前必修**。
- 🟠 **High** — 在常见路径中会触发明显偏差 (重复开仓 / 多算持仓 / 无法平仓 / 价格错位)，需要修复但短期靠环境兜底。
- 🟡 **Medium** — 边界场景下会偏离设计意图 (rounding / 字段不一致 / 锁粒度)。
- 🟢 **Low** — 日志 / 一致性 / 可维护性。

---

## 1. Blocker

### 🔴 B-1: 策略决策循环完全没被触发

- **路径**: `src/main.py`
- **症状**: `engine.on_binance_tick()` 与 `engine.on_token_tick()` 在整个 codebase 中没有任何调用方 (全局搜索 `on_binance_tick` / `on_token_tick` 仅在 `src/strategy/engine.py` 自身出现)。
- **影响**: `_evaluate()` 永远不会被调用 → `_check_t1_entry` / `_check_average_down` / `_check_take_profit` 全部死代码。T1 入场永远不发，补仓和止盈也不会被周期性评估。如果"看到 T1 placed 日志"过，那是旧版本。
- **例外**: T1 一旦发出后 `_make_fill_callback` / `_make_maker_fill_callback` 仍然会跑 — 因为 fill 是被动事件驱动。所以实际上第一笔以后才有反应。
- **修复方向**:
  - 路线 A (推荐): 把 `_midpoint_poll_loop` 改造为决策驱动器，每 0.5–1s 触发一次 `state.engine.on_binance_tick()` / `state.engine.on_token_tick(token_id)`。
  - 路线 B: 新增独立 `_evaluate_loop` task，每 250–500ms 调 `_evaluate()`。
  - **关键要求**: 同时只能有一个 _evaluate 在跑，引入 `asyncio.Lock` 或 in-flight 标志防并发。

### 🔴 B-2: TP 卖单的 fill 完全不进 position 状态，残仓永远清不掉

- **路径**:
  - `src/execution/order_manager.py:303-396` (place_tp_with_fallback)
  - `src/execution/clob_client.py:450-521` (place_fak_sell)
  - `src/strategy/engine.py:472-527` (_check_take_profit)
  - `src/main.py:202-234` (_make_fill_callback)
- **症状**:
  1. `_check_take_profit` 在 mid >= target 时把 `pos._tp_inflight = True`。
  2. router 调 `om.place_tp_with_fallback` → 发 GTD SELL；如果 75s 后未填满 → 发 FAK SELL。
  3. FAK 没有 tracked order (fill-or-kill 直接成交或撤单)，`by_server_id` 找不到。
  4. `_make_fill_callback` 内 `apply_fill_event(order_id)` 是 no-op (找不到 order)。
  5. `eng.on_order_filled(asset_id, order_id, price, size)` 在 `engine.py:174` 循环 `pos.tiers` 找匹配 — TP 单不在 `pos.tiers` → `log.debug("engine.fill_unknown")` 沉默丢弃。
  6. **结果**: `pos._tp_inflight` 仍是 True，`pos.total_qty` 未减，`pos._total_cost` 未减。下一次 _evaluate 命中 `if pos._tp_inflight: return` → 永远不再发 TP。
- **影响**: TP 触发后只要 GTD 部分填 + FAK 卖出残余，正常情况下残仓 clear；但因 fill state 没进 `_tp_inflight` mutex 释放路径，**第二次及以后 TP 全堵**。一次失败就坏。
- **修复方向**:
  - 在 `engine.on_order_filled` 里也接受 TP 单 (server_order_id 命中 TP 单的 metadata)，把卖出股的 fill 应用到一个 sell-side position 结构 (`pos.sells[]`) 或者直接更新 `_tp_inflight` / `total_qty`。
  - 替代方案: `place_tp_with_fallback` 完成 (无论 GTD-only / FAK-fallback / 全部失败) 后由 router 显式通知 engine 清 `_tp_inflight` 并扣减 `total_qty`。
  - **根本**: TP 卖单需要被 record 进 position state，目前 record_fill 只对 BUY tier 起作用。

### 🔴 B-3: GTD BUY 的 polling 回调和 WS 回调同时记账会重复累加 fill

- **路径**:
  - `src/execution/clob_client.py:226-265` (place_gtd_buy polling 触发 `_fire_fill_callback(token_id, order_id, avg_px, total_sz)`)
  - `src/execution/clob_client.py:387-394` (place_gtd_sell 同上)
  - `src/main.py:202-234` (`_make_fill_callback`: `fill_size` 是 cumulative)
  - `src/main.py:680-718` (`_make_maker_fill_callback`: `fill_size_inc` 是 incremental)
  - `src/strategy/round_state.py:226` (`record_fill` 永远 `size_filled += size`)
- **症状**:
  - `place_gtd_buy` 每 0.5s 轮询，发现在某次轮询有 fill → fire `_fire_fill_callback(token_id, order_id, avg_px, total_sz=累计)`.
  - 同时 `UserWS` 推 `maker_orders[]` 增量 arrive → `_make_maker_fill_callback` 把 `fill.size` (单 trade 增量) 喂进 `on_order_filled`。
  - 同一个 fill 被 polling 和 WS 双方通知到，`record_fill` 累加两次。
- **影响**:
  - `size_filled` 翻倍，可能超过 `size_expected * 0.995`，快速 `is_complete=True`，跳过部分 tier 的实际 fill cost (cost_filled 也会算大)。
  - `_total_cost` 和 `_avg_cost` 错位 (用了偏高的 fill_price 两次)。
  - 如果 WS 已经在更早到达后被 polling 又重复推送，`pos.baseline_price` 锚定值会偏。
  - Buy 后 `_avg_cost` (basis) 偏高 → `tp_target` 偏高 → TP 触发难度变高 → 该止盈不止盈。
- **修复方向**:
  - 在 OrderManager 增加 `last_fired_cumulative_size[server_order_id]` 单调比较器；fire 时只 fire (cumulative - last_fired) 的 delta；polling 和 WS 都走同一个 `apply_fill_event` + monotonic。
  - 进一步: 关闭 `CLOBClient.fill_callback` 的 polling 路径，把 fill 全部依赖 WS；polling 仅在 WS 失联恢复后做 reconcile。
  - **根本**: 单一信息源 (single source of truth) 决定 size_filled 增量。

### 🔴 B-4: TP 触发使用 `mid_price` 但下单用 `bid_price`，触发价和成交价错位

- **路径**:
  - `src/strategy/engine.py:472-527` (line 499: `if mid_price < tp_target: return`)
  - `src/execution/order_manager.py:303-396` (line 327-335: `place_gtd_sell(... price=price)`)
  - `src/main.py:178-185` (router 把 `action.price=bid_price` 透传)
- **症状**:
  - 触发条件: `mid_price >= avg_cost * (1 + TP_PROFIT_PCT)`。
  - 实际下单价: `bid_price` (best bid @ 触发瞬间)。在 0.95 元附近的 token，bid 通常比 mid 低 0.01–0.03 (≈ 1–3%)。
  - 假设 entry 价 = 0.40，TP_PROFIT_PCT = 0.20，则 `tp_target = 0.48`；当 mid = 0.48 时，bid 可能 = 0.46。
  - **实际成交价 0.46 vs 期望 0.48**，盈亏比并不如设计 20%。
  - 更糟: mid >= 0.48 时盘口可能很浅，挂 bid 0.46 几乎一定会被成交，但是**实际盈利只 ~0.06/股=15%**，不到 20%。
- **影响**:
  - TP 的盈亏比被默默压低。
  - 在 0.99 / 0.01 极端盘口 (90+% certainty)，`bid << mid`，实际可能平在 0.96 而非 0.998 → 滑点 ~3%。
- **修复方向**:
  - 改为: 触发条件维持 `mid >= target` (entry gate)，但下单价改为 `min(ask * 0.995, target)`，即 ask-side 报单以保证成交接近 mid。
  - 或者: 触发条件改为 `bid_price >= target`，**只有 bid 才真的能成交**，把 mid 和 bid 统一。
  - 并测试在浅盘口的成交率。

---

## 2. High

### 🟠 H-1: `place_tp_with_fallback` 阻塞 ActionRouter 90 秒，期间策略完全停摆

- **路径**:
  - `src/main.py:178-201` (router 直接 `await om.place_tp_with_fallback` — 这是同步栈链)
  - `src/execution/order_manager.py:303-396` (内部 `await asyncio.sleep(75)` 然后再做 fallback)
- **症状**:
  - `ActionRouter.__call__` 是 `engine._evaluate` 的 await 目标。整个决策循环 await 在 router 上 75–90 秒。
  - 在这期间: `engine._evaluate` 内 `_check_take_profit` 已经 set `_tp_inflight = True` (line 512)，所以新的 TP 会被 mutex 挡住，**但 binance drift 失败**或**重试入 T1/T2/T3/T4**也无法处理，因为 engine 还在等 router。
  - 整 90s 内，BTC 价格剧烈波动时无法响应 cancel / 二次评估。
- **影响**:
  - 策略对盘口突发反向行情 90s 无反应 (止损或风险止盈全失)。
  - CLOB 端的 GTD 单被取消、成交、或到期，全无 engine 端的策略应变。
  - 期间其他 feeder (Binance) 推送的 tick 在 `_evaluate` 上排队，但**等路由 await 返回**才能处理。
- **修复方向**:
  - `place_tp_with_fallback` 改为 `asyncio.create_task(...)`，`_check_take_profit` await `asyncio.sleep(0)` (只是 schedule，yield 一下) 然后 return。engine 的下一次 evaluate 仍然要 mutex 拦住重发 (这点现有 mutex OK)。
  - 或者拆成两阶段: 先 post GTD (返回 TrackedOrder，立即返回)，由 timer task 在 75s 后再走 fallback。
  - **关键**: mutex flag 必须与 fire-and-forget 兼容，确保 TP inflight 在 fallback 完成时正确清掉 (参见 B-2)。

### 🟠 H-2: TP 触发后 mid_price 已回落到 tp_target 之下也无法撤单

- **路径**:
  - `src/execution/order_manager.py:303-396`
  - `src/execution/gtd_expiry_watcher.py:88-133`
- **症状**:
  - TP 触发并 await 在 GTD 单上 → 突发利空，mid 跌穿 tp_target。
  - 引擎无法取消这张 GTD 单 (没有 cancel 路径)；也没法重新评估是否撤单改持 / 反手。
  - GTD 仍在盘口挂 bid → 任何 take-down 都会被吃，结果是以**比目标价还低**的价格卖出。
- **影响**:
  - TP 触发后如果市场快速反转，止损触发前已经把仓位以"目标价"的 90% 甚至更低平掉了。
  - 单边行情下损失放大。
- **修复方向**:
  - 给 TP 单配备**价格监测回调** (post-only GTD sell 可以挂 cancel-on-price-below-X)。
  - 或者在 TP fire-and-forget 后另起一个 monitor: mid < tp_target * (1 - tolerance) 时主动 `om.cancel(client_id)`。
  - 同样需要解决 B-2 的 TP fill-state 问题。

### 🟠 H-3: TP GTD 单的过期时间在 client 与 server 间相差 91 秒

- **路径**:
  - `src/execution/clob_client.py:182` (`gtd_seconds = max(lifetime_s, GTD_MIN_SEC + 1) = max(90, 181) = 181`)
  - `src/execution/order_manager.py:240` (`expiration_ts = int(time.time()) + gtd_lifetime = +90`)
  - `src/execution/gtd_expiry_watcher.py:97-104` (按 `tier.expires_at = +90` 检查过期)
- **症状**:
  - CLOB 端的实际 GTD expiration = `now + 181` (server side)。
  - `TrackedOrder.lifetime_s = 90`，`OrderManager` 写入的 `expires_at = now + 90`。
  - `gtd_expiry_watcher` 在 90s 后判定"过期"，发 `om.cancel(client_id)`。
  - CLOB 接到 cancel 时单可能正在 partial fill，cancel 处理可能成功也可能不。
  - 即使 cancel OK，原 TP 单实际上要到 181s 才真正到期 → **TP 流程在 client 看来"完成了"但 CLOB 上 behavior 不一致**。
- **影响**:
  - watcher 在 90s 错误地把还活着的 TP 当作 expired → `mark_expired` 清 `placed=False`。
  - `place_tp_with_fallback` 又基于原 CLOB 单进行 fallback (90s sleep 之前已 post) → 流程重入或异常。
  - 看起来 fallback 那段 polling / get_order (line 360) 还看得见 size_matched 增长，但已被前端 cancelled。
- **修复方向**:
  - **统一 lifetime**: 在 OrderManager 和 CLOBClient 之间统一用 `GTD_MIN_SEC + 1` = 181 作为 TP / BUY 的 max。同时看 `TP_GTD_LIFETIME_S` 是否还可以 < 180 — 如果 < 180，应该改 OrderManager 把它升到 GTD_MIN_SEC + 1 后**一致**写入 tracked order。
  - 或者，OrderManager 写入 `tracked.lifetime_s = max(90, GTD_MIN_SEC + 1)`，`expires_at = now + max(...)`，watcher 也按这个判断。

### 🟠 H-4: FAK SELL 的 `place_fak_sell` 把"no fills"视为 `success=False`，但 router 把 `None` / `False` 都视为 fallback 失败，不区分"FAK 取消未成交"和"网络错"

- **路径**:
  - `src/execution/clob_client.py:495-510`
  - `src/execution/order_manager.py:402-462` (place_sell return None on `not success`)
  - `src/main.py:178-201` (TP handler，对 `(None, None)` 只 log "tp_failed")
- **症状**:
  - `place_fak_sell` 在 `fills=[]` 或 `total_sz <= 0` 时返回 `success=False, error="FAK_no_fills"`。
  - `place_sell` 直接 `return None` (因为 `result.success` False)。
  - router 只看 `_gtd or _fak` 是否 Truthy，(None, None) 路径打 "tp_failed"。
  - 但实际是 FAK 没成交，仓位还在。TP inflight 没清。
- **影响**: 类似 B-2 的 mask，但更宽：FAK 失败 + watcher 后续 expire GTD 的话，会触发另一个 TP — 同样卡 B-2。
- **修复方向**: TP path 必须**显式 ack**，无论 GTD 还是 FAK / 失败，都要更新 `pos._tp_inflight` 和 `pos.total_qty`。

### 🟠 H-5: T1 entry 在 fill 之前其它 tick 进来若 BOT side 选择变了，会先入错边

- **路径**: `src/strategy/engine.py:298-368`
- **症状**: engine 同时评估 UP 和 DOWN，谁先 `mid <= cap` 就先入谁；但 mid 在两侧不对称跳动时，先凑齐 cap 的那一边可能被虚假 noise 触发。GTD BUY 用 `bid + BUY_STEP` — 如果实际 bid 已经因为别的大单推高，**place_buy 会拉到一个比 cap 还高的价位成交**(cap 检查在 strategy 层通过但 OrderManager 的 `_adjust_buy_price` 拉价无边)。
- **影响**: T1 entry 价格可能 > `ENTRY_PRICE_CAP`(0.40)，违反阈值设计意图。
- **修复方向**:
  - 在 `place_buy` 内对 `adjusted_price` 做 cap 检查，超过 cap 直接拒绝 / cancel。
  - 或者把 cap 检查移到 OrderManager 接口。

### 🟠 H-6: `_check_average_down` 和 `_check_t1_entry` 在多 tick 并发时可能同帧双发 T1 / T2

- **路径**: `src/strategy/engine.py:269-296`
- **症状**: 在 B-1 解决后，`_evaluate` 会被 tick 多协程并发调用 — `_check_t1_entry` 命中后 `await self._handler(ActionOpenT1(...))` 实际**不是同步锁**，新 tick 又进 _check_t1_entry，可能再次满足 entry 条件。
  - engine 内 `pos.entry_token is None` 在 await 期间仍然是 None，第二次 evaluate 同样能发 T1。
- **影响**: 同帧两份 T1 在盘口挂 → 既刷了 notional cap 又占盘口流动性。
- **修复方向**:
  - 在 `_check_t1_entry` 发 Action 前 set `entry_token/entry_side` 占位 (或加 `_t1_inflight: bool`)。
  - 整体给 _evaluate 加 in-flight 锁或 actor pattern。

### 🟠 H-7: 补仓 avg_drop baseline 是从 T1 avg_fill_price 锚定的，与 `_gate.can_double_down` 的 baseline 解耦，但 `next_trigger` 计算仍用 baseline — 如果 T1 是 partial 后继续 drop，T2 trigger 比预期浅

- **路径**:
  - `src/strategy/round_state.py:60-66` (`trigger_price` baseline avg_fill_price)
  - `src/strategy/gatekeeper.py:93-119` (can_double_down 用 caller 传的 baseline)
  - `src/strategy/engine.py:392` (baseline = `pos.baseline_price`)
- **症状**:
  - T1 partial 50% 时 baseline = 当前 partial fill 均价。
  - T2 trigger = baseline * (1 - 0.20)。
  - 但用户设计的"应该是从 T1 设计价 -20%" — 如果实际 T1 fill 平均高于设计价，结果 T2 trigger 偏高 (更难触发)；反之亦然。
- **影响**:
  - martingale 的心理预期是基于"设计价"，但 engine 实际是基于"实成交均价"。T1 fill 不好时，T2 触发更近；T1 fill 极好(买在低位)，T2 触发更远 → 整轮收益不对称。
- **修复方向**:
  - 在 `bind_server_order_id` 时把 "intended price" 也存下来 (例如 `order.price` = 下单价)，用此计算 baseline。
  - 或者文档化这个行为，承认 baseline = 实际 avg fill 价。

---

## 3. Medium

### 🟡 M-1: `size_expected` 与 `placed size` 因 buy_step 调整导致 floating point 不匹配，tier 可能永远 `is_complete=False`

- **路径**:
  - `src/execution/order_manager.py:100-109` (`_adjust_buy_price` → `bid + BUY_STEP`)
  - `src/main.py:110` (`size = action.notional / action.price` 用 engine 给的 bid)
  - `src/main.py:131` (`expected_price=order.price` 用 adjusted)
  - `src/strategy/engine.py:262` (`size_expected = t.notional / expected_price if expected_price > 0 else 0.0`)
- **症状**:
  - 例如: notional=2.0, bid=0.39 → size=5.128.
  - adjusted_price=0.391 (after buy_step).
  - order placed: size=5.128 @ 0.391, 实际 notional = 5.128 × 0.391 ≈ 2.005 (轻微超出 planned)。
  - size_expected = 2.0/0.391 ≈ 5.115。
  - filled size_matched=5.128 → is_complete @ 99.5% = 5.115 × 0.995 = 5.089, already 5.128 > 5.089 ✓ — 这个特定 case 是 OK 的；但在其他 tick / price 下，可能 5.128 × 0.99 (回踩略低 fill 价) = 5.077 size_filled → 5.077 < 5.089 → is_complete=False，**触发"complete"条件失败**。
- **影响**: round 中多个 tier 被 stuck 在 "placed but not complete"，watcher 会触发 expiry。补仓可能在 `pos.tier_is_saturated=True` 边缘出错 (cost_filled < cap)。
- **修复方向**:
  - `size_expected` 不再用 `notional / adjusted_price`，改用 placed size 的 99.5% 阈值：`size_expected = placed_size` (round 后存进 tier)。
  - 或者把阈值从 99.5% 降到 98%。

### 🟡 M-2: Tier `trigger_price` baseline 没有 fallback — 如果 T1 用 ask-side 成交导致 avg_fill_price > ask 的反向，baseline 异常

- **路径**: `src/strategy/round_state.py:60-66`, `record_fill` 在 `round_state.py:210-267`
- **症状**: `baseline_price = tier.avg_fill_price or price`。第一次 fill 已经 set，后续 fill 不会修改，但 if partial fill 平均价异常，无法 override。
- **影响**: 不会立刻爆炸，但锚定的 baseline 永远只用第一次 fill — 这恰恰是原意 (not 改成 weighted avg across all partial fills)，但代码注释说的"on first partial fill"和实际行为一致，**反向问题**：第一次 fill 是 0.005 shares partial，**baseline 偏差巨大**。
- **修复方向**:
  - 等待 T1 完成 (size_filled >= size_expected * 0.5?) 再锚 baseline，或者加权滑动。
  - 或者在 fill 累计到一定数量后才允许 T2/T3 触发。

### 🟡 M-3: `tier_remaining < tier.notional * 0.5` skip 阈值在 TIER_NOTIONAL_MAX_MULT=1.1 下几乎触发不到

- **路径**: `src/strategy/engine.py:438-443`
- **症状**: tier_remaining = `tier.notional * 1.1 - cost_filled`；当 cost_filled > tier.notional * 0.6 时 (即 0.6/1.1 = 0.545 of cap)，跳过。T2 notional=2，cost_filled > 2*0.6=1.2 即 skip — 这个条件命中不难；但用户预期是"如果上一次 GTD partial fill ~50% 才补"，实际是 >=60% 才补，意味着**前一次补仓超过 60% 才放弃再补**。
- **影响**: 这个保护层与 `tier_is_saturated`(cost >= notional * 1.1) 双层叠加冗余。删除 M-3 也安全。或合并到一个阈值。
- **修复方向**: 删除第二个阈值，统一用 `tier_is_saturated`，理由更清晰。

### 🟡 M-4: `pos.is_empty` 的 property 语义不严密

- **路径**: `src/strategy/round_state.py:165-167`
- **症状**: `is_empty` 要求 `not t.placed and not t.filled for t in tiers` 且 `_total_qty == 0`。但已经 sold out (即 TP FAK 把仓清零后) 时，`tiers[].filled` 仍然是 True (`record_fill` 只在 BUY tier 调用)，所以 `is_empty = False`。
  - 主要在 `engine.on_order_filled` line 166 `if self._round.position.is_empty: return` 这条分支被用上 — T1 真正入场前这个判断合理；但 sell 出完时这条判断的反义会触发**不必要的 log**。
- **影响**: 日志噪声，逻辑上没有 bug。
- **修复方向**: 增加 sell-side total_qty 跟踪，或者重新定义 `is_empty = total_qty == 0 and not any(t.placed for t in tiers)`。

### 🟡 M-5: entry 失败无重试

- **路径**: `src/strategy/engine.py:353-368`
- **症状**: T1 entry 一旦发出 await handler，如果 `om.place_buy` 返回 None (网络失败 / CLOB 拒绝)，engine 不知道 — `entry_token/entry_side` 没设，下次 evaluate 又可以重新挑一侧发。**但**engine 的中间状态这次没记下 `placed=True` 这个 tier，watcher 也不会 watch。OK 行为合理，但 `await self._handler(ActionOpenT1(...))` 后无 return check。
- **影响**: 失败后下次 tick 还会重试，没 bug；但 engine 没在失败时打 warning 日志 (line 135 "router.t1_placed" 是成功 log)。
- **修复方向**: `_check_t1_entry` 检查 router 是否实际 ack 成功 (需要在 router 里 `bool(order)` 反馈到 engine)。

### 🟡 M-6: `gtd_expiry_watcher` 直接调 `mark_expired` 而不通过 `engine.on_order_expired`

- **路径**:
  - `src/execution/gtd_expiry_watcher.py:88-133`
  - `src/strategy/engine.py:220-239` (`on_order_expired`)
  - `src/strategy/round_state.py:269-285` (`mark_expired`)
- **症状**: watcher 走 `rs.position.mark_expired(tier)`，跳过了 engine 自己的 log (`engine.tier_expired` line 232)。watcher 自己打了 `gtd_expiry.detected` log，但 engine 端的 "engine.tier_expired" 漏掉 — 监控/sentry 报警（基于 `engine.tier_expired`）会失效。
- **影响**: 监控可观测性问题。
- **修复方向**: watcher 调 `engine.on_order_expired(server_id)` 而不是直接 `mark_expired`。`on_order_expired` 调 `mark_expired` + log。

### 🟡 M-7: `_gate.can_double_down` "next tier trigger" 旁路与 `_check_average_down` 自身 trigger 是相同条件的两份代码

- **路径**:
  - `src/strategy/gatekeeper.py:93-119`
  - `src/strategy/engine.py:421-422` (`if mid_price > trigger: continue`)
  - `src/strategy/engine.py:432-434` (`if not self._gate.can_double_down(...) continue`)
- **症状**: `can_double_down` 内部再次计算 `next_trigger = baseline * (1 - next_tier_drop_pct)`。两个 drop-pct 表 (`TIER2_DROP_PCT` 等) 都被独立读取并独立计算。
- **影响**: 数据流耦合但单测覆盖力度可能不足；一处改 drop 阈值，另一处忘记同步。
- **修复方向**: 抽取 `compute_trigger_price(baseline, tier)` helper。

### 🟡 M-8: `_tp_inflight` mutex 在 `_check_take_profit` 设置但失败兜底缺失

- **路径**: `src/strategy/engine.py:487-526`
- **症状**: line 512 `pos._tp_inflight = True` 在 await handler 之前，handler 抛错时不重置。
- **影响**: TP 触发一次失败 → 永久锁住。
- **修复方向**: try/except 包住 await handler，失败时 reset `_tp_inflight = False`。

### 🟡 M-9: WS subscription 未订阅 TP 单的 token 时也可，TP 不受影响；但 cancel_market 用 `round_start_ts` 过滤，会同时取消仍在途的 GTD BUY（设计意图）

- **路径**:
  - `src/main.py:500-526` (`cancel_market`)
  - `src/execution/order_manager.py:126-131` (`open_orders_for_round`)
- **症状**: round 切换时取消上一 round 全部 order，但如果 BUY 还在 partial fill 状态会被 cancel，错失剩余 fill。
- **影响**: 低概率，但部分成交会丢。
- **修复方向**: round_end 不取消 BUY，只取消 GTD SELL (TP)，BUY 不取消 — 让它自己 expire (不到 180s 后被 CLOB 取消)。

---

## 4. Low

### 🟢 L-1: `_make_fill_callback` 注释 "fill_size is cumulative" 与 `_make_maker_fill_callback` "per-trade size (incremental)" 解释不清

- **路径**: `src/main.py:202-234`, `src/main.py:680-718`
- **修复方向**: 重写注释，明确二者都接到 `record_fill`，但二者语义不同。

### 🟢 L-2: `set_binance_baseline` 两侧调用 (immediate seed + sampled baseline) 可能短时间内重设

- **路径**: `src/main.py:329-374`
- **影响**: 两次 set_binance_baseline，最终值还是来自 5 分钟采样，OK 但有两套日志。
- **修复方向**: 仅保留采样，initial seed 即可用于早期 gate。

### 🟢 L-3: `bind_server_order_id` 给 `expires_at` 用 `order.expires_at`，但 order.expires_at 是下单时刻算的，没考虑 clob client 内部的 GTD_MIN_SEC 上调

- **路径**: `src/main.py:123-130`, `src/execution/order_manager.py:240` vs `src/execution/clob_client.py:182`
- **影响**: 与 H-3 同一个根因，但只对 Engine 端过期判断有影响。

### 🟢 L-4: `EntrySkipCap` 与 `_check_t1_entry` 触发后没有清理 log

- **路径**: `src/strategy/engine.py:346-351`
- **影响**: 日志量大。
- **修复方向**: 改为 sample log (every 30s) 或只在 cap_change 时打。

### 🟢 L-5: `ActionCancelTier` 数据类已定义但实际没有发出 action

- **路径**: `src/strategy/engine.py:60-66`
- **影响**: 死代码。
- **修复方向**: 删除或实际化 (e.g. drift gate 触发取消 BUY)。

### 🟢 L-6: `_make_user_ws_order_callback` 只处理 CANCELLATION / UPDATE，不处理 PLACEMENT

- **路径**: `src/main.py:721-752`
- **影响**: PLACEMENT 事件被 cache，但 engine 已经从 router bind 过 server_order_id 了，重复绑定可能没事但不严格。

---

## 5. 修复优先级建议

如果资源有限，按以下顺序修 (P0 → P4)：

| P  | 项          | 预估改动量 | 关联依赖 |
|----|-------------|-----------|---------|
| P0 | **B-1**     | medium    | 解锁整个策略循环 |
| P0 | **B-2**     | medium    | TP 残仓清算 |
| P0 | **B-3**     | medium    | fill 计量正确性 |
| P0 | **B-4**     | small     | TP 滑点控制 |
| P1 | H-1         | small     | 依赖 B-2 的解法 |
| P1 | H-3         | small     | 独立 |
| P1 | H-2 / H-4   | medium    | 依赖 B-2 |
| P1 | H-5         | small     | 独立 |
| P1 | H-6         | medium    | 依赖 B-1 |
| P2 | H-7         | small     | 可选 |
| P2 | M-1         | small     | 独立 |
| P2 | M-2         | small     | 独立 |
| P2 | M-3 ~ M-9   | small     | 独立 |
| P3 | L-1 ~ L-6   | small     | 文档/重构 |

---

## 6. 建议测试用例 (在修代码前后跑)

每条问题在修复后，应加单测 (或 e2e) 验证。建议先覆盖：

1. **B-1**: `_evaluate_loop` 每 0.5s 触发，验证 mid < cap 时会发出 `ActionOpenT1`。
2. **B-2**: TP 单 partial fill → engine 收到 fill → `_tp_inflight = False` → 下次 evaluate 重发 TP。
3. **B-3**: 同一订单同 fill 由 WS + polling 双向通知，断言 `size_filled` 只增 1 次。
4. **B-4**: mid=0.48 时 TP 触发，下单价 ≤ mid - spread (用 mock best ask/bid 仿真)。
5. **H-3**: `TrackedOrder.lifetime_s == expires_at - now` 永远 == `clob.expiration_ts - now`。

---

## 7. 附录: 关键路径速查表

| 文件 | 行数 | 角色 |
|------|------|------|
| `src/strategy/engine.py` | 527 | 决策核心 (T1/T2-T4/TP) |
| `src/strategy/round_state.py` | 334 | 状态机 (Position/TierSnapshot/RoundState) |
| `src/strategy/gatekeeper.py` | 119 | BTC drift gate + 平均建仓旁路 |
| `src/execution/order_manager.py` | 613 | 下单 / 取消 / 部分成交管理 |
| `src/execution/clob_client.py` | 619 | GTD BUY / GTD SELL / FAK SELL |
| `src/execution/gtd_expiry_watcher.py` | 141 | GTD 到期清扫 |
| `src/main.py` | 760 | ActionRouter + Wiring |
| `src/config.py` | 200 | 阈值 / martingale 配置 |

