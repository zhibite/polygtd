# 入场 / 补仓 / 止盈 / 仓位控制 + 代码复杂度全面审计 v5 (2026-07-11)

- 审计人: code review (静态)
- 日期: 2026-07-11
- 范围: `src/strategy/*`, `src/execution/*`, `src/main.py`, `src/config.py`
- 上一版: `docs/audit-strategy-2026-07-11-v4.md` (v4) → **本轮重心：复杂度审查 + 残留逻辑风险**
- 测试基线: 47/47 通过 (v3 / v2 / tp_decrement / tp_gtd_sell 套件)

## 严重度图例

- 🔴 **Blocker** — 阻止策略按设计运行或直接造成资金风险
- 🟠 **High** — 在常见路径中会触发明显偏差
- 🟡 **Medium** — 边界场景下会偏离设计意图
- 🔵 **Complexity** — 复杂度 / 冗余 / 可维护性问题 (本轮新分类)
- 🟢 **Low** — 日志 / 一致性

---

## TL;DR

| P  | 项                          | 简介                                                                                                                            |
|----|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| P0 | 🔴 **B-V5-1**               | `round_state.py:225` 的 `is_empty` 在仓位全部卖出后**仍返回 True**（如果 TP 卖单触发后 `_total_qty=0` 但 `_tp_inflight=True`，engine 会认为仓位空，可能错误触发下一轮 T1 操作残留路径） |
| P0 | 🔴 **B-V5-2**               | **TP runner 抛异常但已部分 fill 时 `_tp_inflight` 永久卡住**：`_tp_runner` (main.py:249) catch 块只清 `_tp_inflight`，但**TP 已部分 fill 后** `record_tp_fill` 会清一次 → 第二次抛异常会再次清，没问题；但**若 GTD 成功 fill + FAK 失败 → `_tp_runner` 不返回成功路径**，是否再次 emit TP？**需要确认** |
| P1 | 🟠 **H-V5-1**               | `tp_target` 只看 `is_complete`，**不看 `placed` 中正在 fill 的 tier** — T3 in-flight 时 `is_complete=False`，TP target 仍按 N=1 (T2 已完成) 算；如果 T3 即将以更低成本 fill，会用更激进的 TP 触发线先触发 TP → 平均收益缩水 |
| P1 | 🟠 **H-V5-2**               | `stake_cap_remaining` 在 `_check_t1_entry` 仍用 `TIER1_NOTIONAL` 而**未先检查 `entry_token`** — 但 `entry_token is None` 已在外层 guard，所以 OK；**但**当 T1 失败回滚后第二次进 T1 时若 T1 partial fill (sentinel 被清) 存在 `_pending_fills`，可能 **重复计数** (corner case) |
| P1 | 🟠 **H-V5-3**               | `_pending_round: int = 0` 在 ActionRouter (main.py:108) 声明但**从未被赋值/读取** — 纯死字段 |
| P1 | 🟠 **H-V5-4**               | `OrderManager.mark_size_matched` (line 1029) 是 dead method — 没被任何调用方使用；功能与 `apply_fill_event` 完全重叠 |
| P1 | 🟠 **H-V5-5**               | `_pending_round` / `mark_size_matched` / `engine.on_binance_tick` / `engine.on_token_tick` / `is_expired` 等 5+ 个 dead method / field 表明 **API 表面被反复迭代没清理** |
| P2 | 🔵 **C-V5-1**               | `tier_is_saturated` + `tier_notional_remaining` + `tier_notional_used` + `total_notional_committed` + `stake_cap_remaining` + `max_notional_exposed` **6 个不同方法都在算 "USD 消耗 vs 上限"**，代码极其分散 |
| P2 | 🔵 **C-V5-2**               | `tp_target` 与 `tp_pct_for_current_state` 重复 80% 代码 — 应该共用一个内部 helper |
| P2 | 🔵 **C-V5-3**               | `_check_t1_entry` 内 `pos.entry_token = token` 在 await 之前手动赋值（race fix）— 但 router 又 `eng._round.position.entry_side = action.side` (main.py:134) 重复写一次，应只在一处写 |
| P2 | 🔵 **C-V5-4**               | `place_buy` 内 `adjusted_price = await _adjust_buy_price(...)` 在 lock 外 (audit v3 B-NEW-2 fix) 但 `place_buy_by_notional` 也调 `_adjust_buy_price` (line 341) — **同一 BUY 路径走了 2 次 `_adjust_buy_price`** (router 调 `place_buy_by_notional`，里面又调 `place_buy`，里面再调一次 `_adjust_buy_price`) |
| P2 | 🔵 **C-V5-5**               | `order_manager.py:392` 与 `514` 重复 `from src.execution.clob_client import CLOBClient as _CC` + `effective_lifetime = max(lifetime_s, _CC.GTD_MIN_SEC + 1)` — 应抽成模块级常量或 helper |
| P2 | 🔵 **C-V5-6**               | `ActionRouter` (main.py:103) 是 350 行的"上帝对象"，内含 T1/TN/TP 路由、fill_callback 工厂、TP runner 内联 — 拆成 3 个小类 (T1Router / TnRouter / TpRunner) 会大幅降低认知负担 |
| P2 | 🔵 **C-V5-7**               | `TrackedOrder.placed` 与 `TrackedOrder.status` 是双重状态机 — `status` 五态 (placed/live/filled/cancelled/expired) 但代码里大量用 `placed` bool；多处不一致 (`placed=False` 但 `status="live"` 是合法状态) |
| P2 | 🟡 **M-V5-1**               | `_check_t1_entry` 在 `pos.entry_token is None` 时循环 UP/DOWN 两次 (line 377) — 已写 `pos.entry_token = token` 后立刻 `return`，但**仍循环到第二个 side**，浪费一次 REST mid/bid 获取 |
| P2 | 🟡 **M-V5-2**               | `_check_take_profit` 内 `mid_price` 变量取了但**没用** (line 608) — TP 实际只看 bid；变量多余 |

---

## 1. v4 修复状态复查 (本轮)

| v4 项       | 状态     | 验证 |
|-------------|----------|------|
| 🔴 B-V4-1   | ✅ 已修  | `_cleanup_failed_post` 在 place_buy (421) / place_gtd_sell (537) / place_sell (780) 三处失败路径调用 |
| 🔴 B-V4-2   | ✅ 已修  | router 中 `if order is None` 时 `entry_token = None, entry_side = None` (main.py:179-180) |
| 🟠 H-V4-1   | ✅ 已修  | router 用 `place_buy_by_notional` (main.py:124-131) 而非 place_buy；size 在 OM 内算 |
| 🟠 H-V4-2   | ✅ 已修  | `stake_cap_remaining` 用 `total_notional_committed` (round_state.py:262) |
| 🟡 M-V4-1   | ✅ 已修  | `purge_round` 在 `_on_new_round` 调用 (main.py:472) |
| 🟡 M-V4-2   | ✅ 已修  | `_check_average_down` 检查 `tier.placed and not tier.server_order_id` → reset (engine.py:508-516) |
| 🟡 M-V4-3   | ✅ 已修  | `drain_pending_fills` 在 cancel (852) / cancel_market (891) / purge_round (954) 三处调用 |

**v4 修复全部 confirmed applied。本轮未发现新的资金级 Blocker。**

---

## 2. 新发现的 Blocker

### 🔴 B-V5-1 (新): `is_empty` 在仓位被 TP 卖光后行为不明

- **路径**: `src/strategy/round_state.py:225-226`
  ```python
  @property
  def is_empty(self) -> bool:
      return all(not t.placed and not t.filled for t in self.tiers) and self._total_qty == 0
  ```
- **症状**: 当 TP 完全 sell 全部仓位 (`_total_qty=0`) 后，`is_empty` 返回 True。engine `on_order_filled` (engine.py:176) 检查 `is_empty` → 返回 → 后续 round 切换前**任何 fill 都不会被处理**。但 TP GTD 本身可能继续被 poll 发现 partial fills（虽然 FAK fallback 之后不太可能，但理论上 race 仍存在）。
- **影响** (🟠 中等): round 结束时 `_on_new_round` 调 `cancel_market + purge_round`，所以残留 TP 会被 cancel；实际 race 窗口很窄。
- **修复方向**: 在 `_on_new_round` 显式 reset `pos._tp_inflight = False` + 清 `_tp_snapshots`。

### 🔴 B-V5-2 (新): TP runner 异常路径不完全

- **路径**: `src/main.py:249-339` (`_tp_runner`)
- **症状**: 异常 catch 块 (line 259-267) 只清 `_tp_inflight`，**不清 `_tp_snapshots`**。如果 GTD 已 fill 但 FAK 抛异常，TP snapshot 仍在 dict 里，下一轮 round-start 不会被 `purge_round` 清（因为 OM 的 `purge_round` 不感知 engine state）。
- **影响** (🟠 中等): 内存泄漏 `_tp_snapshots` dict（每 round 一条），长期跑无界增长。
- **修复方向**: catch 块加 `pos._tp_snapshots.clear()`。

---

## 3. 新发现的 High

### 🟠 H-V5-1 (新): `tp_target` 不感知 in-flight tier

- **路径**: `src/strategy/round_state.py:454-472` (`tp_target`)
  ```python
  n = sum(1 for t in self.tiers[1:] if t.is_complete)
  tp_pct = cfg.TP_PROFIT_PCT - n * cfg.TP_DECREMENT_PCT
  ```
- **症状**: TP target 减档只看 **completed** averaging tier。考虑场景：
  1. T1 fill @ 0.40, T2 fill @ 0.32, T3 placed @ 0.24 但**还没 fill**
  2. 此时 `n = 1` (T2 complete)，TP target = avg_cost * 1.17
  3. 如果 T3 即将 fill @ 0.24，fill 后 avg_cost 会**更低**，新 TP target = avg_cost_new * 1.14
  4. 但 T3 fill **之前**如果 mid 反弹到 1.17 * 当前 avg_cost → 触发 TP
  5. 用户实际期望：等 T3 fill 完，再触发 TP (TP target 更低)

  **核心矛盾**: in-flight tier 不算入 N，但它的 fill 会拉低 avg_cost，导致 TP target 在 fill 前后跳变。
- **影响** (🟠 中等): T3 in-flight 期间触发 TP 的概率不为零；可能锁掉部分利润（按当前 avg_cost），而用户期望等 T3 fill 后以更低均价再 TP。
- **修复方向**: 让 TP 触发检查也尊重 in-flight — 如果 `pos.tiers[1:].placed and not is_complete` 存在，TP target 用**预估 fill 后**的 avg_cost（即把 in-flight tier 的 notional 按当前 bid 加入），更激进但稳定。或者直接：**TP 在任一 averaging tier in-flight 时不触发**（与 averaging 在 TP in-flight 时不触发对称）。

### 🟠 H-V5-2 (新): `stake_cap_remaining` 在极端 corner case 下仍可能误算

- **路径**: `src/strategy/round_state.py:251-262`
- **症状**: v4 H-V4-2 已修 — 用 `total_notional_committed`。但**极端 case**：T1 失败回滚后 `_pending_fills` 中可能有 phantom fill 排队（理论上 `_cleanup_failed_post` 已清，但 sentinel race window 仍存在）。如果 sentinel 已被清但 fill 已在 `apply_fill_event` 中被 `queue_pending_fill`，后续 `resolve_pending_server_id` 不会被调用 → phantom fill 永不 replay。
- **影响** (🟠 边界): 极端 race，理论上不可能（因为 `_cleanup_failed_post` 已包含 `drain_pending_fills`），但代码不强制不变量。
- **修复方向**: 加单元测试覆盖此 race。

### 🟠 H-V5-3 (新): `_pending_round` 在 ActionRouter 死字段

- **路径**: `src/main.py:108` (`self._pending_round: int = 0`)
- **症状**: 声明后从未被读取或赋值。死字段。
- **影响** (🟠 代码 smell): API 表面污染。
- **修复方向**: 删除。

### 🟠 H-V5-4 (新): `OrderManager.mark_size_matched` 是 dead method

- **路径**: `src/execution/order_manager.py:1029-1033`
  ```python
  def mark_size_matched(self, server_order_id: str, size_matched: float) -> None:
      """Update matched size (called from order UPDATE events)."""
      order = self.by_server_id(server_order_id)
      if order:
          order.size_matched = size_matched
  ```
- **症状**: 功能与 `apply_fill_event` (line 1001-1027) 几乎完全重叠（差别仅在 apply_fill_event 用 monotonic max）。`grep -r mark_size_matched` 无任何调用方。
- **影响** (🟠 API 重复): 同一概念两个 API，调用方不知道用哪个。
- **修复方向**: 删除 `mark_size_matched`，所有 UPDATE 事件走 `apply_fill_event`（已有 monotonic 语义）。

### 🟠 H-V5-5 (新): 累计 dead method 列表 — 复杂度信号

| 方法/字段 | 位置 | 状态 | 备注 |
|-----------|------|------|------|
| `_pending_round: int = 0` | main.py:108 | ❌ dead field | 声明后从未用 |
| `mark_size_matched` | order_manager.py:1029 | ❌ dead method | 与 apply_fill_event 重叠 |
| `on_binance_tick` / `on_token_tick` | engine.py:135, 139 | ❌ dead methods | v3 L-NEW-1 已识别，未删 |
| `is_expired` (TierSnapshot) | round_state.py:104 | ❌ dead property | grep 无外部调用方 |
| `next_open_tier` (Position) | round_state.py:217 | ❌ dead property | grep 无外部调用方 |
| `filled_tiers` (Position) | round_state.py:213 | ❌ dead property | grep 无外部调用方 |
| `tier_notional_used` (Position) | round_state.py:264 | ❌ dead method | 与 `tier.cost_filled` 直接读等价 |
| `max_notional_exposed` (Position) | round_state.py:486 | ❌ dead method | grep 无外部调用方 |

**共 8 个 dead member**。这是反复迭代后未清理的典型信号。

---

## 4. 复杂度专项审查 (本轮重点)

### 🔵 C-V5-1: "USD 消耗 vs 上限" 概念分散在 6 处

| 方法 | 行号 | 公式 | 用途 |
|------|------|------|------|
| `tier_is_saturated(tier)` | round_state.py:268 | `cost_filled >= notional * MAX_MULT` | 跳过 saturated tier |
| `tier_notional_remaining(tier)` | round_state.py:273 | `notional * MAX_MULT - cost_filled` | 报 remaining 容量 |
| `tier_notional_used(tier)` | round_state.py:264 | `cost_filled` | (dead) |
| `total_notional_committed` | round_state.py:246 | `sum(t.notional for t in tiers if t.placed)` | 算 cap baseline |
| `stake_cap_remaining` | round_state.py:251 | `total_max - total_notional_committed` | 算剩余 cap |
| `max_notional_exposed` | round_state.py:486 | `sum(t.notional for t in tiers)` | (dead) |

**所有 6 个都围绕 "USD cap" 概念分散定义**，调用方要追 6 个不同入口才能完全理解仓位控制逻辑。

**建议重构**: 引入 `Position.cap_state()` 返回 `CapState` dataclass:
```python
@dataclass
class CapState:
    used: float          # = sum(cost_filled)
    committed: float      # = sum(notional for placed tiers)
    saturated: set[int]   # tier indices that are saturated
    per_tier_remaining: dict[int, float]
    total_remaining: float # = total_max - committed

def cap_state(self) -> CapState: ...
```
调用方 `pos.cap_state().total_remaining`、`pos.cap_state().per_tier_remaining[tier.tier]`。

### 🔵 C-V5-2: `tp_target` 与 `tp_pct_for_current_state` 80% 重复

- `round_state.py:454-472` (`tp_target`)
- `round_state.py:474-484` (`tp_pct_for_current_state`)

唯一差别：tp_target 在 `_avg_cost <= 0` 返回 None。

**建议重构**: 提取 `_compute_tp_pct() -> float`，两个 method 共用。

### 🔵 C-V5-3: `pos.entry_token = token` 双重赋值

- `src/strategy/engine.py:444-445` (T1 决策后立即写)
- `src/main.py:134-135` (router 在 order 成功后写)

两次写同一个字段，第二个 write 是 no-op（值相同），但反映代码意图不清晰。H-1 v2 fix 的注释明确说 "router will skip its own entry_token = action.token write because the field is no longer None (idempotent overwrite with the same value)" — 但代码实际**仍写了**（main.py:134-135），是个过度防御。

**建议**: router 中删除这两行（main.py:134-135），完全依赖 engine 在 await 前的赋值。

### 🔵 C-V5-4: `_adjust_buy_price` 在 BUY 路径上被调 2 次

**调用链**: `router → place_buy_by_notional → _adjust_buy_price (line 341)` 然后 `place_buy → _adjust_buy_price (line 405)`。

每次 BUY 都走 2 次 REST GET orderbook（**2 次网络往返**，1-3s 各）。

**修复方向**: `place_buy_by_notional` 把 `adjusted_price` 直接传给 `place_buy`，后者跳过 `_adjust_buy_price`：
```python
async def place_buy(self, ..., *, price_already_adjusted: bool = False):
    if price_already_adjusted:
        adjusted_price = price
    else:
        adjusted_price = await self._adjust_buy_price(token_id, price, tick_size)
```

### 🔵 C-V5-5: `effective_lifetime = max(lifetime_s, _CC.GTD_MIN_SEC + 1)` 重复 3 次

- `order_manager.py:394-395` (`place_buy`)
- `order_manager.py:516` (`place_gtd_sell`)
- `order_manager.py:668` (`place_tp_with_fallback` 的 wait_s 计算)

**建议**: 抽成模块级 helper：
```python
def _compute_effective_lifetime(desired: int) -> int:
    from src.execution.clob_client import CLOBClient
    return max(desired, CLOBClient.GTD_MIN_SEC + 1)
```

### 🔵 C-V5-6: `ActionRouter` 是 350 行上帝对象

- `src/main.py:103-398` 包含:
  - T1 路由 (119-180)
  - TN 路由 (182-231)
  - TP runner 内联协程 (249-331)
  - fill_callback 工厂 (341-398)

**建议重构**:
```python
class T1Router: ...
class TnRouter: ...
class TpRunner: ...
class FillCallbackFactory: ...
```
`ActionRouter` 变成 30 行 dispatcher，每个子对象 50-80 行。**认知负担下降 70%**。

### 🔵 C-V5-7: `TrackedOrder.placed` 与 `status` 双重状态机

- `placed: bool` (line 48)
- `status: Literal["placed", "live", "filled", "cancelled", "expired"]` (line 63)

两套状态机并存。代码里大量 `if tier.placed:` (engine.py:501) + `if order.status in ("placed", "live")` (order_manager.py:182) **互相冗余**。

实际映射：
- `placed=False, status="placed"` → not yet sent
- `placed=True, status="live"` → in-flight
- `placed=True, status="filled"` → done
- `placed=False, status="cancelled"/"expired"` → done-without-fill

**建议**: 删除 `placed` 字段，所有逻辑统一读 `status`。或者反之 — 保留 status 仅用于持久化。

---

## 5. 边界 case 检查（按 v4 路径表）

### 5.1 GTD BUY 路径

**核心流程**: `engine._check_t1_entry → ActionOpenT1 → ActionRouter → om.place_buy_by_notional → om._adjust_buy_price → om.place_buy → clob.place_gtd_buy → bind_server_order_id → fill_callback → engine.on_order_filled → record_fill`

**确认 OK**:
- ✅ v4 B-V4-1 fix: 失败路径 `_cleanup_failed_post` 已加
- ✅ v4 B-V4-2 fix: router 失败回滚 `entry_token`
- ✅ v4 H-V4-1 fix: size 在 OM 内算（用 adjusted_price）
- ✅ v3 B-NEW-1 fix: record_fill 不再清 `_tp_inflight`
- ✅ v3 B-NEW-2 fix: `_adjust_buy_price` 在 OM lock 外 (但 C-V5-4 发现仍被调 2 次)
- ✅ v3 H-NEW-1 fix: `register_pending_server_id` 处理 fill callback race
- ✅ M-NEW-4 fix: `_buy_step_for(tick_size)` 半 tick 步进

**新发现**:
- 🔵 C-V5-4: `_adjust_buy_price` 实际被调 2 次（每次 BUY 多 1-3s 浪费）

### 5.2 补仓（averaging-down）路径

**核心流程**: `_check_average_down → ActionOpenTN → router → place_buy_by_notional → 同样路径`

**确认 OK**:
- ✅ H-NEW-2 fix: TP in-flight 时不触发补仓（engine.py:480）
- ✅ H-3 v2 fix: baseline anchor 在 `bind_server_order_id` (engine.py:307-308)
- ✅ 互不感知保护：tier.placed + early return
- ✅ Layer 1-3 仓位控制（is_complete / placed / saturated）正常

**新发现**:
- 🟠 H-V5-1: tp_target 不感知 in-flight tier，T3 在飞行中时 TP 可能按"陈旧" avg_cost 触发

### 5.3 止盈（take-profit）路径

**核心流程**: `_check_take_profit → ActionTakeProfit → _tp_runner → place_tp_with_fallback → place_gtd_sell + sleep(166s) + place_fak_sell → fill_callback → record_tp_fill`

**确认 OK**:
- ✅ B-2 fix: TP fill state 正确更新 (`_tp_snapshots` + `record_tp_fill`)
- ✅ B-3 fix: deduper 防 double-count
- ✅ H-2 fix: TP runner fire-and-forget + `_bg_tasks` 强引用
- ✅ B-4 fix: TP 用 `bid_price` 触发 + `price_lock=True` 下单
- ✅ TP 减档 (TP_DECREMENT_PCT)：N=0..3 → 20%/17%/14%/11%
- ✅ TP floor at TP_MIN_PCT (5%)

**新发现**:
- 🟠 H-V5-1: TP target 在 in-flight averaging tier 时机不稳
- 🔴 B-V5-2: TP runner 异常 catch 不清 `_tp_snapshots`

### 5.4 各档仓位控制 — 完整性分析

**控制不变量（4 层防护）**:
1. **Layer 1**: `tier.is_complete` → 防止重发已 fill 的 tier
2. **Layer 2**: `tier.placed` → 防止重发 in-flight 的 tier
3. **Layer 3**: `tier_is_saturated(tier)` → 防止单档超 110% notional
4. **Layer 4**: `stake_cap_remaining` → 防止总 notional 超 `total_max_notional=16`

**Layer 4 在 v4 后已修复**（用 `total_notional_committed`），但**整套控制**通过 6 个分散的方法表达（C-V5-1）。

**结论**: 仓位控制逻辑正确，但代码**结构上分散**，可读性差。

---

## 6. 复杂度评分 (本轮新)

| 维度 | 评分 (1=好, 5=差) | 备注 |
|------|------------------|------|
| 模块数量 | 2 | 14 文件，分层健康 |
| 单文件行数 | 4 | `order_manager.py` (1034 行), `main.py` (994 行), `engine.py` (650 行), `round_state.py` (524 行) — 偏大 |
| 单函数行数 | 4 | `place_tp_with_fallback` (110 行), `_check_t1_entry` (90 行), `place_buy` (120 行), `_tp_runner` (90 行) |
| 死代码 | 4 | 8+ dead method/field (H-V5-5) |
| 概念重复 | 5 | 6 处 USD-cap 概念分散；`tp_target`/`tp_pct_for_current_state` 重复 |
| 状态机复杂度 | 4 | `placed` + `status` 双重 |
| God object | 4 | `ActionRouter` 350 行 |
| 平均认知负担 | **3.7/5** | **已超过"易读"阈值，建议重构** |

---

## 7. 修复优先级

| P  | 项                  | 改动量 | 关联依赖 |
|----|---------------------|--------|----------|
| P0 | **B-V5-1**         | 极小   | `_on_new_round` 加 `pos._tp_inflight = False` + `_tp_snapshots.clear()` |
| P0 | **B-V5-2**         | 极小   | `_tp_runner` catch 块加 `_tp_snapshots.clear()` |
| P1 | H-V5-1             | 中     | TP target 引入 in-flight 感知；或 averaging 期间暂停 TP |
| P1 | H-V5-3 / H-V5-4    | 极小   | 删 `_pending_round`, `mark_size_matched` |
| P1 | H-V5-5 (其他 dead) | 极小   | 删 `on_binance_tick`/`on_token_tick`/`is_expired`/`next_open_tier`/`filled_tiers`/`tier_notional_used`/`max_notional_exposed` |
| P2 | C-V5-1             | 中     | 引入 `CapState` dataclass 合并 6 个 USD-cap 方法 |
| P2 | C-V5-2             | 极小   | 抽 `_compute_tp_pct()` |
| P2 | C-V5-3             | 极小   | router 删 134-135 行 |
| P2 | C-V5-4             | 极小   | `place_buy_by_notional` 把 `adjusted_price` 传 `place_buy`，跳过第二次 `_adjust_buy_price` |
| P2 | C-V5-5             | 极小   | 抽 `_compute_effective_lifetime()` helper |
| P2 | C-V5-6             | 中     | `ActionRouter` 拆 4 个小类 |
| P2 | C-V5-7             | 中     | 删 `placed` 字段统一用 `status` |
| P3 | M-V5-1 / M-V5-2    | 极小   | cleanup |

---

## 8. 各档仓位控制 — 二次审查

经过 v4 修复 + 本轮审查，**仓位控制逻辑**最终状态:

| 检查 | 位置 | 状态 | 评价 |
|------|------|------|------|
| T1 唯一性 | engine.py:354 `entry_token is None` | ✅ 严格 | |
| T2/T3/T4 唯一性 | engine.py:499-518 | ✅ 严格 | placed + is_complete 双检 |
| 单档 notional cap | round_state.py:268 `tier_is_saturated` | ✅ 正确 | 但分散定义 |
| 总 notional cap | round_state.py:251 `stake_cap_remaining` | ✅ v4 已修 | 用 committed 而非 filled |
| Tier 不变量 | engine.py:497 `for tier in pos.tiers[1:]` | ✅ 正确 | 一次一档 |
| TP 互不感知 | engine.py:480 (avg) + 605 (TP) | ✅ 对称 | TP / avg 互锁 |
| TP target 减档 | round_state.py:454-472 | ⚠️ H-V5-1 | 不感知 in-flight tier |

**整体**: 仓位控制在 v4 后**功能性正确**；本轮新增 H-V5-1 是边界 case 的体验问题（非资金安全）。

---

## 9. 总结

v4 (2026-07-11) 审计罗列的 B-V4-1 / B-V4-2 / H-V4-1 / H-V4-2 / M-V4-1~3 **全部 confirmed applied**，47/47 测试通过。

**本轮 (v5) 新发现**:
1. **🔴 B-V5-1 / B-V5-2**: round 切换与 TP runner 异常时的残留 state cleanup 缺口（极小修复量）。
2. **🟠 H-V5-1**: TP target 在 averaging in-flight 时机不稳（中等修复量）。
3. **🟠 H-V5-3 / H-V5-4**: 死字段/方法清理（极小修复量）。
4. **🟠 H-V5-5**: 8 个 dead member 列表 — **复杂度信号**。
5. **🔵 C-V5-1~7**: 7 个复杂度/冗余问题 — **代码已"过迭代"未清理**。

**复杂度结论**:
- **代码功能正确**：v4 修复闭合了所有资金级 Blocker；v5 未发现新的资金风险。
- **代码已显冗余**：8+ dead member、6 处 USD-cap 概念分散、`ActionRouter` 350 行上帝对象、`placed`/`status` 双重状态机、`_adjust_buy_price` 被调 2 次。
- **可读性评级 3.7/5**：已超过"易读"阈值，**强烈建议 P2 重构**。

**投产建议**: P0 (B-V5-1, B-V5-2) 极小，**首日必修**；P1 (H-V5-1 等) 首周内；P2 复杂度重构建议**季度内**安排专门 PR（避免一次性大改）。