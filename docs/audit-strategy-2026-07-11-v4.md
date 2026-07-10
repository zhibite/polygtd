# 入场 / 补仓 / 止盈 全面审计 v4 (聚焦 GTD BUY、补仓、止盈、各档仓位控制)

- 审计人: code review (静态 + 动态)
- 日期: 2026-07-11
- 范围: `src/strategy/*`, `src/execution/*`, `src/main.py`, `src/config.py`
- 上一版: `docs/audit-strategy-2026-07-11-v3.md` (v3) → **本轮发现 2 个新 Blocker, 2 个新 High, 3 个新 Medium**
- 测试基线: 47/47 通过 (现有 v3 / v2 / tp_decrement / tp_gtd_sell 套件)

## 严重度图例

- 🔴 **Blocker** — 阻止策略按设计运行或直接造成**资金风险**
- 🟠 **High** — 在常见路径中会触发明显偏差
- 🟡 **Medium** — 边界场景下会偏离设计意图
- 🟢 **Low** — 日志 / 一致性 / 可维护性

---

## TL;DR (聚焦本次审查)

| P  | 项                          | 简介                                                                                                                            | 改动量 |
|----|-----------------------------|---------------------------------------------------------------------------------------------------------------------------------|--------|
| P0 | **🔴 B-V4-1**               | `place_buy` / `place_gtd_sell` / `place_sell` 失败路径**不清理 sentinel 与 pending_fills** → CLOB 上若已 fill，回调进来后 `_by_server` 仍指向 `_pending_<sid>`，永远不会被 `resolve_pending_server_id` 替换 → 静默丢 fill | 极小   |
| P0 | **🔴 B-V4-2**               | `ActionRouter` 对 `ActionOpenT1` / `ActionOpenTN` 失败 (order=None) **静默吞错**：engine 已在 `await` 之前写入 `entry_token` / `entry_side`，但 router 跳过 `bind_server_order_id`，导致 (a) `tier.server_order_id` 永远为空，(b) 后续 fill 回调进来时 `engine.on_order_filled` 找不到 tier 也找不到 TP 快照 → fill 静默丢 | 极小   |
| P1 | 🟠 H-V4-1                   | `place_buy` 内 `size = action.notional / action.price` 在 router 内计算，但 price 是 decision-time 的 `bid_price`，不是 order_manager 内 adjusted_price → `size` 与实际 CLOB 接受的 `placed_size` 经常不同，触发 M-1 但**未修正 `size_matched` 比对基准** | 极小   |
| P1 | 🟠 H-V4-2                   | `stake_cap_remaining` 用 `_total_cost` (已 fill) 而非 `total_notional_committed` (已 place) — 当某 tier in-flight 但还没收到 fill 时，cap 实际无效；但因为 averaging 一次一档的限制，目前不会出问题；**风险点在于如果将来引入"并发补仓"则 cap 直接被绕过** | 极小   |
| P2 | 🟡 M-V4-1                   | `_orders` / `_by_server` / `_pending_fills` 在整个 session 内**单调增长**，没有 round-end / 周期清理逻辑 — 长期跑 96 rounds/天 ≈ 1500+ 条记录 | 小     |
| P2 | 🟡 M-V4-2                   | `_check_average_down` 中 `tier.placed` 检查**仅看 `placed`**，不看 `server_order_id` 是否有效 — `mark_expired` 把 `server_order_id=None` 但 `placed=False`，重发时是 OK 的；但若 `placed=True` 但 `server_order_id=None` (如 bind 失败) 会卡死补仓路径 | 极小   |
| P2 | 🟡 M-V4-3                   | `drain_pending_fills` 是 dead code — `cancel` / `cancel_market` / round-end 都不调用；只在 `place_*` 失败时需要它清理 sentinel 时根本不存在调用方 | 极小   |

---

## 1. 必看的新 Blocker

### 🔴 B-V4-1 (新): `place_buy` / `place_gtd_sell` / `place_sell` 失败路径泄漏 sentinel + pending_fills

- **路径**:
  - `src/execution/order_manager.py:340-346` (`place_buy` 失败分支)
  - `src/execution/order_manager.py:453-465` (`place_gtd_sell` 失败分支)
  - `src/execution/order_manager.py:692-698` (`place_sell` 失败分支)
  - `src/execution/order_manager.py:219-229` (`register_pending_server_id` 写入 sentinel)
  - `src/execution/order_manager.py:264-279` (`queue_pending_fill`)

- **症状** (确定): CLOBClient 在 post-response 立即调用 `register_pending(server_id)`，将 `server_id → _pending_<server_id>` 写进 `_by_server`。如果 CLOBClient.poll 在 `place_gtd_buy` await 期间检测到 fill 并触发 `_fire_fill_callback`：
  - 若 callback 通过 `apply_fill_event` 进入 OM，sentinel 被命中 → 调用 `queue_pending_fill` → 写到 `_pending_fills[server_id]`。
  - 然后 `place_gtd_buy` 在 poll timeout / post-error 后 `result.success=False`，`place_buy` line 346 直接 `return None`。
  - **此时 sentinel 仍指向 `_pending_<sid>`，pending_fills 队列里有未被 replay 的 fill**。
  - 后续任何对 `server_id` 的访问 (`by_server_id`, `apply_fill_event`) 都会再次 queue 到 `_pending_fills`，**永不被 resolve**。

- **影响** (🔴 资金级):
  - 第一次 fill 已发生 (CLOB 已扣款)，但 OM 不知道，engine 不知道。
  - `_pending_fills` 队列单调增长，`_by_server` 中的 sentinel 单调增长。
  - **如果 CLOB 实际接收了订单但 server_id 在 poll timeout 前返回 success=False** — 这种情况如果发生，会完全丢 fill；引擎会认为本轮无仓位。
  - `drain_pending_fills` 方法**定义但从未被调用** — 这是 fix 没闭合的明显证据。

- **修复方向**:
  ```python
  # order_manager.py:340-346 改为：
  if not result.success:
      server_id = result.order_id  # might still be set
      if server_id:
          # Drop any sentinel/pending fills we cannot resolve.
          self._by_server.pop(server_id, None)
          self.drain_pending_fills(server_id)
      log.warning(...)
      return None
  ```
  - **方案 A**: 在每个失败分支显式 `pop _by_server[server_id]` 和 `drain_pending_fills(server_id)`。
  - **方案 B**: 把"清 sentinel / drain pending"抽成 `_cleanup_failed_post(server_id)` helper，三处共用。
  - **方案 C** (更彻底): `resolve_pending_server_id` 接受一个 `success=False` 路径，主动释放 pending。
  - 加单测: mock CLOBClient 先 register_pending + queue_pending_fill，再 place_gtd_buy 返回 success=False → 断言 `_by_server` 与 `_pending_fills` 被清理。

- **与 v3 H-NEW-1 的关系**: H-NEW-1 只修了**成功路径**（CLOBClient poll 第一次成功后 fill callback → sentinel 被 replayed 到 TrackedOrder）。**失败路径的 cleanup 缺失**是 v3 修复没覆盖到的反向污染。

---

### 🔴 B-V4-2 (新): `ActionRouter` 对 `ActionOpenT1`/`ActionOpenTN` 失败 (order=None) 静默吞错

- **路径**:
  - `src/strategy/engine.py:444-445` (`pos.entry_side = side; pos.entry_token = token` 在 await 之前)
  - `src/main.py:129-145` (router 中 `if order:` 跳过 `bind_server_order_id` 与 `fill_callback` 注册)

- **症状** (确定): H-1 v2 fix 把 `entry_token` 写入移到 engine 内 await handler 之前，以避免重复 T1。但当 `om.place_buy` 返回 None (CLOB post 失败 / size < 5 / gtd_poll_timeout) 时：

  1. engine 已写入 `pos.entry_token = token` 和 `pos.entry_side = side`。
  2. router 走 `if order:` False 分支，**直接跳过 `bind_server_order_id`**。
  3. router **不重置** `pos.entry_token`，**不发任何 log 表明 T1 失败**。
  4. 后续 `_evaluate` 调用看到 `entry_token is not None` → 跳过 `_check_t1_entry`。
  5. `baseline_set=False` → `_check_average_down` 也直接返回。
  6. **整个 round 卡死**，直到下一轮 round 切换。

  如果 CLOB post 实际成功了但 `place_gtd_buy` poll 在 60s 后 timeout 返回 `success=False` (gtd_poll_timeout)，订单可能还挂在 CLOB 上；如果它后面真的 fill 了，**UserWS 的 maker_fill callback 进来 → `om.by_server_id(order_id)`**：
  - `_by_server[server_id]` 仍指向 sentinel（因为 place_buy 在 resolve 之前 return None）。
  - `by_server_id` 返回 `_pending_<server_id>` 不是 TrackedOrder。
  - `_make_maker_fill_callback.on_maker_fill` line 848-854: `if not tracked: log.debug("router.maker_fill.untracked"); return` → **fill 静默丢**。
  - engine.on_order_filled 也不会被调用，**整个 round 库存为零但 CLOB 上已被扣款**。

- **影响** (🔴 资金级):
  - **资金被扣但 bot 不记账**：极端情况 (post-error + 后续 fill) 实际发生概率虽低，但**可能造成 PnL 不可解释、用户余额与 bot 记录 mismatch**。
  - **整个 round 被锁死**到下次 round 切换 — 浪费一个完整的 15min 窗口。
  - 用户的 GTD 订单可能还在 CLOB 上挂着，到 181s 自动 expire → 用户**被挂单占用流动性**但 bot 完全不感知。

- **修复方向**:
  ```python
  # main.py:129-145 改为：
  if order is None:
      log.error(
          "router.t1_failed",
          side=action.side,
          token=action.token[:16],
          price=action.price,
          notional=action.notional,
          reason="om.place_buy returned None",
      )
      # CRITICAL: roll back the engine state so a future _evaluate can retry
      # on the opposite side, OR the next round starts cleanly.
      eng._round.position.entry_side = None
      eng._round.position.entry_token = None
      return
  ```
  - **方案 A** (推荐): 在 router 中显式回滚 engine 状态 + 打 ERROR log。
  - **方案 B**: engine 内 await handler 后检查 `tier.server_order_id is None`（"binding 失败"），自动 reset state。
  - **方案 C**: 在 `place_buy` 失败时 OM 通过 callback 通知 engine（更复杂，破坏单向流）。
  - 加单测: mock OM.place_buy 返回 None → 断言 entry_token 被回滚 + ERROR log。

- **与 v3 H-1 的关系**: H-1 v2 把 `entry_token` 写入提前是好修复，但**没有对称地处理失败回滚**。这是 v2/v3 修复留下的盲点。

---

## 2. 新发现的 High

### 🟠 H-V4-1 (新): `size = notional / price` 在 router 内算，但 price 是 decision-time bid，不是 adjusted_price

- **路径**:
  - `src/main.py:120` (`size = action.notional / action.price`) — router 内计算 size
  - `src/main.py:158` (T2/T3/T4 同上)
  - `src/strategy/engine.py:449` (`ActionOpenT1(..., price=bid_price, ...)`) — price 是 WS cached bid
  - `src/execution/order_manager.py:329-338` (place_buy 内 `adjusted_price = await _adjust_buy_price(...)` 然后 `clob.place_gtd_buy(price=adjusted_price, size=size, ...)`)
  - `src/execution/clob_client.py:301-302` (`size_q = _round_down(size, tick_d)`, `price_q = _round_down(price, tick_d)`)

- **症状** (确定): router 用 `bid_price` 算 size (e.g., `$2 / 0.39 = 5.1282` shares)；OM 用 `adjusted_price` (= bid + BUY_STEP = 0.395) 下单；CLOB 用 adjusted_price 算实际成本。

  CLOB 实际收的费用 = `size_q * price_q`。
  
  如果 `size_q * price_q > notional`：
  - 例：size = 5.1282 (router)，price = 0.395 (OM) → cost = 2.0256 → 比设计的 $2 多 $0.0256
  - `tier.size_expected = placed_size = result.tokens = _round_down(5.1282, 0.0001) = 5.1282`
  - `tier.size_filled` 累计会到 5.1282，is_complete 触发。
  - 但**实际 cost 超出 1.28%**。

  这违反 "tier.notional 是 hard cap" 的设计意图。即使 tier.cost_filled >= tier.notional (因为 size * adjusted_price > notional)，代码不会立刻拒绝 — 因为 cap 检查在 `_check_average_down` (line 508 `tier_is_saturated`)，只检查 `cost_filled`，不直接检查 `cost_filled <= notional`。

- **影响** (🟠 资金级): 单 tier 多花 1-3% (取决于 bid→ask gap + tick 圆整)；4 tier 都触发 = 多花 ~5%。**TP target 用 `_avg_cost`，所以超额 cost 自动反映在 TP target 里**，最终 PnL 计算仍是正确的；但**用户实际成本比预期高**，直接影响平均收益率。

- **修复方向**:
  ```python
  # main.py:120 改为：
  # Pass notional down, let OM compute size after adjust.
  order = await om.place_buy_by_notional(
      token_id=action.token,
      notional=action.notional,
      bid_price=action.price,  # 用于 fall-back / logging
      round_start_ts=round_ts,
      tier_name="T1",
      tick_size=tick_size,
  )
  ```
  - **方案 A**: 在 OM 内 `place_buy_by_notional(notional)`，先调 `_adjust_buy_price`，得到 adjusted_price 后 `size = notional / adjusted_price`。
  - **方案 B**: router 调 `_adjust_buy_price` 同步算 size（但这会让 router 依赖 PM client）。
  - 加单测: 用 mock OM 让 `_adjust_buy_price` 返回 0.40 (vs 0.39 bid) → 断言 place_buy 收到 size = notional/0.40 而非 notional/0.39。

- **与 M-1 的关系**: M-1 修了 `size_expected` 用 placed_size 而非 notional/price，但没修 size 计算本身的偏差。M-1 是**事后**修 is_complete 阈值；H-V4-1 是**事前**控制 size 与实际 cost 一致。

---

### 🟠 H-V4-2 (新): `stake_cap_remaining` 用 `_total_cost` 而非 `total_notional_committed` — cap 在 in-flight 期实际无效

- **路径**:
  - `src/strategy/round_state.py:251-253` (`stake_cap_remaining = max(0.0, total_max_notional - self._total_cost)`)
  - `src/strategy/round_state.py:245-248` (`total_notional_committed = sum(t.notional for t in tiers if t.placed)`)
  - `src/strategy/engine.py:548` (`if pos.stake_cap_remaining < tier.notional: continue`)

- **症状** (中等严重): `stake_cap_remaining` 用 `_total_cost` (即**已 fill 的成本**) 作为消耗量。
  - T2 fill 100% → `_total_cost = 2.0 + 2.0 = 4.0` → `stake_cap_remaining = 12.0`
  - T3 in-flight (placed but not fill) → `_total_cost = 4.0` → `stake_cap_remaining = 12.0` ✓ 仍足够
  - T4 in-flight → `_total_cost = 4.0` → `stake_cap_remaining = 12.0`，**实际 exposure 已 = T1+T2+T3+T4 = 16.0**！

  **幸运的是**: 当前 `_check_average_down` 强制 "one tier per evaluation" + `tier.placed` 检查，所以 T2/T3/T4 在 in-flight 时不会被触发 (因为前置 tier 还在 placed 状态)。**因此 H-V4-2 目前是 latent risk**。

  **风险点**:
  - 如果将来引入"并发补仓" (例如 T2 partial fill 后立即触发 T3 — v3 audit 的 H-NEW-2 实际上让 averaging 在 TP wait 期**也被压住**，所以当前不会触发)，H-V4-2 会立刻变成 Blocker。
  - 如果 `_check_average_down` 的 `tier.placed` 检查被误改成 `tier.is_complete`，H-V4-2 立即爆发。

- **影响** (🟠 结构性): 当前未触发，但是个**潜在的 over-commit 漏洞**。`total_notional_committed` 已存在 (`round_state.py:245-248`) 但没用上。

- **修复方向**:
  ```python
  # round_state.py:251-253 改为：
  @property
  def stake_cap_remaining(self) -> float:
      """How much more USD could be deployed before hitting total_max_notional.
      
      Uses total_notional_committed (filled + in-flight) — not just filled —
      so the cap is enforced even when tiers are in-flight.
      """
      return max(0.0, _s().total_max_notional - self.total_notional_committed)
  ```
  - 加单测: T1 filled 2.0, T2 placed=True but not filled → 断言 `stake_cap_remaining = 16.0 - 4.0 = 12.0`（而非 14.0）。

- **与 M-3 v2 的关系**: M-3 v2 把 total_max_notional 改为 sum of tier_notionals (16)，但 cap 检查逻辑没动。H-V4-2 是 cap 检查侧的盲点。

---

## 3. 新发现的 Medium

### 🟡 M-V4-1: `_orders` / `_by_server` / `_pending_fills` 长期跑会单调增长

- **路径**: `src/execution/order_manager.py:111-117` (字段定义)，全文搜索 `del self._orders` / `pop` → 无任何删除逻辑。

- **症状**: `_orders` (client_order_id → TrackedOrder) 和 `_by_server` (server_id → client_id) 在整个 session 内累积。Round-end (`_on_new_round`) 只调 `cancel_market`，**不清理**这些 map。

  - 每天 96 轮 × 16 TrackedOrder/round (4 tier + TP) = ~1500 条/天
  - 加上失败的 sentinel、partial-fill queue 等，**长跑 (1 个月+) 会累积 ~50k+ entries**
  - 每次 `by_server_id` 是 O(1) dict lookup，**性能影响小**
  - 但 `reconcile()` 走所有 `_orders.values()` → 启动 reconcile 时会扫大量已结束的订单

- **影响** (🟡 内存): 长期运行内存增长；不影响短期逻辑正确性。

- **修复方向**:
  ```python
  # round_state engine.py on_round_end 中调 OM.purge_round(round_start_ts)
  def purge_round(self, round_start_ts: int) -> None:
      async with self._lock:
          stale = [cid for cid, o in self._orders.items() if o.round_start_ts == round_start_ts]
          for cid in stale:
              o = self._orders.pop(cid)
              if o.server_order_id:
                  self._by_server.pop(o.server_order_id, None)
                  self.drain_pending_fills(o.server_order_id)
  ```
  - 加单测: 模拟 100 轮后断言 `_orders` 大小在合理范围。

---

### 🟡 M-V4-2: `_check_average_down` 中 `tier.placed` 检查过宽，可能与 `server_order_id=None` 状态不一致

- **路径**:
  - `src/strategy/engine.py:501-502` (`if tier.placed: continue`)
  - `src/strategy/round_state.py:427-443` (`mark_expired` 把 `placed=False, server_order_id=None`)

- **症状**: `tier.placed` 仅看布尔值，不看 `server_order_id` 是否有效。考虑这个 race：
  1. engine 调 `place_buy` (e.g. T2)。
  2. OM `place_buy` 在 `_lock` 内 post CLOB 成功，但**`resolve_pending_server_id` 之前的瞬间** 失败 (e.g. process 被 kill)。
  3. 实际上 TrackedOrder 还没插入 `_orders`。
  4. 但 engine 不知道。`tier.placed` 仍是 False (因为 `bind_server_order_id` 没调)。
  5. 下一次 `_check_average_down` 看到 `placed=False` → 重新触发 T2 → **重复下单**。

  这个 race 在生产中概率极低 (同一进程内 post 与 resolve 之间几乎不会被打断)，但**理论上是真实的**。

- **影响** (🟡 边界): 真实触发需要 process restart / signal handler 中断。**不会立即爆发**，但如果有 SIGTERM 介入，可能产生重复订单。

- **修复方向**:
  ```python
  # engine.py:501-502 改为：
  if tier.placed and tier.server_order_id:
      continue  # in-flight with valid server id
  if tier.placed and not tier.server_order_id:
      # Race: bind not completed yet. Mark as expired and re-fire.
      log.warning("engine.tier_no_server_id_reset", tier=tier.tier)
      tier.placed = False
  ```
  - 或者：把 `mark_expired` 也在 engine 内部尝试调（标记 stale tier）。

---

### 🟡 M-V4-3: `drain_pending_fills` 是 dead code

- **路径**: `src/execution/order_manager.py:281-285` 定义；全文搜索调用方 → 无。

- **症状**: `drain_pending_fills(server_id)` 被定义但**从未被调用**。它的 docstring 说 "Explicitly drop pending fills for a server_id (e.g. on cancel)"。但 `cancel()` 和 `cancel_market()` 都**不调用它**。这导致：
  - 用户取消一个 in-flight tier，但已经有 partial fill 在 `_pending_fills` 队列 → 队列里仍有数据，**永不清理**。
  - 与 B-V4-1 联动：失败路径需要调用它但没调。

- **影响** (🟡 一致性): dead method 是 code smell，也意味着 v3 的 H-NEW-1 fix **没有完整的失败路径**。

- **修复方向**:
  ```python
  # cancel() 改为：
  async def cancel(self, client_order_id, reason="manual"):
      async with self._lock:
          ...
          if server_id:
              self.drain_pending_fills(server_id)
              ok = await self._clob.cancel_order(server_id)
              ...
  ```
  - 同步修 B-V4-1 的失败路径，让两处都调。

---

## 4. v3 修复状态复查 (2026-07-11)

| v3 项       | 状态 | 验证 + 备注                                                                                  |
|-------------|------|---------------------------------------------------------------------------------------------|
| **B-NEW-1** | ✅   | `tests/test_audit_fixes_2026_07_11_v3.py::test_b_new_1_buy_fill_does_not_clear_tp_inflight` PASS |
| **B-NEW-2** | ✅   | `test_b_new_2_orderbook_fetch_outside_lock` PASS；但 `_adjust_sell_price` 在 `place_sell` 内仍走 lock — 同样问题存在但 SERVE-only，影响小 |
| **H-NEW-1** | ⚠️   | 成功路径 `resolve_pending_server_id` 已 OK；**失败路径泄漏 (B-V4-1)**                         |
| **H-NEW-2** | ✅   | `test_h_new_2_avg_down_skipped_when_tp_inflight` PASS；但**完全屏蔽补仓**会引入新 risk — 极端场景 T1 大跌但锁被 TP 占着 90s+，补仓被吞 (v3 已识别，本次复查确认) |
| **H-NEW-3** | ✅   | `test_h_new_3_bind_server_order_id_sets_baseline_target` PASS                                |
| **M-NEW-1** | 🟡   | TP 仍 181s，与注释不符                                                                       |
| **M-NEW-2** | ⚠️   | 99.5% 阈值未调整                                                                              |
| **M-NEW-3** | 🟡   | 双 baseline 字段并存，可读性 OK 但仍潜在误导                                                  |
| **M-NEW-4** | ✅   | `_buy_step_for` 修了，半 tick 步进；`_sell_step_for` 同步修                                  |
| **L-NEW-1** | ⚠️   | `engine.on_binance_tick/on_token_tick` 仍是死方法 (~7 行)                                    |
| **L-NEW-2** | ⚠️   | `_eval_inflight` 仍是冗余 bool（loop._inflight 已守门）                                      |

---

## 5. 整体审视：GTD BUY 路径、补仓路径、止盈路径、各档仓位控制

### 5.1 GTD BUY 路径

**核心流程**：`engine._check_t1_entry → ActionOpenT1 → ActionRouter → om.place_buy → _adjust_buy_price → clob.place_gtd_buy → bind_server_order_id → fill_callback → engine.on_order_filled → record_fill`

**已确认 OK**：
- ✅ v3 B-NEW-1 fix: `record_fill` 不再清 `_tp_inflight`（单测覆盖）
- ✅ v3 B-NEW-2 fix: `_adjust_buy_price` 在 OM lock 外执行（单测覆盖）
- ✅ v3 H-NEW-1 fix: `register_pending_server_id` 处理 fill callback 早于 TrackedOrder insert（单测覆盖，成功路径）
- ✅ v3 M-NEW-4 fix: `_buy_step_for(tick_size)` 半 tick 步进（单测覆盖）
- ✅ H-1 v2 fix: `entry_token` 在 await 前写入（单测覆盖）
- ✅ M-1 fix: `placed_size` 写入 `tier.size_expected`（用 `placed_size`，避免 notional/price 误差）

**新发现风险**：
- 🔴 B-V4-1: post 失败路径泄漏 sentinel + pending_fills
- 🔴 B-V4-2: T1 失败时 entry_token 不回滚，round 锁死
- 🟠 H-V4-1: size 用 bid_price 算，但 CLOB 用 adjusted_price 算 cost → 实际多花 1-3%

### 5.2 补仓（averaging-down）路径

**核心流程**：`_check_average_down → ActionOpenTN → ActionRouter → om.place_buy (复用 GTD BUY 路径) → 同样的 bind + fill → record_fill`

**已确认 OK**：
- ✅ H-NEW-2 fix: TP in-flight 时不触发补仓
- ✅ H-3 v2 fix: trigger baseline 用 `baseline_target_price`（不受 fill slip 影响）
- ✅ H-NEW-3 fix: T1 bind_server_order_id 时把 baseline_target_price 锚到 adjusted_price
- ✅ 互不感知保护：`_check_average_down` 一次只下一档；前置 tier `placed=True` 时不触发下一档
- ✅ `_check_t1_entry` 在 entry_token 设置后跳过；`_check_average_down` 在 `baseline_set=False` 时返回

**新发现风险**：
- 🟡 M-V4-2: `tier.placed` 检查不与 `server_order_id` 联动，理论上 bind race 后能重发
- 🟡 M-V4-3: `drain_pending_fills` dead code（影响补仓 fill cleanup）

**各档仓位控制（详细分析）**：

| 检查项 | 位置 | 状态 | 备注 |
|--------|------|------|------|
| `tier_is_saturated(tier)` | engine.py:508 | ✅ OK | 防止 GTD 循环部分填导致单档超支 |
| `tier_notional_remaining < tier.notional * 0.5` | engine.py:540 | 🟡 冗余 | 与 saturated 检查重叠（v2 M-7 已识别） |
| `stake_cap_remaining < tier.notional` | engine.py:548 | 🟠 H-V4-2 | 用 `_total_cost`，未用 `total_notional_committed` |
| `entry_token is None` (T1 才可触发) | engine.py:354 | ✅ OK | 防重复 T1 |
| `pos.entry_token is not None` (TP 才可触发) | engine.py:586 | ✅ OK | 必须先有仓位 |
| `pos.baseline_set` (averaging 前置) | engine.py:469 | ✅ OK | 必须 T1 已 fill |
| `tier.is_complete` (跳过已完成档) | engine.py:499 | ✅ OK | |
| `tier.placed` (跳过 in-flight 档) | engine.py:501 | ⚠️ M-V4-2 | 不与 server_order_id 联动 |

**结论**：档位互斥控制**设计是正确的**，"一次一档" 不变量由 `placed` + "one tier per evaluation return" 保证。T1 完成后才能 averaging，由 `baseline_set` 保证。TP 必须先有仓位，由 `entry_token is not None` + `total_qty > 0` 保证。

### 5.3 止盈（take-profit）路径

**核心流程**：`_check_take_profit → ActionTakeProfit → ActionRouter._tp_runner → om.place_tp_with_fallback → place_gtd_sell + sleep(166s) + place_fak_sell → fill_callback → record_tp_fill → 清 _tp_inflight → 重评估`

**已确认 OK**：
- ✅ v2 B-2 fix: `_tp_snapshots` + `record_tp_fill` 实现正确
- ✅ v3 B-NEW-1 fix: TP fill 正确清 `_tp_inflight`（与 BUY fill 区分）
- ✅ v3 H-NEW-1 fix: TP GTD/FAK 也在 fill callback race 中通过 sentinel 保护
- ✅ v2 B-3 fix: `_bg_tasks` 保留 TP runner 强引用（防 GC）
- ✅ B-4 fix: TP 用 bid_price (price_lock=True) — 触发条件与实际成交价一致
- ✅ TP 减档 (TP_DECREMENT_PCT)：N=0..3 → 20%/17%/14%/11%（单测覆盖）
- ✅ TP floor at TP_MIN_PCT (5%)：防止 sub-cost lock-loss（单测覆盖）
- ✅ `_check_take_profit` 在 `mid_price > tp_target` 时触发，但**实际用 `bid_price` 检查**

**新发现风险**：
- 🔴 B-V4-1: TP GTD/FAK post 失败路径同样泄漏 sentinel
- 🟡 M-V4-3: drain_pending_fills dead code 也会影响 TP

### 5.4 各档仓位控制 — 完整性分析

**控制不变量（4 层防护）**：

1. **Layer 1**: `tier.is_complete` (engine.py:499) → 防止重发已 fill 的 tier
2. **Layer 2**: `tier.placed` (engine.py:501) → 防止重发 in-flight 的 tier
3. **Layer 3**: `tier_is_saturated(tier)` (engine.py:508) → 防止单档超 110% notional
4. **Layer 4**: `stake_cap_remaining` (engine.py:548) → 防止总 notional 超 `total_max_notional=16`

**Layer 4 漏洞（H-V4-2）**：`_total_cost` (filled only) 在 in-flight 期不增长 → 实际 cap 失效。但因 Layer 2 (`tier.placed`) 保证只有 1 个 tier in-flight，所以 Layer 4 漏洞在当前设计下不爆发。

**`total_notional_committed` 已存在但未使用**：
```python
@property
def total_notional_committed(self) -> float:
    """Sum of notional placed but not yet completed (in-flight + filled)."""
    return sum(t.notional for t in self.tiers if t.placed)
```
应当用作 `stake_cap_remaining` 的基准。

---

## 6. 修复优先级

| P  | 项             | 改动量 | 关联依赖 |
|----|---------------|--------|----------|
| P0 | **B-V4-1**    | 极小   | 修 place_buy/gtd_sell/sell 失败路径，加 `_cleanup_failed_post()` helper + 1-2 单测 |
| P0 | **B-V4-2**    | 极小   | router 中 T1/T2/T3/T4 失败时回滚 `entry_token`/`entry_side`，加 ERROR log + 1-2 单测 |
| P1 | H-V4-1        | 极小   | OM 暴露 `place_buy_by_notional(notional)`，router 改用它 + 单测 |
| P1 | H-V4-2        | 极小   | `stake_cap_remaining` 用 `total_notional_committed` + 单测 |
| P2 | M-V4-1 ~ M-V4-3 | 极小 | round-end purge / bind-race guard / drain 调用点 |

---

## 7. 关键路径速查表 (2026-07-11 v4)

| 文件 | 行数 | 角色 | 状态 | 备注 |
|------|------|------|------|------|
| `src/strategy/engine.py` | 634 | 决策核心 | ⚠️ B-V4-2 (失败回滚) | H-1/H-3/H-NEW-2/H-NEW-3 都已 OK |
| `src/strategy/round_state.py` | 515 | 状态机 | ⚠️ H-V4-2 (cap 基准) | B-NEW-1 v3 修已 OK；`total_notional_committed` 已存在未用 |
| `src/strategy/gatekeeper.py` | 120 | BTC drift gate | ✅ OK | |
| `src/strategy/loop.py` | 99 | 250ms 驱动 | ✅ OK | `_inflight` lock 是真锁 |
| `src/execution/order_manager.py` | 735 | 下单 / 取消 | 🔴 B-V4-1 (失败 cleanup) | `drain_pending_fills` dead code |
| `src/execution/clob_client.py` | 819 | GTD BUY / SELL / FAK | ✅ OK | deduper / register_pending 都好 |
| `src/execution/gtd_expiry_watcher.py` | 141 | GTD 到期清扫 | ✅ OK | |
| `src/execution/heartbeat.py` | 111 | 5s 心跳 | ✅ OK | |
| `src/main.py` | 940 | ActionRouter + Wiring | 🔴 B-V4-2 (失败回滚) | |
| `src/config.py` | 209 | 阈值 / martingale | ✅ OK | |
| `src/clob_l1_auth.py` | 184 | L1 wallet 认证 | ✅ OK | |

---

## 8. 必须新加的单测 (P0 修复前先写)

1. **test_b_v4_1_post_failure_drops_sentinel**:
   mock CLOBClient 让 `register_pending(server_id)` 在 `place_gtd_buy` 内被调，然后 `place_gtd_buy` 返回 `success=False` → 断言 `_by_server[server_id]` 不存在。

2. **test_b_v4_1_post_failure_drains_pending_fills**:
   mock CLOBClient 让 `apply_fill_event` 在 sentinel 状态下触发 (fill 进 `_pending_fills`) → `place_buy` 返回 success=False → 断言 `_pending_fills[server_id]` 为空。

3. **test_b_v4_2_router_t1_failure_rolls_back_entry_token**:
   mock OM.place_buy 返回 None → 调 `ActionRouter.__call__(ActionOpenT1(...))` → 断言 `pos.entry_token is None` (被回滚) + ERROR log。

4. **test_b_v4_2_router_t2_failure_rolls_back_tier**:
   mock OM.place_buy 返回 None (T2) → 断言 `pos.tier2.placed == False` + ERROR log。

5. **test_h_v4_1_size_uses_adjusted_price_not_bid**:
   mock OM 暴露 `_adjust_buy_price` → 断言 `place_buy` 收到的 `size = notional / adjusted_price` 而非 `notional / bid_price`。

6. **test_h_v4_2_stake_cap_uses_committed_not_filled**:
   T1 filled (cost=2.0), T2 placed but no fill → 断言 `stake_cap_remaining = 16 - 4 = 12`（而非 14）。

7. **test_m_v4_2_bind_race_re_arms_tier**:
   tier.placed=True, tier.server_order_id=None (人为模拟 bind race) → `_check_average_down` 应跳过 OR 重置并重新触发。

8. **test_m_v4_3_drain_pending_called_on_cancel**:
   模拟 partial fill queue + cancel → 断言 `_pending_fills[server_id]` 被清空。

---

## 9. 总结

v3 (2026-07-11) 审计罗列的 B-NEW-1 / B-NEW-2 / H-NEW-1~3 / M-NEW-4 等**核心 Blocker + High 已修复并单测通过** (47/47 PASS)。

**v4 (本轮) 新发现的**核心问题**：

1. **🔴 B-V4-1 place_* 失败路径泄漏 sentinel + pending_fills** — 与 v3 H-NEW-1 同源（修复未闭合失败路径）。
2. **🔴 B-V4-2 router T1/T2-T4 失败不回滚 entry_token/tier.placed** — 与 v2 H-1 同源（修复未考虑失败回滚）。
3. **🟠 H-V4-1 size 计算用 bid_price，与 adjusted_price 不一致** — 单 tier 多花 1-3%，长期累积影响 PnL。
4. **🟠 H-V4-2 stake_cap_remaining 用 filled 而非 committed** — 当前 latent（因 Layer 2 保护），未来引入并发补仓会爆发。

**各档仓位控制整体审视**：
- 互斥控制 (Layer 1-3) 设计正确，单测覆盖。
- 总额控制 (Layer 4) 有 H-V4-2 漏洞，**目前 latent 但结构性 risk**。
- "一次一档" 不变量由 `placed` + early return 共同保证。
- TP 路径与 averaging 路径通过 `_tp_inflight` 互不感知，符合"TP 是 close，averaging 是 open"的设计意图。

**建议**: P0 修完（B-V4-1 + B-V4-2），代码即可投产；P1 在首周内完成；P2 季度内清理。