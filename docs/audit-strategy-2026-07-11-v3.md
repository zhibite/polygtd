# 入场 / 补仓 / 止盈 全面审计 v3 (深度复核)

- 审计人: code review (静态 + 动态 + 31 项回归测试 + 路径追溯)
- 日期: 2026-07-11
- 范围: `src/strategy/*`, `src/execution/*`, `src/main.py`, `src/config.py`
- 上一版: `docs/audit-strategy-2026-07-11.md` (v2) → **本轮发现 1 个新 Blocker, 3 个新 High, 4 个新 Medium**
- 测试基线: 31/31 通过 (test_audit_fixes_2026_07_10 + _07_11 + test_tp_decrement + test_tp_gtd_sell)

## 严重度图例

- **Blocker** — 阻止策略按设计运行或直接造成**资金风险**
- **High** — 在常见路径中会触发明显偏差
- **Medium** — 边界场景下会偏离设计意图
- **Low** — 日志 / 一致性 / 可维护性

---

## TL;DR (优先处理顺序)

| P  | 项                  | 简介                                                                                          | 改动量 |
|----|---------------------|-----------------------------------------------------------------------------------------------|--------|
| P0 | **🔴 B-NEW-1**      | `record_fill` 无差别清 `_tp_inflight` → 补仓 BUY 触发会**把还在路上的 TP 卖单错误地解锁**，下一次 _evaluate 直接发第二条 TP → 双 TP 卖单 | 小     |
| P0 | **🔴 B-NEW-2**      | `place_buy` 内 `await self._adjust_buy_price` (走 PM orderbook REST) 在 `_lock` 内执行 — 任一 place_buy 失败/慢，所有 BUY 串行排队；如果 T1 GTD 失败取消后立即触发 T2 占着锁，T1 重试会卡 1-3s | 中     |
| P0 | 🟠 H-NEW-1          | `bind_server_order_id` 调用顺序在 `router.__call__` 里发生在 `entry_token = action.token` 之后 (engine.py:425-426 已前置)，但 `om.place_buy` 仍然走 REST → CLOB 两次 `await`，期间 `engine._eval_inflight=True` 阻塞后续 evaluate，但 `_make_fill_callback` 内的 om.by_server_id 依赖的 `_by_server` 字典 insert 发生在 `place_buy` return 之后；T1 fill 在 CLOB 极快成交场景下，callback 可能在 `_by_server[server_id]=cid` 之前到达 → `om.untracked` 静默丢 fill | 小     |
| P0 | 🟠 H-NEW-2          | 补仓决策：`_check_average_down` 一次 evaluate 只下一档 (return at line 549)，但 TP 卖单 GTD 等待 + FAK 期间仍占锁 90-180s，期间**完全不会**评估补仓 → 极端情况下 T1 跌 60% 但锁被 TP 占着，补仓被吞 | 中     |
| P0 | 🟠 H-NEW-3          | T1 BUY 自身在 TP 触发前的最后一次 `_check_take_profit` 内 `pos._tp_inflight=True` 之后，**仍在同一个 `_eval_inflight` 锁里**；同一帧接下来要走 `_check_average_down` (line 347) → 已不触发 (entry_token!=None 时) → 但 `_eval_inflight` 直到 handler await 完成才释放，期间 Binance tick 进不来 | 小     |
| P1 | 🟡 M-NEW-1          | `TP_GTD_LIFETIME_S=90` 与 `GTD_MIN_SEC=180` clamp 后实际生效 181s；CLOB 文档支持短 GTD，但 v2 修后实际**全部 TP 等同 181s**，与文档注释 (`TP_GTD_LIFETIME_S is short on purpose`) 不符 | 极小   |
| P1 | 🟡 M-NEW-2          | TierSnapShot `is_complete` 用 99.5% 阈值 (`size_filled >= size_expected * 0.995`) — 但 `size_expected` 来自 `placed_size` (CLOB 接受)，CLOB 内部 tick 是 0.01；fill 可能 5.1282/5.1282=100%；如果 CLOB 报 5.12 但 placed_size=5.1282 (差异 0.0016 即 0.03%) 阈值卡 99.5% 边界可能不触发 | 小     |
| P1 | 🟡 M-NEW-3          | `trends_baseline_target_price` (engine.py:433) 在 T1 决策时记录**bid_price**，但 `bind_server_order_id` 之后 `record_fill` 又把 `baseline_price = tier.avg_fill_price` 写入 (round_state.py:402)。两条线并存，但**只** `baseline_target_price` 用于补仓触发；`baseline_price` 只在 TP 中通过 `_avg_cost` 间接影响 TP target。文档里说的"H-3 fix 把 fill avg 与 target 分离"完全正确，但**fill avg 与 cost basis 不一致**会让用户看到 PnL 计算偏离实际 | 小     |
| P1 | 🟡 M-NEW-4          | `_adjust_buy_price` 用 `BUY_STEP=0.001` (order_manager.py:64)，但 tick_size=0.01 — `bid + 0.001` 在 tick_size=0.01 时**不是 tick-aligned**，可能被 CLOB 拒接或自动 round-down → `placed_size` 不准 (M-1 已记录，但根因没修) | 极小   |
| P2 | 🟢 L-NEW-1          | `engine.on_binance_tick / on_token_tick` (engine.py:135-141) 现在是死方法 (v2 已识别)，仍保留约 7 行；删之即可 | 极小   |
| P2 | 🟢 L-NEW-2          | `_eval_inflight` 是普通 bool，DecisionLoop 也有 `_inflight: asyncio.Lock`，双重锁；engine 内的 bool 实质是冗余 (M-4 v2 已识别) | 极小   |

---

## 1. 新发现的 Blocker

### 🔴 B-NEW-1 (新): `record_fill` 无差别清除 `_tp_inflight`，触发双 TP 卖单

- **路径**:
  - `src/strategy/round_state.py:390-398` (`record_fill` 内的 `_tp_inflight` 清除分支)
  - `src/strategy/engine.py:565-567` (`_check_take_profit` 用 `_tp_inflight` 防重入)
  - `src/strategy/engine.py:594` (`_check_take_profit` 把 `_tp_inflight=True` 后 await handler)

- **症状** (确定): `record_fill` 在 BUY tier 触发时 (T1/T2/T3/T4) 不区分 fill 来源就清除 `_tp_inflight`：

  ```python
  # round_state.py:390-398
  # Any fill landing while TP is in-flight means the previous TP did
  # not consume the full position (FAK partial), so allow the engine
  # to re-fire TP on the next tick if mid is still above target.
  if self._tp_inflight:
      self._tp_inflight = False
  ```

  这个注释假设"TP in-flight 期间来的 fill 必然是 TP 的 partial fill"，但**实际上**也可能是一笔补仓 BUY 的 fill。

- **复现序列** (假设 T1 @ 0.40 入场, 涨到 0.50 → 触发 TP):
  1. tick t0: `_check_take_profit` 看到 bid=0.50 ≥ tp_target=0.48 → 设置 `_tp_inflight=True`，await handler → router 启动 `_tp_runner`
  2. tick t1 (250ms 后): `_evaluate` → `_check_average_down` 不会触发 (mid=0.50 > T2 trigger 0.32)，`_check_take_profit` 因为 `_tp_inflight=True` skip；OK
  3. 但如果 mid 在 tick t1 反向回到 0.30 (BTC 反向)，同时 TP runner 还在等 GTD 90s：
     - tick t2 (mid 0.30): `_check_take_profit` 因 `_tp_inflight=True` skip → 正确
     - tick t3 (mid 0.30): `_check_average_down` 触发 T2 BUY (place_buy 成功 fill 4 shares @ 0.32)
     - T2 fill → `record_fill(tier2, 0.32, 4.0)` → 进入 line 393 → **`_tp_inflight=False`**
  4. tick t4 (mid 反弹回 0.48)：`_check_take_profit` 看到 `_tp_inflight=False` 且 bid ≥ tp_target → **触发第二条 TP 卖单**
  5. 此时第一条 TP 的 GTD 还活着 (CLOB 上挂)，第二条 TP 又上去了 → **双 TP 卖单同时跑** → 用户可能被卖两次 `total_qty` (如果有那么多货的话；否则 oversell defensive clamp，但仍然多发单)

- **影响** (🔴 资金级): **双 TP 卖单** + 仓内 residual 可能 mismatch，导致 record_tp_fill 走 oversell 分支。

- **修复方向**:
  ```python
  # round_state.py:390-398 改：
  # 只在 TP 的 partial fill (即 record_tp_fill 已走) 才清 _tp_inflight；
  # record_fill 是 BUY tier fill，不应清。
  # 删除 line 393-398 的整段 if。
  ```
  - 或者：把 `_tp_inflight` 的清除搬到 `record_tp_fill` 内 (已经有 line 320, 338 的逻辑)，`record_fill` 完全不动它。
  - 加单测: `test_tp_inflight_not_cleared_by_buy_fill_during_tp`：T1 已入仓、TP 触发 → 模拟 T2 BUY fill 到达 → 断言 `_tp_inflight` 仍 True。

- **与 v2 的关系**: v2 B-2 修的是 "TP fill state 不更新"，没处理 "BUY fill 错误清 TP 锁"。v3 新发现的反向污染。

---

### 🔴 B-NEW-2 (新): `place_buy` 在 OM `_lock` 内做 REST orderbook fetch，serialize 所有 BUY

- **路径**:
  - `src/execution/order_manager.py:163-189` (`place_buy` 整段在 `async with self._lock:` 内)
  - `src/execution/order_manager.py:104-113` (`_adjust_buy_price` 走 `self._pm.get_order_book`)
  - `src/execution/order_manager.py:519-528` (`place_sell` 也在 `_lock` 内，且包含 `_adjust_sell_price` REST)

- **症状** (确定): 整个 `place_buy` 持有 OM 自家 `_lock` 跨两个 `await`：
  1. `await self._adjust_buy_price(token_id, price)` — 走 REST GET orderbook，可慢可失败
  2. `await self._clob.place_gtd_buy(...)` — 走 REST POST create_and_post_order，可能 200ms-2s

- **影响** (🔴 资金级 + 性能):
  - **串行化**: 所有 BUY (T1/T2/T3/T4 + 重试) 都串行。如果某笔 BUY 在 CLOB 上挂起 3s (限流)，后续 BUY 全等。
  - **T1 失败重试** + **T2 同时触发**：T1 GTD 到期 (expire) → `_tp_runner` 没介入；下一个 tick 触发 T2 → `place_buy` 持锁，await `_adjust_buy_price` 慢；T1 走 `on_order_expired` 路径 → 想重新下 T1 → `place_buy` 卡在 T2 后面。
  - **测试可见**: `tests/test_tp_gtd_sell.py` 都没问题，但真实场景 `_adjust_buy_price` 慢 (PM REST 1-2s) 时，250ms DecisionLoop 期间完全插不进来。

- **修复方向**:
  ```python
  # 把 _adjust_buy_price 移到 _lock 外：
  async def place_buy(self, token_id, price, size, ...):
      adjusted_price = await self._adjust_buy_price(token_id, price)
      async with self._lock:
          # 只锁 post + 状态 insert 部分
          result = await self._clob.place_gtd_buy(...)
          ...
  ```
  - 把 `_lock` 范围缩到 CLOB post + 状态 mutation；REST orderbook 查询在外。
  - 或者把 `_adjust_buy_price` 改用 `price_cache.bid()` (已订阅 WS)；省掉 REST 调用。

- **与 v2 的关系**: v2 没专门看 OM `_lock` 的 scope；B-NEW-2 是 OM 并发模型的 hidden cost。

---

## 2. 新发现的 High

### 🟠 H-NEW-1 (新): T1 GTD 极快成交时 fill 回调可能早于 `_by_server` insert

- **路径**:
  - `src/main.py:316-353` (`_make_fill_callback` 内 `on_fill`，调 `om.by_server_id(order_id)`)
  - `src/execution/order_manager.py:228-229` (`self._by_server[server_id] = cid` 发生在 `await place_gtd_buy` 之后)
  - `src/execution/clob_client.py:333-373` (`place_gtd_buy` 在 poll 内检测到 fill → `_fire_fill_callback` 在 return result 之前)

- **症状** (确定性高): CLOBClient.place_gtd_buy 在 poll 循环里检测到第一次 fill 时**立刻** `_fire_fill_callback`，然后才 return FillResult；OrderManager.place_buy 在 `result = await place_gtd_buy(...)` 返回之后才 `_by_server[server_id] = cid`。

  时间线：
  ```
  T0  router.__call__ → om.place_buy(...) 
  T1      OM: await self._clob.place_gtd_buy(...)  ← poll 第一次成功
  T1.5        CLOB poll fires callback (cumulative matched)
  T1.6        deduper.submit returns delta>0
  T1.7        self.fill_callback(token_id, order_id, fill_price, delta, total)
  T1.8            → main._make_fill_callback.on_fill
  T1.9                → om.by_server_id(order_id)  ← _by_server 还是空的!
  T2      CLOBClient: returns FillResult
  T3  OM: self._by_server[server_id] = cid  ← 太晚了
  ```

- **影响** (🟠 资金级): `om.untracked` log 静默丢 fill，engine 不收到 record_fill；但 CLOB 上钱已扣。下次同一 order 的 UPDATE 事件再来时 `_by_server` 已有，能 match 上，但**首次 fill 丢**。

  - 实际症状: tier.size_filled 缺失第一笔；is_complete 可能永远不触发 (因为缺一份 partial)；engine 会一直等；用户钱被扣但 bot 不知道 → 不会进入 TP 路径 (qty 仍 0)，残仓漂到结算。
  - **概率**: 取决于 poll interval (500ms) vs CLOB 成交速度。如果是 market taker 秒填，500ms 内 fill 是常态，概率不低。

- **修复方向**:
  1. **方案 A** (推荐): 在 `place_gtd_buy` 创建 server_id 后立即把 server_id 加入 OM 的 `_by_server` (即便 fill callback 还没来)；OM 暴露 `register_server_id(cid, server_id)`，CLOBClient 在收到 server_id 那一刻就回调 register。
  2. **方案 B**: 在 `_fire_fill_callback` 内部对 `om.untracked` 的情况做 retry (50ms 后再查一次)。
  3. **方案 C**: 把 `_by_server[server_id] = cid` 移到 `await place_gtd_buy(...)` 之前 — 但那时 server_id 还没拿到。
  4. **方案 D** (实测): 把 CLOBClient.poll 的 `await self._fire_fill_callback` 改成只在 OrderManager 显式调用 (polling 报告 fill 时不再 fire callback)；让 OrderManager 拿到 result 后自行 fire。这个改动较大，但语义清晰。

- **与 v2 的关系**: v2 B-3 修的是 deduper，没修 callback 早于 `_by_server` insert 的问题。

---

### 🟠 H-NEW-2 (新): TP GTD + FAK 期间 `_tp_inflight=True` 阻塞补仓评估逻辑

- **路径**:
  - `src/main.py:204-294` (`_tp_runner` 协程)
  - `src/strategy/engine.py:342-348` (`_evaluate` 内 `_check_average_down` 顺序)

- **症状**: 实际路径里 `_check_average_down` 和 `_check_take_profit` **互不感知** `_tp_inflight`。具体：
  - `_check_average_down` (engine.py:442) 看 `pos.entry_token` / `pos.baseline_set`，**不看** `_tp_inflight`。
  - `_check_take_profit` (engine.py:566) 因为 `_tp_inflight=True` skip。

  因此补仓逻辑**理论上**仍能跑。**但实际**：
  1. _tp_runner 启动后会注册 TP 到 `_tp_snapshots`
  2. 一旦 TP GTD partial fill 到达 → `record_tp_fill` 清 `_tp_inflight`
  3. 然后下次 evaluate 又触发 TP (如果 mid 仍 ≥ tp_target)

  而 B-NEW-1 又会把补仓 fill 也清 `_tp_inflight`，导致同一时间多个 TP 路径冲突。

  **更深的问题**: TP 等待 GTD 90s (实际 181s) 期间：
  - 价格可能反向 (TP 触发完不再成立)
  - 但补仓**应该**仍可触发 (TP 是 close 路径，averaging 是 open 路径，逻辑上不冲突)
  - 现在补仓会跑，但下一帧 TP 又会被错误解锁重发 (见 B-NEW-1)

- **影响** (🟠 资金级): 补仓 + TP 路径的相互干扰放大；最坏情况下：TP GTD 在 CLOB 挂着 + 用户又触发补仓 + TP 又被错误重发 → 多次 sell。

- **修复方向**: 修 B-NEW-1 后此问题大幅缓解；额外加固：让 `_check_average_down` 也尊重 `_tp_inflight` (skip during TP wait)，因为 TP 卖单 cover 全部仓位逻辑上不应再补仓。

---

### 🟠 H-NEW-3 (新): 同一 `_eval_inflight` 内 `bid_price` 用于 T1 决策但 bind 之后 fill_price 可能不同

- **路径**:
  - `src/strategy/engine.py:395` (`bid_price = self._pc.bid(token)` 用于 T1 决策)
  - `src/strategy/engine.py:433` (`pos.baseline_target_price = bid_price`)
  - `src/execution/order_manager.py:182` (`adjusted_price = await self._adjust_buy_price(token_id, price)`) — 走 REST orderbook，可能与 `bid_price` 不同
  - `src/execution/order_manager.py:215` (`price=adjusted_price` 存到 TrackedOrder)

- **症状**: T1 决策用的 `bid_price` (来自 `self._pc.bid()` 即 WS 缓存) 与实际下到 CLOB 的 `adjusted_price` (REST orderbook bid + BUY_STEP) 可能差几 tick。`baseline_target_price = bid_price` 锚定的是 WS 缓存，而实际 fill 是按 adjusted_price 成交的。

  ```
  bid_price (WS, 可能 stale 200ms) = 0.39
  REST orderbook bid_now = 0.395
  adjusted_price = 0.395 + 0.001 = 0.396
  actual fill = 0.396 (taker 接受 0.396)
  
  baseline_target_price 记录 0.39
  但实际 fill avg = 0.396
  → T2 trigger = 0.39 * 0.80 = 0.312
  → 用户期望 T2 trigger 是 0.396 * 0.80 = 0.317 (与 fill avg 一致)
  ```

- **影响** (🟠 资金级): 补仓触发线比设计提前 / 推迟，方向取决于 WS vs REST 的 staleness。Slippage 在 0.30 token 上 1 tick = 0.0033 = 1.1% —— 影响 T2 trigger 大约 0.005 (0.005/0.39 ≈ 1.3%)，**显著但不是灾难**。

- **修复方向**:
  - `baseline_target_price` 在 `bind_server_order_id` 后改为 `order.price` (实际 adjusted_price)，与 OM 真相对齐。
  - 或者：在 `_check_t1_entry` 内部直接调 `_adjust_buy_price` 拿到 adjusted 后再决策；但这违反 "engine 不做 IO" 的分层。
  - 或者：把 `baseline_target_price = order.price` 移到 `bind_server_order_id` 内 (engine.py:265-296)。

---

## 3. 新发现的 Medium

### 🟡 M-NEW-1: `TP_GTD_LIFETIME_S=90` 与 CLOB clamp=181 的语义偏差

- **路径**:
  - `src/execution/order_manager.py:33-34` (`TP_GTD_LIFETIME_S: int = 90`)
  - `src/execution/order_manager.py:280` (`effective_lifetime = max(gtd_lifetime, _CC.GTD_MIN_SEC + 1)` = 181)
  - `src/execution/order_manager.py:424` (`wait_s = max(TP_GTD_LIFETIME_S, _CC.GTD_MIN_SEC + 1) - TP_FALLBACK_BUFFER_S` = 166s)

- **症状**: v2 H-3 修把 lifetime 与 CLOB clamp 对齐，但**TP 等待时间实际 166s** (不是用户配置的 90s)，TP runner 占事件循环 166s 期间 DecisionLoop 仍能跑 (是 fire-and-forget)，但 BUY/fill callback 仍然能 push 进 pos 状态。

- **影响** (🟡): 与文档注释不符；如果用户改 TP_GTD_LIFETIME_S=30 (预期 30s 内 GTD 到期)，实际仍等 166s。

- **修复方向**:
  - 文档注释改 "TP_GTD_LIFETIME_S is min desired, CLOB clamps to 181s"
  - 或者：暴露 `TP_GTD_LIFETIME_S` 的实际生效值 (`effective_lifetime`) 到日志。
  - 或者：用 CLOB 实际 GTD 寿命 (`result.expiration_ts`) 来计算 wait_s，而非用本地 `TP_GTD_LIFETIME_S`。

---

### 🟡 M-NEW-2: `is_complete` 99.5% 阈值在 tick_size=0.01 的 placed_size 上可能不触发

- **路径**:
  - `src/strategy/round_state.py:86-90` (`is_complete` 计算)
  - `src/execution/order_manager.py:221` (`placed_size=result.tokens if result.tokens > 0 else size`)

- **症状**: CLOB `placed_size=5.1282` (例如)；fill 实际可能是 5.1282 (一次填满) 或 5.0000 (5.1282 拆成 5 + 0.1282 partial partial)。后者 `5.0000 / 5.1282 = 97.5%` < 99.5% → `is_complete=False`。

  如果 CLOB 报 fill 拆成两笔 (5 + 0.1282)：
  - 第一次 fill 5.0 → size_filled=5.0, is_complete=False (97.5%)
  - 第二次 fill 0.1282 → size_filled=5.1282, is_complete=True

  deduper 保证两次都进 record_fill。OK。

  **但是**：如果 CLOB 报 cumulative matched=5.1282 但第二笔 fill 的 SDK 事件没到 → is_complete 永远 False。这是 CLOB event 丢失问题，引擎层面无法修。

- **影响** (🟡 边界): `is_complete` 99.5% 阈值的容错空间仅 0.5%。在 5-share minimum 下，0.5% = 0.025 shares ≈ 0.5 tick。如果是 5.0000/5.1282 这种 CLOB round-down 案例，5.0000 离 99.5% 阈值差 2.5%，所以**这个具体案例不会卡住**。但理论上 5.0001/5.0050 = 99.9% 卡线就难说。

- **修复方向**:
  - 把 99.5% 改成 98% (容错更大)；或者
  - 把判定从 `size_filled/size_expected` 改成 `cost_filled/(tier.notional * TIER_NOTIONAL_MAX_MULT)` 直接用 USD 而非 share 数；或者
  - 加 `size_matched` (来自 CLOB API) 作为辅助信号。

---

### 🟡 M-NEW-3: `baseline_target_price` (T1 bid) vs `baseline_price` (T1 fill avg) 双线并存的可读性

- **路径**:
  - `src/strategy/round_state.py:170-176` (`baseline_target_price` 字段)
  - `src/strategy/round_state.py:168-169` (`baseline_price` 字段)
  - `src/strategy/round_state.py:401-403` (`record_fill` 内 anchor baseline_price)
  - `src/strategy/engine.py:471` (`baseline = pos.baseline_target_price or pos.baseline_price`)

- **症状**: H-3 v2 修引入 `baseline_target_price` 后，两条 baseline 并存：

  | 字段                | 写入时机            | 用途                       | 影响                    |
  |---------------------|---------------------|----------------------------|-------------------------|
  | `baseline_target_price` | T1 决策时 (engine.py:433) | T2/T3/T4 触发线           | slippage 隔离           |
  | `baseline_price`       | T1 fill 后 (round_state.py:402) | TP target (通过 _avg_cost) | 实际成本锚定            |

  这俩**事实上都正确**，但：
  - 文档不清晰：`_avg_cost` 用的是 fill avg，而 T2 触发用的是 target price。如果用户问"为什么 T2 触发了但 avg_cost 没动" 会困惑。
  - `_tp_target()` 用 `_avg_cost` (来自 fill avg)，而 T3 trigger 用 `baseline_target_price`。两个不同锚点，但都在 round_state 里，可能误导。
  - audit v2 H-3 修错把 baseline_target_price 作为补仓 trigger 是对的，但 `_avg_cost` 与 `baseline_target_price` 没有显式关系文档。

- **影响** (🟡 可读性 + 维护): 不影响运行，但容易让后人误改。

- **修复方向**:
  - 加 docstring 明确两字段的语义；
  - 或合并：`baseline_target_price` 仅作为 T1 decision-time anchor (immutable after T1)，`baseline_price` 改名 `_avg_fill_cost` 表示 fill cost。

---

### 🟡 M-NEW-4: `BUY_STEP=0.001` 与 tick_size=0.01 的 tick 对齐问题

- **路径**:
  - `src/execution/order_manager.py:64` (`BUY_STEP: float = 0.001`)
  - `src/execution/order_manager.py:112` (`round(bid + self.BUY_STEP, 4)`)
  - `src/execution/clob_client.py:294` (`price_q = _round_down(price, tick_d)` where tick_d = Decimal("0.01"))

- **症状**: OM 算 `bid + 0.001 = 0.395001`，CLOB 用 `_round_down(0.395001, Decimal("0.01"))` = 0.39 (因为 ROUND_DOWN 到 0.01)。也就是说 `bid=0.39, adjusted=0.391` 但实际到 CLOB 是 0.39 — 与 bid 等价，**没真正"加 BUY_STEP"**。

  CLOB tick_size=0.001 的市场（例如 binary options 早期）会受益；现在 Polymarket BTC 15m 是 0.01 tick，`BUY_STEP=0.001` 是死参数。

- **影响** (🟡 微小): 如果 CLOB tick=0.01，`bid + 0.001 → 0.395+0.001=0.396`，round_down 到 0.39 = 0.39，等于 bid。**实际上没真正 ask-bid 抓 taker**。但因为 bid 已经在 CLOB 挂单簿上，下 GTD @ bid 通常立即 fill (taker 抢挂)，所以仍 OK。

- **修复方向**:
  - 用 `tick_size` 计算 step：`step = tick_size / 2`，让 GTD 价在 bid 和 ask 之间。
  - 或维持 `bid + tick_size` 但**确保 round-down 后不等于 bid**。

---

## 4. v2 修复状态复查 (2026-07-11)

| v2 项 | 状态 | 验证 |
|-------|------|------|
| **B-1 (复制粘贴 bug)** | ✅ 已修 | `tests/test_audit_fixes_2026_07_11.py::test_b1_v2_buy_single_call` PASS |
| **B-2 (TP fill state)** | ✅ 已修 | `_tp_snapshots` + `record_tp_fill` 实现正确；但 B-NEW-1 引入新风险 |
| **B-3 (deduper)** | ✅ 已修 | `_fire_fill_callback` 走 deduper；但 H-NEW-1 时序问题 |
| **H-1 (T1 race)** | ✅ 已修 | `engine.py:425-426` 前置写 `entry_token` |
| **H-2 (TP sleep)** | ✅ 已修 | fire-and-forget via `_tp_runner` + `_bg_tasks` |
| **H-3 (baseline)** | ✅ 已修 | `baseline_target_price` 引入；但 B-NEW-1/H-NEW-3 衍生 |
| **H-4 (drop_pct vs notional)** | ⚠️ 设计层 | martingale 数学未实现 assertion；**当前部署影响不大** (用户用 v3 表) |
| **H-5 (compute helper)** | ⚠️ 仍重复 | `trigger_price` 在 `TierSnapshot._next_tier_drop` + `can_double_down` + `trigger_price` 三处 |
| **H-6 (B-1 fix 联动)** | ✅ 自然消解 | B-1 修了 H-6 自动消失 |
| **H-7 (cancel residual)** | ⚠️ 未修 | `cancel_market` 仍不 reconcile partial fill residual |
| **M-1 (size_expected)** | ✅ 已修 | `placed_size` 路径 |
| **M-2 (BUY cancel race)** | ⚠️ 理论 OK | 没显式 race 测试 |
| **M-3 (TP clamp)** | ⚠️ 仍 181s | M-NEW-1 |
| **M-4 (双重 inflight)** | ⚠️ 仍双重 | `engine._eval_inflight` 与 `loop._inflight` 互不感知但 OK |
| **M-5 (tier index guard)** | ⚠️ 仍 IndexError | 没显式 1..4 校验 |
| **M-6 (apply_fill_event vs mark_filled)** | ⚠️ 仍双调 | main.py:330, 336 两处 |
| **M-7 (tier_remaining 0.5)** | ⚠️ 仍双重 | engine.py:517 与 `tier_is_saturated` 重叠 |
| **L-1 (token_id unused)** | ⚠️ 保留 | deduper 签名冗余 |
| **L-2 (tp_oversell warning)** | ⚠️ 缺日志 | round_state.py:318 |
| **L-3 (size_matched 双源)** | ⚠️ 仍双源 | deduper bookmark vs TrackedOrder |
| **L-4 (cancel_market dead return)** | ⚠️ 未用返回值 | main.py:423 |
| **L-5 (is_empty sold_out)** | ⚠️ sold_out 后 False | round_state.py:214 |
| **L-6 (engine.on_*_tick dead)** | ⚠️ 死方法 | engine.py:135-141 (建议删) |

---

## 5. 修复优先级 (本轮新增)

| P  | 项             | 改动量 | 关联依赖 |
|----|---------------|--------|----------|
| P0 | **B-NEW-1**   | 极小   | 删 `record_fill` 的 _tp_inflight 清除分支；移到 record_tp_fill；加 1 个 mock 单测 |
| P0 | **B-NEW-2**   | 小     | 把 `_adjust_buy_price` 移到 `_lock` 外；或用 price_cache.bid() |
| P0 | H-NEW-1       | 小     | 注册 server_id 提前；或 _fire_fill_callback 改 sync 通知 |
| P0 | H-NEW-2       | 小     | `_check_average_down` 尊重 `_tp_inflight` |
| P0 | H-NEW-3       | 极小   | `baseline_target_price = order.price` 在 `bind_server_order_id` 内 |
| P1 | M-NEW-1 ~ M-NEW-4 | 各极小 | 注释 / docstring / 阈值 |
| P2 | L-NEW-1, L-NEW-2 | 极小 | 删除 / 整合 |

---

## 6. 关键路径速查表 (2026-07-11 v3)

| 文件 | 行数 | 角色 | 状态 |
|------|------|------|------|
| `src/strategy/engine.py` | 611 | 决策核心 | ✅ 已加固 (H-1/H-3 v2 fix) |
| `src/strategy/round_state.py` | 507 | 状态机 | ⚠️ B-NEW-1 需修 |
| `src/strategy/gatekeeper.py` | 120 | BTC drift gate | ✅ OK |
| `src/strategy/loop.py` | 100 | 250ms 驱动 | ✅ OK |
| `src/execution/order_manager.py` | 735 | 下单 / 取消 | ⚠️ B-NEW-2 (lock scope) |
| `src/execution/clob_client.py` | 783 | GTD BUY / SELL / FAK | ✅ 已加固 (deduper) |
| `src/execution/gtd_expiry_watcher.py` | 141 | GTD 到期清扫 | ✅ OK |
| `src/execution/heartbeat.py` | 111 | 5s 心跳 | ✅ OK |
| `src/main.py` | 933 | ActionRouter + Wiring | ⚠️ H-NEW-1 (callback 时序) |
| `src/config.py` | 209 | 阈值 / martingale 配置 | ✅ OK |
| `src/clob_l1_auth.py` | 184 | L1 wallet 认证 | ✅ OK |

---

## 7. 必须新加的单测 (建议在修复前先写)

1. **test_b_new_1_tp_inflight_not_cleared_by_buy_fill**：T1 已 fill，TP GTD 在 CLOB 挂着 (`pos._tp_inflight=True`)，模拟 T2 BUY partial fill 到达 → 断言 `_tp_inflight` 仍 True。
2. **test_b_new_2_buy_lock_not_blocked_by_adjuster**：用 stub `_adjust_buy_price` 慢 1s，断言两次 `place_buy` 并发调用时第二次不被卡 (需要修改 _lock scope 后才过)。
3. **test_h_new_1_fill_callback_finds_order_after_fast_fill**：模拟 place_gtd_buy 在 poll 第一次返回前 fire callback，断言 om.by_server_id 不返回 None (需要 server_id 注册提前)。
4. **test_h_new_2_evaluate_skips_avg_during_tp_inflight**：TP in-flight 时 evaluate 应不触发 averaging。
5. **test_h_new_3_baseline_target_uses_actual_order_price**：T1 决策后 `bind_server_order_id` 调用，断言 `pos.baseline_target_price` 等于 `order.price` (而非 engine 的 `bid_price`)。
6. **test_m_new_4_buy_step_tick_alignment**：mock tick_size=0.01 时，断言 `adjusted_price` 至少 +1 tick (而非 +0.001)。

---

## 8. v2 vs v3 总结

v2 (2026-07-11) 审计罗列的 B-1 / B-2 / B-3 / H-1 / H-3 等已**修复并单测通过** (31/31 PASS)。

v3 (本轮) 新发现的**核心**问题：

1. **🔴 B-NEW-1 record_fill 误清 _tp_inflight** —— 可能双 TP 卖单，必须第一时间修。
2. **🔴 B-NEW-2 place_buy 在 lock 内做 REST** —— 序列化所有 BUY，slow REST 阻塞关键路径。
3. **🟠 H-NEW-1 fill callback 早于 _by_server insert** —— 可能丢首笔 fill，账本与 CLOB 不一致。
4. **🟠 H-NEW-2 TP 与 averaging 路径互不感知** —— 加剧 B-NEW-1 的多发 TP 风险。
5. **🟠 H-NEW-3 baseline_target 用 WS bid 而非 adjusted price** —— 补仓触发线偏离 fill avg。

代码整体结构清晰，分层健康，deduper / expiry_watcher / heartbeat / DecisionLoop 都是好设计。**P0 修完即可投产**。