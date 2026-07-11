# Strategy audit — 2026-07-11 v6

Round switch window never advances beyond the boot round on startup-near-boundary.
This continues the audit series (`v1` → `v5`).

## Symptom (production log 2026-07-11 00:29:59 → 00:31:29)

```text
00:29:59.573  window_mgr.started   current_round=1783728900  period_s=900
00:30:00.385  window_mgr discovered  end=00:30:00
00:30:00.807  engine.new_round     round_start_ts=1783728900     ← boot _on_new
00:30:00.863  engine.gate          drift=0.000784898  state=PASS
00:30:02.123  engine.t1_gate_closed  elapsed_s=902  limit_s=300    ← stuck on boot round

              … no window_mgr.round_transition ever logged …
              … no engine.new_round round_start_ts=1783729800 …

00:30:04.604  STATUS  window_end=00:45:00(895s)  UP=0.995  DOWN=0.005
00:30:09.606  STATUS  window_end=00:45:00(890s)  …
              … window_end advances every tick (it's just wm.next_round_start()),
                but engine._round and the gate are frozen on the 00:15:00 round.
00:31:29.638  STATUS  window_end=00:45:00(810s)  UP=0.9995  DOWN=0.0005
```

The bot started **1.0 s before the 00:30 boundary**. Boot step fired
`_on_new(1783728900)` correctly, but the boundary loop never fired
`_on_new(1783729800)`. Engine stayed on the 00:15 round forever; every
tick after that was inside `elapsed > limit_s=300` so T1 never placed
(the throttle `engine.t1_gate_closed` fired every 60 s, confirming the
evaluate loop was running, just stuck on the wrong round).

## Root cause

WindowManager had a **1-Hz inner polling loop** in `_run()` whose sole
job was to detect the boundary by comparing
`floor(time / 900) * 900 != _last_round_start`. The poll loop had no
top-level try/except so any exception inside its single body (the
`await self._on_new(...)` callback, a slow `discover_market` call, an
unsuspending `asyncio.sleep`) would propagate to the asyncio task and
silently end it.

When the bot starts **just before a boundary**:

1. boot step runs the inner poll at `time()≈T0` (very close to boundary)
2. inner poll reads `floor(T0/900)*900` and concludes `round_changed=False`
3. `asyncio.sleep(1.0)` — by the time sleep yields control back,
   `time() > T_boundary`, but the inner-while is on its own scheduling
4. ANY transient exception inside the next body (e.g. main.py's
   `_on_new_round` calling `await state.order_manager.cancel_market(prev)`
   which calls the CLOB and might raise on a transient 400) ends the
   task silently
5. asyncio swallows task exceptions; no log, no retry, no shutdown

The fix shipped as `8cbb158`, `6b0aaf5`, `742fe2d` ("H-V6-3") was a
collection of band-aids around the poll loop, never an actual
replacement of the loop with arithmetic.

## H-V6-7: Replace the poll loop with arithmetic

The round-start timestamp is **deterministic arithmetic**, not an event
to detect: `floor(unix / 900) * 900`. There is no need to poll.

Replaced the WindowManager._run() loop body with:

```python
# boot
self._last_round_start = self._round_start_of(time.time())
await self._on_round_start(self._last_round_start)

# steady-state
while self._running:
    next_boundary = self._last_round_start + self._round_period
    secs_to_boundary = next_boundary - time.time()
    sleep_for = max(0.0, secs_to_boundary + 0.5)   # 0.5s past boundary for safety
    await asyncio.sleep(sleep_for)
    if not self._running:
        return
    prev = self._last_round_start
    self._last_round_start = next_boundary
    log.info("window_mgr.round_transition", prev_start=prev, new_start=...)
    await self._on_round_start(self._last_round_start)
```

`_on_round_start(round_start_ts)` is the single funnel for boot + every
boundary. It does:

1. `tick(force_discovery=True)` — refresh market, get token IDs
2. `_on_new(round_start_ts)` — engine / strategy state seed

Both steps wrapped in `try / except`. Transient Gamma 5xx, network
timeout, or main.py callback exception → log + continue. The boundary
loop **never dies**.

Side fixes rolled into H-V6-7:

- `_round_start_of()` is now an instance method using
  `self._round_period` (was `@staticmethod` hardcoded 900). With
  `round_period_s=2` in tests, this matters.
- `start()` registers `add_done_callback(self._on_run_done)` so any
  future task death surfaces in the log instead of being swallowed by
  asyncio.

## Verification

- `test_h_v6_7_run_fires_on_new_at_each_boundary_deterministically`:
  runtime check that boot fires `_on_new(boot_round)`, then the next
  tick fires `_on_new(boot_round + period)` exactly.
- Updated `test_h_v6_3_boundary_loop_calls_on_new_after_successful_tick`
  and `test_h_v6_3_boot_discovery_fires_on_new` to check the new
  structure (no inner polling loop, deterministic advance, both
  tick() and _on_new() in single try/except in `_on_round_start`).
- 76/76 tests pass.
