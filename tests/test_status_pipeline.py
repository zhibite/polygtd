"""End-to-end STATUS pipeline test (no network).

Simulates the WS fill -> FillDeduper -> on_order_filled -> record_fill ->
TotalQty/Cost accumulation, then asks Position.tp_target() and computes
uPnL the same way STATUS does. Prints each value and the STATUS line.

We also throw in deliberate re-fires of the same cumulative size to prove
FillDeduper won't double-count.
"""
import asyncio
import sys

from src.config import strategy
from src.execution.clob_client import FillDeduper
from src.strategy.round_state import (
    Position,
    TierSnapshot,
    RoundState,
)


async def main() -> int:
    cfg = strategy()
    print(
        f"TP_PROFIT_PCT={cfg.TP_PROFIT_PCT}  "
        f"TP_DECREMENT_PCT={cfg.TP_DECREMENT_PCT}  "
        f"TP_MIN_PCT={cfg.TP_MIN_PCT}"
    )
    print(
        f"TIER_NOTIONAL={cfg.tier_notionals}  "
        f"TIER1_NOTIONAL={cfg.TIER1_NOTIONAL}"
    )

    # 1) Set up Position — bound to a server_id we will use.
    pos = Position()
    tier1 = pos.tiers[0]
    tier1.server_order_id = "0xORDER_T1"
    tier1.token_id = "0xTOKEN_DOWN"
    tier1.size_expected = 5.0
    tier1.notional = 2.0
    pos.entry_side = "DOWN"
    pos.entry_token = "0xTOKEN_DOWN"

    # 2) Build a FillDeduper and route a partial fill through it twice
    #    (replay) to prove delta is 0 on replay and totals don't double.
    deduper = FillDeduper()
    fire_log: list[tuple[float, float]] = []

    async def on_fill(token_id: str, order_id: str, fill_price: float,
                      fill_delta: float, fill_total: float) -> None:
        fire_log.append((fill_delta, fill_total))
        # This is exactly what main.py::_make_fill_callback does after
        # deduper has already accepted the observation:
        tier1.size_filled += fill_delta
        tier1.cost_filled += fill_price * fill_delta
        if tier1.size_filled > 1e-9:
            tier1.avg_fill_price = tier1.cost_filled / tier1.size_filled
        if not tier1.placed and fill_delta > 0:
            tier1.placed = True
        if tier1.is_complete:
            tier1.filled = True
        pos._total_qty += fill_delta
        pos._total_cost += fill_price * fill_delta
        if pos._total_qty > 0:
            pos._avg_cost = pos._total_cost / pos._total_qty
        if tier1.tier == 1 and tier1.size_filled > 0 and not pos.baseline_set:
            pos.baseline_price = tier1.avg_fill_price
            pos.baseline_set = True
        if tier1.tier == 1 and tier1.size_filled > 1e-9:
            new_r = tier1.cost_filled / tier1.size_filled
            pos.baseline_realized_price = new_r
            pos.baseline_set_realized = True

    async def simulate_observation(price: float, cumulative: float) -> None:
        delta, total = await deduper.submit(
            "0xTOKEN_DOWN", "0xORDER_T1", price, cumulative,
        )
        if delta <= 0:
            print(f"  DEDUP-SKIP: cum={cumulative} delta={delta}")
            return
        await on_fill("0xTOKEN_DOWN", "0xORDER_T1", price, delta, total)

    # First fill: 2 shares at 0.40
    await simulate_observation(0.40, 2.0)
    # Same fill arrives via WS path again — must be skipped
    await simulate_observation(0.40, 2.0)
    # Second fill: cumulative 5 at 0.42 (3 more shares)
    await simulate_observation(0.42, 5.0)
    # Replay second fill — must be skipped
    await simulate_observation(0.42, 5.0)

    print()
    print("=" * 60)
    print("FillDeduper fire_log (delta, total):")
    for f in fire_log:
        print(f"  {f}")
    print(f"Bookmark: {deduper.get_bookmark('0xORDER_T1')}")
    print()

    # 3) Apply a TP watcher result too — make sure dedup_key + avg_fill_price
    #    reaches apply_tier_watcher_result correctly.
    from src.execution.tier_watcher import TierOutcome
    pos.apply_tier_watcher_result(
        tier1, TierOutcome.FILLED.value, 5.0,
        dedup_key="0xORDER_T1|filled", avg_fill_price=0.412,
    )
    # Replay: dedup_key already seen → no-op
    pos.apply_tier_watcher_result(
        tier1, TierOutcome.FILLED.value, 5.0,
        dedup_key="0xORDER_T1|filled", avg_fill_price=0.412,
    )

    # 4) Now compute STATUS fields exactly like main.py does.
    held_mid = 0.44  # pretend DOWN token's midpoint right now
    if pos.avg_cost > 0 and pos.total_qty > 0 and held_mid > 0:
        unreal_pnl = (held_mid - pos.avg_cost) * pos.total_qty
        unreal_pnl_pct = (held_mid - pos.avg_cost) / pos.avg_cost
        sign = "+" if unreal_pnl > 0 else "-"
        upnl_str = f"${sign}{abs(unreal_pnl):.2f}({unreal_pnl_pct*100:+.1f}%)"
    else:
        upnl_str = "$0.00"
    tp = pos.tp_target()
    tp_str = f"tpTarget={tp:.4f}" if tp is not None else "tpTarget=None"

    print("=" * 60)
    print(f"position.total_qty    = {pos.total_qty:.4f}")
    print(f"position.total_cost   = ${pos.total_cost:.4f}")
    print(f"position.avg_cost     = {pos.avg_cost:.4f}")
    print(f"position.tp_target()  = {tp}")
    print(f"baseline_realized     = {pos.baseline_realized_price}")
    print(f"baseline_set_realized = {pos.baseline_set_realized}")
    print()
    print(
        f"STATUS  binance=...   side={pos.entry_side} "
        f"tiers=[T1={'OK' if tier1.is_complete else 'OPEN'}] "
        f"qty={pos.total_qty:.2f} avg={pos.avg_cost:.4f} "
        f"cost=${pos.total_cost:.2f} mid={held_mid:.4f} "
        f"uPnL={upnl_str} {tp_str}"
    )
    print("=" * 60)

    # Expected:
    #   qty = 5.0, cost = 2*0.4 + 3*0.42 = 0.8 + 1.26 = 2.06
    #   avg_cost = 2.06 / 5.0 = 0.412
    #   uPnL at mid=0.44 = (0.44-0.412)*5 = 0.14
    #   tp_target = 0.412 * (1+0.20) = 0.4944 (N=0)
    expected_qty = 5.0
    expected_cost = 2 * 0.40 + 3 * 0.42
    expected_avg = expected_cost / expected_qty
    expected_pnl = (held_mid - expected_avg) * expected_qty
    expected_tp = expected_avg * (1 + cfg.TP_PROFIT_PCT)
    print()
    print(f"EXPECTED qty   = {expected_qty:.4f}")
    print(f"EXPECTED cost  = ${expected_cost:.4f}")
    print(f"EXPECTED avg   = {expected_avg:.4f}")
    print(f"EXPECTED uPnL  = ${expected_pnl:+.4f}")
    print(f"EXPECTED tp    = {expected_tp:.4f}")
    print()

    ok = (
        abs(pos.total_qty - expected_qty) < 1e-9
        and abs(pos.total_cost - expected_cost) < 1e-9
        and abs(pos.avg_cost - expected_avg) < 1e-9
        and abs(tp - expected_tp) < 1e-9
    )
    if ok:
        print("ALL ASSERTIONS PASSED")
        return 0
    else:
        print("ASSERTION FAILED — pipeline is broken")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))