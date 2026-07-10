"""
window_context.py — Window lifecycle state.

Each asset maintains one WindowContext instance, managing all state for that
asset's current window. When window switches (condition_id changes), a new
instance is allocated by WindowManager and the old one is discarded.

Responsibilities:
- Window metadata (condition_id, window_start/end)
- Window-level trading records (traded_sides)
- Martin take-profit behavior (hot_restart_pending, hot_restart_side, hot_exit_fills)
- Position reference (position)

Does NOT involve:
- Strategy execution logic (entry/exit/add)
- Price fetching
- Interaction with Polymarket API
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from src.market.models import MarketInfo, OpenPosition, Side


@dataclass
class WindowContext:
    """
    Lifecycle state container for one Polymarket window.

    When condition_id changes (new window starts), WindowManager allocates
    a new instance. Old instance is discarded, state reset to zero.
    """

    asset: str
    condition_id: str
    window_start: datetime
    window_end: datetime

    traded_sides: set[Side] = field(default_factory=set)
    hot_restart_pending: bool = False
    hot_restart_side: Optional[Side] = None
    hot_exit_fills: int = 0
    original_side: Optional[Side] = None
    profitable_locked_sides: set[Side] = field(default_factory=set)
    no_new_order_locked: bool = False
    market: Optional[MarketInfo] = None
    position: Optional[OpenPosition] = None
    skip_until_next_window: bool = False
    cold_entry_abandoned: bool = False
    prev_window_btc_close: Optional[float] = None

    @property
    def has_position(self) -> bool:
        return self.position is not None

    @property
    def is_traded(self) -> bool:
        """Whether position was opened in this window (any side)."""
        return len(self.traded_sides) > 0

    def mark_traded(self, side: Side) -> None:
        self.traded_sides.add(side)

    def on_take_profit(self, side: Side) -> None:
        """Called after take-profit: activate hot-restart."""
        self.hot_restart_pending = True
        self.hot_restart_side = side
        if self.original_side is None:
            self.original_side = side

    def lock_side_after_take_profit(self, side: Side) -> None:
        """Lock this side after take-profit (no new entries in same window)."""
        self.profitable_locked_sides.add(side)

    def reset_hot_restart(self) -> None:
        self.hot_restart_pending = False
        self.hot_restart_side = None

    def reset_for_new_window(self) -> None:
        """Reset all window-level state on new window (position handled separately)."""
        self.traded_sides.clear()
        self.hot_restart_pending = False
        self.hot_restart_side = None
        self.hot_exit_fills = 0
        self.original_side = None
        self.profitable_locked_sides.clear()
        self.no_new_order_locked = False
        self.skip_until_next_window = False
        self.cold_entry_abandoned = False

    def elapsed(self, now: Optional[datetime] = None) -> float:
        if now is None:
            now = datetime.now(timezone.utc)
        return (now - self.window_start).total_seconds()

    def is_late(self, cutoff_sec: float) -> bool:
        return self.elapsed() >= cutoff_sec - 1e-9
