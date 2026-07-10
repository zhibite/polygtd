"""
models.py — Data models for Polymarket window management.
Only data structures, no business logic.
"""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Literal, Optional


class Side(Enum):
    UP = "UP"
    DOWN = "DOWN"

    def opposite(self) -> "Side":
        return Side.DOWN if self == Side.UP else Side.UP


@dataclass
class MarketInfo:
    """Polymarket fixed-interval window market info (e.g. 5m / 15m).

    This is the single canonical MarketInfo class. It replaces the old
    discovery.py version and includes all fields needed by both the
    market layer (window_manager, market_service) and the strategy
    layer (engine.py).
    """
    asset: str
    condition_id: str
    token_up_id: str
    token_down_id: str
    window_start: datetime
    window_end: datetime
    neg_risk: bool = False

    # ── Fields needed by strategy/engine.py ──────────────────────────────
    slug: str = ""                   # e.g. "btc-updown-15m-{ts}"
    tick_size: float = 0.01          # minimum price increment
    round_start_ts: int = 0          # Unix seconds (UTC), derived from window_start
    end_date: str = ""               # ISO string, for legacy compat

    # ── Property aliases (legacy compat: engine.py used these names) ──────
    @property
    def up_token(self) -> str:
        return self.token_up_id

    @property
    def down_token(self) -> str:
        return self.token_down_id

    def token_for_side(self, side: Literal["UP", "DOWN"]) -> str:
        return self.token_up_id if side == "UP" else self.token_down_id

    @staticmethod
    def slug_for_round(round_start_ts: int) -> str:
        return f"btc-updown-15m-{round_start_ts}"

    # ── Dynamic real-time prices (injected by strategy main loop) ──────
    _up_mid: float = 0.0
    _down_mid: float = 0.0

    # ── Window open Binary Option mid prices ──────────────────────────────
    window_open_up: float = 0.0
    window_open_down: float = 0.0

    # ── BTC spot prices (binance_spot.py) ────────────────────────────────
    btc_spot_at_window_open: float = 0.0
    btc_spot_now: float = 0.0


@dataclass
class OpenPosition:
    """Current position info."""
    asset: str
    condition_id: str
    side: Side
    token_id_bought: str
    entry_price: float
    effective_entry_price: float
    tokens_bought: float
    bet_usdc: float
    actual_cost_usdc: float
    taker_fee_bps: int
    min_exit_price: float
    opened_at: datetime
    window_start: datetime
    window_end: datetime
    sell_retry_count: int = 0
    _pending_close: bool = False
    _close_triggered: bool = False
    market: Optional[MarketInfo] = None

    tp_hit: dict | None = None

    martingale_anchor_price: float = 0.0
    martingale_fill_count: int = 0

    _pending_fill_ids: list = field(default_factory=list)
    _fill_ids_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _trade_recorded: bool = False
    _trade_recorded_reason: str = ""
    _last_pnl_pct: float = 0.0
    _last_pnl_usd: float = 0.0
