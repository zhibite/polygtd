"""Global configuration parsed from config.toml and .env."""
from __future__ import annotations

import tomllib
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root = parent of the `src` package. config.toml / .env live there.
_ROOT = Path(__file__).parent.parent
_TOML_PATH = _ROOT / "config.toml"


@lru_cache(maxsize=1)
def _toml_data() -> dict:
    """Load config.toml once per process. Returns {} if file missing."""
    if not _TOML_PATH.exists():
        return {}
    with _TOML_PATH.open("rb") as f:
        raw = tomllib.load(f)
    return {k.lower(): v for k, v in raw.items()}


def _toml_section(key: str) -> dict:
    """Return TOML section with env-var override semantics (uppercase keys)."""
    section = _toml_data().get(key.lower(), {})
    return {k.upper(): v for k, v in section.items()}


# ---------------------------------------------------------------------------
# Toml-driven "general" settings
# ---------------------------------------------------------------------------


class GeneralConfig(BaseSettings):
    ROUND_PERIOD_S: int = 900
    WINDOW_PRE_OPEN_S: int = 300

    def __init__(self, **kwargs):
        super().__init__(**{**_toml_section("GENERAL"), **kwargs})


class BinanceConfig(BaseSettings):
    WS_URL: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    RECONNECT_BASE_S: float = 1.0
    RECONNECT_MAX_S: float = 30.0
    BUFFER_RETENTION_S: int = 1200

    def __init__(self, **kwargs):
        super().__init__(**{**_toml_section("BINANCE"), **kwargs})


class BaselineConfig(BaseSettings):
    WINDOW_S: int = 300
    MIN_SAMPLES: int = 30

    def __init__(self, **kwargs):
        super().__init__(**{**_toml_section("BASELINE"), **kwargs})


class LogConfig(BaseSettings):
    LEVEL: str = "INFO"
    HEARTBEAT_S: int = 5

    def __init__(self, **kwargs):
        super().__init__(**{**_toml_section("LOG"), **kwargs})


# ---------------------------------------------------------------------------
# Env-driven strategy settings
# ---------------------------------------------------------------------------


class StrategySettings(BaseSettings):
    """
    All values come from .env (or .env file).

    IMPORTANT: DRY_RUN has been removed. Trading is always live.
    """

    model_config = SettingsConfigDict(env_file=str(_ROOT / ".env"), extra="ignore")

    # ── CLOB Credentials ──────────────────────────────────────────────────
    # Option A: provide all three L2 API credentials directly (recommended)
    API_KEY: str = ""
    API_SECRET: str = ""
    API_PASSPHRASE: str = ""

    # Option B: provide private key + wallet address (key is derived at startup)
    WALLET_PRIVATE_KEY: str = ""
    WALLET_ADDRESS: str = ""
    # Signature types:
    #   0 = EOA (standard wallet, gas paid by signer)
    #   1 = POLY_PROXY (Magic Link / email login)
    #   2 = GNOSIS_SAFE (manual Safe Transaction required for pUSD approval)
    SIGNATURE_TYPE: int = 0
    # H-V7-1 fix: deposit / funder address. Required by SIGNATURE_TYPE=1 / 2
    # so the order's `maker` field points at the smart-contract wallet that
    # actually holds the funds (not the EOA signer). Without this, every
    # CLOB /order post is rejected with
    #   "maker address not allowed, please use the deposit wallet flow".
    # EOA flow (SIGNATURE_TYPE=0) also benefits — setting this explicitly
    # makes the maker field deterministic regardless of which signer
    # derived the API key.
    FUNDER_ADDRESS: str = ""

    # ── CLOB Endpoints (usually no need to change) ───────────────────────
    GAMMA_API: str = "https://gamma-api.polymarket.com"
    POLYMARKET_HOST: str = "https://clob.polymarket.com"
    POLYGON_RPC_URL: str = "https://polygon-bor.publicnode.com"

    # ── Martingale tiers (USD notional per tier) ─────────────────────────
    # Four-layer budget: T1 entry followed by three averaging orders.
    # Maximum planned exposure per round is 2 + 3 + 5 + 6 = 16 USD.
    TIER1_NOTIONAL: float = 2.0
    TIER2_NOTIONAL: float = 3.0
    TIER3_NOTIONAL: float = 5.0
    TIER4_NOTIONAL: float = 6.0

    # ── Strategy thresholds ───────────────────────────────────────────────
    ENTRY_PRICE_CAP: float = 0.40      # max token price to enter
    TIER2_DROP_PCT: float = 0.20       # T2 triggers at baseline * (1 - 20%)
    TIER3_DROP_PCT: float = 0.40       # T3 triggers at baseline * (1 - 40%)
    TIER4_DROP_PCT: float = 0.60       # T4 triggers at baseline * (1 - 60%)
    TP_PROFIT_PCT: float = 0.20         # take profit target as fraction of avg cost
    # N is the number of completed averaging tiers (T2-T4):
    # 0 fills -> 20%, 1 -> 17%, 2 -> 14%, 3 -> 11%.
    TP_DECREMENT_PCT: float = 0.03
    TP_MIN_PCT: float = 0.05             # floor (5%) so TP can never lock in a loss
    BASELINE_DRIFT_LIMIT: float = 0.0015  # 0.15% BTC spot drift cap

    # T1 entry time gate: T1 must enter within this many seconds of window open.
    # Prevents chasing at the very end of the 15-min window.
    T1_ENTRY_WINDOW_S: float = 300.0    # 5 minutes

    # Boot-time window-age guard: if the process starts more than
    # ENTRY_OPEN_OFFSET_GRACE_SEC into a window, skip that window entirely.
    SKIP_IF_LATE_WINDOW_AT_BOOT: bool = True
    ENTRY_OPEN_OFFSET_GRACE_SEC: float = 180.0

    # Skip a new window when BTC spot jumped more than this percent between
    # the previous window's recorded price and the new window's open.
    # 0 disables the guard.
    CROSS_WINDOW_DRIFT_SKIP_PCT: float = 0.5

    # ── Order settings ────────────────────────────────────────────────────
    # GTD lifetime in seconds. CLOB enforces a 180-second hard floor
    # (`CLOBClient.GTD_MIN_SEC = 180`); values below that are clamped to
    # 181 by the CLOB client. The default 300 s gives the maker an extra
    # 2 minutes on the book after the pre-arm window opens — covering the
    # full 5-min averaging chain without forcing a re-post in the middle
    # of a fast cascade.
    GTD_LIFETIME_S: int = 300

    # Slippage for FAK sell orders (as a fraction, e.g. 0.02 = 2%)
    SELL_SLIPPAGE_PCT: float = 0.02

    # Startup reconcile: force-cancel every open CLOB order on the account.
    # This frees maker allowance left behind by a crashed process, but it is
    # account-wide — if the same wallet/API key is shared with another bot or
    # manual orders, those will be cancelled too. Set to False when sharing.
    STARTUP_RECONCILE_CANCEL_ALL: bool = True

    # Before POSTing a GTD, query the CLOB's open orders for the same
    # asset + side + price and refuse to place if one already exists.
    # This is the defence against ORPHAN orders: a POST whose response was
    # lost (timeout / connection reset) may have been accepted server-side,
    # so the local OrderManager does not know about it. Without this check
    # the next strategy tick would place a second GTD at the same tier.
    # The query failing is treated as "cannot verify" → refuse to place.
    VERIFY_NO_EXISTING_ORDER: bool = True

    # ── GTD pre-arm margin (核心新参数) ────────────────────────────────────
    # GTD 挂单**时机**提前量，按策略价的比例计算。
    #   BUY  : 触发线 = strategy_price * (1 + PRICE_PRE_MARGIN)
    #           例 PRICE_PRE_MARGIN=0.10, strategy_price=0.40 → mid<=0.44 时挂 0.40
    #   SELL : 触发线 = strategy_price * (1 - PRICE_PRE_MARGIN)
    #           例 PRICE_PRE_MARGIN=0.10, tp_target=0.40 → mid>=0.36 时挂 0.40
    # 设计目的：把 GTD 提前挂在订单簿上，等市价回到策略价成交。
    # 挂单价 = strategy_price（不变），价格提前量只用于触发判断。
    PRICE_PRE_MARGIN: float = 0.10

    # TierWatcher WS-event 兜底超时（秒）。
    # Watcher 主路径是等 `order.UPDATE` 推过来（~50ms 唤醒），但当 WS
    # 失联 / 漏事件时，必须有一个上限——超过这个时间就必须主动查 REST
    # balance，否则持仓更新会卡住。这不是"主动轮询间隔"——99% 的 tick
    # 是事件驱动，poll_s 只决定"如果 WS 不来，最长等多久"。生产默认
    # 1.5s 给 WS 充分时间重连；测试用 0.5s 是 TierWatcher.__init__ 里
    # `max(0.5, ...)` 的下限——再低也不会生效。
    TIER_WATCHER_POLL_S: float = 1.5

    # ── Derived helpers ───────────────────────────────────────────────────

    @property
    def max_tier(self) -> int:
        return 4

    @property
    def tier_notionals(self) -> list[float]:
        return [
            self.TIER1_NOTIONAL,
            self.TIER2_NOTIONAL,
            self.TIER3_NOTIONAL,
            self.TIER4_NOTIONAL,
        ]

    @property
    def tier_drop_pcts(self) -> list[float | None]:
        return [
            None,
            self.TIER2_DROP_PCT,
            self.TIER3_DROP_PCT,
            self.TIER4_DROP_PCT,
        ]

    @property
    def total_max_notional(self) -> float:
        """Maximum total USD exposure across all tiers."""
        return sum(self.tier_notionals)

    @property
    def has_credentials(self) -> bool:
        """True if L2 API credentials are available."""
        return bool(self.API_KEY and self.API_SECRET)


# ---------------------------------------------------------------------------
# Singleton access
# ---------------------------------------------------------------------------

_general: GeneralConfig | None = None
_binance: BinanceConfig | None = None
_baseline: BaselineConfig | None = None
_log: LogConfig | None = None
_strategy: StrategySettings | None = None


def general() -> GeneralConfig:
    global _general
    if _general is None:
        _general = GeneralConfig()
    return _general


def binance() -> BinanceConfig:
    global _binance
    if _binance is None:
        _binance = BinanceConfig()
    return _binance


def baseline_cfg() -> BaselineConfig:
    global _baseline
    if _baseline is None:
        _baseline = BaselineConfig()
    return _baseline


def log_config() -> LogConfig:
    global _log
    if _log is None:
        _log = LogConfig()
    return _log




def strategy() -> StrategySettings:
    global _strategy
    if _strategy is None:
        _strategy = StrategySettings()
    return _strategy
