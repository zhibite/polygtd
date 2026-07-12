"""Global configuration parsed from config.toml and .env."""
from __future__ import annotations

import tomllib
from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

_ROOT = Path(__file__).parent.parent
_TOML_PATH = _ROOT.parent / "config.toml"


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

    model_config = SettingsConfigDict(env_file=str(_ROOT.parent / ".env"), extra="ignore")

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
    # H-NEW-2026-07-11-4 (simplification): cap the strategy at 3 tiers
    # (T1 entry + 2 averaging layers = T1, T2, T3). The 4-tier cascade
    # was the single biggest source of incidents during the prod review —
    # 4 GTD lifecycles per round meant a 14-min market spent most of
    # its tail in transition logic instead of waiting on a real fill,
    # and any WS re-fire or cross-tier mis-trigger compounded across 4
    # open orders. The 3-tier cap trades max drawdown (still 14 USD)
    # for a 25% reduction in moving parts.
    #
    # Notionals follow a geometric 2x progression: 2 / 4 / 8 USD. Drop
    # thresholds are deliberately wider than the previous 2/2/4/8
    # ladder — T2 fires at -30%, T3 at -60% of the realised baseline
    # (previously 20% / 40% / 60%). Fewer tiers, deeper per-tier
    # averaging.
    TIER1_NOTIONAL: float = 2.0
    TIER2_NOTIONAL: float = 4.0
    TIER3_NOTIONAL: float = 8.0

    # Per-tier notional hard cap (multiplier over planned notional).
    # Anything above this is treated as the tier being "saturated" and the
    # engine will refuse to place another order for that tier.
    # Example: 1.25 means T3 may spend at most 4 * 1.25 = 5 USD before being
    # blocked. Prevents GTD loops / repeated partial fills from silently
    # over-spending into the next tier's budget.
    TIER_NOTIONAL_MAX_MULT: float = 1.1

    # ── Strategy thresholds ───────────────────────────────────────────────
    # H-NEW-2026-07-11-5 (tighter entry gate): tightened from 0.40 to
    # 0.36. The 0.40 cap was permissive enough that T1 entries routinely
    # filled at 0.38–0.40 against thin books at window-open, which
    # compressed the runway into T2's -30% trigger. With 0.36 the bot
    # refuses to enter at extreme ask prices — T1 only fires when the
    # mid is comfortably below the 0.40 historical ceiling.
    ENTRY_PRICE_CAP: float = 0.36      # max token price to enter
    TIER2_DROP_PCT: float = 0.30       # T2 triggers at baseline * (1 - 30%)
    TIER3_DROP_PCT: float = 0.60       # T3 triggers at baseline * (1 - 60%)
    # H-NEW-2026-07-11-4: TIER4_DROP_PCT removed. No tier 4 exists.
    TP_PROFIT_PCT: float = 0.20         # take profit target as fraction of avg cost
    # TP decreases by this amount per averaging tier fired. Final value is
    # clamped at TP_MIN_PCT to avoid a sub-cost lock-loss TP target.
    # H-NEW-2026-07-11-5 (aggressive decrement): bumped from 3% to 5%.
    # With the 3-tier cap the deepest depth is N=2 (T1+T2+T3 fired),
    # so the table is now:
    #   N=0 (T1 only)        → TP_PROFIT_PCT                  (20%)
    #   N=1 (T1+T2 fired)    → TP_PROFIT_PCT - 1*5%           (15%)
    #   N=2 (T1..T3 fired)   → TP_PROFIT_PCT - 2*5%           (10%)
    # The steeper decrement means the TP target walks down faster as
    # we average in, locking in a smaller-but-real profit sooner —
    # which is the right trade when a 3-tier cascade is already on
    # the table.
    TP_DECREMENT_PCT: float = 0.05
    TP_MIN_PCT: float = 0.05             # floor (5%) so TP can never lock in a loss
    BASELINE_DRIFT_LIMIT: float = 0.0015  # 0.15% BTC spot drift cap

    # T1 entry time gate: T1 must enter within this many seconds of window open.
    # Prevents chasing at the very end of the 15-min window.
    T1_ENTRY_WINDOW_S: float = 300.0    # 5 minutes

    # ── Order settings ────────────────────────────────────────────────────
    # GTD lifetime in seconds. Must be >= 180 (CLOB hard floor).
    GTD_LIFETIME_S: int = 180

    # Slippage for FAK sell orders (as a fraction, e.g. 0.02 = 2%)
    SELL_SLIPPAGE_PCT: float = 0.02

    # ── GTD pre-arm margin (核心新参数) ────────────────────────────────────
    # GTD 挂单**时机**提前量，按策略价的比例计算。
    #   BUY  : 触发线 = strategy_price * (1 + PRICE_PRE_MARGIN)
    #           例 PRICE_PRE_MARGIN=0.10, strategy_price=0.40 → mid<=0.44 时挂 0.40
    #   SELL : 触发线 = strategy_price * (1 - PRICE_PRE_MARGIN)
    #           例 PRICE_PRE_MARGIN=0.10, tp_target=0.40 → mid>=0.36 时挂 0.40
    # 设计目的：把 GTD 提前挂在订单簿上，等市价回到策略价成交。
    # 挂单价 = strategy_price（不变），价格提前量只用于触发判断。
    PRICE_PRE_MARGIN: float = 0.10

    # TierWatcher 轮询 CLOB token balance 的间隔（秒）
    # 主动查询持仓 share 数 → 取消未填满的 GTD → 让下一档继续推进。
    TIER_WATCHER_POLL_S: float = 1.5

    # TierWatcher 判定「此档基本够了」的比例阈值（占 size_expected）。
    # 触发条件：get_token_balance(token_id) >= size_expected * this_ratio
    # H-NEW-2026-07-12: 0.9 比 0.995 更合理。0.995 太严：薄市场 / 跨档流
    # 动下,残余 0.5% 经常在 GTD lifetime 内不会成交,watcher 会再多
    # poll ~1.5s 才退出。这段时间如果 WS TRADE 在丢报,position view
    # 会与实仓漂移(H-NEW-2026-07-12 的 continuous-recon 已经收窄但无法
    # 完全消除这个窗口)。90% 覆盖率就报 FILLED + 取消 GTD 才是干净
    # 的退出方式;残余量由 apply_tier_watcher_result 用 recorded
    # filled_qty 对 expected_size 自动 reconcile。
    TIER_WATCHER_FILL_RATIO: float = 0.9

    # ── Derived helpers ───────────────────────────────────────────────────

    # H-NEW-2026-07-11-4: max tier index exposed in the strategy. The
    # `tier_notionals` / `tier_drop_pcts` lists and the engine's
    # averaging-down loop all terminate at this index. Bumping the
    # strategy back to 4 tiers later is a single edit here + adding
    # the new factory + restoring `_next_tier_drop` to use this index.
    @property
    def max_tier(self) -> int:
        return 3

    @property
    def tier_notionals(self) -> list[float]:
        return [self.TIER1_NOTIONAL, self.TIER2_NOTIONAL,
                self.TIER3_NOTIONAL]

    @property
    def tier_drop_pcts(self) -> list[float | None]:
        return [None, self.TIER2_DROP_PCT, self.TIER3_DROP_PCT]

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
