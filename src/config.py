"""Global configuration parsed from config.toml and .env."""
from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Toml-driven "general" settings
# ---------------------------------------------------------------------------


class GeneralConfig(BaseSettings):
    model_config = SettingsConfigDict(toml_file=str(_ROOT.parent / "config.toml"), extra="ignore")

    ROUND_PERIOD_S: int = 900
    WINDOW_PRE_OPEN_S: int = 300


class BinanceConfig(BaseSettings):
    model_config = SettingsConfigDict(toml_file=str(_ROOT.parent / "config.toml"), extra="ignore")

    WS_URL: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    RECONNECT_BASE_S: float = 1.0
    RECONNECT_MAX_S: float = 30.0
    BUFFER_RETENTION_S: int = 1200


class RoundClockConfig(BaseSettings):
    model_config = SettingsConfigDict(toml_file=str(_ROOT.parent / "config.toml"), extra="ignore")

    TIMEZONE: Literal["local", "utc"] = "local"


class BaselineConfig(BaseSettings):
    model_config = SettingsConfigDict(toml_file=str(_ROOT.parent / "config.toml"), extra="ignore")

    WINDOW_S: int = 300
    MIN_SAMPLES: int = 30


class LogConfig(BaseSettings):
    model_config = SettingsConfigDict(toml_file=str(_ROOT.parent / "config.toml"), extra="ignore")

    LEVEL: str = "INFO"
    HEARTBEAT_S: int = 5


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

    # ── CLOB Endpoints (usually no need to change) ───────────────────────
    GAMMA_API: str = "https://gamma-api.polymarket.com"
    POLYMARKET_HOST: str = "https://clob.polymarket.com"
    POLYGON_RPC_URL: str = "https://polygon-bor.publicnode.com"

    # ── Martingale tiers (USD notional per tier) ─────────────────────────
    TIER1_NOTIONAL: float = 2.0
    TIER2_NOTIONAL: float = 2.0
    TIER3_NOTIONAL: float = 4.0
    TIER4_NOTIONAL: float = 8.0

    # ── Strategy thresholds ───────────────────────────────────────────────
    ENTRY_PRICE_CAP: float = 0.40      # max token price to enter
    TIER2_DROP_PCT: float = 0.20       # T2 triggers at baseline * (1 - 20%)
    TIER3_DROP_PCT: float = 0.40
    TIER4_DROP_PCT: float = 0.60
    TP_PROFIT_PCT: float = 0.20         # take profit target as fraction of avg cost
    BASELINE_DRIFT_LIMIT: float = 0.0015  # 0.15% BTC spot drift cap

    # T1 entry time gate: T1 must enter within this many seconds of window open.
    # Prevents chasing at the very end of the 15-min window.
    T1_ENTRY_WINDOW_S: float = 300.0    # 5 minutes

    # ── Order settings ────────────────────────────────────────────────────
    # GTD lifetime in seconds. Must be >= 180 (CLOB hard floor).
    GTD_LIFETIME_S: int = 180

    # Slippage for FAK sell orders (as a fraction, e.g. 0.02 = 2%)
    SELL_SLIPPAGE_PCT: float = 0.02

    # ── Derived helpers ───────────────────────────────────────────────────

    @property
    def tier_notionals(self) -> list[float]:
        return [self.TIER1_NOTIONAL, self.TIER2_NOTIONAL,
                self.TIER3_NOTIONAL, self.TIER4_NOTIONAL]

    @property
    def tier_drop_pcts(self) -> list[float | None]:
        return [None, self.TIER2_DROP_PCT, self.TIER3_DROP_PCT, self.TIER4_DROP_PCT]

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
_round_clock: RoundClockConfig | None = None
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


def round_clock() -> RoundClockConfig:
    global _round_clock
    if _round_clock is None:
        _round_clock = RoundClockConfig()
    return _round_clock


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
