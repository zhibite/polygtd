"""Runtime strategy-config overrides, editable from the dashboard.

The `/config` page writes a small JSON overlay to
``<data dir>/runtime_config.json``. At start-up ``apply_persisted_overrides()``
loads it and sets the fields on the `StrategySettings` singleton, so every
module that reads ``strategy().X`` at use time sees the new value.

Why a JSON file rather than ``.env``:

  * ``.env.docker`` is baked into the image — writing it would be lost on the
    next build and it is not on a persistent volume.
  * ``.env`` does not exist in the container.
  * ``/app/data`` is already the persisted volume (``polymarket.db``), so the
    overlay survives restarts and redeploys.

Only whitelisted, non-secret strategy parameters are accepted. Endpoints,
credentials and web-auth settings are deliberately absent — those need a
restart and must not be editable from the dashboard.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config import StrategySettings, strategy
from src.infra.logging import get_logger

log = get_logger(__name__)

_DATA_DIR = Path(
    os.getenv("POLYMARKET_DATA_DIR")
    or (Path(__file__).resolve().parent.parent.parent / "data")
)
CONFIG_PATH = Path(
    os.getenv("POLYGTD_RUNTIME_CONFIG") or (_DATA_DIR / "runtime_config.json")
)


# ---------------------------------------------------------------------------
# Parameter whitelist
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Param:
    key: str
    label: str
    group: str
    kind: str                  # "float" | "int" | "bool"
    minimum: float | None = None
    maximum: float | None = None
    step: float = 0.01
    scope: str = "immediate"   # "immediate" | "next_round"
    help: str = ""


PARAMS: tuple[Param, ...] = (
    # ── 入场 ────────────────────────────────────────────────────────────
    Param("ENTRY_PRICE_CAP", "入场价格上限", "入场", "float", 0.01, 0.99, 0.01,
          help="T1 挂单价上限；mid ≤ 上限×(1+预挂幅度) 时触发"),
    Param("PRICE_PRE_MARGIN", "预挂幅度", "入场", "float", 0.0, 1.0, 0.01,
          help="提前挂单的宽容度，例如 0.10 = mid 到上限的 110% 就挂"),
    Param("T1_ENTRY_WINDOW_S", "T1 入场时间窗 (秒)", "入场", "int", 0, 900, 30,
          help="窗口开启后多少秒内允许 T1；0 = 整轮都可入场"),

    # ── 加仓 ────────────────────────────────────────────────────────────
    Param("TIER1_NOTIONAL", "T1 名义金额 (USDC)", "加仓", "float", 0.01, 10000, 0.5,
          "next_round", "T1 用 taker 市价买入（1 USD 不足 5 股，不能挂 GTD）；"
                        "下一轮生效（本轮持仓的分层金额在本轮开始时已固定）"),
    Param("TIER2_NOTIONAL", "T2 名义金额 (USDC)", "加仓", "float", 0.01, 10000, 0.5,
          "next_round"),
    Param("TIER3_NOTIONAL", "T3 名义金额 (USDC)", "加仓", "float", 0.01, 10000, 0.5,
          "next_round"),
    Param("TIER4_NOTIONAL", "T4 名义金额 (USDC)", "加仓", "float", 0.01, 10000, 0.5,
          "next_round"),
    Param("TIER2_DROP_PCT", "T2 触发跌幅", "加仓", "float", 0.0, 0.99, 0.01,
          help="相对 T1 成交均价下跌多少触发 T2"),
    Param("TIER3_DROP_PCT", "T3 触发跌幅", "加仓", "float", 0.0, 0.99, 0.01,
          help="必须大于 T2 触发跌幅"),
    Param("TIER4_DROP_PCT", "T4 触发跌幅", "加仓", "float", 0.0, 0.99, 0.01,
          help="必须大于 T3 触发跌幅"),

    # ── 止盈 ────────────────────────────────────────────────────────────
    Param("TP_PROFIT_PCT", "止盈百分比", "止盈", "float", 0.01, 1.0, 0.01,
          help="相对持仓均价的止盈目标，例如 0.20 = +20%"),
    Param("TP_DECREMENT_PCT", "每层止盈递减", "止盈", "float", 0.0, 0.5, 0.01,
          help="每多成交一层加仓，止盈目标下调这么多"),
    Param("TP_MIN_PCT", "止盈下限", "止盈", "float", 0.0, 1.0, 0.01,
          help="止盈目标不会低于该值；不能高于止盈百分比"),

    # ── 风控 ────────────────────────────────────────────────────────────
    Param("PAUSE_NEW_ENTRIES", "暂停开仓/补仓", "风控", "bool",
          help="开启后不再开新仓/补仓，止盈与止损仍正常执行；回测期间可打开"),
    Param("BASELINE_DRIFT_LIMIT", "BTC 漂移上限", "风控", "float", 0.0, 0.1, 0.0005,
          help="相对窗口开盘价的绝对漂移，例如 0.0015 = 0.15%"),
    Param("CROSS_WINDOW_DRIFT_SKIP_PCT", "跨窗跳空跳过阈值 (%)", "风控", "float", 0.0, 10.0, 0.1,
          "next_round", "跨窗开盘价跳空超过该百分比则跳过新窗口；0 = 关闭"),
    Param("SKIP_IF_LATE_WINDOW_AT_BOOT", "晚启动跳过窗口", "风控", "bool",
          scope="restart",
          help="只在进程启动后的第一个窗口判定，修改后需重启才生效"),
    Param("ENTRY_OPEN_OFFSET_GRACE_SEC", "晚启动宽限期 (秒)", "风控", "int", 0, 900, 10,
          "next_round", "启动时窗口已开启超过该秒数则跳过"),
    Param("TP_REQUIRE_DRIFT_GATE", "止盈也受 BTC 漂移门限制", "风控", "bool",
          help="关闭时止盈只看价格触发线，不受 BTC 漂移影响（推荐关闭）"),
    Param("BTC_STALE_MAX_AGE_S", "BTC 行情最大延迟 (秒)", "风控", "float", 0.0, 120.0, 1.0,
          help="Binance WS 超过该秒数没有新成交时，暂停新开仓/补仓（止盈不受影响）；0 = 关闭"),
    Param("BTC_VOL_WINDOW_S", "急涨急跌检测窗口 (秒)", "风控", "float", 10.0, 300.0, 5.0,
          help="在该时间窗口内计算 BTC 的最大振幅"),
    Param("BTC_VOL_MAX_MOVE_PCT", "急涨急跌阈值 (%)", "风控", "float", 0.0, 5.0, 0.05,
          help="窗口内振幅超过该百分比视为急涨急跌，暂停开仓/补仓并撤销挂单；0 = 关闭"),
    Param("BTC_VOL_COOLDOWN_S", "急涨急跌冷却 (秒)", "风控", "float", 0.0, 300.0, 5.0,
          help="触发后至少暂停这么久，波动平息后才恢复"),
    Param("T4_DRIFT_BYPASS_MIN_REMAINING_S", "T4 漂移豁免剩余时间 (秒)", "风控", "float",
          0.0, 900.0, 30.0,
          help="窗口剩余时间 ≥ 该值时，T4 可在 BTC 漂移 FAIL 下补仓；0 = 关闭"),
    Param("T4_MIN_REMAINING_S", "T4 最短剩余时间 (秒)", "风控", "float",
          0.0, 900.0, 30.0,
          help="剩余时间少于此值时不再补 T4（300s GTD 与止盈都来不及走完）；0 = 关闭"),
    Param("T4_STRIKE_GATE_ENABLED", "T4 行权价距离闸门", "风控", "bool",
          help="只允许在 BTC 仍接近窗口开盘价（z 分数不超限）时补 T4；"
               "避免在明确趋势中越补越深"),
    Param("T4_MAX_STRIKE_Z", "T4 行权价距离 z 上限", "风控", "float",
          0.0, 5.0, 0.25,
          help="z = 不利方向距离 / (1 分钟波动 × √剩余分钟)；越小越保守，"
               "0 = 只在 BTC 位于有利一侧时补"),
    Param("T4_STRIKE_SIGMA_PCT_PER_MIN", "T4 波动率下限 (%/分钟)", "风控", "float",
          0.01, 1.0, 0.01,
          help="60 秒振幅样本不足时的 BTC 1 分钟波动率；实盘用 60s 振幅/2"),
    Param("T4_MAX_GAP_BELOW_TRIGGER_PCT", "T4 允许低于触发价幅度", "风控", "float",
          0.0, 0.5, 0.01,
          help="mid 已跌到触发价下方超过该比例时不挂 T4，等价格回到预挂区；"
               "0 = 关闭（旧行为）"),
    Param("STOP_LOSS_ENABLED", "T4 后启用止损", "风控", "bool",
          help="T4 成交后，价格继续下跌时按两段式规则自动减仓/清仓"),
    Param("STOP_LOSS_T4_DROP_PCT", "止损触发跌幅 (相对 T4 成交价)", "风控", "float",
          0.0, 0.9, 0.05,
          help="0.45 = T4 成交价再跌 45% 触发第一段止损"),
    Param("STOP_LOSS_CONFIRM_S", "止损确认时间 (秒)", "风控", "float",
          0.0, 300.0, 5.0,
          help="价格需持续低于触发线这么久才卖出，防止单笔坏价/薄盘口插针"),
    Param("STOP_LOSS_STAGE1_SELL_PCT", "第一段止损卖出比例", "风控", "float",
          0.0, 1.0, 0.05,
          help="默认 60%；剩余仓位由第二段/时间止损处理"),
    Param("STOP_LOSS_STAGE2_EXTRA_DROP_PCT", "第二段追加跌幅", "风控", "float",
          0.0, 0.9, 0.05,
          help="在第一段触发线基础上再跌这么多则清仓"),
    Param("STOP_LOSS_TIME_EXIT_S", "时间止损剩余 (秒)", "风控", "float",
          0.0, 900.0, 30.0,
          help="剩余时间 ≤ 该值且 mid ≤ 地板价时清仓剩余"),
    Param("STOP_LOSS_FLOOR", "时间止损地板价", "风控", "float",
          0.0, 1.0, 0.01,
          help="时间止损生效所需的 mid 上限"),
    Param("STOP_LOSS_FINAL_EXIT_S", "尾盘清仓剩余 (秒)", "风控", "float",
          0.0, 300.0, 15.0,
          help="最后这么多秒内，若 mid < 尾盘清仓上限则清仓，避免归零"),
    Param("STOP_LOSS_FINAL_MID", "尾盘清仓 mid 上限", "风控", "float",
          0.0, 1.0, 0.05,
          help="尾盘清仓的 mid 阈值；默认 0.50（只清掉亏损/五五开的仓位）"),

    # ── 订单 ────────────────────────────────────────────────────────────
    Param("GTD_LIFETIME_S", "GTD 有效期 (秒)", "订单", "int", 181, 3600, 10,
          help="CLOB 硬下限 180 秒，实际会钳制到 ≥181"),
    Param("TIER_WATCHER_POLL_S", "TierWatcher 轮询间隔 (秒)", "订单", "float", 0.5, 30.0, 0.5,
          help="WS 漏事件时的 REST 兜底轮询"),
    Param("SELL_SLIPPAGE_PCT", "卖出滑点", "订单", "float", 0.0, 0.5, 0.01,
          help="仅 FAK 卖出路径使用"),
    Param("TAKER_FEE_RATE", "Taker 手续费率", "订单", "float", 0.0, 0.2, 0.005,
          help="官方公式 fee = C×feeRate×p×(1-p)，仅 taker 收取（Crypto=0.07）；"
               "maker 免费。0 = 关闭手续费统计"),
    Param("CANCEL_HYSTERESIS_PCT", "撤单滞回幅度", "订单", "float", 0.0, 0.2, 0.005,
          help="撤单线比挂单线多走的比例，避免 mid 在阈值附近抖动时反复挂撤；"
               "0 = 关闭（旧的即时撤单行为）"),
)

SPEC: dict[str, Param] = {p.key: p for p in PARAMS}
GROUP_ORDER: tuple[str, ...] = ("入场", "加仓", "止盈", "风控", "订单")


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _coerce(p: Param, raw: Any) -> Any:
    if p.kind == "bool":
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)) and raw in (0, 1):
            return bool(raw)
        if isinstance(raw, str):
            s = raw.strip().lower()
            if s in ("1", "true", "yes", "on"):
                return True
            if s in ("0", "false", "no", "off", ""):
                return False
        raise ValueError(f"{p.key} 需要布尔值")

    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{p.key} 需要数字") from None
    if not math.isfinite(value):
        raise ValueError(f"{p.key} 需要有限数字")
    if p.kind == "int":
        if abs(value - round(value)) > 1e-9:
            raise ValueError(f"{p.key} 需要整数")
        value = int(round(value))
    if p.minimum is not None and value < p.minimum:
        raise ValueError(f"{p.key} 不能小于 {p.minimum:g}")
    if p.maximum is not None and value > p.maximum:
        raise ValueError(f"{p.key} 不能大于 {p.maximum:g}")
    return value


def _validate_merged(values: dict[str, Any]) -> None:
    """Cross-field rules that a single-field range check cannot express."""
    if values["TP_MIN_PCT"] > values["TP_PROFIT_PCT"] + 1e-12:
        raise ValueError("TP_MIN_PCT 不能高于 TP_PROFIT_PCT")
    if not (values["TIER2_DROP_PCT"] < values["TIER3_DROP_PCT"] < values["TIER4_DROP_PCT"]):
        raise ValueError("加仓跌幅必须满足 T2 < T3 < T4")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load_overrides() -> dict[str, Any]:
    """Return the persisted override dict (validated, unknown keys dropped)."""
    if not CONFIG_PATH.exists():
        return {}
    try:
        raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        log.warning("config.overrides_unreadable", path=str(CONFIG_PATH), error=str(exc))
        return {}
    if not isinstance(raw, dict):
        return {}

    out: dict[str, Any] = {}
    for key, value in raw.items():
        param = SPEC.get(key)
        if param is None:
            continue
        try:
            out[key] = _coerce(param, value)
        except ValueError as exc:
            log.warning("config.override_invalid", key=key, error=str(exc))
    return out


def _write_overrides(overrides: dict[str, Any]) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(overrides, indent=2, sort_keys=True)
    # Atomic replace: a crash mid-write must not leave a truncated file that
    # would silently reset every overridden parameter on the next boot.
    fd, tmp_name = tempfile.mkstemp(
        dir=str(CONFIG_PATH.parent), prefix=".runtime_config.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp_name, CONFIG_PATH)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def _apply(values: dict[str, Any]) -> None:
    cfg = strategy()
    for key, value in values.items():
        setattr(cfg, key, value)


def current_values() -> dict[str, Any]:
    cfg = strategy()
    return {key: getattr(cfg, key) for key in SPEC}


def apply_persisted_overrides() -> int:
    """Load the JSON overlay and apply it to the live config singleton.

    Called once from `main()` before any strategy component reads config.
    Returns the number of applied overrides.
    """
    overrides = load_overrides()
    if not overrides:
        return 0
    _apply(overrides)
    log.info(
        "config.runtime_overrides_applied",
        count=len(overrides),
        path=str(CONFIG_PATH),
        keys=sorted(overrides),
    )
    return len(overrides)


def update(patch: dict[str, Any]) -> dict[str, Any]:
    """Validate and apply a partial patch, then persist it.

    Only the keys present in ``patch`` are written — untouched parameters keep
    following ``.env`` / process-env / defaults, so a future env change still
    takes effect for them.
    """
    if not isinstance(patch, dict) or not patch:
        raise ValueError("请求体必须是非空 JSON 对象")

    unknown = sorted(set(patch) - set(SPEC))
    if unknown:
        raise ValueError(f"不允许的配置项: {', '.join(unknown)}")

    coerced: dict[str, Any] = {}
    for key, raw in patch.items():
        coerced[key] = _coerce(SPEC[key], raw)

    merged_overrides = {**load_overrides(), **coerced}

    # Validate against the effective values (env defaults + overrides).
    effective = current_values()
    effective.update(coerced)
    _validate_merged(effective)

    _write_overrides(merged_overrides)
    _apply(coerced)
    log.info(
        "config.updated",
        keys=sorted(coerced),
        values=coerced,
        overrides=len(merged_overrides),
        path=str(CONFIG_PATH),
    )
    return current_values()


def reset() -> dict[str, Any]:
    """Drop the overlay and restore values from env / .env.docker / defaults."""
    try:
        if CONFIG_PATH.exists():
            CONFIG_PATH.unlink()
    except OSError as exc:
        log.warning("config.reset_unlink_failed", error=str(exc))
    fresh = StrategySettings()
    _apply({key: getattr(fresh, key) for key in SPEC})
    log.info("config.reset", path=str(CONFIG_PATH))
    return current_values()


def spec_payload() -> dict[str, Any]:
    """Metadata for the UI (groups, ranges, scopes)."""
    return {
        "groups": list(GROUP_ORDER),
        "params": [
            {
                "key": p.key,
                "label": p.label,
                "group": p.group,
                "kind": p.kind,
                "min": p.minimum,
                "max": p.maximum,
                "step": p.step,
                "scope": p.scope,
                "help": p.help,
            }
            for p in PARAMS
        ],
        "config_path": str(CONFIG_PATH),
    }
