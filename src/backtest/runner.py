"""In-process backtest job manager.

The web dashboard starts a job; a daemon thread collects missing windows
(future.news + Polymarket data API + Binance), replays the current strategy
and a handful of parameter variants, and writes a JSON report under the
persistent data dir. The bot's asyncio loop is never blocked.
"""
from __future__ import annotations

import dataclasses
import datetime as _dt
import json
import os
import statistics
import threading
import time
from pathlib import Path
from typing import Any

from src.config import backtest as backtest_cfg
from src.config import strategy as strategy_cfg
from src.infra.logging import get_logger

from . import store
from .model import Params, RoundResult, replay

log = get_logger(__name__)

_DATA_DIR = Path(
    os.getenv("POLYMARKET_DATA_DIR")
    or (Path(__file__).resolve().parents[2] / "data")
)

# The A/B variants shown in every report. Keep this list small: each
# variant is a full replay of the same windows (fast, but the report
# should stay readable).
VARIANTS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("baseline", {}),
    ("t4_off", {"tier_notionals": (1.0, 2.0, 4.0, 0.0)}),
    ("t4_2u", {"tier_notionals": (1.0, 2.0, 4.0, 2.0)}),
    ("stop_30", {"stop_loss_t4_drop_pct": 0.30}),
    ("z_1", {"t4_max_strike_z": 1.0}),
)


def _f(settings: Any, name: str, default: float) -> float:
    value = getattr(settings, name, default)
    return float(value) if isinstance(value, (int, float)) else float(default)


def live_params() -> Params:
    """Map the deployed StrategySettings onto the replay Params."""
    s = strategy_cfg()
    tier_notionals = tuple(
        float(x) for x in (
            s.TIER1_NOTIONAL, s.TIER2_NOTIONAL,
            s.TIER3_NOTIONAL, s.TIER4_NOTIONAL,
        )
    )
    return dataclasses.replace(
        Params(),
        entry_price_cap=_f(s, "ENTRY_PRICE_CAP", 0.40),
        price_pre_margin=_f(s, "PRICE_PRE_MARGIN", 0.10),
        tier_notionals=tier_notionals,
        tier_drop_pcts=(
            0.0, _f(s, "TIER2_DROP_PCT", 0.20),
            _f(s, "TIER3_DROP_PCT", 0.40), _f(s, "TIER4_DROP_PCT", 0.60),
        ),
        tp_profit_pct=_f(s, "TP_PROFIT_PCT", 0.20),
        tp_decrement_pct=_f(s, "TP_DECREMENT_PCT", 0.03),
        tp_min_pct=_f(s, "TP_MIN_PCT", 0.05),
        t1_entry_window_s=_f(s, "T1_ENTRY_WINDOW_S", 300.0),
        baseline_drift_limit=_f(s, "BASELINE_DRIFT_LIMIT", 0.0015),
        t4_min_remaining_s=_f(s, "T4_MIN_REMAINING_S", 240.0),
        t4_strike_gate_enabled=bool(
            getattr(s, "T4_STRIKE_GATE_ENABLED", True)
        ),
        t4_max_strike_z=_f(s, "T4_MAX_STRIKE_Z", 2.0),
        t4_strike_sigma_pct_per_min=_f(s, "T4_STRIKE_SIGMA_PCT_PER_MIN", 0.05),
        t4_max_gap_below_trigger_pct=_f(s, "T4_MAX_GAP_BELOW_TRIGGER_PCT", 0.05),
        t4_drift_bypass_min_remaining_s=_f(
            s, "T4_DRIFT_BYPASS_MIN_REMAINING_S", 540.0
        ),
        stop_loss_enabled=bool(getattr(s, "STOP_LOSS_ENABLED", True)),
        stop_loss_t4_drop_pct=_f(s, "STOP_LOSS_T4_DROP_PCT", 0.45),
        stop_loss_confirm_s=_f(s, "STOP_LOSS_CONFIRM_S", 20.0),
        stop_loss_stage1_sell_pct=_f(s, "STOP_LOSS_STAGE1_SELL_PCT", 0.60),
        stop_loss_stage2_extra_drop_pct=_f(
            s, "STOP_LOSS_STAGE2_EXTRA_DROP_PCT", 0.30
        ),
        stop_loss_time_exit_s=_f(s, "STOP_LOSS_TIME_EXIT_S", 120.0),
        stop_loss_floor=_f(s, "STOP_LOSS_FLOOR", 0.05),
        stop_loss_final_exit_s=_f(s, "STOP_LOSS_FINAL_EXIT_S", 60.0),
        stop_loss_final_mid=_f(s, "STOP_LOSS_FINAL_MID", 0.50),
        taker_fee_rate=_f(s, "TAKER_FEE_RATE", 0.07),
        btc_vol_max_move_pct=_f(s, "BTC_VOL_MAX_MOVE_PCT", 0.3),
    )


def _block(rs: list[RoundResult]) -> dict[str, Any]:
    if not rs:
        return {"n": 0}
    return {
        "n": len(rs),
        "pnl": round(sum(r.pnl for r in rs), 2),
        "avg": round(statistics.mean(r.pnl for r in rs), 3),
        "win_rate": round(sum(1 for r in rs if r.pnl > 0) / len(rs), 3),
    }


def summarize(results: list[RoundResult], params: Params, days: int) -> dict:
    entries = [r for r in results if r.exit != "NO_ENTRY"]
    wins = [r for r in entries if r.pnl > 1e-9]
    losses = [r for r in entries if r.pnl < -1e-9]
    gross_win = sum(r.pnl for r in wins)
    gross_loss = -sum(r.pnl for r in losses)

    def _tier(r: RoundResult, n: int) -> bool:
        return any(f.tier == f"T{n}" and f.shares > 0 for f in r.fills)

    t1_only = [r for r in entries if not _tier(r, 2)]
    t2_hi = [r for r in entries if _tier(r, 2) and not _tier(r, 3)]
    t3_hi = [r for r in entries if _tier(r, 3) and not _tier(r, 4)]
    t4_hi = [r for r in entries if _tier(r, 4)]

    cum = peak = max_dd = 0.0
    for r in sorted(results, key=lambda x: x.start_ts):
        cum += r.pnl
        peak = max(peak, cum)
        max_dd = min(max_dd, cum - peak)

    exits: dict[str, int] = {}
    for r in entries:
        exits[r.exit] = exits.get(r.exit, 0) + 1

    return {
        "days": days,
        "params": dataclasses.asdict(params),
        "windows_replayed": len(results),
        "no_entry": len(results) - len(entries),
        "entries": len(entries),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(entries), 4) if entries else None,
        "pnl_total": round(sum(r.pnl for r in results), 2),
        "pnl_avg_per_window": (
            round(sum(r.pnl for r in results) / len(results), 3) if results else None
        ),
        "pnl_avg_per_entry": (
            round(sum(r.pnl for r in entries) / len(entries), 3) if entries else None
        ),
        "gross_win": round(gross_win, 2),
        "gross_loss": round(gross_loss, 2),
        "profit_factor": round(gross_win / gross_loss, 2) if gross_loss else None,
        "max_drawdown": round(max_dd, 2),
        "exits": exits,
        "by_highest_tier": {
            "T1": _block(t1_only), "T2": _block(t2_hi),
            "T3": _block(t3_hi), "T4": _block(t4_hi),
        },
        "t4_rounds": len(t4_hi),
        "t4_pnl": round(sum(r.pnl for r in t4_hi), 2),
    }


def build_conclusion(days: int, baseline: dict, variants: list[dict]) -> str:
    b = baseline
    if not b.get("entries"):
        return "样本期内没有可回放的入场，暂时无法给出结论。"
    parts = [
        f"样本 {b['windows_replayed']} 轮（{days} 天），"
        f"入场 {b['entries']} 次，胜率 {b['win_rate']:.1%}，"
        f"总盈亏 ${b['pnl_total']:+.2f}，"
        f"盈亏比 {b['profit_factor']}，最大回撤 ${b['max_drawdown']:+.2f}。"
    ]
    t4 = b["by_highest_tier"].get("T4") or {}
    if t4.get("n"):
        parts.append(
            f"T4 触发 {t4['n']} 轮，合计 ${t4['pnl']:+.2f}（胜率 {t4['win_rate']:.1%}），"
            f"是当前配置的主要{'亏损' if t4['pnl'] < 0 else '利润'}来源。"
        )
    if len(variants) > 1:
        ranked = sorted(
            variants, key=lambda v: v["summary"].get("pnl_total") or -1e9,
            reverse=True,
        )
        best = ranked[0]
        if best["name"] != "baseline":
            parts.append(
                f"参数对比中最好的是 {best['name']}："
                f"${best['summary']['pnl_total']:+.2f}。"
            )
    if b["pnl_total"] < 0:
        parts.append(
            "当前配置在样本期内为负期望；不要直接提高仓位，先用回测里的对比配置"
            "继续验证，并注意回测的成交/延迟假设偏乐观。"
        )
    else:
        parts.append("样本期内为正期望，但样本有限，仍需更长周期验证后再考虑放大。")
    return " ".join(parts)


class BacktestManager:
    """Single-flight background job + report cache."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._state: dict[str, Any] = {
            "phase": "idle", "message": "", "days": 0,
            "done": 0, "total": 0, "ok": 0, "skipped": 0, "failed": 0,
            "requests": 0, "started_at": None, "finished_at": None,
            "error": None,
        }
        self._report: dict[str, Any] | None = None

    # ---------------------------------------------------------------- paths
    def db_path(self) -> Path:
        cfg = backtest_cfg()
        explicit = (cfg.BACKTEST_DB_PATH or "").strip()
        return Path(explicit) if explicit else _DATA_DIR / "backtest.db"

    def report_path(self) -> Path:
        cfg = backtest_cfg()
        explicit = (cfg.BACKTEST_REPORT_PATH or "").strip()
        return Path(explicit) if explicit else _DATA_DIR / "backtest_report.json"

    # ---------------------------------------------------------------- status
    def status(self) -> dict[str, Any]:
        with self._lock:
            state = dict(self._state)
        state["coverage"] = self.coverage()
        state["running"] = bool(self._thread and self._thread.is_alive())
        return state

    def coverage(self) -> dict[str, Any]:
        try:
            return store.coverage(self.db_path())
        except Exception as exc:  # noqa: BLE001
            return {"windows": 0, "error": str(exc)}

    def latest_report(self) -> dict[str, Any] | None:
        with self._lock:
            if self._report is not None:
                return self._report
        try:
            path = self.report_path()
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            log.warning("backtest.report_read_failed", error=str(exc))
        return None

    # ---------------------------------------------------------------- run
    def start(self, days: int, refresh: bool = False,
              variants: bool = True) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            cfg = backtest_cfg()
            days = max(1, min(int(days), int(cfg.BACKTEST_MAX_DAYS)))
            self._state = {
                "phase": "starting", "message": "starting", "days": days,
                "done": 0, "total": 0, "ok": 0, "skipped": 0, "failed": 0,
                "requests": 0,
                "started_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                "finished_at": None, "error": None,
            }
            self._thread = threading.Thread(
                target=self._run, args=(days, refresh, variants),
                name="backtest-job", daemon=True,
            )
            self._thread.start()
            return True

    def _set(self, **fields: Any) -> None:
        with self._lock:
            self._state.update(fields)

    def _progress(self, info: dict[str, Any]) -> None:
        with self._lock:
            self._state.update({
                "phase": info.get("phase", self._state["phase"]),
                "done": info.get("done", self._state["done"]),
                "total": info.get("total", self._state["total"]),
                "ok": info.get("ok", self._state["ok"]),
                "skipped": info.get("skipped", self._state["skipped"]),
                "failed": info.get("failed", self._state["failed"]),
                "requests": info.get("requests", self._state["requests"]),
                "message": info.get("message", self._state["message"]),
            })

    def _run(self, days: int, refresh: bool, variants: bool) -> None:
        t0 = time.time()
        try:
            cfg = backtest_cfg()
            self._set(phase="collecting", message="collecting history")
            db = self.db_path()
            store.collect(
                db, days, refresh=refresh,
                rate_limit_s=float(cfg.BACKTEST_RATE_LIMIT_S),
                progress=self._progress,
            )
            self._set(phase="replaying", message="replaying windows")

            end_ts = store.align_start()
            start_ts = end_ts - int(days) * 86_400
            base = live_params()
            variant_params = []
            for name, overrides in VARIANTS if variants else VARIANTS[:1]:
                variant_params.append(
                    (name, dataclasses.replace(base, **overrides))
                )
            results: dict[str, list[RoundResult]] = {
                name: [] for name, _ in variant_params
            }
            conn = store.connect(db)
            total = store.window_count(conn, start_ts, end_ts)
            for i, ts in enumerate(store.iter_windows(conn, start_ts, end_ts), 1):
                w = store.load_window(conn, ts)
                if w is None or w.up_won is None:
                    continue
                for name, params in variant_params:
                    results[name].append(replay(w, params))
                if i % 200 == 0:
                    self._set(message=f"replaying {i}/{total}")
            conn.close()

            summaries = {
                name: summarize(results[name], params, days)
                for name, params in variant_params
            }
            baseline = summaries[variant_params[0][0]]
            for s in summaries.values():
                s["windows_total"] = total
            report = {
                "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
                "days": days,
                "elapsed_s": round(time.time() - t0, 1),
                "data": self.coverage(),
                "baseline_name": variant_params[0][0],
                "baseline": baseline,
                "variants": [
                    {"name": name, "summary": summaries[name]}
                    for name, _ in variant_params
                ],
                "conclusion": build_conclusion(days, baseline, [
                    {"name": name, "summary": summaries[name]}
                    for name, _ in variant_params
                ]),
            }
            path = self.report_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2),
                           encoding="utf-8")
            os.replace(tmp, path)
            with self._lock:
                self._report = report
            self._set(
                phase="done", message="done",
                finished_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
            )
            log.info("backtest.done", days=days, windows=baseline["windows_replayed"],
                     pnl=baseline["pnl_total"], path=str(path))
        except Exception as exc:  # noqa: BLE001
            log.exception("backtest.failed", error=str(exc))
            self._set(phase="error", message=str(exc), error=str(exc),
                      finished_at=_dt.datetime.now(_dt.timezone.utc).isoformat())


_manager: BacktestManager | None = None


def get_manager() -> BacktestManager:
    global _manager
    if _manager is None:
        _manager = BacktestManager()
    return _manager
