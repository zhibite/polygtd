"""Emergency-stop switch for the strategy.

Semantics when engaged:

  * ``StrategyEngine._evaluate`` returns immediately — no new actions.
  * ``OrderManager`` refuses every ``place_*`` call (defense in depth; an
    action already in flight when the switch flips must not reach the CLOB).
  * ``ActionRouter`` drops actions emitted before the flag was set.
  * The dashboard control endpoint cancels every open GTD and stops the
    TierWatchers before returning.

The state is persisted to ``data/emergency_stop.json``, so a restart or
redeploy does **not** silently resume trading — the operator must press
「恢复运行」. (Deleting the file by hand also releases it.)

The position is deliberately NOT sold automatically: an emergency stop
halts new orders; it does not market-sell inventory at an arbitrary price.
Closing the position stays a human decision.
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from src.infra.logging import get_logger

log = get_logger(__name__)

_DATA_DIR = Path(
    os.getenv("POLYMARKET_DATA_DIR")
    or (Path(__file__).resolve().parents[2] / "data")
)
STATE_PATH = Path(
    os.getenv("POLYGTD_KILLSWITCH_FILE") or (_DATA_DIR / "emergency_stop.json")
)


@dataclass
class KillSwitch:
    """Shared flag read by the engine, router and order manager."""

    active: bool = False
    since: float = 0.0
    reason: str = ""
    path: Path = field(default=STATE_PATH, repr=False)

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def engage(self, reason: str = "manual") -> None:
        if not self.active:
            self.since = time.time()
        self.active = True
        self.reason = reason or "manual"
        self._save()

    def release(self) -> None:
        self.active = False
        self.since = 0.0
        self.reason = ""
        self._delete()

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        return {
            "active": self.active,
            "since": self.since or None,
            "reason": self.reason or None,
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {"active": self.active, "since": self.since, "reason": self.reason},
                indent=2,
            )
            fd, tmp = tempfile.mkstemp(
                dir=str(self.path.parent), prefix=".killswitch.", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(payload)
                os.replace(tmp, self.path)
            except Exception:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
        except Exception as exc:  # noqa: BLE001
            # The in-memory flag is what matters right now; a disk failure
            # only loses the "stay stopped after restart" guarantee.
            log.warning("killswitch.persist_failed", error=str(exc))

    def _delete(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            log.warning("killswitch.unlink_failed", error=str(exc))

    @classmethod
    def load(cls, path: Path | None = None) -> "KillSwitch":
        target = Path(path) if path is not None else STATE_PATH
        ks = cls(path=target)
        if not target.exists():
            return ks
        try:
            raw = json.loads(target.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            log.warning("killswitch.state_unreadable", path=str(target), error=str(exc))
            return ks
        if isinstance(raw, dict) and raw.get("active"):
            ks.active = True
            ks.since = float(raw.get("since") or 0.0)
            ks.reason = str(raw.get("reason") or "manual")
            log.warning(
                "killswitch.restored_stopped",
                since=ks.since,
                reason=ks.reason,
                note="bot stays stopped after restart until released from the dashboard",
            )
        return ks


# ---------------------------------------------------------------------------
# Engage / release helpers (used by the web API and tests)
# ---------------------------------------------------------------------------

async def engage(state: Any, reason: str = "dashboard") -> dict[str, Any]:
    """Flip the switch, stop watchers and cancel every open order."""
    ks = getattr(state, "killswitch", None)
    if ks is None:
        raise RuntimeError("killswitch not initialised")
    already = ks.active
    ks.engage(reason)

    cancelled = 0
    router = getattr(state, "router", None)
    if router is not None:
        try:
            await router.stop_all_watchers()
        except Exception as exc:  # noqa: BLE001
            log.warning("killswitch.stop_watchers_failed", error=str(exc))

    om = getattr(state, "order_manager", None)
    if om is not None:
        try:
            cancelled = await om.cancel_all_open(reason="killswitch")
        except Exception as exc:  # noqa: BLE001
            log.warning("killswitch.cancel_failed", error=str(exc))

    log.warning(
        "killswitch.engaged",
        reason=reason,
        already_active=already,
        cancelled_orders=cancelled,
    )
    return {"killswitch": ks.snapshot(), "cancelled_orders": cancelled}


def release(state: Any, reason: str = "dashboard") -> dict[str, Any]:
    ks = getattr(state, "killswitch", None)
    if ks is None:
        raise RuntimeError("killswitch not initialised")
    ks.release()
    log.warning("killswitch.released", reason=reason)
    return {"killswitch": ks.snapshot()}
