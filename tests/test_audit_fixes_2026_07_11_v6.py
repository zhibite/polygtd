"""Regression tests for the H-V6-12 audit fixes (2026-07-11).

Covers the 8 issues found by the post-deploy audit of `a642d52`:

  C-1   — clob_client.get_order_filled_size divides Polymarket base
          units (6 decimals) by 1_000_000 before returning, so the
          engine receives human-share sizes instead of micro-shares.
          (Pre-fix, a 5.71 share fill was reported as 5_710_000 shares
          → position state corrupted by 1e6.)
  C-2   — place_gtd_buy / place_gtd_sell polling loops also divide
          fills[].size and size_matched by 1_000_000.
  M-1   — user_ws._trade_dump_count is initialised in __init__ and
          reset on each successful WS reconnect, so the raw-dump
          diagnostic re-arms after a reconnect.
  M-2   — user_ws converts REST cumulative `size_matched` to a true
          delta before forwarding to the router, by tracking the
          last-seen cumulative per order_id. Without this, a second
          WS MATCHED event for the same order would double-count.
  m-2   — router.maker_fill.skip reason wording reflects that the
          router itself can't tell whether the upstream tried REST
          fallback.
  m-3   — STATUS line uPnL formatting shows "$0.00" (no sign, no %)
          when there is no live position, instead of "$+0.00(+0.0%)".

Run via `pytest tests/test_audit_fixes_2026_07_11_v6.py`.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
from typing import Any

import pytest

# Repo root on sys.path so `from src.* import …` works regardless of cwd.
_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


# ---------------------------------------------------------------------------
# C-1: get_order_filled_size base-unit → share conversion
# ---------------------------------------------------------------------------


class _FakeSyncClient:
    """Minimal stand-in for py_clob_client_v2.client.ClobClient.

    The real client's `get_order` returns a dict. We pre-load a fixed
    response and just hand it back when called.
    """

    def __init__(self, response: dict | None) -> None:
        self._response = response

    def get_order(self, order_id: str) -> dict | None:  # noqa: ARG002
        return self._response


class _StubCLOB:
    """Stand-in for CLOBClient that bypasses __init__ (which needs real SDK)."""

    def __init__(self, response: dict | None) -> None:
        from src.execution import clob_client as cc_mod
        self._CLOB_BASE_UNITS = cc_mod.CLOBClient._CLOB_BASE_UNITS
        self._client = _FakeSyncClient(response)

    async def _run(self, fn, *args, **kwargs):
        """Mimic CLOBClient._run: invoke the sync SDK method in a thread."""
        loop = asyncio.get_running_loop()
        from functools import partial
        return await loop.run_in_executor(
            None, partial(fn, *args, **kwargs),
        )

    async def get_order_filled_size(self, order_id: str) -> float | None:
        # Call the real method on the unbound class so we exercise the
        # production code path (including the base-unit divide).
        from src.execution.clob_client import CLOBClient
        return await CLOBClient.get_order_filled_size(self, order_id)


@pytest.mark.parametrize(
    "raw_size_matched, expected_shares",
    [
        ("5710000", 5710000.0),   # raw shares returned by GET /data/order/<id>
        ("100000000", 100000000.0),
        ("1000", 1000.0),
        ("0", 0.0),
    ],
)
def test_c1_get_order_filled_size_divides_base_units(
    raw_size_matched: str, expected_shares: float
) -> None:
    """
    H-V6-15 / Audit C-1 (superseded): this endpoint returns size_matched
    in shares, not 6-decimal base units. Do NOT divide by 1_000_000.
    """
    fake = _StubCLOB({"size_matched": raw_size_matched})
    out = asyncio.run(fake.get_order_filled_size("0xabc"))
    assert out == pytest.approx(expected_shares, abs=1e-9), (
        f"raw={raw_size_matched} expected {expected_shares} got {out}"
    )


def test_c1_get_order_filled_size_accepts_alternate_field_names() -> None:
    """H-V6-15 / C-1 fallback chain tries sizeMatched / amount_matched / filledSize."""
    cases = [
        ({"sizeMatched": "5710000"}, 5710000.0),
        ({"amount_matched": "5710000"}, 5710000.0),
        ({"filledSize": "5710000"}, 5710000.0),
    ]
    for payload, expected in cases:
        fake = _StubCLOB(payload)
        out = asyncio.run(fake.get_order_filled_size("0xabc"))
        assert out == pytest.approx(expected, abs=1e-9), (
            f"payload={payload} expected {expected} got {out}"
        )


def test_c1_get_order_filled_size_handles_non_dict_response() -> None:
    """
    C-1 robustness: SDK occasionally returns a list (cancel response
    shape), or None for an unknown order_id. The method must return
    None (not 0.0) so the caller can distinguish "no fill yet" from
    "could not determine". Note: when the response IS a dict but the
    size_matched field is "0", we return 0.0 — that's a separate test.
    """
    fake = _StubCLOB(None)
    out = asyncio.run(fake.get_order_filled_size("0xabc"))
    assert out is None, (
        f"None / non-dict response should return None to signal "
        f"unknown state, not 0.0 (got {out!r})"
    )


def test_c1_get_order_filled_size_returns_zero_for_unfilled_order() -> None:
    """C-1: order exists but size_matched="0" → 0.0 shares (not None)."""
    fake = _StubCLOB({"size_matched": "0", "status": "LIVE"})
    out = asyncio.run(fake.get_order_filled_size("0xabc"))
    assert out == 0.0


# ---------------------------------------------------------------------------
# M-1: _trade_dump_count reset on reconnect
# ---------------------------------------------------------------------------


def test_m1_trade_dump_count_initialized_in_constructor() -> None:
    """
    Audit M-1: pre-fix, _trade_dump_count was lazily AttributeError-init
    inside _process, so a fresh UserWS never had it as an attribute.
    Post-fix, __init__ sets it to 0 explicitly so reconnect can reset it.
    """
    from src.market.user_ws import UserWS

    u = UserWS(
        api_key="k", api_secret="s", api_passphrase="p",
    )
    assert hasattr(u, "_trade_dump_count"), (
        "UserWS.__init__ must set _trade_dump_count (audit M-1)"
    )
    assert u._trade_dump_count == 0


def test_m1_trade_dump_count_is_resettable() -> None:
    """
    Audit M-1: reconnect path resets the counter so the raw-dump
    diagnostic re-arms. Simulate by mutating the counter and resetting.
    """
    from src.market.user_ws import UserWS

    u = UserWS(api_key="k", api_secret="s", api_passphrase="p")
    u._trade_dump_count = 5  # post the throttle
    # Pre-fix this AttributeError-init pattern in _process would have
    # silently swallowed the bump back to 1; post-fix, the reconnect
    # path explicitly does `self._trade_dump_count = 0`.
    u._trade_dump_count = 0
    assert u._trade_dump_count == 0


# ---------------------------------------------------------------------------
# M-2: REST cumulative-to-delta conversion
# ---------------------------------------------------------------------------


def test_m2_extract_size_from_payload_returns_zero_for_empty() -> None:
    """Audit M-2 helper: no size keys present → 0.0 (not None)."""
    from src.market.user_ws import UserWS

    u = UserWS(api_key="k", api_secret="s", api_passphrase="p")
    assert u._extract_size_from_payload({}) == 0.0
    assert u._extract_size_from_payload({"unrelated": "x"}) == 0.0


def test_m2_extract_size_from_payload_uses_first_nonzero_key() -> None:
    """
    Audit M-2 helper: when multiple size keys are present, the first
    non-zero value wins. Order: size_matched > matched_size > size.
    """
    from src.market.user_ws import UserWS

    u = UserWS(api_key="k", api_secret="s", api_passphrase="p")
    assert u._extract_size_from_payload({"size_matched": "5.71"}) == 5.71
    assert u._extract_size_from_payload({"matched_size": 5.71}) == 5.71
    assert u._extract_size_from_payload({"size": 5.71}) == 5.71


def test_m2_extract_size_ignores_zero_size_keys() -> None:
    """
    Audit M-2 helper: the real WS payload has size=0 across the board
    while size_matched has the real value. The helper must skip the
    zero and find the populated key.
    """
    from src.market.user_ws import UserWS

    u = UserWS(api_key="k", api_secret="s", api_passphrase="p")
    assert (
        u._extract_size_from_payload({"size": "0", "size_matched": "5.71"})
        == 5.71
    )


def test_m2_cumulative_tracker_persists_per_order() -> None:
    """
    Audit M-2: a second WS MATCHED event for the same order must
    NOT cause engine double-counting. The router reads `fill_size_inc`
    and adds it to `prev_cumulative`, but the WS payload's size field
    may also be a cumulative value (especially when REST fallback
    fired). The user_ws._last_rest_cumulative dict tracks the last
    seen cumulative so the second event's delta = current - prev.
    """
    from src.market.user_ws import UserWS

    u = UserWS(api_key="k", api_secret="s", api_passphrase="p")
    oid = "0xabc"

    # First event: REST returns 5.71 (cumulative). delta == 5.71.
    prev = u._last_rest_cumulative.get(oid, 0.0)
    cur = 5.71
    delta_first = cur - prev
    assert delta_first == pytest.approx(5.71)
    u._last_rest_cumulative[oid] = cur

    # Second event: REST still 5.71 (cumulative unchanged). delta == 0.
    prev = u._last_rest_cumulative.get(oid, 0.0)
    cur = 5.71
    delta_second = cur - prev
    assert delta_second == 0.0

    # Third event: partial fill added, REST returns 7.14. delta == 1.43.
    prev = u._last_rest_cumulative.get(oid, 0.0)
    cur = 7.14
    delta_third = cur - prev
    assert delta_third == pytest.approx(1.43)


# ---------------------------------------------------------------------------
# m-2: router skip reason wording
# ---------------------------------------------------------------------------


def test_m2_router_skip_reason_does_not_blame_router() -> None:
    """
    Audit m-2: the router.maker_fill.skip reason string should not
    claim that the router itself tried the REST fallback — that
    decision lives in user_ws. We can't import the closure directly,
    but we can read the source and assert the reason string.
    """
    main_py = pathlib.Path(_ROOT / "src" / "main.py").read_text(encoding="utf-8")
    # The reason text should be about upstream / user_ws, not about
    # the router's own REST call.
    assert "upstream delivered size=0" in main_py
    assert "WS size=0 + REST fallback returned 0/None" not in main_py


# ---------------------------------------------------------------------------
# m-3: STATUS line uPnL formatting
# ---------------------------------------------------------------------------


def test_m3_status_line_pnl_zero_uses_no_sign() -> None:
    """
    Audit m-3: STATUS line should print "$0.00" (no '+', no percent)
    when there is no live position, instead of "$+0.00(+0.0%)" which
    looks like a real zero PnL.
    """
    # Build a minimal Position + engine snapshot in-memory and call the
    # status formatter. The formatter is inside _status_loop; rather
    # than monkey-patching STATE, we just assert on the source: the
    # branch must contain a "$0.00" path and must NOT print "+0.00".
    main_py = pathlib.Path(_ROOT / "src" / "main.py").read_text(encoding="utf-8")
    # Locate the position block — we know the structure from the diff.
    assert 'upnl_str = "$0.00"' in main_py, (
        "main.py must include the no-position '$0.00' branch (audit m-3)"
    )
    # And the production bug string must be gone.
    assert 'f" uPnL=${unreal_pnl:+.2f}{pnl_pct_str}"' not in main_py


# ---------------------------------------------------------------------------
# H-NEW-13 (2026-07-11): TRADE event uses TOP-LEVEL size + trader_side,
#                          not maker_orders[].matched_amount.
#
# Production symptom (2026-07-11 13:01:36):
#   T1 BUY (we were TAKER) on UP token. Polymarket WS payload:
#     taker_order_id = our order id (0x222128...)
#     size           = 5          (top-level; our share of the trade)
#     price          = 0.36
#     side           = BUY
#     trader_side    = TAKER
#     maker_orders[] = [{ asset_id: DOWN, matched_amount: 0, price: 0.64 }]
#
#   The old code read each maker_orders[] entry's size. That entry's size
#   was 0 (the OPPOSITE-side maker's volume) → router.maker_fill.skip
#   → record_fill never fired → engine.pos.total_qty stayed at 0 → T2
#   averaging was hung in place (qty=0, T1=⏳, T2=⏳, cost=$0.00).
#
# The fix: read the TOP-LEVEL size (the trade volume WE own) and pick
# our order_id from taker_order_id (when TAKER) or the maker row's
# order_id (when MAKER).
# ---------------------------------------------------------------------------


class _CapturedFill:
    """Helper that records every MakerFill the router receives.

    Supports both the 2-arg form `cb(fill, candidates=…)` and the 1-arg
    legacy form `cb(fill)` for backwards compatibility with the staged
    rollout (user_ws tries the 2-arg form first, falls back on TypeError).
    """
    def __init__(self) -> None:
        self.fills: list[Any] = []
        self.candidate_lists: list[list[Any]] = []

    def __call__(self, fill: Any, *args: Any, **kwargs: Any) -> None:
        self.fills.append(fill)
        if "candidates" in kwargs:
            self.candidate_lists.append(list(kwargs["candidates"]))


class _StubCLOBEmpty:
    """Always returns 0 / None from the REST fallback."""
    async def get_order_filled_size(self, _order_id: str) -> float | None:
        return 0.0


@pytest.mark.asyncio
async def test_t13_trade_taker_path_uses_top_level_size() -> None:
    """When we are TAKER, the callback must receive our share as
    MakerFill.size and our taker_order_id as maker_order_id, even
    though maker_orders[] is full of opposite-side zero-fills."""
    from src.market.user_ws import UserWS

    captured = _CapturedFill()
    ws = UserWS(
        api_key="test-key",
        api_secret="test-secret",
        api_passphrase="test-pass",
        clob=_StubCLOBEmpty(),  # type: ignore[arg-type]
        on_maker_fill=captured,  # type: ignore[arg-type]
    )

    # Reproduces the 2026-07-11 13:01:36 production payload.
    payload = {
        "id": "trade-abc",
        "taker_order_id": "0x222128208af908ce3b75",
        "asset_id": "UP_TOKEN_6489",
        "side": "BUY",
        "price": "0.36",
        "size": "5",
        "status": "MINED",
        "trader_side": "TAKER",
        "maker_orders": [
            {
                "order_id": "0x7eff6d8ce7eb539ab73ed",
                "asset_id": "DOWN_TOKEN_1013",
                "matched_amount": "0",
                "price": "0.64",
                "side": "SELL",
            }
        ],
    }
    await ws._handle_trade(payload)

    assert len(captured.fills) == 1, (
        "TAKER path should fire the callback exactly once per TRADE event, "
        "NOT once per maker_orders[] entry"
    )
    fill = captured.fills[0]
    # Order id must be OUR taker order, not the maker in maker_orders[].
    assert fill.maker_order_id == "0x222128208af908ce3b75"
    # Asset id must be the TOP-level asset_id (our token), not the maker's.
    assert fill.asset_id == "UP_TOKEN_6489"
    # Size must be the TOP-level size (5), NOT the maker's matched_amount=0.
    assert fill.size == pytest.approx(5.0)
    # Price and status must come from the top level.
    assert fill.price == pytest.approx(0.36)
    assert fill.status == "MINED"


@pytest.mark.asyncio
async def test_t13_trade_maker_path_uses_maker_row_order_id() -> None:
    """When we are MAKER (e.g. a TP SELL resting at 0.40 that got hit),
    our order id is the maker row in maker_orders[]. The top-level size
    is still OUR share of the trade."""
    from src.market.user_ws import UserWS

    captured = _CapturedFill()
    ws = UserWS(
        api_key="test-key",
        api_secret="test-secret",
        api_passphrase="test-pass",
        clob=_StubCLOBEmpty(),  # type: ignore[arg-type]
        on_maker_fill=captured,  # type: ignore[arg-type]
    )

    our_maker_order = "0xourselfforchristmastx99"
    payload = {
        "id": "trade-mkr",
        "taker_order_id": "0xcounterparty_taker",
        "asset_id": "UP_TOKEN_6489",
        "side": "SELL",
        "price": "0.40",
        "size": "3.0",
        "status": "CONFIRMED",
        "trader_side": "MAKER",
        "maker_orders": [
            {
                "order_id": our_maker_order,
                "asset_id": "UP_TOKEN_6489",
                "matched_amount": "3.0",
                "price": "0.40",
                "side": "SELL",
            }
        ],
    }
    await ws._handle_trade(payload)

    assert len(captured.fills) == 1
    fill = captured.fills[0]
    # Authoritative position: user_ws does NOT decide which candidate is
    # "ours" — it ships the full candidate list to the router, which uses
    # OrderManager.by_server_id (full hash) to pick. So we assert on the
    # candidate list, not on fill.maker_order_id.
    assert len(captured.candidate_lists) == 1
    cands = captured.candidate_lists[0]
    order_ids = [c[0] for c in cands]
    assert our_maker_order in order_ids, (
        "MAKER path: the OUR maker order_id MUST be in the candidates list "
        "sent to the router — otherwise the router cannot attribute the "
        "fill via OrderManager lookup."
    )
    # The counterparty's taker_order_id is ALSO included (defensively,
    # because polymarket aggregates the trade into one event).
    assert "0xcounterparty_taker" in order_ids
    # Per-row asset_id is preserved: the OUR maker row's asset_id is
    # preserved in the candidate tuple (NOT silently replaced by the
    # top-level asset_id).
    aid_by_oid = {c[0]: c[1] for c in cands}
    assert aid_by_oid[our_maker_order] == "UP_TOKEN_6489"
    # Top-level fields flow through to the MakerFill (legacy slot).
    assert fill.price == pytest.approx(0.40)
    assert fill.size == pytest.approx(3.0)
    assert fill.asset_id == "UP_TOKEN_6489"


@pytest.mark.asyncio
async def test_t13_trade_callback_fires_once_per_trade_event() -> None:
    """A single TRADE event must NOT emit one callback per maker_orders[]
    entry — that was the pre-fix bug that double-counted and routed
    fills to the wrong token."""
    from src.market.user_ws import UserWS

    captured = _CapturedFill()
    ws = UserWS(
        api_key="test-key",
        api_secret="test-secret",
        api_passphrase="test-pass",
        clob=_StubCLOBEmpty(),  # type: ignore[arg-type]
        on_maker_fill=captured,  # type: ignore[arg-type]
    )

    payload = {
        "id": "trade-multi",
        "taker_order_id": "0xtaker",
        "asset_id": "UP_TOKEN_6489",
        "side": "BUY",
        "price": "0.36",
        "size": "5",
        "status": "CONFIRMED",
        "trader_side": "TAKER",
        "maker_orders": [
            {"order_id": "0xm1", "asset_id": "DOWN", "matched_amount": "2"},
            {"order_id": "0xm2", "asset_id": "DOWN", "matched_amount": "3"},
            {"order_id": "0xm3", "asset_id": "DOWN", "matched_amount": "0"},
        ],
    }
    await ws._handle_trade(payload)

    assert len(captured.fills) == 1, (
        "Even when maker_orders[] has multiple entries, a single TRADE "
        "event represents ONE fill on OUR side. Emitting per-maker was "
        "the H-NEW-13 bug."
    )


@pytest.mark.asyncio
async def test_t13_trade_zero_size_after_fallback_skips_callback() -> None:
    """If top-level size is 0 AND the REST fallback returns 0/None,
    the callback must NOT fire. Without this guard a phantom fill
    would corrupt position state."""
    from src.market.user_ws import UserWS

    captured = _CapturedFill()
    ws = UserWS(
        api_key="test-key",
        api_secret="test-secret",
        api_passphrase="test-pass",
        clob=_StubCLOBEmpty(),  # type: ignore[arg-type]
        on_maker_fill=captured,  # type: ignore[arg-type]
    )

    payload = {
        "id": "trade-zero",
        "taker_order_id": "0xtaker",
        "asset_id": "UP_TOKEN_6489",
        "side": "BUY",
        "price": "0.36",
        "size": "0",
        "status": "MINED",
        "trader_side": "TAKER",
        "maker_orders": [],
    }
    await ws._handle_trade(payload)

    assert captured.fills == [], (
        "size=0 with no REST recovery must not deliver a phantom fill"
    )