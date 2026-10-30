"""Runtime strategy-config overrides (dashboard /config page).

Covers:
  * whitelist / range / cross-field validation
  * persistence to the data-volume JSON and re-application at boot
  * reset back to .env / code defaults
  * GateKeeper reading the drift limit live (so edits apply immediately)
  * the HTTP API used by the config page
"""
from __future__ import annotations

import pytest

from src.config import StrategySettings, strategy
from src.strategy.gatekeeper import GateKeeper
from src.web import runtime_config as rc


@pytest.fixture()
def cfg_file(tmp_path, monkeypatch):
    monkeypatch.setattr(rc, "CONFIG_PATH", tmp_path / "runtime_config.json")
    snapshot = rc.current_values()
    yield rc
    live = strategy()
    for key, value in snapshot.items():
        setattr(live, key, value)


@pytest.fixture()
def web_auth():
    """Configure dashboard credentials for the login-gated config routes."""
    live = strategy()
    old_user, old_pass = live.WEB_USERNAME, live.WEB_PASSWORD
    live.WEB_USERNAME = "admin"
    live.WEB_PASSWORD = "test-pass"
    yield
    live.WEB_USERNAME = old_user
    live.WEB_PASSWORD = old_pass


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def test_rejects_unknown_keys(cfg_file) -> None:
    with pytest.raises(ValueError, match="不允许的配置项"):
        cfg_file.update({"WEB_PASSWORD": "hunter2"})


def test_rejects_out_of_range(cfg_file) -> None:
    with pytest.raises(ValueError, match="不能大于"):
        cfg_file.update({"ENTRY_PRICE_CAP": 1.5})
    with pytest.raises(ValueError, match="不能小于"):
        cfg_file.update({"TIER_WATCHER_POLL_S": 0.1})


def test_rejects_non_integer_for_int_param(cfg_file) -> None:
    with pytest.raises(ValueError, match="需要整数"):
        cfg_file.update({"T1_ENTRY_WINDOW_S": 120.5})


def test_rejects_inverted_tp_floor(cfg_file) -> None:
    with pytest.raises(ValueError, match="TP_MIN_PCT"):
        cfg_file.update({"TP_MIN_PCT": 0.9, "TP_PROFIT_PCT": 0.2})


def test_rejects_tier_drop_order(cfg_file) -> None:
    with pytest.raises(ValueError, match="T2 < T3 < T4"):
        cfg_file.update({"TIER2_DROP_PCT": 0.5, "TIER3_DROP_PCT": 0.4})


def test_bool_accepts_strings(cfg_file) -> None:
    values = cfg_file.update({"SKIP_IF_LATE_WINDOW_AT_BOOT": "false"})
    assert values["SKIP_IF_LATE_WINDOW_AT_BOOT"] is False
    values = cfg_file.update({"SKIP_IF_LATE_WINDOW_AT_BOOT": "true"})
    assert values["SKIP_IF_LATE_WINDOW_AT_BOOT"] is True


# ---------------------------------------------------------------------------
# Apply / persist / reset
# ---------------------------------------------------------------------------

def test_update_applies_and_persists(cfg_file) -> None:
    values = cfg_file.update({
        "ENTRY_PRICE_CAP": 0.33,
        "GTD_LIFETIME_S": 240,
        "BASELINE_DRIFT_LIMIT": 0.002,
    })

    assert values["ENTRY_PRICE_CAP"] == pytest.approx(0.33)
    assert strategy().ENTRY_PRICE_CAP == pytest.approx(0.33)
    assert strategy().GTD_LIFETIME_S == 240
    assert cfg_file.CONFIG_PATH.exists()
    assert cfg_file.load_overrides() == {
        "ENTRY_PRICE_CAP": 0.33,
        "GTD_LIFETIME_S": 240,
        "BASELINE_DRIFT_LIMIT": 0.002,
    }


def test_update_only_writes_patched_keys(cfg_file) -> None:
    """Untouched params must keep following .env, not freeze as overrides."""
    cfg_file.update({"ENTRY_PRICE_CAP": 0.33})
    cfg_file.update({"TP_PROFIT_PCT": 0.25})
    overrides = cfg_file.load_overrides()
    assert set(overrides) == {"ENTRY_PRICE_CAP", "TP_PROFIT_PCT"}


def test_persisted_overrides_reapplied_at_boot(cfg_file) -> None:
    cfg_file.update({"ENTRY_PRICE_CAP": 0.31})
    # Simulate a process restart: config singleton back at the env value.
    strategy().ENTRY_PRICE_CAP = 0.40

    applied = cfg_file.apply_persisted_overrides()

    assert applied == 1
    assert strategy().ENTRY_PRICE_CAP == pytest.approx(0.31)


def test_reset_restores_env_values(cfg_file) -> None:
    cfg_file.update({"ENTRY_PRICE_CAP": 0.31, "TIER1_NOTIONAL": 9.0})

    values = cfg_file.reset()

    fresh = StrategySettings()
    assert values["ENTRY_PRICE_CAP"] == pytest.approx(fresh.ENTRY_PRICE_CAP)
    assert values["TIER1_NOTIONAL"] == pytest.approx(fresh.TIER1_NOTIONAL)
    assert not cfg_file.CONFIG_PATH.exists()


def test_corrupt_override_file_is_ignored(cfg_file) -> None:
    cfg_file.CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    cfg_file.CONFIG_PATH.write_text("{not json", encoding="utf-8")
    assert cfg_file.load_overrides() == {}
    assert cfg_file.apply_persisted_overrides() == 0


# ---------------------------------------------------------------------------
# GateKeeper must see a runtime drift-limit change immediately
# ---------------------------------------------------------------------------

def test_gatekeeper_reads_live_drift_limit(cfg_file) -> None:
    gate = GateKeeper(100.0)
    old = strategy().BASELINE_DRIFT_LIMIT
    try:
        strategy().BASELINE_DRIFT_LIMIT = 0.0001
        assert gate.state(100.5).within_limit is False
        strategy().BASELINE_DRIFT_LIMIT = 0.01
        assert gate.state(100.5).within_limit is True
    finally:
        strategy().BASELINE_DRIFT_LIMIT = old


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

def _login(client) -> None:
    r = client.post(
        "/api/auth/login",
        json={"username": "admin", "password": "test-pass"},
    )
    assert r.status_code == 200, r.text


def test_config_api_requires_login(cfg_file, web_auth) -> None:
    from fastapi.testclient import TestClient

    from src.web.app import app

    client = TestClient(app)
    assert client.get("/api/config").status_code == 401
    assert client.put("/api/config", json={"ENTRY_PRICE_CAP": 0.35}).status_code == 401
    assert client.post("/api/config/reset").status_code == 401


def test_config_api_roundtrip(cfg_file, web_auth) -> None:
    from fastapi.testclient import TestClient

    from src.web import auth
    from src.web.app import app

    auth.reset_login_failures("testclient")
    client = TestClient(app)
    _login(client)

    r = client.get("/api/config")
    assert r.status_code == 200
    body = r.json()
    assert body["values"]["ENTRY_PRICE_CAP"] == pytest.approx(
        strategy().ENTRY_PRICE_CAP
    )
    assert body["spec"]["groups"]
    assert any(p["key"] == "ENTRY_PRICE_CAP" for p in body["spec"]["params"])

    r = client.put("/api/config", json={"ENTRY_PRICE_CAP": 0.35})
    assert r.status_code == 200
    assert r.json()["values"]["ENTRY_PRICE_CAP"] == pytest.approx(0.35)

    r = client.put("/api/config", json={"NOT_A_PARAM": 1})
    assert r.status_code == 400

    r = client.put("/api/config", json={"ENTRY_PRICE_CAP": 9})
    assert r.status_code == 400

    r = client.post("/api/config/reset")
    assert r.status_code == 200
    assert r.json()["overrides"] == {}


def test_config_page_requires_login(cfg_file, web_auth) -> None:
    from fastapi.testclient import TestClient

    from src.web import auth
    from src.web.app import app

    auth.reset_login_failures("testclient")
    client = TestClient(app)

    r = client.get("/config", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["location"].startswith("/login")

    _login(client)
    r = client.get("/config")
    assert r.status_code == 200
    assert "策略配置" in r.text
