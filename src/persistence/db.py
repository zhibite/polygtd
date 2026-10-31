"""SQLite persistence layer.

Schema:
  rounds     — one row per 15-min round (keyed by round_start_ts)
  orders     — one row per tracked order
  fills      — one row per fill event
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.infra.logging import get_logger

log = get_logger(__name__)

# Runtime data lives under <project>/data by default. In Docker that path is
# /app/data, which is the volume mounted by docker-compose, so the DB survives
# container recreation. Both env overrides are available for custom layouts.
_DATA_DIR = Path(
    os.getenv("POLYMARKET_DATA_DIR")
    or (Path(__file__).parent.parent.parent / "data")
)
_DB_PATH = Path(os.getenv("POLYMARKET_DB_PATH") or (_DATA_DIR / "polymarket.db"))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS rounds (
  round_start_ts  INTEGER PRIMARY KEY,
  slug            TEXT,
  condition_id    TEXT,
  up_token        TEXT,
  down_token      TEXT,
  tick_size       REAL DEFAULT 0.01,
  binance_baseline REAL,
  finished        INTEGER DEFAULT 0,
  started_at      TEXT,
  finished_at     TEXT
);

CREATE TABLE IF NOT EXISTS orders (
  client_order_id TEXT PRIMARY KEY,
  server_order_id TEXT,
  round_start_ts  INTEGER,
  side            TEXT,
  token_id        TEXT,
  tier_name       TEXT,
  price           REAL,
  size            REAL,
  lifetime_s      INTEGER,
  expires_at      INTEGER,
  placed_at       INTEGER,
  size_matched    REAL DEFAULT 0.0,
  status          TEXT DEFAULT 'placed'
);

CREATE TABLE IF NOT EXISTS fills (
  fill_id        TEXT PRIMARY KEY,
  order_id       TEXT,
  round_start_ts INTEGER,
  token_id       TEXT,
  side           TEXT,
  price          REAL,
  size           REAL,
  ts_ms          INTEGER,
  confirmed      INTEGER DEFAULT 0,
  recorded_at     TEXT,
  trader_side    TEXT,
  fee            REAL DEFAULT 0
);

-- The dashboard filters rounds by activity. These keep that lookup cheap
-- as fills accumulate (rounds are keyed by round_start_ts already).
CREATE INDEX IF NOT EXISTS idx_orders_round ON orders(round_start_ts);
CREATE INDEX IF NOT EXISTS idx_fills_round  ON fills(round_start_ts);
"""


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def init_db(db_path: Path = _DB_PATH) -> sqlite3.Connection:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    for stmt in SCHEMA.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)
    # Lightweight migrations for databases created by older builds.
    # `CREATE TABLE IF NOT EXISTS` never adds columns, so apply the
    # additive changes explicitly.
    try:
        fill_cols = {row[1] for row in conn.execute("PRAGMA table_info(fills)")}
        if "trader_side" not in fill_cols:
            conn.execute("ALTER TABLE fills ADD COLUMN trader_side TEXT")
            log.info("db.migrate.fills_trader_side")
        if "fee" not in fill_cols:
            conn.execute("ALTER TABLE fills ADD COLUMN fee REAL DEFAULT 0")
            log.info("db.migrate.fills_fee")
    except Exception as exc:  # noqa: BLE001
        log.warning("db.migrate.failed", error=str(exc))
    conn.commit()
    log.info("db.init", path=str(db_path))
    return conn


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class DB:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._c = conn

    def upsert_round(
        self,
        round_start_ts: int,
        slug: str | None = None,
        condition_id: str | None = None,
        up_token: str | None = None,
        down_token: str | None = None,
        tick_size: float = 0.01,
        binance_baseline: float | None = None,
    ) -> None:
        # `started_at` is the WINDOW start, derived from the round key, not
        # the moment this row happened to be written. Rounds are inserted
        # lazily on the first fill (see OrderManager._ensure_round_row), so
        # a wall-clock value here would show the fill time under the
        # "开始 (UTC)" column instead of the actual window start.
        started_at = datetime.fromtimestamp(
            int(round_start_ts), tz=timezone.utc
        ).isoformat()
        self._c.execute("""
            INSERT INTO rounds (round_start_ts, slug, condition_id, up_token, down_token,
                                tick_size, binance_baseline, started_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(round_start_ts) DO UPDATE SET
              slug=COALESCE(excluded.slug, slug),
              condition_id=COALESCE(excluded.condition_id, condition_id),
              up_token=COALESCE(excluded.up_token, up_token),
              down_token=COALESCE(excluded.down_token, down_token),
              tick_size=excluded.tick_size,
              binance_baseline=COALESCE(excluded.binance_baseline, binance_baseline)
        """, (round_start_ts, slug, condition_id, up_token, down_token,
              tick_size, binance_baseline, started_at))
        self._c.commit()

    def finish_round(self, round_start_ts: int) -> None:
        self._c.execute(
            "UPDATE rounds SET finished=1, finished_at=? WHERE round_start_ts=?",
            (datetime.now(timezone.utc).isoformat(), round_start_ts)
        )
        self._c.commit()

    def insert_order(
        self,
        client_order_id: str,
        server_order_id: str | None,
        round_start_ts: int,
        side: str,
        token_id: str,
        tier_name: str,
        price: float,
        size: float,
        lifetime_s: int,
        expires_at: int,
        placed_at: int,
    ) -> None:
        self._c.execute("""
            INSERT OR REPLACE INTO orders
              (client_order_id, server_order_id, round_start_ts, side, token_id,
               tier_name, price, size, lifetime_s, expires_at, placed_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'placed')
        """, (client_order_id, server_order_id, round_start_ts, side, token_id,
              tier_name, price, size, lifetime_s, expires_at, placed_at))
        self._c.commit()

    def update_order_status(
        self, client_order_id: str, status: str, size_matched: float = 0.0
    ) -> None:
        self._c.execute(
            "UPDATE orders SET status=?, size_matched=? WHERE client_order_id=?",
            (status, size_matched, client_order_id)
        )
        self._c.commit()

    def update_server_order_id(self, client_order_id: str, server_order_id: str) -> None:
        self._c.execute(
            "UPDATE orders SET server_order_id=? WHERE client_order_id=?",
            (server_order_id, client_order_id)
        )
        self._c.commit()

    def insert_fill(
        self,
        fill_id: str,
        order_id: str,
        round_start_ts: int,
        token_id: str,
        side: str,
        price: float,
        size: float,
        ts_ms: int,
        confirmed: int = 0,
        trader_side: str | None = None,
        fee: float = 0.0,
    ) -> None:
        self._c.execute("""
            INSERT OR IGNORE INTO fills
              (fill_id, order_id, round_start_ts, token_id, side,
               price, size, ts_ms, confirmed, recorded_at, trader_side, fee)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (fill_id, order_id, round_start_ts, token_id, side,
              price, size, ts_ms, confirmed,
              datetime.now(timezone.utc).isoformat(), trader_side,
              float(fee or 0.0)))
        self._c.commit()

    def confirm_fill(self, fill_id: str) -> None:
        self._c.execute("UPDATE fills SET confirmed=1 WHERE fill_id=?", (fill_id,))
        self._c.commit()

    def rounds_in_progress(self) -> list[tuple]:
        cur = self._c.execute(
            "SELECT round_start_ts, slug, condition_id, up_token, down_token, tick_size "
            "FROM rounds WHERE finished=0 ORDER BY round_start_ts DESC LIMIT 1"
        )
        return cur.fetchall()

    def orders_for_round(self, round_start_ts: int) -> list[tuple]:
        cur = self._c.execute(
            "SELECT client_order_id, server_order_id, round_start_ts, side, token_id, "
            "tier_name, price, size, lifetime_s, expires_at, placed_at, size_matched, status "
            "FROM orders WHERE round_start_ts=?", (round_start_ts,)
        )
        return cur.fetchall()
