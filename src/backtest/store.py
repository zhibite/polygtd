"""Historical data collector for the BTC 15m backtest.

Sources (verified 2026-10-31 from the VPS):
  * future.news event endpoint  -> token ids + resolved winner
  * future.news token_kline     -> ~5 s bars over the 15 m window
  * Binance REST klines 1m      -> BTC spot for the drift / z gates

Everything is cached in SQLite and the collector is resumable: re-running
skips windows with ``kline_ok=1`` unless ``--refresh`` is passed. The API
is third-party; requests are serialized and spaced (default 0.12 s).
"""
from __future__ import annotations

import datetime as _dt
import json
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from .model import BtcBar, PricePoint, WindowData

FUTURE_BASE = "https://future.news"
DATA_API = "https://data-api.polymarket.com"
BINANCE_KLINES = "https://api.binance.com/api/v3/klines"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS windows (
  start_ts INTEGER PRIMARY KEY,
  slug TEXT NOT NULL,
  up_token TEXT,
  down_token TEXT,
  condition_id TEXT,
  resolved_asset_id TEXT,
  up_won INTEGER,
  status TEXT,
  volume REAL,
  kline_ok INTEGER DEFAULT 0,
  fetched_at TEXT
);
CREATE TABLE IF NOT EXISTS prices (
  start_ts INTEGER NOT NULL,
  side TEXT NOT NULL,
  time_ms INTEGER NOT NULL,
  price REAL NOT NULL,
  volume REAL,
  PRIMARY KEY (start_ts, side, time_ms)
);
CREATE TABLE IF NOT EXISTS btc_1m (
  time_ms INTEGER PRIMARY KEY,
  open REAL, high REAL, low REAL, close REAL, volume REAL
);
CREATE INDEX IF NOT EXISTS idx_prices_window ON prices(start_ts, side, time_ms);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


class Fetcher:
    def __init__(self, min_interval_s: float = 0.12, timeout_s: float = 20.0) -> None:
        self.min_interval_s = min_interval_s
        self.timeout_s = timeout_s
        self._last = 0.0
        self.requests = 0

    def _sleep(self) -> None:
        wait = self.min_interval_s - (time.time() - self._last)
        if wait > 0:
            time.sleep(wait)
        self._last = time.time()

    def get_json(self, url: str, referer: str) -> dict | None:
        headers = {
            "User-Agent": UA,
            "Accept": "application/json, text/plain, */*",
            "Referer": referer,
            "Origin": "https://future.news",
        }
        for attempt in range(4):
            self._sleep()
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                    self.requests += 1
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 500, 502, 503, 504) and attempt < 3:
                    time.sleep(2 ** attempt)
                    continue
                print(f"  HTTP {exc.code} {url}")
                return None
            except Exception as exc:  # noqa: BLE001
                if attempt < 3:
                    time.sleep(2 ** attempt)
                    continue
                print(f"  ERROR {type(exc).__name__} {url}: {exc}")
                return None
        return None


def align_start(ts: float | None = None) -> int:
    """Current 15-minute window start (UTC epoch seconds)."""
    now = time.time() if ts is None else ts
    return int(now // 900) * 900


def slug_for(start_ts: int) -> str:
    return f"btc-updown-15m-{int(start_ts)}"


def fetch_event(f: Fetcher, start_ts: int) -> dict | None:
    slug = slug_for(start_ts)
    d = f.get_json(
        f"{FUTURE_BASE}/api/v1/event/polymarket/{slug}",
        f"{FUTURE_BASE}/event/{slug}",
    )
    if not d or d.get("code") != 0 or not d.get("data"):
        return None
    data = d["data"]
    mi = (data.get("marketInfo") or [{}])[0]
    tokens = mi.get("assetIds") or []
    if len(tokens) < 2:
        return None
    resolved = mi.get("resolvedAssetId") or ""
    return {
        "slug": data.get("slug") or slug,
        "up_token": str(tokens[0]),
        "down_token": str(tokens[1]),
        "condition_id": mi.get("conditionId") or "",
        "resolved_asset_id": str(resolved),
        "status": data.get("status") or "",
        "volume": float(data.get("volume") or 0.0),
    }


def fetch_kline(f: Fetcher, start_ts: int, token: str) -> list[PricePoint]:
    slug = slug_for(start_ts)
    d = f.get_json(
        f"{FUTURE_BASE}/api/v1/token_kline/polymarket/{token}?period=15m",
        f"{FUTURE_BASE}/event/{slug}",
    )
    if not d or d.get("code") != 0 or not d.get("data"):
        return []
    out: list[PricePoint] = []
    for row in d["data"].get("list") or []:
        try:
            out.append(
                PricePoint(
                    time_ms=int(row["time"]),
                    price=float(row["price"]),
                    volume=float(row.get("volume") or 0.0),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return out


def fetch_trades(f: Fetcher, start_ts: int, condition_id: str) -> list[dict]:
    """Individual fills for a market, from the public Polymarket data API.

    The future.news kline starts ~75 s after the window opens, so fast
    early ladders (T1/T2/T3 inside the first minute) are invisible there.
    The data API returns every fill for the condition, including the
    first seconds. We keep one point per (asset, second): the newest
    trade in that second wins.
    """
    if not condition_id:
        return []
    base = (
        f"{DATA_API}/trades?market={condition_id}"
        f"&start={int(start_ts) - 120}&end={int(start_ts) + 960}"
        "&limit=500"
    )
    out: list[dict] = []
    offset = 0
    for _ in range(10):  # hard bound; a 15m market rarely needs >5 pages
        d = f.get_json(f"{base}&offset={offset}", f"{FUTURE_BASE}/event/{slug_for(start_ts)}")
        if not isinstance(d, list) or not d:
            break
        out.extend(d)
        if len(d) < 500:
            break
        offset += 500
    return out


def upsert_window(conn: sqlite3.Connection, start_ts: int, ev: dict,
                  kline_ok: bool) -> None:
    up_won: int | None
    resolved = ev.get("resolved_asset_id") or ""
    if resolved:
        up_won = 1 if resolved == ev["up_token"] else 0
    else:
        up_won = None
    conn.execute(
        """
        INSERT INTO windows (start_ts, slug, up_token, down_token, condition_id,
                             resolved_asset_id, up_won, status, volume, kline_ok,
                             fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(start_ts) DO UPDATE SET
          slug=excluded.slug, up_token=excluded.up_token,
          down_token=excluded.down_token, condition_id=excluded.condition_id,
          resolved_asset_id=excluded.resolved_asset_id, up_won=excluded.up_won,
          status=excluded.status, volume=excluded.volume,
          kline_ok=excluded.kline_ok, fetched_at=excluded.fetched_at
        """,
        (
            int(start_ts), ev["slug"], ev["up_token"], ev["down_token"],
            ev["condition_id"], resolved, up_won, ev["status"], ev["volume"],
            1 if kline_ok else 0,
            _dt.datetime.now(_dt.timezone.utc).isoformat(),
        ),
    )


def upsert_prices(conn: sqlite3.Connection, start_ts: int, side: str,
                  points: list[PricePoint]) -> None:
    conn.executemany(
        "INSERT OR REPLACE INTO prices (start_ts, side, time_ms, price, volume) "
        "VALUES (?,?,?,?,?)",
        [(int(start_ts), side, p.time_ms, p.price, p.volume) for p in points],
    )


def window_done(conn: sqlite3.Connection, start_ts: int) -> bool:
    row = conn.execute(
        "SELECT kline_ok FROM windows WHERE start_ts=?", (int(start_ts),)
    ).fetchone()
    return bool(row and row["kline_ok"])


def collect_btc(conn: sqlite3.Connection, f: Fetcher, start_ms: int,
                end_ms: int) -> int:
    """Fetch Binance 1 m BTCUSDT klines and upsert them."""
    total = 0
    cursor = int(start_ms)
    while cursor < end_ms:
        url = (
            f"{BINANCE_KLINES}?symbol=BTCUSDT&interval=1m"
            f"&startTime={cursor}&limit=1000"
        )
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                rows = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            print(f"  binance ERROR {type(exc).__name__}: {exc}")
            break
        if not rows:
            break
        payload = [
            (int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]),
             float(r[5]))
            for r in rows
        ]
        conn.executemany(
            "INSERT OR REPLACE INTO btc_1m "
            "(time_ms, open, high, low, close, volume) VALUES (?,?,?,?,?,?)",
            payload,
        )
        conn.commit()
        total += len(payload)
        cursor = int(rows[-1][0]) + 60_000
        time.sleep(0.05)
    return total


def collect(db_path: str | Path, days: int, end_ts: int | None = None,
            refresh: bool = False, rate_limit_s: float = 0.15,
            progress=None) -> dict:
    """Fetch/caches the last ``days`` completed windows.

    ``progress`` is called after every window with a small dict so the web
    UI can show a live progress bar. The function is blocking; the runner
    calls it from a background thread.
    """
    conn = connect(db_path)
    end_ts = align_start() if end_ts is None else int(end_ts)
    start_ts = end_ts - int(days) * 86_400
    f = Fetcher(min_interval_s=rate_limit_s)
    t0 = time.time()
    print(f"collect: {days}d  {start_ts} -> {end_ts}  db={db_path}")
    if progress:
        progress({"phase": "btc", "done": 0, "total": 0, "message": "fetching BTC 1m"})
    btc_n = collect_btc(conn, f, (start_ts - 120) * 1000,
                        (end_ts + 120) * 1000)
    print(f"  btc_1m rows upserted: {btc_n}")

    windows = list(range(start_ts, end_ts, 900))
    ok = skipped = failed = 0
    for i, ts in enumerate(windows, 1):
        if not refresh and window_done(conn, ts):
            skipped += 1
            if progress and i % 5 == 0:
                progress({"phase": "collecting", "done": i, "total": len(windows),
                          "ok": ok, "skipped": skipped, "failed": failed,
                          "requests": f.requests})
            continue
        ev = fetch_event(f, ts)
        if ev is None:
            failed += 1
            continue
        up = fetch_kline(f, ts, ev["up_token"])
        down = fetch_kline(f, ts, ev["down_token"])
        trades = fetch_trades(f, ts, ev["condition_id"])

        # Merge: the 5 s kline is mid-like and smooth; the per-second
        # trades are bid/ask prints (a single 0.40 SELL print can lie well
        # below the mid). Use trades ONLY before the first kline sample,
        # which is exactly the ~75 s blind spot at the window open. After
        # that the kline wins, so a noisy print cannot fake a T1 entry.
        first_kline_ms = min(
            [p.time_ms for p in up] + [p.time_ms for p in down]
        ) if (up or down) else None
        merged = {"UP": {p.time_ms: p for p in up},
                  "DOWN": {p.time_ms: p for p in down}}
        for tr in reversed(trades):  # API is newest-first; last write wins
            try:
                asset = str(tr["asset"])
                t_ms = int(tr["timestamp"]) * 1000
                price = float(tr["price"])
                size = float(tr.get("size") or 0.0)
            except (KeyError, TypeError, ValueError):
                continue
            if first_kline_ms is not None and t_ms >= first_kline_ms:
                continue  # kline covers this period
            if asset == ev["up_token"]:
                merged["UP"][t_ms] = PricePoint(t_ms, price, size)
            elif asset == ev["down_token"]:
                merged["DOWN"][t_ms] = PricePoint(t_ms, price, size)
        up_pts = sorted(merged["UP"].values(), key=lambda x: x.time_ms)
        down_pts = sorted(merged["DOWN"].values(), key=lambda x: x.time_ms)
        kline_ok = len(up_pts) >= 5 and len(down_pts) >= 5
        upsert_window(conn, ts, ev, kline_ok)
        # Refresh replaces the window's series; drop the old rows first so
        # a changed merge rule cannot leave stale points behind.
        conn.execute("DELETE FROM prices WHERE start_ts=?", (int(ts),))
        if up_pts:
            upsert_prices(conn, ts, "UP", up_pts)
        if down_pts:
            upsert_prices(conn, ts, "DOWN", down_pts)
        conn.commit()
        ok += 1
        if progress:
            progress({"phase": "collecting", "done": i, "total": len(windows),
                      "ok": ok, "skipped": skipped, "failed": failed,
                      "requests": f.requests})
        if i % 25 == 0 or i == len(windows):
            rate = i / max(time.time() - t0, 1e-6)
            print(
                f"  {i}/{len(windows)} ok={ok} skip={skipped} fail={failed} "
                f"requests={f.requests} {rate:.1f} win/s"
            )
    elapsed = time.time() - t0
    print(f"collect done: {elapsed:.1f}s  ok={ok} skip={skipped} fail={failed}")
    if progress:
        progress({"phase": "collecting", "done": len(windows),
                  "total": len(windows), "ok": ok, "skipped": skipped,
                  "failed": failed, "requests": f.requests})
    conn.close()
    return {"ok": ok, "skipped": skipped, "failed": failed,
            "elapsed_s": elapsed, "windows": len(windows),
            "requests": f.requests}


def coverage(db_path: str | Path) -> dict:
    conn = connect(db_path)
    row = conn.execute(
        "SELECT COUNT(*) AS n, MIN(start_ts) AS lo, MAX(start_ts) AS hi, "
        "SUM(CASE WHEN up_won IS NOT NULL THEN 1 ELSE 0 END) AS resolved "
        "FROM windows WHERE kline_ok=1"
    ).fetchone()
    last = conn.execute(
        "SELECT MAX(fetched_at) AS t FROM windows"
    ).fetchone()
    conn.close()
    return {
        "windows": int(row["n"] or 0),
        "resolved": int(row["resolved"] or 0),
        "earliest": int(row["lo"]) if row["lo"] else None,
        "latest": int(row["hi"]) if row["hi"] else None,
        "last_fetched_at": last["t"] if last else None,
    }


def load_window(conn: sqlite3.Connection, start_ts: int) -> WindowData | None:
    row = conn.execute(
        "SELECT start_ts, slug, up_won, kline_ok FROM windows WHERE start_ts=?",
        (int(start_ts),),
    ).fetchone()
    if row is None or not row["kline_ok"]:
        return None
    up = [
        PricePoint(r["time_ms"], r["price"], r["volume"] or 0.0)
        for r in conn.execute(
            "SELECT time_ms, price, volume FROM prices "
            "WHERE start_ts=? AND side='UP' ORDER BY time_ms",
            (int(start_ts),),
        )
    ]
    down = [
        PricePoint(r["time_ms"], r["price"], r["volume"] or 0.0)
        for r in conn.execute(
            "SELECT time_ms, price, volume FROM prices "
            "WHERE start_ts=? AND side='DOWN' ORDER BY time_ms",
            (int(start_ts),),
        )
    ]
    if not up or not down:
        return None
    start_ms = int(start_ts) * 1000
    btc = [
        BtcBar(r["time_ms"], r["open"], r["high"], r["low"], r["close"])
        for r in conn.execute(
            "SELECT time_ms, open, high, low, close FROM btc_1m "
            "WHERE time_ms BETWEEN ? AND ? ORDER BY time_ms",
            (start_ms - 180_000, start_ms + 900_000 + 180_000),
        )
    ]
    up_won = None if row["up_won"] is None else bool(row["up_won"])
    return WindowData(
        start_ts=int(start_ts), slug=row["slug"], up_won=up_won,
        up=up, down=down, btc=btc,
    )


def iter_windows(conn: sqlite3.Connection, start_ts: int, end_ts: int):
    rows = conn.execute(
        "SELECT start_ts FROM windows "
        "WHERE start_ts >= ? AND start_ts < ? AND kline_ok=1 "
        "AND up_won IS NOT NULL ORDER BY start_ts",
        (int(start_ts), int(end_ts)),
    ).fetchall()
    for r in rows:
        yield int(r["start_ts"])


def window_count(conn: sqlite3.Connection, start_ts: int, end_ts: int) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM windows WHERE start_ts >= ? AND start_ts < ? "
        "AND kline_ok=1 AND up_won IS NOT NULL",
        (int(start_ts), int(end_ts)),
    ).fetchone()
    return int(row["n"])
