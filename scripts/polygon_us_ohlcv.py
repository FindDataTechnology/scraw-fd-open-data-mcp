"""Crawl US equity daily OHLCV via Polygon.io -> semantic_observations (master).

Polygon free tier = 5 calls/min. ``get_aggs`` returns the FULL daily history in
ONE call per ticker (limit=50000 bars ≈ 200y), so N tickers = N calls. A curated
universe of ~150 major US tickers takes ~35 min at 13s spacing -- background-safe.

Concepts (entity_type='stock', currency_usd / shares):
  price.close / price.open / price.high / price.low / price.vwap (currency_usd)
  trading_volume (shares)
These are the currency_usd OHLCV concepts (cid 403-407, 273) which currently
have ~0 observations -- the akshare A-share OHLCV lives in separate currency_cny
concepts (cid 233-238), so there is no collision.

entity_id is resolved via ``entities.code == ticker`` (entity_type='stock').
The fd-world US ticker universe already registered rows for major tickers, so
the curated set resolves cleanly; any ticker lacking an entity row is skipped +
logged (never invented -- the registry resync just cleaned it).

Targets the canonical DB: fd-postgres on guangzhou-xinru :30432 (fd_open_data),
reached from the Mac via the SSH tunnel (localhost:30432). Auth: POLYGON_API_KEY
read from fd-polygon/.env (never hardcoded); a missing key aborts before any
network call.

Resumable: each ticker's bars are upserted immediately, so a kill mid-run keeps
all prior progress. Per-ticker result is logged.
"""
from __future__ import annotations

import datetime
import logging
import os
import sys
import time

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("polygon")

ENTITY_TYPE = "stock"
SOURCE = "polygon"
GRANULARITY = "day"
START = "2015-01-01"
SPACING = 13          # seconds between calls -> ~4.6/min (under 5/min free tier)
RETRY_429 = 90        # backoff on rate-limit
RETRIES = 3

# Canonical store on guangzhou-xinru :30432; real password lives in
# guangzhou-xinru:/etc/fd-open-data/db-credentials.env. LAN 192.168.1.4:5433
# retired read-only 2026-08-18.
DB_DEFAULT = "postgresql+psycopg2://fd:FD_PG_PASSWORD@localhost:30432/fd_open_data"
ENV_FILE = "/Users/chengsishi/finddata/fd-polygon/.env"

# column -> (concept_code, unit). Polygon aggs columns are clean English names.
COL_CONCEPT = {
    "close":  ("price.close", "currency_usd"),
    "open":   ("price.open", "currency_usd"),
    "high":   ("price.high", "currency_usd"),
    "low":    ("price.low", "currency_usd"),
    "vwap":   ("price.vwap", "currency_usd"),
    "volume": ("trading_volume", "shares"),
}

# Curated high-value US ticker universe (~150): mega-cap tech, S&P 100 core,
# major financials / healthcare / energy / consumer / industrials, + key ETFs.
# Share-class tickers (BRK.B, BF.B) use polygon's dot form; if the entity row
# uses a different code they are simply skipped + logged.
TICKERS = [
    # mega-cap tech / comm
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "AVGO",
    "ORCL", "ADBE", "CRM", "CSCO", "INTC", "AMD", "QCOM", "TXN", "AMAT",
    "MU", "NVDA", "IBM", "NOW", "INTU", "UBER", "SHOP", "SNOW", "PLTR",
    "NFLX", "DIS", "CMCSA", "T", "VZ", "TMUS",
    # large cap / dow core
    "LLY", "JPM", "V", "UNH", "WMT", "MA", "PG", "JNJ", "HD", "COST",
    "BAC", "KO", "PEP", "MRK", "ABBV", "CVX", "MCD", "ACN", "WFC", "TMO",
    "ABT", "DHR", "LIN", "AXP", "MS", "GS", "BLK", "SCHW", "C", "MET",
    "BMY", "PFE", "MRNA", "GILD", "AMGN", "ELI", "MDT", "ISRG", "VRTX",
    # financials / insurance
    "BRK.B", "VZ", "USB", "PNC", "COF", "AIG", "ALL", "TRV", "CB", "SPGI",
    "MCO", "BK", "TFC", "STT", "PRU", "AON", "MMC", "KMB",
    # consumer / retail / staples
    "NKE", "SBUX", "LOW", "TGT", "F", "GM", "TSLA", "RIVN", "LCID",
    "UPS", "FDX", "EBAY", "BKNG", "ABNB", "PYPL", "SBUX", "MNST", "KHC",
    "CL", "MDLZ", "MO", "PM", "WBA", "TJX", "ROST",
    # energy
    "XOM", "CVX", "COP", "SLB", "EOG", "PSX", "MPC", "VLO", "OXY", "PXD",
    # industrials / materials / defense
    "BA", "CAT", "GE", "HON", "UPS", "RTX", "LMT", "NOC", "DE", "MMM",
    "EMR", "ETN", "ITW", "FCX", "NEM",
    # healthcare / pharma
    "ELV", "HUM", "CI", "DOW", "ZTS", "BAX", "BDX", "SYK", "BSX", "HCA",
    # ETFs (broad market + sector)
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO", "VEA", "VWO", "AGG", "BND",
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE",
    "SMH", "XME", "KWEB", "EEM", "EFA", "TLT", "GLD", "SLV", "USO",
    # other large / popular
    "PYPL", "SQ", "SHOP", "SNAP", "PINS", "ROKU", "ZM", "DOCU", "WDAY",
    "TEAM", "MDB", "NET", "CRWD", "ZS", "DDOG", "OKTA", "TTD", "RBLX",
]


def _fmt(v) -> str | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN
        return None
    s = repr(f)
    if s.endswith(".0"):
        s = s[:-2]
    return s


def _load_key() -> str:
    """Read POLYGON_API_KEY from env or fd-polygon/.env."""
    key = os.environ.get("POLYGON_API_KEY")
    if key:
        return key
    if os.path.exists(ENV_FILE):
        for line in open(ENV_FILE):
            line = line.strip()
            if line.startswith("POLYGON_API_KEY="):
                key = line.split("=", 1)[1].strip()
                if key:
                    return key
    return ""


def _ts_to_date(ts) -> str:
    if ts is None:
        return ""
    try:
        return datetime.datetime.fromtimestamp(
            ts / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError):
        return ""


def fetch_aggs(client, ticker, start, end):
    """get_aggs with 429 backoff. Returns list of Agg."""
    last = None
    for attempt in range(RETRIES):
        try:
            return client.get_aggs(
                ticker, 1, "day", start, end,
                adjusted=True, sort="asc", limit=50000)
        except Exception as e:  # noqa: BLE001
            last = e
            msg = str(e).lower()
            if "429" in msg or "rate" in msg:
                log.warning("  %s: 429 rate-limited, backing off %ds (try %d/%d)",
                            ticker, RETRY_429, attempt + 1, RETRIES)
                time.sleep(RETRY_429)
            else:
                break
    raise last


BATCH = 2000


def upsert(engine, rows: list[dict]) -> int:
    if not rows:
        return 0
    cols = ["concept_id", "entity_type", "entity_id", "date", "value",
            "unit", "source_used", "fetched_at", "granularity"]
    upd = ("value=EXCLUDED.value, unit=EXCLUDED.unit, "
           "source_used=EXCLUDED.source_used, fetched_at=EXCLUDED.fetched_at")
    sql = (f"INSERT INTO semantic_observations ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT (concept_id, entity_type, entity_id, date, granularity) "
           f"DO UPDATE SET {upd}")
    vals = [tuple(r[c] for c in cols) for r in rows]
    total = 0
    for bi in range(0, len(vals), BATCH):
        chunk = vals[bi:bi + BATCH]
        last = None
        for attempt in range(5):
            try:
                with engine.begin() as conn:
                    cur = conn.connection.driver_connection.cursor()
                    try:
                        execute_values(cur, sql, chunk, page_size=500)
                    finally:
                        cur.close()
                total += len(chunk)
                break
            except Exception as e:  # noqa: BLE001
                last = e
                log.warning("  upsert batch %d attempt %d failed: %s",
                            bi // BATCH + 1, attempt + 1, e)
                engine.dispose()
                time.sleep(2 * (attempt + 1))
        else:
            raise last
    return total


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", default=DB_DEFAULT)
    ap.add_argument("--start", default=START)
    ap.add_argument("--end", default=None,
                    help="range end YYYY-MM-DD (default: today)")
    ap.add_argument("--delay", type=float, default=SPACING)
    ap.add_argument("--limit", type=int, default=0,
                    help="cap N tickers (0 = all, for testing)")
    args = ap.parse_args()

    key = _load_key()
    if not key:
        log.error("POLYGON_API_KEY not set and not in %s -- aborting", ENV_FILE)
        return 1
    os.environ["POLYGON_API_KEY"] = key
    from polygon import RESTClient
    client = RESTClient(api_key=key)

    end = args.end or datetime.date.today().isoformat()
    log.info("polygon crawl: %s..%s, delay=%.0fs, db=master",
             args.start, end, args.delay)

    engine = create_engine(args.db_url, connect_args={"connect_timeout": 10},
                           pool_pre_ping=True, pool_recycle=300)

    # resolve ticker -> entity_id (entity_type='stock', code=ticker)
    tickers = list(dict.fromkeys(TICKERS))  # dedup, preserve order
    if args.limit:
        tickers = tickers[:args.limit]
    with engine.connect() as c:
        rows = c.execute(text(
            "SELECT id, code FROM entities WHERE entity_type='stock' "
            "AND code = ANY(:codes)"), {"codes": tickers}).all()
    ticker_eid = {r[1]: r[0] for r in rows}
    missing = [t for t in tickers if t not in ticker_eid]
    log.info("ticker universe: %d requested, %d resolved, %d missing",
             len(tickers), len(ticker_eid), len(missing))
    if missing:
        log.info("  missing (skipped): %s", ", ".join(missing[:40]))

    # resolve (concept_code, unit) -> concept_id
    concept_codes = sorted({cc for cc, _ in COL_CONCEPT.values()})
    with engine.connect() as c:
        rows = c.execute(text(
            "SELECT id, code, unit FROM concepts WHERE entity_type=:et "
            "AND code = ANY(:codes)"), {"et": ENTITY_TYPE, "codes": concept_codes}).all()
    concept_map = {(r[1], r[2]): r[0] for r in rows}
    log.info("concepts resolved: %d", len(concept_map))
    for col, (cc, unit) in COL_CONCEPT.items():
        if (cc, unit) not in concept_map:
            log.warning("  MISSING concept: col=%s code=%s unit=%s", col, cc, unit)

    now = datetime.datetime.now(datetime.timezone.utc)
    grand = 0
    done = 0
    for i, ticker in enumerate(tickers, 1):
        eid = ticker_eid.get(ticker)
        if eid is None:
            continue
        t0 = time.time()
        try:
            aggs = fetch_aggs(client, ticker, args.start, end)
        except Exception as e:  # noqa: BLE001
            log.warning("[%3d/%d] %-6s FETCH FAILED (%.1fs): %s",
                        i, len(tickers), ticker, time.time() - t0, e)
            time.sleep(args.delay)
            continue
        results = getattr(aggs, "results", aggs) or []
        rows = []
        for a in results:
            d = _ts_to_date(getattr(a, "timestamp", None))
            if not d:
                continue
            for col, (cc, unit) in COL_CONCEPT.items():
                cid = concept_map.get((cc, unit))
                if cid is None:
                    continue
                val = _fmt(getattr(a, col, None))
                if val is None:
                    continue
                rows.append({"concept_id": cid, "entity_type": ENTITY_TYPE,
                             "entity_id": eid, "date": d, "value": val,
                             "unit": unit, "source_used": SOURCE,
                             "fetched_at": now, "granularity": GRANULARITY})
        n = upsert(engine, rows)
        grand += n
        done += 1
        log.info("[%3d/%d] %-6s eid=%-6d bars=%-5d -> %d obs (%.1fs) [cum %d]",
                 i, len(tickers), ticker, eid, len(results), n,
                 time.time() - t0, grand)
        if i < len(tickers):
            time.sleep(args.delay)

    log.info("=== done: %d tickers, %d total polygon observations upserted ===",
             done, grand)
    return 0


if __name__ == "__main__":
    sys.exit(main())
