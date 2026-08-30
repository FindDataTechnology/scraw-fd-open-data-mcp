"""Backfill China macro indicators via akshare macro_china_* functions.

Small data (~10k rows across ~13 functions) -> runs locally against the
in-cluster DB port-forward in under a minute. No k8s job needed.

All series are entity_type='country', entity_id = China (CN). The unique key
(concept_id, entity_type, entity_id, date, granularity) upserts cleanly against
any prior macro obs.

Three akshare DataFrame shapes are handled:

  Shape A (报告型):  [商品, 日期, 今值, 预测值, 前值]
    - 日期 is mixed: pre-~2015 rows are period-dated (YYYY-MM-01), recent rows
      are announcement-dated (YYYY-MM-DD e.g. 2025-08-09). To avoid duplicating
      the period-dated recent obs already in the DB, only rows whose date day
      component == 01 are ingested (clean period-dated history backfill).
  Shape B (NBS型):   [月份, 当月, 同比增长, 环比增长, 累计, ...]  -> period monthly
  Shape C (PMI合并): [月份, 制造业-指数, ..., 非制造业-指数, ...]  -> period monthly
  Shape D (LPR型):   [TRADE_DATE, LPR1Y, LPR5Y, RATE_1, RATE_2]   -> normalize to month-start
  Shape E (GDP分产业): [季度, 国内生产总值-绝对值, 国内生产总值-同比增长, ...] -> quarter-end
  Shape F (unemployment, long): [date, item, value] -> filter item, YYYYMM -> YYYY-MM-01
"""
from __future__ import annotations

import datetime
import logging
import os
import re
import signal
import socket
import sys
import time

# akshare calls eastmoney directly -> must not leak the macOS system proxy
for _v in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(_v, None)

from sqlalchemy import create_engine, text
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("macro")

ENTITY_TYPE = "country"
COUNTRY_CODE = "CN"
SOURCE = "akshare"
SOCKET_TIMEOUT = 60
FETCH_ALARM = 120  # SIGALRM wall-clock backstop per fetch (eastmoney trickle-hang guard)

# --------------------------------------------------------------------------
# Per-function extraction configs.
#   shape: A | B | C | D | E | F
#   maps:  {value_column: concept_code}   (which DataFrame col -> which concept)
# --------------------------------------------------------------------------
FUNCS = [
    # --- Shape A: period-dated history backfill (day==01 rows only) ---
    {"name": "macro_china_cpi_yearly", "shape": "A",
     "maps": {"今值": "CPI_YOY"}},
    {"name": "macro_china_cpi_monthly", "shape": "A",
     "maps": {"今值": "CPI_MOM"}},
    {"name": "macro_china_ppi_yearly", "shape": "A",
     "maps": {"今值": "PPI_YOY"}},
    {"name": "macro_china_trade_balance", "shape": "A",
     "maps": {"今值": "TRADE_BALANCE"}},
    {"name": "macro_china_exports_yoy", "shape": "A",
     "maps": {"今值": "EXPORT_YOY"}},
    {"name": "macro_china_imports_yoy", "shape": "A",
     "maps": {"今值": "IMPORT_YOY"}},

    # --- Shape C: PMI (period monthly, 月份) ---
    {"name": "macro_china_pmi", "shape": "C",
     "maps": {"制造业-指数": "PMI_MFG", "非制造业-指数": "PMI_NONMFG"}},

    # --- Shape E: GDP (quarter-end, 季度) ---
    {"name": "macro_china_gdp", "shape": "E",
     "maps": {"国内生产总值-绝对值": "GDP_AMOUNT",
              "国内生产总值-同比增长": "GDP_YOY"}},

    # --- Shape D: LPR (normalize TRADE_DATE -> month-start, last per month) ---
    {"name": "macro_china_lpr", "shape": "D",
     "maps": {"LPR1Y": "LPR_1Y", "LPR5Y": "LPR_5Y"}},

    # --- Shape B: NBS monthly (月份) ---
    {"name": "macro_china_consumer_goods_retail", "shape": "B",
     "maps": {"当月": "RETAIL_MONTHLY", "累计": "RETAIL_CUMULATIVE",
              "同比增长": "RETAIL_YOY"}},
    {"name": "macro_china_fdi", "shape": "B",
     "maps": {"当月": "FDI_AMOUNT"}},
    {"name": "macro_china_gdzctz", "shape": "B",
     "maps": {"当月": "FAI_MONTHLY", "自年初累计": "FAI_CUMULATIVE"}},

    # --- Shape F: unemployment (long format, filter item) ---
    {"name": "macro_china_urban_unemployment", "shape": "F",
     "maps": {"value": "UNEMPLOYMENT_CN"}, "item_filter": "全国城镇调查失业率"},
]


# --------------------------------------------------------------------------
def _fetch(fn, *args, **kwargs):
    """Call an akshare fn with a hard SIGALRM timeout."""
    def _handler(signum, frame):  # noqa: ARG001
        raise TimeoutError(f"{fn.__name__} exceeded {FETCH_ALARM}s")
    old = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(FETCH_ALARM)
    try:
        return fn(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)


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


_MONTH_RE = re.compile(r"(\d{4})年(\d{1,2})月份?")
_QUARTER_RE = re.compile(r"(\d{4})年第(\d)(?:-(\d))?季度")


def _norm_month(s: str) -> str | None:
    """'2026年07月份' / '2026年7月' -> '2026-07-01'."""
    m = _MONTH_RE.search(str(s))
    if not m:
        return None
    return f"{m.group(1)}-{int(m.group(2)):02d}-01"


def _norm_quarter(s: str) -> str | None:
    """'2026年第1-2季度' -> '2026-06-30'; '2025年第1-4季度' -> '2025-12-31'."""
    m = _QUARTER_RE.search(str(s))
    if not m:
        return None
    year = int(m.group(1))
    q_end = int(m.group(3)) if m.group(3) else int(m.group(2))
    month = q_end * 3
    day = {3: 31, 6: 30, 9: 30, 12: 31}[month]
    return f"{year}-{month:02d}-{day:02d}"


def _norm_ym(s: str) -> str | None:
    """'201801' -> '2018-01-01'."""
    s = str(s).strip()
    if len(s) == 6 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-01"
    return None


def _month_start(s: str) -> str | None:
    """'2026-07-20' -> '2026-07-01' (LPR announcement -> period month)."""
    s = str(s).strip()
    if len(s) >= 7 and s[:4].isdigit():
        return f"{s[:4]}-{s[5:7]}-01"
    return None


# --------------------------------------------------------------------------
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
                log.warning("upsert batch %d attempt %d failed: %s",
                            bi // BATCH + 1, attempt + 1, e)
                engine.dispose()
                time.sleep(2 * (attempt + 1))
        else:
            raise last
    return total


def extract(df, cfg, concepts, entity_id, now) -> list[dict]:
    """Extract obs rows from one DataFrame per its shape config."""
    if df is None or df.empty:
        return []
    rows = []
    shape = cfg["shape"]
    maps = cfg["maps"]

    if shape == "A":
        # date col 日期, value col per map. Only day==01 rows (period history).
        if "日期" not in df.columns:
            return []
        for _, r in df.iterrows():
            d = str(r.get("日期", "")).strip()
            # period-dated rows look like YYYY-MM-01
            if len(d) < 10 or d[8:10] != "01":
                continue
            obs_date = d
            for col, ccode in maps.items():
                meta = concepts.get(ccode)
                if meta is None or col not in df.columns:
                    continue
                val = _fmt(r.get(col))
                if val is None:
                    continue
                rows.append({"concept_id": meta["id"], "entity_type": ENTITY_TYPE,
                             "entity_id": entity_id, "date": obs_date, "value": val,
                             "unit": meta["unit"], "source_used": SOURCE,
                             "fetched_at": now, "granularity": "day"})

    elif shape in ("B", "C"):
        # date col 月份 -> YYYY-MM-01
        if "月份" not in df.columns:
            return []
        for _, r in df.iterrows():
            obs_date = _norm_month(r.get("月份"))
            if obs_date is None:
                continue
            for col, ccode in maps.items():
                meta = concepts.get(ccode)
                if meta is None or col not in df.columns:
                    continue
                val = _fmt(r.get(col))
                if val is None:
                    continue
                rows.append({"concept_id": meta["id"], "entity_type": ENTITY_TYPE,
                             "entity_id": entity_id, "date": obs_date, "value": val,
                             "unit": meta["unit"], "source_used": SOURCE,
                             "fetched_at": now, "granularity": "day"})

    elif shape == "D":
        # LPR: TRADE_DATE -> month-start, keep LAST row per month (latest quote)
        if "TRADE_DATE" not in df.columns:
            return []
        latest = {}  # month -> {col: val}
        for _, r in df.iterrows():
            ms = _month_start(r.get("TRADE_DATE"))
            if ms is None:
                continue
            cur = latest.setdefault(ms, {})
            for col in maps:
                if col not in df.columns:
                    continue
                val = _fmt(r.get(col))
                if val is not None:
                    cur[col] = val  # last non-null wins
        for obs_date, colvals in latest.items():
            for col, ccode in maps.items():
                meta = concepts.get(ccode)
                if meta is None:
                    continue
                val = colvals.get(col)
                if val is None:
                    continue
                rows.append({"concept_id": meta["id"], "entity_type": ENTITY_TYPE,
                             "entity_id": entity_id, "date": obs_date, "value": val,
                             "unit": meta["unit"], "source_used": SOURCE,
                             "fetched_at": now, "granularity": "day"})

    elif shape == "E":
        # GDP: 季度 -> quarter-end date
        if "季度" not in df.columns:
            return []
        for _, r in df.iterrows():
            obs_date = _norm_quarter(r.get("季度"))
            if obs_date is None:
                continue
            for col, ccode in maps.items():
                meta = concepts.get(ccode)
                if meta is None or col not in df.columns:
                    continue
                val = _fmt(r.get(col))
                if val is None:
                    continue
                rows.append({"concept_id": meta["id"], "entity_type": ENTITY_TYPE,
                             "entity_id": entity_id, "date": obs_date, "value": val,
                             "unit": meta["unit"], "source_used": SOURCE,
                             "fetched_at": now, "granularity": "day"})

    elif shape == "F":
        # long: date(YYYYMM), item, value. filter item.
        if "date" not in df.columns or "value" not in df.columns:
            return []
        item_filter = cfg.get("item_filter")
        for _, r in df.iterrows():
            if item_filter and str(r.get("item", "")).strip() != item_filter:
                continue
            obs_date = _norm_ym(r.get("date"))
            if obs_date is None:
                continue
            for col, ccode in maps.items():
                meta = concepts.get(ccode)
                if meta is None:
                    continue
                val = _fmt(r.get(col))
                if val is None:
                    continue
                rows.append({"concept_id": meta["id"], "entity_type": ENTITY_TYPE,
                             "entity_id": entity_id, "date": obs_date, "value": val,
                             "unit": meta["unit"], "source_used": SOURCE,
                             "fetched_at": now, "granularity": "day"})
    return rows


def main() -> int:
    import argparse
    import akshare as ak
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url",
                    default="postgresql+psycopg2://postgres:admin123@127.0.0.1:55432/postgres")
    args = ap.parse_args()

    socket.setdefaulttimeout(SOCKET_TIMEOUT)
    engine = create_engine(args.db_url, connect_args={"connect_timeout": 10},
                           pool_pre_ping=True, pool_recycle=300)

    with engine.connect() as c:
        row = c.execute(text(
            "SELECT id FROM entities WHERE entity_type='country' AND code=:code"
        ), {"code": COUNTRY_CODE}).first()
    if row is None:
        log.error("China (CN) country entity not found — aborting")
        return 1
    cn_id = row[0]
    log.info("China entity_id=%d", cn_id)

    all_codes = sorted({cc for f in FUNCS for cc in f["maps"].values()})
    with engine.connect() as c:
        rows = c.execute(text(
            "SELECT id, code, unit FROM concepts WHERE entity_type=:et AND code = ANY(:codes)"
        ), {"et": ENTITY_TYPE, "codes": all_codes}).all()
    concepts = {r[1]: {"id": r[0], "unit": r[2]} for r in rows}
    log.info("concepts resolved: %d/%d", len(concepts), len(all_codes))
    missing = [c for c in all_codes if c not in concepts]
    if missing:
        log.warning("MISSING concepts: %s", missing)

    now = datetime.datetime.now(datetime.timezone.utc)
    grand = 0
    for cfg in FUNCS:
        fn = getattr(ak, cfg["name"], None)
        if fn is None:
            log.warning("%s: function not found", cfg["name"])
            continue
        t0 = time.time()
        try:
            df = _fetch(fn)
        except Exception as e:  # noqa: BLE001
            log.warning("%s FETCH FAILED (%.1fs): %s", cfg["name"], time.time() - t0, e)
            continue
        nrows = 0 if df is None else len(df)
        rows = extract(df, cfg, concepts, cn_id, now)
        n = upsert(engine, rows)
        grand += n
        log.info("%-40s shape=%s rows=%d -> %d obs (%.1fs) [cum %d]",
                 cfg["name"], cfg["shape"], nrows, n, time.time() - t0, grand)
        time.sleep(0.3)

    log.info("=== done: %d total macro observations upserted ===", grand)
    return 0


if __name__ == "__main__":
    sys.exit(main())
