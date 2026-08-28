#!/usr/bin/env python3
"""Re-sync entity registry: resolve the stock entity_id collision.

PROBLEM
-------
~6967 A-share stock entity_ids in semantic_observations have no matching
entities row of entity_type='stock'. Their ids collide with other entity
types occupying the same global entities.id slot:

    exchange (3 rows, ids 1-3)   : 0 obs, 0 rels, 0 identifiers  -> orphaned
    person  (4280 rows, 5394-9673): 0 obs, but hold all 696 graph rels
    fund    (4 of 1049 rows)      : 63/68/5369/5370 have REAL fund obs

semantic_observations and entity_source_identifiers have NO foreign key to
entities(id) -- only entity_relationships does (ON DELETE CASCADE). So entity
rows can be inserted/deleted freely without touching the 104.8M observations.

FIX (single transaction; aborts entirely on any error; non-destructive to obs)
  1. MOVE the 4128 obstructing person entities to fresh ids (20000+),
     preserving their relationships + identifiers (snapshot rels, delete,
     re-id, re-insert rels with remapped endpoints).
  2. DELETE the 3 orphaned exchange entities.
  3. REASSIGN stock obs at the 4 fund-conflict eids (63/68/5369/5370) to
     fresh ids (25000-25003) so the fund entities + their fund obs stay intact.
  4. INSERT stock entity rows for every stock-obs eid lacking a stock row,
     named from /tmp/ashare_names.csv (akshare code -> name_zh).
  5. setval(entities_id_seq, max(id)).
  6. VERIFY: 0 stock-obs eids lack a stock entity row.
"""
from __future__ import annotations

import csv
import sys

import psycopg2
from psycopg2.extras import Json, execute_values

DB = dict(host="127.0.0.1", port=55432, dbname="postgres",
          user="postgres", password="admin123")
NAMES_CSV = "/tmp/ashare_names.csv"

# Fresh id ranges. entity_source_identifiers has orphaned rows up to eid
# 136764 (no matching entities row), so uq_entity_source(entity_type,entity_id,
# source) can fire on ANY target id <= 136764. Pick targets well above that.
PERSON_BASE = 200000         # persons moved to 200000 + offset (4128 rows)
STOCK_REASSIGN_BASE = 300000  # stock obs at fund-collision eids moved here


def log(msg, *a):
    print(msg % a if a else msg, flush=True)


def main() -> int:
    conn = psycopg2.connect(**DB)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        # --- stock obs eid set ---
        cur.execute(
            "SELECT DISTINCT entity_id FROM semantic_observations "
            "WHERE entity_type='stock' ORDER BY 1")
        stock_eids = [r[0] for r in cur.fetchall()]
        log("stock obs eids: %d (range %d..%d)",
            len(stock_eids), stock_eids[0], stock_eids[-1])

        # --------------------------------------------------------------
        # 1. MOVE obstructing person entities (those whose id is a stock eid)
        # --------------------------------------------------------------
        cur.execute(
            "SELECT id FROM entities WHERE entity_type='person' "
            "AND id = ANY(%s) ORDER BY 1", (stock_eids,))
        person_ids = [r[0] for r in cur.fetchall()]
        log("persons to move (obstructing stock eids): %d", len(person_ids))
        person_map = {old: PERSON_BASE + i for i, old in enumerate(person_ids)}

        # 1a. snapshot relationships touching these persons (all fund->person)
        cur.execute(
            "SELECT source_id, relation_type, target_id, valid_from, "
            "valid_to, metadata_json FROM entity_relationships "
            "WHERE source_id = ANY(%s) OR target_id = ANY(%s)",
            (person_ids, person_ids))
        rels = cur.fetchall()
        log("  rels snapshot: %d", len(rels))

        # 1b. delete those rels so person ids have no FK references
        cur.execute(
            "DELETE FROM entity_relationships "
            "WHERE source_id = ANY(%s) OR target_id = ANY(%s)",
            (person_ids, person_ids))
        log("  rels deleted: %d", cur.rowcount)

        # 1c. move person entities (id PK; no FK refs remain)
        cur.execute(
            "CREATE TEMP TABLE person_remap(old_id int, new_id int) "
            "ON COMMIT DROP")
        # execute_values = ONE round-trip for all 4128 rows (executemany would
        # send 4128 separate INSERTs over the port-forward and hang for minutes).
        execute_values(cur, "INSERT INTO person_remap (old_id, new_id) VALUES %s",
                       list(person_map.items()))
        cur.execute(
            "UPDATE entities SET id = pr.new_id, updated_at = now() "
            "FROM person_remap pr WHERE entities.id = pr.old_id")
        log("  persons re-id'd: %d", cur.rowcount)

        # 1d. move person identifiers (no FK; update in place)
        cur.execute(
            "UPDATE entity_source_identifiers SET entity_id = pr.new_id "
            "FROM person_remap pr "
            "WHERE entity_type='person' AND entity_id = pr.old_id")
        log("  person identifiers re-id'd: %d", cur.rowcount)

        # 1e. re-insert rels with remapped endpoints
        if rels:
            new_rels = []
            for (s, rt, t, vf, vt, mj) in rels:
                # metadata_json fetched back as a dict -> wrap in Json for INSERT
                new_rels.append((person_map.get(s, s), rt,
                                 person_map.get(t, t), vf, vt,
                                 Json(mj) if mj is not None else None))
            # execute_values: one round-trip (executemany hangs over port-forward)
            execute_values(
                cur,
                "INSERT INTO entity_relationships "
                "(source_id, relation_type, target_id, valid_from, "
                "valid_to, metadata_json) VALUES %s",
                new_rels, page_size=200)
            log("  rels re-inserted: %d", len(new_rels))

        # --------------------------------------------------------------
        # 2. DELETE orphaned exchange entities (0 obs, 0 rels, 0 idents)
        # --------------------------------------------------------------
        cur.execute("DELETE FROM entities WHERE entity_type='exchange'")
        log("exchange entities deleted: %d", cur.rowcount)

        # --------------------------------------------------------------
        # 3. REASSIGN stock obs at fund-conflict eids
        # --------------------------------------------------------------
        # Fund entities that share an id with a stock obs eid keep their fund
        # obs -- we cannot overwrite them with stock entities. So move the STOCK
        # obs (and stock identifiers) to fresh ids (STOCK_REASSIGN_BASE+) via a
        # temp-table JOIN -- ONE seq-scan of semantic_observations instead of N.
        # New ids are above the max entity_id in entity_source_identifiers
        # (yfinance 1..27795, edgar 9523..27795), and any pre-existing stock
        # identifiers at the new ids are deleted first to avoid
        # uq_entity_source UNIQUE(entity_type,entity_id,source) violations.
        cur.execute(
            "SELECT id FROM entities WHERE entity_type='fund' "
            "AND id = ANY(%s) ORDER BY 1", (stock_eids,))
        fund_conflicts = [r[0] for r in cur.fetchall()]
        stock_reassign = {old: STOCK_REASSIGN_BASE + i
                          for i, old in enumerate(fund_conflicts)}
        log("fund-conflict eids to reassign stock obs: %s", fund_conflicts)
        if fund_conflicts:
            cur.execute(
                "CREATE TEMP TABLE stock_reassign(old_id int, new_id int) "
                "ON COMMIT DROP")
            execute_values(
                cur, "INSERT INTO stock_reassign (old_id, new_id) VALUES %s",
                [(o, stock_reassign[o]) for o in fund_conflicts])
            cur.execute(
                "UPDATE semantic_observations SET entity_id = sr.new_id "
                "FROM stock_reassign sr "
                "WHERE entity_type='stock' AND entity_id = sr.old_id")
            log("  stock obs reassigned: %d rows", cur.rowcount)
            # delete existing stock identifiers at new ids (from other sources)
            cur.execute(
                "DELETE FROM entity_source_identifiers USING stock_reassign sr "
                "WHERE entity_type='stock' AND entity_id = sr.new_id")
            log("  old stock identifiers at new ids deleted: %d rows", cur.rowcount)
            cur.execute(
                "UPDATE entity_source_identifiers SET entity_id = sr.new_id "
                "FROM stock_reassign sr "
                "WHERE entity_type='stock' AND entity_id = sr.old_id")
            log("  stock identifiers reassigned: %d rows", cur.rowcount)

        # refresh stock eid set (now includes reassigned ids)
        cur.execute(
            "SELECT DISTINCT entity_id FROM semantic_observations "
            "WHERE entity_type='stock' ORDER BY 1")
        stock_eids = [r[0] for r in cur.fetchall()]

        # --------------------------------------------------------------
        # 4. INSERT stock entities for eids lacking a stock row
        # --------------------------------------------------------------
        # existing stock entity ids AND codes (skip / avoid collision)
        cur.execute("SELECT id, code FROM entities WHERE entity_type='stock'")
        existing_stock: dict[int, str] = {}
        seen_codes: set[str] = set()
        for eid, code in cur.fetchall():
            existing_stock[eid] = code
            seen_codes.add(code)

        # build eid -> preferred stock code from identifiers (prefer 6-digit)
        cur.execute(
            "SELECT entity_id, source, identifier FROM entity_source_identifiers "
            "WHERE entity_type='stock'")
        eid_code: dict[int, str] = {}
        for eid, src, ident in cur.fetchall():
            ident = str(ident).strip()
            is_real = ident.isdigit() and len(ident) == 6
            cur_code = eid_code.get(eid)
            if cur_code is None:
                eid_code[eid] = ident
            elif is_real and not (cur_code.isdigit() and len(cur_code) == 6):
                eid_code[eid] = ident

        # load akshare code -> name_zh
        names: dict[str, str] = {}
        with open(NAMES_CSV, newline="") as f:
            for row in csv.DictReader(f):
                code = (row.get("code") or "").strip()
                name = (row.get("name") or "").strip()
                if code:
                    names[code] = name

        # NOTE: entity_source_identifiers has 5222 identifier VALUES shared across
        # different eids (mostly bogus sequential indices, but some real tickers
        # duplicated across sources). entities has UNIQUE(entity_type, code), so we
        # must guarantee every inserted code is distinct: first eid keeps the bare
        # ticker; later collisions become "<ticker>#<eid>"; code-less eids get
        # "STOCK_<eid>".
        to_insert = []
        unnamed = 0
        deduped = 0
        for eid in stock_eids:
            if eid in existing_stock:
                continue
            code = eid_code.get(eid)
            name = names.get(code) if code else None
            if not name:
                unnamed += 1
            if not code:
                code = f"STOCK_{eid}"
            if code in seen_codes:
                code = f"{code}#{eid}"
                deduped += 1
            seen_codes.add(code)
            to_insert.append((eid, "stock", code, name, None))

        log("stock entities to insert: %d (unnamed: %d, code-deduped: %d)",
            len(to_insert), unnamed, deduped)
        if to_insert:
            # execute_values = ONE round-trip (executemany would hang on ~7k rows
            # over the port-forward). now() baked into the per-row template.
            execute_values(
                cur,
                "INSERT INTO entities "
                "(id, entity_type, code, name_zh, name_en, updated_at) VALUES %s",
                to_insert, template="(%s,%s,%s,%s,%s, now())", page_size=500)
            log("  inserted: %d", len(to_insert))

        # --------------------------------------------------------------
        # 5. bump sequence
        # --------------------------------------------------------------
        cur.execute("SELECT COALESCE(max(id),0) FROM entities")
        max_id = cur.fetchone()[0]
        cur.execute("SELECT setval('entities_id_seq', %s)", (max_id,))
        log("entities_id_seq setval -> %d", max_id)

        # --------------------------------------------------------------
        # 6. VERIFY
        # --------------------------------------------------------------
        cur.execute(
            "SELECT count(DISTINCT so.entity_id) FROM semantic_observations so "
            "LEFT JOIN entities e ON e.id = so.entity_id "
            "AND e.entity_type = so.entity_type "
            "WHERE so.entity_type='stock' AND e.id IS NULL")
        unresolved = cur.fetchone()[0]
        log("UNRESOLVED stock obs eids (must be 0): %d", unresolved)

        cur.execute(
            "SELECT count(*) FROM entities WHERE entity_type='stock'")
        n_stock = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM entity_relationships")
        n_rels = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM entities WHERE entity_type='person'")
        n_person = cur.fetchone()[0]
        log("POST: stock entities=%d, person entities=%d, relationships=%d",
            n_stock, n_person, n_rels)

        if unresolved != 0:
            log("ABORT: %d unresolved stock eids remain -- rolling back",
                unresolved)
            conn.rollback()
            return 1

        conn.commit()
        log("COMMITTED.")
        return 0
    except Exception as e:  # noqa: BLE001
        log("ERROR: %s -- rolling back", e)
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
