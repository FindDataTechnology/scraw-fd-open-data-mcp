#!/usr/bin/env python3
"""ONE-SHOT historical script (ran 2026-08-17): re-sync the entity registry on
the then-master LAN DB (192.168.1.4:5433) to resolve stock entity_id collisions.

That LAN DB is RETIRED read-only since 2026-08-18 -- the canonical store is
fd-postgres on guangzhou-xinru :30432 (fd_open_data). Kept as the audit record
of the id-reassignment below; do NOT re-run against anything.

Generalized version of entity_registry_resync.py (which fixed the in-cluster
fd-open-pg copy). The master has a richer collision -- stock obs eids are blocked
by 12 different entity types, not just person/exchange/fund:

  - person blockers (0 obs): MOVE the person entity to 200000+ (preserving its
    relationships + identifiers), freeing the eid for a stock entity. Cheap,
    because persons carry no observations -- nothing in semantic_observations
    moves.
  - other-type blockers (fund/industry/city/organization/company/country/index/
    crypto/future/bond -- may carry their OWN obs): REASSIGN the *stock* obs +
    stock identifiers at those eids to fresh ids (300000+), then insert a stock
    entity at the fresh id. The blocking entity and its own obs are untouched.
  - no-row eids (576): INSERT a stock entity directly.

Non-destructive to observation *values* (only entity_id moves within
semantic_observations). Single transaction; aborts entirely on any error;
verify-gated (commits only if 0 unresolved stock eids). A permanent
stock_reassign_log_20260817 table records old_id->new_id for reversibility.
"""
from __future__ import annotations

import csv
import sys

import psycopg2
from psycopg2.extras import Json, execute_values

# Retired LAN DB (read-only since 2026-08-18); password redacted from the repo.
DB = dict(host="192.168.1.4", port=5433, dbname="postgres",
          user="admin", password="REDACTED")
NAMES_CSV = "/tmp/ashare_names.csv"

# Fresh id ranges. Master max_ident_eid is 27795, so any target <= 27795 can
# trip uq_entity_source(entity_type, entity_id, source). Pick targets far above.
PERSON_BASE = 200000          # persons moved to 200000 + offset
STOCK_REASSIGN_BASE = 300000  # stock obs at non-person blockers moved here


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

        # existing stock entity ids + codes (skip / dedup against these)
        cur.execute("SELECT id, code FROM entities WHERE entity_type='stock'")
        existing_stock: dict[int, str] = {}
        seen_codes: set[str] = set()
        for eid, code in cur.fetchall():
            existing_stock[eid] = code
            seen_codes.add(code)
        log("existing stock entities: %d", len(existing_stock))

        # --------------------------------------------------------------
        # 1. MOVE obstructing person entities (those whose id is a stock eid)
        #    to 200000+. Persons have 0 obs, so this is cheap; their rels +
        #    identifiers move with them.
        # --------------------------------------------------------------
        cur.execute(
            "SELECT id FROM entities WHERE entity_type='person' "
            "AND id = ANY(%s) ORDER BY 1", (stock_eids,))
        person_ids = [r[0] for r in cur.fetchall()]
        log("persons to move (obstructing stock eids): %d", len(person_ids))
        person_map = {old: PERSON_BASE + i for i, old in enumerate(person_ids)}

        # 1a. snapshot relationships touching these persons
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
                new_rels.append((person_map.get(s, s), rt,
                                 person_map.get(t, t), vf, vt,
                                 Json(mj) if mj is not None else None))
            execute_values(
                cur,
                "INSERT INTO entity_relationships "
                "(source_id, relation_type, target_id, valid_from, "
                "valid_to, metadata_json) VALUES %s",
                new_rels, page_size=200)
            log("  rels re-inserted: %d", len(new_rels))

        # --------------------------------------------------------------
        # 2. REASSIGN stock obs at ALL OTHER non-stock blockers -> 300000+
        #    (fund/industry/city/organization/company/country/index/crypto/
        #     future/bond). These may carry their own obs, so we move the
        #    *stock* obs+idents away rather than disturbing the blocker.
        # --------------------------------------------------------------
        cur.execute(
            "SELECT DISTINCT so.entity_id FROM semantic_observations so "
            "JOIN entities e ON e.id = so.entity_id AND e.entity_type <> 'stock' "
            "WHERE so.entity_type='stock' ORDER BY 1")
        conflict_eids = [r[0] for r in cur.fetchall()]
        log("non-person blocking eids (reassign stock obs): %d",
            len(conflict_eids))

        # permanent log for reversibility (not ON COMMIT DROP)
        cur.execute(
            "CREATE TABLE IF NOT EXISTS stock_reassign_log_20260817 "
            "(old_id int, new_id int, moved_at timestamptz default now())")
        cur.execute("TRUNCATE stock_reassign_log_20260817")

        if conflict_eids:
            stock_reassign = {old: STOCK_REASSIGN_BASE + i
                              for i, old in enumerate(conflict_eids)}
            cur.execute(
                "CREATE TEMP TABLE stock_reassign(old_id int, new_id int) "
                "ON COMMIT DROP")
            execute_values(
                cur, "INSERT INTO stock_reassign (old_id, new_id) VALUES %s",
                list(stock_reassign.items()))
            execute_values(
                cur,
                "INSERT INTO stock_reassign_log_20260817 (old_id, new_id) VALUES %s",
                list(stock_reassign.items()))

            # delete pre-existing stock identifiers at new ids (other sources
            # may have orphaned idents up to 27795; 300000+ is clear, but be
            # safe) to avoid uq_entity_source violations.
            cur.execute(
                "DELETE FROM entity_source_identifiers USING stock_reassign sr "
                "WHERE entity_type='stock' AND entity_id = sr.new_id")
            log("  pre-existing stock idents at new ids deleted: %d",
                cur.rowcount)

            # move stock observations (no FK; one JOIN-driven UPDATE)
            cur.execute(
                "UPDATE semantic_observations SET entity_id = sr.new_id "
                "FROM stock_reassign sr "
                "WHERE entity_type='stock' AND entity_id = sr.old_id")
            log("  stock obs reassigned: %d rows", cur.rowcount)

            # move stock identifiers
            cur.execute(
                "UPDATE entity_source_identifiers SET entity_id = sr.new_id "
                "FROM stock_reassign sr "
                "WHERE entity_type='stock' AND entity_id = sr.old_id")
            log("  stock identifiers reassigned: %d rows", cur.rowcount)

        # --------------------------------------------------------------
        # 3. INSERT stock entities for every eid still lacking a stock row
        #    (person-freed eids, the 300000+ new ids, and the no-row eids)
        # --------------------------------------------------------------
        cur.execute(
            "SELECT DISTINCT entity_id FROM semantic_observations "
            "WHERE entity_type='stock' ORDER BY 1")
        stock_eids = [r[0] for r in cur.fetchall()]

        # eid -> preferred stock code from identifiers (prefer 6-digit akshare)
        cur.execute(
            "SELECT entity_id, source, identifier FROM entity_source_identifiers "
            "WHERE entity_type='stock'")
        eid_code: dict[int, str] = {}
        for eid, _src, ident in cur.fetchall():
            ident = str(ident).strip()
            is_real = ident.isdigit() and len(ident) == 6
            cur_code = eid_code.get(eid)
            if cur_code is None or (
                is_real and not (cur_code.isdigit() and len(cur_code) == 6)
            ):
                eid_code[eid] = ident

        # akshare code -> name_zh
        names: dict[str, str] = {}
        with open(NAMES_CSV, newline="") as f:
            for row in csv.DictReader(f):
                code = (row.get("code") or "").strip()
                name = (row.get("name") or "").strip()
                if code:
                    names[code] = name

        # entities has UNIQUE(entity_type, code): guarantee distinct codes.
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
            execute_values(
                cur,
                "INSERT INTO entities "
                "(id, entity_type, code, name_zh, name_en, updated_at) VALUES %s",
                to_insert, template="(%s,%s,%s,%s,%s, now())", page_size=500)
            log("  inserted: %d", len(to_insert))

        # --------------------------------------------------------------
        # 4. bump sequence
        # --------------------------------------------------------------
        cur.execute("SELECT COALESCE(max(id),0) FROM entities")
        max_id = cur.fetchone()[0]
        cur.execute("SELECT setval('entities_id_seq', %s)", (max_id,))
        log("entities_id_seq setval -> %d", max_id)

        # --------------------------------------------------------------
        # 5. VERIFY
        # --------------------------------------------------------------
        cur.execute(
            "SELECT count(DISTINCT so.entity_id) FROM semantic_observations so "
            "LEFT JOIN entities e ON e.id = so.entity_id "
            "AND e.entity_type = so.entity_type "
            "WHERE so.entity_type='stock' AND e.id IS NULL")
        unresolved = cur.fetchone()[0]
        log("UNRESOLVED stock obs eids (must be 0): %d", unresolved)

        cur.execute("SELECT count(*) FROM entities WHERE entity_type='stock'")
        n_stock = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM entity_relationships")
        n_rels = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM entities WHERE entity_type='person'")
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
