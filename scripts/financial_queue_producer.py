#!/usr/bin/env python
"""Queue producer for financial_crawl spider.

Initializes the Redis queue with all (stock, function) tasks.
Run once before starting the crawl jobs.

Usage:
    python scripts/financial_queue_producer.py \
        --concepts 239,240,241,242,243,244 \
        --entity-type stock \
        --start-date 2024-01-01 \
        --end-date 2026-12-31
"""
import argparse
import json
import redis as redis_lib
from sqlalchemy import create_engine, text


def enqueue_all_tasks(
    concepts: list[int],
    entity_type: str = "stock",
    start_date: str = "2024-01-01",
    end_date: str = "2026-12-31",
    redis_url: str = "redis://fd-open-redis.scraw:6379/0"
):
    """Enqueue all (stock, function) tasks to Redis queue."""

    # Connect to DB and Redis
    db_url = "postgresql+psycopg2://postgres:admin123@fd-open-pg.scraw:5432/postgres"
    engine = create_engine(db_url)
    redis_client = redis_lib.from_url(redis_url)

    QUEUE_KEY = "financial_crawl:start_urls"
    DUPEFILTER_KEY = "financial_crawl:dupefilter"
    PROGRESS_KEY = "financial_crawl:progress"

    # Clear old queue
    redis_client.delete(QUEUE_KEY, DUPEFILTER_KEY, PROGRESS_KEY)

    # Get all stocks
    print(f"Loading stocks for concepts {concepts}, type={entity_type}...")
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT entity_id, identifier
            FROM entity_source_identifiers esi
            WHERE source = 'akshare' AND entity_type = :et
            ORDER BY entity_id
        """), {"et": entity_type})

        stocks = [(row[0], row[1]) for row in result.fetchall()]
        total_stocks = len(stocks)
        print(f"Total stocks: {total_stocks}")

    # Enqueue functions for each stock
    functions_to_enqueue = [
        ("akshare", "annual_bs", "balance_sheet_by_yearly_em"),
        ("akshare", "annual_is", "profit_sheet_by_yearly_em"),
        ("akshare", "annual_cf", "cash_flow_sheet_by_yearly_em"),
        ("akshare", "quarterly_bs", "balance_sheet_by_report_em"),
        ("akshare", "quarterly_is", "profit_sheet_by_report_em"),
        ("akshare", "quarterly_cf", "cash_flow_sheet_by_report_em"),
        ("yfinance", "annual_bs", "ticker_balance_sheet"),
        ("yfinance", "annual_is", "ticker_financials"),
        ("yfinance", "annual_cf", "ticker_cashflow"),
        ("yfinance", "quarterly_bs", "quarterly_balance_sheet"),
        ("yfinance", "quarterly_is", "quarterly_financials"),
        ("yfinance", "quarterly_cf", "quarterly_cashflow"),
    ]

    tasks_enqueued = 0
    for entity_id, identifier in stocks:
        for source, func_name, fn_cmd in functions_to_enqueue:
            task_key = f"{entity_id}:{source}:{func_name}"

            # Dedup against existing tasks
            if not redis_client.sadd(task_key, 1):
                continue  # Already exists

            task_data = {
                "entity_id": entity_id,
                "identifier": identifier,
                "source": source,
                "function_name": func_name,
                "function_command": fn_cmd,
                "concept_ids": concepts,
                "start_date": start_date,
                "end_date": end_date,
            }

            redis_client.rpush(QUEUE_KEY, json.dumps(task_data, ensure_ascii=False, default=str))
            tasks_enqueued += 1

    # Save progress
    redis_client.set(PROGRESS_KEY, f"initialized:{tasks_enqueued}")

    print(f"✓ Enqueued {tasks_enqueued} tasks to Redis queue")
    print(f"  - {total_stocks} stocks × {len(functions_to_enqueue)} functions")

    return tasks_enqueued


def main():
    parser = argparse.ArgumentParser(description="Initialize financial crawl queue")
    parser.add_argument("--concepts", type=str, default="239,240,241,242,243,244",
                       help="Comma-separated concept IDs")
    parser.add_argument("--entity-type", type=str, default="stock",
                       help="Entity type (default: stock)")
    parser.add_argument("--start-date", type=str, default="2024-01-01",
                       help="Start date for filtering")
    parser.add_argument("--end-date", type=str, default="2026-12-31",
                       help="End date for filtering")
    parser.add_argument("--redis-url", type=str, default="redis://fd-open-redis.scraw:6379/0",
                       help="Redis URL")

    args = parser.parse_args()
    concepts = [int(c.strip()) for c in args.concepts.split(",")]

    enqueue_all_tasks(
        concepts=concepts,
        entity_type=args.entity_type,
        start_date=args.start_date,
        end_date=args.end_date,
        redis_url=args.redis_url
    )


if __name__ == "__main__":
    main()
