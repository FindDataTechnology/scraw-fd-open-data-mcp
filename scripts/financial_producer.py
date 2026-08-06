"""Producer: Read tasks from database and push to Redis queue."""
import os
import json
import redis
from sqlalchemy import create_engine, text

REDIS_URL = os.environ.get("REDIS_URL", "redis://fd-open-redis.scraw:6379/0")
DB_URL = os.environ.get(
    "FD_OPEN_DATA_MCP_DATABASE_URL",
    "postgresql+psycopg2://postgres:admin123@fd-open-pg.scraw:5432/postgres"
)
QUEUE_KEY = "financial_crawl:tasks"
PROGRESS_KEY = "financial_crawl:progress"

def main():
    print("=== Financial Crawl Producer ===")
    print(f"Redis: {REDIS_URL}")
    print(f"DB: {DB_URL}")

    # Connect to Redis
    r = redis.from_url(REDIS_URL)
    r.delete(QUEUE_KEY)  # Clear old queue
    r.delete(PROGRESS_KEY)

    # Connect to DB
    engine = create_engine(DB_URL)

    # Get all stocks
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT entity_id, identifier
            FROM entity_source_identifiers
            WHERE source = 'akshare' AND entity_type = 'stock'
            AND identifier ~ '^[0-9]{6}$'
            ORDER BY entity_id
        """))
        stocks = [(row[0], row[1]) for row in result.fetchall()]

    print(f"Total stocks: {len(stocks)}")

    # Functions to extract
    functions = [
        # akshare (annual + quarterly)
        ("akshare", "annual_bs", "balance_sheet_by_yearly_em"),
        ("akshare", "annual_is", "profit_sheet_by_yearly_em"),
        ("akshare", "annual_cf", "cash_flow_sheet_by_yearly_em"),
        ("akshare", "quarterly_bs", "balance_sheet_by_report_em"),
        ("akshare", "quarterly_is", "profit_sheet_by_report_em"),
        ("akshare", "quarterly_cf", "cash_flow_sheet_by_report_em"),
        # yfinance (annual + quarterly)
        ("yfinance", "annual_bs", "ticker_balance_sheet"),
        ("yfinance", "annual_is", "ticker_financials"),
        ("yfinance", "annual_cf", "ticker_cashflow"),
        ("yfinance", "quarterly_bs", "quarterly_balance_sheet"),
        ("yfinance", "quarterly_is", "quarterly_financials"),
        ("yfinance", "quarterly_cf", "quarterly_cashflow"),
    ]

    # Push tasks to queue
    tasks_pushed = 0
    for entity_id, identifier in stocks:
        for source, func_name, func_command in functions:
            task = {
                "entity_id": entity_id,
                "identifier": identifier,
                "source": source,
                "function_name": func_name,
                "function_command": func_command,
            }
            r.rpush(QUEUE_KEY, json.dumps(task))
            tasks_pushed += 1

    r.set(PROGRESS_KEY, f"queued:{tasks_pushed}")
    print(f"✓ Pushed {tasks_pushed} tasks to queue")

if __name__ == "__main__":
    main()
