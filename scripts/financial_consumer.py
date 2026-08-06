"""Consumer: Pull tasks from Redis queue and execute them."""
import os
import json
import time
import redis
from sqlalchemy import create_engine, text

REDIS_URL = os.environ.get("REDIS_URL", "redis://fd-open-redis.scraw:6379/0")
DB_URL = os.environ.get(
    "FD_OPEN_DATA_MCP_DATABASE_URL",
    "postgresql+psycopg2://postgres:admin123@fd-open-pg.scraw:5432/postgres"
)
QUEUE_KEY = "financial_crawl:tasks"
PROGRESS_KEY = "financial_crawl:progress"
POD_ID = os.environ.get("POD_NAME", "worker-unknown")

def execute_task(task):
    """Execute a single task: call akshare/yfinance and write to DB."""
    from fd_open_data_mcp.fetch.runner import run_upstream

    source = task["source"]
    func_command = task["function_command"]
    identifier = task["identifier"]
    entity_id = task["entity_id"]

    try:
        # Call the data source
        if source == "akshare":
            # akshare functions expect symbol parameter
            params = {"symbol": identifier}
            df = run_upstream(source, func_command, params)
        elif source == "yfinance":
            # yfinance functions expect symbol parameter
            # Convert A-share code to yfinance format
            if identifier.startswith("6"):
                yf_symbol = f"{identifier}.SS"
            else:
                yf_symbol = f"{identifier}.SZ"
            params = {"symbol": yf_symbol}
            df = run_upstream(source, func_command, params)
        else:
            print(f"[{POD_ID}] Unknown source: {source}")
            return 0

        if df is None or len(df) == 0:
            return 0

        # Parse DataFrame and write to DB
        observations = parse_dataframe(source, func_command, df, entity_id)
        write_observations(observations)

        return len(observations)

    except Exception as e:
        print(f"[{POD_ID}] Error executing {source}.{func_command} for {identifier}: {e}")
        return 0

def parse_dataframe(source, func_name, df, entity_id):
    """Parse DataFrame into observations."""
    observations = []

    if source == "akshare":
        field_mapping = {
            "annual_bs": [("TOTAL_ASSETS", 242), ("TOTAL_LIABILITIES", 243), ("TOTAL_EQUITY", 244)],
            "annual_is": [("OPERATE_INCOME", 239), ("NETPROFIT", 240)],
            "annual_cf": [("NETCASH_OPERATE", 241)],
            "quarterly_bs": [("TOTAL_ASSETS", 242), ("TOTAL_LIABILITIES", 243), ("TOTAL_EQUITY", 244)],
            "quarterly_is": [("OPERATE_INCOME", 239), ("NETPROFIT", 240)],
            "quarterly_cf": [("NETCASH_OPERATE", 241)],
        }

        if func_name not in field_mapping:
            return []

        for _, row in df.iterrows():
            report_date = str(row.get('REPORT_DATE', ''))[:10]
            if not report_date:
                continue

            for field, concept_id in field_mapping[func_name]:
                val = row.get(field)
                if val is not None and str(val) != 'nan':
                    observations.append({
                        "entity_id": entity_id,
                        "date": report_date,
                        "concept_id": concept_id,
                        "value": str(val),
                        "source": "akshare",
                    })

    elif source == "yfinance":
        field_mapping = {
            "annual_bs": [("Total Assets", 242), ("Total Liab Net Debt", 243), ("Stockholders Equity", 244)],
            "annual_is": [("Total Revenue", 239), ("Net Income", 240)],
            "annual_cf": [("Total Cash From Operating Activities", 241)],
            "quarterly_bs": [("Total Assets", 242), ("Total Liab Net Debt", 243), ("Stockholders Equity", 244)],
            "quarterly_is": [("Total Revenue", 239), ("Net Income", 240)],
            "quarterly_cf": [("Total Cash From Operating Activities", 241)],
        }

        if func_name not in field_mapping:
            return []

        dates = [str(d)[:10] for d in df.columns if hasattr(d, '__str__')]

        for yf_field, concept_id in field_mapping[func_name]:
            if yf_field not in df.index:
                continue

            values = df.loc[yf_field]
            for i, dt in enumerate(dates):
                if i < len(values) and values.iloc[i] is not None and str(values.iloc[i]) != 'nan':
                    observations.append({
                        "entity_id": entity_id,
                        "date": dt,
                        "concept_id": concept_id,
                        "value": str(values.iloc[i]),
                        "source": "yfinance",
                    })

    return observations

def write_observations(observations):
    """Write observations to database."""
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        for obs in observations:
            conn.execute(text("""
                INSERT INTO semantic_observations
                (concept_id, entity_type, entity_id, date, value, unit, source_used, fetched_at)
                VALUES (:c, 'stock', :e, :d, :v, 'currency', :s, NOW())
                ON CONFLICT DO NOTHING
            """), {
                "c": obs["concept_id"],
                "e": obs["entity_id"],
                "d": obs["date"],
                "v": obs["value"],
                "s": obs["source"],
            })

def main():
    print(f"=== Financial Crawl Consumer [{POD_ID}] ===")
    print(f"Redis: {REDIS_URL}")

    r = redis.from_url(REDIS_URL)

    # Get initial queue size
    queue_size = r.llen(QUEUE_KEY)
    print(f"Queue size: {queue_size} tasks")

    processed = 0
    errors = 0

    import time

    while True:
        # Pull task from queue (non-blocking check first)
        queue_size = r.llen(QUEUE_KEY)

        if queue_size == 0:
            # No tasks, sleep briefly then check again
            print(f"[{POD_ID}] Queue empty, sleeping 3s... Processed: {processed}")
            time.sleep(3)
            continue

        # There are tasks, try to get one with a short timeout
        try:
            task_json = r.blpop(QUEUE_KEY, timeout=3)
        except Exception as e:
            # Any Redis error - just sleep and retry
            print(f"[{POD_ID}] Redis error: {type(e).__name__}, sleeping 3s...")
            time.sleep(3)
            continue

        if task_json is None:
            # Should not happen due to queue_size check above
            print(f"[{POD_ID}] blpop returned None unexpectedly")
            continue

        # Parse and execute task
        task = json.loads(task_json[1])
        obs_count = execute_task(task)

        if obs_count > 0:
            processed += 1
            if processed % 10 == 0:
                print(f"[{POD_ID}] Processed {processed} tasks, wrote {obs_count} observations")
        else:
            errors += 1

    # Update progress - use SET (not HSET) since producer used r.set()
    try:
        r.set(PROGRESS_KEY, f"processed:{processed}")
    except Exception as e:
        print(f"[{POD_ID}] Failed to update progress: {e}")

    print(f"[{POD_ID}] Done. Processed: {processed}")

if __name__ == "__main__":
    main()
