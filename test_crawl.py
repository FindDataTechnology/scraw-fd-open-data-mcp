#!/usr/bin/env python3
"""
Test the complete crawling pipeline for scraw-fd-open-data-mcp.
This script simulates what will run in the Kubernetes pod.
"""
import sys
import os
import json
from datetime import date, timedelta

# Load environment from .env file
env_file = "/Users/chengsishi/finddata/.env"
for line in open(env_file):
    line = line.strip()
    if "=" in line and not line.startswith("#"):
        key, value = line.split("=", 1)
        os.environ[key] = value

print("=" * 70)
print("scraw-fd-open-data-mcp Crawler Test")
print("=" * 70)

# Step 1: Check dependencies
print("\n[Step 1/5] Checking dependencies...")
try:
    import scrapy
    print(f"✓ Scrapy version: {scrapy.__version__}")
except ImportError:
    print("✗ Scrapy not installed")
    sys.exit(1)

try:
    import sqlalchemy
    print(f"✓ SQLAlchemy version: {sqlalchemy.__version__}")
except ImportError:
    print("✗ SQLAlchemy not installed")
    sys.exit(1)

try:
    from fd_open_data_mcp.fetch.runner import run_upstream
    print("✓ fd_open_data_mcp adapters loaded")
except ImportError as e:
    print(f"✗ Failed to load adapters: {e}")
    sys.exit(1)

# Step 2: Create crawl plan
print("\n[Step 2/5] Creating crawl plan...")

crawl_plan = {
    "wanted_concepts": [
        {
            "concept_id": 234,  # Close price (收盘)
            "entity_type": "stock",
            "unit": "currency",
            "ranked_sources": [
                {
                    "source": "akshare",
                    "function_command": "stock_zh_a_hist",
                    "column_name": "收盘"
                }
            ]
        }
    ],
    "entity_scope": {
        "entity_type": "stock",
        "entity_ids": [1]  # 000001 - Ping An Bank
    },
    "date_range": {
        "start": "2024-06-24",
        "end": "2024-06-28"
    }
}

with open("/tmp/crawl_test_plan.json", "w") as f:
    json.dump(crawl_plan, f, indent=2)
    
# Count dates
start_date = date.fromisoformat(crawl_plan["date_range"]["start"])
end_date = date.fromisoformat(crawl_plan["date_range"]["end"])
num_dates = (end_date - start_date).days + 1
print(f"✓ Crawl plan created: {len(crawl_plan['wanted_concepts'])} concept(s), {num_dates} date(s)")

# Step 3: Execute fetch requests
print("\n[Step 3/5] Executing fetch requests...")
results = []

with open("/tmp/crawl_test_plan.json", "r") as f:
    plan = json.load(plan) if False else json.load(open("/tmp/crawl_test_plan.json"))

# Get entities
from sqlalchemy import create_engine, text
db_url = os.environ.get("FD_OPEN_DATA_MCP_DATABASE_URL", "")
engine = create_engine(db_url)

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT entity_id, identifier 
        FROM entity_source_identifiers 
        WHERE source=:source AND entity_type=:et 
        AND entity_id IN :entity_ids
    """), {
        "source": "akshare",
        "et": "stock",
        "entity_ids": tuple(crawl_plan["entity_scope"]["entity_ids"])
    })
    
    entities = result.fetchall()
    print(f"  Found {len(entities)} entity(ies) for akshare")

# Execute fetches
dates = []
cur = start_date
while cur <= end_date:
    dates.append(cur.isoformat())
    cur += timedelta(days=1)

for pc in plan["wanted_concepts"]:
    for rs in pc["ranked_sources"]:
        for entity_id, identifier in entities:
            for date_str in dates:
                try:
                    print(f"  Fetching {identifier} on {date_str}...", end=" ")
                    
                    result_df = run_upstream(rs["source"], rs["function_command"], {
                        "symbol": identifier,
                        "start_date": date_str.replace("-", ""),
                        "end_date": date_str.replace("-", ""),
                        "adjust": "qfq"
                    })
                    
                    import pandas as pd
                    if isinstance(result_df, pd.DataFrame) and not result_df.empty:
                        close_price = result_df['收盘'].values[0]
                        results.append({
                            "concept_id": pc["concept_id"],
                            "entity_type": pc["entity_type"],
                            "entity_id": entity_id,
                            "date": date_str,
                            "value": str(close_price),
                            "unit": pc["unit"],
                            "source_used": rs["source"]
                        })
                        print(f"✓ ¥{close_price:.2f}")
                    else:
                        print("✗ No data")
                        
                except Exception as e:
                    print(f"✗ Error: {str(e)[:50]}")

# Step 4: Write observations to database
print("\n[Step 4/5] Writing results to database...")
try:
    with engine.begin() as conn:
        insert_sql = """
            INSERT INTO semantic_observations 
            (concept_id, entity_type, entity_id, date, value, unit, source_used, fetched_at)
            VALUES (:concept_id, :entity_type, :entity_id, :date, :value, :unit, :source_used, NOW())
            ON CONFLICT DO NOTHING
        """
        
        for result in results:
            conn.execute(text(insert_sql), result)
        
    print(f"✓ Written {len(results)} observation(s) to semantic_observations")
except Exception as e:
    print(f"✗ Error writing to database: {e}")

# Step 5: Verify
print("\n[Step 5/5] Verifying results...")
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(*) FROM semantic_observations 
        WHERE concept_id = 234 AND entity_id = 1 
        AND date BETWEEN '2024-06-24' AND '2024-06-28'
    """))
    count = result.scalar()
    print(f"  Verified: {count} records in semantic_observations for stock 000001")

print("\n" + "=" * 70)
if len(results) > 0:
    print(f"✓ SUCCESS! Fetched {len(results)} data points")
    print("\nSample results:")
    for r in results[:3]:
        print(f"  {r['date']}: ¥{float(r['value']):.2f} ({r['source_used']})")
else:
    print("✗ No data fetched successfully")
print("=" * 70)
