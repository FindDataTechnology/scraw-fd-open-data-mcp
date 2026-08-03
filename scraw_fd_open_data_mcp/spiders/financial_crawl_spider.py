"""Financial data crawl spider - uses scrapy-redis queue for dynamic load balancing.

Spider extracts annual + quarterly financial statements via akshare/yfinance.
Tasks are queued in Redis with deduplication -- multiple pods can process the queue
in parallel without duplication or race conditions.

Usage:
    scrapy crawl financial_crawl \
        -a concept_ids=239,240,241,242,243,244 \
        -a entity_type=stock \
        -a start_date=2024-01-01 \
        -a end_date=2026-12-31
"""
from __future__ import annotations

import json
import redis as redis_lib
from typing import Optional, List, Any

import scrapy
from scrapy.http import Request, Response


class FinancialCrawlSpider(scrapy.Spider):
    """Dynamic queue-based financial crawler."""

    name = "financial_crawl"

    # Default redis configuration (can be overridden by env vars)
    REDIS_HOST = "redis://fd-open-redis.scraw:6379/0"
    QUEUE_KEY = "financial_crawl:start_urls"
    DUPEFILTER_KEY = "financial_crawl:dupefilter"
    PROGRESS_KEY = "financial_crawl:progress"

    custom_settings = {
        "CONCURRENT_REQUESTS": 4,
        "DOWNLOAD_TIMEOUT": 30,
        "RETRY_TIMES": 2,
        # Scrapy-redis scheduler config (for this spider only)
        "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
        "SCHEDULER_QUEUE_CLASS": "scrapy_redis.queue.SpiderQueue",
        "SCHEDULER_PERSIST": True,  # Keep tasks when stopped
        "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
        "REDIS_URL": REDIS_HOST,
        "REDISCACHE_DUPEFILTER": False,
    }

    def __init__(self, concept_ids: Optional[str] = None,
                 entity_type: str = "stock",
                 start_date: Optional[str] = "2024-01-01",
                 end_date: Optional[str] = "2026-12-31",
                 *args, **kwargs):
        super().__init__(*args, **concept_ids)

        self.concept_ids = [int(c.strip()) for c in concept_ids.split(",")] if concept_ids else []
        self.entity_type = entity_type
        self.start_date = start_date
        self.end_date = end_date

        # Redis connection
        try:
            self.redis_client = redis_lib.from_url(self.REDIS_URL)
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None

    def start_requests(self):
        """Initialize queue from database and yield tasks to redis queue."""
        if not self.redis_client:
            self.logger.error("Redis not available, cannot initialize queue")
            return

        # Connect to DB
        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker

        db_url = self.settings.get(
            "FD_OPEN_DATA_MCP_DATABASE_URL",
            "postgresql+psycopg2://postgres:admin123@fd-open-pg.scraw:5432/postgres"
        )
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Get all stocks that need extraction
            self.logger.info(f"Loading stocks for concepts {self.concept_ids}, type={self.entity_type}")

            result = session.execute(text("""
                SELECT entity_id, identifier
                FROM entity_source_identifiers esi
                WHERE source = 'akshare' AND entity_type = :et
                ORDER BY entity_id
            """), {"et": self.entity_type})

            stocks = [(row[0], row[1]) for row in result.fetchall()]
            total = len(stocks)
            self.logger.info(f"Total stocks: {total}")

            # Enqueue ALL (stock, concept, function) combinations
            # This way each pod picks up what it needs from the shared queue
            tasks_enqueued = 0

            for entity_id, identifier in stocks:
                # For each stock, enqueue all 12 functions (6 akshare + 6 yfinance)
                functions_to_extract = [
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

                for source, func_name, fn_cmd in functions_to_extract:
                    task_key = f"{entity_id}:{source}:{func_name}"

                    # Check if already processed (skip duplicates)
                    exists = self.redis_client.sadd(task_key, 1)
                    if not exists:
                        self.logger.debug(f"Task already enqueued: {task_key}")
                        continue

                    # Create task for redis queue
                    task_data = {
                        "entity_id": entity_id,
                        "identifier": identifier,
                        "source": source,
                        "function_name": func_name,
                        "function_command": fn_cmd,
                        "concept_ids": self.concept_ids,
                        "start_date": self.start_date,
                        "end_date": self.end_date,
                    }

                    # Push to scrapy-redis queue
                    self.redis_client.rpush(
                        self.QUEUE_KEY,
                        json.dumps(task_data, ensure_ascii=False, default=str)
                    )
                    tasks_enqueued += 1

            # Update progress
            self.redis_client.set(self.PROGRESS_KEY, f"initialized:{tasks_enqueued}")
            self.logger.info(f"Enqueued {tasks_enqueued} tasks to Redis queue")

        finally:
            session.close()

    def parse(self, response: Response):
        """Parse results and write to DB."""
        task_data = response.request.meta["task_data"]

        # Extract value based on source and function
        source = task_data["source"]
        func_name = task_data["function_name"]

        value = self._extract_value(source, func_name, response)

        if value is not None:
            self._write_result(task_data, value)

        # Update progress
        self.redis_client.sincr(self.PROGRESS_KEY, 1)

    def _extract_value(self, source: str, func_name: str, response: Response) -> Any:
        """Extract data from response based on source and function."""
        df = response.text
        if df is None or len(df) == 0:
            return None

        # Parse DataFrame based on source
        if source == "akshare":
            return self._parse_akshare(func_name, df)
        elif source == "yfinance":
            return self._parse_yfinance(func_name, df)

        return None

    def _parse_akshare(self, func_name: str, df) -> list:
        """Parse akshare DataFrame into observations."""
        observations = []

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
                        "entity_id": row.get('SECUCODE') or row.get('SECURITY_CODE'),
                        "date": report_date,
                        "concept_id": concept_id,
                        "value": str(val),
                        "source": "akshare",
                    })

        return observations

    def _parse_yfinance(self, func_name: str, df) -> list:
        """Parse yfinance DataFrame into observations."""
        observations = []

        field_mapping = {
            "balance_sheet": [("Total Assets", 242), ("Total Liab Net Debt", 243), ("Stockholders Equity", 244)],
            "income": [("Total Revenue", 239), ("Net Income", 240)],
            "cashflow": [("Total Cash From Operating Activities", 241)],
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
                        "entity_id": getattr(df.index[0], 'name', ''),
                        "date": dt,
                        "concept_id": concept_id,
                        "value": str(values.iloc[i]),
                        "source": "yfinance",
                    })

        return observations

    def _write_result(self, task_data: dict, value: Any):
        """Write observations to database."""
        from sqlalchemy import create_engine, text

        db_url = self.settings.get(
            "FD_OPEN_DATA_MCP_DATABASE_URL",
            "postgresql+psycopg2://postgres:admin123@fd-open-pg.scraw:5432/postgres"
        )
        engine = create_engine(db_url)

        # Convert value (could be list of obs or single obs)
        observations = value if isinstance(value, list) else [value]

        for obs in observations:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO semantic_observations
                    (concept_id, entity_type, entity_id, date, value, unit, source_used, fetched_at)
                    VALUES (:c,'stock',:e,:d,:v,'currency',:s,NOW())
                    ON CONFLICT DO NOTHING
                """), {
                    "c": obs["concept_id"],
                    "e": obs["entity_id"],
                    "d": obs["date"],
                    "v": obs["value"],
                    "s": obs["source"],
                })

        self.logger.info(f"Wrote {len(observations)} observations for {task_data['identifier']}")
