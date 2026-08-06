#!/usr/bin/env python3
"""Proxy pool sync script: fetch free proxies, validate, and upsert to proxies table.

This script fetches free proxies from multiple sources, validates them, and upserts
to the proxies table. It runs as a CronJob every 10 minutes.

Usage:
    python scripts/proxy_pool_sync.py

Environment variables:
    FD_OPEN_DATA_MCP_DATABASE_URL: PostgreSQL connection string
    PROXY_FETCH_COUNT: Number of proxies to fetch (default: 50)
    PROXY_VALIDATION_TIMEOUT: Timeout for proxy validation in seconds (default: 10)
"""
import os
import sys
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
DATABASE_URL = os.environ.get(
    'FD_OPEN_DATA_MCP_DATABASE_URL',
    'postgresql+psycopg2://postgres:admin123@fd-open-pg.scraw:5432/postgres'
)
PROXY_FETCH_COUNT = int(os.environ.get('PROXY_FETCH_COUNT', '50'))
PROXY_VALIDATION_TIMEOUT = int(os.environ.get('PROXY_VALIDATION_TIMEOUT', '10'))

# Target sources for validation (real data sources, not libraries)
# These are the actual endpoints that akshare/yfinance call under the hood
VALIDATION_TARGETS = [
    {
        'name': 'eastmoney',
        'url': 'https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.600000&fields1=f1&fields2=f51&klt=101&fqt=1&beg=20240101&end=20241231',
        'description': '东方财富 (East Money) - primary source for A-share data'
    },
    {
        'name': 'tencent',
        'url': 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh600000,day,2024-01-01,2024-12-31,1,qfq',
        'description': '腾讯财经 (Tencent Finance) - failover for A-share data'
    },
    {
        'name': 'yahoo_finance',
        'url': 'https://query1.finance.yahoo.com/v8/finance/chart/600000.SS?interval=1d&range=1d',
        'description': 'Yahoo Finance - source for global market data'
    }
]

# Proxy sources (free proxy list websites)
PROXY_SOURCES = [
    'https://www.sslproxies.org/',
    'https://free-proxy-list.net/',
    'https://www.us-proxy.org/',
]


def fetch_proxies_from_source(source_url: str) -> list[dict]:
    """Fetch proxies from a single source website."""
    proxies = []
    try:
        response = requests.get(source_url, timeout=15)
        if response.status_code != 200:
            logger.warning(f"Failed to fetch from {source_url}: status {response.status_code}")
            return proxies

        # Parse HTML for proxy table (ip:port pattern)
        # Most proxy list sites have tables with IP and Port columns
        ip_pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        port_pattern = r'<td>(\d{2,5})</td>'

        # Find all IP addresses
        ips = re.findall(ip_pattern, response.text)
        # Find all ports (usually in the next <td> after IP)
        ports = re.findall(port_pattern, response.text)

        # Pair IPs with ports
        for i in range(min(len(ips), len(ports))):
            try:
                ip = ips[i]
                port = int(ports[i])
                if 1 <= port <= 65535:
                    proxies.append({
                        'scheme': 'http',
                        'ip': ip,
                        'port': port
                    })
            except (ValueError, IndexError):
                continue

        logger.info(f"Fetched {len(proxies)} proxies from {source_url}")
    except Exception as e:
        logger.error(f"Error fetching from {source_url}: {e}")

    return proxies


def fetch_proxies() -> list[dict]:
    """Fetch free proxies from multiple sources."""
    logger.info(f"Fetching up to {PROXY_FETCH_COUNT} proxies from {len(PROXY_SOURCES)} sources...")
    all_proxies = []

    for source_url in PROXY_SOURCES:
        proxies = fetch_proxies_from_source(source_url)
        all_proxies.extend(proxies)
        if len(all_proxies) >= PROXY_FETCH_COUNT:
            break

    # Remove duplicates
    seen = set()
    unique_proxies = []
    for proxy in all_proxies:
        key = f"{proxy['ip']}:{proxy['port']}"
        if key not in seen:
            seen.add(key)
            unique_proxies.append(proxy)

    # Limit to PROXY_FETCH_COUNT
    unique_proxies = unique_proxies[:PROXY_FETCH_COUNT]
    logger.info(f"Fetched {len(unique_proxies)} unique proxies")

    return unique_proxies


def validate_proxy(proxy: dict, target: dict) -> bool:
    """Validate a proxy against a target source."""
    proxy_url = f"{proxy['scheme']}://{proxy['ip']}:{proxy['port']}"
    proxies_dict = {'http': proxy_url, 'https': proxy_url}

    try:
        response = requests.get(
            target['url'],
            proxies=proxies_dict,
            timeout=PROXY_VALIDATION_TIMEOUT
        )
        # Accept 2xx or 5xx (5xx means proxy works but target is temporarily unavailable)
        if response.status_code < 600:
            logger.debug(f"Proxy {proxy_url} validated against {target['name']} (status={response.status_code})")
            return True
    except Exception as e:
        logger.debug(f"Proxy {proxy_url} failed validation against {target['name']}: {e}")

    return False


def validate_proxies(proxies: list[dict]) -> list[dict]:
    """Validate proxies against all target sources."""
    logger.info(f"Validating {len(proxies)} proxies against {len(VALIDATION_TARGETS)} real data sources...")

    # Track success per real_source
    success_by_source = {target['name']: 0 for target in VALIDATION_TARGETS}
    valid_proxies = []

    for proxy in proxies:
        # A proxy is valid if it works for at least one target
        validated = False
        for target in VALIDATION_TARGETS:
            if validate_proxy(proxy, target):
                success_by_source[target['name']] += 1
                if not validated:
                    valid_proxies.append(proxy)
                    validated = True
                # Continue testing other sources to get per-source stats

    # Log per-source success rates
    for source_name, count in success_by_source.items():
        rate = (count / len(proxies) * 100) if proxies else 0
        logger.info(f"  {source_name}: {count}/{len(proxies)} proxies ({rate:.1f}% success)")

    overall_rate = (len(valid_proxies) / len(proxies) * 100) if proxies else 0
    logger.info(f"Validated {len(valid_proxies)} proxies ({overall_rate:.1f}% overall success rate)")
    return valid_proxies


def upsert_proxies(proxies: list[dict]) -> int:
    """Upsert proxies to the proxies table. Returns number of upserted proxies."""
    engine = create_engine(DATABASE_URL)
    upserted = 0

    with engine.begin() as conn:
        for proxy in proxies:
            # Check if proxy already exists
            result = conn.execute(
                text("SELECT id FROM proxies WHERE ip = :ip AND port = :port"),
                {'ip': proxy['ip'], 'port': proxy['port']}
            ).fetchone()

            if result:
                # Update existing proxy
                conn.execute(
                    text("""
                        UPDATE proxies
                        SET status = 'active', retired_at = NULL, updated_at = NOW()
                        WHERE ip = :ip AND port = :port
                    """),
                    {'ip': proxy['ip'], 'port': proxy['port']}
                )
                logger.debug(f"Updated proxy: {proxy['ip']}:{proxy['port']}")
            else:
                # Insert new proxy
                conn.execute(
                    text("""
                        INSERT INTO proxies (scheme, ip, port, auth, status, label, created_at, updated_at)
                        VALUES (:scheme, :ip, :port, NULL, 'active', 'free-proxy', NOW(), NOW())
                    """),
                    {'scheme': proxy['scheme'], 'ip': proxy['ip'], 'port': proxy['port']}
                )
                logger.debug(f"Inserted proxy: {proxy['ip']}:{proxy['port']}")

            upserted += 1

    logger.info(f"Upserted {upserted} proxies")
    return upserted


def retire_stale_proxies() -> int:
    """Retire proxies not seen in the last 3 sync cycles (30 minutes). Returns number of retired proxies."""
    engine = create_engine(DATABASE_URL)
    retired = 0

    # Calculate cutoff time (30 minutes ago)
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=30)

    with engine.begin() as conn:
        # Find proxies that are active but not updated in the last 30 minutes
        result = conn.execute(
            text("""
                UPDATE proxies
                SET status = 'retired', retired_at = NOW()
                WHERE status = 'active'
                  AND (updated_at IS NULL OR updated_at < :cutoff)
                RETURNING id
            """),
            {'cutoff': cutoff}
        )
        retired = len(result.fetchall())

    logger.info(f"Retired {retired} stale proxies")
    return retired


def main():
    """Main entry point."""
    logger.info("Starting proxy pool sync...")

    # Fetch proxies
    proxies = fetch_proxies()
    if not proxies:
        logger.warning("No proxies fetched, exiting")
        sys.exit(1)

    # Validate proxies
    valid_proxies = validate_proxies(proxies)
    if not valid_proxies:
        logger.warning("No valid proxies after validation, exiting")
        sys.exit(1)

    # Upsert valid proxies
    upserted = upsert_proxies(valid_proxies)

    # Retire stale proxies
    retired = retire_stale_proxies()

    logger.info(f"Sync complete: {upserted} upserted, {retired} retired")


if __name__ == '__main__':
    main()
