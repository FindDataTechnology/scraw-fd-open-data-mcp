"""反旁路静态闸：本仓任何直写 `semantic_observations` 的地方都必须执行留存契约。

为什么需要这道闸：包仓（`fd-open-data-mcp`）的 `tests/test_bulk_retention_guard.py`
有一道同型闸，但它只扫**包仓自己的** `scripts/*.py`。本仓的写入点分散在
`scripts/`（direct 策略的脚本源）与 `scraw_fd_open_data_mcp/`（管线 + 个别蜘蛛），
两边都不在它的扫描路径里——2026-11-13 实测，四处裸 SQL 写点因此长期没有收尾：

    scripts/macro_backfill.py            （akshare 宏观，W/F 族）
    scripts/polygon_us_ohlcv.py          （polygon price/trading_volume，W/730）
    scripts/financial_consumer.py        （yfinance → akshare/financials，T/30）
    scraw_fd_open_data_mcp/spiders/financial_crawl_spider.py（同一批 stock 概念）

后果不是"少一层保护"，而是**契约只在一半的写路径上成立**：窗口淘汰工具清完，
下一次全历史重抓又把超窗行整批写回来；T 族每跑一次多留一个日期。

判据故意简单且宁可误报：文件里出现 `INSERT INTO semantic_observations`，就必须同时
出现 `apply_bulk_retention`（直写脚本的收尾）或 `apply_retention`（管线的收尾，
`db.py` 用的就是它）。只读该表的报表/审计脚本不会被误判。

放行名单每一条都必须写明理由，且必须**仍然命中**判据——条目失效即红，防止名单
变成"抄一次就不用再想"的永久豁免。
"""
from __future__ import annotations

import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
SCAN_DIRS = (ROOT / "scripts", ROOT / "scraw_fd_open_data_mcp")
# 只认**调用**形态（带左括号）。按裸字符串判会假绿：删掉调用、只留 import 行时，
# `apply_bulk_retention` 仍出现在文件里——2026-11-13 实测，第一版判据就是这样放过了
# 一次注入（把调用改名后闸照旧全绿）。
MARKERS = ("apply_bulk_retention(", "apply_retention(")
INSERT = "INSERT INTO semantic_observations"

ALLOWLIST = {
    # 端到端演练脚本：手工跑的验证脚本，不是产线写入入口；它写的是测试店/测试实体。
    "test_crawl.py": "仓库根的手工 e2e 演练脚本，非产线写入路径",
}


def _sources():
    # 根目录的 *.py 也要扫：`test_crawl.py` 就是放在根上的手工演练脚本，
    # 只扫两个子目录会让放行名单失效（清单条目永远"没命中判据"）。
    for p in sorted(ROOT.glob("*.py")):
        yield p
    for d in SCAN_DIRS:
        if not d.is_dir():
            continue
        for p in sorted(d.rglob("*.py")):
            yield p


def test_every_writer_under_this_repo_applies_retention():
    offenders, matched_allow = [], set()
    for path in _sources():
        src = path.read_text(encoding="utf-8", errors="replace")
        if INSERT not in src:
            continue
        if any(m in src for m in MARKERS):
            continue
        if path.name in ALLOWLIST:
            matched_allow.add(path.name)
            continue
        offenders.append(str(path.relative_to(ROOT)))

    stale = set(ALLOWLIST) - matched_allow
    assert not stale, f"放行名单里有条目已不再命中判据（应删掉或改判据）：{sorted(stale)}"
    assert not offenders, (
        "这些写入点直接写 semantic_observations 却没有执行留存契约："
        f"{offenders}——在同一个事务里补 apply_bulk_retention(...)，"
        "或在管线路径上补 apply_retention(...)")


def test_the_known_four_writers_are_actually_covered():
    """钉住四处已知裸 SQL 写点：它们是这道闸存在的理由，不能靠"没扫到"变绿。

    这条与上面那条互补：上面那条可能因为路径写错而整体空转（一条都不扫 = 全绿），
    这条要求**这四个文件必须被扫到且必须含收尾标记**，让"闸空转"自身变成红灯。
    """
    expected = [
        "scripts/macro_backfill.py",
        "scripts/polygon_us_ohlcv.py",
        "scripts/financial_consumer.py",
        "scraw_fd_open_data_mcp/spiders/financial_crawl_spider.py",
    ]
    scanned = {str(p.relative_to(ROOT)) for p in _sources()}
    for rel in expected:
        assert rel in scanned, f"这个写点没被扫到（闸空转了）：{rel}——检查 SCAN_DIRS"
        src = (ROOT / rel).read_text(encoding="utf-8", errors="replace")
        assert INSERT in src, f"{rel} 里找不到 INSERT 判据，闸已对它失效"
        assert any(m in src for m in MARKERS), f"{rel} 没有执行留存契约"
