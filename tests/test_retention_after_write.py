"""写路径的留存收尾（T 类只留当期）——爬虫这条路此前从未执行过。

背景（2026-11-12 实测，decisions §5.54/§5.57）：爬虫用**裸 SQL** 写
`semantic_observations`（`db.py` 的 `INSERT ... ON CONFLICT`），完全绕开包的
`cache.write_cache*`。于是包里的 `_snapshot_consolidate` 在这条路上一次都没跑过：
`observation_revisions` 里 `snapshot-consolidate-*` 全时段 0 条，而 `akshare/nav`
长回"一实体三日期"（346 个实体 / 692 行）。

本文件钉的是**接线**与**降级**，不是合并算法本身：
- ``apply_bulk_retention`` 合并 T 行的语义由包仓 `fd-open-data-mcp` 的
  `tests/test_bulk_retention_guard.py` 覆盖（本仓 dev venv 里装的是 0.5.35，
  连 `fetch` 子包都没有，在这里重测算法只会测出"版本旧"）；
- 这里要证的是：**关闭爬虫时会调用它、调用一次、键是对的、失败不炸**。
"""
from __future__ import annotations

import sys
import types

import pytest

from scraw_fd_open_data_mcp import db as obs_db
from scraw_fd_open_data_mcp.pipelines import ObservationUpsertPipeline


class _Spider:
    class logger:
        @staticmethod
        def info(*a, **k):
            pass

        @staticmethod
        def warning(*a, **k):
            pass

        @staticmethod
        def debug(*a, **k):
            pass


def _item(concept_id=930001, date="2026-11-12", source="akshare"):
    return {"concept_id": concept_id, "entity_type": "fund", "entity_id": 42,
            "date": date, "granularity": "day", "value": 1.0, "unit": "CNY",
            "source_used": source}


@pytest.fixture
def stub_bulk(monkeypatch):
    """把包的 retention_bulk 换成桩：只记录被调用的 (source, concept_ids)。

    走 sys.modules 注入（父包与子模块都要占位），这样即使 venv 里的
    fd-open-data-mcp 是 0.5.35（没有 fetch 子包）也能测接线。
    """
    calls: list[tuple[str, list[int]]] = []

    def fake_apply(conn, *, source, concept_ids, reason=None):
        calls.append((source, sorted(concept_ids)))
        return {"t_deleted": len(concept_ids), "w_deleted": 0}

    parent = sys.modules.get("fd_open_data_mcp.fetch") or types.ModuleType(
        "fd_open_data_mcp.fetch")
    mod = types.ModuleType("fd_open_data_mcp.fetch.retention_bulk")
    mod.apply_bulk_retention = fake_apply
    monkeypatch.setitem(sys.modules, "fd_open_data_mcp.fetch", parent)
    monkeypatch.setitem(sys.modules, "fd_open_data_mcp.fetch.retention_bulk", mod)
    monkeypatch.setattr(obs_db, "_engine", lambda: _FakeEngine())
    return calls


class _FakeEngine:
    """apply_retention 只需要一个能 `with eng.begin() as conn` 的引擎。"""

    class _Ctx:
        def __enter__(self):
            return object()

        def __exit__(self, *exc):
            return False

    def begin(self):
        return self._Ctx()


def test_close_spider_calls_retention_once_with_every_touched_key(monkeypatch, stub_bulk):
    """一次作业一次收尾；键按 (源, 概念) 去重，不是按批。"""
    monkeypatch.setattr(obs_db, "write_observations", lambda rows: (len(rows), len(rows)))
    pipe = ObservationUpsertPipeline()
    pipe.BATCH = 2                     # 逼出多次 flush，验证收尾仍只发生一次

    for i in range(6):
        pipe.process_item(_item(concept_id=930001 + (i % 2)), _Spider())

    assert stub_bulk == [], "flush 期间不该收尾（BATCH=2 已 flush 三次）"
    pipe.close_spider(_Spider())
    assert stub_bulk == [("akshare", [930001, 930002])], "收尾一次、键去重"


def test_retention_failure_does_not_break_the_job(monkeypatch):
    """收尾抛异常时不能把已经落库的爬取判成失败。"""
    monkeypatch.setattr(obs_db, "write_observations", lambda rows: (len(rows), len(rows)))

    def boom(keys):
        raise RuntimeError("retention backend down")

    monkeypatch.setattr(obs_db, "apply_retention", boom)
    pipe = ObservationUpsertPipeline()
    pipe.process_item(_item(), _Spider())
    pipe.close_spider(_Spider())      # 不抛


def test_old_package_degrades_to_noop(monkeypatch):
    """镜像里的包没有 retention_bulk（如 0.5.35）时：不抛、返回零。"""
    monkeypatch.setitem(sys.modules, "fd_open_data_mcp.fetch.retention_bulk", None)
    stats = obs_db.apply_retention({("akshare", 930001)})
    assert stats == {"t_deleted": 0, "w_deleted": 0, "errors": 0}


def test_empty_keys_short_circuits(monkeypatch):
    called = {"n": 0}

    def fake_apply(*a, **k):
        called["n"] += 1
        return {}

    monkeypatch.setattr(obs_db, "_engine", lambda: _FakeEngine())
    monkeypatch.setitem(sys.modules, "fd_open_data_mcp.fetch.retention_bulk",
                        types.SimpleNamespace(apply_bulk_retention=fake_apply))
    monkeypatch.setitem(sys.modules, "fd_open_data_mcp.fetch",
                        sys.modules.get("fd_open_data_mcp.fetch")
                        or types.ModuleType("fd_open_data_mcp.fetch"))
    assert obs_db.apply_retention(set()) == {"t_deleted": 0, "w_deleted": 0, "errors": 0}
    assert called["n"] == 0
