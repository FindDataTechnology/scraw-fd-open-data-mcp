
# --- series start=None clamp (tencent-crawl-fleet-expansion leftover) ---

def test_extract_series_none_start_clamped():
    import pandas as pd
    from scraw_fd_open_data_mcp.fetch_handler import _extract_series

    class A:
        def extract_series(self, result, column, start, end):
            assert isinstance(start, str)  # must never receive None
            return {"2026-01-01": 1.0}

    out = _extract_series(A(), pd.DataFrame(), "c", None, "2026-12-31")
    assert out == {"2026-01-01": 1.0}
