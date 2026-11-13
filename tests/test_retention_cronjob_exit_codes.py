"""留存 CronJob 的退出码映射：谁红了、谁不该红，必须钉死。

`k8s/retention-audit-cronjob.yaml` 的 wrapper 里有两段 `case`：

* evict 的 `1` 是"有活干"（dry-run 有待淘汰 / `--apply` 已搬完），**不是失败**；
  照原样交给 CronJob 会每天报 Failed —— 红久了就没人看红，真事故被淹没。
* audit 的 `3` 是"仅契约待补"（医药域在建新族时常态），不判失败但留痕；
  而 `1`（容量违规）与 `2`（未知）**必须保持红** —— "未知不等于通过"。

所以这段 shell 是"违规能不能被人看见"的唯一开关。它此前只被实测过两条路径
（audit 0 → 绿、evict 1 → 绿），红的那两条从没验过。本文件用 YAML 里的**真实文本**
跑全表，避免"测试里抄了一份 shim、线上跑的是另一份"。

判据为什么是这个形状：任何把两个退出码合成一个的"简化"（例如
`exit $(( a || e ))`）都会让容量违规变成绿——本表的 (1,0) 与 (2,0) 两行就是为了拦住它。
"""
from __future__ import annotations

import pathlib
import subprocess

import pytest

yaml = pytest.importorskip("yaml")

MANIFEST = pathlib.Path(__file__).resolve().parents[1] / "k8s" / "retention-audit-cronjob.yaml"

#: (audit 退出码, evict 退出码) -> wrapper 应该给出的退出码
CASES = [
    (0, 0, 0, "两边都干净"),
    (0, 1, 0, "evict 有活干过但已搬完，算成功"),
    (3, 0, 0, "只剩契约待补，不判失败"),
    (3, 1, 0, "契约待补 + 已搬完，仍算成功"),
    (1, 0, 1, "容量违规必须红"),
    (1, 1, 1, "容量违规必须红（即使 evict 干了活）"),
    (2, 0, 2, "未知必须红（未知不等于通过）"),
    (2, 3, 2, "未知优先于待补"),
]


def _wrapper_script() -> str:
    doc = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    return doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["args"][0]


@pytest.fixture
def python_shim(tmp_path, monkeypatch):
    """把 `python <script>` 变成「按环境变量退出」，脚本本体仍是 YAML 里的原文。"""
    shim = tmp_path / "python"
    shim.write_text(
        "#!/bin/bash\n"
        'case "$1" in\n'
        '  *audit_retention_contracts.py) exit "${STUB_AUDIT:-0}" ;;\n'
        '  *evict_retention_window.py)    exit "${STUB_EVICT:-0}" ;;\n'
        "  *) exit 0 ;;\n"
        "esac\n",
        encoding="utf-8")
    shim.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    return tmp_path


def test_wrapper_reads_both_scripts(python_shim):
    """先证 wrapper 真的调了这两个脚本——否则下面的全表是在测空气。"""
    script = _wrapper_script()
    assert "audit_retention_contracts.py" in script
    assert "evict_retention_window.py" in script


@pytest.mark.parametrize("audit,evict,expect,why", CASES,
                         ids=[c[3] for c in CASES])
def test_exit_code_mapping(audit, evict, expect, why, python_shim):
    r = subprocess.run(["bash", "-c", _wrapper_script()],
                       capture_output=True, text=True,
                       env={"PATH": f"{python_shim}:/usr/bin:/bin",
                            "STUB_AUDIT": str(audit), "STUB_EVICT": str(evict),
                            "FD_OPEN_DATA_MCP_DATABASE_URL": "postgresql://stub/none"})
    assert r.returncode == expect, f"{why}：期望 {expect}，实得 {r.returncode}；stderr={r.stderr[:200]}"
