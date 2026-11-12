# Multi-stage build for scraw-fd-open-data-mcp.
#
# fd-open-data-mcp is NOT vendored in the build context: it is fetched from
# git at a PINNED COMMIT (FD_ODM_REF) and installed from that source tree
# (see the ARG block below for why pinning replaced "install latest from
# PyPI"). To build against a different commit — e.g. a release candidate, or
# a known-good rollback — pass it explicitly:
#   docker build --build-arg FD_ODM_REF=<sha> \
#     --build-arg FD_ODP_INSTALL=/build/fd-open-data-protocol -t ... .
# Keep the [data] extra (the RUN installs "/build/fodm-src[data]") — without
# it the image silently loses yfinance/edgartools/… and any datasource or
# probe that imports them crashes with ModuleNotFoundError.
#
# scraw-fd-open-data-mcp itself is vendored (it's the crawler, not on PyPI).
#
# Pins: scrapy>=2.12,<2.13 (2.13+ broke start_requests + sync download_handler),
# Twisted<25 (removed _setAcceptableProtocols that scrapy 2.12 needs),
# w3lib>=2.1,<2.5 (2.5 removed _safe_chars that scrapy 2.12 imports — the
# 2026-10-01 fleet regression; see openspec scraw-runner-image-regression).
# fd-open-data-mcp's [data] extra pulls the data-source deps (akshare, wbgapi,
# yfinance, edgartools, ...) so adding a data source to pyproject.toml [data]
# is all that's needed - no Dockerfile edit, no per-package maintenance.
# Browser rendering is NOT part of [data] since fd-open-data-mcp 0.5.34
# (openspec image-slimming): scrapling/playwright live in [browser] and the
# fleet image stays browser-free; a browser-capable variant is
# fd-open-data-mcp[data,browser] if one is ever needed.
#
# fix-silent-zero-yield-crawls task 7.1: the data deps themselves are pinned
# BELOW (after the fd-open-data-mcp install) so a rebuild can no longer
# silently move the fleet's dependency set. akshare/pandas are the versions
# verified working from the cluster on 2026-08-24 — the 08-22 rebuild had
# moved akshare 1.18.83 -> 1.18.94 and pandas -> 3.0.5 under an unpinned
# :latest. Bump these pins DELIBERATELY (verify a snapshot call from a worker
# after any bump), never implicitly.

FROM python:3.12-slim AS builder
WORKDIR /build
# Optional CN build mirrors — empty by default (upstream sources, CI-friendly).
# On CN build boxes (e.g. the chengsi fleet) pass:
#   --build-arg APT_MIRROR=mirrors.aliyun.com --build-arg PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
# PIP_INDEX_URL is picked up by pip automatically for every install below.
ARG APT_MIRROR=""
ARG PIP_INDEX_URL=""
RUN if [ -n "$APT_MIRROR" ]; then \
        sed -i "s|deb.debian.org|$APT_MIRROR|g" /etc/apt/sources.list.d/debian.sources; \
    fi \
 && apt-get update && apt-get install -y --no-install-recommends \
    build-essential libssl-dev libffi-dev git && rm -rf /var/lib/apt/lists/*
COPY . /build/scraw-fd-open-data-mcp
# fd-datacommons is not published (PyPI/GitHub) — vendored for the datacommons
# datasource entry point; update from the fd-datacommons workspace checkout.
COPY vendor/fd-datacommons /build/vendor/fd-datacommons
# fd-open-data-mcp is NOT vendored anymore: pip resolves from PyPI. Floor
# history: >=0.5.17 (eastmoney-ok nodeSelector fix) -> >=0.5.24 -> >=0.5.34
# (first release whose [data] extra excludes the browser stack — the fleet
# image must never silently re-fatten with scrapling/playwright) -> >=0.5.41
# (add-crawl-job-deadline: Job activeDeadlineSeconds hard ceiling + the
# `deadline_exceeded` poll state, so a wedged crawler can never again hold a
# Job at Running forever and starve the host — 2026-11-07 fd-deploy incident).
# -> >=0.5.42 (deadline-killed Jobs whose pods were never created carry only a
# Failed condition, no failed counter — 0.5.41 read "unknown" and left the
# policy_runs row dangling in `running`) -> >=0.5.43 (the local kubectl poll
# branch read jsonpath positionally and reported a starved deadline Job as
# `success`; all three transports now share _job_state()) -> >=0.5.45 (census
# savepoints: 0.5.44 took session.connection() before begin_nested(), so
# SQLAlchemy emitted no SAVEPOINT at all and one dead shard still discarded the
# whole census; the connection is now taken inside each block) -> >=0.5.46 (the
# startup log no longer prints the database password) -> >=0.5.47 (yfinance
# egresses on the proxy the fetch attempt was given; every direct egress in
# # the fleet is blocked by Yahoo's edge) -> >=0.5.48 (direct-script Jobs stop
# carrying the database DSN on their command line) -> >=0.5.49 (DB/Redis reach the pod
# via secretKeyRef, not a literal env value) -> >=0.5.50 (series reads are
# scoped to the concept's own entity_type, so mislabelled rows cannot surface)
# -> >=0.5.52 (scoping to the row's DECLARED type could not see the 343,411 rows
# that declare 'stock' while hanging off a person/industry/fund entity; reads now
# resolve the entity's own class, and the city-level star-rated series that 0.5.50
# had made unreadable is readable again) -> >=0.5.53 (four ingest scripts still
# required --db-url on argv or defaulted to a dev DSN, so policies 7/8/2658 died
# in seconds after the launcher stopped passing it; all now read
# FD_OPEN_DATA_MCP_DATABASE_URL) -> >=0.5.57 (skipped runs now record WHY they skipped;
# OPEN-past-cooldown, so a manual_reset HALF_OPEN was never resolved by anyone —
# is_selectable() excludes it and keys carry no TTL, so the proxy was stranded;
# cooldown-less OPEN keys had the same fate. Both are probe-eligible now)
# -> >=0.5.58 (a plan whose concepts were ALL refused now closes as `failed`
# with the refused codes in detail, instead of launching a pod that can only
# report zeros and closing as no_op "watermark already current" — four enabled
# policies ran that way for 54 days after their concepts were deprecated on
# 2026-09-17/19, with no error and no alert) -> >=0.5.60 (a window whose period
# has NOT finished is no longer launched: _next_period_start advances one period
# and the executor expands to that period's END, so a yearly concept whose
# watermark was the last completed year asked for <current year>-12-31 — a year
# that cannot have data. wbgapi answers it with an empty frame, every cell fails,
# and the failures open circuits on the egress: policy 4 sent 49 such requests on
# 2026-11-10 and left seven wbgapi circuits OPEN while the pool was healthy).

# fd-open-data-mcp 的来源（2026-11-11 起默认按 commit 钉住，不再"装 PyPI 最新"）
# -----------------------------------------------------------------------
# 为什么改：留存语义（`fetch/retention.py` 契约闸 + `fetch/retention_bulk.py` 批量
# 收尾，含 W 窗口裁剪与 T 只留当期）是**未发布**的代码。镜像从 PyPI 解析到 0.5.61
# 时，爬虫集群写观测完全不执行契约。库里的指纹很硬：`nav` 1,227/1,236 行、
# `tenure_days` 4,280/4,280 行、`aum_total` 4,234/4,280 行 `obs_month IS NULL`
# ——cache 路径必填该列，不填的必然是另一条（或更老的）写入栈。实测 2026-11-11：
# 基金快照族每天新增一行、旧的从不清，T 类"只留当期"在集群侧从未生效。
#
# 钉 commit 还顺带消除"同库双口径"：线上 MCP 服务端镜像走源码构建（有留存语义），
# 爬虫镜像走 PyPI（没有），两个写者对同一张表执行两套规则。
# 回退：把 FD_ODM_REF 指向想要的历史 commit 即可；要回到 PyPI 路线就 revert 这次改动。
# 必须是**完整 40 位 SHA**：`git fetch origin <短sha>` 不认（实测
# "couldn't find remote ref"），而按全 SHA 拉单个提交是 GitHub 支持的。
# 2026-11-12：34d55b6 → dcbaa57。两组语义一次拿齐，且是**纯超集**——
# `git merge-base --is-ancestor 34d55b6 dcbaa57` 为真，留存语义（闸门档位透传）
# 原样保留，只多出集群凭据生命周期那批：
#   · `visibility/credentials.py`（bound token 寿命判定，STALE_FRACTION=1/3）
#   · `refresh/reconciler.py` 的 401/403 归因（`WorkerCredentialError` +
#     消息里带可执行的 rotate 命令）与 `adopt_job_if_present`（响应丢失但 Job 已落地
#     时采纳，而不是记成 failed）
# 为什么必须抬：线上派发侧镜像一直是 0.5.61，401 只会被记成"launch failed"，
# 与"计划坏了"同形；响应丢失那一笔也一直在把成功的 Job 记成失败。
# 验证依据（抬之前实测，不是看 tag 名）：旧镜像 pod 内 `import` 不到
# `visibility.credentials`、`WorkerCredentialError` 报 ImportError；0.5.62 本仓镜像里两者都在。
#
# 2026-11-12（第二次）：dcbaa57 → 0f0070e。同样是**纯超集**
# （`git merge-base --is-ancestor dcbaa57 0f0070e` 为真），只多出这三笔：
#   · fix(astock) 限流退避 + 连续三次即中止（`RATE_LIMIT_BACKOFF_S` /
#     `run_symbols`）——原来 3 次重试之间**零等待**，5203 个符号各打 3 次，
#     akshare 自己写着"反复触发后可能需要 40 分钟以上"，于是 5 分钟封禁被拖长
#   · fix(astock) 补 `import os`（DSN 回退分支 NameError，线上 ConfigMap 那份缺）
#   · fix(scan) 挂载在但零 token 不再报 checked:true
# 为什么必须抬：`reconciler._read_script` 的搜索顺序是 `/app/scripts` →
# `/opt/fd-scripts`，**镜像内那份优先**；所以改 ConfigMap 对派发出去的 Job 无效
# （改过并同步了，仍不生效，实测镜像内 sha 不含 `RATE_LIMIT_BACKOFF_S`）。
ARG FD_ODM_REF="0f0070e13cabcbf4a870ec7bfdca1c775c4ae2e5"
ARG FD_ODM_GIT_URL="https://github.com/FindDataTechnology/fd-open-data-mcp.git"
ARG FD_ODP_INSTALL=""
ARG FD_CNREPORT_INSTALL="fd-cn-report>=0.3.3"

RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /opt/venv/bin/pip install --no-cache-dir "scrapy>=2.12,<2.13" "Twisted<25" "w3lib>=2.1,<2.5" \
 && if [ -n "$FD_ODP_INSTALL" ]; then \
        /opt/venv/bin/pip install --no-cache-dir "$FD_ODP_INSTALL"; \
    fi

# 取源码树：pip 直接从这里装，副本同时喂给 /app/scripts（direct 策略的脚本来源）。
# 用 `git fetch <sha>` 而不是 clone --depth：GitHub 允许按 SHA 拉单个提交，
# 不受 main 后续推进影响（钉住的 commit 永远可复现）。
RUN mkdir -p /build/fodm-src \
 && git -C /build/fodm-src init -q \
 && git -C /build/fodm-src remote add origin "$FD_ODM_GIT_URL" \
 && git -C /build/fodm-src fetch -q --depth 1 origin "$FD_ODM_REF" \
 && git -C /build/fodm-src checkout -q FETCH_HEAD \
 && grep -m1 __version__ /build/fodm-src/fd_open_data_mcp/__init__.py \
 && test -f /build/fodm-src/fd_open_data_mcp/fetch/retention_bulk.py \
 && grep -q apply_bulk_retention /build/fodm-src/fd_open_data_mcp/fetch/retention_bulk.py \
 && grep -q "FD_RETENTION_GATE" /build/fodm-src/fd_open_data_mcp/refresh/reconciler.py

# The dependency install is split across RUN steps deliberately: one giant
# ~350MB layer wedges transpacific registry pushes (buildx blob upload stalls
# with no timeout against ccr.ccs.tencentyun.com); ~150MB layers complete.
RUN /opt/venv/bin/pip install --no-cache-dir "/build/fodm-src[data]" \
 && /opt/venv/bin/pip install --no-cache-dir \
      "akshare>=1.18.94" \
      "pandas>=3.0.5"

# Direct-script policies (crawl_policies.executor='direct') get their script
# source read from /app/scripts at launch (reconciler _read_script), but the
# package wheel doesn't ship repo-level scripts/. Take them from the SAME
# source tree we just installed — downloading the sdists from PyPI would give
# a *different* (released) version's scripts, silently reverting the retention
# wiring those scripts now carry.
RUN mkdir -p /build/fodm-scripts \
 && cp /build/fodm-src/scripts/*.py /build/fodm-scripts/ \
 && test "$(grep -l apply_bulk_retention /build/fodm-scripts/bulk_ingest_*.py | wc -l)" -ge 4

RUN /opt/venv/bin/pip install --no-cache-dir "$FD_CNREPORT_INSTALL" \
 && /opt/venv/bin/pip install --no-cache-dir /build/vendor/fd-datacommons \
 && /opt/venv/bin/pip install --no-cache-dir /build/scraw-fd-open-data-mcp \
 # fd-cn-report's rules_db auto-seeds from indicator_rules.json; the flat-py-module
 # wheel doesn't ship the JSON next to the module, so download it to a known path
 # and point CNREPORT_RULES_JSON at it.
 && /opt/venv/bin/pip download --no-deps --no-binary :all: fd-cn-report -d /tmp/cnr-src \
 && mkdir -p /opt/rules \
 && tar -xzf /tmp/cnr-src/fd_cn_report-*.tar.gz -C /tmp/cnr-src \
 && cp /tmp/cnr-src/fd_cn_report-*/indicator_rules.json /opt/rules/indicator_rules.json \
 && rm -rf /tmp/cnr-src

FROM python:3.12-slim
WORKDIR /app
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/rules /opt/rules
COPY . /app
# bulk-ingest scripts land after the repo COPY so the scraw repo's own
# scripts/ (financial_*, proxy_*) and these share /app/scripts without
# shadowing each other; names are disjoint.
COPY --from=builder /build/fodm-scripts /app/scripts
RUN mkdir -p /app/output /plan /tmp/output /data
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CNREPORT_RULES_JSON=/opt/rules/indicator_rules.json \
    DAAS_DATABASE_URL=sqlite:////data/daas.db \
    CNREPORT_CACHE_DIR=/data/cnreport_cache
WORKDIR /app
ENTRYPOINT ["scraw-fd-open-data-mcp"]
CMD ["--help"]
