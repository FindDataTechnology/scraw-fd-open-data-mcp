# Multi-stage build for scraw-fd-open-data-mcp.
#
# fd-open-data-mcp is installed FROM PYPI (not vendored) so pushing a new
# fd-open-data-mcp release + rebuilding this image = automatically picks up new
# data-source adapters + the proxy-health modules. Override with a build-arg to
# vendor a local checkout for development:
#   docker build --build-arg FD_ODM_INSTALL=/build/fd-open-data-mcp[data] \
#     --build-arg FD_ODP_INSTALL=/build/fd-open-data-protocol -t ... .
# (when vendoring, the build context must include those dirs; keep the [data]
# extra — without it the image silently loses yfinance/edgartools/… and any
# datasource or probe that imports them crashes with ModuleNotFoundError.)
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
# image must never silently re-fatten with scrapling/playwright).

ARG FD_ODM_INSTALL="fd-open-data-mcp[data]>=0.5.34"
ARG FD_ODP_INSTALL=""
ARG FD_CNREPORT_INSTALL="fd-cn-report>=0.3.3"

RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /opt/venv/bin/pip install --no-cache-dir "scrapy>=2.12,<2.13" "Twisted<25" "w3lib>=2.1,<2.5" \
 && if [ -n "$FD_ODP_INSTALL" ]; then \
        /opt/venv/bin/pip install --no-cache-dir "$FD_ODP_INSTALL"; \
    fi

# The dependency install is split across RUN steps deliberately: one giant
# ~350MB layer wedges transpacific registry pushes (buildx blob upload stalls
# with no timeout against ccr.ccs.tencentyun.com); ~150MB layers complete.
RUN /opt/venv/bin/pip install --no-cache-dir "$FD_ODM_INSTALL" \
 && /opt/venv/bin/pip install --no-cache-dir \
      "akshare>=1.18.94" \
      "pandas>=3.0.5"

# Direct-script policies (crawl_policies.executor='direct') get their script
# source read from /app/scripts at launch (reconciler _read_script), but the
# fd-open-data-mcp wheel doesn't ship repo-level scripts/. Pull them from the
# sdist instead — same recipe as the fd-cn-report rules_db below. Requires the
# sdist to carry scripts/ (fd-open-data-mcp MANIFEST.in recursive-include).
RUN /opt/venv/bin/pip download --no-deps --no-binary :all: fd-open-data-mcp -d /tmp/fodm-src \
 && tar -xzf /tmp/fodm-src/fd_open_data_mcp-*.tar.gz -C /tmp/fodm-src \
 && mkdir -p /build/fodm-scripts \
 && cp /tmp/fodm-src/fd_open_data_mcp-*/scripts/*.py /build/fodm-scripts/ \
 && rm -rf /tmp/fodm-src

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
