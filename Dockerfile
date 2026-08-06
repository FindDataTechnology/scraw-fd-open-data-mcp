# Multi-stage build for scraw-fd-open-data-mcp.
#
# fd-open-data-mcp is installed FROM PYPI (not vendored) so pushing a new
# fd-open-data-mcp release + rebuilding this image = automatically picks up new
# data-source adapters + the proxy-health modules. Override with a build-arg to
# vendor a local checkout for development:
#   docker build --build-arg FD_ODM_INSTALL=/build/fd-open-data-mcp \
#     --build-arg FD_ODP_INSTALL=/build/fd-open-data-protocol -t ... .
# (when vendoring, the build context must include those dirs.)
#
# scraw-fd-open-data-mcp itself is vendored (it's the crawler, not on PyPI).
#
# Pins: scrapy>=2.12,<2.13 (2.13+ broke start_requests + sync download_handler),
# Twisted<25 (removed _setAcceptableProtocols that scrapy 2.12 needs), akshare
# (optional extra in fd-open-data-mcp - install explicitly).

FROM python:3.12-slim AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libssl-dev libffi-dev git && rm -rf /var/lib/apt/lists/*
COPY . /build/scraw-fd-open-data-mcp

ARG FD_ODM_INSTALL="fd-open-data-mcp>=0.4.1"
ARG FD_ODP_INSTALL=""
ARG FD_CNREPORT_INSTALL="fd-cn-report>=0.3.2"

RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /opt/venv/bin/pip install --no-cache-dir "scrapy>=2.12,<2.13" "Twisted<25" "akshare>=1.17" \
 && if [ -n "$FD_ODP_INSTALL" ]; then \
        /opt/venv/bin/pip install --no-cache-dir "$FD_ODP_INSTALL"; \
    fi \
 && /opt/venv/bin/pip install --no-cache-dir "$FD_ODM_INSTALL" \
 && /opt/venv/bin/pip install --no-cache-dir "$FD_CNREPORT_INSTALL" \
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
