# Master control plane - independent Postgres + Redis + PgBouncer
#
# The single source of truth for the multi-cluster fleet. Dedicated (independent)
# services in their own namespace - NOT co-located with Harbor/Argo/other infra.
# Deploy on the US cloud server; worker clusters (anywhere in the world) connect
# here over TLS.
#
#   scraw-fd-open-data-mcp crawl  ──► master-pg (PgBouncer) ──► master-postgres
#                                  ──► master-redis
#
# The reconciler also runs here (see crawl-reconciler-cronjob.yaml) so it can
# reach the master DB + all worker-cluster APIs.
#
# Apply order:
#   kubectl apply -f k8s/master/namespace.yaml
#   kubectl apply -f k8s/master/master-postgres.yaml
#   kubectl apply -f k8s/master/master-redis.yaml
#   kubectl apply -f k8s/master/master-pgbouncer.yaml
#
# Secrets (create before applying the workloads):
#   kubectl create secret generic master-db-secret -n fd-master \
#     --from-literal=POSTGRES_PASSWORD='<strong-password>' \
#     --from-literal=DATABASE_URL='postgresql+psycopg2://postgres:<strong-password>@master-pg.fd-master:5432/fd_open_data'
#   kubectl create secret generic master-redis-secret -n fd-master \
#     --from-literal=REDIS_PASSWORD='<strong-password>'
#
# Migrate the existing control-plane data in ONCE (HISTORICAL 2026-08-11 -- the
# LAN source 192.168.1.4 is retired read-only since 2026-08-18; everything now
# lives in the canonical DB on guangzhou-xinru :30432):
#   pg_dump -h 192.168.1.4 -p 5433 -U postgres postgres \
#     -t crawl_policies -t policy_runs -t semantic_observations -t concepts \
#     -t concept_bindings -t sources -t functions -t ... \
#   | psql -h master-pg.fd-master -U postgres fd_open_data
#   fd-open-data-mcp migrate   # adds clusters table + policy_runs/proxies.cluster_id
