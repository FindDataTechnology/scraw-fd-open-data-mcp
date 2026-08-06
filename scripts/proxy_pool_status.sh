#!/bin/bash
# Proxy pool health check script
# Usage: ./scripts/proxy_pool_status.sh

set -e

echo "=== Proxy Pool Health Check ==="
echo

echo "1. Proxy count by status:"
kubectl exec -n scraw fd-open-pg-789d56dbb5-fkdbl -- psql -U postgres -d postgres -c "SELECT status, count(*) FROM proxies GROUP BY status ORDER BY status;" 2>/dev/null | grep -E "active|retired" | awk '{printf "   %-10s %s\n", $1, $3}'
echo

echo "2. Recent proxy sync (last 5):"
kubectl get jobs -n scraw -l job-name=proxy-pool-sync --sort-by=.metadata.creationTimestamp 2>/dev/null | tail -5 | awk 'NR>1 {printf "   %s %s %s\n", $1, $2, $3}'
echo

echo "3. Circuit breaker state (akshare):"
kubectl exec -n scraw fd-open-redis-b5f4b9698-jg5l9 -- redis-cli -n 0 KEYS "circuit:akshare:*" 2>/dev/null | head -10 | while read key; do
  state=$(kubectl exec -n scraw fd-open-redis-b5f4b9698-jg5l9 -- redis-cli -n 0 HGET "$key" state 2>/dev/null)
  printf "   %-40s %s\n" "$key" "$state"
done
echo

echo "4. Recent fetch outcomes (last 10):"
kubectl exec -n scraw fd-open-pg-789d56dbb5-fkdbl -- psql -U postgres -d postgres -c "SELECT source, proxy_id, status, classification, timestamp FROM fetch_log ORDER BY timestamp DESC LIMIT 10;" 2>/dev/null | grep -E "akshare|yfinance" | awk '{printf "   %-10s proxy=%-3s %-10s %-10s %s\n", $1, $3, $5, $7, $9}'
echo

echo "5. CronJob schedule:"
kubectl get cronjob proxy-pool-sync -n scraw -o jsonpath='{.spec.schedule}' 2>/dev/null
echo
echo

echo "=== End of Health Check ==="
