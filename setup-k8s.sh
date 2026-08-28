#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
NAMESPACE="fd-open-data"
DEPLOYMENT_NAME="scraw-fd-open-data-mcp"
REGISTRY="23.144.68.246:30880"
HARBOR_PROJECT="fd-open-data"
IMAGE_NAME="${REGISTRY}/${HARBOR_PROJECT}/scraw-fd-open-data-mcp"

# Harbor credentials (from .env)
HARBOR_USER="robot\$lawcraw_business"
HARBOR_PASS="REDACTED-HARBOR-ROBOT-PASSWORD"

# Canonical store on guangzhou-xinru (mesh peers reach it by name; real
# passwords live in guangzhou-xinru:/etc/fd-open-data/db-credentials.env)
PG_HOST="guangzhou-xinru"
PG_PORT="30432"
PG_USER="fd"
PG_PASS="FD_PG_PASSWORD"
PG_DB="fd_open_data"
REDIS_URL="redis://:FD_REDIS_PASSWORD@guangzhou-xinru:30380/0"
SCRAPYD_URL="http://scrapyd.scrapyd-ops:6800"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  scraw-fd-open-data-mcp K8s Setup${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Step 1: Check if kubectl is configured
echo -e "${YELLOW}[1/6] Checking kubectl configuration...${NC}"
if ! kubectl cluster-info &>/dev/null; then
    echo -e "${RED}Error: Cannot connect to Kubernetes cluster${NC}"
    echo "Please configure kubectl first (check kubeconfig)"
    exit 1
fi
echo -e "${GREEN}✓ Connected to Kubernetes cluster${NC}"

# Step 2: Create namespace
echo -e "${YELLOW}[2/6] Creating namespace ${NAMESPACE}...${NC}"
kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
echo -e "${GREEN}✓ Namespace created${NC}"

# Step 3: Create Harbor registry secret
echo -e "${YELLOW}[3/6] Creating Harbor registry secret...${NC}"
kubectl delete secret harbor-registry-secret -n ${NAMESPACE} --ignore-not-found
kubectl create secret docker-registry harbor-registry-secret \
    --namespace=${NAMESPACE} \
    --docker-server=${REGISTRY} \
    --docker-username=${HARBOR_USER} \
    --docker-password=${HARBOR_PASS} \
    --dry-run=client -o yaml | kubectl apply -f -
echo -e "${GREEN}✓ Harbor secret created${NC}"

# Step 4: Create application secrets
echo -e "${YELLOW}[4/6] Creating database and Redis secrets...${NC}"
DATABASE_URL="postgresql+psycopg2://${PG_USER}:${PG_PASS}@${PG_HOST}:${PG_PORT}/${PG_DB}"

kubectl delete secret fd-open-data-secrets -n ${NAMESPACE} --ignore-not-found
kubectl create secret generic fd-open-data-secrets \
    --namespace=${NAMESPACE} \
    --from-literal=DATABASE_URL="${DATABASE_URL}" \
    --from-literal=REDIS_URL="${REDIS_URL}" \
    --from-literal=SCRAPYD_URL="${SCRAPYD_URL}" \
    --dry-run=client -o yaml | kubectl apply -f -
echo -e "${GREEN}✓ Secrets created${NC}"

# Step 5: Apply deployment
echo -e "${YELLOW}[5/6] Applying Kubernetes deployment...${NC}"
# Update image tag in deployment
sed "s|image: .*|image: ${IMAGE_NAME}:latest|" k8s/deployment.yaml > /tmp/deployment-updated.yaml
kubectl apply -f /tmp/deployment-updated.yaml -n ${NAMESPACE}
echo -e "${GREEN}✓ Deployment applied${NC}"

# Step 6: Verify deployment
echo -e "${YELLOW}[6/6] Verifying deployment...${NC}"
sleep 5
kubectl get pods -n ${NAMESPACE} -l app=${DEPLOYMENT_NAME}
echo -e "${GREEN}✓ Deployment complete!${NC}"
echo ""
echo -e "${GREEN}Next steps:${NC}"
echo "1. Watch pod startup: kubectl logs -n ${NAMESPACE} -f -l app=${DEPLOYMENT_NAME}"
echo "2. Check service: kubectl get svc -n ${NAMESPACE}"
echo "3. For Argo CD integration, apply argocd-application.yaml"
