# scraw-fd-open-data-mcp CI/CD Setup

This directory contains the complete CI/CD configuration for deploying `scraw-fd-open-data-mcp` to Kubernetes via GitHub Actions, Harbor, and Argo CD.

## Overview

```mermaid
graph LR
    A[Git Push] --> B[GitHub Actions]
    B --> C[Build Docker Image]
    C --> D[Push to Harbor]
    D --> E[Argo CD detects change]
    E --> F[Deploy to K8s]
```

## Components

1. **GitHub Repository**: Private repo at `FindDataOfficial/scraw-fd-open-data-mcp`
2. **GitHub Actions**: Automates build and push to Harbor
3. **Harbor Registry**: Stores Docker images at `23.144.68.246:30880/fd-open-data/scraw-fd-open-data-mcp`
4. **Kubernetes**: Deployments managed by Argo CD
5. **Argo CD**: GitOps controller for automatic deployments

## Setup Instructions

### Step 1: Create Harbor Project

In Harbor UI or via API:
```bash
# Already created: fd-open-data project
# Robot account: robot$lawcraw_business
```

### Step 2: Configure GitHub Secrets

Go to your GitHub repository → Settings → Secrets and variables → Actions

Add these secrets:

| Secret Name | Value | Description |
|-------------|-------|-------------|
| `HARBOR_REGISTRY` | `23.144.68.246:30880` | Harbor registry address |
| `HARBOR_USERNAME` | `robot$lawcraw_business` | Robot account username |
| `HARBOR_PASSWORD` | `REDACTED-HARBOR-ROBOT-PASSWORD` | Robot account password |
| `ARGOCD_SERVER` | `https://23.144.68.246:30910` | Argo CD server URL |
| `ARGOCD_USERNAME` | `admin` | Argo CD admin username |
| `ARGOCD_PASSWORD` | `eZlHCI2ieChpgswj` | Argo CD admin password |
| `GITHUB_TOKEN` | (auto-generated) | For pushing updated manifests |

### Step 3: Initialize GitHub Repository

```bash
cd /Users/chengsishi/finddata/scraw-fd-open-data-mcp

# Initialize git if not done
git init
git add .
git commit -m "Initial commit"

# Create remote (do this in GitHub web UI first)
gh repo create scraw-fd-open-data-mcp --private --source=. --remote=origin
git push -u origin main
```

### Step 4: Deploy to Kubernetes

Apply the Argo CD application manifest:

```bash
kubectl apply -f argocd-application.yaml
```

Or use Argo CD CLI:

```bash
argocd app create scraw-fd-open-data-mcp \
  --repo https://github.com/FindDataOfficial/scraw-fd-open-data-mcp.git \
  --path k8s \
  --dest-server https://kubernetes.default.svc \
  --dest-namespace fd-open-data \
  --sync-policy automated --self-heal --prune
  
argocd login https://23.144.68.246:30910 \
  --username admin --password eZlHCI2ieChpgswj
  
argocd app sync scraw-fd-open-data-mcp
```

### Step 5: Create Kubernetes Secrets

```bash
kubectl create secret generic fd-open-data-secrets \
  --namespace fd-open-data \
  --from-literal=DATABASE_URL="postgresql+psycopg2://admin:admin123@192.168.1.4:5433/postgres" \
  --from-literal=REDIS_URL="redis://192.168.1.4:6379/0" \
  --from-literal=SCRAPYD_URL="http://scrapyd.scrapyd-ops:6800" \
  --dry-run=client -o yaml | kubectl apply -f -
```

### Step 6: Create Harbor Registry Secret

```bash
kubectl create secret docker-registry harbor-registry-secret \
  --namespace fd-open-data \
  --docker-server=23.144.68.246:30880 \
  --docker-username='robot$lawcraw_business' \
  --docker-password='REDACTED-HARBOR-ROBOT-PASSWORD' \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Workflow Triggers

- **Push to `main` or `master`**: Full CI/CD pipeline (build → push → deploy)
- **Pull Request**: Build and test only (no deployment)

## Monitoring

### Check GitHub Actions
- URL: `https://github.com/FindDataOfficial/scraw-fd-open-data-mcp/actions`
- Look for the "CI/CD - Build, Push & Deploy" workflow

### Check Argo CD
- URL: `https://23.144.68.246:30910`
- Login with admin credentials
- Find application: `scraw-fd-open-data-mcp`
- Check sync status and health

### Check Pod Status
```bash
kubectl get pods -n fd-open-data
kubectl logs -n fd-open-data -l app=scraw-fd-open-data-mcp
```

## Updating Images

The workflow automatically updates the image tag on each push:
- Image: `23.144.68.246:30880/fd-open-data/scraw-fd-open-data-mcp:{git-sha}`
- Also tagged as `latest` for convenience

## Rollback

If needed, rollback to previous version:

```bash
# In Argo CD UI: 
#   Navigate to Application → History → Revert to previous revision

# Or via CLI:
argocd app revert scraw-fd-open-data-mcp
argocd app sync scraw-fd-open-data-mcp
```

## Troubleshooting

### Build fails
- Check Dockerfile syntax
- Verify dependencies in `pyproject.toml`
- Review GitHub Actions logs for specific errors

### Image push fails
- Verify Harbor credentials in GitHub secrets
- Check if Harbor project exists and is accessible
- Ensure robot account has write permissions

### Deployment fails
- Check Kubernetes event logs: `kubectl describe pod -n fd-open-data`
- Verify secrets are created correctly
- Ensure Harbor secret exists in namespace

### Argo CD sync fails
- Check Argo CD application status in UI
- Verify repo URL is accessible
- Confirm path to k8s manifests is correct
