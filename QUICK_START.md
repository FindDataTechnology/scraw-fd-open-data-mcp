# Quick Start Guide - Complete CI/CD Pipeline

This guide will help you set up the full deployment pipeline for `scraw-fd-open-data-mcp`.

## 📋 Prerequisites Checklist

- [x] Git repository initialized in `/Users/chengsishi/finddata/scraw-fd-open-data-mcp`
- [ ] GitHub private repo created at `https://github.com/FindDataOfficial/scraw-fd-open-data-mcp`
- [ ] Harbor project `fd-open-data` exists (robot account: `robot$lawcraw_business`)
- [ ] Kubernetes cluster accessible via kubectl
- [ ] Argo CD installed at `https://23.144.68.246:30910`

## 🚀 Step-by-Step Setup

### Step 1: Push Code to GitHub

```bash
cd /Users/chengsishi/finddata/scraw-fd-open-data-mcp

# Create repo on GitHub first: https://github.com/new
# Name: scraw-fd-open-data-mcp
# Owner: FindDataOfficial (or your username)
# Visibility: Private

git remote add origin https://github.com/FindDataOfficial/scraw-fd-open-data-mcp.git
git branch -M main
git push -u origin main
```

### Step 2: Configure GitHub Secrets

Go to: `https://github.com/FindDataOfficial/scraw-fd-open-data-mcp/settings/secrets/actions`

Add these secrets:

| Name | Value |
|------|-------|
| `HARBOR_REGISTRY` | `23.144.68.246:30880` |
| `HARBOR_USERNAME` | `robot$lawcraw_business` |
| `HARBOR_PASSWORD` | `REDACTED-HARBOR-ROBOT-PASSWORD` |
| `ARGOCD_SERVER` | `https://23.144.68.246:30910` |
| `ARGOCD_USERNAME` | `admin` |
| `ARGOCD_PASSWORD` | `eZlHCI2ieChpgswj` |

### Step 3: Deploy to Kubernetes

#### Option A: Use automated script

```bash
cd /Users/chengsishi/finddata/scraw-fd-open-data-mcp
./setup-k8s.sh
```

#### Option B: Manual steps

```bash
# Create namespace
kubectl create namespace fd-open-data --dry-run=client -o yaml | kubectl apply -f -

# Create Harbor registry secret
kubectl create secret docker-registry harbor-registry-secret \
  --namespace=fd-open-data \
  --docker-server=23.144.68.246:30880 \
  --docker-username='robot$lawcraw_business' \
  --docker-password='REDACTED-HARBOR-ROBOT-PASSWORD' \
  --dry-run=client -o yaml | kubectl apply -f -

# Create application secrets
DATABASE_URL="postgresql+psycopg2://admin:admin123@192.168.1.4:5433/postgres"
kubectl create secret generic fd-open-data-secrets \
  --namespace=fd-open-data \
  --from-literal=DATABASE_URL="${DATABASE_URL}" \
  --from-literal=REDIS_URL="redis://192.168.1.4:6379/0" \
  --from-literal=SCRAPYD_URL="http://scrapyd.scrapyd-ops:6800" \
  --dry-run=client -o yaml | kubectl apply -f -

# Apply deployment
sed "s|image: .*|image: 23.144.68.246:30880/fd-open-data/scraw-fd-open-data-mcp:latest|" \
  k8s/deployment.yaml | kubectl apply -f - -n fd-open-data
```

### Step 4: Set up Argo CD (Optional but Recommended)

```bash
# Login to Argo CD
argocd login https://23.144.68.246:30910 \
  --username admin \
  --password eZlHCI2ieChpgswj \
  --insecure

# Create application
argocd app create scraw-fd-open-data-mcp \
  --repo https://github.com/FindDataOfficial/scraw-fd-open-data-mcp.git \
  --path k8s \
  --dest-server https://kubernetes.default.svc \
  --dest-namespace fd-open-data \
  --sync-policy automated --self-heal --prune \
  --project default
  
# Or simply apply the YAML:
kubectl apply -f argocd-application.yaml
```

## ✅ Verify Everything is Working

### Check GitHub Actions
```
Visit: https://github.com/FindDataOfficial/scraw-fd-open-data-mcp/actions
Expected: Workflow "CI/CD - Build, Push & Deploy" should be listed
```

### Check Harbor
```
Visit: http://23.144.68.246:30880
Navigate: Projects → fd-open-data → Images → scraw-fd-open-data-mcp
Expected: You'll see images with tags like sha-xxxxxx and latest
```

### Check Kubernetes Pods
```bash
kubectl get pods -n fd-open-data -w
# Expected: Pod should transition from ContainerCreating → Running
```

### Check Argo CD
```bash
argocd app list
argocd app get scraw-fd-open-data-mcp
```

## 🔁 How Updates Work

Once configured, the pipeline works automatically:

1. **You push code** → GitHub Actions triggers
2. **Build & Push** → Docker image built and pushed to Harbor
3. **Argo CD detects** → New image tag in git repo
4. **Auto deploy** → Kubernetes updates to new image

## 📝 Files Created

```
scraw-fd-open-data-mcp/
├── .github/workflows/
│   └── cicd.yml              # CI/CD workflow
├── k8s/
│   └── deployment.yaml       # K8s manifests
├── Dockerfile                # Container build config
├── argocd-application.yaml   # Argo CD app definition
├── CICD_SETUP.md             # Full documentation
├── QUICK_START.md           # This file
├── setup-github-repo.sh     # Helper script
└── setup-k8s.sh             # Deployment script
```

## 🐛 Troubleshooting

### GitHub Actions fails to build
- Check Dockerfile syntax
- Verify Python dependencies in `pyproject.toml`
- Review action logs for specific errors

### Image push to Harbor fails
- Verify Harbor credentials in GitHub secrets
- Ensure robot account has write permissions
- Test: `docker pull 23.144.68.246:30880/fd-open-data/scraw-fd-open-data-mcp`

### Pod stays in CrashLoopBackOff
```bash
# Check pod logs
kubectl logs -n fd-open-data -l app=scraw-fd-open-data-mcp

# Check events
kubectl describe pod -n fd-open-data -l app=scraw-fd-open-data-mcp

# Verify secrets exist
kubectl get secrets -n fd-open-data
```

### Argo CD shows sync failure
```bash
# Check application status
argocd app get scraw-fd-open-data-mcp

# Sync manually
argocd app sync scraw-fd-open-data-mcp

# Rollback if needed
argocd app revert scraw-fd-open-data-mcp
```

## 🎯 Next Steps

1. Test the pipeline with a small code change
2. Monitor the GitHub Actions run
3. Verify the new image appears in Harbor
4. Watch Argo CD sync the update to Kubernetes
5. Check pod health and logs

Need more help? Check `CICD_SETUP.md` for detailed instructions!
