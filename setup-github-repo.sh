#!/bin/bash
# This script will help you set up the GitHub repository

echo "=========================================="
echo "GitHub Repository Setup for scraw-fd-open-data-mcp"
echo "=========================================="
echo ""

# Step 1: Initialize git repo if needed
if [ ! -d ".git" ]; then
    echo "Initializing Git repository..."
    git init
    git add .
    git commit -m "Initial commit"
    echo "✓ Git repo initialized"
else
    echo "✓ Git repo already exists"
fi

# Step 2: Add remote origin (you'll need to create the repo first)
echo ""
echo "Next steps:"
echo "1. Go to https://github.com/new"
echo "2. Create a NEW private repository named: scraw-fd-open-data-mcp"
echo "3. Owner/Username: FindDataOfficial (or your personal account)"
echo "4. After creation, run these commands:"
echo ""
echo "   cd /Users/chengsishi/finddata/scraw-fd-open-data-mcp"
echo "   git remote add origin https://github.com/FindDataOfficial/scraw-fd-open-data-mcp.git"
echo "   git branch -M main"
echo "   git push -u origin main"
echo ""
echo "5. Then configure GitHub Secrets in Settings → Secrets and variables → Actions"
echo ""
echo "Required Secrets:"
echo "  - HARBOR_REGISTRY: 23.144.68.246:30880"
echo "  - HARBOR_USERNAME: robot\$lawcraw_business"
echo "  - HARBOR_PASSWORD: ${HARBOR_PASSWORD:-read-from-operator-creds}"
echo "  - ARGOCD_SERVER: https://23.144.68.246:30910"
echo "  - ARGOCD_USERNAME: admin"
echo "  - ARGOCD_PASSWORD: eZlHCI2ieChpgswj"
echo ""
