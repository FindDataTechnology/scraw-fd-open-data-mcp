# Deploy

```bash
SCRAPYD_URL=http://localhost:6800 ./deploy.sh   # build egg + scrapyd-deploy
python schedule.py concept_crawl --plan plan.json   # scrapyd /schedule.json + dup-run guard
```
The `[project.entry-points."scrapy"] settings = "scraw_fd_open_data_mcp.settings"`
entry point is required for scrapyd egg activation.
