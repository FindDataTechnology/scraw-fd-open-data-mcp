"""Schedule a crawl run via the scrapyd HTTP API with a duplicate-run guard.

Usage: python schedule.py concept_crawl --plan /path/to/plan.json
"""
import argparse
import json
import os
import sys
import urllib.request

SCRAPYD_URL = os.environ.get("SCRAPYD_URL", "http://localhost:6800")
PROJECT = "scraw_fd_open_data_mcp"


def _listjobs():
    with urllib.request.urlopen(f"{SCRAPYD_URL}/listjobs.json?project={PROJECT}", timeout=10) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("spider")
    ap.add_argument("--plan", required=True, help="CrawlPlan JSON path")
    args = ap.parse_args()

    jobs = _listjobs()
    if jobs.get("running") or jobs.get("pending"):
        print(f"a job is already running/pending for {PROJECT}; not scheduling", file=sys.stderr)
        sys.exit(1)

    data = urllib.parse.urlencode({
        "project": PROJECT, "spider": args.spider, "setting": "BOT_NAME=" + PROJECT,
        "arg0": "plan", "arg1": args.plan,
    }).encode()
    with urllib.request.urlopen(f"{SCRAPYD_URL}/schedule.json", data, timeout=10) as r:
        result = json.load(r)
    print(json.dumps(result))
    if result.get("jobid"):
        print(f"jobid={result['jobid']}")


import urllib.parse  # noqa: E402

if __name__ == "__main__":
    main()
