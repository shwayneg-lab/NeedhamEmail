"""Daily digest orchestrator — runs twice a day via GitHub Actions."""
from __future__ import annotations

import csv
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pytz

from src import mailer
from src.digest import (
    build_earnings_week,
    build_movers,
    build_rating_actions,
    build_watchlist,
)
from src.fetch import fetch_ticker
from src.render import render_digest, render_index, render_ticker_page

REPO_FULL = os.environ.get("GITHUB_REPOSITORY", "shwayneg-lab/NeedhamEmail")
REPO_OWNER, REPO_NAME = (REPO_FULL.split("/", 1) + ["NeedhamEmail"])[:2]
PAGES_BASE = f"https://{REPO_OWNER.lower()}.github.io/{REPO_NAME}"

COVERAGE_FILE = os.environ.get("COVERAGE_FILE", "coverage.csv")
SKIP_EMAIL = os.environ.get("DIGEST_SKIP_EMAIL") == "1"
SKIP_TIME_CHECK = os.environ.get("DIGEST_SKIP_TIME_CHECK") == "1"


def load_coverage(path: str) -> list[dict]:
    with open(path) as f:
        return list(csv.DictReader(f))


def fetch_all(coverage: list[dict]) -> list[dict]:
    results = []
    done = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_ticker, row["ticker"]): row for row in coverage}
        for fut in as_completed(futures):
            row = futures[fut]
            try:
                data = fut.result()
            except Exception as e:
                print(f"  {row['ticker']} failed: {e}", file=sys.stderr)
                data = {"ticker": row["ticker"], "news": [], "upgrades": []}
            data["company_name"] = row.get("company_name", "")
            data["analyst"] = row.get("analyst", "")
            data["sector"] = row.get("sector", "")
            results.append(data)
            done += 1
            if done % 50 == 0:
                print(f"[{done}/{len(coverage)}] fetched", flush=True)
    results.sort(key=lambda r: r["ticker"])
    return results


def main() -> int:
    mode = os.environ.get("DIGEST_MODE", "morning")
    et = pytz.timezone("America/New_York")
    now_et = datetime.now(et)

    if not SKIP_TIME_CHECK:
        if now_et.weekday() >= 5:
            print(f"Weekend ({now_et:%a}), skipping")
            return 0
        hour = now_et.hour
        if mode == "morning" and hour not in (6, 7):
            print(f"Not in 7am ET window (hour={hour}), skipping")
            return 0
        if mode == "closing" and hour not in (16, 17):
            print(f"Not in 5:30pm ET window (hour={hour}), skipping")
            return 0

    print(f"Running {mode} digest at {now_et:%Y-%m-%d %H:%M %Z}")

    coverage = load_coverage(COVERAGE_FILE)
    print(f"Loaded {len(coverage)} tickers from {COVERAGE_FILE}")

    state_path = Path("state/prev_ratings.json")
    prev_state = {}
    if state_path.exists():
        try:
            prev_state = json.loads(state_path.read_text() or "{}")
        except json.JSONDecodeError:
            prev_state = {}

    results = fetch_all(coverage)

    watchlist = build_watchlist(results, mode, now_et)
    movers = build_movers(results)
    earnings = build_earnings_week(results, now_et)

    if not prev_state:
        print("No previous state — seeding baseline, skipping rating-actions section")
        new_actions = []
    else:
        new_actions = build_rating_actions(results, prev_state)

    print(
        f"Watchlist: {len(watchlist)} | "
        f"Gainers/Decliners: {len(movers['gainers'])}/{len(movers['decliners'])} | "
        f"Earnings window: {len(earnings)} | "
        f"New actions: {len(new_actions)}"
    )

    tickers_dir = Path("docs/tickers")
    tickers_dir.mkdir(parents=True, exist_ok=True)
    for row in results:
        html = render_ticker_page(row, PAGES_BASE)
        (tickers_dir / f"{row['ticker']}.html").write_text(html)
    Path("docs/index.html").write_text(render_index(results, PAGES_BASE))
    print(f"Rendered {len(results)} ticker pages + index")

    subject_tag = "AM" if mode == "morning" else "PM"
    subject = f"Needham Digest — {now_et:%a %b %d} {subject_tag}"
    email_html = render_digest(mode, watchlist, movers, earnings, new_actions, now_et, PAGES_BASE)
    Path("docs/latest.html").write_text(email_html)

    if SKIP_EMAIL:
        print("DIGEST_SKIP_EMAIL=1 — not sending email")
    else:
        code = mailer.send(subject, email_html)
        print(f"Email: HTTP {code}")

    state_path.parent.mkdir(exist_ok=True)
    new_state = {r["ticker"]: r.get("upgrades", [])[:5] for r in results}
    state_path.write_text(json.dumps(new_state, indent=2))
    print("State saved")

    return 0


if __name__ == "__main__":
    sys.exit(main())
