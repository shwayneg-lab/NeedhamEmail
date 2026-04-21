"""Daily digest orchestrator — runs twice a day via GitHub Actions."""
from __future__ import annotations

import csv
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytz

from src import mailer
from src.digest import (
    build_earnings_week,
    build_movers,
    build_rating_actions,
    build_watchlist,
)
from src.fetch import fetch_ticker, save_desc_cache
from src.render import render_digest, render_index, render_ticker_page

REPO_FULL = os.environ.get("GITHUB_REPOSITORY", "shwayneg-lab/NeedhamEmail")
REPO_OWNER, REPO_NAME = (REPO_FULL.split("/", 1) + ["NeedhamEmail"])[:2]
PAGES_BASE = f"https://{REPO_OWNER.lower()}.github.io/{REPO_NAME}"

COVERAGE_FILE = os.environ.get("COVERAGE_FILE", "coverage.csv")
SKIP_EMAIL = os.environ.get("DIGEST_SKIP_EMAIL") == "1"
SKIP_TIME_CHECK = os.environ.get("DIGEST_SKIP_TIME_CHECK") == "1"

FULL_CACHE_PATH = Path("state/full_data.json")
HISTORY_PATH = Path("state/price_history.json")
HISTORY_MAX_DAYS = 30


def load_coverage(path: str) -> list[dict]:
    with open(path) as f:
        return list(csv.DictReader(f))


def _empty_result(ticker: str) -> dict:
    return {
        "ticker": ticker,
        "price": None,
        "pct_1d": None,
        "pct_5d": None,
        "next_earnings": None,
        "consensus": None,
        "price_target": None,
        "upside_pct": None,
        "n_analysts": None,
        "description": "",
        "news": [],
        "upgrades": [],
    }


def load_price_history() -> dict:
    if HISTORY_PATH.exists():
        try:
            return json.loads(HISTORY_PATH.read_text() or "{}")
        except json.JSONDecodeError:
            return {}
    return {}


def save_price_history(history: dict, now_et: datetime):
    cutoff = (now_et.date() - timedelta(days=HISTORY_MAX_DAYS)).isoformat()
    pruned = {
        t: {d: p for d, p in prices.items() if d >= cutoff}
        for t, prices in history.items()
    }
    HISTORY_PATH.parent.mkdir(exist_ok=True)
    HISTORY_PATH.write_text(json.dumps(pruned, indent=2, sort_keys=True))


def update_price_history_and_compute_5d(results: list[dict], history: dict, now_et: datetime):
    today = now_et.strftime("%Y-%m-%d")
    for r in results:
        if r.get("price") is None:
            continue
        ticker_hist = history.setdefault(r["ticker"], {})
        ticker_hist[today] = r["price"]
    for r in results:
        prices = history.get(r["ticker"], {})
        if len(prices) < 6 or r.get("price") is None:
            continue
        dates = sorted(prices.keys())
        if today not in dates:
            continue
        idx = dates.index(today)
        if idx >= 5:
            five_ago_price = prices[dates[idx - 5]]
            if five_ago_price > 0:
                r["pct_5d"] = round((r["price"] / five_ago_price - 1) * 100, 2)


def fetch_shallow(coverage: list[dict]) -> list[dict]:
    """Pass 1: quote + 5-day + description for all tickers (sequential, rate-limited)."""
    results = []
    for i, row in enumerate(coverage, start=1):
        try:
            data = fetch_ticker(row["ticker"], deep=False)
        except Exception as e:
            print(f"  {row['ticker']} failed: {e}", file=sys.stderr)
            data = _empty_result(row["ticker"])
        data["company_name"] = row.get("company_name", "")
        data["analyst"] = row.get("analyst", "")
        data["sector"] = row.get("sector", "")
        results.append(data)
        if i % 50 == 0:
            print(f"[{i}/{len(coverage)}] shallow fetched", flush=True)
    results.sort(key=lambda r: r["ticker"])
    n_price = sum(1 for r in results if r.get("price") is not None)
    n_1d = sum(1 for r in results if r.get("pct_1d") is not None)
    n_desc = sum(1 for r in results if r.get("description"))
    print(
        f"Shallow coverage — price: {n_price}/{len(results)} | "
        f"1d: {n_1d} | desc: {n_desc}"
    )
    return results


def fetch_deep_tickers(tickers: set[str], full_cache: dict) -> dict:
    """Pass 2: recommendation + price target + news for digest-relevant tickers only."""
    for i, ticker in enumerate(sorted(tickers), start=1):
        try:
            deep = fetch_ticker(ticker, deep=True)
            full_cache[ticker] = {
                "consensus": deep.get("consensus"),
                "price_target": deep.get("price_target"),
                "upside_pct": deep.get("upside_pct"),
                "n_analysts": deep.get("n_analysts"),
                "news": deep.get("news", []),
                "updated": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            print(f"  {ticker} deep failed: {e}", file=sys.stderr)
        if i % 10 == 0:
            print(f"[{i}/{len(tickers)}] deep fetched", flush=True)
    return full_cache


def load_full_cache() -> dict:
    if FULL_CACHE_PATH.exists():
        try:
            return json.loads(FULL_CACHE_PATH.read_text() or "{}")
        except json.JSONDecodeError:
            return {}
    return {}


def save_full_cache(cache: dict):
    FULL_CACHE_PATH.parent.mkdir(exist_ok=True)
    FULL_CACHE_PATH.write_text(json.dumps(cache, indent=2, sort_keys=True))


def merge_cache_into_results(results: list[dict], cache: dict):
    for r in results:
        c = cache.get(r["ticker"])
        if not c:
            continue
        for field in ("consensus", "price_target", "upside_pct", "n_analysts", "news"):
            if c.get(field) is not None and not r.get(field):
                r[field] = c[field]
        if r.get("price") and r.get("price_target"):
            r["upside_pct"] = round((r["price_target"] / r["price"] - 1) * 100, 2)


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

    print("Pass 1: shallow fetch (quote + description)")
    results = fetch_shallow(coverage)
    save_desc_cache()

    print("Updating local price history + computing 5-day moves")
    history = load_price_history()
    update_price_history_and_compute_5d(results, history, now_et)
    save_price_history(history, now_et)
    n_5d = sum(1 for r in results if r.get("pct_5d") is not None)
    print(f"5-day available for {n_5d}/{len(results)} tickers")

    watchlist = build_watchlist(results, mode, now_et)
    movers = build_movers(results)
    earnings = build_earnings_week(results, now_et)

    digest_tickers = set()
    digest_tickers.update(item["ticker"] for item in watchlist)
    digest_tickers.update(m["ticker"] for m in movers["gainers"])
    digest_tickers.update(m["ticker"] for m in movers["decliners"])
    digest_tickers.update(e["ticker"] for e in earnings)

    print(f"Pass 2: deep fetch for {len(digest_tickers)} digest-relevant tickers")
    full_cache = load_full_cache()
    full_cache = fetch_deep_tickers(digest_tickers, full_cache)
    save_full_cache(full_cache)
    merge_cache_into_results(results, full_cache)

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
