"""Local bootstrap — run on your Mac (NOT in GitHub Actions).

Yahoo/yfinance blocks cloud IPs but works fine from home WiFi.
This script pulls:
  1. Rich paragraph-long company descriptions (longBusinessSummary)
  2. 15 days of historical closes (seeds 5-day price-change calcs)

Both are written to state/ and committed. Future cloud runs read from them.

Usage:
    cd ~/NeedhamEmail
    python3 -m pip install yfinance pandas
    python3 scripts/populate.py
    git add state/ && git commit -m "Bootstrap descriptions + price history" && git push
"""
from __future__ import annotations

import csv
import json
import sys
import time
from pathlib import Path

import yfinance as yf

COVERAGE = Path("coverage.csv")
DESC_PATH = Path("state/descriptions.json")
HISTORY_PATH = Path("state/price_history.json")


def load_existing(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text() or "{}")
        except json.JSONDecodeError:
            return {}
    return {}


def save(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def main():
    tickers = []
    with open(COVERAGE) as f:
        for row in csv.DictReader(f):
            tickers.append(row["ticker"])

    print(f"Bootstrapping {len(tickers)} tickers via yfinance (local)...")
    descs = load_existing(DESC_PATH)
    history = load_existing(HISTORY_PATH)
    ok_desc = 0
    ok_hist = 0

    for i, ticker in enumerate(tickers, 1):
        try:
            t = yf.Ticker(ticker)
            info = t.info or {}
            desc = info.get("longBusinessSummary") or ""
            if desc:
                descs[ticker] = desc[:2500]
                ok_desc += 1

            hist = t.history(period="20d", auto_adjust=False)
            if not hist.empty:
                closes = hist["Close"].dropna()
                ticker_hist = history.setdefault(ticker, {})
                for date, price in closes.items():
                    date_str = date.strftime("%Y-%m-%d")
                    ticker_hist[date_str] = round(float(price), 2)
                if closes.size:
                    ok_hist += 1
        except Exception as e:
            print(f"  {ticker} failed: {e}", file=sys.stderr)

        if i % 25 == 0:
            print(f"  [{i}/{len(tickers)}] descs: {ok_desc} | history: {ok_hist}")
            save(DESC_PATH, descs)
            save(HISTORY_PATH, history)

        time.sleep(0.3)

    save(DESC_PATH, descs)
    save(HISTORY_PATH, history)
    print(f"\nDone.")
    print(f"  descriptions: {ok_desc}/{len(tickers)}")
    print(f"  price history: {ok_hist}/{len(tickers)}")
    total_dates = sum(len(v) for v in history.values())
    print(f"  total history entries: {total_dates}")
    print(f"\nNext: git add state/ && git commit -m 'Bootstrap descriptions + history' && git push")


if __name__ == "__main__":
    main()
