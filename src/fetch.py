"""Finnhub-based fetcher. 5-day history is built locally from past runs (see main.py)."""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "")
FINNHUB_BASE = "https://finnhub.io/api/v1"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NeedhamDigest/1.0"})

_desc_cache_path = Path("state/descriptions.json")
_desc_cache: dict[str, str] = {}
_desc_cache_loaded = False

_earnings_cache: dict[str, str] = {}
_earnings_cache_loaded = False


class _RateLimiter:
    def __init__(self, per_minute: int):
        self.min_interval = 60.0 / per_minute
        self.last = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.monotonic()
            sleep_for = self.min_interval - (now - self.last)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self.last = time.monotonic()


_limiter = _RateLimiter(per_minute=55)


def _finnhub(path: str, **params):
    if not FINNHUB_KEY:
        raise RuntimeError("FINNHUB_API_KEY not set")
    params["token"] = FINNHUB_KEY
    _limiter.wait()
    r = SESSION.get(f"{FINNHUB_BASE}{path}", params=params, timeout=15)
    if r.status_code == 429:
        time.sleep(2.0)
        _limiter.wait()
        r = SESSION.get(f"{FINNHUB_BASE}{path}", params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def rating_label(mean: float) -> str:
    if mean <= 1.5:
        return "Strong Buy"
    if mean <= 2.5:
        return "Buy"
    if mean <= 3.5:
        return "Hold"
    if mean <= 4.5:
        return "Sell"
    return "Strong Sell"


def _consensus_from_buckets(rec: dict) -> tuple[str | None, int | None]:
    sb = rec.get("strongBuy") or 0
    b = rec.get("buy") or 0
    h = rec.get("hold") or 0
    s = rec.get("sell") or 0
    ss = rec.get("strongSell") or 0
    total = sb + b + h + s + ss
    if total == 0:
        return None, None
    mean = (sb * 1 + b * 2 + h * 3 + s * 4 + ss * 5) / total
    return rating_label(mean), total


def _load_desc_cache():
    global _desc_cache, _desc_cache_loaded
    if _desc_cache_loaded:
        return
    if _desc_cache_path.exists():
        try:
            _desc_cache = json.loads(_desc_cache_path.read_text() or "{}")
        except json.JSONDecodeError:
            _desc_cache = {}
    _desc_cache_loaded = True


def save_desc_cache():
    _desc_cache_path.parent.mkdir(exist_ok=True)
    _desc_cache_path.write_text(json.dumps(_desc_cache, indent=2, sort_keys=True))


def _load_earnings():
    global _earnings_cache, _earnings_cache_loaded
    if _earnings_cache_loaded:
        return
    try:
        today = datetime.now(timezone.utc).date()
        end = today + timedelta(days=14)
        data = _finnhub(
            "/calendar/earnings",
            **{"from": today.isoformat(), "to": end.isoformat()},
        )
        for item in (data or {}).get("earningsCalendar", []) or []:
            sym = item.get("symbol")
            date = item.get("date")
            if sym and date and sym not in _earnings_cache:
                _earnings_cache[sym] = date
    except Exception as e:
        print(f"  earnings calendar failed: {e}")
    _earnings_cache_loaded = True


def fetch_ticker(ticker: str, deep: bool = True) -> dict:
    """
    Fetch per-ticker data.
    deep=True: fetch rec + price target + news (heavy; only for digest-relevant tickers).
    deep=False: just quote + 5-day + earnings + cached description.
    """
    _load_desc_cache()
    _load_earnings()

    out = {
        "ticker": ticker,
        "price": None,
        "pct_1d": None,
        "pct_5d": None,
        "next_earnings": _earnings_cache.get(ticker),
        "consensus": None,
        "price_target": None,
        "upside_pct": None,
        "n_analysts": None,
        "description": _desc_cache.get(ticker, ""),
        "news": [],
        "upgrades": [],
    }

    try:
        q = _finnhub("/quote", symbol=ticker)
        price = q.get("c")
        pct_1d = q.get("dp")
        if isinstance(price, (int, float)) and price > 0:
            out["price"] = round(float(price), 2)
        if isinstance(pct_1d, (int, float)) and abs(pct_1d) < 30:
            out["pct_1d"] = round(float(pct_1d), 2)
    except Exception as e:
        print(f"  {ticker} quote failed: {e}")

    if not out["description"]:
        try:
            prof = _finnhub("/stock/profile2", symbol=ticker)
            name = prof.get("name") or ""
            industry = prof.get("finnhubIndustry") or ""
            weburl = prof.get("weburl") or ""
            desc_parts = [x for x in [name, industry, weburl] if x]
            desc = " · ".join(desc_parts)
            if desc:
                out["description"] = desc
                _desc_cache[ticker] = desc
        except Exception as e:
            print(f"  {ticker} profile failed: {e}")

    if not deep:
        return out

    try:
        recs = _finnhub("/stock/recommendation", symbol=ticker)
        if isinstance(recs, list) and recs:
            latest = recs[0]
            label, total = _consensus_from_buckets(latest)
            if label:
                out["consensus"] = label
                out["n_analysts"] = total
    except Exception as e:
        print(f"  {ticker} recommendation failed: {e}")

    try:
        pt = _finnhub("/stock/price-target", symbol=ticker)
        target = pt.get("targetMean")
        if isinstance(target, (int, float)) and target > 0 and out["price"]:
            if 0.2 * out["price"] <= target <= 5 * out["price"]:
                out["price_target"] = round(float(target), 2)
                out["upside_pct"] = round((target / out["price"] - 1) * 100, 2)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 403:
            pass
        else:
            print(f"  {ticker} price-target failed: {e}")
    except Exception as e:
        print(f"  {ticker} price-target failed: {e}")

    try:
        today = datetime.now(timezone.utc).date()
        two_weeks_ago = today - timedelta(days=14)
        news = _finnhub(
            "/company-news",
            symbol=ticker,
            **{"from": two_weeks_ago.isoformat(), "to": today.isoformat()},
        )
        if isinstance(news, list):
            for item in news[:5]:
                title = item.get("headline") or ""
                link = item.get("url") or ""
                publisher = item.get("source") or ""
                dt = item.get("datetime")
                if isinstance(dt, (int, float)):
                    date_str = datetime.fromtimestamp(dt, tz=timezone.utc).strftime("%Y-%m-%d")
                else:
                    date_str = ""
                if title:
                    out["news"].append(
                        {
                            "title": title,
                            "link": link,
                            "publisher": publisher,
                            "date": date_str,
                        }
                    )
    except Exception as e:
        print(f"  {ticker} news failed: {e}")

    return out
