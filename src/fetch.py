"""yfinance wrappers: returns a dict of everything we need per ticker."""
from __future__ import annotations

import random
import time
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from curl_cffi import requests as cffi_requests

_session = cffi_requests.Session(impersonate="chrome")


def _retry(fn, retries: int = 4):
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            msg = str(e)
            throttled = "429" in msg or "Too Many" in msg or "Expecting value" in msg
            if throttled and attempt < retries - 1:
                time.sleep((2 ** attempt) + random.uniform(0, 0.5))
                continue
            raise


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


def _clean(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(v)
    return "" if s == "nan" else s


def fetch_ticker(ticker: str) -> dict:
    """Return a dict with all fields; any field can be None/empty on failure."""
    t = yf.Ticker(ticker, session=_session)
    out = {
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

    try:
        hist = _retry(lambda: t.history(period="15d", auto_adjust=False))
        if not hist.empty:
            closes = hist["Close"].dropna()
            if len(closes) >= 1:
                out["price"] = round(float(closes.iloc[-1]), 2)
            if len(closes) >= 2:
                out["pct_1d"] = round((closes.iloc[-1] / closes.iloc[-2] - 1) * 100, 2)
            if len(closes) >= 6:
                out["pct_5d"] = round((closes.iloc[-1] / closes.iloc[-6] - 1) * 100, 2)
    except Exception:
        pass

    try:
        cal = _retry(lambda: t.calendar)
        if isinstance(cal, dict):
            e = cal.get("Earnings Date")
            if isinstance(e, list) and e:
                out["next_earnings"] = str(e[0])[:10]
            elif e:
                out["next_earnings"] = str(e)[:10]
    except Exception:
        pass

    try:
        info = _retry(lambda: t.info) or {}
        mean = info.get("recommendationMean")
        if isinstance(mean, (int, float)) and mean > 0:
            out["consensus"] = rating_label(float(mean))
        target = info.get("targetMeanPrice")
        if isinstance(target, (int, float)) and target > 0:
            out["price_target"] = round(float(target), 2)
            if out["price"]:
                out["upside_pct"] = round((float(target) / out["price"] - 1) * 100, 2)
        n = info.get("numberOfAnalystOpinions")
        if isinstance(n, int) and n > 0:
            out["n_analysts"] = n
        desc = info.get("longBusinessSummary") or ""
        out["description"] = desc[:2500]
    except Exception:
        pass

    try:
        news = _retry(lambda: t.news) or []
        for item in news[:5]:
            content = item.get("content") if isinstance(item.get("content"), dict) else item
            title = content.get("title") or ""
            link_obj = content.get("canonicalUrl")
            if isinstance(link_obj, dict):
                link = link_obj.get("url", "")
            else:
                link = content.get("link") or item.get("link", "")
            prov_obj = content.get("provider")
            if isinstance(prov_obj, dict):
                publisher = prov_obj.get("displayName", "")
            else:
                publisher = item.get("publisher", "")
            pub_time = content.get("pubDate") or item.get("providerPublishTime")
            if isinstance(pub_time, (int, float)):
                pub_time = datetime.fromtimestamp(pub_time, tz=timezone.utc).strftime("%Y-%m-%d")
            elif isinstance(pub_time, str):
                pub_time = pub_time[:10]
            if title:
                out["news"].append(
                    {
                        "title": title,
                        "link": link,
                        "publisher": publisher or "",
                        "date": pub_time or "",
                    }
                )
    except Exception:
        pass

    try:
        ud = _retry(lambda: t.upgrades_downgrades)
        if ud is not None and not ud.empty:
            recent = ud.head(10).reset_index().fillna("")
            for _, row in recent.iterrows():
                date = row.get("GradeDate")
                if hasattr(date, "strftime"):
                    date = date.strftime("%Y-%m-%d")
                out["upgrades"].append(
                    {
                        "date": _clean(date)[:10],
                        "firm": _clean(row.get("Firm")),
                        "from": _clean(row.get("FromGrade")),
                        "to": _clean(row.get("ToGrade")),
                        "action": _clean(row.get("Action")),
                    }
                )
    except Exception:
        pass

    time.sleep(0.3 + random.uniform(0, 0.3))
    return out
