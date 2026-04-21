"""Build digest sections from fetched ticker data."""
from collections import defaultdict
from datetime import datetime, timedelta

LARGE_CAP_THRESHOLD = 10_000_000_000  # $10B


def _next_trading_days(now, n=5):
    """Return next n weekdays (Mon-Fri, ignores market holidays) as YYYY-MM-DD."""
    days = []
    d = now.date()
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime("%Y-%m-%d"))
        d = d + timedelta(days=1)
    return days


def build_watchlist(rows, mode, now_et):
    """Names worth glancing at: earnings today/tomorrow, 1d move >5%, recent rating change."""
    today = now_et.strftime("%Y-%m-%d")
    tomorrow = (now_et + timedelta(days=1)).strftime("%Y-%m-%d")

    watchlist = []
    for r in rows:
        reasons = []

        if r.get("next_earnings") == today:
            reasons.append("Reports today")
        elif r.get("next_earnings") == tomorrow:
            reasons.append("Reports tomorrow")

        pct = r.get("pct_1d")
        if isinstance(pct, (int, float)) and abs(pct) > 5:
            arrow = "↑" if pct > 0 else "↓"
            reasons.append(f"1d {arrow} {abs(pct):.1f}%")

        for ud in r.get("upgrades", [])[:3]:
            try:
                d = datetime.strptime(ud["date"], "%Y-%m-%d").date()
                if (now_et.date() - d).days <= 3:
                    firm = ud.get("firm") or "Analyst"
                    action = ud.get("action") or "rating change"
                    reasons.append(f"{firm} {action}")
                    break
            except (ValueError, TypeError):
                pass

        if reasons:
            watchlist.append({**r, "reasons": reasons})
    return watchlist


def build_movers(rows, top_n=5):
    """Top N gainers and decliners (5-day)."""
    with_move = [r for r in rows if isinstance(r.get("pct_5d"), (int, float))]
    with_move.sort(key=lambda r: r["pct_5d"], reverse=True)
    gainers = with_move[:top_n]
    decliners = with_move[-top_n:][::-1] if len(with_move) >= top_n else []
    return {"gainers": gainers, "decliners": decliners}


def build_earnings_week(rows, now_et):
    """Every covered name reporting in the next 5 trading days, sorted by date."""
    window = set(_next_trading_days(now_et, 5))
    reporting = [r for r in rows if r.get("next_earnings") in window]
    reporting.sort(key=lambda r: (r["next_earnings"], r["ticker"]))
    return reporting


def split_movers_by_cap(rows, market_caps, threshold=LARGE_CAP_THRESHOLD, top_n=5):
    """Separate gainers/decliners for large-cap (>$10B) vs small-cap names."""
    with_move = [r for r in rows if isinstance(r.get("pct_5d"), (int, float))]
    large, small = [], []
    for r in with_move:
        cap = market_caps.get(r["ticker"])
        if isinstance(cap, (int, float)) and cap >= threshold:
            large.append(r)
        else:
            small.append(r)

    def _top(bucket):
        bucket.sort(key=lambda r: r["pct_5d"], reverse=True)
        gainers = bucket[:top_n]
        decliners = bucket[-top_n:][::-1] if len(bucket) >= top_n else []
        return {"gainers": gainers, "decliners": decliners}

    return {"large": _top(large), "small": _top(small)}


def build_sector_rotation(rows, top_n=3):
    """Average 5-day % move by sector; returns top N leaders + bottom N laggards."""
    by_sector = defaultdict(list)
    for r in rows:
        pct = r.get("pct_5d")
        sector = r.get("sector")
        if isinstance(pct, (int, float)) and sector:
            by_sector[sector].append(pct)

    avgs = []
    for sector, moves in by_sector.items():
        if len(moves) >= 3:  # ignore tiny sectors
            avgs.append(
                {"sector": sector, "avg_5d": round(sum(moves) / len(moves), 2), "n": len(moves)}
            )
    avgs.sort(key=lambda s: s["avg_5d"], reverse=True)
    leaders = avgs[:top_n]
    laggards = avgs[-top_n:][::-1] if len(avgs) >= top_n else []
    return {"leaders": leaders, "laggards": laggards}


def build_week_review(rows, now_et, price_history):
    """Monday-close → Friday-close move for each ticker. Call only on Friday PM."""
    today = now_et.date()
    monday = today - timedelta(days=today.weekday())
    monday_str = monday.strftime("%Y-%m-%d")

    weekly = []
    for r in rows:
        ticker_hist = price_history.get(r["ticker"], {})
        monday_close = ticker_hist.get(monday_str)
        current = r.get("price")
        if not (isinstance(monday_close, (int, float)) and monday_close > 0):
            continue
        if not isinstance(current, (int, float)):
            continue
        pct_week = round((current / monday_close - 1) * 100, 2)
        weekly.append({**r, "pct_week": pct_week})

    weekly.sort(key=lambda r: r["pct_week"], reverse=True)
    gainers = weekly[:10]
    decliners = weekly[-10:][::-1] if len(weekly) >= 10 else []
    return {"gainers": gainers, "decliners": decliners, "n_total": len(weekly)}


def build_macro(macro_rows):
    """Pass-through — just ensures pct_1d / pct_5d are present where available."""
    return [
        {
            "ticker": m["ticker"],
            "label": m.get("label", m["ticker"]),
            "category": m.get("category", ""),
            "price": m.get("price"),
            "pct_1d": m.get("pct_1d"),
            "pct_5d": m.get("pct_5d"),
        }
        for m in macro_rows
    ]


def build_rating_actions(rows, prev_state):
    """Street rating/PT actions not present in prev_state."""
    if not prev_state:
        return []

    new_actions = []
    for r in rows:
        prev = prev_state.get(r["ticker"], [])
        prev_keys = {(u.get("date"), u.get("firm"), u.get("to")) for u in prev}
        for u in r.get("upgrades", [])[:5]:
            key = (u.get("date"), u.get("firm"), u.get("to"))
            if u.get("date") and key not in prev_keys:
                new_actions.append(
                    {
                        "ticker": r["ticker"],
                        "company_name": r.get("company_name", ""),
                        "analyst": r.get("analyst", ""),
                        "sector": r.get("sector", ""),
                        **u,
                    }
                )
    new_actions.sort(key=lambda a: a["date"], reverse=True)
    return new_actions
