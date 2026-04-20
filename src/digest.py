"""Build digest sections from fetched ticker data."""
from datetime import datetime, timedelta


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
