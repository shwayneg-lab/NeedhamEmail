"""Render email digest and per-ticker / index HTML pages."""
from jinja2 import Environment, FileSystemLoader, select_autoescape

_env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape(["html"]),
    trim_blocks=True,
    lstrip_blocks=True,
)


def render_digest(mode, watchlist, movers, earnings, new_actions, now_et, pages_base):
    tmpl = _env.get_template("digest.html.j2")
    return tmpl.render(
        mode=mode,
        watchlist=watchlist,
        movers=movers,
        earnings=earnings,
        new_actions=new_actions,
        now=now_et,
        pages_base=pages_base,
    )


def render_ticker_page(row, pages_base):
    tmpl = _env.get_template("ticker.html.j2")
    return tmpl.render(row=row, pages_base=pages_base)


def render_index(rows, pages_base):
    tmpl = _env.get_template("index.html.j2")
    return tmpl.render(rows=rows, pages_base=pages_base)
