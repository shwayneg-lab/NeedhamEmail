# NeedhamEmail

Daily analyst coverage digest for the ~492-name Needham universe.

Runs on GitHub Actions: morning digest at 7am ET and closing update at 5:30pm ET, Mon-Fri.
Emails via SendGrid; per-ticker detail pages hosted on GitHub Pages.

## Layout

- `coverage.csv` — ticker → analyst → sector (canonical)
- `src/` — Python source
- `templates/` — Jinja2 templates (email + HTML pages)
- `docs/` — generated HTML, published by GitHub Pages
- `state/prev_ratings.json` — last-run ratings snapshot for diffing
- `.github/workflows/daily_digest.yml` — cron + CI pipeline

## Local dev / smoke test

```bash
pip install -r requirements.txt
DIGEST_SKIP_EMAIL=1 DIGEST_SKIP_TIME_CHECK=1 COVERAGE_FILE=coverage_sample.csv \
  python -m src.main
open docs/latest.html
```
