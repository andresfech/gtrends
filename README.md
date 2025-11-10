# Brand Trends Dashboard (PyTrends → Google Sheets → Looker Studio)

Auto-updating Google Trends pipeline for **Ria Money Transfer** vs competitors (Remitly, Western Union, Wise, TapTap Send, MoneyGram, Felix Pago, Xoom, Global66) across **US, CA, CL, ES**. 
Runs **daily** and **weekly** via GitHub Actions, writes normalized time series to **Google Sheets**, and is ready to visualize in **Looker Studio**.

## Quick Start (Local)

1. **Create service account** in Google Cloud → enable *Google Sheets API* and *Drive API*. Share your target Sheet with the service account email.
2. Save the credentials JSON as `service_account.json` in the project root.
3. Create a virtual env and install deps:
   ```bash
   python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
4. (Optional) rename the target sheet via env var:
   ```bash
   export TRENDS_SHEET_NAME="Brand Trends Dashboard"
   ```
5. Run (tune sleep to dodge 429s):
   ```bash
   # activate venv first
   python trends_pipeline.py --sleep-sec 120 --max-retries 8 --backoff-sec 90 --daily
   python trends_pipeline.py --sleep-sec 120 --max-retries 8 --backoff-sec 90 --weekly
   ```
   Use `--resume-from-geo=CL` or `--resume-from-phase=weekly` if a run throttles midway.

### CLI flags
- `--sleep-sec` controls the pause between payloads (default from `PYTRENDS_SLEEP_SEC`).
- `--max-retries` + `--backoff-sec` apply exponential retry when Google returns 429.
- `--daily` / `--weekly` let you run one timeframe; run neither to fetch both.
- `--geos`, `--keywords`, `--anchor` accept comma-separated overrides (defaults: geos `US,CA,CL,ES` and keywords `Ria Money Transfer, Western Union, Remitly, Wise money transfer, TapTap Send, MoneyGram, Felix Pago, Xoom money transfer, Global66`). Override a specific geo via env var `PYTRENDS_KEYWORDS_<GEO>` (ex: `PYTRENDS_KEYWORDS_CL="Ria Money Transfer,Western Union,MoneyGram,Global66"`).
- `--resume-from-geo`, `--resume-from-phase` let you pick up a halted run.
- `--use-topics` swaps raw keywords for Google Trends topic IDs (when available) so related queries roll up under one label. Set `PYTRENDS_USE_TOPICS=true` (or `yes/1`) to enable topics for GitHub Actions.
- `--daily-days` controls the trailing range for daily fetches (default 180). Adjust via CLI or set `PYTRENDS_DAILY_DAYS`.
- Chile (`CL`) defaults to a slimmer competitor list (`Ria Money Transfer, Western Union, MoneyGram, Global66`). Change it via `PYTRENDS_KEYWORDS_CL` or CLI overrides if you need more brands.

## GitHub Actions (Cloud Automation)

1. Create a new private repo and push these files.
2. In **Repo → Settings → Secrets and variables → Actions → New repository secret**:
   - `GOOGLE_SERVICE_ACCOUNT_JSON` → paste the full JSON (one line) of your service account.
   - *(optional)* `SLACK_WEBHOOK_URL` → to push spike alerts.
3. (Optional) adjust pacing via repository variables if Google rate-limits you:
   - `PYTRENDS_SLEEP_SEC` → default 2; increase to 30–120 if runs throttle.
   - `PYTRENDS_MAX_RETRIES` / `PYTRENDS_BACKOFF_SEC` to mirror CLI flags.
4. Trigger a manual run (Actions → “Update Google Trends to Sheets” → **Run workflow**) to validate the setup.
5. The included workflow `.github/workflows/trends.yml` then runs automatically **daily at 06:00 UTC** and **weekly Mondays at 07:00 UTC**.

## Looker Studio

1. Create a **Data Source → Google Sheets → Brand Trends Dashboard**.
2. Pick a tab like `US_daily` or `US_weekly`.
3. Ensure the **date** field is typed as a Date.
4. Build time-series charts with the brand columns as metrics.
5. Duplicate pages for CA, CL, ES and switch to their respective tabs.

## Customize

- Edit `KEYWORDS` and `GEOS` in `trends_pipeline.py`.
- Add more geos (e.g., `US-CA`) or change timeframes.
- Tune the spike alert logic in `maybe_send_spike_alert`.

## Notes

- Google Trends allows **max 5 terms** per query. This project batches terms with a constant **anchor** (`"Ria Money Transfer"`) to normalize results across batches.
- We output **daily (6 months)** and **weekly (5 years)** series per geo into separate sheet tabs: `GEO_daily` and `GEO_weekly`.