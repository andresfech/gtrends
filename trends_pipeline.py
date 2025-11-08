import os
import time
import argparse
import datetime as dt
from typing import List, Dict

import pandas as pd
from pytrends.request import TrendReq
from pytrends import exceptions as pytrends_exceptions

# ======= CONFIG (defaults; overridable via CLI/env) =======
KEYWORDS_DEFAULT = [
    "Ria Money Transfer",      # anchor
    "Western Union",
    "Remitly",
    "Wise money transfer",
    "TapTap Send",
    "MoneyGram",
    "Felix Pago",
    "Xoom money transfer",
    "Global66",
]
ANCHOR_DEFAULT = "Ria Money Transfer"
GEOS_DEFAULT = ["US", "CA", "CL", "ES"]

# Timeframes
TF_DAILY = "today 12-m"  # daily granularity ~ last 12 months
TF_WEEKLY = "today 5-y"  # weekly granularity ~ last 5 years

# Google Sheets
SPREADSHEET_NAME = os.getenv("TRENDS_SHEET_NAME", "Brand Trends Dashboard")

# Rate-limit safety
SLEEP_SEC_DEFAULT = float(os.getenv("PYTRENDS_SLEEP_SEC", "2.0"))

# ======= OPTIONAL: Slack webhook for spike alerts =======
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# ======= GSpread auth via service account JSON (from file or env var) =======
USE_GSPREAD = True
GSA_PATH = "service_account.json"
GSA_JSON_ENV = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

if USE_GSPREAD:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials


def _ensure_gspread_client():
    if not USE_GSPREAD:
        return None
    if GSA_JSON_ENV and not os.path.exists(GSA_PATH):
        with open(GSA_PATH, "w") as f:
            f.write(GSA_JSON_ENV)
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GSA_PATH, scope)
    return gspread.authorize(creds)


def chunk_with_anchor(keywords: List[str], anchor: str, limit: int = 5):
    """
    Google Trends supports up to 5 terms per request.
    We create batches that always include the anchor so we can scale later.
    """
    others = [k for k in keywords if k != anchor]
    batch_size = limit - 1  # anchor + N others
    batches = []
    for i in range(0, len(others), batch_size):
        batches.append([anchor] + others[i:i+batch_size])
    return batches


def fetch_trends_batch(
    pytrends: TrendReq,
    terms: List[str],
    geo: str,
    timeframe: str,
    alias_map: Dict[str, str],
    max_retries: int = 5,
    backoff_sec: float = 60.0,
) -> pd.DataFrame:
    """
    Wrap pytrends calls with exponential backoff to deal with 429s.
    """
    for attempt in range(1, max_retries + 1):
        try:
            pytrends.build_payload(terms, timeframe=timeframe, geo=geo)
            df = pytrends.interest_over_time().reset_index()
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])
            rename_map = {
                col: alias_map.get(col, col)
                for col in df.columns
                if col in alias_map
            }
            if rename_map:
                df = df.rename(columns=rename_map)
            return df  # columns: date + one col per term
        except pytrends_exceptions.TooManyRequestsError:
            if attempt == max_retries:
                raise
            sleep_for = backoff_sec * attempt
            print(
                f"[WARN] 429 for {geo} {timeframe} terms={terms}. "
                f"Retrying in {sleep_for:.0f}s (attempt {attempt}/{max_retries})."
            )
            time.sleep(sleep_for)


def normalize_to_anchor(first_anchor: pd.Series, current_anchor: pd.Series) -> float:
    """
    Compute scaling factor to bring current_anchor onto first_anchor's scale.
    Uses median ratio over overlapping non-zero points for robustness.
    """
    join = pd.concat([first_anchor, current_anchor], axis=1, keys=["first", "current"]).dropna()
    # Avoid division by zero; filter zeros
    join = join[(join["first"] > 0) & (join["current"] > 0)]
    if join.empty:
        # fallback factor 1.0 if no overlap
        return 1.0
    ratios = join["first"] / join["current"]
    return float(ratios.median())


def stitch_batches(
    pytrends: TrendReq,
    all_keywords: List[str],
    anchor_term: str,
    anchor_label: str,
    alias_map: Dict[str, str],
    geo: str,
    timeframe: str,
    sleep_between_batches: float,
    max_terms_per_batch: int,
    max_retries: int,
    backoff_sec: float,
) -> pd.DataFrame:
    """
    1) Query multiple batches with anchor
    2) Use the anchor overlap to scale subsequent batches to the first batch
    3) Return a wide DF: date + one column per keyword, normalized across batches
    """
    batches = chunk_with_anchor(all_keywords, anchor_term, limit=max_terms_per_batch)
    stitched = None
    base_anchor_col = None

    for idx, terms in enumerate(batches):
        df = fetch_trends_batch(
            pytrends,
            terms,
            geo,
            timeframe,
            alias_map=alias_map,
            max_retries=max_retries,
            backoff_sec=backoff_sec,
        )
        time.sleep(sleep_between_batches)  # gentle rate limit

        if idx == 0:
            stitched = df.copy()
            base_anchor_col = stitched[anchor_label].copy()
        else:
            # compute scaling factor for this batch anchor -> base anchor
            factor = normalize_to_anchor(base_anchor_col, df[anchor_label])
            # scale non-date columns (except date)
            for col in terms:
                if col == "date":
                    continue
                label = alias_map.get(col, col)
                if label == anchor_label:
                    continue
                df[label] = df[label] * factor
            # combine into stitched
            for col in terms:
                label = alias_map.get(col, col)
                if label in ["date", anchor_label]:
                    continue  # anchor already exists from first batch
                if label not in stitched.columns:
                    stitched = stitched.merge(df[["date", label]], on="date", how="outer")
                else:
                    # combine by taking max across overlaps (both are normalized)
                    merged = stitched[["date", label]].merge(df[["date", label]], on="date", how="outer", suffixes=("_x", "_y"))
                    merged[label] = merged[[f"{label}_x", f"{label}_y"]].max(axis=1, skipna=True)
                    stitched = stitched.drop(columns=[label]).merge(merged[["date", label]], on="date", how="outer")

    # sort by date, fill missing with 0
    stitched = stitched.sort_values("date").reset_index(drop=True).fillna(0)

    # Add metadata
    stitched["geo"] = geo
    stitched["timeframe"] = timeframe
    stitched["last_updated_utc"] = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    return stitched


def write_to_sheet(gc, df: pd.DataFrame, worksheet_title: str):
    sh = None
    try:
        sh = gc.open(SPREADSHEET_NAME)
    except Exception:
        sh = gc.create(SPREADSHEET_NAME)
    try:
        ws = sh.worksheet(worksheet_title)
        sh.del_worksheet(ws)
    except Exception:
        pass
    ws = sh.add_worksheet(title=worksheet_title, rows=str(len(df)+10), cols=str(len(df.columns)+5))
    # Reorder columns: date, keywords..., geo, timeframe, last_updated_utc
    cols = ["date"] + [c for c in df.columns if c not in ["date", "geo", "timeframe", "last_updated_utc"]] + ["geo", "timeframe", "last_updated_utc"]
    df = df[cols]
    # Convert date to string for Sheets
    out = df.copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    ws.update([out.columns.tolist()] + out.values.tolist())


def maybe_send_spike_alert(df_daily: pd.DataFrame, geo: str, anchor: str):
    if not SLACK_WEBHOOK:
        return
    try:
        # Simple spike heuristic: last value vs 30-day rolling mean + 3*std of anchor
        sub = df_daily[["date", anchor]].copy()
        sub[anchor] = pd.to_numeric(sub[anchor], errors="coerce").fillna(0)
        sub = sub.set_index("date")
        roll = sub[anchor].rolling(30, min_periods=10)
        if len(roll.mean().dropna()) == 0:
            return
        last_val = sub[anchor].iloc[-1]
        mu = roll.mean().iloc[-1]
        sd = roll.std(ddof=0).iloc[-1]
        if sd > 0 and last_val > mu + 3 * sd:
            msg = f":rotating_light: {anchor} spike in {geo}: last={last_val:.1f}, mean30={mu:.1f}, sd30={sd:.1f}"
            import requests
            requests.post(SLACK_WEBHOOK, json={"text": msg})
    except Exception:
        pass


def parse_args():
    parser = argparse.ArgumentParser(description="Run Google Trends pipeline.")
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=SLEEP_SEC_DEFAULT,
        help="Seconds to sleep between payloads (default from PYTRENDS_SLEEP_SEC or 2.0).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=int(os.getenv("PYTRENDS_MAX_RETRIES", "5")),
        help="Max retries per payload on 429.",
    )
    parser.add_argument(
        "--backoff-sec",
        type=float,
        default=float(os.getenv("PYTRENDS_BACKOFF_SEC", "60")),
        help="Base backoff seconds for retries (multiplied by attempt).",
    )
    parser.add_argument(
        "--daily",
        dest="run_daily",
        action="store_true",
        help="Run daily timeframe (default when neither --daily nor --weekly is set).",
    )
    parser.add_argument(
        "--weekly",
        dest="run_weekly",
        action="store_true",
        help="Run weekly timeframe.",
    )
    parser.add_argument(
        "--geos",
        type=lambda s: [g.strip() for g in s.split(",") if g.strip()],
        default=GEOS_DEFAULT,
        help="Comma-separated geos to fetch.",
    )
    parser.add_argument(
        "--keywords",
        type=lambda s: [k.strip() for k in s.split(",") if k.strip()],
        default=KEYWORDS_DEFAULT,
        help="Comma-separated keywords (anchor must be included).",
    )
    parser.add_argument(
        "--anchor",
        type=str,
        default=ANCHOR_DEFAULT,
        help="Anchor keyword used for normalization.",
    )
    parser.add_argument(
        "--max-terms-per-batch",
        type=int,
        default=int(os.getenv("PYTRENDS_MAX_TERMS", "5")),
        help="Max terms per payload (<=5 per API limitation).",
    )
    parser.add_argument(
        "--resume-from-geo",
        type=str,
        default="",
        help="Skip geos before this value (useful if throttled mid-run).",
    )
    parser.add_argument(
        "--resume-from-phase",
        choices=["daily", "weekly"],
        default="",
        help="Skip phases before this value.",
    )
    parser.add_argument(
        "--use-topics",
        action="store_true",
        help="Resolve keywords to their top Google Trends topic IDs before fetching.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    pytrends = TrendReq(hl="en-US", tz=0)

    keywords_display = args.keywords
    anchor_display = args.anchor
    if anchor_display not in keywords_display:
        raise ValueError(f"Anchor '{anchor_display}' must be present in keyword list.")

    display_to_query: Dict[str, str] = {}
    query_to_display: Dict[str, str] = {}

    for display in keywords_display:
        query = display
        if args.use_topics:
            try:
                suggestions = pytrends.suggestions(display)
                if suggestions:
                    candidate = suggestions[0].get("mid")
                    if candidate:
                        query = candidate
            except Exception:
                # fallback to raw keyword if suggestions fail
                query = display
        display_to_query[display] = query
        query_to_display[query] = display

    anchor_query = display_to_query[anchor_display]
    keywords_query_order = [display_to_query[name] for name in keywords_display]

    gc = _ensure_gspread_client() if USE_GSPREAD else None

    run_daily = args.run_daily or (not args.run_daily and not args.run_weekly)
    run_weekly = args.run_weekly or (not args.run_daily and not args.run_weekly)

    resume_geo = args.resume_from_geo.upper()
    resume_phase = args.resume_from_phase

    def should_run_phase(phase_name: str) -> bool:
        if resume_phase and phase_name < resume_phase:
            return False
        return True

    if run_daily and should_run_phase("daily"):
        for geo in args.geos:
            geo_upper = geo.upper()
            if resume_geo and geo_upper < resume_geo:
                continue
            print(f"[INFO] Fetching daily data for {geo_upper}...")
            daily = stitch_batches(
                pytrends,
                keywords_query_order,
                anchor_query,
                anchor_display,
                query_to_display,
                geo=geo_upper,
                timeframe=TF_DAILY,
                sleep_between_batches=args.sleep_sec,
                max_terms_per_batch=args.max_terms_per_batch,
                max_retries=args.max_retries,
                backoff_sec=args.backoff_sec,
            )
            if gc:
                write_to_sheet(gc, daily, worksheet_title=f"{geo_upper}_daily")
            maybe_send_spike_alert(daily, geo=geo_upper, anchor=anchor_display)

    if run_weekly and should_run_phase("weekly"):
        for geo in args.geos:
            geo_upper = geo.upper()
            if resume_geo and geo_upper < resume_geo:
                continue
            print(f"[INFO] Fetching weekly data for {geo_upper}...")
            weekly = stitch_batches(
                pytrends,
                keywords_query_order,
                anchor_query,
                anchor_display,
                query_to_display,
                geo=geo_upper,
                timeframe=TF_WEEKLY,
                sleep_between_batches=args.sleep_sec,
                max_terms_per_batch=args.max_terms_per_batch,
                max_retries=args.max_retries,
                backoff_sec=args.backoff_sec,
            )
            if gc:
                write_to_sheet(gc, weekly, worksheet_title=f"{geo_upper}_weekly")


if __name__ == "__main__":
    main()