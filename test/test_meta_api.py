"""
test_meta_api.py — Test Meta (Facebook) Ads API connection and fetch campaign data.

Pulls daily campaign insights with country breakdown via the Marketing API,
filters for APAC countries, and saves output as Excel
matching the pipeline's expected column format.

Usage:
  python test_meta_api.py
  python test_meta_api.py --days 30
  python test_meta_api.py --start 2026-04-01 --end 2026-04-20
"""

import argparse
import json
import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()  # reads from .env in current directory

# ── API credentials (from .env) ──────────────────────────────────────────────
META_AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
META_ACCESS_TOKEN  = os.getenv("META_ACCESS_TOKEN")
META_API_VERSION   = os.getenv("META_API_VERSION", "v19.0")
META_BASE_URL      = f"https://graph.facebook.com/{META_API_VERSION}"

if not all([META_AD_ACCOUNT_ID, META_ACCESS_TOKEN]):
    raise SystemExit("❌ Missing Meta credentials in .env — see .env.example")

# ── APAC countries to keep ────────────────────────────────────────────────────
APAC_COUNTRIES = {"VN", "TH", "SG", "MY", "CN", "HK", "TW", "ID", "PH", "IN", "MN"}


def fetch_meta_insights(date_from: str, date_to: str) -> list:
    """
    Call Meta Marketing API Insights endpoint with pagination.
    Returns a list of all insight rows (dicts).
    """
    url = f"{META_BASE_URL}/{META_AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": META_ACCESS_TOKEN,
        "fields": "campaign_name,adset_name,impressions,clicks,spend,reach",
        "level": "campaign",
        "time_increment": "1",       # daily granularity
        "breakdowns": "country",     # break by country
        "time_range": json.dumps({"since": date_from, "until": date_to}),
        "limit": 500,                # max per page
    }

    print(f"  Calling Meta Ads API...")
    print(f"  Account: {META_AD_ACCOUNT_ID}")
    print(f"  Date range: {date_from} → {date_to}")

    all_rows = []
    page = 1

    while url:
        resp = requests.get(url, params=params, timeout=60)

        print(f"  Page {page}: HTTP {resp.status_code}", end="")

        if resp.status_code != 200:
            print(f"\n  ❌ API error: {resp.text[:500]}")
            # Common errors
            try:
                error_data = resp.json()
                error_msg = error_data.get("error", {}).get("message", "Unknown")
                error_code = error_data.get("error", {}).get("code", "N/A")
                print(f"  Error code: {error_code}")
                print(f"  Message: {error_msg}")
                if "OAuthException" in str(error_data):
                    print("\n  💡 Hint: Your access token may have expired.")
                    print("     Meta access tokens typically expire after 60 days.")
                    print("     Generate a new one at: https://developers.facebook.com/tools/explorer/")
            except:
                pass
            return all_rows if all_rows else None

        data = resp.json()
        rows = data.get("data", [])
        all_rows.extend(rows)
        print(f" — {len(rows)} rows")

        # Pagination
        paging = data.get("paging", {})
        next_url = paging.get("next")
        if next_url:
            url = next_url
            params = {}  # params are baked into the next URL
            page += 1
        else:
            url = None

    return all_rows


def parse_meta_response(rows: list) -> pd.DataFrame:
    """
    Parse raw API rows into a DataFrame matching the pipeline schema.
    Filters for APAC countries.
    """
    if not rows:
        print("  ⚠️  No data rows returned from API.")
        return pd.DataFrame()

    print(f"  Total rows from API: {len(rows):,}")

    # Build DataFrame
    records = []
    for item in rows:
        records.append({
            "Campaign name": item.get("campaign_name", ""),
            "Country": item.get("country", ""),
            "Day": item.get("date_start", ""),
            "Reach": item.get("reach", 0),
            "Impressions": item.get("impressions", 0),
            "Amount spent (AUD)": item.get("spend", 0),
            "Clicks (all)": item.get("clicks", 0),
            "Reporting starts": item.get("date_start", ""),
            "Reporting ends": item.get("date_stop", ""),
        })

    df = pd.DataFrame(records)

    # Show all countries in response
    all_countries = sorted(df["Country"].unique())
    print(f"  All countries in response: {all_countries}")

    # Filter to APAC
    df = df[df["Country"].str.upper().isin(APAC_COUNTRIES)].copy()
    df["Country"] = df["Country"].str.upper()
    print(f"  APAC rows after filter: {len(df):,}")

    if df.empty:
        return df

    # ── Apply pipeline-compatible transformations ─────────────────────────

    df["Date"] = pd.to_datetime(df["Day"]).dt.date
    df["Channel"] = "Meta"
    df["Channel_Group"] = "Meta"
    df["Creative"] = None
    df["QL"] = None
    df["FT"] = None

    # Numeric conversions
    df["Impressions"] = pd.to_numeric(df["Impressions"], errors="coerce")
    df["Clicks"] = pd.to_numeric(df["Clicks (all)"], errors="coerce")
    df["Spend (AUD)"] = pd.to_numeric(df["Amount spent (AUD)"], errors="coerce")
    df["Reach"] = pd.to_numeric(df["Reach"], errors="coerce")

    # CTR: compute from clicks / impressions
    df["CTR"] = (df["Clicks"] / df["Impressions"]).where(df["Impressions"] > 0, other=None)

    # Rename to match pipeline
    df = df.rename(columns={"Campaign name": "Campaign"})

    # Pipeline-format columns
    pipeline_cols = [
        "Date", "Country", "Channel", "Campaign", "Creative",
        "Impressions", "Clicks", "CTR", "Spend (AUD)",
        "QL", "FT", "Channel_Group"
    ]
    # Keep extra columns for debugging
    extra_cols = ["Reach", "Reporting starts", "Reporting ends"]
    output_cols = pipeline_cols + [c for c in extra_cols if c in df.columns]

    return df[output_cols]


def main():
    parser = argparse.ArgumentParser(description="Test Meta Ads API connection")
    parser.add_argument("--days", type=int, default=7, help="Number of days to look back (default: 7)")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    print("=" * 55)
    print("  Meta Ads API Test — Fetch Campaign Insights")
    print("=" * 55)

    # Determine date range
    if args.start and args.end:
        date_from = args.start
        date_to = args.end
    else:
        date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    # Step 1: Fetch
    print(f"\n[1/3] Fetching data from Meta Ads API...")
    raw_rows = fetch_meta_insights(date_from, date_to)

    if raw_rows is None:
        print("\n❌ Failed to fetch data. Check access token and network.")
        return

    # Save raw JSON for debugging
    import os
    os.makedirs("output", exist_ok=True)
    with open("output/meta_raw_response.json", "w") as f:
        json.dump(raw_rows, f, indent=2)
    print(f"  💾 Raw JSON saved → output/meta_raw_response.json")

    if not raw_rows:
        print("\n⚠️  API returned empty data. This could mean:")
        print("     - No campaign activity in the date range")
        print("     - Access token expired")
        print("     - Account permissions issue")
        return

    # Step 2: Parse
    print(f"\n[2/3] Parsing response...")
    df = parse_meta_response(raw_rows)

    if df.empty:
        print("\n⚠️  No APAC data to save.")
        return

    # Step 3: Save
    print(f"\n[3/3] Saving output...")

    output_path = f"output/Meta_API_test_{date_from}_to_{date_to}.xlsx"
    df.to_excel(output_path, index=False, sheet_name="Meta_API_Data")
    print(f"  💾 Excel saved → {output_path}")

    # Summary
    print(f"\n{'=' * 55}")
    print(f"  ✅ Meta Ads API test complete!")
    print(f"  Rows fetched  : {len(df):,}")
    print(f"  Date range    : {df['Date'].min()} → {df['Date'].max()}")
    print(f"  Countries     : {sorted(df['Country'].unique())}")
    print(f"  Campaigns     : {df['Campaign'].nunique()}")
    print(f"  Total Spend   : AUD {df['Spend (AUD)'].sum():,.2f}")
    print(f"  Total Clicks  : {df['Clicks'].sum():,.0f}")
    print(f"  Total Impr    : {df['Impressions'].sum():,.0f}")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
