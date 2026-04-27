"""
test_adroll_api.py — Test AdRoll API connection and fetch APAC campaign data.

Pulls daily metrics via AdRoll's GraphQL reporting API,
filters for APAC campaigns, and saves output as Excel
matching the pipeline's expected column format.

Usage:
  python test_adroll_api.py
  python test_adroll_api.py --days 30        # last 30 days
  python test_adroll_api.py --start 2026-04-01 --end 2026-04-20
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
ADROLL_API_URL   = os.getenv("ADROLL_API_URL", "https://services.adroll.com/reporting/api/v1/query")
ADROLL_API_KEY   = os.getenv("ADROLL_API_KEY")
ADROLL_AUTH_TOKEN = os.getenv("ADROLL_AUTH_TOKEN")
ADVERTISER_EID   = os.getenv("ADROLL_ADVERTISER_EID")
CURRENCY         = os.getenv("ADROLL_CURRENCY", "AUD")

if not all([ADROLL_API_KEY, ADROLL_AUTH_TOKEN, ADVERTISER_EID]):
    raise SystemExit("❌ Missing AdRoll credentials in .env — see .env.example")

# ── Country mapping (same as your pipeline) ───────────────────────────────────
ADROLL_COUNTRY_MAP = {
    "Hong Kong":  "HK",
    "Taiwan":     "TW",
    "Thailand":   "TH",
    "Vietnam":    "VN",
    "Singapore":  "SG",
    "Indonesia":  "ID",
    "Philippines": "PH",
}

ADROLL_CHANNEL_RULES = {
    "Retargeting": "AdRoll - Retargeting",
    "lookalike":   "AdRoll - Lookalike",
    "Contextual":  "AdRoll - Contextual",
}

# ── GraphQL query (same as your n8n workflow) ─────────────────────────────────
GRAPHQL_QUERY = """
query ($eid: String!, $start: Date!, $end: Date!, $currency: String!) {
  advertisable {
    byEID(advertisable: $eid) {
      campaigns {
        eid
        name
        channel
        startDate
        endDate
        status
        metrics(start: $start, end: $end, currency: $currency) {
          byDate {
            date
            impressions
            clicks
            cost
            conversions
            ctr
            cpa
          }
        }
      }
    }
  }
}
"""


def fetch_adroll_data(date_from: str, date_to: str) -> dict:
    """Call AdRoll reporting API and return raw JSON response."""
    headers = {
        "Authorization": f"Token {ADROLL_AUTH_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "query": GRAPHQL_QUERY,
        "variables": {
            "eid": ADVERTISER_EID,
            "start": date_from,
            "end": date_to,
            "currency": CURRENCY,
        },
    }

    print(f"  Calling AdRoll API...")
    print(f"  Date range: {date_from} → {date_to}")
    print(f"  URL: {ADROLL_API_URL}")

    resp = requests.post(
        f"{ADROLL_API_URL}?apikey={ADROLL_API_KEY}",
        headers=headers,
        json=payload,
        timeout=60,
    )

    print(f"  HTTP status: {resp.status_code}")

    if resp.status_code != 200:
        print(f"  ❌ API error: {resp.text[:500]}")
        return None

    data = resp.json()

    # Check for GraphQL errors
    if "errors" in data:
        print(f"  ❌ GraphQL errors: {json.dumps(data['errors'], indent=2)}")
        return None

    return data


def parse_adroll_response(data: dict) -> pd.DataFrame:
    """
    Parse raw API response into a DataFrame matching the pipeline schema.
    Filters for APAC campaigns only.
    """
    campaigns = data.get("data", {}).get("advertisable", {}).get("byEID", {}).get("campaigns", [])

    if not campaigns:
        print("  ⚠️  No campaigns found in response.")
        return pd.DataFrame()

    print(f"  Total campaigns in account: {len(campaigns)}")

    rows = []
    apac_campaigns = [c for c in campaigns if "apac" in (c.get("name") or "").lower()]
    print(f"  APAC campaigns found: {len(apac_campaigns)}")

    if not apac_campaigns:
        print("  ⚠️  No APAC campaigns found. Listing all campaign names:")
        for c in campaigns[:20]:
            print(f"     - {c.get('name', 'N/A')} (status: {c.get('status', 'N/A')})")
        if len(campaigns) > 20:
            print(f"     ... and {len(campaigns) - 20} more")
        return pd.DataFrame()

    for c in apac_campaigns:
        name = c.get("name", "")
        by_date = c.get("metrics", {}).get("byDate", []) or []

        for r in by_date:
            rows.append({
                "Date": r.get("date"),
                "Campaign": name,
                "api_channel": c.get("channel"),
                "Campaign_Start": c.get("startDate"),
                "Campaign_End": c.get("endDate"),
                "Campaign_Status": c.get("status"),
                "Impressions": r.get("impressions", 0),
                "Clicks": r.get("clicks", 0),
                "CTR": r.get("ctr"),
                "Conversions": r.get("conversions", 0),
                "Spend (AUD)": r.get("cost", 0),
                "CPA": r.get("cpa"),
            })

    if not rows:
        print("  ⚠️  APAC campaigns found but no daily data rows returned.")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    print(f"  Raw rows from API: {len(df):,}")

    # ── Apply pipeline-compatible transformations ─────────────────────────

    # 1. Extract country from campaign name (APAC_<Country>_...)
    import re
    def extract_country(name):
        m = re.search(r"APAC_([^_]+)_", str(name))
        if m:
            return ADROLL_COUNTRY_MAP.get(m.group(1), m.group(1))
        return None

    df["Country"] = df["Campaign"].apply(extract_country)

    # 2. Derive Channel from campaign name
    def derive_channel(name):
        n = str(name)
        for key, label in ADROLL_CHANNEL_RULES.items():
            if key.lower() in n.lower():
                return label
        return "AdRoll"

    df["Channel"] = df["Campaign"].apply(derive_channel)
    df["Channel_Group"] = df["Channel"].apply(
        lambda x: "AdRoll" if "AdRoll" in str(x) else "Others"
    )

    # 3. Add missing pipeline columns
    df["Creative"] = None
    df["QL"] = None
    df["FT"] = None

    # 4. Reorder to match pipeline schema
    pipeline_cols = [
        "Date", "Country", "Channel", "Campaign", "Creative",
        "Impressions", "Clicks", "CTR", "Spend (AUD)",
        "QL", "FT", "Channel_Group"
    ]
    # Keep extra columns for debugging
    extra_cols = [c for c in df.columns if c not in pipeline_cols]
    output_cols = pipeline_cols + extra_cols

    return df[output_cols]


def main():
    parser = argparse.ArgumentParser(description="Test AdRoll API connection")
    parser.add_argument("--days", type=int, default=7, help="Number of days to look back (default: 7)")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    print("=" * 55)
    print("  AdRoll API Test — Fetch APAC Campaign Data")
    print("=" * 55)

    # Determine date range
    if args.start and args.end:
        date_from = args.start
        date_to = args.end
    else:
        date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    # Step 1: Fetch
    print(f"\n[1/3] Fetching data from AdRoll API...")
    raw_data = fetch_adroll_data(date_from, date_to)

    if raw_data is None:
        print("\n❌ Failed to fetch data. Check credentials and network.")
        return

    # Save raw JSON for debugging
    with open("output/adroll_raw_response.json", "w") as f:
        json.dump(raw_data, f, indent=2)
    print(f"  💾 Raw JSON saved → output/adroll_raw_response.json")

    # Step 2: Parse
    print(f"\n[2/3] Parsing response...")
    df = parse_adroll_response(raw_data)

    if df.empty:
        print("\n⚠️  No data to save.")
        return

    # Step 3: Save
    print(f"\n[3/3] Saving output...")
    import os
    os.makedirs("output", exist_ok=True)

    output_path = f"output/AdRoll_API_test_{date_from}_to_{date_to}.xlsx"
    df.to_excel(output_path, index=False, sheet_name="AdRoll_API_Data")
    print(f"  💾 Excel saved → {output_path}")

    # Summary
    print(f"\n{'=' * 55}")
    print(f"  ✅ AdRoll API test complete!")
    print(f"  Rows fetched  : {len(df):,}")
    print(f"  Date range    : {df['Date'].min()} → {df['Date'].max()}")
    print(f"  Countries     : {sorted(df['Country'].dropna().unique())}")
    print(f"  Channels      : {sorted(df['Channel'].unique())}")
    print(f"  Campaigns     : {df['Campaign'].nunique()}")
    print(f"  Total Spend   : AUD {df['Spend (AUD)'].sum():,.2f}")
    print(f"  Total Clicks  : {df['Clicks'].sum():,.0f}")
    print(f"  Total Impr    : {df['Impressions'].sum():,.0f}")
    print(f"{'=' * 55}")


if __name__ == "__main__":
    main()
