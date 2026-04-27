"""
test_both_apis.py — Fetch AdRoll + Meta via API, combine into one file
                     matching the BigQuery ad_performance schema.

Output schema (same as BQ):
  Date, Country, Channel, Campaign, Creative,
  Impressions, Clicks, CTR, Spend_AUD,
  QL, FT, Channel_Group, Date_Added, Date_Modified

Usage:
  python test_both_apis.py
  python test_both_apis.py --days 14
  python test_both_apis.py --start 2026-04-01 --end 2026-04-20
"""

import argparse
import os
import json
import pandas as pd
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

load_dotenv()

# ── BQ schema column order ────────────────────────────────────────────────────
BQ_COLUMNS = [
    "Date", "Country", "Channel", "Campaign", "Creative",
    "Impressions", "Clicks", "CTR", "Spend_AUD",
    "QL", "FT", "Channel_Group", "Date_Added", "Date_Modified",
]

TODAY = date.today().isoformat()


def to_bq_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Force any DataFrame into the exact BQ schema:
    - Rename Spend (AUD) → Spend_AUD
    - Add Date_Added / Date_Modified
    - Coerce types
    - Reorder columns
    """
    out = df.copy()

    # Rename spend column
    if "Spend (AUD)" in out.columns:
        out = out.rename(columns={"Spend (AUD)": "Spend_AUD"})

    # Ensure all BQ columns exist
    for col in BQ_COLUMNS:
        if col not in out.columns:
            out[col] = None

    # Date → proper date
    out["Date"] = pd.to_datetime(out["Date"]).dt.date

    # Numeric coercion
    for col in ["Impressions", "Clicks", "CTR", "Spend_AUD"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in ["QL", "FT"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")

    # Audit columns
    out["Date_Added"] = pd.to_datetime(TODAY).date()
    out["Date_Modified"] = pd.to_datetime(TODAY).date()

    # Reorder to exact BQ schema
    return out[BQ_COLUMNS].copy()


def fetch_adroll(date_from: str, date_to: str) -> pd.DataFrame:
    """Fetch AdRoll and return DataFrame in pipeline schema."""
    from test_adroll_api import fetch_adroll_data, parse_adroll_response

    raw = fetch_adroll_data(date_from, date_to)
    if not raw:
        print("  ❌ AdRoll API call failed.")
        return pd.DataFrame()

    # Save raw JSON
    os.makedirs("output", exist_ok=True)
    with open("output/adroll_raw_response.json", "w") as f:
        json.dump(raw, f, indent=2)

    df = parse_adroll_response(raw)
    if df.empty:
        return df

    # Keep only pipeline columns before BQ conversion
    pipeline_cols = [
        "Date", "Country", "Channel", "Campaign", "Creative",
        "Impressions", "Clicks", "CTR", "Spend (AUD)",
        "QL", "FT", "Channel_Group"
    ]
    return df[[c for c in pipeline_cols if c in df.columns]]


def fetch_meta(date_from: str, date_to: str) -> pd.DataFrame:
    """Fetch Meta and return DataFrame in pipeline schema."""
    from test_meta_api import fetch_meta_insights, parse_meta_response

    raw_rows = fetch_meta_insights(date_from, date_to)
    if raw_rows is None:
        print("  ❌ Meta API call failed.")
        return pd.DataFrame()

    # Save raw JSON
    os.makedirs("output", exist_ok=True)
    with open("output/meta_raw_response.json", "w") as f:
        json.dump(raw_rows, f, indent=2)

    df = parse_meta_response(raw_rows)
    if df.empty:
        return df

    pipeline_cols = [
        "Date", "Country", "Channel", "Campaign", "Creative",
        "Impressions", "Clicks", "CTR", "Spend (AUD)",
        "QL", "FT", "Channel_Group"
    ]
    return df[[c for c in pipeline_cols if c in df.columns]]


def main():
    parser = argparse.ArgumentParser(description="Fetch AdRoll + Meta → combined BQ-schema file")
    parser.add_argument("--days", type=int, default=7, help="Days to look back (default: 7)")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    os.makedirs("output", exist_ok=True)

    if args.start and args.end:
        date_from = args.start
        date_to = args.end
    else:
        date_to = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        date_from = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    print("=" * 60)
    print("  Pepperstone APAC — API Fetch → Combined BQ Schema")
    print(f"  Date range: {date_from} → {date_to}")
    print("=" * 60)

    frames = []

    # ── AdRoll ────────────────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("  FETCH 1: AdRoll")
    print("─" * 60)
    try:
        adroll_df = fetch_adroll(date_from, date_to)
        if not adroll_df.empty:
            frames.append(adroll_df)
            print(f"  ✅ AdRoll: {len(adroll_df):,} rows")
        else:
            print(f"  ⚠️  AdRoll: 0 rows")
    except Exception as e:
        print(f"  ❌ AdRoll error: {e}")

    # ── Meta ──────────────────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("  FETCH 2: Meta Ads")
    print("─" * 60)
    try:
        meta_df = fetch_meta(date_from, date_to)
        if not meta_df.empty:
            frames.append(meta_df)
            print(f"  ✅ Meta: {len(meta_df):,} rows")
        else:
            print(f"  ⚠️  Meta: 0 rows")
    except Exception as e:
        print(f"  ❌ Meta error: {e}")

    # ── Combine + convert to BQ schema ────────────────────────────────────
    if not frames:
        print("\n❌ No data from either API. Nothing to save.")
        return

    print("\n" + "─" * 60)
    print("  COMBINING → BQ Schema")
    print("─" * 60)

    combined = pd.concat(frames, ignore_index=True)
    combined = to_bq_schema(combined)
    combined = combined.sort_values(["Date", "Country", "Channel", "Campaign"]).reset_index(drop=True)

    # Save
    output_path = f"output/API_combined_{date_from}_to_{date_to}.xlsx"
    combined.to_excel(output_path, index=False, sheet_name="ad_performance")
    print(f"  💾 Saved → {output_path}")

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"  ✅ Combined output — BQ ad_performance schema")
    print(f"{'=' * 60}")
    print(f"  Total rows   : {len(combined):,}")
    print(f"  Date range   : {combined['Date'].min()} → {combined['Date'].max()}")
    print(f"  Countries    : {sorted(combined['Country'].dropna().unique())}")
    print(f"  Channels     : {sorted(combined['Channel'].unique())}")
    print(f"  Campaigns    : {combined['Campaign'].nunique()}")

    # Per-channel breakdown
    print(f"\n  Per-channel breakdown:")
    summary = combined.groupby("Channel").agg(
        Rows=("Date", "count"),
        Spend=("Spend_AUD", "sum"),
        Clicks=("Clicks", "sum"),
        Impressions=("Impressions", "sum"),
    )
    for ch, row in summary.iterrows():
        print(f"    {ch:<25} {int(row['Rows']):>6} rows  "
              f"AUD {row['Spend']:>10,.2f}  "
              f"{int(row['Clicks']):>8,} clicks  "
              f"{int(row['Impressions']):>12,} impr")

    print(f"\n  Columns: {list(combined.columns)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
