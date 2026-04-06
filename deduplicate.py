"""
deduplicate.py — Remove confirmed date-format duplicates (all channels)

What it does:
  1. Reads the backup Excel file
  2. Finds ALL rows with old-format dates (no leading zeros)
     that have an IDENTICAL new-format match (same values in every column)
  3. Removes ONLY those confirmed duplicates — skips rows where values differ
  4. Saves two files:
     - CLEANED_Ad_Performance.xlsx  → your clean data (upload this)
     - DELETED_rows.xlsx            → the rows that were removed (for verification)

Usage:
  python deduplicate.py
  python deduplicate.py path/to/backup.xlsx
"""

import os, sys
import pandas as pd
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
OUTPUT_DIR = "./output"
BACKUP_DIR = os.path.join(OUTPUT_DIR, "backups")

def find_latest_backup():
    import glob
    backups = sorted(glob.glob(os.path.join(BACKUP_DIR, "Ad_Performance_backup_*.xlsx")))
    if not backups:
        print("❌ No backups found in ./output/backups/")
        return None
    latest = backups[-1]
    print(f"📂 Using latest backup: {latest}")
    return latest


def deduplicate(filepath):
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return

    print(f"\n{'='*55}")
    print(f"  Date-Format Deduplication (All Channels)")
    print(f"{'='*55}")

    # ── Step 1: Read ──────────────────────────────────────────────────────
    print(f"\n[1/5] Reading {os.path.basename(filepath)} ...")
    df = pd.read_excel(filepath, dtype=str)
    print(f"      Total rows: {len(df):,}")

    # ── Step 2: Normalise dates ───────────────────────────────────────────
    print(f"\n[2/5] Normalising dates ...")
    df["Date_norm"] = pd.to_datetime(
        df["Date"], format="mixed", dayfirst=False
    ).dt.strftime("%m/%d/%Y")

    old_format = df[df["Date"] != df["Date_norm"]]
    new_format = df[df["Date"] == df["Date_norm"]]
    print(f"      Old-format dates (no leading zeros): {len(old_format):,} rows")
    print(f"      New-format dates (leading zeros):    {len(new_format):,} rows")

    # ── Step 3: Find duplicates across all channels ───────────────────────
    print(f"\n[3/5] Finding duplicates across all channels ...")

    def norm_val(v):
        s = str(v).strip()
        return "" if s in ("", "nan", "None", "NaN", "none", "null") else s

    def make_key(row):
        return "||".join([
            row["Date_norm"],
            norm_val(row.get("Country", "")),
            norm_val(row.get("Channel", "")),
            norm_val(row.get("Campaign", "")),
            norm_val(row.get("Creative", ""))
        ])

    old_format = old_format.copy()
    new_format = new_format.copy()
    old_format["_key"] = old_format.apply(make_key, axis=1)
    new_format["_key"] = new_format.apply(make_key, axis=1)

    # Build lookup: new-format key → row data
    new_lookup = {}
    for idx, row in new_format.iterrows():
        key = row["_key"]
        if key not in new_lookup:
            new_lookup[key] = row

    # ── Step 4: Verify each match is truly identical ──────────────────────
    print(f"\n[4/5] Verifying values row by row ...")
    compare_cols = ["Impressions", "Clicks", "CTR", "Spend (AUD)", "QL", "FT"]

    safe_to_delete = []
    skipped_different = []

    for idx, old_row in old_format.iterrows():
        key = old_row["_key"]
        if key not in new_lookup:
            continue  # No new-format match — keep the old row

        new_row = new_lookup[key]
        identical = True

        for col in compare_cols:
            ov = norm_val(old_row.get(col, ""))
            nv = norm_val(new_row.get(col, ""))
            if ov == "" and nv == "":
                continue
            if ov == "":
                ov = "0"
            if nv == "":
                nv = "0"
            try:
                if abs(float(ov) - float(nv)) > 0.01:
                    identical = False
                    break
            except (ValueError, TypeError):
                if ov != nv:
                    identical = False
                    break

        if identical:
            safe_to_delete.append(idx)
        else:
            skipped_different.append(idx)

    print(f"      Confirmed identical (will delete): {len(safe_to_delete):,}")
    print(f"      Different values (will keep):      {len(skipped_different):,}")

    # Show breakdown by channel
    if safe_to_delete:
        del_channels = df.loc[safe_to_delete].groupby("Channel").size().sort_values(ascending=False)
        print(f"\n      Duplicates by channel:")
        for ch, count in del_channels.items():
            print(f"         {ch:<25} {count:,} rows")

    if skipped_different:
        skip_channels = df.loc[skipped_different].groupby("Channel").size().sort_values(ascending=False)
        print(f"\n      Skipped (different values) by channel:")
        for ch, count in skip_channels.items():
            print(f"         {ch:<25} {count:,} rows")

    if len(safe_to_delete) == 0:
        print(f"\n      ✅ No confirmed duplicates found. Nothing to do.")
        return

    # ── Step 5: Save files ────────────────────────────────────────────────
    print(f"\n[5/5] Saving files ...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Deleted rows (for verification)
    deleted_df = df.loc[safe_to_delete].drop(columns=["Date_norm", "_key"], errors="ignore")
    deleted_path = os.path.join(OUTPUT_DIR, f"DELETED_rows_{timestamp}.xlsx")
    deleted_df.to_excel(deleted_path, index=False)

    # Cleaned data
    cleaned_df = df.drop(index=safe_to_delete).drop(columns=["Date_norm", "_key"], errors="ignore")
    cleaned_path = os.path.join(OUTPUT_DIR, f"CLEANED_Ad_Performance_{timestamp}.xlsx")
    cleaned_df.to_excel(cleaned_path, index=False)

    # Calculate spend impact
    deleted_df["_spend"] = pd.to_numeric(deleted_df.get("Spend (AUD)"), errors="coerce").fillna(0)
    doubled_spend = deleted_df["_spend"].sum()

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*55}")
    print(f"  DONE")
    print(f"{'='*55}")
    print(f"\n  Before:  {len(df):,} rows")
    print(f"  Deleted: {len(deleted_df):,} duplicates")
    print(f"  After:   {len(cleaned_df):,} rows")
    print(f"\n  Doubled spend removed: ${doubled_spend:,.2f} AUD")
    print(f"\n  📄 Cleaned file  → {cleaned_path}")
    print(f"  🗑️  Deleted rows  → {deleted_path}")
    print(f"\n  ➡️  Next steps:")
    print(f"     1. Open DELETED_rows — verify these are all dupes")
    print(f"     2. Open CLEANED file — spot check a few rows")
    print(f"     3. Once verified, use this as your source of truth")
    print(f"{'='*55}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        filepath = find_latest_backup()

    if filepath:
        deduplicate(filepath)
