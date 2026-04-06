"""
Pepperstone APAC Performance Marketing Pipeline
"""

from parsers import parse_all
from uploader import upload_to_sheets, save_run_snapshot
from bq_uploader import upload_to_bigquery
from config import GOOGLE_SHEET_ID, OUTPUT_FILE

def main():
    print("=" * 55)
    print("  Pepperstone APAC Performance Marketing Pipeline")
    print("=" * 55)

    print("\n[1/5] Parsing raw files from ./input/ ...")
    combined, failed_channels = parse_all()
    ad_rows = combined[combined['QL'].isna() & combined['FT'].isna()]
    ql_rows = combined[combined['QL'].notna() | combined['FT'].notna()]
    print(f"      Ad rows    : {len(ad_rows):,}")
    print(f"      QL/FT rows : {len(ql_rows):,}")
    print(f"      Total QL   : {combined['QL'].fillna(0).astype(int).sum():,}")
    print(f"      Total FT   : {combined['FT'].fillna(0).astype(int).sum():,}")
    print(f"      Total rows : {len(combined):,}")

    print("\n[2/5] Uploading to BigQuery ...")
    bq_success = upload_to_bigquery(combined)

    print("\n[3/5] Uploading to Google Sheets (backup) ...")
    sheets_success = upload_to_sheets(combined, GOOGLE_SHEET_ID)

    print("\n[4/5] Saving snapshot ...")
    if sheets_success:
        save_run_snapshot(GOOGLE_SHEET_ID)
    else:
        print("      ⚠️  Skipped — Sheets upload did not succeed.")

    print("\n[5/5] Done.")

    # ── Final summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    if bq_success:
        print("✅  BigQuery updated.")
    else:
        print("⚠️  BigQuery upload failed.")

    if sheets_success:
        print("✅  Google Sheets updated (backup).")
    else:
        print("⚠️  Google Sheets upload failed.")

    if failed_channels:
        print("\n⚠️  The following channels did NOT update this run:")
        for label, reason in failed_channels:
            print(f"   ✗  {label:<25}  ({reason})")
    else:
        print("\n✅  All channels updated successfully.")
    print("=" * 55)

if __name__ == "__main__":
    main()
