"""
Pepperstone APAC Performance Marketing Pipeline

Flow:
  1. Parse raw files from ./input/ (or S3 folder)
  2. Upload to BigQuery (source of truth)

Google Sheets reads from BigQuery via Connected Sheets — no pipeline writing needed.
BigQuery time travel (7 days) + fail-safe (7 days) provides backup.
Raw files archived in S3.
"""

from parsers import parse_all
from bq_uploader import upload_to_bigquery

def main():
    print("=" * 55)
    print("  Pepperstone APAC Performance Marketing Pipeline")
    print("=" * 55)

    print("\n[1/2] Parsing raw files from ./input/ ...")
    combined, failed_channels = parse_all()
    ad_rows = combined[combined['QL'].isna() & combined['FT'].isna()]
    ql_rows = combined[combined['QL'].notna() | combined['FT'].notna()]
    print(f"      Ad rows    : {len(ad_rows):,}")
    print(f"      QL/FT rows : {len(ql_rows):,}")
    print(f"      Total QL   : {combined['QL'].fillna(0).astype(int).sum():,}")
    print(f"      Total FT   : {combined['FT'].fillna(0).astype(int).sum():,}")
    print(f"      Total rows : {len(combined):,}")

    print("\n[2/2] Uploading to BigQuery ...")
    bq_success = upload_to_bigquery(combined)

    # ── Final summary ─────────────────────────────────────────────────────────
    print("\n" + "=" * 55)
    if bq_success:
        print("✅  BigQuery updated.")
    else:
        print("⚠️  BigQuery upload failed.")

    if failed_channels:
        print("\n⚠️  The following channels did NOT update this run:")
        for label, reason in failed_channels:
            print(f"   ✗  {label:<25}  ({reason})")
    else:
        print("\n✅  All channels updated successfully.")
    print("=" * 55)

if __name__ == "__main__":
    main()