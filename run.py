"""
Pepperstone APAC Performance Marketing Pipeline
"""

from parsers import parse_all
from uploader import upload_to_sheets, fallback_to_excel
from config import GOOGLE_SHEET_ID, OUTPUT_FILE

def main():
    print("=" * 55)
    print("  Pepperstone APAC Performance Marketing Pipeline")
    print("=" * 55)

    print("\n[1/3] Parsing raw files from ./input/ ...")
    combined = parse_all()
    ad_rows = combined[combined['QL'].isna() & combined['FT'].isna()]
    ql_rows = combined[combined['QL'].notna() | combined['FT'].notna()]
    print(f"      Ad rows    : {len(ad_rows):,}")
    print(f"      QL/FT rows : {len(ql_rows):,}")
    print(f"      Total QL   : {combined['QL'].fillna(0).astype(int).sum():,}")
    print(f"      Total FT   : {combined['FT'].fillna(0).astype(int).sum():,}")
    print(f"      Total rows : {len(combined):,}")

    print("\n[2/3] Uploading to Google Sheets ...")
    success = upload_to_sheets(combined, GOOGLE_SHEET_ID)

    print("\n[3/3] Excel backup disabled.")
    # fallback_to_excel(combined, OUTPUT_FILE)

    if success:
        print("\n✅ Done! Google Sheet updated + local backup saved.")
    else:
        print("\n⚠️  Google Sheets upload failed. Local Excel saved to ./output/")
        print("   Check credentials — see README.md for setup instructions.")

if __name__ == "__main__":
    main()