"""
rollback.py — Restore the last backup to Google Sheets

Usage:  python rollback.py
        python rollback.py path/to/specific_backup.xlsx

If no path given, uses the most recent backup in ./output/backups/
"""

import os, sys, glob
import pandas as pd
from config import GOOGLE_SHEET_ID, CREDENTIALS_FILE, SHEET_AD_PERFORMANCE

BACKUP_DIR = os.path.join("output", "backups")


def find_latest_backup():
    backups = sorted(glob.glob(os.path.join(BACKUP_DIR, "Ad_Performance_backup_*.xlsx")))
    if not backups:
        print("❌ No backups found in ./output/backups/")
        return None
    latest = backups[-1]
    print(f"📂 Latest backup: {latest}")
    return latest


def rollback(filepath):
    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return False

    df = pd.read_excel(filepath, dtype=str)
    print(f"📊 Backup contains {len(df):,} rows, {len(df.columns)} columns")
    print(f"   Columns: {list(df.columns)}")

    # Confirm before proceeding
    print(f"\n⚠️  This will REPLACE all data in the Google Sheet with the backup.")
    print(f"   Sheet: {GOOGLE_SHEET_ID}")
    print(f"   Tab:   {SHEET_AD_PERFORMANCE}")
    confirm = input("   Type 'YES' to confirm: ").strip()
    if confirm != "YES":
        print("   Cancelled.")
        return False

    try:
        import gspread
        from google.oauth2.service_account import Credentials
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds  = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=scopes)
        client = gspread.authorize(creds)
        sheet  = client.open_by_key(GOOGLE_SHEET_ID)
        ws     = sheet.worksheet(SHEET_AD_PERFORMANCE)

        print("\n🔄 Clearing sheet...")
        ws.clear()

        print("📝 Writing backup data...")
        all_rows = [df.columns.tolist()] + df.fillna("").values.tolist()

        required_rows = len(all_rows) + 500
        ws.resize(rows=required_rows, cols=len(df.columns))

        BATCH_SIZE = 400
        for i in range(0, len(all_rows), BATCH_SIZE):
            chunk = all_rows[i:i + BATCH_SIZE]
            start_row = i + 1
            ws.update(chunk, f"A{start_row}")
            pct = min(100, int((i + len(chunk)) / len(all_rows) * 100))
            print(f"   {pct}% written ({i + len(chunk):,} / {len(all_rows):,} rows)")

        print(f"\n✅ Rollback complete — {len(df):,} rows restored.")
        print(f"   https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}")
        return True

    except Exception as e:
        import traceback
        print(f"❌ Rollback failed: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        filepath = find_latest_backup()

    if filepath:
        rollback(filepath)
