"""
uploader.py — Google Sheets upload + Excel fallback (single sheet)
"""

import os
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from config import (
    CREDENTIALS_FILE, SHEET_AD_PERFORMANCE,
    OUTPUT_DIR, DEDUP_KEYS_AD
)


def upload_to_sheets(combined, sheet_id):
    if not sheet_id or sheet_id == "YOUR_GOOGLE_SHEET_ID_HERE":
        print("      ⚠️  No Google Sheet ID configured — skipping upload.")
        return False
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"      ⚠️  credentials.json not found — skipping upload.")
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
        sheet  = client.open_by_key(sheet_id)
        _write_tab(sheet, SHEET_AD_PERFORMANCE, combined, DEDUP_KEYS_AD)
        print(f"      ✅ Google Sheet updated.")
        print(f"         https://docs.google.com/spreadsheets/d/{sheet_id}")
        return True
    except ImportError:
        print("      ❌ gspread not installed. Run: pip install gspread google-auth")
        return False
    except Exception as e:
        import traceback
        print(f"      ❌ Google Sheets upload failed: {e}")
        traceback.print_exc()
        return False


def _write_tab(sheet, tab_name, new_df, dedup_keys):
    try:
        ws = sheet.worksheet(tab_name)
        existing_data = ws.get_all_records()
        existing_df   = pd.DataFrame(existing_data)
    except Exception:
        ws = sheet.add_worksheet(title=tab_name, rows=50000, cols=20)
        existing_df = pd.DataFrame()

    new_df = new_df.copy()
    new_df["Date"] = pd.to_datetime(new_df["Date"]).dt.strftime("%m/%d/%Y")

    if not existing_df.empty:
        existing_df["Date"] = existing_df["Date"].astype(str)
        # Dedup by Date + Country + Channel (not just Date)
        existing_keys = set(
            zip(
                existing_df["Date"],
                existing_df["Country"].astype(str),
                existing_df["Channel"].astype(str)
            )
        )
        new_rows = new_df[~new_df.apply(
            lambda r: (r["Date"], str(r["Country"]), str(r["Channel"])) in existing_keys, axis=1
        )]
        if len(new_rows) == 0:
            print(f"         ✓ {tab_name}: no new rows — nothing added (existing: {len(existing_df):,} rows)")
            return
        combined = pd.concat([existing_df, new_rows], ignore_index=True)
        combined = combined.sort_values(dedup_keys).reset_index(drop=True)
        print(f"         ✓ {tab_name}: +{len(new_rows):,} new rows added ({len(combined):,} total)")
    else:
        combined = new_df
        print(f"         ✓ {tab_name}: {len(combined):,} rows written (fresh sheet)")

    ws.clear()
    all_rows = [combined.columns.tolist()] + combined.fillna("").values.tolist()

    # Resize sheet to fit all rows (with buffer) before writing
    required_rows = len(all_rows) + 500
    ws.resize(rows=required_rows, cols=20)

    # gspread ~5000 cell limit per call — batch to avoid truncation
    BATCH_SIZE = 400  # 400 rows x 11 cols = 4,400 cells, safely under limit
    for i in range(0, len(all_rows), BATCH_SIZE):
        chunk = all_rows[i:i + BATCH_SIZE]
        start_row = i + 1
        ws.update(chunk, f"A{start_row}")


def fallback_to_excel(combined, filepath):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    out = combined.copy()
    out["Date"] = pd.to_datetime(out["Date"]).dt.strftime("%m/%d/%Y")

    if os.path.exists(filepath):
        try:
            existing = pd.read_excel(filepath, sheet_name=SHEET_AD_PERFORMANCE, dtype=str)
            existing_dates = set(existing["Date"].unique())
            new_rows = out[~out["Date"].isin(existing_dates)]
            out = pd.concat([existing, new_rows], ignore_index=True).sort_values(DEDUP_KEYS_AD).reset_index(drop=True)
            print(f"      ↻  Appended: +{len(new_rows):,} new rows")
        except Exception as e:
            print(f"      ⚠️  Could not read existing file ({e}) — overwriting.")

    for col in ["Impressions","Clicks","Spend (AUD)","CTR","QL","FT"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    _write_excel(out, filepath)
    print(f"      ✅ Excel saved → {filepath}")
    print(f"         Total rows: {len(out):,}")


def _write_excel(df, filepath):
    wb = Workbook()
    HEADER_FILL = PatternFill("solid", start_color="1F3864")
    ALT_FILL    = PatternFill("solid", start_color="EEF2F7")
    WHITE_FILL  = PatternFill("solid", start_color="FFFFFF")
    HEADER_FONT = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    DATA_FONT   = Font(name="Arial", size=10)
    CENTER      = Alignment(horizontal="center", vertical="center")
    LEFT        = Alignment(horizontal="left",   vertical="center")
    RIGHT       = Alignment(horizontal="right",  vertical="center")
    thin        = Side(style="thin", color="D0D0D0")
    BORDER      = Border(left=thin, right=thin, top=thin, bottom=thin)

    ws = wb.active
    ws.title = SHEET_AD_PERFORMANCE
    ws.freeze_panes = "A2"

    headers    = ["Date","Country","Channel","Campaign","Creative","Impressions","Clicks","CTR","Spend (AUD)","QL","FT","Channel_Group"]
    col_widths = [14, 10, 22, 48, 24, 14, 12, 10, 14, 10, 10, 16]

    ws.row_dimensions[1].height = 22
    for ci, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(1, ci, h)
        cell.font = HEADER_FONT; cell.fill = HEADER_FILL
        cell.alignment = CENTER
        ws.column_dimensions[get_column_letter(ci)].width = w

    for ri, row in enumerate(df.itertuples(index=False), 2):
        fill = ALT_FILL if ri % 2 == 0 else WHITE_FILL
        ws.row_dimensions[ri].height = 18

        def c(ci, val, align=RIGHT):
            cell = ws.cell(ri, ci, val)
            cell.font = DATA_FONT; cell.fill = fill
            cell.alignment = align
            return cell

        c(1,  str(row.Date), LEFT)
        c(2,  str(row.Country)       if pd.notna(row.Country)       else "", LEFT)
        c(3,  str(row.Channel)       if pd.notna(row.Channel)       else "", LEFT)
        c(4,  str(row.Campaign)      if pd.notna(row.Campaign)      else "", LEFT)
        impr = getattr(row,"Impressions",None)
        c(5,  float(impr) if pd.notna(impr) else "")
        clks = getattr(row,"Clicks",None)
        c(6,  float(clks) if pd.notna(clks) else "")
        ctr  = getattr(row,"CTR",None)
        if pd.notna(ctr):
            cell = c(7, round(float(ctr)*100, 4))
            cell.number_format = '0.00"%"'
        else:
            c(7, "")
        spnd = getattr(row,"Spend (AUD)",None)
        c(8,  float(spnd) if pd.notna(spnd) else "")
        ql   = getattr(row,"QL",None)
        c(9,  int(float(ql)) if pd.notna(ql) and str(ql) != '' else "")
        ft   = getattr(row,"FT",None)
        c(10, int(float(ft)) if pd.notna(ft) and str(ft) != '' else "")
        c(11, str(row.Channel_Group) if pd.notna(row.Channel_Group) else "", LEFT)

    for ri in range(2, len(df)+2):
        ws.cell(ri,5).number_format = "#,##0"
        ws.cell(ri,6).number_format = "#,##0"
        ws.cell(ri,8).number_format = "#,##0.00"
        ws.cell(ri,9).number_format = "#,##0"
        ws.cell(ri,10).number_format = "#,##0"

    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"
    wb.save(filepath)