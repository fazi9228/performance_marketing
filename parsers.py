"""
parsers.py — One parser per data source
Single output: Ad_Performance with QL, FT, Channel_Group, Date_Added columns
"""

import os, re, glob, shutil
import pandas as pd
from config import (
    INPUT_DIR, ARCHIVE_DIR, FILE_PATTERNS,
    BING_CHANNEL_RULES, ADROLL_CHANNEL_RULES, ADROLL_COUNTRY_MAP,
    META_CHANNEL, BILIBILI_CHANNEL, REDNOTE_CHANNEL, BILIBILI_COUNTRY,
    TRADINGVIEW_CHANNEL, TRADINGVIEW_FX_RATE,
    APPLE_CHANNEL, APPLE_COUNTRY_MAP,
    TIKTOK_CHANNEL,
    DOUYIN_CHANNEL, DOUYIN_COUNTRY,
    APAC_COUNTRIES, AD_PERFORMANCE_COLS, UTM_TO_CHANNEL, AD_CHANNEL_GROUP
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def find_file(pattern_key):
    pattern = FILE_PATTERNS[pattern_key]
    matches = glob.glob(os.path.join(INPUT_DIR, f"*{pattern}*"))
    if not matches:
        print(f"      ⚠️  No file found for '{pattern_key}' (pattern: *{pattern}*) — skipping.")
        return None

    def extract_date(path):
        m = re.search(r'(\d{6})', os.path.basename(path))
        if not m: return '000000'
        d = m.group(1)  # DDMMYY
        return d[4:6] + d[2:4] + d[0:2]  # Reorder to YYMMDD

    matches = sorted(matches, key=extract_date, reverse=True)

    if len(matches) > 1:
        print(f"      ⚠️  Multiple files found for '{pattern_key}' — using: {os.path.basename(matches[0])}")
    print(f"      📄 {pattern_key.upper():<10} → {os.path.basename(matches[0])}")
    return matches[0]


def archive_file(filepath):
    """
    Move a processed input file to ./input/archive/, handling name collisions.
    Wrapped in try/except so a locked file (Windows) never crashes the pipeline.
    """
    if not filepath or not os.path.exists(filepath):
        return
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    dest = os.path.join(ARCHIVE_DIR, os.path.basename(filepath))
    if os.path.exists(dest):
        base, ext = os.path.splitext(os.path.basename(filepath))
        import time
        dest = os.path.join(ARCHIVE_DIR, f"{base}_{int(time.time())}{ext}")
    try:
        shutil.move(filepath, dest)
        print(f"      📦 Archived → {os.path.relpath(dest)}")
    except PermissionError as e:
        print(f"      ⚠️  Could not archive {os.path.basename(filepath)} (file still open?) — skipping. {e}")
    except Exception as e:
        print(f"      ⚠️  Could not archive {os.path.basename(filepath)} — {e}")


def empty_df():
    return pd.DataFrame(columns=AD_PERFORMANCE_COLS)

def std_cols(df):
    for col in AD_PERFORMANCE_COLS:
        if col not in df.columns:
            df[col] = None
    return df[AD_PERFORMANCE_COLS].copy()

def get_channel_group(channel):
    return AD_CHANNEL_GROUP.get(str(channel), "Others")

def map_utm(utm):
    u = str(utm).strip()
    ul = u.lower()
    if not u or ul in ['', '-', 'nan', 'none', 'unknown', '(not set)']:
        return ("Organic", "Organic")
    if ul.replace('-','').replace('_','').isdigit():
        return ("IB", "IB")
    if ul.startswith('affiliate-'):
        return ("Affiliates", "Affiliates")
    if ul in ('fb', 'ig') or ul.startswith('facebook') or ul.startswith('instagram'):
        return ("Meta", "Meta")
    if ul in UTM_TO_CHANNEL:
        return UTM_TO_CHANNEL[ul]
    return (u, "Others")

def map_utm_medium(utm, medium):
    m = str(medium).strip().lower()
    if m == 'ib':
        return ("IB", "IB")
    if m == 'affiliates':
        return ("Affiliates", "Affiliates")
    return map_utm(utm)


# ── BING ──────────────────────────────────────────────────────────────────────

def parse_bing(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        df = xl.parse(xl.sheet_names[0], header=None)
        header_row = None
        for i, row in df.iterrows():
            if any("Campaign name" in str(v) for v in row):
                header_row = i; break
        if header_row is None:
            xl.close()
            raise ValueError("Could not find header row.")
        df = xl.parse(xl.sheet_names[0], skiprows=header_row)
        xl.close()
        df.columns = ["Campaign","CampaignID","CampaignType","Date",
                      "Impressions","Clicks","CTR","Spend"]
        df = df[df["Campaign"].notna()]
        df = df[~df["Campaign"].astype(str).str.startswith("©")]
        df = df[df["Campaign"].astype(str) != "Total"]

        def bing_country(name):
            m = re.match(r"^(HK|TH|TW|VN|MY|SG)", str(name))
            return m.group(1) if m else None

        def bing_channel(name):
            n = str(name)
            if "[Pmax]" in n: return BING_CHANNEL_RULES["[Pmax]"]
            if "Brand" in n:  return BING_CHANNEL_RULES["Brand"]
            return BING_CHANNEL_RULES["default"]

        df["Country"]       = df["Campaign"].apply(bing_country)
        df["Channel"]       = df["Campaign"].apply(bing_channel)
        df["Channel_Group"] = df["Channel"].apply(get_channel_group)
        df = df[df["Country"].notna()]
        df["Date"]       = pd.to_datetime(df["Date"]).dt.date
        df["QL"]         = None
        df["FT"]         = None
        df["Creative"]   = None
        df["Date_Added"] = None
        df["Date_Modified"] = None
        out = df[["Date","Country","Channel","Campaign","Creative","Impressions","Clicks","CTR","Spend","QL","FT","Channel_Group","Date_Added","Date_Modified"]].copy()
        out.columns = AD_PERFORMANCE_COLS
        return out, None
    except Exception as e:
        print(f"      ❌ Bing parse error: {e}")
        return empty_df(), str(e)


# ── META ──────────────────────────────────────────────────────────────────────

def parse_meta(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        for s in xl.sheet_names:
            if "raw" in s.lower() or "data" in s.lower():
                sheet = s; break
        df = xl.parse(sheet)
        xl.close()
        if "Country" in df.columns:
            df = df[df["Country"].isin(APAC_COUNTRIES)]
        df["Date"]          = pd.to_datetime(df["Day"]).dt.date
        df["Channel"]       = META_CHANNEL
        df["Channel_Group"] = "Meta"
        df["Clicks"]        = pd.to_numeric(df.get("Clicks (all)"), errors="coerce")
        df["QL"]            = None
        df["FT"]            = None
        df["Creative"]      = None
        df["Date_Added"]    = None
        if "Ad set name" in df.columns:
            df["Campaign name"] = df["Campaign name"].astype(str) + " | " + df["Ad set name"].astype(str)
        rename = {
            "Campaign name"      : "Campaign",
            "CTR (all)"          : "CTR",
            "Amount spent (AUD)" : "Spend (AUD)",
        }
        df = df.rename(columns=rename)
        if "CTR" in df.columns:
            df["CTR"] = pd.to_numeric(df["CTR"], errors="coerce") / 100
        else:
            imp = pd.to_numeric(df.get("Impressions"), errors="coerce")
            df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)
        return std_cols(df), None
    except Exception as e:
        print(f"      ❌ Meta parse error: {e}")
        return empty_df(), str(e)


# ── ADROLL ────────────────────────────────────────────────────────────────────

def parse_adroll(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        df = xl.parse("Daily", header=None)
        header_row = None
        for i, row in df.iterrows():
            vals = [str(v) for v in row if str(v) != "nan"]
            if "Day" in vals and "Campaign" in vals:
                header_row = i; break
        if header_row is None:
            xl.close()
            raise ValueError("Could not find header row in 'Daily' sheet.")
        df = xl.parse("Daily", skiprows=header_row)
        xl.close()

        df = df[df["Day"].notna()]
        df = df[df["Day"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}")]

        def ar_country(name):
            m = re.search(r"APAC_([^_]+)_", str(name))
            return ADROLL_COUNTRY_MAP.get(m.group(1)) if m else None

        def ar_channel(name):
            n = str(name)
            for key, label in ADROLL_CHANNEL_RULES.items():
                if key == "default": continue
                if key.lower() in n.lower(): return label
            return ADROLL_CHANNEL_RULES["default"]

        df["Country"]       = df["Campaign"].apply(ar_country)
        df["Channel"]       = df["Campaign"].apply(ar_channel)
        df["Channel_Group"] = df["Channel"].apply(get_channel_group)
        df["Date"]          = pd.to_datetime(df["Day"]).dt.date
        spend_col           = "Spend" if "Spend" in df.columns else df.columns[df.columns.str.lower().str.contains("spend").argmax()]
        if "Clicks" not in df.columns: df["Clicks"] = None
        df["QL"]         = None
        df["FT"]         = None
        df["Creative"]   = None
        df["Date_Added"] = None
        df["Date_Modified"] = None
        df = df.rename(columns={spend_col: "Spend (AUD)"})
        out = df[["Date","Country","Channel","Campaign","Creative","Impressions","Clicks","CTR","Spend (AUD)","QL","FT","Channel_Group","Date_Added","Date_Modified"]].copy()
        out.columns = AD_PERFORMANCE_COLS
        return out, None
    except Exception as e:
        print(f"      ❌ AdRoll parse error: {e}")
        return empty_df(), str(e)


# ── BILIBILI ──────────────────────────────────────────────────────────────────

def parse_bilibili(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        for s in xl.sheet_names:
            if s.lower() not in ["daily"]:
                sheet = s; break
        df = xl.parse(sheet)
        xl.close()
        df["Country"]       = BILIBILI_COUNTRY
        df["Channel"]       = BILIBILI_CHANNEL
        df["Channel_Group"] = "BiliBili"
        df["Date"]          = pd.to_datetime(df["Date"]).dt.date
        df["QL"]            = None
        df["FT"]            = None
        df["Date_Added"]    = None
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if "impression" in cl: col_map[c] = "Impressions"
            elif "click" in cl and "cost" not in cl and "cpc" not in cl: col_map[c] = "Clicks"
            elif "ctr" in cl: col_map[c] = "CTR"
            elif "cost" in cl and "aud" in cl.replace(" ",""): col_map[c] = "Spend (AUD)"
            elif "campaign" in cl and "name" in cl: col_map[c] = "Campaign"
        df = df.rename(columns=col_map)
        df["Creative"] = df["Creative Type"].astype(str) if "Creative Type" in df.columns else None
        if "Targeting approach" in df.columns and "Creative Type" in df.columns:
            df["Campaign"] = df["Campaign"].astype(str) + " | " + df["Targeting approach"].astype(str) + " | " + df["Creative Type"].astype(str)
        elif "Targeting approach" in df.columns:
            df["Campaign"] = df["Campaign"].astype(str) + " | " + df["Targeting approach"].astype(str)
        return std_cols(df), None
    except Exception as e:
        print(f"      ❌ BiliBili parse error: {e}")
        return empty_df(), str(e)


# ── REDNOTE ───────────────────────────────────────────────────────────────────

def parse_rednote(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        for s in xl.sheet_names:
            sl = s.lower()
            if "daily" not in sl and "reference" not in sl and "참고" not in sl:
                sheet = s; break
        df = xl.parse(sheet)
        xl.close()
        df["Channel"]       = REDNOTE_CHANNEL
        df["Channel_Group"] = "RedNote"
        df["Campaign"]      = df["Main Account"].astype(str) + " - " + df["Placement"].astype(str)
        df["Date"]          = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].notna()].copy()
        df["Date"]          = df["Date"].dt.date
        df["QL"]            = None
        df["FT"]            = None
        df["Date_Added"]    = None
        df["Click"]      = pd.to_numeric(df["Click"],      errors="coerce").fillna(0)
        df["Impression"] = pd.to_numeric(df["Impression"], errors="coerce").fillna(0)
        df["Cost (AUD)"] = pd.to_numeric(df["Cost (AUD)"], errors="coerce").fillna(0)
        df["CTR"] = df.apply(
            lambda r: r["Click"]/r["Impression"] if r["Impression"] > 0 else 0, axis=1)
        df = df.rename(columns={"Impression": "Impressions", "Click": "Clicks", "Cost (AUD)": "Spend (AUD)"})
        df["Creative"] = df["Creative"].astype(str) if "Creative" in df.columns else None
        base = df["Main Account"].astype(str) + " - " + df["Placement"].astype(str) + " | " + df["Targeting Approach"].astype(str) + " | " + df["Creative"].astype(str)
        df["_grp_key"] = df["Date"].astype(str) + base
        df["_seq"] = df.groupby("_grp_key").cumcount() + 1
        df["Campaign"] = base + df["_seq"].apply(lambda x: f" #{x}" if x > 1 else "")
        df = df.drop(columns=["_grp_key", "_seq"])
        return std_cols(df), None
    except Exception as e:
        print(f"      ❌ RedNote parse error: {e}")
        return empty_df(), str(e)


# ── TRADINGVIEW ───────────────────────────────────────────────────────────────

def parse_tradingview(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        frames = []
        for sheet in xl.sheet_names:
            country = sheet.strip().upper()
            if country not in APAC_COUNTRIES:
                continue
            df = xl.parse(sheet)
            if df.empty:
                continue
            df["Country"]       = country
            df["Channel"]       = TRADINGVIEW_CHANNEL
            df["Channel_Group"] = "TradingView"
            df["Campaign"]      = TRADINGVIEW_CHANNEL
            df["Creative"]      = None
            df["QL"]            = None
            df["FT"]            = None
            df["Date_Added"]    = None
            df = df[df["Date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}")]
            df["Date"] = pd.to_datetime(df["Date"]).dt.date

            spend_col = "Amount spent (USD)" if "Amount spent (USD)" in df.columns else None
            if spend_col:
                df["Spend (AUD)"] = pd.to_numeric(df[spend_col], errors="coerce") * TRADINGVIEW_FX_RATE
            elif "Amount spent (AUD)" in df.columns:
                df["Spend (AUD)"] = pd.to_numeric(df["Amount spent (AUD)"], errors="coerce")
            else:
                df["Spend (AUD)"] = None

            df["Impressions"] = pd.to_numeric(df.get("Impressions"), errors="coerce")
            df["Clicks"]      = pd.to_numeric(df.get("Clicks"),      errors="coerce")
            df["CTR"]         = pd.to_numeric(df.get("CTR"),         errors="coerce")

            frames.append(std_cols(df))

        xl.close()
        if not frames:
            raise ValueError("No valid APAC country tabs found.")
        return pd.concat(frames, ignore_index=True), None

    except Exception as e:
        print(f"      ❌ TradingView parse error: {e}")
        return empty_df(), str(e)


# ── APPLE SEARCH ADS ──────────────────────────────────────────────────────────

def parse_apple(filepath):
    """
    Parse Apple Search Ads CSV export.
    File has 7 metadata header rows before the actual column headers.
    Spend is already in AUD. No FX conversion needed.
    """
    try:
        df = pd.read_csv(filepath, skiprows=7)

        df["Country"] = df["Country or Region"].map(APPLE_COUNTRY_MAP)
        df = df[df["Country"].notna() & df["Country"].isin(APAC_COUNTRIES)].copy()

        if df.empty:
            raise ValueError("No rows matched APAC countries after mapping.")

        df["Date"]          = pd.to_datetime(df["Day"]).dt.date
        df["Channel"]       = APPLE_CHANNEL
        df["Channel_Group"] = get_channel_group(APPLE_CHANNEL)
        df["Campaign"]      = df["Campaign Name"].astype(str)
        df["Creative"]      = None
        df["QL"]            = None
        df["FT"]            = None
        df["Date_Added"]    = None

        df["Impressions"]   = pd.to_numeric(df["Impressions"], errors="coerce")
        df["Clicks"]        = pd.to_numeric(df["Taps"],        errors="coerce")
        df["Spend (AUD)"]   = pd.to_numeric(df["Spend"],       errors="coerce")
        df["CTR"] = (
            df["CR (Tap-Through)"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
            .div(100)
        )

        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ Apple parse error: {e}")
        return empty_df(), str(e)


# ── TIKTOK ────────────────────────────────────────────────────────────────────

def parse_tiktok(filepath):
    """
    Parse TikTok Ads Excel export.

    Supports TWO formats:
      A) New (2025+): Single 'Daily' sheet with a Country column.
         Columns: Date, Country, Campaign name, Ad group name, Ad name,
                  Cost (AUD), Impressions, Clicks (all), …
      B) Legacy: One sheet per country (TH, TW, VN …).
         Columns: Week, Campaign name, Ad set name, Amount spent (AUD),
                  Impressions, Clicks (all), CTR, …
    """
    try:
        xl = pd.ExcelFile(filepath)
        frames = []

        # ── Detect format ─────────────────────────────────────────────────
        has_daily = "Daily" in xl.sheet_names
        has_country_tabs = any(s.strip().upper() in APAC_COUNTRIES for s in xl.sheet_names)

        # ── Format A: single Daily sheet with Country column ──────────────
        if has_daily and not has_country_tabs:
            df = xl.parse("Daily", header=0)
            df.columns = [c.strip() for c in df.columns]

            df = df[df["Country"].astype(str).str.strip().str.upper().isin(APAC_COUNTRIES)]
            df["Country"] = df["Country"].astype(str).str.strip().str.upper()

            df = df[df["Campaign name"].notna()]
            df = df[df["Campaign name"].astype(str).str.strip() != ""]

            if df.empty:
                raise ValueError("Daily sheet found but no valid APAC rows.")

            df["Channel"]       = TIKTOK_CHANNEL
            df["Channel_Group"] = get_channel_group(TIKTOK_CHANNEL)
            df["Date"]          = pd.to_datetime(df["Date"]).dt.date
            df["Campaign"]      = df["Campaign name"].astype(str)

            # Creative: prefer Ad name, fall back to Ad group name
            if "Ad name" in df.columns:
                df["Creative"] = df["Ad name"].astype(str)
            elif "Ad group name" in df.columns:
                df["Creative"] = df["Ad group name"].astype(str)
            else:
                df["Creative"] = None

            df["QL"]         = None
            df["FT"]         = None
            df["Date_Added"] = None

            df["Impressions"] = pd.to_numeric(df.get("Impressions"),  errors="coerce")
            df["Clicks"]      = pd.to_numeric(df.get("Clicks (all)"), errors="coerce")

            # Spend: try Cost (AUD) first, then Amount spent (AUD)
            if "Cost (AUD)" in df.columns:
                df["Spend (AUD)"] = pd.to_numeric(df["Cost (AUD)"], errors="coerce")
            elif "Amount spent (AUD)" in df.columns:
                df["Spend (AUD)"] = pd.to_numeric(df["Amount spent (AUD)"], errors="coerce")
            else:
                df["Spend (AUD)"] = None

            # CTR: use column if present, otherwise compute
            if "CTR" in df.columns:
                df["CTR"] = pd.to_numeric(df["CTR"], errors="coerce")
            else:
                imp = pd.to_numeric(df.get("Impressions"), errors="coerce")
                df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)

            frames.append(std_cols(df))

        # ── Format B: one sheet per country (legacy) ──────────────────────
        else:
            for sheet in xl.sheet_names:
                country = sheet.strip().upper()
                if country not in APAC_COUNTRIES:
                    continue

                df = xl.parse(sheet, header=0)
                df.columns = [c.strip() for c in df.columns]

                df["Week"]          = df["Week"].ffill()
                df["Campaign name"] = df["Campaign name"].ffill()

                df = df[df["Campaign name"].astype(str) != "Total"]
                df = df[df["Week"].astype(str) != "Total"]
                df = df[df["Ad set name"].notna()]
                df = df[df["Ad set name"].astype(str).str.strip().ne("")]

                if df.empty:
                    continue

                df["Country"]       = country
                df["Channel"]       = TIKTOK_CHANNEL
                df["Channel_Group"] = get_channel_group(TIKTOK_CHANNEL)
                df["Date"]          = pd.to_datetime(df["Week"]).dt.date
                df["Campaign"]      = df["Campaign name"].astype(str)
                df["Creative"]      = df["Ad set name"].astype(str)
                df["QL"]            = None
                df["FT"]            = None
                df["Date_Added"]    = None

                df["Impressions"]   = pd.to_numeric(df["Impressions"],        errors="coerce")
                df["Clicks"]        = pd.to_numeric(df["Clicks (all)"],       errors="coerce")
                df["Spend (AUD)"]   = pd.to_numeric(df["Amount spent (AUD)"], errors="coerce")
                df["CTR"]           = pd.to_numeric(df["CTR"],                errors="coerce")

                frames.append(std_cols(df))

        xl.close()

        if not frames:
            raise ValueError("No valid APAC country tabs found.")

        return pd.concat(frames, ignore_index=True), None

    except Exception as e:
        print(f"      ❌ TikTok parse error: {e}")
        return empty_df(), str(e)


# ── DOUYIN ─────────────────────────────────────────────────────────────────────

def parse_douyin(filepath):
    """
    Parse Douyin (Chinese TikTok) Excel export.
    Single 'Daily' sheet. Country is always CN.
    Video Play → Impressions, Profile Views → Clicks.
    """
    try:
        xl = pd.ExcelFile(filepath)
        sheet = "Daily" if "Daily" in xl.sheet_names else xl.sheet_names[0]
        df = xl.parse(sheet, header=0)
        df.columns = [c.strip() for c in df.columns]

        df = df[df["Campaign Name"].notna()]
        df = df[df["Campaign Name"].astype(str).str.strip() != ""]

        if df.empty:
            raise ValueError("No valid rows found.")

        df["Country"]       = DOUYIN_COUNTRY
        df["Channel"]       = DOUYIN_CHANNEL
        df["Channel_Group"] = get_channel_group(DOUYIN_CHANNEL)
        df["Date"]          = pd.to_datetime(df["Date"]).dt.date

        # Campaign: Campaign Name | Targeting approach | Creative Type (same pattern as BiliBili)
        df["Campaign"] = (
            df["Campaign Name"].astype(str) + " | " +
            df["Targeting approach"].astype(str) + " | " +
            df["Creative Type"].astype(str)
        )
        df["Creative"]   = df["Creative Type"].astype(str) if "Creative Type" in df.columns else None
        df["QL"]         = None
        df["FT"]         = None
        df["Date_Added"] = None

        df["Impressions"] = pd.to_numeric(df.get("Video Play"),     errors="coerce")
        df["Clicks"]      = pd.to_numeric(df.get("Profile Views"),  errors="coerce")
        df["Spend (AUD)"] = pd.to_numeric(df.get("Cost (AUD)"),     errors="coerce")

        # CTR: compute from Profile Views / Video Play
        imp = df["Impressions"]
        df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)

        xl.close()
        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ Douyin parse error: {e}")
        return empty_df(), str(e)


# ── AFFILIATES ────────────────────────────────────────────────────────────────

def parse_affiliate(filepath):
    """
    Parse Affiliates Excel export.
    Structure: Date (fills down), Country, Type, Commission, Qualified Lead, Funded Trading Client.
    APAC countries only. Commission → Spend (AUD). No impressions/clicks/CTR.
    Added on an ad-hoc basis — not uploaded every week.
    Affiliates rows are excluded from SF QL/FT parse to avoid double-counting.
    """
    try:
        xl = pd.ExcelFile(filepath)
        df = xl.parse(xl.sheet_names[0], header=0)
        xl.close()

        df.columns = ["Date", "Country", "Type", "Commission", "QL", "FT"]

        # Fill down Date, drop grand total row
        df["Date"] = df["Date"].ffill()
        df = df[df["Date"].astype(str) != "Grand Total"]
        df = df[df["Country"].astype(str) != "Total"]
        df = df[df["Country"].notna()]

        # Parse date
        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)

        # All countries except AU and NZ (domestic, not APAC marketing)
        EXCLUDE_COUNTRIES = ["AU", "NZ"]
        df = df[df["Country"].notna()].copy()
        df = df[~df["Country"].isin(EXCLUDE_COUNTRIES)]

        if df.empty:
            raise ValueError("No valid rows found.")

        df["Date"]          = df["Date"].dt.date
        df["Channel"]       = "Affiliates"
        df["Channel_Group"] = "Affiliates"
        df["Campaign"]      = "Affiliates"
        df["Creative"]      = None
        df["Impressions"]   = None
        df["Clicks"]        = None
        df["CTR"]           = None
        df["Spend (AUD)"]   = pd.to_numeric(
            df["Commission"].astype(str).str.replace("$", "", regex=False)
                                         .str.replace(",", "", regex=False)
                                         .str.strip(),
            errors="coerce"
        )
        df["QL"]            = pd.to_numeric(df["QL"], errors="coerce").fillna(0).astype(int)
        df["FT"]            = pd.to_numeric(df["FT"], errors="coerce").fillna(0).astype(int)
        df["Date_Added"]    = None

        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ Affiliates parse error: {e}")
        return empty_df(), str(e)


# ── QL / FT (Salesforce) ──────────────────────────────────────────────────────

def _parse_sf_file(filepath, required_cols, label):
    xl  = pd.ExcelFile(filepath)
    raw = xl.parse(xl.sheet_names[0], header=None)
    xl.close()

    required_lower = {c.lower() for c in required_cols}
    header_row = None
    col_pos    = {}
    for i, row in raw.iterrows():
        vals_lower = {str(v).strip().lower() for v in row if str(v).strip() not in ('', 'nan')}
        if required_lower.issubset(vals_lower):
            vals_list = [str(v).strip() for v in row]
            for req in required_cols:
                for j, v in enumerate(vals_list):
                    if v.lower() == req.lower():
                        col_pos[req] = j; break
            header_row = i
            break

    if header_row is None:
        print(f"      ❌ {label}: Header row not found. Required columns: {required_cols}")
        shown = 0
        for i, row in raw.iterrows():
            vals = [str(v).strip() for v in row if str(v).strip() not in ('', 'nan')]
            if vals:
                print(f"           row {i}: {vals}")
                shown += 1
                if shown >= 5: break
        return None

    c_country = col_pos.get('Billing Country', 1)
    c_date    = col_pos.get('Created Date',    3)
    c_utm     = col_pos.get('Google UTM Source', 4)
    c_medium  = col_pos.get('Google UTM Medium', None)
    c_stage   = col_pos.get('Stage', 5)

    records = []
    for i, row in raw.iterrows():
        if i <= header_row: continue
        vals = [str(v).strip() for v in row]
        if all(v in ('', 'nan') for v in vals): continue

        country = vals[c_country] if c_country < len(vals) else ''
        date    = vals[c_date]    if c_date    < len(vals) else ''
        utm     = vals[c_utm]     if c_utm     < len(vals) else ''
        medium  = vals[c_medium]  if (c_medium is not None and c_medium < len(vals)) else ''
        stage   = vals[c_stage]   if c_stage   < len(vals) else ''

        if country in ('', 'nan', 'Total', 'Grand Total', 'Subtotal', 'Count'): continue
        if country not in APAC_COUNTRIES: continue
        if utm    in ('', 'nan'): utm = ''
        if medium in ('', 'nan'): medium = ''

        rec = {'Country': country, 'Date': date, 'UTM': utm, 'Medium': medium, 'Stage': stage}
        try:
            pd.to_datetime(date, dayfirst=True)
            records.append(rec)
        except:
            continue

    if not records:
        print(f"      ❌ {label}: Header found at row {header_row} but no valid data rows extracted.")
        return None

    return pd.DataFrame(records)


def parse_ql_ft(ql_path, ft_path):
    """Returns (df, error_message_or_None)"""
    try:
        REQUIRED = ['Billing Country', 'Created Date', 'Google UTM Source', 'Stage']

        ql_raw = _parse_sf_file(ql_path, REQUIRED, label="QL")
        if ql_raw is None:
            return empty_df(), "QL header/data not found"

        ql_raw['Date'] = pd.to_datetime(ql_raw['Date'], dayfirst=True)
        ql_raw['Mapped']        = ql_raw.apply(lambda r: map_utm_medium(r['UTM'], r['Medium']), axis=1)
        ql_raw['Channel']       = ql_raw['Mapped'].apply(lambda x: x[0])
        ql_raw['Channel_Group'] = ql_raw['Mapped'].apply(lambda x: x[1])

        # ── Drop Affiliates rows — these come from the dedicated Affiliates file ──
        ql_raw = ql_raw[ql_raw['Channel_Group'] != 'Affiliates']

        ql_agg = ql_raw.groupby(['Date','Country','Channel','Channel_Group']).size().reset_index(name='QL')

        ft_raw = _parse_sf_file(ft_path, REQUIRED, label="FT")
        if ft_raw is None:
            return empty_df(), "FT header/data not found"

        ft_raw = ft_raw[ft_raw['Stage'].isin(['Active', 'Funded NT', 'Funded'])]
        if ft_raw.empty:
            return empty_df(), "FT: no rows with Stage in [Active, Funded NT, Funded]"

        ft_raw['Date'] = pd.to_datetime(ft_raw['Date'], dayfirst=True)
        ft_raw['Mapped']        = ft_raw.apply(lambda r: map_utm_medium(r['UTM'], r['Medium']), axis=1)
        ft_raw['Channel']       = ft_raw['Mapped'].apply(lambda x: x[0])
        ft_raw['Channel_Group'] = ft_raw['Mapped'].apply(lambda x: x[1])

        # ── Drop Affiliates rows — these come from the dedicated Affiliates file ──
        ft_raw = ft_raw[ft_raw['Channel_Group'] != 'Affiliates']

        ft_agg = ft_raw.groupby(['Date','Country','Channel','Channel_Group']).size().reset_index(name='FT')

        merged = pd.merge(ql_agg, ft_agg, on=['Date','Country','Channel','Channel_Group'], how='outer').fillna(0)
        merged[['QL','FT']] = merged[['QL','FT']].astype(int)

        merged['Campaign']    = merged['Channel']
        merged['Creative']    = None
        merged['Impressions'] = None
        merged['Clicks']      = None
        merged['CTR']         = None
        merged['Spend (AUD)'] = None
        merged['Date_Added']  = None
        merged['Date_Modified'] = None
        merged = merged.sort_values(['Date','Country','Channel']).reset_index(drop=True)
        return merged[AD_PERFORMANCE_COLS], None

    except Exception as e:
        import traceback
        traceback.print_exc()
        return empty_df(), str(e)


# ── MASTER PARSE FUNCTION ─────────────────────────────────────────────────────

def parse_all():
    """
    Returns (combined_df, failed_channels)
    failed_channels: list of (channel_label, reason) tuples for the run summary.
    """
    frames          = []
    processed_files = []
    failed_channels = []

    parsers = {
        "Bing"             : ("bing",        parse_bing),
        "Meta"             : ("meta",        parse_meta),
        "AdRoll"           : ("adroll",      parse_adroll),
        "BiliBili"         : ("bilibili",    parse_bilibili),
        "RedNote"          : ("rednote",     parse_rednote),
        "TradingView"      : ("tradingview", parse_tradingview),
        "Apple Search Ads" : ("apple",       parse_apple),
        "TikTok"           : ("tiktok",      parse_tiktok),
        "Douyin"           : ("douyin",      parse_douyin),
        "Affiliates"       : ("affiliates", parse_affiliate),
    }

    for label, (key, parser_fn) in parsers.items():
        filepath = find_file(key)
        if not filepath:
            failed_channels.append((label, "No input file found"))
            continue
        df, err = parser_fn(filepath)
        if err or len(df) == 0:
            reason = err if err else "Parser returned 0 rows"
            failed_channels.append((label, reason))
        else:
            frames.append(df)
            processed_files.append(filepath)
            print(f"             → {len(df):,} rows parsed")

    if frames:
        ad_performance = pd.concat(frames, ignore_index=True)
        ad_performance["Date"] = pd.to_datetime(ad_performance["Date"])
        ad_performance = ad_performance.sort_values(
            ["Date","Country","Channel","Campaign"]).reset_index(drop=True)
    else:
        ad_performance = empty_df()

    # QL + FT rows (Salesforce — Affiliates rows excluded inside parse_ql_ft)
    ql_path = find_file("ql")
    ft_path = find_file("ft")
    if ql_path and ft_path:
        ql_ft_rows, err = parse_ql_ft(ql_path, ft_path)
        if err or len(ql_ft_rows) == 0:
            reason = err if err else "Parser returned 0 rows"
            failed_channels.append(("QL/FT (Salesforce)", reason))
        else:
            print(f"             → {len(ql_ft_rows):,} QL/FT rows parsed")
            ql_ft_rows["Date"] = pd.to_datetime(ql_ft_rows["Date"])
            processed_files.extend([ql_path, ft_path])
            ad_performance = pd.concat([ad_performance, ql_ft_rows], ignore_index=True)
            ad_performance = ad_performance.sort_values(["Date","Country","Channel"]).reset_index(drop=True)
    else:
        missing = []
        if not ql_path: missing.append("QL_ file missing")
        if not ft_path: missing.append("FT_ file missing")
        failed_channels.append(("QL/FT (Salesforce)", "; ".join(missing)))

    # Archive all successfully processed input files
    if processed_files:
        print("\n      Archiving processed input files...")
        for fp in processed_files:
            archive_file(fp)

    return ad_performance, failed_channels