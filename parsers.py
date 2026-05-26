"""
parsers.py — One parser per data source
Single output: Ad_Performance with QL, FT, Channel_Group, Date_Added columns
"""

import os, re, glob, shutil
import pandas as pd
from config import (
    INPUT_DIR, ARCHIVE_DIR, FILE_PATTERNS,
    BING_CHANNEL_RULES, ADROLL_CHANNEL_RULES, ADROLL_COUNTRY_MAP,
    META_CHANNEL, META_AGENCY_CHANNEL,
    BILIBILI_CHANNEL, REDNOTE_CHANNEL, BILIBILI_COUNTRY,
    TRADINGVIEW_CHANNEL, TRADINGVIEW_FX_RATE,
    APPLE_CHANNEL, APPLE_COUNTRY_MAP,
    TIKTOK_CHANNEL,
    DOUYIN_CHANNEL, DOUYIN_COUNTRY,
    KUAISHOU_CHANNEL, KUAISHOU_COUNTRY,
    TA_MEDIA_CHANNEL,
    WECHAT_CHANNEL, WECHAT_COUNTRY,
    YOUTUBE_CHANNEL,
    APAC_COUNTRIES, AD_PERFORMANCE_COLS, UTM_TO_CHANNEL, AD_CHANNEL_GROUP
)

VIDEO_PLAY_PCT_COLS = {
    "Video plays at 25%":  "Video_Plays_25",
    "Video plays at 50%":  "Video_Plays_50",
    "Video plays at 75%":  "Video_Plays_75",
    "Video plays at 100%": "Video_Plays_100",
}

def _extract_video_cols(df, views_source_col):
    """
    Coerce the 5 video metrics to numeric on `df` in place.
    `views_source_col` is the source column that maps to Video_Views
    (e.g. '3-second video plays' for Meta, 'Views' for TikTok / DV360).
    Missing columns become None.
    """
    if views_source_col and views_source_col in df.columns:
        df["Video_Views"] = pd.to_numeric(df[views_source_col], errors="coerce")
    else:
        df["Video_Views"] = None
    for src, dst in VIDEO_PLAY_PCT_COLS.items():
        df[dst] = pd.to_numeric(df[src], errors="coerce") if src in df.columns else None

# ── Helpers ───────────────────────────────────────────────────────────────────

def find_file(pattern_key):
    pattern = FILE_PATTERNS[pattern_key]
    matches = glob.glob(os.path.join(INPUT_DIR, f"*{pattern}*"))

    # Exclude files that match a more-specific pattern.
    # e.g. when searching for "Meta_", exclude files matching "Meta_Agency_"
    more_specific = [
        v for k, v in FILE_PATTERNS.items()
        if k != pattern_key and v.startswith(pattern) and len(v) > len(pattern)
    ]
    if more_specific:
        matches = [
            m for m in matches
            if not any(sp in os.path.basename(m) for sp in more_specific)
        ]

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
    # Known UTM mappings (exact match)
    if ul in UTM_TO_CHANNEL:
        return UTM_TO_CHANNEL[ul]

    # ── Substring-based fallback ──────────────────────────────────────────
    _SUBSTRING_RULES = [
        ("youtube",     ("YouTube",           "YouTube")),
        ("google",      ("Google",           "Google")),
        ("bing",        ("Bing",             "Bing")),
        ("apple",       ("Apple Search Ads", "Apple Search Ads")),
        ("tiktok",      ("TikTok",           "TikTok")),
        ("kuaishou",    ("Kuaishou",         "Kuaishou")),
        ("ta-media",    ("TA Media",         "TA Media")),
        ("chatgpt",     ("ChatGPT",          "ChatGPT")),
        ("coccoc",      ("CocCoc",           "CocCoc")),
        ("adroll",      ("AdRoll",           "AdRoll")),
        ("tradingview", ("TradingView",      "TradingView")),
    ]
    for keyword, mapping in _SUBSTRING_RULES:
        if keyword in ul:
            return mapping

    return (u, "Others")

def map_utm_medium(utm, medium):
    m = str(medium).strip().lower()
    if m == 'ib':
        return ("IB", "IB")
    if m == 'affiliates':
        return ("Affiliates", "Affiliates")
    return map_utm(utm)


# ── BING ──────────────────────────────────────────────────────────────────────

# Regex matches all APAC country codes at start of campaign name
_BING_COUNTRY_RE = re.compile(r"^(HK|TH|TW|VN|MY|SG|CN|IN|ID|PH|MN)")

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
            m = _BING_COUNTRY_RE.match(str(name))
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
        df = df.rename(columns={"Spend": "Spend (AUD)"})
        return std_cols(df), None
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

        # Video metrics — Meta's first-stage view is the 3-second play.
        _extract_video_cols(df, views_source_col="3-second video plays")

        return std_cols(df), None
    except Exception as e:
        print(f"      ❌ Meta parse error: {e}")
        return empty_df(), str(e)


# ── META AGENCY ───────────────────────────────────────────────────────────────

def parse_meta_agency(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        for s in xl.sheet_names:
            sl = s.lower()
            if "daily" in sl or "raw" in sl or "data" in sl:
                sheet = s; break
        df = xl.parse(sheet)
        xl.close()

        if "Country" in df.columns:
            df = df[df["Country"].isin(APAC_COUNTRIES)]

        date_col = "Date" if "Date" in df.columns else "Day"
        df["Date"] = pd.to_datetime(df[date_col], errors="coerce")
        df = df[df["Date"].notna()].copy()
        df["Date"] = df["Date"].dt.date

        df["Channel"]       = META_AGENCY_CHANNEL
        df["Channel_Group"] = "Meta"
        df["QL"]            = None
        df["FT"]            = None
        df["Creative"]      = None
        df["Date_Added"]    = None
        df["Date_Modified"] = None

        clicks_col = "Clicks (all)" if "Clicks (all)" in df.columns else "Clicks"
        df["Clicks"] = pd.to_numeric(df.get(clicks_col), errors="coerce")
        df["Impressions"] = pd.to_numeric(df.get("Impressions"), errors="coerce")

        if "Ad set name" in df.columns and "Campaign name" in df.columns:
            df["Campaign"] = df["Campaign name"].astype(str) + " | " + df["Ad set name"].astype(str)
        elif "Campaign name" in df.columns:
            df["Campaign"] = df["Campaign name"]
        else:
            df["Campaign"] = META_AGENCY_CHANNEL

        spend_col = "Amount spent (AUD)" if "Amount spent (AUD)" in df.columns else None
        if spend_col:
            df["Spend (AUD)"] = pd.to_numeric(df[spend_col], errors="coerce")
        else:
            df["Spend (AUD)"] = None

        if "CTR (all)" in df.columns:
            df["CTR"] = pd.to_numeric(df["CTR (all)"], errors="coerce") / 100
        elif "CTR" in df.columns:
            df["CTR"] = pd.to_numeric(df["CTR"], errors="coerce") / 100
        else:
            df["CTR"] = (df["Clicks"] / df["Impressions"]).where(df["Impressions"] > 0, other=None)

        # Video metrics — Meta's first-stage view is the 3-second play.
        _extract_video_cols(df, views_source_col="3-second video plays")

        return std_cols(df), None
    except Exception as e:
        print(f"      ❌ Meta Agency parse error: {e}")
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
        return std_cols(df), None
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

        # Country: read from file if column exists, else default to CN
        if "Country" in df.columns:
            df["Country"] = df["Country"].astype(str).str.strip().str.upper()
        else:
            df["Country"] = BILIBILI_COUNTRY

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
            if s.lower() == "daily":
                sheet = s; break
        df = xl.parse(sheet)
        xl.close()
        df["Channel"]       = REDNOTE_CHANNEL
        df["Channel_Group"] = "RedNote"
        df["Country"]       = df["Country"].astype(str).str.strip().str.upper() if "Country" in df.columns else None
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
    Parse Apple Search Ads export.
    Supports both .xlsx and .csv formats — both have 6 metadata rows
    before the header row (row 7 in 1-indexed / skiprows=6 in pandas).
    Columns: Date, Campaign Name, Country or Region, CR (Tap-Through),
             Spend, Impressions, Taps, Installs (Tap-Through)
    CTR may be a decimal (0.142857) or a percentage string ("14.29%").
    """
    try:
        # ── Read file (xlsx or csv) ───────────────────────────────────────
        ext = os.path.splitext(filepath)[1].lower()
        if ext in (".xlsx", ".xls"):
            xl = pd.ExcelFile(filepath)
            df = xl.parse(xl.sheet_names[0], skiprows=6)
            xl.close()
        else:
            try:
                df = pd.read_csv(filepath, skiprows=6, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, skiprows=6, encoding="cp1252")

        df.columns = [c.strip() for c in df.columns]

        df["Country"] = df["Country or Region"].map(APPLE_COUNTRY_MAP)
        df = df[df["Country"].notna() & df["Country"].isin(APAC_COUNTRIES)].copy()

        if df.empty:
            raise ValueError("No rows matched APAC countries after mapping.")

        # Date column — may be called "Date" or "Day"
        date_col = "Date" if "Date" in df.columns else "Day"
        df["Date"]          = pd.to_datetime(df[date_col]).dt.date
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

        # CTR: handle both decimal (0.14) and percentage string ("14.29%")
        ctr_raw = df["CR (Tap-Through)"].astype(str).str.strip()
        has_pct = ctr_raw.str.contains("%", na=False).any()
        if has_pct:
            df["CTR"] = (
                ctr_raw.str.replace("%", "", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
                .div(100)
            )
        else:
            df["CTR"] = pd.to_numeric(df["CR (Tap-Through)"], errors="coerce")

        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ Apple parse error: {e}")
        return empty_df(), str(e)


# ── TIKTOK ────────────────────────────────────────────────────────────────────

def parse_tiktok(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        frames = []

        has_daily = "Daily" in xl.sheet_names
        has_country_tabs = any(s.strip().upper() in APAC_COUNTRIES for s in xl.sheet_names)

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

            if "Cost (AUD)" in df.columns:
                df["Spend (AUD)"] = pd.to_numeric(df["Cost (AUD)"], errors="coerce")
            elif "Amount spent (AUD)" in df.columns:
                df["Spend (AUD)"] = pd.to_numeric(df["Amount spent (AUD)"], errors="coerce")
            else:
                df["Spend (AUD)"] = None

            if "CTR" in df.columns:
                df["CTR"] = pd.to_numeric(df["CTR"], errors="coerce")
            else:
                imp = pd.to_numeric(df.get("Impressions"), errors="coerce")
                df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)

            # Video metrics — TikTok's first-stage view is the 2-sec "Views" column.
            _extract_video_cols(df, views_source_col="Views")

            frames.append(std_cols(df))

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
    try:
        xl = pd.ExcelFile(filepath)
        sheet = "Daily" if "Daily" in xl.sheet_names else xl.sheet_names[0]
        df = xl.parse(sheet, header=0)
        df.columns = [c.strip() for c in df.columns]

        df = df[df["Campaign Name"].notna()]
        df = df[df["Campaign Name"].astype(str).str.strip() != ""]

        if df.empty:
            raise ValueError("No valid rows found.")

        # Country: read from file if column exists, else default to CN
        if "Country" in df.columns:
            df["Country"] = df["Country"].astype(str).str.strip().str.upper()
        else:
            df["Country"] = DOUYIN_COUNTRY

        df["Channel"]       = DOUYIN_CHANNEL
        df["Channel_Group"] = get_channel_group(DOUYIN_CHANNEL)
        df["Date"]          = pd.to_datetime(df["Date"]).dt.date

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

        imp = df["Impressions"]
        df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)

        xl.close()
        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ Douyin parse error: {e}")
        return empty_df(), str(e)


# ── KUAISHOU ──────────────────────────────────────────────────────────────────

def parse_kuaishou(filepath):
    """
    Parse Kuaishou (快手) daily performance data.
    Structure mirrors RedNote: Daily sheet with Date, Country, Main Account,
    Placement, Targeting Approach, Creative, Impression, Click, Cost (AUD), CTR.
    """
    try:
        xl = pd.ExcelFile(filepath)
        sheet = "Daily" if "Daily" in xl.sheet_names else xl.sheet_names[0]
        df = xl.parse(sheet)
        xl.close()

        df["Channel"]       = KUAISHOU_CHANNEL
        df["Channel_Group"] = get_channel_group(KUAISHOU_CHANNEL)

        # Country: read from file if column exists, else default to CN
        if "Country" in df.columns:
            df["Country"] = df["Country"].astype(str).str.strip().str.upper()
        else:
            df["Country"] = KUAISHOU_COUNTRY

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].notna()].copy()
        df["Date"] = df["Date"].dt.date

        df["QL"]         = None
        df["FT"]         = None
        df["Date_Added"] = None

        df["Click"]      = pd.to_numeric(df["Click"],      errors="coerce").fillna(0)
        df["Impression"] = pd.to_numeric(df["Impression"], errors="coerce").fillna(0)
        df["Cost (AUD)"] = pd.to_numeric(df["Cost (AUD)"], errors="coerce").fillna(0)
        df["CTR"] = df.apply(
            lambda r: r["Click"] / r["Impression"] if r["Impression"] > 0 else 0, axis=1)

        df = df.rename(columns={
            "Impression": "Impressions",
            "Click":      "Clicks",
            "Cost (AUD)": "Spend (AUD)",
        })

        df["Creative"] = df["Creative"].astype(str) if "Creative" in df.columns else None

        # Build campaign string: Account - Placement | Targeting | Creative
        base = (
            df["Main Account"].astype(str) + " - " +
            df["Placement"].astype(str)    + " | " +
            df["Targeting Approach"].astype(str) + " | " +
            df["Creative"].astype(str)
        )

        # Deduplicate same-day identical campaign strings with sequence numbers
        df["_grp_key"] = df["Date"].astype(str) + base
        df["_seq"]     = df.groupby("_grp_key").cumcount() + 1
        df["Campaign"] = base + df["_seq"].apply(lambda x: f" #{x}" if x > 1 else "")
        df = df.drop(columns=["_grp_key", "_seq"])

        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ Kuaishou parse error: {e}")
        return empty_df(), str(e)


# ── TA MEDIA ──────────────────────────────────────────────────────────────────

def parse_ta_media(filepath):
    """
    Parse TA Media daily performance data.
    Columns: Date, Country, Ad group, Creative, Cost (AUD), Impression, Clicks.
    Date format may be YYYY.M.DD (e.g. 2026.4.13).
    Country column is required.
    """
    try:
        xl = pd.ExcelFile(filepath)
        sheet = "Daily" if "Daily" in xl.sheet_names else xl.sheet_names[0]
        df = xl.parse(sheet)
        xl.close()

        # Country is required
        if "Country" not in df.columns:
            raise ValueError("Country column not found — please add Country to the file.")

        df["Country"] = df["Country"].astype(str).str.strip().str.upper()
        df = df[df["Country"].isin(APAC_COUNTRIES)]

        if df.empty:
            raise ValueError("No valid APAC country rows found.")

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].notna()].copy()
        df["Date"] = df["Date"].dt.date

        df["Channel"]       = TA_MEDIA_CHANNEL
        df["Channel_Group"] = get_channel_group(TA_MEDIA_CHANNEL)
        df["QL"]            = None
        df["FT"]            = None
        df["Date_Added"]    = None

        # Clean trailing tabs from Creative values
        df["Creative"] = df["Creative"].astype(str).str.strip() if "Creative" in df.columns else None

        # Build campaign: Ad group | Creative
        if "Ad group" in df.columns:
            df["Campaign"] = df["Ad group"].astype(str).str.strip() + " | " + df["Creative"].astype(str)
        else:
            df["Campaign"] = TA_MEDIA_CHANNEL

        df["Impressions"]  = pd.to_numeric(df.get("Impression"), errors="coerce")
        df["Clicks"]       = pd.to_numeric(df.get("Clicks"),     errors="coerce")
        df["Spend (AUD)"]  = pd.to_numeric(df.get("Cost (AUD)"), errors="coerce")

        imp = df["Impressions"]
        df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)

        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ TA Media parse error: {e}")
        return empty_df(), str(e)


# ── WECHAT ────────────────────────────────────────────────────────────────────

def parse_wechat(filepath):
    """
    WeChat ads — single sheet, header on row 1.
    Columns: Date, Week, Country, Placement, Targeting Approach, Creative,
             Cost (AUD), Impressions, Clicks
    Campaign is a composite of Placement | Targeting Approach | Creative
    (no native Campaign Name column in the export).
    """
    try:
        xl = pd.ExcelFile(filepath)
        df = xl.parse(xl.sheet_names[0], header=0)
        xl.close()
        df.columns = [str(c).strip() for c in df.columns]
        if "Impression" in df.columns and "Impressions" not in df.columns:
            df = df.rename(columns={"Impression": "Impressions"})

        df = df[df["Date"].notna()]
        if df.empty:
            raise ValueError("No valid rows found.")

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].notna()].copy()
        df["Date"] = df["Date"].dt.date

        if "Country" in df.columns:
            df["Country"] = df["Country"].astype(str).str.strip().str.upper()
        else:
            df["Country"] = WECHAT_COUNTRY

        df["Channel"]       = WECHAT_CHANNEL
        df["Channel_Group"] = get_channel_group(WECHAT_CHANNEL)
        df["QL"]            = None
        df["FT"]            = None
        df["Date_Added"]    = None
        df["Date_Modified"] = None

        df["Impressions"] = pd.to_numeric(df.get("Impressions"), errors="coerce")
        df["Clicks"]      = pd.to_numeric(df.get("Clicks"),      errors="coerce")
        df["Spend (AUD)"] = pd.to_numeric(df.get("Cost (AUD)"),  errors="coerce")

        imp = df["Impressions"]
        df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)

        creative_col = df["Creative"].astype(str) if "Creative" in df.columns else ""
        df["Creative"] = creative_col

        placement = df["Placement"].astype(str)         if "Placement" in df.columns         else pd.Series([""] * len(df), index=df.index)
        targeting = df["Targeting Approach"].astype(str) if "Targeting Approach" in df.columns else pd.Series([""] * len(df), index=df.index)
        df["Campaign"] = placement.str.cat(targeting, sep=" | ").str.cat(df["Creative"].astype(str), sep=" | ")

        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ WeChat parse error: {e}")
        return empty_df(), str(e)


# ── DV360 (YouTube) ───────────────────────────────────────────────────────────

def parse_dv360(filepath):
    """
    Parse DV360 export for YouTube media.
    Columns: Date, Country, Campaign Name, Ad Set Name, Ad Name,
             Cost (AUD), Clicks, Impressions, Views,
             Video plays at 25/50/75/100%
    Channel is YouTube — DV360 is the source platform.
    """
    try:
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        for s in xl.sheet_names:
            sl = s.lower()
            if sl in ("data", "daily", "raw"):
                sheet = s; break
        df = xl.parse(sheet)
        xl.close()

        df.columns = [str(c).strip() for c in df.columns]

        if "Country" in df.columns:
            df["Country"] = df["Country"].astype(str).str.strip().str.upper()
            df = df[df["Country"].isin(APAC_COUNTRIES)]

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].notna()].copy()
        df["Date"] = df["Date"].dt.date

        if df.empty:
            raise ValueError("No valid APAC rows found.")

        df["Channel"]       = YOUTUBE_CHANNEL
        df["Channel_Group"] = get_channel_group(YOUTUBE_CHANNEL)
        df["QL"]            = None
        df["FT"]            = None
        df["Date_Added"]    = None
        df["Date_Modified"] = None

        df["Impressions"] = pd.to_numeric(df.get("Impressions"), errors="coerce")
        df["Clicks"]      = pd.to_numeric(df.get("Clicks"),      errors="coerce")
        df["Spend (AUD)"] = pd.to_numeric(df.get("Cost (AUD)"),  errors="coerce")

        imp = df["Impressions"]
        df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)

        # Campaign = Campaign Name | Ad Set Name; Creative = Ad Name
        camp = df["Campaign Name"].astype(str) if "Campaign Name" in df.columns else pd.Series([""] * len(df), index=df.index)
        if "Ad Set Name" in df.columns:
            df["Campaign"] = camp + " | " + df["Ad Set Name"].astype(str)
        else:
            df["Campaign"] = camp
        df["Creative"] = df["Ad Name"].astype(str) if "Ad Name" in df.columns else None

        # Video metrics — DV360's first-stage view is "Views".
        _extract_video_cols(df, views_source_col="Views")

        return std_cols(df), None

    except Exception as e:
        print(f"      ❌ DV360 parse error: {e}")
        return empty_df(), str(e)


# ── AFFILIATES ────────────────────────────────────────────────────────────────

def parse_affiliate(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        df = xl.parse(xl.sheet_names[0], header=0)
        xl.close()

        df.columns = ["Date", "Country", "Type", "Commission", "QL", "FT"]

        df["Date"] = df["Date"].ffill()
        df = df[df["Date"].astype(str) != "Grand Total"]
        df = df[df["Country"].astype(str) != "Total"]
        df = df[df["Country"].notna()]

        df["Date"] = pd.to_datetime(df["Date"], dayfirst=True)

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

_SF_COUNTRY_SKIP = {'', 'nan', 'total', 'grand total', 'subtotal', 'count'}

def _parse_sf_file(filepath, required_cols, label, date_col='Created Date'):
    """
    date_col: which column to use as the row Date.
        QL uses 'Created Date' (lead creation event).
        FT uses 'First Trade Date' (when the funded trade actually happened).
    """
    xl  = pd.ExcelFile(filepath)
    raw = xl.parse(xl.sheet_names[0], header=None)
    xl.close()

    if date_col not in required_cols:
        required_cols = list(required_cols) + [date_col]
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
    c_date    = col_pos.get(date_col,          3)
    c_utm     = col_pos.get('Google UTM Source', 4)
    c_medium  = col_pos.get('Google UTM Medium', None)
    c_stage   = col_pos.get('Stage', 5)

    records = []
    seen_countries = set()
    for i, row in raw.iterrows():
        if i <= header_row: continue
        vals = [str(v).strip() for v in row]
        if all(v in ('', 'nan') for v in vals): continue

        country = vals[c_country] if c_country < len(vals) else ''
        date    = vals[c_date]    if c_date    < len(vals) else ''
        utm     = vals[c_utm]     if c_utm     < len(vals) else ''
        medium  = vals[c_medium]  if (c_medium is not None and c_medium < len(vals)) else ''
        stage   = vals[c_stage]   if c_stage   < len(vals) else ''

        if country.lower().strip() in _SF_COUNTRY_SKIP:
            continue
        if country not in APAC_COUNTRIES:
            continue
        if utm    in ('', 'nan'): utm = ''
        if medium in ('', 'nan'): medium = ''

        seen_countries.add(country)
        rec = {'Country': country, 'Date': date, 'UTM': utm, 'Medium': medium, 'Stage': stage}
        try:
            pd.to_datetime(date, dayfirst=True)
            records.append(rec)
        except:
            continue

    if not records:
        print(f"      ❌ {label}: Header found at row {header_row} but no valid data rows extracted.")
        return None

    print(f"         {label} countries found: {sorted(seen_countries)}")
    return pd.DataFrame(records)


_SF_REQUIRED = ['Billing Country', 'Created Date', 'Google UTM Source', 'Google UTM Medium', 'Stage']


def _build_sf_rows(raw, count_col):
    """
    Map UTM, drop Affiliates rows, aggregate by Date+Country+Channel+Channel_Group.
    The other metric column (FT when count_col='QL', and vice versa) is set to <NA>
    so the BQ MERGE preserves whatever target already has for it.
    """
    raw = raw.copy()
    # Normalize to date — FT's First Trade Date includes a timestamp, which would
    # otherwise fragment groupby keys.
    raw['Date'] = pd.to_datetime(raw['Date'], dayfirst=True).dt.normalize()
    raw['Mapped']        = raw.apply(lambda r: map_utm_medium(r['UTM'], r['Medium']), axis=1)
    raw['Channel']       = raw['Mapped'].apply(lambda x: x[0])
    raw['Channel_Group'] = raw['Mapped'].apply(lambda x: x[1])
    raw = raw[raw['Channel_Group'] != 'Affiliates']

    agg = raw.groupby(['Date','Country','Channel','Channel_Group']).size().reset_index(name=count_col)
    agg[count_col] = agg[count_col].astype('Int64')

    other = 'FT' if count_col == 'QL' else 'QL'
    agg[other] = pd.array([pd.NA] * len(agg), dtype='Int64')

    agg['Campaign']      = agg['Channel']
    agg['Creative']      = None
    agg['Impressions']   = None
    agg['Clicks']        = None
    agg['CTR']           = None
    agg['Spend (AUD)']   = None
    agg['Date_Added']    = None
    agg['Date_Modified'] = None
    agg = agg.sort_values(['Date','Country','Channel']).reset_index(drop=True)
    return agg[AD_PERFORMANCE_COLS]


def parse_ql(ql_path):
    """Returns (df, error_or_None). FT column is <NA> so the MERGE leaves target.FT alone."""
    try:
        raw = _parse_sf_file(ql_path, _SF_REQUIRED, label="QL")
        if raw is None:
            return empty_df(), "QL header/data not found"
        return _build_sf_rows(raw, 'QL'), None
    except Exception as e:
        import traceback
        traceback.print_exc()
        return empty_df(), str(e)


def parse_ft(ft_path):
    """
    Returns (df, error_or_None). QL column is <NA> so the MERGE leaves target.QL alone.
    FT is bucketed by First Trade Date (when the funded trade actually happened),
    not by Created Date — Created Date is when the lead was first created and can
    be months earlier than the trade itself.
    """
    try:
        raw = _parse_sf_file(ft_path, _SF_REQUIRED, label="FT", date_col='First Trade Date')
        if raw is None:
            return empty_df(), "FT header/data not found (does the file include a 'First Trade Date' column?)"
        raw = raw[raw['Stage'].isin(['Active', 'Funded NT', 'Funded'])]
        if raw.empty:
            return empty_df(), "FT: no rows with Stage in [Active, Funded NT, Funded]"
        return _build_sf_rows(raw, 'FT'), None
    except Exception as e:
        import traceback
        traceback.print_exc()
        return empty_df(), str(e)


# ── MASTER PARSE FUNCTION ─────────────────────────────────────────────────────

def parse_all():
    frames          = []
    processed_files = []
    failed_channels = []

    parsers = {
        "Bing"             : ("bing",         parse_bing),
        "Meta"             : ("meta",         parse_meta),
        "Meta - Agency"    : ("meta_agency",  parse_meta_agency),
        "AdRoll"           : ("adroll",       parse_adroll),
        "BiliBili"         : ("bilibili",     parse_bilibili),
        "RedNote"          : ("rednote",      parse_rednote),
        "TradingView"      : ("tradingview",  parse_tradingview),
        "Apple Search Ads" : ("apple",        parse_apple),
        "TikTok"           : ("tiktok",       parse_tiktok),
        "Douyin"           : ("douyin",       parse_douyin),
        "Kuaishou"         : ("kuaishou",     parse_kuaishou),
        "TA Media"         : ("ta_media",     parse_ta_media),
        "WeChat"           : ("wechat",       parse_wechat),
        "YouTube (DV360)"  : ("dv360",        parse_dv360),
        "Affiliates"       : ("affiliates",   parse_affiliate),
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

    # QL and FT run independently — either can be re-processed alone.
    # The BQ MERGE uses IFNULL on QL/FT, so a file covering only one metric
    # leaves the other untouched in the target table.
    sf_frames = []
    for label, key, parser_fn in [
        ("QL (Salesforce)", "ql", parse_ql),
        ("FT (Salesforce)", "ft", parse_ft),
    ]:
        path = find_file(key)
        if not path:
            failed_channels.append((label, f"{key.upper()}_ file missing"))
            continue
        rows, err = parser_fn(path)
        if err or len(rows) == 0:
            reason = err if err else "Parser returned 0 rows"
            failed_channels.append((label, reason))
            continue
        print(f"             → {len(rows):,} {label.split()[0]} rows parsed")
        rows["Date"] = pd.to_datetime(rows["Date"])
        processed_files.append(path)
        sf_frames.append(rows)

    if sf_frames:
        ad_performance = pd.concat([ad_performance] + sf_frames, ignore_index=True)
        ad_performance = ad_performance.sort_values(["Date","Country","Channel"]).reset_index(drop=True)

    if processed_files:
        print("\n      Archiving processed input files...")
        for fp in processed_files:
            archive_file(fp)

    return ad_performance, failed_channels
