"""
parsers.py — One parser per data source
Single output: Ad_Performance with QL, FT, Channel_Group columns
"""

import os, re, glob
import pandas as pd
from config import (
    INPUT_DIR, FILE_PATTERNS,
    BING_CHANNEL_RULES, ADROLL_CHANNEL_RULES, ADROLL_COUNTRY_MAP,
    META_CHANNEL, BILIBILI_CHANNEL, REDNOTE_CHANNEL, BILIBILI_COUNTRY,
    TRADINGVIEW_CHANNEL, TRADINGVIEW_FX_RATE,
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
    """Map raw UTM string → (channel_name, channel_group)"""
    u = str(utm).strip()
    ul = u.lower()
    # Blank / dash → Organic
    if not u or ul in ['', '-', 'nan', 'none', 'unknown', '(not set)']:
        return ("Organic", "Organic")
    # Numeric IDs → IB (medium-based override handled in map_utm_medium)
    if ul.replace('-','').replace('_','').isdigit():
        return ("IB", "IB")
    # affiliate-* source → Affiliate
    if ul.startswith('affiliate-'):
        return ("Affiliate", "Affiliate")
    # Facebook / Instagram → Meta (before dict lookup to catch all variants)
    if ul in ('fb', 'ig') or ul.startswith('facebook') or ul.startswith('instagram'):
        return ("Meta", "Meta")
    # Known UTM mappings
    if ul in UTM_TO_CHANNEL:
        return UTM_TO_CHANNEL[ul]
    # Unknown UTM — use raw value as channel name, group as Others
    return (u, "Others")

def map_utm_medium(utm, medium):
    """
    Channel assignment using both UTM Medium and UTM Source.
    Medium takes priority:
      - 'ib'         → IB
      - 'affiliates' → Affiliate
    Anything else falls through to map_utm(source).
    """
    m = str(medium).strip().lower()
    if m == 'ib':
        return ("IB", "IB")
    if m == 'affiliates':
        return ("Affiliate", "Affiliate")
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
            print("      ❌ Bing: Could not find header row.")
            return empty_df()
        df = xl.parse(xl.sheet_names[0], skiprows=header_row)
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
        df["Date"] = pd.to_datetime(df["Date"]).dt.date
        df["QL"] = None; df["FT"] = None
        df["Creative"] = None
        out = df[["Date","Country","Channel","Campaign","Creative","Impressions","Clicks","CTR","Spend","QL","FT","Channel_Group"]].copy()
        out.columns = AD_PERFORMANCE_COLS
        return out
    except Exception as e:
        print(f"      ❌ Bing parse error: {e}")
        return empty_df()


# ── META ──────────────────────────────────────────────────────────────────────

def parse_meta(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        for s in xl.sheet_names:
            if "raw" in s.lower() or "data" in s.lower():
                sheet = s; break
        df = xl.parse(sheet)
        # Keep only valid APAC countries (drops MO, unknown, MN, AU, etc.)
        if "Country" in df.columns:
            df = df[df["Country"].isin(APAC_COUNTRIES)]
        df["Date"]          = pd.to_datetime(df["Day"]).dt.date
        df["Channel"]       = META_CHANNEL
        df["Channel_Group"] = "Meta"
        df["Clicks"]        = pd.to_numeric(df.get("Clicks (all)"), errors="coerce")
        df["QL"] = None; df["FT"] = None
        df["Creative"]      = None
        if "Ad set name" in df.columns:
            df["Campaign name"] = df["Campaign name"].astype(str) + " | " + df["Ad set name"].astype(str)
        rename = {
            "Campaign name"      : "Campaign",
            "CTR (all)"          : "CTR",
            "Amount spent (AUD)" : "Spend (AUD)",
        }
        df = df.rename(columns=rename)
        # CTR: use existing column if present (divide by 100 since it's a %), else compute from Impressions/Clicks
        if "CTR" in df.columns:
            df["CTR"] = pd.to_numeric(df["CTR"], errors="coerce") / 100
        else:
            imp = pd.to_numeric(df.get("Impressions"), errors="coerce")
            df["CTR"] = (df["Clicks"] / imp).where(imp > 0, other=None)
        return std_cols(df)
    except Exception as e:
        print(f"      ❌ Meta parse error: {e}")
        return empty_df()


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
            print("      ❌ AdRoll: Could not find header row.")
            return empty_df()
        # Use actual header names — column order has varied across exports
        df = xl.parse("Daily", skiprows=header_row)
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
        df["QL"] = None; df["FT"] = None
        df["Creative"] = None
        df = df.rename(columns={spend_col: "Spend (AUD)"})
        out = df[["Date","Country","Channel","Campaign","Creative","Impressions","Clicks","CTR","Spend (AUD)","QL","FT","Channel_Group"]].copy()
        out.columns = AD_PERFORMANCE_COLS
        return out
    except Exception as e:
        print(f"      ❌ AdRoll parse error: {e}")
        return empty_df()


# ── BILIBILI ──────────────────────────────────────────────────────────────────

def parse_bilibili(filepath):
    try:
        xl = pd.ExcelFile(filepath)
        sheet = xl.sheet_names[0]
        for s in xl.sheet_names:
            if s.lower() not in ["daily"]:
                sheet = s; break
        df = xl.parse(sheet)
        df["Country"]       = BILIBILI_COUNTRY
        df["Channel"]       = BILIBILI_CHANNEL
        df["Channel_Group"] = "BiliBili"
        df["Date"]          = pd.to_datetime(df["Date"]).dt.date
        df["QL"] = None; df["FT"] = None
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
        return std_cols(df)
    except Exception as e:
        print(f"      ❌ BiliBili parse error: {e}")
        return empty_df()


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
        df["Channel"]       = REDNOTE_CHANNEL
        df["Channel_Group"] = "RedNote"
        df["Campaign"]      = df["Main Account"].astype(str) + " - " + df["Placement"].astype(str)
        df["Date"]          = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].notna()].copy()
        df["Date"]          = df["Date"].dt.date
        df["QL"] = None; df["FT"] = None
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
        return std_cols(df)
    except Exception as e:
        print(f"      ❌ RedNote parse error: {e}")
        return empty_df()


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
            df = df[df["Date"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}")]
            df["Date"] = pd.to_datetime(df["Date"]).dt.date

            # Convert USD spend to AUD
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

        if not frames:
            print("      ⚠️  TradingView: no valid country tabs found.")
            return empty_df()
        return pd.concat(frames, ignore_index=True)

    except Exception as e:
        print(f"      ❌ TradingView parse error: {e}")
        return empty_df()


# ── QL / FT (Salesforce) ──────────────────────────────────────────────────────

def _parse_sf_file(filepath, required_cols, label):
    """
    Parse a Salesforce Excel export (QL or FT).
    Scans for the header row containing all required_cols (case-insensitive,
    whitespace-stripped), then resolves exact column positions dynamically.
    Returns a DataFrame with columns [Country, Date, UTM, *extra_cols] or None on failure.
    """
    xl  = pd.ExcelFile(filepath)
    raw = xl.parse(xl.sheet_names[0], header=None)

    # Find header row
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
        print(f"         First 5 non-empty rows:")
        shown = 0
        for i, row in raw.iterrows():
            vals = [str(v).strip() for v in row if str(v).strip() not in ('', 'nan')]
            if vals:
                print(f"           row {i}: {vals}")
                shown += 1
                if shown >= 5: break
        return None

    # Extract data rows using resolved positions
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
        print(f"         Column positions used → Country:{c_country} Date:{c_date} UTM:{c_utm} Stage:{c_stage}")
        return None

    return pd.DataFrame(records)


def parse_ql_ft(ql_path, ft_path):
    try:
        REQUIRED = ['Billing Country', 'Created Date', 'Google UTM Source', 'Stage']

        # ── QL ────────────────────────────────────────────────────────────────
        ql_raw = _parse_sf_file(ql_path, REQUIRED, label="QL")
        if ql_raw is None:
            return empty_df()

        # QL: every row is one lead — count all stages
        ql_raw['Date'] = pd.to_datetime(ql_raw['Date'], dayfirst=True)
        ql_raw['Mapped']       = ql_raw.apply(lambda r: map_utm_medium(r['UTM'], r['Medium']), axis=1)
        ql_raw['Channel']      = ql_raw['Mapped'].apply(lambda x: x[0])
        ql_raw['Channel_Group']= ql_raw['Mapped'].apply(lambda x: x[1])
        ql_agg = ql_raw.groupby(['Date','Country','Channel','Channel_Group']).size().reset_index(name='QL')

        # ── FT ────────────────────────────────────────────────────────────────
        ft_raw = _parse_sf_file(ft_path, REQUIRED, label="FT")
        if ft_raw is None:
            return empty_df()

        # FT: only Active / Funded / Funded NT rows count
        ft_raw = ft_raw[ft_raw['Stage'].isin(['Active', 'Funded NT', 'Funded'])]
        if ft_raw.empty:
            print("      ❌ FT: No rows with Stage in [Active, Funded NT, Funded].")
            return empty_df()

        ft_raw['Date'] = pd.to_datetime(ft_raw['Date'], dayfirst=True)
        ft_raw['Mapped']       = ft_raw.apply(lambda r: map_utm_medium(r['UTM'], r['Medium']), axis=1)
        ft_raw['Channel']      = ft_raw['Mapped'].apply(lambda x: x[0])
        ft_raw['Channel_Group']= ft_raw['Mapped'].apply(lambda x: x[1])
        ft_agg = ft_raw.groupby(['Date','Country','Channel','Channel_Group']).size().reset_index(name='FT')

        # ── Merge ─────────────────────────────────────────────────────────────
        merged = pd.merge(ql_agg, ft_agg, on=['Date','Country','Channel','Channel_Group'], how='outer').fillna(0)
        merged[['QL','FT']] = merged[['QL','FT']].astype(int)

        # Build as Ad_Performance rows (no impressions/clicks/spend)
        merged['Campaign']     = merged['Channel']
        merged['Creative']     = None
        merged['Impressions']  = None
        merged['Clicks']       = None
        merged['CTR']          = None
        merged['Spend (AUD)']  = None
        merged = merged.sort_values(['Date','Country','Channel']).reset_index(drop=True)
        return merged[AD_PERFORMANCE_COLS]

    except Exception as e:
        import traceback
        print(f"      ❌ QL/FT parse error: {e}")
        traceback.print_exc()
        return empty_df()


# ── MASTER PARSE FUNCTION ─────────────────────────────────────────────────────

def parse_all():
    frames = []
    parsers = {
        "bing"        : parse_bing,
        "meta"        : parse_meta,
        "adroll"      : parse_adroll,
        "bilibili"    : parse_bilibili,
        "rednote"     : parse_rednote,
        "tradingview" : parse_tradingview,
    }
    for key, parser_fn in parsers.items():
        filepath = find_file(key)
        if filepath:
            df = parser_fn(filepath)
            if len(df) > 0:
                frames.append(df)
                print(f"             → {len(df):,} rows parsed")

    if frames:
        ad_performance = pd.concat(frames, ignore_index=True)
        ad_performance["Date"] = pd.to_datetime(ad_performance["Date"])
        ad_performance = ad_performance.sort_values(
            ["Date","Country","Channel","Campaign"]).reset_index(drop=True)
    else:
        ad_performance = empty_df()

    # QL + FT rows
    ql_path = find_file("ql")
    ft_path = find_file("ft")
    if ql_path and ft_path:
        ql_ft_rows = parse_ql_ft(ql_path, ft_path)
        print(f"             → {len(ql_ft_rows):,} QL/FT rows parsed")
        ql_ft_rows["Date"] = pd.to_datetime(ql_ft_rows["Date"])
        combined = pd.concat([ad_performance, ql_ft_rows], ignore_index=True)
        combined = combined.sort_values(["Date","Country","Channel"]).reset_index(drop=True)
    else:
        print("      ⚠️  Need both QL_ and FT_ files in input/")
        combined = ad_performance

    return combined