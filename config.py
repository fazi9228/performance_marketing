"""
config.py — All settings for the pipeline
"""

import os

# ── Google Sheets ─────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID  = "1_gqrbmEvmVYu3_Bu5IrJa2zKE9DWcZZQEmMvYnfuR4I"
CREDENTIALS_FILE = "credentials.json"

SHEET_AD_PERFORMANCE = "Ad_Performance"

# ── BigQuery ──────────────────────────────────────────────────────────────────
BQ_PROJECT_ID  = "gen-lang-client-0602500310"
BQ_DATASET     = "pepperstone_apac"
BQ_TABLE       = "ad_performance"
BQ_LOCATION    = "asia-southeast1"

# ── Local Paths ───────────────────────────────────────────────────────────────
INPUT_DIR   = "./input"
ARCHIVE_DIR = "./input/archive"
OUTPUT_DIR  = "./output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Pepperstone_APAC_Performance_Master.xlsx")

# ── File Name Patterns ────────────────────────────────────────────────────────
FILE_PATTERNS = {
    "bing"         : "Bing_",
    "meta"         : "Meta_",
    "meta_agency"  : "Meta_Agency_",
    "adroll"       : "Adroll_",
    "bilibili"     : "Bilibili_",
    "rednote"      : "Rednote_",
    "tradingview"  : "TradingView_",
    "ql"           : "QL_",
    "ft"           : "FT_",
    "apple"        : "Apple_",
    "tiktok"       : "Tiktok_",
    "douyin"       : "Douyin_",
    "affiliates"   : "Affiliates_",
}

# ── Country Mappings ──────────────────────────────────────────────────────────
APAC_COUNTRIES = ["VN", "TH", "SG", "MY", "CN", "HK", "TW", "IN", "ID", "PH", "MN"]

ADROLL_COUNTRY_MAP = {
    "Hong Kong"   : "HK",
    "Taiwan"      : "TW",
    "Thailand"    : "TH",
    "Vietnam"     : "VN",
    "Singapore"   : "SG",
    "Malaysia"    : "MY",
    "Indonesia"   : "ID",
    "Philippines" : "PH",
    "India"       : "IN",
    "Mongolia"    : "MN",
    "China"       : "CN",
}

APPLE_COUNTRY_MAP = {
    "Hong Kong"   : "HK",
    "Taiwan"      : "TW",
    "Thailand"    : "TH",
    "Vietnam"     : "VN",
    "Singapore"   : "SG",
    "Malaysia"    : "MY",
    "China"       : "CN",
    "Indonesia"   : "ID",
    "Philippines" : "PH",
    "India"       : "IN",
    "Mongolia"    : "MN",
}

# ── Channel Label Rules ───────────────────────────────────────────────────────
BING_CHANNEL_RULES = {
    "[Pmax]" : "Bing - PMax",
    "Brand"  : "Bing - Brand",
    "default": "Bing - Category",
}

ADROLL_CHANNEL_RULES = {
    "Retargeting" : "AdRoll - Retargeting",
    "lookalike"   : "AdRoll - Lookalike",
    "Contextual"  : "AdRoll - Contextual",
    "default"     : "AdRoll",
}

META_CHANNEL          = "Meta"
META_AGENCY_CHANNEL   = "Meta - Agency"
BILIBILI_CHANNEL      = "BiliBili"
REDNOTE_CHANNEL       = "RedNote"
BILIBILI_COUNTRY      = "CN"
TRADINGVIEW_CHANNEL   = "TradingView"
TRADINGVIEW_FX_RATE   = 1.58  # USD to AUD — update monthly

APPLE_CHANNEL       = "Apple Search Ads"   # spend already in AUD, no FX needed
TIKTOK_CHANNEL      = "TikTok"             # spend already in AUD, no FX needed
DOUYIN_CHANNEL      = "Douyin"             # Chinese TikTok — spend in AUD
DOUYIN_COUNTRY      = "CN"                 # Default fallback if Country column missing

# ── UTM → (Channel, Channel_Group) mapping ───────────────────────────────────
# For QL/FT rows from Salesforce. Numeric IDs → IB. Blank/- → Organic.
# Source: PM_report_master mapping sheet.
# NOTE: Exact-match dict is checked first; substring rules in parsers.py
# catch future UTM variants containing "bing", "google", "apple", "tiktok", etc.
UTM_TO_CHANNEL = {
    # ── Bing ──────────────────────────────────────────────────────────────
    "bing"                          : ("Bing",             "Bing"),
    "bing-search-category"          : ("Bing",             "Bing"),
    "bing-search-brand"             : ("Bing",             "Bing"),
    "bing-performance-max-category" : ("Bing",             "Bing"),
    "bing-performance-max-brand"    : ("Bing",             "Bing"),
    # ── Google ────────────────────────────────────────────────────────────
    "google"                        : ("Google",           "Google"),
    "google-search-brand"           : ("Google",           "Google"),
    "google-search-category"        : ("Google",           "Google"),
    "google-performance-max-brand"  : ("Google",           "Google"),
    "google-performance-max-category": ("Google",          "Google"),
    "google-video"                  : ("Google",           "Google"),
    "google-play"                   : ("Google",           "Google"),
    "youtube-video"                 : ("YouTube",           "YouTube"),
    # ── TradingView ───────────────────────────────────────────────────────
    "tradingview"                   : ("TradingView",      "TradingView"),
    "direct-deal-tradingview"       : ("TradingView",      "TradingView"),
    "direct-deal-tradingview-profile": ("TradingView",     "TradingView"),
    # ── CocCoc ────────────────────────────────────────────────────────────
    "coccoc"                        : ("CocCoc",           "CocCoc"),
    "coccoc-display"                : ("CocCoc",           "CocCoc"),
    "coccoc-logo"                   : ("CocCoc",           "CocCoc"),
    "coccoc-search-brand"           : ("CocCoc",           "CocCoc"),
    "coccoc-search-category"        : ("CocCoc",           "CocCoc"),
    # ── Meta ──────────────────────────────────────────────────────────────
    "instagram-organic-display"     : ("Meta",             "Meta"),
    # ── AdRoll ────────────────────────────────────────────────────────────
    "adroll-display"                : ("AdRoll",           "AdRoll"),
    # ── Apple ─────────────────────────────────────────────────────────────
    "apple search ads"              : ("Apple Search Ads", "Apple Search Ads"),
    # ── BiliBili ──────────────────────────────────────────────────────────
    "bili-video"                    : ("BiliBili",         "BiliBili"),
    # ── TikTok ────────────────────────────────────────────────────────────
    "tiktok"                        : ("TikTok",           "TikTok"),
    # ── ChatGPT ───────────────────────────────────────────────────────────
    "chatgpt.com"                   : ("ChatGPT",          "ChatGPT"),
    # ── Others ────────────────────────────────────────────────────────────
    "direct-deal-followme"          : ("Follow Me",        "Others"),
    "follow me"                     : ("Follow Me",        "Others"),
    "direct-deal-mt5"               : ("metaquotes",       "Others"),
    "direct-deal-myfxbook"          : ("myfxbook",         "Others"),
    "direct-deal-fxstreet"          : ("fxstreet",         "Others"),
    "direct-deal-forexfactory"      : ("forexfactory",     "Others"),
    "direct-deal-foodpanda"         : ("FoodPanda",        "Others"),
    "direct-deal-investopedia"      : ("Investopedia",     "Others"),
    "ctrader-eu"                    : ("ctrader",          "Others"),
    "mt5"                           : ("mt5 terminal app", "Others"),
    "mt4"                           : ("mt4",              "Others"),
    "perplexity"                    : ("perplexity",       "Others"),
    "fx110"                         : ("FX110",            "Others"),
    "zalo"                          : ("zalo",             "Others"),
    "line"                          : ("Line",             "Others"),
    "email"                         : ("email",            "Others"),
    "sendgrid"                      : ("email",            "Others"),
    "sfmc"                          : ("email",            "Others"),
    "transactional"                 : ("email",            "Others"),
}

# ── Channel → Channel_Group mapping for ad channels ──────────────────────────
AD_CHANNEL_GROUP = {
    "Bing - Brand"         : "Bing",
    "Bing - Category"      : "Bing",
    "Bing - PMax"          : "Bing",
    "Bing"                 : "Bing",
    "Meta"                 : "Meta",
    "Meta - Agency"        : "Meta",
    "AdRoll - Retargeting" : "AdRoll",
    "AdRoll - Lookalike"   : "AdRoll",
    "AdRoll - Contextual"  : "AdRoll",
    "AdRoll"               : "AdRoll",
    "BiliBili"             : "BiliBili",
    "RedNote"              : "RedNote",
    "TradingView"          : "TradingView",
    "Apple Search Ads"     : "Apple Search Ads",
    "TikTok"               : "TikTok",
    "Douyin"               : "Douyin",
    "Affiliates"           : "Affiliates",
    "Google"               : "Google",
    "CocCoc"               : "CocCoc",
    "ChatGPT"              : "ChatGPT",
    "YouTube"              : "YouTube",
}

# ── Master Sheet Columns ──────────────────────────────────────────────────────
AD_PERFORMANCE_COLS = [
    "Date", "Country", "Channel", "Campaign", "Creative",
    "Impressions", "Clicks", "CTR", "Spend (AUD)",
    "QL", "FT", "Channel_Group", "Date_Added", "Date_Modified"
]

# ── Deduplication Keys ────────────────────────────────────────────────────────
DEDUP_KEYS_AD = ["Date", "Country", "Channel", "Campaign", "Creative"]
