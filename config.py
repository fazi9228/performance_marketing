"""
config.py — All settings for the pipeline
"""

import os

# ── Google Sheets ─────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID  = "1_gqrbmEvmVYu3_Bu5IrJa2zKE9DWcZZQEmMvYnfuR4I"
CREDENTIALS_FILE = "credentials.json"

SHEET_AD_PERFORMANCE = "Ad_Performance"

# ── BigQuery ──────────────────────────────────────────────────────────────────
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "your-project-id")
BQ_DATASET    = os.getenv("BQ_DATASET",    "your_dataset")
BQ_TABLE      = os.getenv("BQ_TABLE",      "ad_performance")
BQ_LOCATION   = os.getenv("BQ_LOCATION",   "asia-southeast1")

# ── Local Paths ───────────────────────────────────────────────────────────────
INPUT_DIR   = "./input"
ARCHIVE_DIR = os.path.join(INPUT_DIR, "archive")
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
    "apple"        : "Apple_",
    "tiktok"       : "Tiktok_",
    "douyin"       : "Douyin_",
    "kuaishou"     : "Kuaishou_",
    "ta_media"     : "TA_Media_",
    "affiliates"   : "Affiliates_",
    "ql"           : "QL_",
    "ft"           : "FT_",
}

# ── Country Mappings ──────────────────────────────────────────────────────────
APAC_COUNTRIES = ["VN", "TH", "SG", "MY", "CN", "HK", "TW", "ID", "PH", "IN", "MN"]

ADROLL_COUNTRY_MAP = {
    "Hong Kong"    : "HK",
    "Taiwan"       : "TW",
    "Thailand"     : "TH",
    "Vietnam"      : "VN",
    "Singapore"    : "SG",
    "Indonesia"    : "ID",
    "Philippines"  : "PH",
}

APPLE_COUNTRY_MAP = {
    "Hong Kong"    : "HK",
    "Taiwan"       : "TW",
    "Thailand"     : "TH",
    "Vietnam"      : "VN",
    "Singapore"    : "SG",
    "Malaysia"     : "MY",
    "Indonesia"    : "ID",
    "Philippines"  : "PH",
    "India"        : "IN",
    "China mainland": "CN",
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
APPLE_CHANNEL         = "Apple Search Ads"
TIKTOK_CHANNEL        = "TikTok"
DOUYIN_CHANNEL        = "Douyin"
DOUYIN_COUNTRY        = "CN"
KUAISHOU_CHANNEL      = "Kuaishou"
KUAISHOU_COUNTRY      = "CN"
TA_MEDIA_CHANNEL      = "TA Media"

# ── UTM → (Channel, Channel_Group) mapping ───────────────────────────────────
# For QL/FT rows from Salesforce. Numeric IDs → IB. Blank/- → Organic.
UTM_TO_CHANNEL = {
    "direct-deal-followme"           : ("follow me",        "Others"),
    "direct-deal-mt5"                : ("metaquotes",       "Others"),
    "direct-deal-myfxbook"           : ("myfxbook",         "Others"),
    "direct-deal-fxstreet"           : ("fxstreet",         "Others"),
    "direct-deal-tradingview"        : ("TradingView",      "TradingView"),
    "direct-deal-forexfactory"       : ("forexfactory",     "Others"),
    "bing-search-category"           : ("Bing",             "Bing"),
    "bing-search-brand"              : ("Bing",             "Bing"),
    "bing-performance-max-category"  : ("Bing",             "Bing"),
    "bing"                           : ("Bing",             "Bing"),
    "google"                         : ("Google",           "Others"),
    "google-search-brand"            : ("Google",           "Google"),
    "google-search-category"         : ("Google",           "Google"),
    "google-performance-max-brand"   : ("Google",           "Google"),
    "google-performance-max-category": ("Google",           "Google"),
    "google-video"                   : ("Google",           "Google"),
    "google-play"                    : ("Google",           "Others"),
    "tradingview"                    : ("TradingView",      "TradingView"),
    "ctrader-eu"                     : ("ctrader",          "Others"),
    "mt5"                            : ("mt5 terminal app", "Others"),
    "mt4"                            : ("mt4",              "Others"),
    "chatgpt.com"                    : ("chatgpt",          "Others"),
    "perplexity"                     : ("perplexity",       "Others"),
    "youtube-video"                  : ("youtube",          "Others"),
    "fx110"                          : ("fx110",            "Others"),
    "zalo"                           : ("zalo",             "Others"),
    "line"                           : ("line",             "Others"),
    "email"                          : ("email",            "Others"),
    "sendgrid"                       : ("email",            "Others"),
    "sfmc"                           : ("email",            "Others"),
    "transactional"                  : ("email",            "Others"),
    "coccoc-display"                 : ("coccoc",           "Others"),
    "instagram-organic-display"      : ("Meta",             "Meta"),
    "adroll-display"                 : ("AdRoll",           "AdRoll"),
}

# ── Channel → Channel_Group mapping for ad channels ──────────────────────────
AD_CHANNEL_GROUP = {
    "Bing - Brand"         : "Bing",
    "Bing - Category"      : "Bing",
    "Bing - PMax"          : "Bing",
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
    "Kuaishou"             : "Kuaishou",
    "TA Media"             : "TA Media",
    "Affiliates"           : "Affiliates",
}

# ── Master Sheet Columns ──────────────────────────────────────────────────────
AD_PERFORMANCE_COLS = [
    "Date", "Country", "Channel", "Campaign", "Creative",
    "Impressions", "Clicks", "CTR", "Spend (AUD)",
    "QL", "FT", "Channel_Group"
]

# ── Deduplication Keys ────────────────────────────────────────────────────────
DEDUP_KEYS_AD = ["Date", "Country", "Channel", "Campaign", "Creative"]
