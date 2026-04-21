"""
config.py — All settings for the pipeline
"""

import os

# ── Google Sheets ─────────────────────────────────────────────────────────────
GOOGLE_SHEET_ID  = "1_gqrbmEvmVYu3_Bu5IrJa2zKE9DWcZZQEmMvYnfuR4I"
CREDENTIALS_FILE = "credentials.json"

SHEET_AD_PERFORMANCE = "Ad_Performance"

# ── Local Paths ───────────────────────────────────────────────────────────────
INPUT_DIR   = "./input"
OUTPUT_DIR  = "./output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Pepperstone_APAC_Performance_Master.xlsx")

# ── File Name Patterns ────────────────────────────────────────────────────────
FILE_PATTERNS = {
    "bing"        : "Bing_",
    "meta"        : "Meta_",
    "adroll"      : "Adroll_",
    "bilibili"    : "Bilibili_",
    "rednote"     : "Rednote_",
    "tradingview" : "TradingView_",
    "ql"          : "QL_",
    "ft"          : "FT_",
}

# ── Country Mappings ──────────────────────────────────────────────────────────
APAC_COUNTRIES = ["VN", "TH", "SG", "MY", "CN", "HK", "TW"]

ADROLL_COUNTRY_MAP = {
    "Hong Kong" : "HK",
    "Taiwan"    : "TW",
    "Thailand"  : "TH",
    "Vietnam"   : "VN",
    "Singapore" : "SG",
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

META_CHANNEL        = "Meta"
BILIBILI_CHANNEL    = "BiliBili"
REDNOTE_CHANNEL     = "RedNote"
BILIBILI_COUNTRY    = "CN"
TRADINGVIEW_CHANNEL = "TradingView"
TRADINGVIEW_FX_RATE = 1.58  # USD to AUD — update monthly

# ── UTM → (Channel, Channel_Group) mapping ───────────────────────────────────
# For QL/FT rows from Salesforce. Numeric IDs → IB. Blank/- → Organic.
UTM_TO_CHANNEL = {
    "direct-deal-followme"         : ("follow me",        "Others"),
    "direct-deal-mt5"              : ("metaquotes",       "Others"),
    "direct-deal-myfxbook"         : ("myfxbook",         "Others"),
    "direct-deal-fxstreet"         : ("fxstreet",         "Others"),
    "direct-deal-tradingview"      : ("TradingView",     "TradingView"),
    "direct-deal-forexfactory"     : ("forexfactory",     "Others"),
    "bing-search-category"         : ("Bing",             "Bing"),
    "bing-search-brand"            : ("Bing",             "Bing"),
    "bing-performance-max-category": ("Bing",             "Bing"),
    "bing"                         : ("Bing",             "Bing"),
    "google"                       : ("Google",           "Others"),
    "google-search-brand"          : ("Google",           "Google"),
    "google-search-category"       : ("Google",           "Google"),
    "google-performance-max-brand" : ("Google",           "Google"),
    "google-performance-max-category": ("Google",         "Google"),
    "google-video"                 : ("Google",           "Google"),
    "google-play"                  : ("Google",           "Others"),
    "tradingview"                  : ("TradingView",     "TradingView"),
    "ctrader-eu"                   : ("ctrader",          "Others"),
    "mt5"                          : ("mt5 terminal app", "Others"),
    "mt4"                          : ("mt4",              "Others"),
    "chatgpt.com"                  : ("chatgpt",          "Others"),
    "perplexity"                   : ("perplexity",       "Others"),
    "youtube-video"                : ("youtube",          "Others"),
    "fx110"                        : ("fx110",            "Others"),
    "zalo"                         : ("zalo",             "Others"),
    "line"                         : ("line",             "Others"),
    "email"                        : ("email",            "Others"),
    "sendgrid"                     : ("email",            "Others"),
    "sfmc"                         : ("email",            "Others"),
    "transactional"                : ("email",            "Others"),
    "coccoc-display"               : ("coccoc",           "Others"),
    "instagram-organic-display"    : ("Meta",             "Meta"),
    "adroll-display"               : ("AdRoll",           "AdRoll"),
}

# ── Channel → Channel_Group mapping for ad channels ──────────────────────────
AD_CHANNEL_GROUP = {
    "Bing - Brand"         : "Bing",
    "Bing - Category"      : "Bing",
    "Bing - PMax"          : "Bing",
    "Meta"                 : "Meta",
    "AdRoll - Retargeting" : "AdRoll",
    "AdRoll - Lookalike"   : "AdRoll",
    "AdRoll - Contextual"  : "AdRoll",
    "AdRoll"               : "AdRoll",
    "BiliBili"             : "BiliBili",
    "RedNote"              : "RedNote",
    "TradingView"          : "TradingView",
}

# ── Master Sheet Columns ──────────────────────────────────────────────────────
AD_PERFORMANCE_COLS = [
    "Date", "Country", "Channel", "Campaign", "Creative",
    "Impressions", "Clicks", "CTR", "Spend (AUD)",
    "QL", "FT", "Channel_Group"
]

# ── Deduplication Keys ────────────────────────────────────────────────────────
DEDUP_KEYS_AD = ["Date", "Country", "Channel", "Campaign", "Creative"]
