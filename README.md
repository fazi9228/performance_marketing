# Pepperstone APAC Performance Marketing Pipeline

Transforms raw ad channel files into a clean master sheet with two tabs:
- **Ad_Performance** — all channels, daily granularity
- **QL_FT** — qualified leads and funded traders by country + date

---

## Folder Structure

```
perf_marketing_pipeline/
│
├── run.py              ← Entry point — run this
├── parsers.py          ← One parser per data source
├── uploader.py         ← Google Sheets upload + Excel fallback
├── config.py           ← All settings (sheet ID, file patterns, mappings)
├── credentials.json    ← Google Service Account key (you add this)
├── README.md
│
├── input/              ← Drop raw files here before running
│   ├── Bing_ads_*.xlsx
│   ├── Meta_*.xlsx
│   ├── AdRoll_Pepperstone_*.xlsx
│   ├── FY26_APAC_BiliBili_*.xlsx
│   ├── FY26_APAC_Rednote_*.xlsx
│   └── _TABLE_*.xlsx   ← QL/FT registration table
│
└── output/             ← Local Excel backup written here
    └── Pepperstone_APAC_Performance_Master.xlsx
```

---

## Setup (One-Time)

### 1. Install dependencies
```bash
pip install pandas openpyxl gspread google-auth
```

### 2. Set up Google Service Account
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project → Enable **Google Sheets API** and **Google Drive API**
3. Go to **IAM & Admin → Service Accounts → Create Service Account**
4. Download the JSON key → save as `credentials.json` in this folder
5. Copy the service account email (looks like: `xxx@project.iam.gserviceaccount.com`)
6. Open your Google Sheet → **Share** → paste the service account email → give **Editor** access

### 3. Configure config.py
Open `config.py` and update:
```python
GOOGLE_SHEET_ID = "your_sheet_id_here"  # from the URL of your Google Sheet
```

---

## Running the Pipeline

### Each period (daily / weekly / monthly):
1. Drop new raw files into the `input/` folder
2. Run:
```bash
python run.py
```
3. Done ✅ — Google Sheet is updated, local Excel backup saved to `output/`

### What happens:
- Each source file is found by matching the filename pattern in `config.py`
- Data is parsed, cleaned, and standardised
- New rows are **appended** to existing data (no duplicates — deduped on Date + Country + Channel + Campaign)
- Both Google Sheets and a local Excel file are updated

---

## Source Files & Expected Format

| Source | File pattern | Key columns |
|--------|-------------|-------------|
| Bing | `Bing_ads*` | Campaign name, Date, Impressions, Clicks, CTR, Spend |
| Meta | `Meta*` | Campaign name, Country, Day, Impressions, CTR (all), Amount spent (AUD) |
| AdRoll | `AdRoll_Pepperstone*` | Day, Campaign, Impressions, Clicks, CTR, Spend |
| BiliBili | `BiliBili*` | Week, Campaign Name, Impression, Click, CTR, Cost (AUD) |
| RedNote | `Rednote*` | Week, Country, Main Account, Placement, Impression, Click, Cost (AUD) |
| QL/FT | `_TABLE_*` | Date, Country, Qualified lead user_id, Funded user_id |

---

## Channels in Output

| Channel label | Source |
|---|---|
| Bing - Brand | Bing — campaign name contains "Brand" |
| Bing - Category | Bing — default |
| Bing - PMax | Bing — campaign name contains "[Pmax]" |
| Meta | Meta — all campaigns |
| AdRoll - Retargeting | AdRoll — "Retargeting" in campaign name |
| AdRoll - Lookalike | AdRoll — "lookalike" in campaign name |
| AdRoll - Contextual | AdRoll — "Contextual" in campaign name |
| BiliBili | BiliBili — all campaigns (Country = CN) |
| RedNote | RedNote — aggregated by Account + Placement |

---

## Looker Studio Notes

### QL / FT — Avoid Double Counting
Since QL_FT is a **separate tab**, blend it into Looker Studio as a second data source:
- Join key: `Date` + `Country`
- Use `SUM(QL)` and `SUM(FT)` normally — no double counting risk

### Meta CTR
Meta exports CTR as a percentage (e.g. 8.5 = 8.5%). The pipeline converts this to decimal (0.085) to match other channels.

---

## Troubleshooting

| Issue | Fix |
|---|---|
| `No file found for 'bing'` | Check filename contains "Bing_ads" — update `FILE_PATTERNS` in config.py if needed |
| `credentials.json not found` | Download service account key from Google Cloud Console |
| `gspread not installed` | Run `pip install gspread google-auth` |
| Google Sheets upload fails | Check service account has Editor access to the sheet |
| BiliBili/RedNote rows seem off | These are weekly — each row = one week, not one day |