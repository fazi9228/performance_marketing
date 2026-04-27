# Low-Level Design (LLD)
## Pepperstone APAC Performance Marketing Pipeline

**Version:** 1.0
**Last Updated:** 2026-04-27
**Scope:** Module-by-module design, function contracts, data shapes, and edge-case handling.

---

## 1. Repository Layout

```
perf_marketing_pipeline/
├── run.py              ← entry point
├── parsers.py          ← per-channel parsers + master parse_all()
├── bq_uploader.py      ← BigQuery MERGE upsert
├── config.py           ← all settings, mappings, schema definitions
├── credentials.json    ← service-account key (gitignored)
├── README.md
├── input/              ← drop raw files here
│   └── archive/        ← processed files moved here automatically
├── output/             ← legacy local Excel artefacts (no longer written by run.py)
└── docs/
    ├── HLD.md
    ├── LLD.md
    └── RELEASE_NOTES.md
```

---

## 2. `run.py` — orchestration

```python
def main():
    combined, failed_channels = parse_all()           # parsers.py
    bq_success                = upload_to_bigquery(   # bq_uploader.py
                                    combined)
    # prints summary including failed_channels
```

- Two-step contract: `parse_all` returns one combined DataFrame; `upload_to_bigquery` consumes it.
- No retry logic — re-running is safe (idempotent), so failed runs are simply re-run by the analyst.
- Splits combined frame into ad-only vs QL/FT rows for the on-screen counts (ad rows = `QL.isna() & FT.isna()`).

---

## 3. `config.py` — settings reference

### Sections
1. **Google Sheets** — `GOOGLE_SHEET_ID`, `CREDENTIALS_FILE`, `SHEET_AD_PERFORMANCE`. Sheets is no longer the write target but the constants remain for legacy / Connected Sheets reference.
2. **BigQuery** — `BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_TABLE`, `BQ_LOCATION`. **Critical**: any change here changes the destination table.
3. **Local paths** — `INPUT_DIR`, `ARCHIVE_DIR`, `OUTPUT_DIR`, `OUTPUT_FILE`.
4. **File patterns** — `FILE_PATTERNS` keyed by parser name. The matcher in `find_file()` excludes more-specific patterns (e.g. searching `Meta_` excludes `Meta_Agency_`).
5. **Country mappings** — `APAC_COUNTRIES`, `ADROLL_COUNTRY_MAP`, `APPLE_COUNTRY_MAP`.
6. **Channel rules** — `BING_CHANNEL_RULES`, `ADROLL_CHANNEL_RULES`, plus single-string channel constants per source.
7. **UTM → Channel mapping** — `UTM_TO_CHANNEL` (exact match) and substring fallback rules in `parsers.map_utm()`.
8. **Channel → Channel_Group** — `AD_CHANNEL_GROUP`.
9. **Schema** — `AD_PERFORMANCE_COLS` (the canonical ordered column list parsers must produce). The full BQ schema (with `Date_Added`, `Date_Modified`) lives in `bq_uploader.BQ_SCHEMA`.

> ⚠️ **Active drift**: `config.py` was simplified in commit `64c0ca3` and currently does not export several constants that `parsers.py` and `bq_uploader.py` import (`ARCHIVE_DIR`, `META_AGENCY_CHANNEL`, `APPLE_CHANNEL`, `APPLE_COUNTRY_MAP`, `TIKTOK_CHANNEL`, `DOUYIN_CHANNEL`, `DOUYIN_COUNTRY`, `KUAISHOU_CHANNEL`, `KUAISHOU_COUNTRY`, `TA_MEDIA_CHANNEL`, `BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_TABLE`, `BQ_LOCATION`). The pipeline will currently fail at import time. See `RELEASE_NOTES.md`.

---

## 4. `parsers.py` — per-channel parsing

### 4.1 Common contract

Every channel parser returns:

```python
(df: pd.DataFrame, error: Optional[str])
```

- On success: `df` has exactly the columns in `AD_PERFORMANCE_COLS` and `error is None`.
- On failure: `df` is `empty_df()` (an empty frame with the right columns) and `error` is a string.
- Parsers do **not** raise — exceptions are caught, printed, and returned as `error`.

### 4.2 Helpers

| Helper | Purpose |
|---|---|
| `find_file(pattern_key)` | Glob `input/*<pattern>*`, exclude more-specific patterns, sort by trailing DDMMYY date in filename, return newest. |
| `archive_file(filepath)` | Move file to `input/archive/`. Handles name collisions (timestamp suffix) and Windows file-locks (warn-and-continue). |
| `empty_df()` | Empty DataFrame with `AD_PERFORMANCE_COLS`. |
| `std_cols(df)` | Force the output to have exactly `AD_PERFORMANCE_COLS` (adds NULLs for missing). |
| `get_channel_group(channel)` | Lookup in `AD_CHANNEL_GROUP`, default `"Others"`. |
| `map_utm(utm)` | Salesforce UTM → `(Channel, Channel_Group)`. Order: blank/`-`/null → Organic; pure-numeric → IB; `affiliate-` prefix → Affiliates; `fb`/`ig`/`facebook*`/`instagram*` → Meta; exact-match dict; substring rules; fallback `(utm, "Others")`. |
| `map_utm_medium(utm, medium)` | Same as `map_utm` but `medium == 'ib'` short-circuits to IB and `'affiliates'` to Affiliates. |

### 4.3 Per-channel design notes

| Parser | Source structure | Country derivation | Channel derivation | Notes |
|---|---|---|---|---|
| `parse_bing` | Excel; header row autodetected by "Campaign name" cell | Regex `^(HK|TH|TW|VN|MY|SG|CN|IN|ID|PH|MN)` on Campaign | `[Pmax]` → PMax; `Brand` → Brand; else Category | Filters out `©` rows and "Total". |
| `parse_meta` | Excel; sheet picked by "raw"/"data" in name | Country column filtered to APAC | Constant `Meta` | Concatenates `Campaign name | Ad set name`; CTR converted from percent to decimal. |
| `parse_meta_agency` | Excel; sheet picked by "daily"/"raw"/"data" | Country column filtered to APAC | Constant `Meta - Agency` | Same shape as Meta but separate channel label. |
| `parse_adroll` | Excel; "Daily" sheet, header autodetected | `APAC_<CountryWord>_` regex on Campaign mapped via `ADROLL_COUNTRY_MAP` | Substring of campaign matches `Retargeting`/`lookalike`/`Contextual` else default. |
| `parse_bilibili` | Excel; non-Daily sheet | Country column if present, else `CN` | Constant `BiliBili` | Weekly granularity — Date is the week start. Campaign enriched with Targeting + Creative Type. |
| `parse_rednote` | Excel; "Daily" sheet | Country column upper-cased | Constant `RedNote` | Campaign = `Account - Placement | Targeting | Creative` with `#N` suffix to disambiguate same-day duplicates. |
| `parse_tradingview` | Excel; one sheet per APAC country | Sheet name | Constant `TradingView` | USD spend → AUD via `TRADINGVIEW_FX_RATE`. |
| `parse_apple` | xlsx or csv; 6 metadata rows skipped | `Country or Region` mapped via `APPLE_COUNTRY_MAP` | Constant `Apple Search Ads` | CTR auto-detects percent (`14.29%`) vs decimal (`0.142857`). |
| `parse_tiktok` | Excel; either single "Daily" sheet OR per-country tabs | Country column or tab name | Constant `TikTok` | Two layouts supported; falls back to per-country tabs when Daily isn't present. |
| `parse_douyin` | Excel; "Daily" or first sheet | Country column else `CN` | Constant `Douyin` | Impressions = `Video Play`, Clicks = `Profile Views`. |
| `parse_kuaishou` | Excel; "Daily" or first sheet | Country column else `CN` | Constant `Kuaishou` | RedNote-style campaign assembly with `#N` deduplication. |
| `parse_ta_media` | Excel; "Daily" or first sheet | Country column **required** (raises if missing) | Constant `TA Media` | Date may be `YYYY.M.DD`. |
| `parse_affiliate` | Excel; flat table | Country column; excludes AU/NZ | Constant `Affiliates` | Spend = Commission (USD/AUD agnostic — strips `$` and `,`). Produces only QL/FT counts plus Spend (AUD); ad metrics NULL. |

### 4.4 `parse_ql_ft(ql_path, ft_path)` — Salesforce

1. Both files share the required columns: `Billing Country`, `Created Date`, `Google UTM Source`, `Google UTM Medium`, `Stage`.
2. `_parse_sf_file` autodetects the header row by looking for those column names.
3. APAC-only filter applied to `Billing Country`.
4. UTM → `(Channel, Channel_Group)` via `map_utm_medium`.
5. **FT filter**: only `Stage in {"Active", "Funded NT", "Funded"}` counts.
6. **Affiliates excluded** from QL/FT counts (avoids double counting; affiliate-driven QL/FT come from `parse_affiliate`).
7. QL and FT aggregated separately by `Date+Country+Channel+Channel_Group`, then outer-joined and zero-filled.
8. Ad-metric columns left NULL → distinguishes these rows in MERGE.

### 4.5 `parse_all()` — master driver

Returns `(combined_df, failed_channels: List[Tuple[label, reason]])`.

1. Iterates the parser registry; for each, finds file → parses → either appends to frames or appends to `failed_channels`.
2. Concatenates ad frames; sorts by Date/Country/Channel/Campaign.
3. Adds QL/FT rows on top.
4. Archives every successfully processed file.

---

## 5. `bq_uploader.py` — BigQuery upsert

### 5.1 Schema

`BQ_SCHEMA` is the authoritative wire schema (14 fields, see HLD §6). The table is monthly-partitioned on `Date`.

### 5.2 Function contracts

| Function | Contract |
|---|---|
| `_get_client()` | Returns an authenticated `bigquery.Client` using `CREDENTIALS_FILE`. Scope: `bigquery`. Project + location from config. |
| `_ensure_table(client) -> bool` | True if table existed, False if it was created. Creates with BQ_SCHEMA + monthly Date partitioning. |
| `_prepare_df(df) -> pd.DataFrame` | Coerces types: `Date` to `date`; rename `Spend (AUD)`→`Spend_AUD`; numeric columns to float; QL/FT to nullable `Int64`; strings → None on NaN; injects `Date_Added`/`Date_Modified` = today; reindexes to schema columns. |
| `upload_to_bigquery(combined) -> bool` | End-to-end upsert (see §5.3). Returns False on any exception. |
| `load_initial_data(filepath)` | One-time helper to backfill from a clean Excel file via `WRITE_TRUNCATE`. Use only for migrations. |

### 5.3 Upload sequence

```
1. Skip if credentials.json missing or input is empty.
2. _ensure_table(client)
3. SELECT COUNT(*) FROM target  -- for end-of-run delta reporting
4. _prepare_df(combined)
5. Aggregate within each row type to remove same-key duplicates in the input batch:
   - QL/FT keys: Date + Country + Channel + Channel_Group  (sum QL, sum FT)
   - Ad keys:    Date + Country + Channel + Campaign + Creative  (sum Impressions/Clicks/Spend)
6. WRITE_TRUNCATE staging table _staging_<table>
7. MERGE into target using row-type-aware ON clause (CASE: NULL Impressions/Spend ⇒ QL/FT key; else Ad key)
8. SELECT COUNT(*) FROM target  -- compute updated vs inserted
9. DROP staging
```

### 5.4 The MERGE — why two key shapes

The fact table mixes ad rows and QL/FT rows. They can't share a single natural key:

- Ad row natural key: `Date + Country + Channel + Campaign + Creative`.
- QL/FT row natural key: `Date + Country + Channel + Channel_Group` (no campaign-level context from Salesforce).

The `ON` clause in the MERGE uses a `CASE` based on whether `Impressions` and `Spend_AUD` are both NULL (the QL/FT signature) and applies the appropriate key. Without this split, an ad row and a QL/FT row sharing `Date+Country+Channel` would either collide or stay duplicated.

### 5.5 Update / Insert semantics

- `WHEN MATCHED`: full overwrite of metric columns + `Date_Modified`. `Date_Added` is preserved.
- `WHEN NOT MATCHED`: full insert with both `Date_Added` and `Date_Modified` set to today.

---

## 6. Edge Cases & Behaviours

| Case | Handling |
|---|---|
| Empty `input/` | Each parser logs "No file found" → all channels in `failed_channels`; nothing uploaded. |
| Multiple matching files | `find_file` picks the newest by trailing DDMMYY in the filename and warns. |
| Locked file on Windows during archive | Logged but not raised; file stays in `input/`. |
| Weekly source (BiliBili) | Each row's Date = week start; aggregations downstream must be week-aware. |
| Non-APAC rows in source | Filtered out per-parser using `APAC_COUNTRIES`. |
| Affiliate AU/NZ rows | Hard-filtered in `parse_affiliate`. |
| Numeric-only UTM source | Treated as IB. |
| `affiliate-...` UTM source | Treated as Affiliates. |
| `Stage` not in funded set (FT) | Excluded — no FT count contribution. |
| CTR percent vs decimal | Apple auto-detects by presence of `%`; Meta divides by 100; native decimals passed through. |
| TradingView FX | Multiplied by `TRADINGVIEW_FX_RATE` (1.58, manually maintained). |

---

## 7. Coding Conventions

- One parser per channel; pure functions returning `(df, err)`.
- Never raise out of a parser.
- Always end with `std_cols(df)` so downstream concatenation is column-stable.
- Use `pd.to_numeric(..., errors="coerce")` for any numeric coercion from raw cells.
- Use `pd.to_datetime(..., errors="coerce")` and drop NaT rows before `.dt.date`.
- Avoid in-place mutation of incoming DataFrames once they're shared.
- Print progress with the existing emoji prefixes for consistency in the analyst-facing log.

---

## 8. Local Run Recipe

```
pip install pandas openpyxl google-cloud-bigquery google-auth
# place credentials.json at repo root
# drop raw files into ./input/
python run.py
```

Output: console summary of per-channel rows + final BigQuery row count and updated/inserted breakdown.

---

## 9. Open Issues / Tech Debt

- Configuration drift between `config.py` and the imports in `parsers.py` / `bq_uploader.py` (see Release Notes).
- No `requirements.txt` / lockfile.
- `output/` artefacts and the duplicated nested `performance_marketing/` directory are legacy and unused by `run.py`; candidates for cleanup.
- FX rate (`TRADINGVIEW_FX_RATE`) is a hard-coded constant — should ideally be looked up at run time.
- No unit tests; parsing is verified by visual inspection of console output and BQ row counts.
