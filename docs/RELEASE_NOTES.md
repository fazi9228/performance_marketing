# Release Notes
## Pepperstone APAC Performance Marketing Pipeline

> Authoritative source: `git log` on `main`. This file summarises the user-visible changes per release in chronological order (oldest at the bottom).

---

## Current state — `main` @ commit `64c0ca3` (2026-04-21)

**Status:** ⚠️ **Pre-release — known broken at HEAD**

### Summary
- Adds **TA Media** as a new channel (parser, file pattern, channel mapping).
- Adds **Kuaishou** as a new channel.
- Adds **Meta - Agency** as a separate channel from Meta.
- Adds **Apple Search Ads**, **TikTok**, **Douyin** parsers (introduced over prior commits).
- Updates Affiliates QL/FT parsing logic.
- Migrates Sheets-as-source-of-truth → **BigQuery as source of truth**, with MERGE upsert.

### ⚠️ Known issue at HEAD
Commit `64c0ca3` ("modified config.py") removed several constants from `config.py` that `parsers.py` and `bq_uploader.py` still import. Running `python run.py` against this HEAD raises `ImportError` immediately.

Missing exports:
- `ARCHIVE_DIR`
- `META_AGENCY_CHANNEL`
- `APPLE_CHANNEL`, `APPLE_COUNTRY_MAP`
- `TIKTOK_CHANNEL`
- `DOUYIN_CHANNEL`, `DOUYIN_COUNTRY`
- `KUAISHOU_CHANNEL`, `KUAISHOU_COUNTRY`
- `TA_MEDIA_CHANNEL`
- `BQ_PROJECT_ID`, `BQ_DATASET`, `BQ_TABLE`, `BQ_LOCATION`

Also, `AD_PERFORMANCE_COLS` no longer includes `Date_Added` / `Date_Modified`, so `std_cols(df)` will drop those columns before they reach the uploader. The uploader re-adds them in `_prepare_df()`, so this is non-fatal but inconsistent.

**Fix path:** restore the removed constants in `config.py` (or revert `64c0ca3`). The previous commit `f059fc0` had all required exports. After the fix, validate with a dry run on a small input set.

### Action items before declaring this a release
1. Reconcile `config.py` with the constants imported by `parsers.py` and `bq_uploader.py`.
2. Tag the resulting commit (suggested: `v1.0.0`).
3. Add a `requirements.txt` so install is reproducible.
4. Smoke-test with one file per channel against a non-prod BigQuery dataset.

---

## Release history (from git log)

### `f059fc0` — Add TA Media channel
- New `parse_ta_media` parser; `Daily` sheet, columns `Date / Country / Ad group / Creative / Cost (AUD) / Impression / Clicks`.
- Added `TA_Media_` filename pattern, `TA Media` channel label, channel-group mapping, and substring rule for `ta-media` UTMs.
- Country column required — parser raises if missing.

### `11cd40d` — Affiliates QL/FT parsing update
- Affiliates rows from the affiliate report now contribute QL / FT counts directly (with Spend = Commission).
- Salesforce QL/FT pipeline now **excludes** rows where `Channel_Group == "Affiliates"` to prevent double-counting.

### `f7dea69` — Add Kuaishou channel
- `parse_kuaishou` mirrors RedNote shape (Daily sheet, `Main Account / Placement / Targeting Approach / Creative`).
- Defaults Country to `CN` if column missing.
- Same-day duplicate campaigns are disambiguated with `#N` suffix.

### `91032c2` — Add ID, PH support in AdRoll
- `ADROLL_COUNTRY_MAP` extended to include Indonesia → ID and Philippines → PH.
- `APAC_COUNTRIES` widened accordingly (also covers IN, MN in upstream commits).

### `c4714a1` — Add column for Meta_Agency
- `parse_meta_agency` introduced as a separate parser/channel from `parse_meta`.
- Distinct channel label `Meta - Agency` keeps agency-managed spend reportable on its own while still rolling up under `Meta` channel-group.

### `cf61e81` — Upload to BigQuery + Sheets overwrite
- Pipeline now writes to BigQuery in addition to (then in place of) Google Sheets.
- Connected Sheets is the recommended downstream pattern.

### `129a3a7` — Simplified pipeline; BigQuery is source of truth
- Removed local dedup/rollback machinery.
- BigQuery becomes the single source of truth — its 7-day time-travel and 7-day fail-safe replace the homegrown rollback.

### `c0e2880` — Added new UTM mappings
- Expanded `UTM_TO_CHANNEL` with additional Salesforce UTM source values seen in the wild (Bing PMax, Google variants, CocCoc, etc.).

### `513ab24` — Added Meta_Agency source
- Initial introduction of the Meta agency feed; later split into its own parser in `c4714a1`.

### `5678da1` — Updated parsers
- General parser hardening (header autodetection, better column-name fallbacks, NaN handling).

### `3d4ec1d` — Migrated to BigQuery; restructured project
- Initial BigQuery uploader with deduplication and rollback semantics; project layout normalised.

### `9cf7f28` — 2026-03-15 baseline
- The original Sheets-only pipeline; this is the snapshot the current architecture has evolved from.

---

## Versioning

The repo is currently un-tagged. Once the HEAD-import issue is fixed, recommend tagging:

| Tag | Commit | Description |
|---|---|---|
| `v0.1` (retrospective) | `9cf7f28` | Sheets-only baseline |
| `v0.5` (retrospective) | `3d4ec1d` | First BQ-enabled cut |
| `v0.9` (retrospective) | `129a3a7` | BQ as source of truth |
| `v1.0` (proposed) | TBD post-fix | TA Media + Kuaishou + Meta Agency, BQ MERGE upsert, full APAC country coverage |

---

## Upgrade Notes (for analysts)

- After `v0.9`, **stop running the Sheets uploader path**. Looker Studio / Sheets must read from BigQuery via Connected Sheets.
- After `v1.0` (once shipped), the input folder will accept files matching: `Bing_*`, `Meta_*`, `Meta_Agency_*`, `Adroll_*`, `Bilibili_*`, `Rednote_*`, `TradingView_*`, `Apple_*`, `Tiktok_*`, `Douyin_*`, `Kuaishou_*`, `TA_Media_*`, `Affiliates_*`, `QL_*`, `FT_*`. Use the trailing `DDMMYY` convention so the parser picks the newest file.
- `TRADINGVIEW_FX_RATE` (USD→AUD) is a manual constant — confirm at the start of every month.

---

## Rollback

Because BigQuery is now the source of truth and MERGE is idempotent:

1. To roll back code: `git checkout <previous-tag>` and re-run.
2. To roll back data: use BigQuery time travel — `SELECT ... FROM target FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)` (within the 7-day window).
3. To reprocess a specific day: drop the matching files back into `input/` from `input/archive/` and re-run; MERGE will overwrite that day's rows.
