# High-Level Design (HLD)
## Pepperstone APAC Performance Marketing Pipeline

**Version:** 1.0
**Last Updated:** 2026-04-27
**Owner:** APAC Performance Marketing
**Repo root:** `perf_marketing_pipeline/`

---

## 1. Purpose

A batch ETL pipeline that consolidates raw marketing-channel exports and Salesforce lead/funded-trader exports into a single, deduplicated, daily-grain fact table in BigQuery. The BigQuery table is the **single source of truth**; downstream Looker Studio / Google Sheets read from BigQuery via Connected Sheets.

---

## 2. Goals & Non-Goals

### Goals
- Unify ~13 ad-channel sources + Salesforce QL/FT exports into one schema.
- Standardise per-day, per-country, per-channel, per-campaign rows.
- Idempotent re-runs (MERGE upsert — no duplicates, late-arriving data corrects past rows).
- Self-service: marketing analyst drops files in `input/` and runs one command.
- Survive partial failures: a broken channel does not abort the pipeline.

### Non-Goals
- Real-time / streaming ingestion (this is a daily batch).
- Direct API pulls from ad platforms (manual file drop is intentional).
- Visualisation — Looker Studio / Google Sheets handle that.
- Data warehouse modelling beyond the single fact table (no star schema, no dimensions).

---

## 3. System Context

```
┌──────────────────────┐
│ Ad Platforms (manual │
│ exports → xlsx/csv): │
│  Bing, Meta, AdRoll, │
│  BiliBili, RedNote,  │
│  TradingView, Apple, │
│  TikTok, Douyin,     │
│  Kuaishou, TA Media, │
│  Affiliates          │
└──────────┬───────────┘
           │
           ▼
   ┌──────────────┐         ┌──────────────────┐
   │  ./input/    │◄────────│  Salesforce QL/  │
   │  (drop zone) │         │  FT exports      │
   └──────┬───────┘         └──────────────────┘
          │
          ▼
   ┌──────────────────────────────────────────┐
   │  Pipeline (Python, run.py)               │
   │  ┌──────────┐   ┌──────────────────┐     │
   │  │ parsers  │──▶│  bq_uploader     │     │
   │  │  .py     │   │  (MERGE upsert)  │     │
   │  └──────────┘   └────────┬─────────┘     │
   └────────────────────────── │ ─────────────┘
                               ▼
                      ┌──────────────────┐
                      │  BigQuery        │
                      │  ad_performance  │
                      │  (fact table)    │
                      └────────┬─────────┘
                               │ Connected Sheets
                               ▼
                ┌────────────────────────────┐
                │ Google Sheets / Looker     │
                │ Studio dashboards          │
                └────────────────────────────┘

   ┌──────────────┐
   │ ./input/     │  ← processed files moved here after a successful run
   │   archive/   │
   └──────────────┘
```

---

## 4. Architecture Overview

### Components

| Component | File | Responsibility |
|---|---|---|
| Orchestrator | `run.py` | Two-step driver: parse → upload. Prints summary. |
| Parsers | `parsers.py` | One function per channel; normalises each source to the common schema. |
| BQ Uploader | `bq_uploader.py` | Ensures table, prepares typed DataFrame, runs MERGE upsert, cleans staging. |
| Configuration | `config.py` | Filename patterns, channel labels, country maps, UTM → channel mapping, BQ refs. |
| Credentials | `credentials.json` | Google service-account key (gitignored). |

### Data flow (one run)

1. `run.py` calls `parse_all()` in `parsers.py`.
2. For each channel, the matching file is found by filename pattern, parsed, and normalised to the common schema. Failed channels are collected, not raised.
3. QL/FT Salesforce files are parsed separately and produce rows where ad metrics are NULL.
4. All frames are concatenated into one DataFrame.
5. `upload_to_bigquery()` writes that DataFrame to a staging table, runs a row-type-aware `MERGE` into the main table, and drops staging.
6. Successfully processed input files are moved to `input/archive/`.

---

## 5. External Dependencies

| Dependency | Used for | Notes |
|---|---|---|
| Google BigQuery | Source-of-truth fact table | Project, dataset, table, location set in `config.py`. |
| Google Cloud Service Account | Auth to BigQuery | JSON key stored locally as `credentials.json`; **must not** be committed. |
| `pandas`, `openpyxl` | Excel/CSV parsing & transforms | |
| `google-cloud-bigquery`, `google-auth` | BQ client + auth | |
| Connected Sheets (downstream) | Reads BigQuery from Sheets/Looker | Outside the pipeline's responsibility. |

---

## 6. Data Model (Fact Table)

Table: `<BQ_PROJECT_ID>.<BQ_DATASET>.ad_performance`
Partitioned: by `Date` (monthly).

| Column | Type | Notes |
|---|---|---|
| Date | DATE | Day grain; weekly sources (BiliBili) use the week-start date. |
| Country | STRING | ISO-style 2-letter code (HK, TW, SG, MY, TH, VN, CN, ID, PH, IN, MN). |
| Channel | STRING | Fine-grained label, e.g. `Bing - PMax`, `AdRoll - Retargeting`, `Meta`. |
| Campaign | STRING | Source campaign name (often enriched with ad-set / placement / creative). |
| Creative | STRING | Where available; nullable. |
| Impressions | FLOAT64 | NULL on QL/FT-only rows. |
| Clicks | FLOAT64 | NULL on QL/FT-only rows. |
| CTR | FLOAT64 | Decimal (0.085 = 8.5%). |
| Spend_AUD | FLOAT64 | All spend normalised to AUD. TradingView USD→AUD via `TRADINGVIEW_FX_RATE`. |
| QL | INT64 | Qualified leads (count). NULL on pure ad rows. |
| FT | INT64 | Funded traders (count). NULL on pure ad rows. |
| Channel_Group | STRING | Coarser grouping, e.g. `Bing`, `AdRoll`, `Google`. |
| Date_Added | DATE | First insert timestamp (audit). |
| Date_Modified | DATE | Last update timestamp (audit). |

### Two row types coexist in one table

- **Ad rows** — have Impressions/Spend; identified by `Date + Country + Channel + Campaign + Creative`.
- **QL/FT rows** — Impressions/Spend are NULL; identified by `Date + Country + Channel + Channel_Group`.

The MERGE statement uses a `CASE` expression so each row type matches on the correct natural key. This is critical: if both row types collapsed to the same key the QL/FT counts would overwrite the ad metrics or vice versa.

---

## 7. Idempotency & Late-Arriving Data

- The MERGE upsert means the same input file can be re-run safely; existing rows update in place.
- If yesterday's data arrives today, MERGE updates yesterday's row rather than inserting a duplicate.
- BigQuery 7-day time travel + 7-day fail-safe window provides a free recovery path for accidental bad loads.
- Raw files are archived (not deleted), giving a manual reprocessing path.

---

## 8. Failure Modes & Tolerance

| Failure | Behaviour | Recovery |
|---|---|---|
| One channel's parser raises | That channel is recorded in `failed_channels`, pipeline continues. | Fix file/format; re-run — MERGE handles upsert. |
| `credentials.json` missing | Upload step prints a warning, returns False, run completes without BQ write. | Add credentials, re-run. |
| BQ table does not exist | `_ensure_table()` auto-creates it with the correct schema and partitioning. | None needed. |
| Locked file on archive (Windows) | Archive of that file is skipped with a warning; data already loaded. | Manually move the file later. |
| Multiple files match a pattern | Newest-by-filename-date is used; warning is printed. | Remove or rename old files in `input/`. |

---

## 9. Operational Model

- **Cadence:** ad-hoc, typically daily/weekly by an analyst.
- **Trigger:** human runs `python run.py` after dropping new files into `input/`.
- **Run time:** seconds to a few minutes depending on file count.
- **Alerting:** stdout summary at end of run; the human reviews `failed_channels` list.
- **Backup:** BigQuery time travel (7 days) + archived raw files in `input/archive/`.

---

## 10. Security & Secrets

- `credentials.json` is the only secret; gitignored.
- Service account requires only `bigquery.dataEditor` + `bigquery.jobUser` on the target dataset.
- No PII in the fact table — Salesforce exports are aggregated to counts before insertion (the user_id columns are dropped during aggregation in `parse_ql_ft`).

---

## 11. Out of Scope / Future Considerations

- Direct API ingestion (Meta Marketing API, Bing Ads API, Salesforce API) — would remove manual file handling.
- Airflow / Prefect orchestration — currently run by hand.
- Schema evolution tooling — schema is hard-coded in `bq_uploader.py`.
- A `requirements.txt` / `pyproject.toml` is currently missing; dependencies are documented only in the README.
