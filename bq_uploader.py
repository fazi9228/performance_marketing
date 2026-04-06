"""
bq_uploader.py — BigQuery upload with SQL MERGE upsert

Features:
  1. Auto-creates table if it doesn't exist (with proper schema + DATE type)
  2. MERGE upsert: updates changed rows, inserts new rows — no duplicates
  3. Uses row-type-aware keys (same logic as Sheets uploader):
     - Ad rows:   Date + Country + Channel + Campaign + Creative
     - QL/FT rows: Date + Country + Channel + Channel_Group
  4. Reports exactly how many rows were updated vs inserted
  5. Uses your existing credentials.json — no new setup needed
"""

import os
import pandas as pd
from datetime import date
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from config import (
    CREDENTIALS_FILE, BQ_PROJECT_ID, BQ_DATASET, BQ_TABLE, BQ_LOCATION
)

TODAY = date.today().isoformat()

# Full table reference
TABLE_REF = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
STAGING_TABLE = f"{BQ_PROJECT_ID}.{BQ_DATASET}._staging_{BQ_TABLE}"

# ── Schema ────────────────────────────────────────────────────────────────────

BQ_SCHEMA = [
    bigquery.SchemaField("Date",          "DATE"),
    bigquery.SchemaField("Country",       "STRING"),
    bigquery.SchemaField("Channel",       "STRING"),
    bigquery.SchemaField("Campaign",      "STRING"),
    bigquery.SchemaField("Creative",      "STRING"),
    bigquery.SchemaField("Impressions",   "FLOAT64"),
    bigquery.SchemaField("Clicks",        "FLOAT64"),
    bigquery.SchemaField("CTR",           "FLOAT64"),
    bigquery.SchemaField("Spend_AUD",     "FLOAT64"),
    bigquery.SchemaField("QL",            "INT64"),
    bigquery.SchemaField("FT",            "INT64"),
    bigquery.SchemaField("Channel_Group", "STRING"),
    bigquery.SchemaField("Date_Added",    "DATE"),
    bigquery.SchemaField("Date_Modified", "DATE"),
]


def _get_client():
    """Create an authenticated BigQuery client using the existing service account."""
    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(
        project=BQ_PROJECT_ID,
        credentials=creds,
        location=BQ_LOCATION
    )


def _ensure_table(client):
    """Create the table if it doesn't exist."""
    try:
        client.get_table(TABLE_REF)
        return True  # already exists
    except Exception:
        table = bigquery.Table(TABLE_REF, schema=BQ_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="Date"
        )
        client.create_table(table)
        print(f"         📊 Created BigQuery table: {TABLE_REF}")
        print(f"            Partitioned by Date (monthly)")
        return False  # newly created


def _prepare_df(df):
    """Prepare DataFrame for BigQuery: proper types and column names."""
    out = df.copy()

    # Date → proper date type
    out["Date"] = pd.to_datetime(out["Date"]).dt.date

    # Rename Spend (AUD) → Spend_AUD (BigQuery doesn't like parens in column names)
    if "Spend (AUD)" in out.columns:
        out = out.rename(columns={"Spend (AUD)": "Spend_AUD"})

    # Numeric columns
    for col in ["Impressions", "Clicks", "CTR", "Spend_AUD"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    for col in ["QL", "FT"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            # Convert to nullable int (Int64) so NaN stays as NULL, not 0
            out[col] = out[col].astype("Int64")

    # String columns — fill NaN with None
    for col in ["Country", "Channel", "Campaign", "Creative", "Channel_Group"]:
        if col in out.columns:
            out[col] = out[col].where(out[col].notna(), None)

    # Date_Added + Date_Modified
    out["Date_Added"] = pd.to_datetime(TODAY).date()
    out["Date_Modified"] = pd.to_datetime(TODAY).date()

    # Keep only schema columns
    schema_cols = [f.name for f in BQ_SCHEMA]
    for col in schema_cols:
        if col not in out.columns:
            out[col] = None

    return out[schema_cols]


def upload_to_bigquery(combined):
    """
    Upload data to BigQuery using MERGE (upsert).
    Returns True on success, False on failure.
    """
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"      ⚠️  credentials.json not found — skipping BigQuery upload.")
        return False

    if combined.empty or len(combined) == 0:
        print(f"      ⚠️  No data to upload — skipping BigQuery.")
        return True

    try:
        client = _get_client()

        # ── Ensure table exists ───────────────────────────────────────────
        is_new = not _ensure_table(client)

        # ── Get existing row count ────────────────────────────────────────
        if not is_new:
            result = client.query(f"SELECT COUNT(*) as cnt FROM `{TABLE_REF}`").result()
            existing_count = list(result)[0].cnt
            print(f"         📊 Existing BigQuery table: {existing_count:,} rows")
        else:
            existing_count = 0

        # ── Prepare data ──────────────────────────────────────────────────
        bq_df = _prepare_df(combined)
        print(f"         📊 New data: {len(bq_df):,} rows to upsert")

        # ── Upload to staging table ───────────────────────────────────────

        # Deduplicate: aggregate rows with the same key before uploading.
        # QL/FT rows (no Impressions/Spend) group by Date+Country+Channel+Channel_Group
        # Ad rows group by Date+Country+Channel+Campaign+Creative

        def _is_qlft(row):
            return pd.isna(row.get("Impressions")) and pd.isna(row.get("Spend_AUD"))

        qlft_mask = bq_df.apply(_is_qlft, axis=1)
        qlft_df = bq_df[qlft_mask].copy()
        ad_df   = bq_df[~qlft_mask].copy()

        # Aggregate QL/FT rows: sum QL and FT for duplicate keys
        if not qlft_df.empty:
            qlft_keys = ["Date", "Country", "Channel", "Channel_Group"]
            agg_dict = {"QL": "sum", "FT": "sum", "Campaign": "first", "Creative": "first",
                        "Impressions": "first", "Clicks": "first", "CTR": "first",
                        "Spend_AUD": "first", "Date_Added": "first", "Date_Modified": "first"}
            qlft_df = qlft_df.groupby(qlft_keys, as_index=False, dropna=False).agg(agg_dict)

        # Aggregate Ad rows: sum numeric values for duplicate keys
        if not ad_df.empty:
            ad_keys = ["Date", "Country", "Channel", "Campaign", "Creative"]
            agg_dict = {"Impressions": "sum", "Clicks": "sum", "Spend_AUD": "sum",
                        "CTR": "first", "QL": "first", "FT": "first",
                        "Channel_Group": "first", "Date_Added": "first", "Date_Modified": "first"}
            ad_df = ad_df.groupby(ad_keys, as_index=False, dropna=False).agg(agg_dict)

        bq_df = pd.concat([ad_df, qlft_df], ignore_index=True)

        job_config = bigquery.LoadJobConfig(
            schema=BQ_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(
            bq_df, STAGING_TABLE, job_config=job_config
        )
        job.result()  # Wait for completion
        print(f"         ✅ Staging table loaded: {len(bq_df):,} rows (deduplicated)")

        # ── MERGE: upsert from staging into main table ────────────────────
        merge_sql = f"""
        MERGE `{TABLE_REF}` AS target
        USING `{STAGING_TABLE}` AS source
        ON (
            -- Row-type-aware matching:
            -- QL/FT rows (no Impressions AND no Spend): match on Date+Country+Channel+Channel_Group
            -- Ad rows (have Impressions or Spend): match on Date+Country+Channel+Campaign+Creative
            CASE
                WHEN (source.Impressions IS NULL AND source.Spend_AUD IS NULL)
                THEN (target.Date = source.Date
                      AND IFNULL(target.Country, '') = IFNULL(source.Country, '')
                      AND IFNULL(target.Channel, '') = IFNULL(source.Channel, '')
                      AND IFNULL(target.Channel_Group, '') = IFNULL(source.Channel_Group, '')
                      AND (target.Impressions IS NULL AND target.Spend_AUD IS NULL))
                ELSE (target.Date = source.Date
                      AND IFNULL(target.Country, '') = IFNULL(source.Country, '')
                      AND IFNULL(target.Channel, '') = IFNULL(source.Channel, '')
                      AND IFNULL(target.Campaign, '') = IFNULL(source.Campaign, '')
                      AND IFNULL(target.Creative, '') = IFNULL(source.Creative, '')
                      AND NOT (target.Impressions IS NULL AND target.Spend_AUD IS NULL))
            END
        )
        WHEN MATCHED THEN UPDATE SET
            target.Impressions    = source.Impressions,
            target.Clicks         = source.Clicks,
            target.CTR            = source.CTR,
            target.Spend_AUD      = source.Spend_AUD,
            target.QL             = source.QL,
            target.FT             = source.FT,
            target.Channel_Group  = source.Channel_Group,
            target.Date_Modified  = source.Date_Modified
        WHEN NOT MATCHED THEN INSERT
            (Date, Country, Channel, Campaign, Creative,
             Impressions, Clicks, CTR, Spend_AUD,
             QL, FT, Channel_Group, Date_Added, Date_Modified)
        VALUES
            (source.Date, source.Country, source.Channel, source.Campaign, source.Creative,
             source.Impressions, source.Clicks, source.CTR, source.Spend_AUD,
             source.QL, source.FT, source.Channel_Group, source.Date_Added, source.Date_Modified)
        """

        merge_job = client.query(merge_sql)
        merge_result = merge_job.result()

        # Get merge stats
        stats = merge_job.num_dml_affected_rows
        print(f"         ✅ MERGE complete: {stats:,} rows affected")

        # ── Post-merge count ──────────────────────────────────────────────
        result = client.query(f"SELECT COUNT(*) as cnt FROM `{TABLE_REF}`").result()
        final_count = list(result)[0].cnt
        n_inserted = final_count - existing_count
        n_updated = stats - n_inserted if stats > n_inserted else 0

        print(f"         📊 Final: {final_count:,} rows ({n_updated:,} updated, {n_inserted:,} inserted)")

        # ── Cleanup staging table ─────────────────────────────────────────
        client.delete_table(STAGING_TABLE, not_found_ok=True)

        print(f"      ✅ BigQuery updated: {TABLE_REF}")
        return True

    except ImportError:
        print("      ❌ google-cloud-bigquery not installed. Run: pip install google-cloud-bigquery")
        return False
    except Exception as e:
        import traceback
        print(f"      ❌ BigQuery upload failed: {e}")
        traceback.print_exc()
        return False


def load_initial_data(filepath):
    """
    One-time initial load: reads a clean Excel file and uploads to BigQuery.
    Use this to migrate your existing clean data.

    Usage:
      python -c "from bq_uploader import load_initial_data; load_initial_data('path/to/CLEANED.xlsx')"
    """
    print(f"\n{'='*55}")
    print(f"  BigQuery Initial Load")
    print(f"{'='*55}")

    if not os.path.exists(filepath):
        print(f"❌ File not found: {filepath}")
        return False

    print(f"\n[1/3] Reading {os.path.basename(filepath)} ...")
    df = pd.read_excel(filepath, dtype=str)
    print(f"      Rows: {len(df):,}")

    print(f"\n[2/3] Preparing data ...")
    bq_df = _prepare_df(df)

    print(f"\n[3/3] Uploading to BigQuery ...")
    try:
        client = _get_client()
        _ensure_table(client)

        job_config = bigquery.LoadJobConfig(
            schema=BQ_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        job = client.load_table_from_dataframe(
            bq_df, TABLE_REF, job_config=job_config
        )
        job.result()

        result = client.query(f"SELECT COUNT(*) as cnt FROM `{TABLE_REF}`").result()
        final_count = list(result)[0].cnt

        print(f"\n{'='*55}")
        print(f"  ✅ Initial load complete!")
        print(f"     Table: {TABLE_REF}")
        print(f"     Rows:  {final_count:,}")
        print(f"{'='*55}")
        return True

    except Exception as e:
        import traceback
        print(f"❌ Initial load failed: {e}")
        traceback.print_exc()
        return False
