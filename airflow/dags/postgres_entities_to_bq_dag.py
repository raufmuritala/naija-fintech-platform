"""
postgres_entities_to_bq_dag.py
────────────────────────────────
Phase 2 — Batch ingestion: entity tables (full daily snapshot)

Handles slowly-changing tables: users, accounts, cards.
Each run snapshots the full table and tags rows with _snapshot_date.
This lets dbt build point-in-time and SCD logic in the marts layer.

Schedule: Daily at 01:30 WAT (00:30 UTC) — 30 min after transactions DAG
"""

import io
import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import psycopg2.extras
from google.cloud import storage, bigquery

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

GCP_PROJECT    = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET     = os.environ["GCS_BUCKET"]
BQ_DATASET     = os.environ["BIGQUERY_DATASET"]
SOURCE_DB_CONN = os.environ["SOURCE_DB_CONN"]

# ─── Table definitions ────────────────────────────────────────────────────────
ENTITY_TABLES = {
    "users": {
        "query": """
            SELECT
                user_id::TEXT, first_name, last_name, email, phone_number,
                bvn, state_of_origin, city, kyc_level, kyc_verified_at,
                is_active, referral_code, referred_by::TEXT,
                created_at, updated_at
            FROM users ORDER BY created_at
        """,
        "bq_schema": [
            bigquery.SchemaField("user_id",          "STRING",  mode="REQUIRED"),
            bigquery.SchemaField("first_name",        "STRING"),
            bigquery.SchemaField("last_name",         "STRING"),
            bigquery.SchemaField("email",             "STRING"),
            bigquery.SchemaField("phone_number",      "STRING"),
            bigquery.SchemaField("bvn",               "STRING"),
            bigquery.SchemaField("state_of_origin",   "STRING"),
            bigquery.SchemaField("city",              "STRING"),
            bigquery.SchemaField("kyc_level",         "INTEGER"),
            bigquery.SchemaField("kyc_verified_at",   "TIMESTAMP"),
            bigquery.SchemaField("is_active",         "BOOLEAN"),
            bigquery.SchemaField("referral_code",     "STRING"),
            bigquery.SchemaField("referred_by",       "STRING"),
            bigquery.SchemaField("created_at",        "TIMESTAMP"),
            bigquery.SchemaField("updated_at",        "TIMESTAMP"),
            bigquery.SchemaField("_snapshot_date",    "DATE"),
            bigquery.SchemaField("_ingested_at",      "TIMESTAMP"),
        ],
    },
    "accounts": {
        "query": """
            SELECT
                account_id::TEXT, user_id::TEXT, account_number,
                account_type, currency, balance, ledger_balance,
                daily_limit, is_frozen, freeze_reason,
                opened_at, closed_at, updated_at
            FROM accounts ORDER BY opened_at
        """,
        "bq_schema": [
            bigquery.SchemaField("account_id",     "STRING",  mode="REQUIRED"),
            bigquery.SchemaField("user_id",        "STRING",  mode="REQUIRED"),
            bigquery.SchemaField("account_number", "STRING"),
            bigquery.SchemaField("account_type",   "STRING"),
            bigquery.SchemaField("currency",       "STRING"),
            bigquery.SchemaField("balance",        "NUMERIC"),
            bigquery.SchemaField("ledger_balance", "NUMERIC"),
            bigquery.SchemaField("daily_limit",    "NUMERIC"),
            bigquery.SchemaField("is_frozen",      "BOOLEAN"),
            bigquery.SchemaField("freeze_reason",  "STRING"),
            bigquery.SchemaField("opened_at",      "TIMESTAMP"),
            bigquery.SchemaField("closed_at",      "TIMESTAMP"),
            bigquery.SchemaField("updated_at",     "TIMESTAMP"),
            bigquery.SchemaField("_snapshot_date", "DATE"),
            bigquery.SchemaField("_ingested_at",   "TIMESTAMP"),
        ],
    },
    "cards": {
        "query": """
            SELECT
                card_id::TEXT, account_id::TEXT, user_id::TEXT,
                card_type, card_scheme, masked_pan,
                expiry_month, expiry_year, status,
                daily_limit, issued_at, blocked_at, block_reason
            FROM cards ORDER BY issued_at
        """,
        "bq_schema": [
            bigquery.SchemaField("card_id",        "STRING",  mode="REQUIRED"),
            bigquery.SchemaField("account_id",     "STRING"),
            bigquery.SchemaField("user_id",        "STRING"),
            bigquery.SchemaField("card_type",      "STRING"),
            bigquery.SchemaField("card_scheme",    "STRING"),
            bigquery.SchemaField("masked_pan",     "STRING"),
            bigquery.SchemaField("expiry_month",   "INTEGER"),
            bigquery.SchemaField("expiry_year",    "INTEGER"),
            bigquery.SchemaField("status",         "STRING"),
            bigquery.SchemaField("daily_limit",    "NUMERIC"),
            bigquery.SchemaField("issued_at",      "TIMESTAMP"),
            bigquery.SchemaField("blocked_at",     "TIMESTAMP"),
            bigquery.SchemaField("block_reason",   "STRING"),
            bigquery.SchemaField("_snapshot_date", "DATE"),
            bigquery.SchemaField("_ingested_at",   "TIMESTAMP"),
        ],
    },
}

DEFAULT_ARGS = {
    "owner":           "data-engineering",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
}


def _extract_and_load(table_name: str, ds: str) -> None:
    """Generic extract → GCS → BigQuery for any entity table."""
    config = ENTITY_TABLES[table_name]
    logger.info(f"Snapshotting {table_name} for {ds}")

    # ── Extract ───────────────────────────────────────────────────────────────
    conn = psycopg2.connect(SOURCE_DB_CONN)
    conn.set_session(readonly=True)
    try:
        with conn.cursor(
            name=f"{table_name}_cur",
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.itersize = 2_000
            cur.execute(config["query"])
            rows = [dict(r) for r in cur]
    finally:
        conn.close()

    logger.info(f"  Extracted {len(rows):,} {table_name} rows")

    # ── Build DataFrame ───────────────────────────────────────────────────────
    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=[f.name for f in config["bq_schema"]]
    )
    df["_snapshot_date"] = pd.to_datetime(ds).date()
    df["_ingested_at"]   = pd.Timestamp.utcnow()

    # ── Upload Parquet to GCS ─────────────────────────────────────────────────
    gcs_path = f"raw/{table_name}/dt={ds}/{table_name}.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    buf.seek(0)

    storage.Client().bucket(GCS_BUCKET).blob(gcs_path).upload_from_string(
        buf.read(), content_type="application/octet-stream"
    )
    logger.info(f"  Uploaded → gs://{GCS_BUCKET}/{gcs_path}")

    # ── Load to BigQuery ──────────────────────────────────────────────────────
    partition = ds.replace("-", "")
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.raw_{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format     = bigquery.SourceFormat.PARQUET,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema            = config["bq_schema"],
        time_partitioning = bigquery.TimePartitioning(
            type_  = bigquery.TimePartitioningType.DAY,
            field  = "_snapshot_date",
        ),
    )
    bq  = bigquery.Client(project=GCP_PROJECT)
    job = bq.load_table_from_uri(
        f"gs://{GCS_BUCKET}/{gcs_path}",
        f"{table_ref}${partition}",
        job_config=job_config,
    )
    job.result()
    logger.info(f"  Loaded {len(rows):,} rows → {table_ref}")


@dag(
    dag_id="postgres_entities_to_bq",
    description="Daily full snapshot: users, accounts, cards → GCS → BigQuery",
    schedule_interval="30 0 * * *",
    start_date=days_ago(7),
    catchup=True,
    max_active_runs=2,
    default_args=DEFAULT_ARGS,
    tags=["batch", "postgres", "bigquery", "entities", "phase-2"],
    doc_md="""
## Entity Tables Snapshot DAG

Pulls a full snapshot of users, accounts, and cards daily.
Each row is tagged with `_snapshot_date` so dbt can build
slowly-changing dimension (SCD) logic in the marts layer.

The three snapshot tasks run in parallel — they are fully independent.
    """,
)
def postgres_entities_to_bq():

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    @task(task_id="snapshot_users")
    def snapshot_users(ds=None, **ctx):
        _extract_and_load("users", ds)

    @task(task_id="snapshot_accounts")
    def snapshot_accounts(ds=None, **ctx):
        _extract_and_load("accounts", ds)

    @task(task_id="snapshot_cards")
    def snapshot_cards(ds=None, **ctx):
        _extract_and_load("cards", ds)

    users    = snapshot_users()
    accounts = snapshot_accounts()
    cards    = snapshot_cards()

    # All three run in parallel — both DAGs end cleanly at `end`
    start >> [users, accounts, cards] >> end


postgres_entities_to_bq()
