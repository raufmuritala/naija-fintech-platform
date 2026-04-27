"""
postgres_transactions_to_bq_dag.py
────────────────────────────────────
Phase 2 — Batch ingestion: transactions

Pipeline:  PostgreSQL → Parquet → GCS → BigQuery (raw_transactions)
Schedule:  Daily at 01:00 WAT (00:00 UTC)
Backfill:  Yes — each run processes only its own execution date (idempotent)
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

# ─── Config ───────────────────────────────────────────────────────────────────
GCP_PROJECT    = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET     = os.environ["GCS_BUCKET"]
BQ_DATASET     = os.environ["BIGQUERY_DATASET"]
BQ_TABLE       = "raw_transactions"
SOURCE_DB_CONN = os.environ["SOURCE_DB_CONN"]

BQ_SCHEMA = [
    bigquery.SchemaField("transaction_id",    "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("account_id",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("transaction_type",  "STRING"),
    bigquery.SchemaField("amount",            "NUMERIC"),
    bigquery.SchemaField("currency",          "STRING"),
    bigquery.SchemaField("direction",         "STRING"),
    bigquery.SchemaField("balance_before",    "NUMERIC"),
    bigquery.SchemaField("balance_after",     "NUMERIC"),
    bigquery.SchemaField("status",            "STRING"),
    bigquery.SchemaField("channel",           "STRING"),
    bigquery.SchemaField("narration",         "STRING"),
    bigquery.SchemaField("reference",         "STRING"),
    bigquery.SchemaField("session_id",        "STRING"),
    bigquery.SchemaField("counterparty_bank", "STRING"),
    bigquery.SchemaField("counterparty_acct", "STRING"),
    bigquery.SchemaField("counterparty_name", "STRING"),
    bigquery.SchemaField("fee_amount",        "NUMERIC"),
    bigquery.SchemaField("vat_amount",        "NUMERIC"),
    bigquery.SchemaField("device_id",         "STRING"),
    bigquery.SchemaField("ip_address",        "STRING"),
    bigquery.SchemaField("location_state",    "STRING"),
    bigquery.SchemaField("created_at",        "TIMESTAMP"),
    bigquery.SchemaField("updated_at",        "TIMESTAMP"),
    bigquery.SchemaField("value_date",        "DATE"),
    bigquery.SchemaField("_ingested_at",      "TIMESTAMP"),
    bigquery.SchemaField("_source_system",    "STRING"),
]

DEFAULT_ARGS = {
    "owner":                    "data-engineering",
    "depends_on_past":          False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "sla":                      timedelta(hours=2),
}


def on_failure_callback(context):
    logger.error("=" * 60)
    logger.error(f"TASK FAILED: {context['dag'].dag_id}.{context['task_instance'].task_id}")
    logger.error(f"Date      : {context['ds']}")
    logger.error(f"Exception : {context.get('exception', 'unknown')}")
    logger.error("=" * 60)


@dag(
    dag_id="postgres_transactions_to_bq",
    description="Daily batch: transactions from Postgres → GCS Parquet → BigQuery",
    schedule_interval="0 0 * * *",
    start_date=days_ago(7),
    catchup=True,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    on_failure_callback=on_failure_callback,
    tags=["batch", "postgres", "bigquery", "phase-2"],
    doc_md="""
## Transactions Batch DAG

Extracts all transactions created on `{{ ds }}` from the NaijaBank PostgreSQL
source database, writes them as Parquet to GCS, then loads into BigQuery.

**Idempotency**: partition decorator + WRITE_TRUNCATE — safe to re-run.
**Backfill**: catchup=True, max_active_runs=3.
    """,
)
def postgres_transactions_to_bq():

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    @task(task_id="extract_from_postgres")
    def extract_from_postgres(ds=None, **context):
        """
        Pull all transactions for the execution date from Postgres.
        Uses a named (server-side) cursor so large result sets
        stream in chunks rather than loading everything into memory.
        """
        logger.info(f"Extracting transactions for date: {ds}")

        conn = psycopg2.connect(SOURCE_DB_CONN)
        conn.set_session(readonly=True)

        try:
            with conn.cursor(
                name="txn_cursor",
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cur.itersize = 5_000

                cur.execute("""
                    SELECT
                        transaction_id::TEXT,
                        account_id::TEXT,
                        transaction_type,
                        amount,
                        currency,
                        direction,
                        balance_before,
                        balance_after,
                        status,
                        channel,
                        narration,
                        reference,
                        session_id,
                        counterparty_bank,
                        counterparty_acct,
                        counterparty_name,
                        fee_amount,
                        vat_amount,
                        device_id,
                        ip_address,
                        location_state,
                        created_at,
                        updated_at,
                        value_date
                    FROM transactions
                    WHERE DATE(created_at AT TIME ZONE 'Africa/Lagos') = %(exec_date)s
                    ORDER BY created_at
                """, {"exec_date": ds})

                rows = [dict(r) for r in cur]

        finally:
            conn.close()

        logger.info(f"Extracted {len(rows):,} transactions for {ds}")
        return {"execution_date": ds, "row_count": len(rows), "rows": rows}

    @task(task_id="write_parquet_to_gcs")
    def write_parquet_to_gcs(extract_result: dict):
        """
        Convert rows to Parquet (Snappy compressed) and upload to GCS.

        Path pattern:
            gs://{bucket}/raw/transactions/dt=YYYY-MM-DD/transactions.parquet

        Hive-style partition prefix lets BigQuery detect the date column
        automatically if you ever need to load the whole folder at once.
        """
        ds   = extract_result["execution_date"]
        rows = extract_result["rows"]
        path = f"raw/transactions/dt={ds}/transactions.parquet"

        if rows:
            df = pd.DataFrame(rows)
            df["_ingested_at"]   = pd.Timestamp.utcnow()
            df["_source_system"] = "postgres_naijabank"

            for col in ["amount", "balance_before", "balance_after",
                        "fee_amount", "vat_amount"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df = pd.DataFrame(columns=[f.name for f in BQ_SCHEMA])

        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="snappy")
        buf.seek(0)
        data = buf.read()

        storage.Client().bucket(GCS_BUCKET).blob(path).upload_from_string(
            data, content_type="application/octet-stream"
        )
        logger.info(f"Uploaded {len(data)/1024:.1f} KB → gs://{GCS_BUCKET}/{path}")

        return {
            "gcs_uri":        f"gs://{GCS_BUCKET}/{path}",
            "execution_date": ds,
            "row_count":      extract_result["row_count"],
        }

    @task(task_id="load_gcs_to_bigquery")
    def load_gcs_to_bigquery(gcs_result: dict):
        """
        Load the Parquet file into a specific BigQuery date partition.

        Uses a partition decorator (table$YYYYMMDD) + WRITE_TRUNCATE so
        re-running the DAG for the same date safely overwrites that partition
        without touching other dates.
        """
        ds        = gcs_result["execution_date"]
        gcs_uri   = gcs_result["gcs_uri"]
        partition = ds.replace("-", "")
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

        job_config = bigquery.LoadJobConfig(
            source_format     = bigquery.SourceFormat.PARQUET,
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema            = BQ_SCHEMA,
            time_partitioning = bigquery.TimePartitioning(
                type_  = bigquery.TimePartitioningType.DAY,
                field  = "value_date",
            ),
            clustering_fields = ["transaction_type", "status", "channel"],
        )

        bq     = bigquery.Client(project=GCP_PROJECT)
        job    = bq.load_table_from_uri(gcs_uri, f"{table_ref}${partition}", job_config=job_config)
        job.result()

        if job.errors:
            raise RuntimeError(f"BigQuery load errors: {job.errors}")

        logger.info(f"Loaded {gcs_result['row_count']:,} rows → {table_ref} (partition {partition})")
        return {"bq_table": table_ref, "partition": partition, "rows_loaded": gcs_result["row_count"]}

    @task(task_id="validate_load")
    def validate_load(load_result: dict, extract_result: dict):
        """
        Row-count reconciliation: BigQuery must match what Postgres gave us.
        Raises on any mismatch — prevents silent data loss going undetected.
        """
        expected  = extract_result["row_count"]
        if expected == 0:
            logger.info("No rows expected — skipping count validation")
            return

        partition = load_result["partition"]
        date_str  = f"{partition[:4]}-{partition[4:6]}-{partition[6:]}"
        bq        = bigquery.Client(project=GCP_PROJECT)

        result    = bq.query(f"""
            SELECT COUNT(*) AS n
            FROM `{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE DATE(value_date) = '{date_str}'
        """).result()

        actual = next(result).n
        if actual != expected:
            raise ValueError(
                f"Row count mismatch! Postgres={expected:,}, BigQuery={actual:,}"
            )
        logger.info(f"✅ Validation passed — {actual:,} rows confirmed in BigQuery")

    # ─── Task wiring ──────────────────────────────────────────────────────────
    extracted  = extract_from_postgres()
    gcs_result = write_parquet_to_gcs(extracted)
    bq_result  = load_gcs_to_bigquery(gcs_result)
    validated  = validate_load(bq_result, extracted)

    start >> extracted
    validated >> end


postgres_transactions_to_bq()
