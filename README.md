# NaijaBank Data Platform 🇳🇬

A production-grade data engineering platform modelled on a Nigerian neobank (Kuda/Moniepoint style). Built to develop hands-on skills across the full modern data stack — Airflow, BigQuery, dbt, Pub/Sub, and GCS — while solving a real, well-defined analytical problem.

---

## Problem Statement

A typical Nigerian neobank accumulates transactional data across multiple sources: a PostgreSQL operational database, third-party APIs (FX rates, telco), and high-frequency user event streams. This data sits in silos — it is not reliably ingested, not transformed into business-ready models, and not surfaced to decision-makers in dashboards.

Engineers and analysts have no dependable data layer to answer critical business questions about users, transactions, or financial performance. Any analysis requires running manual SQL directly against the production database — slow, risky, and not reproducible.

---

## Current State (As-Is)

| Area | Problem |
|---|---|
| Transactional data | Lives only in PostgreSQL — no analytical copy, no history |
| Exchange rates | Not captured at all — no audit trail of NGN/USD rates over time |
| User events | Lost entirely — no log of logins, transfers, or card declines |
| Orchestration | None — any data movement is manual and unreliable |
| Warehouse | None — analytical queries run directly on the production DB |
| Transformation | None — raw fields are not cleaned, typed, or modelled |
| Dashboards | None — business questions require ad hoc SQL on demand |

---

## Intended Solution

Build a fully automated, layered data platform with three distinct ingestion patterns, a governed transformation layer, and business-facing dashboards — all orchestrated by Apache Airflow.

**Three ingestion patterns:**

- **Batch** — Daily automated extract of all transactions, users, accounts, and cards from Postgres → GCS (Parquet) → BigQuery raw layer. Idempotent, backfill-capable, retryable.
- **API (near real-time)** — Hourly NGN exchange rate ingestion from a live REST API, upserted into BigQuery with full rate history preserved for trend analysis.
- **Streaming** — Near-real-time user event capture (login, transfer initiated, transfer completed, card declined) published to Google Pub/Sub and streamed directly into BigQuery, validated and deduplicated by an Airflow DAG.

**Transformation:**
dbt converts raw ingested data into clean, tested, documented models across three layers — staging (type-cast, rename), intermediate (business logic), and marts (dimensional models ready for BI tools).

**Orchestration:**
Apache Airflow manages all pipelines — scheduling, retries, SLA alerts, dependency chains, and data quality gates between extract, load, and transform steps.

**Serving:**
Looker Studio dashboards connected to BigQuery marts answer the five core business questions automatically, refreshed daily.

---

## Desired State (To-Be)

After the platform is fully built, the following is true:

- Any analyst can query clean, tested BigQuery mart tables instead of touching the production database
- All three ingestion patterns run automatically on schedule with retries and alerting
- Exchange rate history is preserved, enabling FX-impact analysis on transaction volume
- User behaviour events are captured in near-real-time, enabling funnel and retention analysis
- Five core business questions are answered in a Looker Studio dashboard, refreshed daily
- The full pipeline — extract → load → transform → test → serve — runs end-to-end without manual intervention
- Monthly GCP cost remains within the free tier (~$0–2)

---

## The Five Business Questions This Platform Answers

1. **Who are our active users and how are we retaining them?**
   MAU, DAU, 7-day and 30-day retention by cohort and state of origin.

2. **What is our transaction volume and success rate by channel?**
   Daily NGN volume, failure rate by channel (mobile, POS, USSD, ATM), average transfer size.

3. **How does NGN devaluation affect our business?**
   Correlate USD-equivalent transaction volume against live FX rate movements over time.

4. **Which interbank transfer routes fail most often?**
   Failure rate by destination bank (GTB, UBA, Zenith, Access) and time of day.

5. **How effective is our referral programme?**
   Referred vs organic user activation rate and transaction behaviour in first 30 days.

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | DAG scheduling, retries, SLA monitoring |
| Source DB | PostgreSQL 15 | Mock neobank transactional data |
| Landing zone | Google Cloud Storage | Raw Parquet file staging |
| Warehouse | Google BigQuery | Analytical warehouse (raw → staging → marts) |
| Transformation | dbt-bigquery 1.7 | Dimensional modelling and data quality tests |
| Streaming | Google Pub/Sub | Near-real-time user event pipeline |
| BI | Looker Studio | Dashboards connected to BigQuery marts |
| Local dev | Docker Compose | Full local environment |
| CI | GitHub Actions | Lint and schema validation on every PR |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         SOURCES                                   │
│   PostgreSQL (txns)   │   Exchange Rate API   │   Event stream    │
└────────┬──────────────┴──────────┬────────────┴────────┬─────────┘
         │ Batch (daily)           │ API (hourly)         │ Streaming
         ▼                         ▼                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Apache Airflow 2.8                             │
│          Schedules · Retries · Sensors · Dependency chains        │
└─────────────────────────────┬────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│         GCS (raw Parquet)   ──→   Google BigQuery (raw)           │
└─────────────────────────────┬────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                       dbt-bigquery                                │
│          raw → staging → intermediate → marts                     │
└─────────────────────────────┬────────────────────────────────────┘
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│              Looker Studio  │  Metabase (optional)                │
└──────────────────────────────────────────────────────────────────┘
```

---

## Build Phases

| Phase | Focus | Status |
|---|---|---|
| 1 | Local environment, PostgreSQL schema, Nigerian seed data, GCP bootstrap | ✅ Done |
| 2 | Airflow batch DAGs — Postgres → GCS → BigQuery (transactions + entities) | 🔄 In progress |
| 3 | API ingestion DAG — hourly NGN exchange rates from live REST API | ⏳ Upcoming |
| 4 | Streaming pipeline — Pub/Sub event generator → BigQuery | ⏳ Upcoming |
| 5 | dbt modelling — staging, intermediate, and marts layers | ⏳ Upcoming |
| 6 | Airflow + dbt integration — full end-to-end orchestrated pipeline | ⏳ Upcoming |
| 7 | Looker Studio dashboards answering the five business questions | ⏳ Upcoming |
| 8 | Production hardening — CI/CD, secrets, documentation, optional cloud deploy | ⏳ Upcoming |

---

## Repository Layout

```
naija-fintech-platform/
├── airflow/
│   ├── dags/
│   │   ├── postgres_transactions_to_bq_dag.py   # Phase 2: daily batch
│   │   ├── postgres_entities_to_bq_dag.py        # Phase 2: users/accounts/cards
│   │   ├── fx_rates_dag.py                       # Phase 3: hourly API
│   │   └── streaming_validate_dag.py             # Phase 4: streaming validation
│   ├── plugins/
│   │   └── utils.py                              # Shared helpers
│   └── logs/                                     # Gitignored
├── dbt/
│   ├── models/
│   │   ├── staging/                              # stg_* models
│   │   ├── intermediate/                         # int_* models
│   │   └── marts/                               # dim_* and fct_* models
│   ├── macros/
│   └── tests/
├── sql/
│   ├── 01_schema.sql                            # PostgreSQL source schema
│   └── 02_bigquery_raw_schema.sql               # BigQuery raw layer DDL
├── scripts/
│   ├── generate_seed_data.py                    # Nigerian neobank data generator
│   └── setup_gcp.sh                             # GCP one-time bootstrap
├── infra/
│   └── pgadmin-servers.json                     # pgAdmin pre-configured connections
├── secrets/                                     # Gitignored — GCP key file lives here
├── .github/
│   └── workflows/
│       └── ci.yml                               # Lint + schema validation on PR
├── docker-compose.yml
├── Makefile
├── requirements.txt
├── .env.example
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker Desktop running
- Python 3.11+
- A GCP account (free tier is sufficient)
- `gcloud` CLI installed and authenticated

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/naija-fintech-platform.git
cd naija-fintech-platform
cp .env.example .env
# Edit .env — set GCP_PROJECT_ID, GCS_BUCKET, EXCHANGE_RATE_API_KEY
```

### 2. Bootstrap GCP (run once)

```bash
gcloud auth login
source .env && bash scripts/setup_gcp.sh
```

This creates your GCS bucket, BigQuery datasets (raw/staging/marts), Pub/Sub topic, and a service account key at `secrets/gcp-service-account.json`.

### 3. Start local services

```bash
make up
```

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@naijabank.dev / admin |
| Source DB | localhost:5433 | naija / naija123 |

### 4. Generate seed data

```bash
make seed
# 500 users, ~90 days of transactions
# For quick testing: make seed-small (50 users, 30 days)
```

### 5. Explore the data

```bash
make psql
# Or open pgAdmin at http://localhost:5050
```

---

## Data Model (Source DB)

The PostgreSQL source database mirrors a real Nigerian neobank:

**users** — customers with Nigerian KYC fields: BVN (masked), state of origin, NUBAN phone number, KYC level (1–3), referral chain.

**accounts** — 10-digit NUBAN account numbers, savings and wallet account types, NGN balances, daily transfer limits, freeze status.

**transactions** — credits and debits across all channels (mobile app, POS, ATM, USSD, web). Includes NIP fee tiers, counterparty bank details using CBN bank codes (GTB=058, UBA=033, Zenith=057, Access=044), and transaction status (successful/failed/pending).

**cards** — Verve, Mastercard, and Visa cards (virtual and physical), masked PAN, card status.

**transfers** — inter-bank and internal transfer records linked to their debit and credit transaction legs, with CBN bank codes for destination institutions.

---

## Key Design Decisions

**Why GCS as a landing zone?** Writing Parquet to GCS before loading to BigQuery decouples extraction from loading. If a BigQuery load fails, the file is already safe in GCS — reprocessing doesn't require hitting the source DB again.

**Why Parquet?** Columnar format means BigQuery reads only the columns it needs. Snappy compression reduces file size by 5–10x vs CSV. Types are preserved — no CSV parsing ambiguity for NUMERIC amounts or TIMESTAMPTZ fields.

**Why full snapshots for entity tables?** Users, accounts, and cards change slowly. Daily full snapshots tagged with `_snapshot_date` let dbt reconstruct point-in-time state and build SCD logic in the marts layer without complex CDC infrastructure.

**Why idempotent loads?** Every batch DAG uses a partition decorator (`table$YYYYMMDD`) with `WRITE_TRUNCATE`. Re-running a DAG for the same date safely overwrites that day's data — no duplicates, no gaps.

**Why `catchup=True` on the transactions DAG?** Enables backfill — if the pipeline is down for three days, Airflow will automatically process each missed date when it comes back up, in parallel (capped by `max_active_runs`).

---

## Cost Notes

Designed to run within GCP free tier limits throughout development:

- BigQuery: 10 GB storage + 1 TB queries per month free
- GCS: 5 GB storage free
- Pub/Sub: 10 GB per month free
- Airflow: runs locally in Docker (no Cloud Composer cost)

Expected monthly GCP cost during development: **$0–2**

---

## License

MIT
