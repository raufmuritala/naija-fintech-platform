# NaijaBank Data Platform 🇳🇬

A production-grade data engineering platform modelled on a Nigerian neobank (Kuda/Moniepoint style). Built to develop hands-on skills across the full modern data stack.

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.8 | DAG scheduling, retries, monitoring |
| Source DB | PostgreSQL 15 | Mock neobank transactional data |
| Landing zone | Google Cloud Storage | Raw file staging (Parquet) |
| Warehouse | Google BigQuery | Analytical warehouse |
| Transformation | dbt-bigquery 1.7 | Staging → marts modelling |
| Streaming | Google Pub/Sub | User event streaming pipeline |
| BI | Looker Studio / Metabase | Dashboards |
| Local dev | Docker Compose | Full local environment |
| CI | GitHub Actions | Lint + schema validation |

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                     SOURCES                          │
│  PostgreSQL (txns)  │  Exchange Rate API  │  Events  │
└────────┬────────────┴────────┬────────────┴────┬─────┘
         │ Batch (daily)       │ API (hourly)    │ Streaming
         ▼                     ▼                 ▼
┌─────────────────────────────────────────────────────┐
│              Apache Airflow 2.8                      │
│     (Schedules, retries, sensors, dependencies)      │
└────────────────────────┬────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────┐
│   GCS (raw Parquet)  ──→  Google BigQuery (raw)      │
└────────────────────────┬────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────┐
│                   dbt-bigquery                       │
│   raw → staging → intermediate → marts              │
└────────────────────────┬────────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────────┐
│         Looker Studio  │  Metabase (optional)        │
└─────────────────────────────────────────────────────┘
```

## Project Phases

| Phase | Focus | Status |
|---|---|---|
| 1 | Local environment, schema, seed data | 🔄 In progress |
| 2 | Airflow batch DAG (Postgres → GCS → BigQuery) | ⏳ Upcoming |
| 3 | API ingestion DAG (exchange rates) | ⏳ Upcoming |
| 4 | Streaming pipeline (Pub/Sub → BigQuery) | ⏳ Upcoming |
| 5 | dbt modelling (staging → marts) | ⏳ Upcoming |
| 6 | Airflow + dbt integration | ⏳ Upcoming |
| 7 | Dashboard (Looker Studio) | ⏳ Upcoming |
| 8 | Production hardening | ⏳ Upcoming |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- GCP account (free tier is sufficient)
- `gcloud` CLI authenticated

### 1. Clone and configure

```bash
git clone https://github.com/YOUR_USERNAME/naija-fintech-platform.git
cd naija-fintech-platform
cp .env.example .env
# Edit .env with your GCP project ID, bucket name, and API key
```

### 2. Bootstrap GCP (run once)

```bash
gcloud auth login
source .env && bash scripts/setup_gcp.sh
```

### 3. Start local services

```bash
make up
# Airflow UI → http://localhost:8080  (admin / admin)
# pgAdmin    → http://localhost:5050  (admin@naijabank.dev / admin)
```

### 4. Generate seed data

```bash
make seed
# Generates 500 users, ~3 months of transactions
# For quick testing: make seed-small
```

### 5. Explore the data

```bash
make psql
# Or open pgAdmin at http://localhost:5050
```

## Repository Layout

```
naija-fintech-platform/
├── airflow/
│   ├── dags/
│   │   ├── postgres_to_gcs_dag.py       # Phase 2: Batch
│   │   ├── fx_rates_dag.py              # Phase 3: API
│   │   └── streaming_validate_dag.py    # Phase 4: Streaming
│   └── plugins/
│       └── utils.py                     # Shared helpers
├── dbt/
│   ├── models/
│   │   ├── staging/                     # stg_* models
│   │   ├── intermediate/                # int_* models
│   │   └── marts/                       # dim_* and fct_* models
│   └── macros/
├── sql/
│   └── 01_schema.sql                    # Source DB schema
├── scripts/
│   ├── generate_seed_data.py            # Data generator
│   └── setup_gcp.sh                     # GCP bootstrap
├── infra/
│   └── pgadmin-servers.json
├── .github/
│   └── workflows/
│       └── ci.yml                       # Lint + validate on PR
├── docker-compose.yml
├── Makefile
├── requirements.txt
└── .env.example
```

## Data Model (Source DB)

The source PostgreSQL database mimics a real Nigerian neobank:

- **users** — customers with Nigerian KYC fields (BVN, state, phone)
- **accounts** — NUBAN account numbers, savings/wallet types
- **transactions** — credits/debits across all channels (mobile, POS, ATM, USSD)
- **cards** — Verve/Mastercard/Visa virtual and physical cards
- **transfers** — inter-bank transfers with CBN bank codes

## Key Business Questions (answered in Phase 7)

- Monthly Active Users (MAU) and retention trends
- Transaction success rate by channel and type
- Transfer volume in NGN vs USD equivalent (using live FX rates)
- Peak transaction hours by day of week
- Failed transaction patterns — which banks fail most for interbank transfers?
- User acquisition via referral programme performance

## Cost Notes

This project is designed to run within GCP free tier limits:
- BigQuery: 10 GB storage + 1 TB queries/month free
- GCS: 5 GB storage free
- Pub/Sub: 10 GB/month free
- Airflow: runs locally in Docker (no Cloud Composer costs)

Total expected monthly GCP cost during development: **$0–2**

## Contributing

This is a personal learning project. PRs and issues welcome.

## License

MIT
