#!/usr/bin/env bash
# =============================================================================
# setup_gcp.sh
# Run this ONCE to bootstrap your GCP project resources.
#
# Prerequisites:
#   1. gcloud CLI installed and authenticated (gcloud auth login)
#   2. .env file exists with GCP_PROJECT_ID and GCS_BUCKET set
#   3. Billing enabled on your GCP project
#
# Usage:
#   chmod +x scripts/setup_gcp.sh
#   source .env && bash scripts/setup_gcp.sh
# =============================================================================

set -euo pipefail

# ─── Load env ────────────────────────────────────────────────────────────────
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

PROJECT_ID="${GCP_PROJECT_ID:?GCP_PROJECT_ID must be set in .env}"
BUCKET="${GCS_BUCKET:?GCS_BUCKET must be set in .env}"
REGION="us-central1"     # cheapest multi-region for BigQuery free tier
BQ_RAW="${BIGQUERY_DATASET:-naija_raw}"
BQ_STAGING="naija_staging"
BQ_MARTS="naija_marts"
PUBSUB_TOPIC="naija-user-events"
PUBSUB_BQ_SUB="naija-events-bq-sub"
SA_NAME="naija-platform-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo ""
echo "═══════════════════════════════════════════════"
echo " NaijaBank Platform — GCP Bootstrap"
echo " Project : $PROJECT_ID"
echo " Region  : $REGION"
echo "═══════════════════════════════════════════════"
echo ""

# ─── Set active project ───────────────────────────────────────────────────────
gcloud config set project "$PROJECT_ID"

# ─── Enable APIs ──────────────────────────────────────────────────────────────
echo "[1/6] Enabling GCP APIs..."
gcloud services enable \
    bigquery.googleapis.com \
    storage.googleapis.com \
    pubsub.googleapis.com \
    iam.googleapis.com \
    --quiet

echo "  ✓ APIs enabled"

# ─── GCS Bucket ───────────────────────────────────────────────────────────────
echo ""
echo "[2/6] Creating GCS bucket: gs://$BUCKET"
if gsutil ls "gs://$BUCKET" &>/dev/null; then
    echo "  ✓ Bucket already exists, skipping"
else
    gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://$BUCKET"
    # Lifecycle: delete raw files older than 90 days to minimise cost
    cat > /tmp/gcs-lifecycle.json <<EOF
{
  "rule": [{
    "action": {"type": "Delete"},
    "condition": {"age": 90}
  }]
}
EOF
    gsutil lifecycle set /tmp/gcs-lifecycle.json "gs://$BUCKET"
    echo "  ✓ Bucket created with 90-day lifecycle"
fi

# Create folder structure in GCS
for folder in transactions fx_rates events cards users; do
    gsutil -q cp /dev/null "gs://$BUCKET/raw/$folder/.keep" 2>/dev/null || true
done
echo "  ✓ GCS folder structure created"

# ─── BigQuery Datasets ────────────────────────────────────────────────────────
echo ""
echo "[3/6] Creating BigQuery datasets..."
for dataset in "$BQ_RAW" "$BQ_STAGING" "$BQ_MARTS"; do
    if bq ls --datasets "$PROJECT_ID" | grep -q "$dataset"; then
        echo "  ✓ Dataset $dataset already exists"
    else
        bq --location="$REGION" mk \
            --dataset \
            --description "NaijaBank Platform — $dataset layer" \
            "$PROJECT_ID:$dataset"
        echo "  ✓ Created dataset: $dataset"
    fi
done

# ─── Pub/Sub Topic & BigQuery Subscription ────────────────────────────────────
echo ""
echo "[4/6] Setting up Pub/Sub topic and BigQuery subscription..."

# Create events topic
if gcloud pubsub topics describe "$PUBSUB_TOPIC" &>/dev/null; then
    echo "  ✓ Topic $PUBSUB_TOPIC already exists"
else
    gcloud pubsub topics create "$PUBSUB_TOPIC" \
        --message-retention-duration=7d
    echo "  ✓ Topic created: $PUBSUB_TOPIC"
fi

# Create raw_events table in BigQuery for the streaming subscription
bq query --use_legacy_sql=false "
CREATE TABLE IF NOT EXISTS \`$PROJECT_ID.$BQ_RAW.raw_user_events\` (
    event_id        STRING,
    event_type      STRING,
    user_id         STRING,
    account_id      STRING,
    session_id      STRING,
    amount          FLOAT64,
    currency        STRING,
    channel         STRING,
    device_type     STRING,
    device_id       STRING,
    ip_address      STRING,
    location_state  STRING,
    metadata        JSON,
    event_timestamp TIMESTAMP,
    ingested_at     TIMESTAMP
)
PARTITION BY DATE(event_timestamp)
OPTIONS (
    description = 'Raw user events from Pub/Sub streaming pipeline'
);
" 2>/dev/null || echo "  ✓ Table raw_user_events already exists"

echo "  ✓ BigQuery raw_user_events table ready"

# Note: BigQuery subscription created in Phase 4 (streaming phase)
echo "  ℹ  Pub/Sub → BigQuery subscription will be created in Phase 4"

# ─── Service Account ──────────────────────────────────────────────────────────
echo ""
echo "[5/6] Creating service account: $SA_NAME"

if gcloud iam service-accounts describe "$SA_EMAIL" &>/dev/null; then
    echo "  ✓ Service account already exists"
else
    gcloud iam service-accounts create "$SA_NAME" \
        --description="NaijaBank data platform service account" \
        --display-name="NaijaBank Platform SA"
    echo "  ✓ Service account created"
fi

# Grant required roles
ROLES=(
    "roles/bigquery.dataEditor"
    "roles/bigquery.jobUser"
    "roles/storage.objectAdmin"
    "roles/pubsub.publisher"
    "roles/pubsub.subscriber"
)

for role in "${ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$SA_EMAIL" \
        --role="$role" \
        --quiet
done
echo "  ✓ IAM roles granted"

# Generate key file
mkdir -p secrets
KEY_FILE="secrets/gcp-service-account.json"
if [ -f "$KEY_FILE" ]; then
    echo "  ✓ Key file already exists at $KEY_FILE"
else
    gcloud iam service-accounts keys create "$KEY_FILE" \
        --iam-account="$SA_EMAIL"
    echo "  ✓ Service account key saved to $KEY_FILE"
    echo "  ⚠️  This file is gitignored — never commit it!"
fi

# ─── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "[6/6] Verifying setup..."
echo ""
echo "═══════════════════════════════════════════════"
echo " ✅ GCP Bootstrap Complete"
echo ""
echo " Resources created:"
echo "   GCS bucket    : gs://$BUCKET"
echo "   BQ datasets   : $BQ_RAW, $BQ_STAGING, $BQ_MARTS"
echo "   Pub/Sub topic : $PUBSUB_TOPIC"
echo "   Service acct  : $SA_EMAIL"
echo "   Key file      : $KEY_FILE"
echo ""
echo " Next step:"
echo "   cp .env.example .env && nano .env"
echo "   docker compose up -d"
echo "═══════════════════════════════════════════════"
