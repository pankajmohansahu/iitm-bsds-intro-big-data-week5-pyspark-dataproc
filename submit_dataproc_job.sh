#!/usr/bin/env bash
set -euo pipefail

# Optional overrides:
# PROJECT_ID="your-project-id" REGION="us-central1" CLUSTER_NAME="week5-dataproc-cluster" BUCKET="ibd-week5-21f2001203" ./submit_dataproc_job.sh
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
REGION="${REGION:-us-central1}"
CLUSTER_NAME="${CLUSTER_NAME:-week5-dataproc-cluster}"
BUCKET="${BUCKET:-ibd-week5-21f2001203}"

INPUT_DIR="gs://${BUCKET}/week5/input"
OUTPUT_DIR="gs://${BUCKET}/week5/output"
CODE_DIR="gs://${BUCKET}/week5/code"

CUSTOMER_FILE="customer_dataset.csv"
TRANSACTION_FILE="transaction_dataset.csv"
PYSPARK_FILE="week5_assignment4_dataproc_pyspark.py"

if [[ -z "${PROJECT_ID}" ]]; then
  echo "ERROR: PROJECT_ID is empty. Set it in environment or via: gcloud config set project <PROJECT_ID>"
  exit 1
fi

gcloud config set project "${PROJECT_ID}"
gcloud services enable dataproc.googleapis.com compute.googleapis.com storage.googleapis.com >/dev/null

for f in "${CUSTOMER_FILE}" "${TRANSACTION_FILE}" "${PYSPARK_FILE}"; do
  if [[ ! -f "${f}" ]]; then
    echo "ERROR: Missing file '${f}' in current directory $(pwd)"
    exit 1
  fi
done

if ! gsutil ls -b "gs://${BUCKET}" >/dev/null 2>&1; then
  echo "ERROR: Bucket gs://${BUCKET} not found or not accessible"
  exit 1
fi

echo "" | gsutil cp - "${INPUT_DIR}/.keep" >/dev/null
echo "" | gsutil cp - "${CODE_DIR}/.keep" >/dev/null
echo "" | gsutil cp - "${OUTPUT_DIR}/.keep" >/dev/null

gsutil cp "${CUSTOMER_FILE}" "${INPUT_DIR}/customer_dataset.csv"
gsutil cp "${TRANSACTION_FILE}" "${INPUT_DIR}/transaction_dataset.csv"
gsutil cp "${PYSPARK_FILE}" "${CODE_DIR}/${PYSPARK_FILE}"

if ! gcloud dataproc clusters describe "${CLUSTER_NAME}" --region="${REGION}" >/dev/null 2>&1; then
  echo "Cluster ${CLUSTER_NAME} not found in ${REGION}. Creating single-node cluster..."
  gcloud dataproc clusters create "${CLUSTER_NAME}" \
    --region="${REGION}" \
    --single-node \
    --master-machine-type="e2-standard-2" \
    --image-version="2.2-debian12"
fi

gcloud dataproc jobs submit pyspark "${CODE_DIR}/${PYSPARK_FILE}" \
  --region="${REGION}" \
  --cluster="${CLUSTER_NAME}" \
  -- \
  --customer_input="${INPUT_DIR}/customer_dataset.csv" \
  --transaction_input="${INPUT_DIR}/transaction_dataset.csv" \
  --output_base="${OUTPUT_DIR}"

gsutil ls -r "${OUTPUT_DIR}/**" || true
echo "Job submitted and completed. Check outputs under ${OUTPUT_DIR}"