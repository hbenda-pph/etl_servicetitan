#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA ETL-CSV2BQ-JOB (Cloud Run Job)
# Multi-Environment: DEV, QUA, PRO
# =============================================================================

set -e  # Salir si hay algún error

declare -A RESOURCES_MEMORY=(      [dev]="16Gi"  [qua]="16Gi"  [pro]="16Gi"  )
declare -A RESOURCES_CPU=(         [dev]="8"      [qua]="8"      [pro]="8"     )
declare -A RESOURCES_TIMEOUT=(     [dev]="7200"   [qua]="7200"   [pro]="7200"  )
declare -A RESOURCES_PARALLELISM=( [dev]="1"      [qua]="3"      [pro]="3"     )
declare -A RESOURCES_TASKS=(       [dev]="1"      [qua]="3"      [pro]="3"     )

CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)

if [ -n "$1" ]; then
    ENVIRONMENT=$(echo "$1" | tr '[:upper:]' '[:lower:]')
    if [[ ! "$ENVIRONMENT" =~ ^(dev|qua|pro)$ ]]; then
        echo "❌ Ambiente inválido: '$ENVIRONMENT'"
        exit 1
    fi
else
    echo "🔍 Detectando ambiente desde proyecto activo de gcloud..."
    case "$CURRENT_PROJECT" in
        platform-partners-des) ENVIRONMENT="dev"; echo "✅ Detectado: DEV" ;;
        platform-partners-qua) ENVIRONMENT="qua"; echo "✅ Detectado: QUA" ;;
        constant-height-455614-i0) ENVIRONMENT="pro"; echo "✅ Detectado: PRO" ;;
        *)
            echo "⚠️  Proyecto activo: ${CURRENT_PROJECT} — no reconocido. Usando DEV por defecto."
            ENVIRONMENT="dev"
            ;;
    esac
fi

case "$ENVIRONMENT" in
    dev)
        PROJECT_ID="platform-partners-des"
        PROJECT_NAME="platform-partners-des"
        JOB_NAME="etl-csv2bq-job-dev"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-des.iam.gserviceaccount.com"
        ;;
    qua)
        PROJECT_ID="platform-partners-qua"
        PROJECT_NAME="platform-partners-qua"
        JOB_NAME="etl-csv2bq-job-qua"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-qua.iam.gserviceaccount.com"
        ;;
    pro)
        PROJECT_ID="constant-height-455614-i0"
        PROJECT_NAME="platform-partners-pro"
        JOB_NAME="etl-csv2bq-job"
        SERVICE_ACCOUNT="etl-servicetitan@${PROJECT_ID}.iam.gserviceaccount.com"
        ;;
esac

MEMORY="${RESOURCES_MEMORY[$ENVIRONMENT]}"
CPU="${RESOURCES_CPU[$ENVIRONMENT]}"
TASK_TIMEOUT="${RESOURCES_TIMEOUT[$ENVIRONMENT]}"
PARALLELISM="${RESOURCES_PARALLELISM[$ENVIRONMENT]}"
TASKS="${RESOURCES_TASKS[$ENVIRONMENT]}"

REGION="us-east1"
IMAGE_NAME="etl-csv2bq"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"
MAX_RETRIES="1"
ETL_MODE="all"

echo ""
echo "🚀 Build & Deploy — ETL-CSV2BQ-JOB"
echo "============================================================"
echo "🌍 AMBIENTE   : ${ENVIRONMENT^^}"
echo "📋 Job Name   : ${JOB_NAME}"
echo "📋 Imagen     : ${IMAGE_TAG}"
echo "📋 SA         : ${SERVICE_ACCOUNT}"
echo ""

if [ ! -f "main.py" ]; then
    echo "❌ Error: main.py no encontrado."
    exit 1
fi

CURRENT_PROJECT=$(gcloud config get-value project)
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    gcloud config set project "${PROJECT_ID}"
fi

echo "🔨 PASO 1: BUILD"
gcloud builds submit --tag "${IMAGE_TAG}"

echo "🚀 PASO 2: CREATE/UPDATE JOB"
PARALLEL_FLAGS=""
if [ "$TASKS" != "1" ]; then
    PARALLEL_FLAGS="--parallelism ${PARALLELISM} --tasks ${TASKS}"
fi

ENV_VARS="GCP_PROJECT=${PROJECT_NAME},ETL_MODE=${ETL_MODE}"

if gcloud run jobs describe "${JOB_NAME}" --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    gcloud run jobs update "${JOB_NAME}" \
        --image "${IMAGE_TAG}" \
        --region "${REGION}" \
        --project "${PROJECT_ID}" \
        --service-account "${SERVICE_ACCOUNT}" \
        --memory "${MEMORY}" \
        --cpu "${CPU}" \
        --max-retries "${MAX_RETRIES}" \
        --task-timeout "${TASK_TIMEOUT}" \
        --set-env-vars "${ENV_VARS}" \
        ${PARALLEL_FLAGS}
else
    gcloud run jobs create "${JOB_NAME}" \
        --image "${IMAGE_TAG}" \
        --region "${REGION}" \
        --project "${PROJECT_ID}" \
        --service-account "${SERVICE_ACCOUNT}" \
        --memory "${MEMORY}" \
        --cpu "${CPU}" \
        --max-retries "${MAX_RETRIES}" \
        --task-timeout "${TASK_TIMEOUT}" \
        --set-env-vars "${ENV_VARS}" \
        ${PARALLEL_FLAGS}
fi

echo "🎉 ¡DEPLOY COMPLETADO!"
