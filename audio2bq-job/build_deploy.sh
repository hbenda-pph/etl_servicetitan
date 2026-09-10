#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA AUDIO2BQ-JOB (Cloud Run Job)
# Multi-Environment: DEV, QUA, PRO
# Transcripción y Feature Extraction con Vertex AI Gemini 2.5 Flash -> BigQuery Silver
# =============================================================================
#
# Uso:
#   ./build_deploy.sh [dev|qua|pro]
#   ./build_deploy.sh        # Detecta el ambiente desde el proyecto activo de gcloud
#
# =============================================================================

set -e

# =============================================================================
# ARRAY DE RECURSOS POR AMBIENTE
# =============================================================================

declare -A RESOURCES_MEMORY=(      [dev]="4Gi"   [qua]="4Gi"   [pro]="8Gi"   )
declare -A RESOURCES_CPU=(         [dev]="2"      [qua]="2"      [pro]="4"     )
declare -A RESOURCES_TIMEOUT=(     [dev]="3600"   [qua]="3600"   [pro]="3600"  )
declare -A RESOURCES_PARALLELISM=( [dev]="1"      [qua]="1"      [pro]="2"     )
declare -A RESOURCES_TASKS=(       [dev]="1"      [qua]="1"      [pro]="2"     )

# =============================================================================
# DETECCIÓN / VALIDACIÓN DE AMBIENTE
# =============================================================================

CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)

if [ -n "$1" ]; then
    ENVIRONMENT=$(echo "$1" | tr '[:upper:]' '[:lower:]')

    if [[ ! "$ENVIRONMENT" =~ ^(dev|qua|pro)$ ]]; then
        echo "❌ Ambiente inválido: '$ENVIRONMENT'"
        echo "Uso: ./build_deploy.sh [dev|qua|pro]"
        exit 1
    fi
else
    echo "🔍 Detectando ambiente desde proyecto activo de gcloud..."
    case "$CURRENT_PROJECT" in
        platform-partners-des)     ENVIRONMENT="dev"; echo "✅ Detectado: DEV" ;;
        platform-partners-qua)     ENVIRONMENT="qua"; echo "✅ Detectado: QUA" ;;
        constant-height-455614-i0) ENVIRONMENT="pro"; echo "✅ Detectado: PRO" ;;
        *)
            echo "⚠️  Proyecto activo: ${CURRENT_PROJECT} — no reconocido. Usando DEV por defecto."
            ENVIRONMENT="dev"
            ;;
    esac
fi

# =============================================================================
# CONFIGURACIÓN POR AMBIENTE
# =============================================================================

case "$ENVIRONMENT" in
    dev)
        PROJECT_ID="platform-partners-des"
        PROJECT_NAME="platform-partners-des"
        JOB_NAME="etl-audio2bq-job-dev"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-des.iam.gserviceaccount.com"
        ;;
    qua)
        PROJECT_ID="platform-partners-qua"
        PROJECT_NAME="platform-partners-qua"
        JOB_NAME="etl-audio2bq-job-qua"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-qua.iam.gserviceaccount.com"
        ;;
    pro)
        PROJECT_ID="constant-height-455614-i0"
        PROJECT_NAME="platform-partners-pro"
        JOB_NAME="etl-audio2bq-job"
        SERVICE_ACCOUNT="etl-servicetitan@${PROJECT_ID}.iam.gserviceaccount.com"
        ;;
esac

MEMORY="${RESOURCES_MEMORY[$ENVIRONMENT]}"
CPU="${RESOURCES_CPU[$ENVIRONMENT]}"
TASK_TIMEOUT="${RESOURCES_TIMEOUT[$ENVIRONMENT]}"
PARALLELISM="${RESOURCES_PARALLELISM[$ENVIRONMENT]}"
TASKS="${RESOURCES_TASKS[$ENVIRONMENT]}"

REGION="us-east1"
IMAGE_NAME="etl-audio2bq"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"
MAX_RETRIES="1"
ETL_MODE="all"

echo ""
echo "🚀 Build & Deploy — AUDIO2BQ-JOB (Modo: ${ETL_MODE^^})"
echo "==========================================================="
echo "🌍 AMBIENTE   : ${ENVIRONMENT^^}"
echo "📋 Proyecto ID: ${PROJECT_ID}"
echo "📋 Project Name: ${PROJECT_NAME}"
echo "📋 Job Name   : ${JOB_NAME}"
echo "📋 Región     : ${REGION}"
echo "📋 Imagen     : ${IMAGE_TAG}"
echo "📋 SA         : ${SERVICE_ACCOUNT}"
echo "📋 Memoria    : ${MEMORY}"
echo "📋 CPU        : ${CPU}"
echo "📋 Timeout    : ${TASK_TIMEOUT}s"
echo "📋 ETL_MODE   : ${ETL_MODE}"
echo ""

if [ ! -f "main.py" ]; then
    echo "❌ Error: main.py no encontrado."
    echo "   Ejecuta este script desde el directorio audio2bq-job/"
    exit 1
fi

if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI no está instalado o no está en el PATH"
    exit 1
fi

CURRENT_PROJECT=$(gcloud config get-value project)
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    echo "⚠️  Proyecto actual: ${CURRENT_PROJECT}"
    echo "🔧 Configurando proyecto a: ${PROJECT_ID}"
    gcloud config set project "${PROJECT_ID}"
fi

# =============================================================================
# PASO 1: BUILD
# =============================================================================

echo ""
echo "🔨 PASO 1: BUILD (Creando imagen Docker)"
echo "=========================================="
gcloud builds submit --tag "${IMAGE_TAG}"

if [ $? -eq 0 ]; then
    echo "✅ Build exitoso!"
else
    echo "❌ Error en el build"
    exit 1
fi

# =============================================================================
# PASO 2: CREATE / UPDATE JOB
# =============================================================================

echo ""
echo "🚀 PASO 2: CREATE/UPDATE JOB"
echo "============================="

PARALLEL_FLAGS=""
if [ "$TASKS" != "1" ]; then
    PARALLEL_FLAGS="--parallelism ${PARALLELISM} --tasks ${TASKS}"
fi

ENV_VARS="GCP_PROJECT=${PROJECT_NAME},ETL_MODE=${ETL_MODE}"

if gcloud run jobs describe "${JOB_NAME}" --region="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "📝 Job existe — actualizando..."
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
        --command="" \
        --args="" \
        ${PARALLEL_FLAGS}
else
    echo "🆕 Job no existe — creando..."
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
        --command="" \
        --args="" \
        ${PARALLEL_FLAGS}
fi

if [ $? -eq 0 ]; then
    echo "✅ Job creado/actualizado!"
else
    echo "❌ Error creando/actualizando job"
    exit 1
fi

echo ""
echo "🎉 ¡DEPLOY COMPLETADO!"
echo "======================"
echo "📊 Ejecutar manualmente:"
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
