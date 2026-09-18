#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA ETL-INBOX-AUDIO2BQ-JOB (Cloud Run Job)
# Proyecto fijo: pph-inbox
# Modo ETL: INBOX (compañías candidatas al consorcio)
# =============================================================================
#
# Uso:
#   ./build_deploy_inbox.sh
#
# Recursos:
#   Job (audio2bq): 8GB / 4 CPU / 60 minutos
#   Scheduler     : Cada 6 horas (a las :30) (0 */6 * * *)
#
# =============================================================================

set -e

# =============================================================================
# CONFIGURACIÓN FIJA DE INBOX
# =============================================================================

PROJECT_ID="pph-inbox"
JOB_NAME="etl-inbox-audio2bq-job"
SERVICE_ACCOUNT="etl-servicetitan@pph-inbox.iam.gserviceaccount.com"
REGION="us-east1"
IMAGE_NAME="etl-inbox-audio2bq"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"

MEMORY="8Gi"
CPU="4"
TASK_TIMEOUT="3600"
MAX_RETRIES="1"

ETL_MODE="inbox"

SCHEDULE_NAME="etl-inbox-audio2bq-schedule"
SCHEDULE_CRON="30 */6 * * *"

# =============================================================================
# RESUMEN DE CONFIGURACIÓN
# =============================================================================

echo ""
echo "🚀 Build & Deploy — ETL-INBOX-AUDIO2BQ-JOB (Modo: ${ETL_MODE^^})"
echo "=================================================================="
echo "📋 Proyecto   : ${PROJECT_ID}"
echo "📋 Job Name   : ${JOB_NAME}"
echo "📋 Región     : ${REGION}"
echo "📋 Imagen     : ${IMAGE_TAG}"
echo "📋 SA         : ${SERVICE_ACCOUNT}"
echo "📋 Memoria    : ${MEMORY}"
echo "📋 CPU        : ${CPU}"
echo "📋 Timeout    : ${TASK_TIMEOUT}s ($(( TASK_TIMEOUT / 60 )) minutos)"
echo "📋 ETL_MODE   : ${ETL_MODE}"
echo "📋 Scheduler  : ${SCHEDULE_NAME} (${SCHEDULE_CRON})"
echo ""

# =============================================================================
# VERIFICACIONES PREVIAS
# =============================================================================

if [ ! -f "main.py" ]; then
    echo "❌ Error: main.py no encontrado."
    echo "   Ejecuta este script desde el directorio audio2bq-job/"
    exit 1
fi

if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI no está instalado o no está en el PATH"
    exit 1
fi

CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
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

ENV_VARS="GCP_PROJECT=${PROJECT_ID},ETL_MODE=${ETL_MODE}"

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
        --args=""
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
        --args=""
fi

if [ $? -eq 0 ]; then
    echo "✅ Job creado/actualizado!"
else
    echo "❌ Error creando/actualizando job"
    exit 1
fi

# =============================================================================
# PASO 3: SCHEDULER (INBOX: cada 6 horas)
# =============================================================================

echo ""
echo "⏰ PASO 3: CONFIGURAR SCHEDULER (INBOX)"
echo "========================================"

JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe "${SCHEDULE_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "📝 Scheduler existe — actualizando..."
    gcloud scheduler jobs update http "${SCHEDULE_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --schedule="${SCHEDULE_CRON}" \
        --uri="${JOB_URI}" \
        --http-method=POST \
        --oauth-service-account-email="${SERVICE_ACCOUNT}" \
        --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform
else
    echo "🆕 Scheduler no existe — creando..."
    gcloud scheduler jobs create http "${SCHEDULE_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --schedule="${SCHEDULE_CRON}" \
        --uri="${JOB_URI}" \
        --http-method=POST \
        --oauth-service-account-email="${SERVICE_ACCOUNT}" \
        --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform
fi

if [ $? -eq 0 ]; then
    echo "✅ Scheduler configurado exitosamente!"
else
    echo "⚠️  Advertencia: No se pudo configurar el Scheduler. Revisa permisos."
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================

echo ""
echo "🎉 ¡DEPLOY INBOX COMPLETADO!"
echo "============================"
echo "📊 Job Name    : ${JOB_NAME}"
echo "📊 Proyecto    : ${PROJECT_ID}"
echo "📊 Región      : ${REGION}"
echo "📊 Imagen      : ${IMAGE_TAG}"
echo "📊 Scheduler   : ${SCHEDULE_NAME} (${SCHEDULE_CRON})"
echo ""
echo "▶️  Ejecutar manualmente ahora:"
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""

