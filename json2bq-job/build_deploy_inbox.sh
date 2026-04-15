#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA ETL-INBOX-JSON2BQ-JOB (Cloud Run Job)
# Proyecto fijo: pph-inbox
# Modo ETL: INBOX (compañías candidatas al consorcio)
# =============================================================================
#
# Uso:
#   ./build_deploy_inbox.sh
#
# Recursos (según proceso_etl.csv):
#   Job2 (json2bq): 8GB / 4 CPU / 1 hora
#   Scheduler     : Cada 6 horas y 15 minutos  (10 */6 * * *)
#
# Nota: INBOX no tiene ambientes DEV/QUA/PRO. Es un proceso temporal
# e independiente. Opera como "dev+pro" en un solo proyecto pph-inbox.
# =============================================================================

set -e  # Salir si hay algún error

# =============================================================================
# CONFIGURACIÓN FIJA DE INBOX
# =============================================================================

PROJECT_ID="pph-inbox"
JOB_NAME="etl-inbox-json2bq-job"
SERVICE_ACCOUNT="etl-servicetitan@pph-inbox.iam.gserviceaccount.com"
REGION="us-east1"
IMAGE_NAME="etl-inbox-json2bq"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"

# Recursos (proceso_etl.csv: Job2 INBOX = 8GB / 4 CPU / 1 hora)
MEMORY="8Gi"
CPU="4"
TASK_TIMEOUT="3600"   # 1 hora
MAX_RETRIES="1"

# INBOX no necesita paralelismo (volumen reducido ~5 compañías)
PARALLELISM="1"
TASKS="1"

# ETL_MODE para el Cloud Run Job (INBOX)
ETL_MODE="inbox"

# Scheduler: cada 6 horas y 15 minutos (Job2 corre después del Job1 que es cada 6 horas)
SCHEDULE_NAME="etl-inbox-json2bq-schedule"
SCHEDULE_CRON="15 */6 * * *"

# =============================================================================
# RESUMEN DE CONFIGURACIÓN
# =============================================================================

echo ""
echo "🚀 Build & Deploy — ETL-INBOX-JSON2BQ-JOB (Modo: ${ETL_MODE^^})"
echo "=================================================================="
echo "📋 Proyecto   : ${PROJECT_ID}"
echo "📋 Job Name   : ${JOB_NAME}"
echo "📋 Región     : ${REGION}"
echo "📋 Imagen     : ${IMAGE_TAG}"
echo "📋 SA         : ${SERVICE_ACCOUNT}"
echo "📋 Memoria    : ${MEMORY}"
echo "📋 CPU        : ${CPU}"
echo "📋 Timeout    : ${TASK_TIMEOUT}s ($(( TASK_TIMEOUT / 3600 )) hora(s))"
echo "📋 Paralelismo: Sin paralelismo (1 tarea — volumen reducido INBOX)"
echo "📋 ETL_MODE   : ${ETL_MODE}"
echo "📋 Scheduler  : ${SCHEDULE_CRON} (cada 6h y 15min)"
echo ""

# =============================================================================
# VERIFICACIONES PREVIAS
# =============================================================================

if [ ! -f "main.py" ]; then
    echo "❌ Error: main.py no encontrado."
    echo "   Ejecuta este script desde el directorio json2bq-job/"
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
# PASO 1: BUILD (imagen Docker unificada)
# =============================================================================

echo ""
echo "🔨 PASO 1: BUILD (Creando imagen Docker)"
echo "=========================================="
echo "ℹ️  Usando el Dockerfile estándar (mismo que build_deploy.sh)"
echo "    El modo INBOX se activa por la variable ETL_MODE=${ETL_MODE}"
echo ""
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
        --set-env-vars "${ENV_VARS}"
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
        --set-env-vars "${ENV_VARS}"
fi

if [ $? -eq 0 ]; then
    echo "✅ Job creado/actualizado!"
else
    echo "❌ Error creando/actualizando job"
    exit 1
fi

# =============================================================================
# PASO 3: SCHEDULER
# INBOX: Corre cada 6 horas y 15 minutos, siempre (es el job oficial de INBOX)
# =============================================================================

echo ""
echo "⏰ PASO 3: CONFIGURAR SCHEDULER"
echo "================================"

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
        --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform \
        --time-zone="America/New_York"
else
    echo "🆕 Scheduler no existe — creando..."
    gcloud scheduler jobs create http "${SCHEDULE_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --schedule="${SCHEDULE_CRON}" \
        --uri="${JOB_URI}" \
        --http-method=POST \
        --oauth-service-account-email="${SERVICE_ACCOUNT}" \
        --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform \
        --time-zone="America/New_York"
fi

if [ $? -eq 0 ]; then
    echo "✅ Scheduler INBOX configurado: ${SCHEDULE_CRON} (cada 6h 15min)"
else
    echo "⚠️  Error configurando scheduler. Verifica que Cloud Scheduler API esté habilitada."
fi

# =============================================================================
# RESUMEN FINAL
# =============================================================================

echo ""
echo "🎉 ¡DEPLOY INBOX COMPLETADO!"
echo "============================="
echo ""
echo "📋 ETL_MODE   : ${ETL_MODE^^}"
echo "💾 Recursos   : ${MEMORY} / ${CPU} CPU / ${TASK_TIMEOUT}s"
echo "⏰ Scheduler  : ${SCHEDULE_CRON}"
echo "📊 Fuente     : pph-inbox.settings.companies"
echo ""
echo "📊 Ejecutar el Job manualmente:"
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔧 Ver logs:"
echo "   gcloud logging read \"resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}\" --limit=50 --format=\"table(timestamp,severity,textPayload)\" --project=${PROJECT_ID}"
echo ""
echo "⏰ Ver detalles del Scheduler:"
echo "   gcloud scheduler jobs describe ${SCHEDULE_NAME} --location=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🧪 Modo TEST desde Cloud Shell (sin desplegar):"
echo "   python main.py --mode test --company-id 1 --endpoint locations --dry-run"
echo ""
echo "📝 Notas INBOX:"
echo "   - Procesa compañías candidatas al consorcio (~5 compañías en evaluación)"
echo "   - Fuente de datos: pph-inbox.settings.companies"
echo "   - Sin paralelismo (volumen reducido vs ALL con 30+ compañías)"
echo "   - Mismo código que ALL — diferenciado solo por ETL_MODE=inbox"
echo ""
