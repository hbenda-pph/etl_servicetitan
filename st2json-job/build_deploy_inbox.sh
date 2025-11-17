#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA ETL-INBOX-ST2JSON-JOB (Cloud Run Job)
# Proyecto específico: pph-inbox
# =============================================================================

set -e  # Salir si hay algún error

# =============================================================================
# CONFIGURACIÓN PARA PROYECTO INBOX
# =============================================================================

PROJECT_ID="pph-inbox"
JOB_NAME="etl-inbox-st2json-job"
SERVICE_ACCOUNT="etl-servicetitan@pph-inbox.iam.gserviceaccount.com"
REGION="us-east1"
IMAGE_NAME="etl-inbox-st2json"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"
MEMORY="1Gi"
CPU="1"
MAX_RETRIES="1"
TASK_TIMEOUT="1200"
SCHEDULE_NAME="etl-inbox-st2json-schedule"
SCHEDULE_CRON="0 */6 * * *"

echo "🚀 Iniciando Build & Deploy para ETL-INBOX-ST2JSON-JOB"
echo "========================================================"
echo "📋 Configuración:"
echo "   Proyecto: ${PROJECT_ID}"
echo "   Job Name: ${JOB_NAME}"
echo "   Región: ${REGION}"
echo "   Imagen: ${IMAGE_TAG}"
echo "   Service Account: ${SERVICE_ACCOUNT}"
echo "   Memoria: ${MEMORY}"
echo "   CPU: ${CPU}"
echo "   Timeout: ${TASK_TIMEOUT}s"
echo "   Schedule: ${SCHEDULE_CRON} (cada 6 horas)"
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "servicetitan_inbox_st_to_json.py" ]; then
    echo "❌ Error: servicetitan_inbox_st_to_json.py no encontrado."
    echo "   Ejecuta este script desde el directorio st2json-job/"
    exit 1
fi

# Verificar que gcloud está configurado
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI no está instalado o no está en el PATH"
    exit 1
fi

# Verificar proyecto activo
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    echo "⚠️  Proyecto actual: ${CURRENT_PROJECT}"
    echo "🔧 Configurando proyecto a: ${PROJECT_ID}"
    gcloud config set project ${PROJECT_ID}
fi

echo ""
echo "🔨 PASO 1: BUILD (Creando imagen Docker)"
echo "=========================================="
echo "📝 Usando Dockerfile con script: servicetitan_inbox_st_to_json.py"
gcloud builds submit --tag ${IMAGE_TAG} --build-arg SCRIPT_NAME=servicetitan_inbox_st_to_json.py

if [ $? -eq 0 ]; then
    echo "✅ Build exitoso!"
else
    echo "❌ Error en el build"
    exit 1
fi

echo ""
echo "🚀 PASO 2: DEPLOY (Creando/Actualizando Cloud Run Job)"
echo "======================================================="

# Intentar actualizar el job existente, si no existe, crearlo
if gcloud run jobs describe ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID} &>/dev/null; then
    echo "📝 Job existente encontrado. Actualizando..."
    gcloud run jobs update ${JOB_NAME} \
        --image ${IMAGE_TAG} \
        --region ${REGION} \
        --project ${PROJECT_ID} \
        --service-account ${SERVICE_ACCOUNT} \
        --memory ${MEMORY} \
        --cpu ${CPU} \
        --max-retries ${MAX_RETRIES} \
        --task-timeout ${TASK_TIMEOUT}
else
    echo "📝 Job no existe. Creando nuevo job..."
    gcloud run jobs create ${JOB_NAME} \
        --image ${IMAGE_TAG} \
        --region ${REGION} \
        --project ${PROJECT_ID} \
        --service-account ${SERVICE_ACCOUNT} \
        --memory ${MEMORY} \
        --cpu ${CPU} \
        --max-retries ${MAX_RETRIES} \
        --task-timeout ${TASK_TIMEOUT}
fi

if [ $? -eq 0 ]; then
    echo "✅ Deploy exitoso!"
else
    echo "❌ Error en el deploy"
    exit 1
fi

echo ""
echo "⏰ PASO 3: CONFIGURAR SCHEDULE (Cloud Scheduler)"
echo "=================================================="

# Crear o actualizar el schedule
# Nota: Cloud Scheduler necesita usar la API REST de Cloud Run Jobs
JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

if gcloud scheduler jobs describe ${SCHEDULE_NAME} --location=${REGION} --project=${PROJECT_ID} &>/dev/null; then
    echo "📝 Schedule existente encontrado. Actualizando..."
    gcloud scheduler jobs update http ${SCHEDULE_NAME} \
        --location=${REGION} \
        --project=${PROJECT_ID} \
        --schedule="${SCHEDULE_CRON}" \
        --uri="${JOB_URI}" \
        --http-method=POST \
        --oidc-service-account-email=${SERVICE_ACCOUNT} \
        --time-zone="America/New_York"
else
    echo "📝 Schedule no existe. Creando nuevo schedule..."
    gcloud scheduler jobs create http ${SCHEDULE_NAME} \
        --location=${REGION} \
        --project=${PROJECT_ID} \
        --schedule="${SCHEDULE_CRON}" \
        --uri="${JOB_URI}" \
        --http-method=POST \
        --oidc-service-account-email=${SERVICE_ACCOUNT} \
        --time-zone="America/New_York"
fi

if [ $? -eq 0 ]; then
    echo "✅ Schedule configurado exitosamente!"
else
    echo "⚠️  Advertencia: Error configurando schedule (puede que Cloud Scheduler API no esté habilitada)"
    echo "   Puedes configurarlo manualmente más tarde"
fi

echo ""
echo "🎉 ¡DEPLOY COMPLETADO EXITOSAMENTE!"
echo "===================================="
echo ""
echo "📊 Para ejecutar el Job:"
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔧 Para ver logs del último Job:"
echo "   gcloud logging read \"resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}\" --limit=50 --format=\"table(timestamp,severity,textPayload)\" --project=${PROJECT_ID}"
echo ""
echo "📋 Para ver detalles del Job:"
echo "   gcloud run jobs describe ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "⏰ Para ver detalles del Schedule:"
echo "   gcloud scheduler jobs describe ${SCHEDULE_NAME} --location=${REGION} --project=${PROJECT_ID}"
echo ""
echo "📝 Notas:"
echo "   - Este job procesa compañías INBOX desde la tabla companies_inbox"
echo "   - Los datos se guardan en el proyecto pph-inbox"
echo "   - Script: servicetitan_inbox_st_to_json.py"
echo "   - Ejecución automática: cada 6 horas (${SCHEDULE_CRON})"
echo "   - Recursos reducidos: ${MEMORY} memoria, ${CPU} CPU (una compañía nueva con pocos registros)"
echo ""

