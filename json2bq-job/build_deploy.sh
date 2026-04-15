#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA ETL-JSON2BQ-JOB (Cloud Run Job)
# Multi-Environment: DEV, QUA, PRO
# Modo ETL: ALL (consorcio — pph-central)
# =============================================================================
#
# Uso:
#   ./build_deploy.sh [dev|qua|pro]
#   ./build_deploy.sh        # Detecta el ambiente desde el proyecto activo de gcloud
#
# Recursos por ambiente (según proceso_etl.csv):
#   PRO: 16GB / 8 CPU / 2 horas  | Paralelismo 3 tasks | Scheduler: en el Orquestador
#   QUA: 16GB / 8 CPU / 2 horas  | Paralelismo 3 tasks | Scheduler: 1 corrida (próxima hora + 15 min)
#   DEV: 16GB / 8 CPU / 2 horas  | Sin paralelismo     | Sin Scheduler (manual en Cloud Shell)
#
# =============================================================================

set -e  # Salir si hay algún error

# =============================================================================
# ARRAY DE RECURSOS POR AMBIENTE
# Formato: MEMORY CPU TASK_TIMEOUT_SECONDS PARALLELISM TASKS
# =============================================================================

declare -A RESOURCES_MEMORY=(      [dev]="16Gi"  [qua]="16Gi"  [pro]="16Gi"  )
declare -A RESOURCES_CPU=(         [dev]="8"      [qua]="8"      [pro]="8"     )
declare -A RESOURCES_TIMEOUT=(     [dev]="7200"   [qua]="7200"   [pro]="7200"  )  # 2 horas
declare -A RESOURCES_PARALLELISM=( [dev]="1"      [qua]="3"      [pro]="3"     )
declare -A RESOURCES_TASKS=(       [dev]="1"      [qua]="3"      [pro]="3"     )

# =============================================================================
# DETECCIÓN / VALIDACIÓN DE AMBIENTE
# =============================================================================

CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)

if [ -n "$1" ]; then
    ENVIRONMENT=$(echo "$1" | tr '[:upper:]' '[:lower:]')

    if [[ ! "$ENVIRONMENT" =~ ^(dev|qua|pro)$ ]]; then
        echo "❌ Ambiente inválido: '$ENVIRONMENT'"
        echo "Uso: ./build_deploy.sh [dev|qua|pro]"
        echo ""
        echo "Ejemplos:"
        echo "  ./build_deploy.sh dev    # DEV (platform-partners-des)"
        echo "  ./build_deploy.sh qua    # QUA (platform-partners-qua)"
        echo "  ./build_deploy.sh pro    # PRO (constant-height-455614-i0)"
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

# =============================================================================
# CONFIGURACIÓN POR AMBIENTE
# =============================================================================

case "$ENVIRONMENT" in
    dev)
        PROJECT_ID="platform-partners-des"
        PROJECT_NAME="platform-partners-des"
        JOB_NAME="etl-json2bq-job-dev"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-des.iam.gserviceaccount.com"
        ;;
    qua)
        PROJECT_ID="platform-partners-qua"
        PROJECT_NAME="platform-partners-qua"
        JOB_NAME="etl-json2bq-job-qua"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-qua.iam.gserviceaccount.com"
        ;;
    pro)
        PROJECT_ID="constant-height-455614-i0"
        PROJECT_NAME="platform-partners-pro"
        JOB_NAME="etl-json2bq-job"
        SERVICE_ACCOUNT="etl-servicetitan@${PROJECT_ID}.iam.gserviceaccount.com"
        ;;
esac

# Leer recursos del array según ambiente
MEMORY="${RESOURCES_MEMORY[$ENVIRONMENT]}"
CPU="${RESOURCES_CPU[$ENVIRONMENT]}"
TASK_TIMEOUT="${RESOURCES_TIMEOUT[$ENVIRONMENT]}"
PARALLELISM="${RESOURCES_PARALLELISM[$ENVIRONMENT]}"
TASKS="${RESOURCES_TASKS[$ENVIRONMENT]}"

REGION="us-east1"
IMAGE_NAME="etl-json2bq"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"
MAX_RETRIES="1"

# ETL_MODE para el Cloud Run Job (ALL)
ETL_MODE="all"

# =============================================================================
# RESUMEN DE CONFIGURACIÓN
# =============================================================================

echo ""
echo "🚀 Build & Deploy — ETL-JSON2BQ-JOB (Modo: ${ETL_MODE^^})"
echo "============================================================"
echo "🌍 AMBIENTE   : ${ENVIRONMENT^^}"
echo "📋 Proyecto ID: ${PROJECT_ID}"
echo "📋 Project Name: ${PROJECT_NAME}"
echo "📋 Job Name   : ${JOB_NAME}"
echo "📋 Región     : ${REGION}"
echo "📋 Imagen     : ${IMAGE_TAG}"
echo "📋 SA         : ${SERVICE_ACCOUNT}"
echo "📋 Memoria    : ${MEMORY}"
echo "📋 CPU        : ${CPU}"
echo "📋 Timeout    : ${TASK_TIMEOUT}s ($(( TASK_TIMEOUT / 3600 )) hora(s))"
if [ "$TASKS" != "1" ]; then
    echo "📋 Paralelismo: ${PARALLELISM} simultáneas / ${TASKS} tareas totales"
else
    echo "📋 Paralelismo: Sin paralelismo (1 tarea)"
fi
echo "📋 ETL_MODE   : ${ETL_MODE}"
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

# Construir flags de paralelismo
PARALLEL_FLAGS=""
if [ "$TASKS" != "1" ]; then
    PARALLEL_FLAGS="--parallelism ${PARALLELISM} --tasks ${TASKS}"
fi

# Variables de entorno del job
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

# =============================================================================
# PASO 3: SCHEDULER
#
# PRO  → el Scheduler lo gestiona el Orquestador externo.
#         Este script solo verifica que no haya un scheduler huérfano.
# QUA  → 1 sola corrida de prueba (próxima hora en punto + 15 min)
# DEV  → Sin scheduler. Se ejecuta manualmente.
# =============================================================================

echo ""
echo "⏰ PASO 3: CONFIGURAR SCHEDULER"
echo "================================"

SCHEDULE_NAME="etl-json2bq-schedule"

case "$ENVIRONMENT" in
    pro)
        echo "ℹ️  PRO: El Scheduler está gestionado por el Orquestador."
        echo "   Este script no crea ni modifica el Scheduler en PRO."
        echo "   Verifica que el Orquestador esté configurado correctamente."
        ;;

    qua)
        # QUA: 1 corrida de prueba. Cron para "próxima hora en punto + 15 min".
        # Se calcula dinámicamente para lanzar en ~75 minutos desde ahora.
        CURRENT_HOUR=$(date -u +"%H")
        NEXT_HOUR=$(( (CURRENT_HOUR + 1) % 24 ))
        SCHEDULE_CRON="15 ${NEXT_HOUR} * * *"

        echo "⏰ QUA: Configurando scheduler para 1 corrida de prueba..."
        echo "   Cron: '${SCHEDULE_CRON}' (aprox. próxima hora + 15 min)"

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
            echo "✅ Scheduler QUA configurado (1 corrida de prueba: ${SCHEDULE_CRON})"
        else
            echo "⚠️  Error configurando scheduler. Verifica que Cloud Scheduler API esté habilitada."
        fi
        ;;

    dev)
        echo "ℹ️  DEV: Sin Scheduler. El job se ejecuta manualmente desde Cloud Shell."
        if gcloud scheduler jobs describe "${SCHEDULE_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
            echo "⚠️  Se encontró un scheduler en DEV — eliminando..."
            gcloud scheduler jobs delete "${SCHEDULE_NAME}" \
                --location="${REGION}" --project="${PROJECT_ID}" --quiet 2>/dev/null || true
            echo "✅ Scheduler eliminado."
        else
            echo "✅ No hay scheduler en DEV (correcto)."
        fi
        ;;
esac

# =============================================================================
# RESUMEN FINAL
# =============================================================================

echo ""
echo "🎉 ¡DEPLOY COMPLETADO!"
echo "======================"
echo ""
echo "🌍 AMBIENTE : ${ENVIRONMENT^^}"
echo "📋 ETL_MODE : ${ETL_MODE^^}"
echo "💾 Recursos : ${MEMORY} / ${CPU} CPU / ${TASK_TIMEOUT}s"
echo ""
echo "📊 Ejecutar el Job manualmente:"
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔧 Ver logs:"
echo "   gcloud logging read \"resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}\" --limit=50 --format=\"table(timestamp,severity,textPayload)\" --project=${PROJECT_ID}"
echo ""
echo "🧪 Modo TEST desde Cloud Shell (sin desplegar):"
echo "   python main.py --mode test --company-id 1 --endpoint locations --dry-run"
echo ""
echo "🔄 Deploy en otros ambientes:"
echo "   ./build_deploy.sh dev"
echo "   ./build_deploy.sh qua"
echo "   ./build_deploy.sh pro"
echo ""
