#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA ETL-ST2JSON-JOB (Cloud Run Job)
# Multi-Environment: DES, QUA, PRO
# =============================================================================

set -e  # Salir si hay algún error

# =============================================================================
# CONFIGURACIÓN DE AMBIENTES
# =============================================================================

# Detectar proyecto activo de gcloud
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)

# Si se proporciona parámetro, usarlo; si no, detectar automáticamente
if [ -n "$1" ]; then
    # Parámetro proporcionado explícitamente
    ENVIRONMENT="$1"
    ENVIRONMENT=$(echo "$ENVIRONMENT" | tr '[:upper:]' '[:lower:]')  # Convertir a minúsculas
    
    # Validar ambiente
    if [[ ! "$ENVIRONMENT" =~ ^(des|qua|pro)$ ]]; then
        echo "❌ Error: Ambiente inválido '$ENVIRONMENT'"
        echo "Uso: ./build_deploy.sh [des|qua|pro]"
        echo ""
        echo "Ejemplos:"
        echo "  ./build_deploy.sh des    # Deploy en DES (platform-partners-des)"
        echo "  ./build_deploy.sh qua    # Deploy en QUA (platform-partners-qua)"
        echo "  ./build_deploy.sh pro    # Deploy en PRO (platform-partners-pro)"
        echo ""
        echo "O ejecuta sin parámetros para usar el proyecto activo de gcloud"
        exit 1
    fi
else
    # Detectar automáticamente según el proyecto activo
    echo "🔍 Detectando ambiente desde proyecto activo de gcloud..."
    
    case "$CURRENT_PROJECT" in
        platform-partners-des)
            ENVIRONMENT="des"
            echo "✅ Detectado: DES (platform-partners-des)"
            ;;
        platform-partners-qua)
            ENVIRONMENT="qua"
            echo "✅ Detectado: QUA (platform-partners-qua)"
            ;;
        platform-partners-pro)
            ENVIRONMENT="pro"
            echo "✅ Detectado: PRO (platform-partners-pro)"
            ;;
        *)
            echo "⚠️  Proyecto activo: ${CURRENT_PROJECT}"
            echo "⚠️  No se reconoce el proyecto. Usando DES por defecto."
            ENVIRONMENT="des"
            ;;
    esac
fi

# Configuración según ambiente
case "$ENVIRONMENT" in
    des)
        PROJECT_ID="platform-partners-des"
        JOB_NAME="etl-st2json-job"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-des.iam.gserviceaccount.com"
        ;;
    qua)
        PROJECT_ID="platform-partners-qua"
        JOB_NAME="etl-st2json-job"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-qua.iam.gserviceaccount.com"
        ;;
    pro)
        PROJECT_ID="platform-partners-pro"
        JOB_NAME="etl-st2json-job"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-pro.iam.gserviceaccount.com"
        ;;
esac

REGION="us-east1"
IMAGE_NAME="etl-st2json"
IMAGE_TAG="gcr.io/${PROJECT_ID}/${IMAGE_NAME}"
MEMORY="2Gi"
CPU="2"
MAX_RETRIES="1"
TASK_TIMEOUT="1800"

echo "🚀 Iniciando Build & Deploy para ETL-ST2JSON-JOB"
echo "=================================================="
echo "🌍 AMBIENTE: ${ENVIRONMENT^^}"
echo "📋 Configuración:"
echo "   Proyecto: ${PROJECT_ID}"
echo "   Job Name: ${JOB_NAME}"
echo "   Región: ${REGION}"
echo "   Imagen: ${IMAGE_TAG}"
echo "   Service Account: ${SERVICE_ACCOUNT}"
echo "   Memoria: ${MEMORY}"
echo "   CPU: ${CPU}"
echo "   Timeout: ${TASK_TIMEOUT}s"
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "servicetitan_all_st_to_json.py" ]; then
    echo "❌ Error: servicetitan_all_st_to_json.py no encontrado."
    echo "   Ejecuta este script desde el directorio st2json-job/"
    exit 1
fi

# Verificar que gcloud está configurado
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI no está instalado o no está en el PATH"
    exit 1
fi

# Verificar proyecto activo
CURRENT_PROJECT=$(gcloud config get-value project)
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    echo "⚠️  Proyecto actual: ${CURRENT_PROJECT}"
    echo "🔧 Configurando proyecto a: ${PROJECT_ID}"
    gcloud config set project ${PROJECT_ID}
fi

echo ""
echo "🔨 PASO 1: BUILD (Creando imagen Docker)"
echo "=========================================="
gcloud builds submit --tag ${IMAGE_TAG}

if [ $? -eq 0 ]; then
    echo "✅ Build exitoso!"
else
    echo "❌ Error en el build"
    exit 1
fi

echo ""
echo "🚀 PASO 2: DEPLOY (Actualizando Cloud Run Job)"
echo "================================================"
gcloud run jobs update ${JOB_NAME} \
    --image ${IMAGE_TAG} \
    --region ${REGION} \
    --project ${PROJECT_ID} \
    --service-account ${SERVICE_ACCOUNT} \
    --memory ${MEMORY} \
    --cpu ${CPU} \
    --max-retries ${MAX_RETRIES} \
    --task-timeout ${TASK_TIMEOUT}

if [ $? -eq 0 ]; then
    echo "✅ Deploy exitoso!"
else
    echo "❌ Error en el deploy"
    exit 1
fi

echo ""
echo "🎉 ¡DEPLOY COMPLETADO EXITOSAMENTE!"
echo "===================================="
echo ""
echo "🌍 AMBIENTE: ${ENVIRONMENT^^}"
echo "📊 Para ejecutar el Job:"
echo "   gcloud run jobs execute ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔧 Para ver logs del último Job:"
echo "   gcloud logging read \"resource.type=cloud_run_job AND resource.labels.job_name=${JOB_NAME}\" --limit=50 --format=\"table(timestamp,severity,textPayload)\" --project=${PROJECT_ID}"
echo ""
echo "📋 Para ver detalles del Job:"
echo "   gcloud run jobs describe ${JOB_NAME} --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔄 Para deploy en otros ambientes:"
echo "   ./build_deploy.sh des    # Deploy en DES (desarrollo)"
echo "   ./build_deploy.sh qua    # Deploy en QUA (validación)"
echo "   ./build_deploy.sh pro    # Deploy en PRO (producción)"
echo ""
echo "📝 Notas:"
echo "   - DES: Para desarrollo y testing"
echo "   - QUA: Para validación y QA"
echo "   - PRO: Para producción con datos reales"
echo ""

