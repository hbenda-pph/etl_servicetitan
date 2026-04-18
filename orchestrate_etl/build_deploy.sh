#!/bin/bash

# =============================================================================
# SCRIPT DE BUILD & DEPLOY PARA ORCHESTRATE-ETL-JOBS (Cloud Function)
# Multi-Environment: DEV, QUA, PRO
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
    if [[ ! "$ENVIRONMENT" =~ ^(dev|qua|pro)$ ]]; then
        echo "❌ Error: Ambiente inválido '$ENVIRONMENT'"
        echo "Uso: ./build_deploy.sh [dev|qua|pro]"
        exit 1
    fi
else
    # Detectar automáticamente según el proyecto activo
    echo "🔍 Detectando ambiente desde proyecto activo de gcloud..."
    
    case "$CURRENT_PROJECT" in
        platform-partners-des)
            ENVIRONMENT="dev"
            echo "✅ Detectado: DEV (platform-partners-des)"
            ;;
        platform-partners-qua)
            ENVIRONMENT="qua"
            echo "✅ Detectado: QUA (platform-partners-qua)"
            ;;
        constant-height-455614-i0|platform-partners-pro)
            ENVIRONMENT="pro"
            echo "✅ Detectado: PRO"
            ;;
        *)
            echo "⚠️  Proyecto activo: ${CURRENT_PROJECT}"
            echo "⚠️  No se reconoce el proyecto. Usando DEV por defecto."
            ENVIRONMENT="dev"
            ;;
    esac
fi

# Configuración según ambiente
case "$ENVIRONMENT" in
    dev)
        PROJECT_ID="platform-partners-des"
        FUNCTION_NAME="orchestrate-etl-jobs-dev"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-des.iam.gserviceaccount.com"
        ;;
    qua)
        PROJECT_ID="platform-partners-qua"
        FUNCTION_NAME="orchestrate-etl-jobs-qua"
        SERVICE_ACCOUNT="etl-servicetitan@platform-partners-qua.iam.gserviceaccount.com"
        ;;
    pro)
        PROJECT_ID="constant-height-455614-i0"  # Project ID real de PRO
        PROJECT_NAME="platform-partners-pro"     # Project name (para variable de entorno)
        FUNCTION_NAME="orchestrate-etl-jobs"
        SERVICE_ACCOUNT="etl-servicetitan@${PROJECT_ID}.iam.gserviceaccount.com"
        ;;
esac

# Para DEV y QUA, PROJECT_NAME = PROJECT_ID (son iguales)
if [ -z "$PROJECT_NAME" ]; then
    PROJECT_NAME="${PROJECT_ID}"
fi

# GCP_PROJECT para variable de entorno: usar PROJECT_NAME para que Python detecte correctamente
GCP_PROJECT_ENV="${PROJECT_NAME}"

REGION="us-east1"
MEMORY="4Gi"
TIMEOUT="3600s"  # 1 hora

echo "🚀 Iniciando Build & Deploy para ORCHESTRATE-ETL-JOBS"
echo "======================================================"
echo "🌍 AMBIENTE: ${ENVIRONMENT^^}"
echo "📋 Configuración:"
echo "   Proyecto ID: ${PROJECT_ID}"
if [ "$ENVIRONMENT" = "pro" ]; then
    echo "   Proyecto Name: ${PROJECT_NAME}"
fi
echo "   Function Name: ${FUNCTION_NAME}"
echo "   Región: ${REGION}"
echo "   Service Account: ${SERVICE_ACCOUNT}"
echo "   Memoria: ${MEMORY}"
echo "   Timeout: ${TIMEOUT}"
echo ""

# Verificar que estamos en el directorio correcto
if [ ! -f "main.py" ]; then
    echo "❌ Error: main.py no encontrado."
    echo "   Ejecuta este script desde el directorio orchestrate_etl/"
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
echo "🚀 PASO 1: DEPLOY FUNCTION"
echo "=========================="

# Construir comando de deploy
DEPLOY_CMD="gcloud functions deploy ${FUNCTION_NAME} \
  --gen2 \
  --project ${PROJECT_ID} \
  --runtime python311 \
  --trigger-http \
  --entry-point orchestrate_etl_jobs \
  --source . \
  --region ${REGION} \
  --service-account ${SERVICE_ACCOUNT} \
  --memory ${MEMORY} \
  --timeout ${TIMEOUT} \
  --set-env-vars GCP_PROJECT=${GCP_PROJECT_ENV} \
  --allow-unauthenticated"

eval ${DEPLOY_CMD}

if [ $? -eq 0 ]; then
    echo "✅ Function desplegada exitosamente!"
else
    echo "❌ Error desplegando function"
    exit 1
fi

echo ""
echo "⏰ PASO 2: CONFIGURAR SCHEDULER (Solo para PRO)"
echo "================================================"

SCHEDULE_NAME="orchestrate-etl-jobs-schedule"
SCHEDULE_CRON="0 5,11,17,23 * * *"  # A las 5am, 11am, 5pm, 11pm

if [ "$ENVIRONMENT" = "pro" ]; then
    # Obtener URL pública de la función (igual que en DEV)
    FUNCTION_URL=$(gcloud functions describe ${FUNCTION_NAME} --gen2 --region=${REGION} --project=${PROJECT_ID} --format="value(serviceConfig.uri)" 2>/dev/null || echo "")
    
    if [ -z "$FUNCTION_URL" ]; then
        echo "⚠️  No se pudo obtener la URL de la función. Usando formato estándar."
        FUNCTION_URL="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"
    fi
    
    # Solo en producción: crear/actualizar scheduler
    # Usar oidcToken (como en DEV) en lugar de oauthToken para poder usar URL pública
    if gcloud scheduler jobs describe ${SCHEDULE_NAME} --location=${REGION} --project=${PROJECT_ID} &>/dev/null; then
        echo "📝 Scheduler existe, actualizando..."
        gcloud scheduler jobs update http ${SCHEDULE_NAME} \
            --location=${REGION} \
            --project=${PROJECT_ID} \
            --schedule="${SCHEDULE_CRON}" \
            --time-zone="America/Mexico_City" \
            --uri="${FUNCTION_URL}" \
            --http-method=POST \
            --attempt-deadline=1800s \
            --oidc-service-account-email=${SERVICE_ACCOUNT}
    else
        echo "🆕 Scheduler no existe, creando..."
        gcloud scheduler jobs create http ${SCHEDULE_NAME} \
            --location=${REGION} \
            --project=${PROJECT_ID} \
            --schedule="${SCHEDULE_CRON}" \
            --time-zone="America/Mexico_City" \
            --uri="${FUNCTION_URL}" \
            --http-method=POST \
            --attempt-deadline=1800s \
            --oidc-service-account-email=${SERVICE_ACCOUNT}
    fi
    
    if [ $? -eq 0 ]; then
        echo "✅ Scheduler configurado exitosamente (cada 6 horas)"
    else
        echo "⚠️  Advertencia: Error configurando scheduler (puede que Cloud Scheduler API no esté habilitada)"
    fi
else
    # En dev/qua: desactivar o eliminar scheduler si existe
    if gcloud scheduler jobs describe ${SCHEDULE_NAME} --location=${REGION} --project=${PROJECT_ID} &>/dev/null; then
        echo "⚠️  Scheduler encontrado en ambiente ${ENVIRONMENT^^}. Desactivando..."
        gcloud scheduler jobs pause ${SCHEDULE_NAME} --location=${REGION} --project=${PROJECT_ID} 2>/dev/null || \
        gcloud scheduler jobs delete ${SCHEDULE_NAME} --location=${REGION} --project=${PROJECT_ID} --quiet 2>/dev/null
        echo "✅ Scheduler desactivado/eliminado (no debe ejecutarse en ${ENVIRONMENT^^})"
    else
        echo "✅ No hay scheduler configurado (correcto para ambiente ${ENVIRONMENT^^})"
    fi
fi

echo ""
echo "🎉 ¡DEPLOY COMPLETADO EXITOSAMENTE!"
echo "===================================="
echo ""
echo "🌍 AMBIENTE: ${ENVIRONMENT^^}"
echo "📊 Para ejecutar la función manualmente:"
echo "   gcloud functions call ${FUNCTION_NAME} --gen2 --region=${REGION} --project=${PROJECT_ID}"
echo ""
echo "🔧 Para ver logs:"
echo "   gcloud logging read \"resource.type=cloud_function AND resource.labels.function_name=${FUNCTION_NAME}\" --limit=50 --format=\"table(timestamp,severity,textPayload)\" --project=${PROJECT_ID}"
echo ""
echo "📋 Para ver detalles de la función:"
echo "   gcloud functions describe ${FUNCTION_NAME} --gen2 --region=${REGION} --project=${PROJECT_ID}"
echo ""
if [ "$ENVIRONMENT" = "pro" ]; then
    echo "⚠️  IMPORTANTE: Desactiva los schedules individuales de los jobs:"
    echo "   gcloud scheduler jobs pause etl-st2json-schedule --location=${REGION} --project=${PROJECT_ID}"
    echo "   gcloud scheduler jobs pause etl-json2bq-schedule --location=${REGION} --project=${PROJECT_ID}"
fi
echo ""
