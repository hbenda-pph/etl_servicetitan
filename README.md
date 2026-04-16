# ETL ServiceTitan - BigQuery

Sistema ETL completo para extraer datos de ServiceTitan API y cargarlos a BigQuery con campos de auditoría y Soft Delete.

## 🏗️ Arquitectura

```
Cloud Function (orchestrate-etl-jobs)
    ↓ Invoca secuencialmente
Cloud Run Job #1 (etl-st2json-job) 
    ↓ Extrae datos de ServiceTitan API → JSON
Cloud Run Job #2 (etl-json2bq-job)
    ↓ Procesa JSON → BigQuery con campos ETL
```

## 📋 Componentes

### 1. **Cloud Function (Orquestador)**
- **Nombre**: `orchestrate-etl-jobs`
- **Función**: Coordina la ejecución secuencial de ambos Jobs
- **Timeout**: 3600s (1 hora)
- **Memoria**: 4Gi
- **Región**: us-east1

### 2. **Cloud Run Job #1 (Extracción)**
- **Nombre**: `etl-st2json-job`
- **Función**: Extrae datos de ServiceTitan API y los convierte a JSON
- **Timeout**: 7200s (2 horas)
- **Memoria**: 1Gi
- **CPU**: 2 cores
- **Tiempo típico**: 7-18 minutos

### 3. **Cloud Run Job #2 (Procesamiento)**
- **Nombre**: `etl-json2bq-job`
- **Función**: Procesa archivos JSON y los carga a BigQuery con campos ETL
- **Timeout**: 7200s (2 horas)
- **Memoria**: 1Gi
- **CPU**: 2 cores
- **Tiempo típico**: 23 minutos máximo

## 🔧 Campos ETL Implementados

Todas las tablas incluyen campos de auditoría automáticos:

| Campo | Tipo | Propósito |
|-------|------|-----------|
| `_etl_synced` | TIMESTAMP | Timestamp de la última operación |
| `_etl_operation` | STRING | Tipo de operación: INSERT, UPDATE, DELETE |

## 🚀 Build y Deploy

### ⚡ Método Recomendado: Scripts Automatizados

Cada job tiene su propio script `build_deploy.sh` que maneja automáticamente el build y deploy según el ambiente.

#### Job #1 (Extracción - st2json-job)

```bash
cd st2json-job

# Deploy según ambiente activo de gcloud
./build_deploy.sh

# O especificar ambiente explícitamente
./build_deploy.sh des    # Deploy en DES
./build_deploy.sh qua    # Deploy en QUA
./build_deploy.sh pro    # Deploy en PRO
```

#### Job #2 (Procesamiento - json2bq-job)

```bash
cd json2bq-job

# Deploy según ambiente activo de gcloud
./build_deploy.sh

# O especificar ambiente explícitamente
./build_deploy.sh des    # Deploy en DES
./build_deploy.sh qua    # Deploy en QUA
./build_deploy.sh pro    # Deploy en PRO
```

### 📝 Características de los Scripts

- ✅ **Detección automática de ambiente** basada en proyecto activo de gcloud
- ✅ **Validación de ambiente** (des/qua/pro)
- ✅ **Configuración automática** de service accounts y recursos según ambiente
- ✅ **Build y Deploy en un solo comando**
- ✅ **Mensajes informativos** con comandos útiles post-deploy

### 🔄 Workflow de Deploy Multi-Ambiente

```bash
# 1. Desarrollo (DES)
gcloud config set project platform-partners-des
cd st2json-job && ./build_deploy.sh && cd ..
cd json2bq-job && ./build_deploy.sh && cd ..

# 2. Validación (QUA)
gcloud config set project platform-partners-qua
cd st2json-job && ./build_deploy.sh && cd ..
cd json2bq-job && ./build_deploy.sh && cd ..

# 3. Producción (PRO)
gcloud config set project platform-partners-pro
cd st2json-job && ./build_deploy.sh && cd ..
cd json2bq-job && ./build_deploy.sh && cd ..
```

O especificar ambiente explícitamente:
```bash
cd st2json-job
./build_deploy.sh des  # Desarrollo
./build_deploy.sh qua  # Validación
./build_deploy.sh pro  # Producción
```

### 🔧 Método Manual (Comandos Individuales)

Si prefieres ejecutar los comandos manualmente:

#### Job #1 - Build & Deploy Manual
```bash
cd st2json-job

# Build
gcloud builds submit --tag gcr.io/platform-partners-des/etl-st2json

# Deploy
gcloud run jobs update etl-st2json-job \
  --image gcr.io/platform-partners-des/etl-st2json \
  --region us-east1 \
  --project platform-partners-des \
  --service-account etl-servicetitan@platform-partners-des.iam.gserviceaccount.com \
  --memory 1Gi \
  --cpu 2 \
  --max-retries 1 \
  --task-timeout 1800
```

#### Job #2 - Build & Deploy Manual
```bash
cd json2bq-job

# Build
gcloud builds submit --tag gcr.io/platform-partners-des/etl-json2bq

# Deploy
gcloud run jobs update etl-json2bq-job \
  --image gcr.io/platform-partners-des/etl-json2bq \
  --region us-east1 \
  --project platform-partners-des \
  --service-account etl-servicetitan@platform-partners-des.iam.gserviceaccount.com \
  --memory 1Gi \
  --cpu 2 \
  --max-retries 1 \
  --task-timeout 2400
```

### Función Orquestadora

#### ⚡ Método Recomendado: Script Automatizado

```bash
cd orchestrate_etl

# Deploy según ambiente activo de gcloud
./build_deploy.sh

# O especificar ambiente explícitamente
./build_deploy.sh dev    # Deploy en DEV
./build_deploy.sh qua    # Deploy en QUA
./build_deploy.sh pro    # Deploy en PRO
```

El script `build_deploy.sh`:
- ✅ Detecta automáticamente el ambiente
- ✅ Configura la función con los parámetros correctos
- ✅ Crea/actualiza el scheduler automáticamente (solo en PRO)
- ✅ Establece variables de entorno correctamente
- ✅ Soporta múltiples ambientes (DEV, QUA, PRO)

#### Deploy Manual (Alternativa)

Si prefieres hacer el deploy manualmente:

**DEV:**
```bash
cd orchestrate_etl
gcloud functions deploy orchestrate-etl-jobs-dev \
  --gen2 \
  --project platform-partners-des \
  --runtime python311 \
  --trigger-http \
  --entry-point orchestrate_etl_jobs \
  --source . \
  --region us-east1 \
  --service-account etl-servicetitan@platform-partners-des.iam.gserviceaccount.com \
  --memory 4Gi \
  --timeout 3600s \
  --set-env-vars GCP_PROJECT=platform-partners-des \
  --allow-unauthenticated
```

**PRO:**
```bash
cd orchestrate_etl
gcloud functions deploy orchestrate-etl-jobs \
  --gen2 \
  --project constant-height-455614-i0 \
  --runtime python311 \
  --trigger-http \
  --entry-point orchestrate_etl_jobs \
  --source . \
  --region us-east1 \
  --service-account etl-servicetitan@constant-height-455614-i0.iam.gserviceaccount.com \
  --memory 4Gi \
  --timeout 3600s \
  --set-env-vars GCP_PROJECT=platform-partners-pro \
  --allow-unauthenticated
```

## 📊 Funcionalidades

### ✅ Soft Delete
- Los registros eliminados se marcan con `_etl_operation = 'DELETE'`
- No se borran físicamente de BigQuery
- Permite auditoría completa y recuperación

### ✅ Auditoría Completa
- Tracking de todas las operaciones (INSERT, UPDATE, DELETE)
- Timestamp de cada sincronización
- Trazabilidad completa del proceso ETL

### ✅ Procesamiento Incremental
- MERGE con detección automática de cambios
- Actualización solo de registros modificados
- Eficiencia en el procesamiento

## 🔍 Endpoints Procesados

- business-units
- job-types
- technicians
- employees
- campaigns
- activities
- timesheets

## 🧪 Herramientas de Testing

### Script de Prueba ETL Completo

**`test_etl_single_company.py`** - Prueba el flujo completo (extracción + carga) para una compañía:

```bash
# Editar COMPANY_ID y TEST_ENDPOINTS en el archivo
python test_etl_single_company.py
```

### Notebook Interactivo

**`test_etl_flow.ipynb`** - Versión visual e interactiva:

```bash
jupyter notebook test_etl_flow.ipynb
```

**Uso:** Ideal para probar nuevos endpoints antes de implementarlos en producción.

## 📁 Estructura del Proyecto

```
etl_servicetitan/
├── orchestrate_etl/
│   └── main.py                              # Función orquestadora
├── st2json-job/
│   ├── Dockerfile                           # Configuración Docker
│   ├── requirements.txt                     # Dependencias Python
│   ├── servicetitan_all_st_to_json.py      # Script principal
│   ├── estimates_single_company.py          # Script para estimates individual
│   └── build_deploy.sh                      # Script de build & deploy
├── json2bq-job/
│   ├── Dockerfile                           # Configuración Docker
│   ├── requirements.txt                     # Dependencias Python
│   ├── servicetitan_all_json_to_bigquery.py # Script principal
│   └── build_deploy.sh                      # Script de build & deploy
├── test_etl_single_company.py               # Script de prueba ETL completo
├── test_etl_flow.ipynb                       # Notebook de prueba ETL
└── README.md                                # Documentación completa
```

## 🛠️ Configuración

### Service Account
- **Nombre**: `etl-servicetitan@platform-partners-des.iam.gserviceaccount.com`
- **Permisos**: BigQuery, Cloud Run, Cloud Storage

### Proyectos
- **Orquestación**: `platform-partners-des`
- **Datos**: `shape-mhs-1` (cada compañía tiene su propio proyecto)

### Permisos para Cloud Functions

#### Verificar Cuenta de Servicio
```bash
gcloud iam service-accounts list --filter="email:etl-servicetitan@platform-partners-des.iam.gserviceaccount.com"
```

#### Verificar Jobs Disponibles
```bash
gcloud run jobs list --region=us-east1
```

#### Configurar Permisos Completos
```bash
# Permisos de administrador de Cloud Run
gcloud projects add-iam-policy-binding platform-partners-des \
  --member="serviceAccount:etl-servicetitan@platform-partners-des.iam.gserviceaccount.com" \
  --role="roles/run.admin"

# Permisos para invocar Jobs
gcloud projects add-iam-policy-binding platform-partners-des \
  --member="serviceAccount:etl-servicetitan@platform-partners-des.iam.gserviceaccount.com" \
  --role="roles/run.invoker"
```

## 🔐 Configuración de Permisos para Jobs INBOX (pph-inbox)

### Permisos para Cloud Build (cuenta de servicio de Compute Engine)

```bash
PROJECT_NUMBER=$(gcloud projects describe pph-inbox --format="value(projectNumber)")
SERVICE_ACCOUNT="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

# Habilitar APIs necesarias
gcloud services enable cloudbuild.googleapis.com --project=pph-inbox
gcloud services enable artifactregistry.googleapis.com --project=pph-inbox

# Otorgar roles necesarios para Cloud Build
gcloud projects add-iam-policy-binding pph-inbox \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/cloudbuild.builds.builder"

gcloud projects add-iam-policy-binding pph-inbox \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/storage.admin"

gcloud projects add-iam-policy-binding pph-inbox \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding pph-inbox \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/logging.logWriter"

# Permiso para usar el service account etl-servicetitan (necesario para Cloud Run Jobs)
gcloud iam service-accounts add-iam-policy-binding etl-servicetitan@pph-inbox.iam.gserviceaccount.com \
    --member="serviceAccount:${SERVICE_ACCOUNT}" \
    --role="roles/iam.serviceAccountUser" \
    --project=pph-inbox
```

### Permisos para el Service Account etl-servicetitan

```bash
# Permisos de BigQuery
gcloud projects add-iam-policy-binding pph-inbox \
    --member="serviceAccount:etl-servicetitan@pph-inbox.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

# Permisos de Storage
gcloud projects add-iam-policy-binding pph-inbox \
    --member="serviceAccount:etl-servicetitan@pph-inbox.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

# Permisos en el bucket específico
gcloud storage buckets add-iam-policy-binding gs://pph-inbox_servicetitan \
    --member="serviceAccount:etl-servicetitan@pph-inbox.iam.gserviceaccount.com" \
    --role="roles/storage.admin" \
    --project=pph-inbox

# Permisos para invocar Cloud Run Jobs (necesario para Cloud Scheduler)
gcloud run jobs add-iam-policy-binding etl-inbox-st2json-job \
    --member="serviceAccount:etl-servicetitan@pph-inbox.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --region=us-east1 \
    --project=pph-inbox

gcloud run jobs add-iam-policy-binding etl-inbox-json2bq-job \
    --member="serviceAccount:etl-servicetitan@pph-inbox.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --region=us-east1 \
    --project=pph-inbox
```

### Comandos GCloud para permisos en Cloud Scheduler para Cloud Run Jobs

```bash
# Comando gcloud para IAM (otorgar permiso a nivel de proyecto)
gcloud projects add-iam-policy-binding pph-inbox \
    --member="serviceAccount:etl-servicetitan@pph-inbox.iam.gserviceaccount.com" \
    --role="roles/run.invoker" \
    --condition=None

# Comandos gcloud para Cloud Scheduler (actualizar schedules con OAuth)
gcloud scheduler jobs update http etl-inbox-st2json-schedule \
    --location=us-east1 \
    --uri="https://us-east1-run.googleapis.com/v2/projects/pph-inbox/locations/us-east1/jobs/etl-inbox-st2json-job:run" \
    --oauth-service-account-email="etl-servicetitan@pph-inbox.iam.gserviceaccount.com" \
    --oauth-token-scope=""

gcloud scheduler jobs update http etl-inbox-json2bq-schedule \
    --location=us-east1 \
    --uri="https://us-east1-run.googleapis.com/v2/projects/pph-inbox/locations/us-east1/jobs/etl-inbox-json2bq-job:run" \
    --oauth-service-account-email="etl-servicetitan@pph-inbox.iam.gserviceaccount.com" \
    --oauth-token-scope=""
```

### Comandos útiles para verificar schedules

```bash
# Listar schedules
gcloud scheduler jobs list --location=us-east1 --project=pph-inbox

# Ver detalles de un schedule
gcloud scheduler jobs describe etl-inbox-st2json-schedule --location=us-east1 --project=pph-inbox

# Ver ejecuciones recientes
gcloud scheduler jobs list-executions etl-inbox-st2json-schedule --location=us-east1 --project=pph-inbox

# Ejecutar manualmente (para pruebas)
gcloud scheduler jobs run etl-inbox-st2json-schedule --location=us-east1 --project=pph-inbox
```

## 📈 Monitoreo

### Logs de Cloud Run Jobs
```bash
# Job 1 - Extracción de datos
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=etl-st2json-job" --limit=50 --format="table(timestamp,severity,textPayload)"

# Job 2 - Procesamiento a BigQuery
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=etl-json2bq-job" --limit=50 --format="table(timestamp,severity,textPayload)"
```

### Logs de Cloud Function
```bash
# Función de orquestación
gcloud logging read "resource.type=cloud_function AND resource.labels.function_name=orchestrate-etl-jobs" --limit=50 --format="table(timestamp,severity,textPayload)"
```

### Monitoreo de BigQuery
- **Dataset**: `shape-mhs-1.management`
- **Tabla de logs**: `etl_logs`
- **Campos ETL**: `_etl_synced`, `_etl_operation`

## 🧪 Pruebas y Scheduling

### Pruebas Manuales
```bash
# Probar la función de orquestación directamente
curl -X POST https://us-east1-platform-partners-des.cloudfunctions.net/orchestrate-etl-jobs
```

### Configuración de Scheduler

**Nota:** El script `build_deploy.sh` configura el scheduler automáticamente en PRO. Si necesitas hacerlo manualmente:

**PRO:**
```bash
# Obtener URL de la función
FUNCTION_URL=$(gcloud functions describe orchestrate-etl-jobs --gen2 --region=us-east1 --project=constant-height-455614-i0 --format="value(serviceConfig.uri)")

# Obtener URL pública de la función
FUNCTION_URL=$(gcloud functions describe orchestrate-etl-jobs --gen2 --region=us-east1 --project=constant-height-455614-i0 --format="value(serviceConfig.uri)")

# Crear/actualizar scheduler (cada 6 horas)
# Usar oidcToken (igual que en DEV) para poder usar URL pública
gcloud scheduler jobs create http orchestrate-etl-jobs-schedule \
  --location=us-east1 \
  --project=constant-height-455614-i0 \
  --schedule="0 */6 * * *" \
  --uri="${FUNCTION_URL}" \
  --http-method=POST \
  --oidc-service-account-email=etl-servicetitan@constant-height-455614-i0.iam.gserviceaccount.com
```

**⚠️ IMPORTANTE:** Después de configurar la función de orquestación, **desactiva los schedules individuales** de los jobs:

```bash
# Pausar schedules individuales en PRO
gcloud scheduler jobs pause etl-st2json-schedule --location=us-east1 --project=constant-height-455614-i0
gcloud scheduler jobs pause etl-json2bq-schedule --location=us-east1 --project=constant-height-455614-i0
```

**Comandos útiles:**
```bash
# Verificar jobs de scheduler
gcloud scheduler jobs list --location=us-east1 --project=constant-height-455614-i0

# Ejecutar manualmente el scheduler
gcloud scheduler jobs run orchestrate-etl-jobs-schedule --location=us-east1 --project=constant-height-455614-i0

# Verificar estado del scheduler
gcloud scheduler jobs describe orchestrate-etl-jobs-schedule --location=us-east1 --project=constant-height-455614-i0
```

## 🔧 Troubleshooting

### Error de Timeout
- **Síntoma**: "Operation did not complete within the designated timeout"
- **Solución**: Verificar que el timeout de la función sea mayor que la suma de ambos Jobs

### Campos ETL Faltantes
- **Síntoma**: Tablas sin campos `_etl_synced` o `_etl_operation`
- **Solución**: Ejecutar script de migración de esquemas

### Jobs No Ejecutados
- **Síntoma**: Función termina sin ejecutar Jobs
- **Solución**: Verificar permisos del Service Account

## 📝 Notas de Desarrollo

- Los campos ETL se agregan automáticamente a nuevas tablas
- El MERGE maneja Soft Delete para registros eliminados
- La orquestación es síncrona (espera a que ambos Jobs terminen)
- Los logs están optimizados para facilitar el monitoreo


## 16 de abril 2026
### Cambio en el metodo multitasking para ambos Jobs
Usaré este comentario para marcar un COMMIT antes de empezar con el Upgrade.
