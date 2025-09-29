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

## 🚀 Comandos de Build y Deploy

### Job #1 (Extracción)

#### Build
```bash
gcloud builds submit --tag gcr.io/platform-partners-des/etl-st2json
```

#### Deploy
```bash
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

### Job #2 (Procesamiento)

#### Build
```bash
gcloud builds submit --tag gcr.io/platform-partners-des/etl-json2bq
```

#### Deploy
```bash
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

#### Deploy (No requiere Build)
```bash
gcloud functions deploy orchestrate-etl-jobs \
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

## 📁 Estructura del Proyecto

```
etl_servicetitan/
├── orchestrate_etl/
│   └── main.py                 # Función orquestadora
├── st2json-job/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── servicetitan_all_st_to_json.py
├── json2bq-job/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── servicetitan_all_json_to_bigquery.py
└── README.md
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
```bash
# Crear job de scheduler (cada 6 horas)
gcloud scheduler jobs create http etl-orchestration-schedule \
  --schedule="0 */6 * * *" \
  --uri="https://us-east1-platform-partners-des.cloudfunctions.net/orchestrate-etl-jobs" \
  --http-method=POST \
  --location=us-east1 \
  --oidc-service-account-email=etl-servicetitan@platform-partners-des.iam.gserviceaccount.com

# Verificar jobs de scheduler
gcloud scheduler jobs list --location=us-east1

# Ejecutar manualmente el scheduler
gcloud scheduler jobs run etl-orchestration-schedule --location=us-east1

# Verificar estado del scheduler
gcloud scheduler jobs describe etl-orchestration-schedule --location=us-east1
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
