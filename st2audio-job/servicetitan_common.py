"""
Módulo común con funciones compartidas para el ETL de Audios de ServiceTitan (st2audio-job).
Contiene exclusivamente la autenticación con ServiceTitan Telecom API, gestión de Storage
y creación/validación de la tabla bronze.call_recordings en BigQuery.
"""

import os
import time
import logging
import warnings
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery, storage

# Suprimir advertencias y configurar logging
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("google.auth").setLevel(logging.ERROR)
logging.getLogger("google.auth.transport").setLevel(logging.ERROR)

warnings.filterwarnings("ignore", message=".*quota project.*", category=UserWarning)
warnings.filterwarnings("ignore", message=".*end user credentials.*", category=UserWarning)

# Configuración central de metadata
METADATA_PROJECT = "pph-central"
METADATA_DATASET = "management"
METADATA_TABLE = "metadata_consolidated_tables"


def get_project_source():
    """
    Obtiene el proyecto del ambiente actual.
    Prioridad:
    1. Variable de entorno GCP_PROJECT (establecida por Cloud Run Jobs)
    2. Variable de entorno GOOGLE_CLOUD_PROJECT
    3. Proyecto por defecto del cliente BigQuery
    4. Fallback 'platform-partners-qua'
    """
    project = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if project:
        return project
    
    try:
        client = bigquery.Client()
        return client.project
    except Exception:
        pass
    
    return "platform-partners-qua"


def get_bigquery_project_id():
    """
    Obtiene el project_id real para usar en queries SQL.
    En PRO, GCP_PROJECT contiene 'platform-partners-pro',
    pero se usa 'constant-height-455614-i0' (project_id) en las queries.
    """
    project_source = get_project_source()
    if project_source == "platform-partners-pro":
        return "constant-height-455614-i0"
    return project_source


def get_balanced_tasks(bq_client, results, task_count, task_index):
    """
    Distribuye las compañías entre las tareas de Cloud Run usando un algoritmo
    Greedy para balancear la carga basada en métricas históricas de duración.
    """
    if task_count <= 1:
        return results

    weights = {}
    try:
        query = f"""
            SELECT company_id, SUM(actual_duration) as total_duration
            FROM `{METADATA_PROJECT}.management.etl_monitoring_snapshot`
            WHERE updated_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
            GROUP BY company_id
        """
        query_job = bq_client.query(query)
        for row in query_job.result():
            weights[int(row.company_id)] = float(row.total_duration)
    except Exception as e:
        print(f"⚠️  [get_balanced_tasks] No se pudieron obtener pesos históricos: {str(e)[:100]}")

    fallback_weight = sum(weights.values()) / len(weights) if weights else 60.0
    
    company_data = []
    for row in results:
        cid = int(row.company_id)
        w = weights.get(cid, fallback_weight)
        company_data.append({'row': row, 'weight': w})

    company_data.sort(key=lambda x: x['weight'], reverse=True)
    
    bins = [[] for _ in range(task_count)]
    bin_weights = [0.0] * task_count
    
    for item in company_data:
        min_bin_idx = bin_weights.index(min(bin_weights))
        bins[min_bin_idx].append(item['row'])
        bin_weights[min_bin_idx] += item['weight']
    
    assigned_companies = bins[task_index]
    assigned_companies.sort(key=lambda x: x.company_id)
    return assigned_companies


class ServiceTitanAuth:
    """Manejo de autenticación OAuth2 y llamadas a ServiceTitan Telecom API."""
    AUTH_URL = "https://auth.servicetitan.io/connect/token"
    BASE_API_URL = "https://api.servicetitan.io"

    def __init__(self, app_id, client_id, client_secret, tenant_id, app_key):
        self.credentials = {
            'app_id': app_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'tenant_id': tenant_id,
            'app_key': app_key
        }
        self._token = None
        self._token_time = 0

    def get_access_token(self):
        """Obtiene o renueva el token OAuth2 si pasaron más de 10 minutos."""
        if self._token and (time.time() - self._token_time) < 600:
            return self._token
        
        response = requests.post(
            self.AUTH_URL,
            auth=HTTPBasicAuth(self.credentials['client_id'], self.credentials['client_secret']),
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'ST-App-Key': self.credentials['app_key']
            },
            data={
                'grant_type': 'client_credentials',
                'client_id': self.credentials['client_id'],
                'client_secret': self.credentials['client_secret']
            },
            timeout=30
        )
        response.raise_for_status()
        self._token = response.json()['access_token']
        self._token_time = time.time()
        return self._token

    def get_call_recording(self, lead_call_id):
        """
        Descarga el stream binario de audio de una llamada desde Telecom API V2.
        Endpoint: GET /telecom/v2/tenant/{tenant_id}/calls/{lead_call_id}/recording

        Args:
            lead_call_id: ID de la llamada en ServiceTitan.

        Returns:
            requests.Response con stream=True
        """
        token = self.get_access_token()
        tenant_id = str(self.credentials['tenant_id']).strip('/')
        base_url = self.BASE_API_URL.rstrip('/')
        url = f"{base_url}/telecom/v2/tenant/{tenant_id}/calls/{lead_call_id}/recording"

        headers = {
            'Authorization': f'Bearer {token}',
            'ST-App-Key': self.credentials['app_key'],
            'Accept': '*/*',
            'Connection': 'keep-alive'
        }

        response = requests.get(url, headers=headers, stream=True, timeout=(15, 120))
        return response


def ensure_audio_bucket_exists(project_id, region="US"):
    """Crea el bucket {project_id}_audio en Cloud Storage si no existe."""
    bucket_name = f"{project_id}_audio"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name, location=region)
        print(f"✅ Bucket de audio creado: {bucket_name}")
    else:
        print(f"Bucket de audio ya existe: {bucket_name}")
    return bucket_name


def upload_to_bucket(storage_client, bucket_name, dest_blob_name, response_stream, content_type="audio/mpeg"):
    """
    Sube directamente el stream de bytes de la petición HTTP a Cloud Storage
    sin almacenar el archivo en disco ni saturar memoria RAM.
    
    Args:
        storage_client : Cliente de Google Cloud Storage.
        bucket_name    : Nombre del bucket destino (ej: shape-mhs-1_audio).
        dest_blob_name : Nombre del blob (ej: 512514305.mp3).
        response_stream: requests.Response (stream=True).
        content_type   : Tipo MIME del archivo (default: audio/mpeg).
        
    Returns:
        int: Tamaño del archivo subido en bytes.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    
    response_stream.raw.decode_content = True
    blob.upload_from_file(response_stream.raw, content_type=content_type)
    
    blob.reload()
    return blob.size


def parse_duration_seconds(duration_str):
    """
    Convierte cadenas de duración ("00:01:15.3180000", "00:00:41", "120") a segundos numéricos.
    """
    if not duration_str:
        return 0.0
    try:
        parts = str(duration_str).strip().split(':')
        if len(parts) == 3:
            h = float(parts[0])
            m = float(parts[1])
            s = float(parts[2])
            return round(h * 3600 + m * 60 + s, 3)
        elif len(parts) == 2:
            m = float(parts[0])
            s = float(parts[1])
            return round(m * 60 + s, 3)
        return round(float(duration_str), 3)
    except Exception:
        return 0.0


def ensure_call_recordings_table_exists(bq_client, project_id, dataset_name="bronze", table_name="call_recordings"):
    """
    Crea la tabla bronze.call_recordings si no existe, con partición mensual por _etl_synced
    y clustering por status.
    """
    dataset_ref = bigquery.DatasetReference(project_id, dataset_name)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        bq_client.create_dataset(dataset, exists_ok=True)
        print(f"✅ Dataset `{project_id}.{dataset_name}` creado.")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{project_id}.{dataset_name}.{table_name}` (
      lead_call_id      INT64 NOT NULL,
      id                INT64,
      gcs_uri           STRING,
      file_name         STRING,
      file_size_bytes   INT64,
      content_type      STRING,
      status            INT64 NOT NULL,
      http_status_code  INT64,
      error_message     STRING,
      retry_count       INT64,
      _etl_synced       TIMESTAMP,
      _etl_operation    STRING
    )
    PARTITION BY TIMESTAMP_TRUNC(_etl_synced, MONTH)
    CLUSTER BY status
    OPTIONS (
      description = "Registro y control de audios descargados desde ServiceTitan Telecom API para ML"
    );
    """
    bq_client.query(ddl).result()
    print(f"✅ Tabla `{project_id}.{dataset_name}.{table_name}` verificada/creada.")
