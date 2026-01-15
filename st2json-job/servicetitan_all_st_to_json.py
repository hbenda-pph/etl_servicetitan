import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import os
import time
import shutil
import re
import gzip
import io
from google.cloud import bigquery, storage

# Configuración de BigQuery
# Detectar proyecto automáticamente desde variable de entorno o metadata del service account
# Si no está disponible, usar el proyecto por defecto del cliente BigQuery
def get_project_source():
    """
    Obtiene el proyecto del ambiente actual.
    Prioridad:
    1. Variable de entorno GCP_PROJECT (establecida por Cloud Run Jobs)
    2. Variable de entorno GOOGLE_CLOUD_PROJECT
    3. Proyecto por defecto del cliente BigQuery
    4. Fallback hardcoded según ambiente detectado
    
    Nota: En PRO, GCP_PROJECT contiene "platform-partners-pro" (project_name).
    BigQuery Client acepta tanto project_name como project_id, así que usamos el project_name
    que es lo que se establece en la variable de entorno GCP_PROJECT.
    """
    # Cloud Run Jobs establece GCP_PROJECT automáticamente
    project = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    if project:
        return project
    
    # Si no hay variable de entorno, intentar detectar desde el cliente
    try:
        client = bigquery.Client()
        return client.project
    except:
        pass
    
    # Fallback: detectar desde service account o usar default
    # En Cloud Run, el service account tiene el formato: service@PROJECT.iam.gserviceaccount.com
    return "platform-partners-qua"  # Fallback por defecto

PROJECT_SOURCE = get_project_source()
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Configuración para tabla de metadata (SIEMPRE centralizada en pph-central)
METADATA_PROJECT = "pph-central"
METADATA_DATASET = "management"
METADATA_TABLE = "metadata_consolidated_tables"

def load_endpoints_from_metadata():
    """
    Carga los endpoints automáticamente desde metadata_consolidated_tables.
    Solo carga endpoints con silver_use_bronze = TRUE (endpoints que este ETL maneja,
    diferenciándolos de los que maneja Fivetran).
    Construye las tuplas (api_url_base, api_data, table_name) desde la metadata.
    
    Returns:
        Lista de tuplas [(api_url_base, api_data, table_name), ...]
    """
    try:
        # Crear cliente con proyecto pph-central (estándar del proyecto)
        # El service account debe tener permisos en pph-central para consultar metadata
        client = bigquery.Client(project=METADATA_PROJECT)
        
        query = f"""
            SELECT 
                endpoint.module,
                endpoint.version,
                endpoint.submodule,
                endpoint.name,
                endpoint.endpoint_type,
                table_name
            FROM `{METADATA_PROJECT}.{METADATA_DATASET}.{METADATA_TABLE}`
            WHERE endpoint IS NOT NULL
              AND active = TRUE
              AND silver_use_bronze = TRUE
            ORDER BY table_name
        """
        
        job_config = bigquery.QueryJobConfig()
        
        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())
        
        endpoints = []
        for row in results:
            module = row.module
            version = row.version
            submodule = row.submodule if row.submodule else None
            name = row.name
            endpoint_type = row.endpoint_type if row.endpoint_type else "normal"
            table_name = row.table_name
            
            # Construir api_url_base: {module}/{version}/tenant
            # Limpiar cualquier barra al inicio/final
            api_url_base = f"{module.strip('/')}/{version.strip('/')}/tenant"
            
            # Construir api_data: si hay submodule, usar {submodule}/{name}, sino solo {name}
            # api_data se usa para construir la URL de la API
            # NOTA: "tenant" es una variable multitenant, NO es un submodule
            # Limpiar barras al inicio/final para evitar doble "/"
            submodule_clean = submodule.strip('/') if submodule else None
            name_clean = name.strip('/')
            
            if submodule_clean:
                api_data = f"{submodule_clean}/{name_clean}"
            else:
                api_data = name_clean
            
            # Retornar tupla con (api_url_base, api_data, table_name)
            # table_name se usa para nombrar archivos JSON y tablas
            endpoints.append((api_url_base, api_data, table_name))
            print(f"📋 Endpoint cargado: {api_url_base} / {api_data} → tabla: {table_name} (tipo: {endpoint_type})")
        
        print(f"✅ Total endpoints cargados desde metadata: {len(endpoints)}")
        return endpoints
        
    except Exception as e:
        print(f"⚠️  Error cargando endpoints desde metadata: {str(e)}")
        print(f"⚠️  Usando lista de endpoints por defecto (hardcoded)")
        # Fallback a lista por defecto si hay error
        # Retornar tuplas de 3 elementos: (api_url_base, api_data, table_name)
        return [
            ("settings/v2/tenant", "business-units", "business_unit"),
            ("jpm/v2/tenant", "job-types", "job_type"),
            ("settings/v2/tenant", "technicians", "technician"),
            ("settings/v2/tenant", "employees", "employee"),
            ("marketing/v2/tenant", "campaigns", "campaign"),
            ("payroll/v2/tenant", "jobs/timesheets", "timesheet"),
            ("inventory/v2/tenant", "purchase-orders", "purchase_order"),
            ("inventory/v2/tenant", "returns", "return"),
            ("inventory/v2/tenant", "vendors", "vendor"),
            ("jpm/v2/tenant", "export/job-canceled-logs", "job_canceled_log"),
            ("jpm/v2/tenant", "jobs/cancel-reasons", "job_cancel_reason")
        ]

# Cargar endpoints automáticamente desde metadata
ENDPOINTS = load_endpoints_from_metadata()


# Clase de autenticación y descarga
class ServiceTitanAuth:
    #Development:
    #AUTH_URL = "https://auth-integration.servicetitan.io/connect/token"
    #BASE_API_URL = "https://api-integration.servicetitan.io"

    #Production:
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
        # Renovar si pasaron más de 10 minutos
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
            }
        )
        response.raise_for_status()
        self._token = response.json()['access_token']
        self._token_time = time.time()
        return self._token
    
    def _build_api_url(self, api_url_base, api_data, query_params=None):
        """
        Construye la URL de la API de forma segura, evitando doble "/".
        
        Args:
            api_url_base: Base de la URL (ej: "settings/v2/tenant")
            api_data: Datos del endpoint (ej: "business-units" o "jobs/timesheets")
            query_params: Diccionario con parámetros de query (opcional)
        
        Returns:
            URL completa y limpia
        """
        # Limpiar barras al inicio/final de cada componente
        base_url = self.BASE_API_URL.rstrip('/')
        api_url_base = api_url_base.strip('/')
        api_data = api_data.strip('/')
        tenant_id = str(self.credentials['tenant_id']).strip('/')
        
        # Construir URL base sin doble "/"
        url = f"{base_url}/{api_url_base}/{tenant_id}/{api_data}"
        
        # Limpiar cualquier doble "/" que pueda haber quedado
        url = re.sub(r'/+', '/', url)
        # Reemplazar "http:/" o "https:/" por "http://" o "https://"
        url = re.sub(r'(https?):/', r'\1://', url)
        
        # Agregar query params si existen
        if query_params:
            query_string = '&'.join([f"{k}={v}" for k, v in query_params.items()])
            url = f"{url}?{query_string}"
        
        return url

    def get_data(self, api_url_base, api_data):
        """Método estándar para endpoints pequeños - retorna lista en memoria"""
        page_size = 5000
        all_data = []
        page = 1
        
        # Determinar qué parámetros acepta el endpoint (solo en primera página)
        use_active_param = True
        use_pagination = True
        
        # Intentar diferentes combinaciones solo en la primera página
        while True:
            token = self.get_access_token()
            success = False
            response = None
            
            # Estrategia 1: Intentar con active=Any + paginación
            if page == 1 and use_active_param and use_pagination:
                query_params = {'page': page, 'pageSize': page_size, 'active': 'Any'}
                url = self._build_api_url(api_url_base, api_data, query_params=query_params)
                response = requests.get(
                    url,
                    headers={
                        'Authorization': f'Bearer {token}',
                        'ST-App-Id': self.credentials['app_id'],
                        'ST-App-Key': self.credentials['app_key'],
                        'Accept': 'application/json',
                        'Connection': 'keep-alive'
                    },
                    timeout=(30, 600),  # Connect: 30s, Read: 10min para respuestas grandes
                    stream=True  # Necesario para usar response.raw
                )
                if response.status_code == 200:
                    success = True
                elif response.status_code == 404:
                    print(f"⚠️  Endpoint no acepta 'active=Any', intentando sin ese parámetro...")
                    use_active_param = False
                    # Continuar al siguiente intento
            
            # Estrategia 2: Intentar sin active=Any pero con paginación
            if not success and page == 1 and use_pagination:
                query_params = {'page': page, 'pageSize': page_size}
                url = self._build_api_url(api_url_base, api_data, query_params=query_params)
                response = requests.get(
                    url,
                    headers={
                        'Authorization': f'Bearer {token}',
                        'ST-App-Id': self.credentials['app_id'],
                        'ST-App-Key': self.credentials['app_key'],
                        'Accept': 'application/json',
                        'Connection': 'keep-alive'
                    },
                    timeout=(30, 600),  # Connect: 30s, Read: 10min para respuestas grandes
                    stream=True  # Necesario para usar response.raw
                )
                if response.status_code == 200:
                    success = True
                elif response.status_code == 404:
                    print(f"⚠️  Endpoint no acepta paginación, intentando sin parámetros...")
                    use_pagination = False
                    use_active_param = False  # Ya no tiene sentido usar active=Any sin paginación
            
            # Estrategia 3: Intentar sin parámetros (sin paginación, sin active=Any)
            if not success and page == 1 and not use_pagination:
                url = self._build_api_url(api_url_base, api_data, query_params=None)
                response = requests.get(
                    url,
                    headers={
                        'Authorization': f'Bearer {token}',
                        'ST-App-Id': self.credentials['app_id'],
                        'ST-App-Key': self.credentials['app_key'],
                        'Accept': 'application/json',
                        'Connection': 'keep-alive'
                    },
                    timeout=(30, 600),  # Connect: 30s, Read: 10min para respuestas grandes
                    stream=True  # Necesario para usar response.raw
                )
                if response.status_code == 200:
                    success = True
            
            # Para páginas siguientes (si usamos paginación), construir URL normalmente
            if not success and page > 1:
                query_params = {'page': page, 'pageSize': page_size}
                if use_active_param:
                    query_params['active'] = 'Any'
                url = self._build_api_url(api_url_base, api_data, query_params=query_params)
                response = requests.get(
                    url,
                    headers={
                        'Authorization': f'Bearer {token}',
                        'ST-App-Id': self.credentials['app_id'],
                        'ST-App-Key': self.credentials['app_key'],
                        'Accept': 'application/json',
                        'Connection': 'keep-alive'
                    },
                    timeout=(30, 600),  # Connect: 30s, Read: 10min para respuestas grandes
                    stream=True  # Necesario para usar response.raw
                )
                if response.status_code == 200:
                    success = True
            
            # Si ninguna estrategia funcionó, lanzar error
            if not success:
                response.raise_for_status()
            
            # Verificar que la respuesta sea JSON válido
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                error_msg = f"Respuesta no es JSON (Content-Type: {content_type}). URL: {url}"
                print(f"⚠️  {error_msg}")
                # Intentar obtener el contenido para debugging
                try:
                    content_preview = response.text[:500] if response.text else "Respuesta vacía"
                    print(f"⚠️  Contenido de respuesta (primeros 500 chars): {content_preview}")
                except:
                    pass
                raise ValueError(error_msg)
            
            # Procesar respuesta exitosa con manejo de errores de JSON
            # Leer desde response.raw directamente para obtener bytes sin ningún procesamiento
            # Esto evita cualquier corrupción de caracteres escapados como \"
            # NOTA: Cuando se usa stream=True y response.raw, requests NO descomprime automáticamente gzip
            try:
                # Leer directamente desde el stream raw para evitar procesamiento intermedio
                response_content = response.raw.read()
                
                # Verificar si la respuesta está comprimida con gzip
                # Cuando usamos stream=True y response.raw, debemos descomprimir manualmente
                content_encoding = response.headers.get('Content-Encoding', '').lower()
                if content_encoding == 'gzip':
                    # Descomprimir el contenido gzip manualmente
                    try:
                        response_content = gzip.decompress(response_content)
                        # Log solo en caso de error, no en cada descompresión exitosa
                    except Exception as gzip_err:
                        error_msg = f"Error descomprimiendo respuesta gzip: {str(gzip_err)}"
                        print(f"❌ {error_msg}")
                        raise ValueError(error_msg) from gzip_err
                
                # Verificar que el contenido esté completo comparando con Content-Length
                # Nota: Content-Length puede referirse al tamaño comprimido, así que solo verificamos si no está comprimido
                # Cuando hay compresión, Content-Length es el tamaño comprimido, así que no podemos validar fácilmente
                if not content_encoding:
                    content_length_header = response.headers.get('Content-Length')
                    if content_length_header:
                        expected_size = int(content_length_header)
                        actual_size = len(response_content)
                        if actual_size < expected_size:
                            error_msg = f"Respuesta truncada: esperados {expected_size} bytes, recibidos {actual_size} bytes"
                            print(f"❌ {error_msg}")
                            raise ValueError(error_msg)
                
                # Decodificar usando UTF-8 explícitamente
                # NO usar response.text porque puede procesar caracteres de manera diferente
                try:
                    response_text = response_content.decode('utf-8')
                except UnicodeDecodeError as decode_err:
                    # Si UTF-8 falla, intentar con el encoding detectado por requests
                    encoding = response.headers.get('Content-Type', '').split('charset=')[-1].split(';')[0] or 'utf-8'
                    if not encoding or encoding == 'ISO-8859-1':
                        encoding = 'utf-8'
                    response_text = response_content.decode(encoding, errors='replace')
                    print(f"⚠️  Advertencia: Se usó encoding '{encoding}' con reemplazo de errores")
                
                # Parsear JSON usando el texto decodificado
                result = json.loads(response_text)
            except json.JSONDecodeError as e:
                error_msg = f"Error parseando JSON: {str(e)}"
                print(f"❌ {error_msg}")
                print(f"❌ URL: {url}")
                print(f"❌ Status Code: {response.status_code}")
                print(f"❌ Content-Type: {content_type}")
                
                # Si el error tiene información de posición, extraerla
                if hasattr(e, 'pos') and e.pos:
                    error_pos = e.pos
                    print(f"❌ Posición del error en JSON: carácter {error_pos}")
                
                # Obtener el contenido para análisis
                try:
                    # El contenido ya debería estar leído en response_content
                    if 'response_content' in locals():
                        # Verificar si necesita descompresión
                        content_encoding = response.headers.get('Content-Encoding', '').lower()
                        if content_encoding == 'gzip':
                            try:
                                response_content = gzip.decompress(response_content)
                            except:
                                pass  # Si falla la descompresión, continuar con el contenido original
                        response_size = len(response_content)
                        try:
                            response_text = response_content.decode('utf-8')
                        except UnicodeDecodeError:
                            response_text = response_content.decode('utf-8', errors='replace')
                    else:
                        # Si no está disponible, intentar leer desde response.raw o response.content
                        try:
                            response_content = response.raw.read() if hasattr(response, 'raw') and response.raw else response.content
                        except:
                            response_content = response.content
                        # Verificar si necesita descompresión
                        content_encoding = response.headers.get('Content-Encoding', '').lower()
                        if content_encoding == 'gzip':
                            try:
                                response_content = gzip.decompress(response_content)
                            except:
                                pass  # Si falla la descompresión, continuar con el contenido original
                        response_size = len(response_content)
                        try:
                            response_text = response_content.decode('utf-8')
                        except UnicodeDecodeError:
                            response_text = response_content.decode('utf-8', errors='replace')
                    print(f"❌ Tamaño de respuesta: {response_size} bytes")
                    
                    # Verificar Content-Length
                    content_length_header = response.headers.get('Content-Length')
                    if content_length_header:
                        expected_size = int(content_length_header)
                        if response_size < expected_size:
                            print(f"❌ Content-Length esperado: {expected_size} bytes (faltan {expected_size - response_size} bytes)")
                        else:
                            print(f"✅ Content-Length coincide: {expected_size} bytes")
                    
                    # Verificar si la respuesta parece truncada
                    if response_text and not response_text.rstrip().endswith('}') and not response_text.rstrip().endswith(']'):
                        print(f"⚠️  La respuesta parece estar truncada (no termina con '}}' o ']')")
                        # Contar llaves y corchetes para ver si están balanceados
                        open_braces = response_text.count('{') - response_text.count('}')
                        open_brackets = response_text.count('[') - response_text.count(']')
                        if open_braces > 0 or open_brackets > 0:
                            print(f"⚠️  JSON no balanceado: {open_braces} llaves abiertas, {open_brackets} corchetes abiertos")
                    
                    # Guardar primeros y últimos caracteres para debugging
                    if len(response_text) > 1000:
                        preview = response_text[:500] + "\n... [truncado] ...\n" + response_text[-500:]
                    else:
                        preview = response_text
                    print(f"❌ Contenido de respuesta: {preview}")
                except Exception as decode_error:
                    print(f"❌ No se pudo obtener contenido de respuesta: {str(decode_error)}")
                
                raise ValueError(error_msg) from e
            
            # Algunos endpoints retornan directamente un array, otros tienen "data"
            if isinstance(result, list):
                page_items = result
            else:
                page_items = result.get("data", [])
            
            if not page_items:
                break
            
            all_data.extend(page_items)
            
            # Si no estamos usando paginación, solo una llamada
            if not use_pagination:
                break
            
            # Si estamos usando paginación, verificar si hay más páginas
            if len(page_items) < page_size:
                break
            page += 1
        
        return all_data

    def get_data_export(self, api_url_base, api_data, output_file, continue_from=None):
        """Método para endpoints /export/ - usa from/continueFrom en vez de paginación"""
        total_records = 0
        
        with open(output_file, 'w', encoding='utf-8') as f:
            while True:
                token = self.get_access_token()
                
                # Construir URL con o sin token de continuación
                if continue_from:
                    url = self._build_api_url(
                        api_url_base, 
                        api_data, 
                        query_params={'from': continue_from}
                    )
                else:
                    url = self._build_api_url(api_url_base, api_data)
                
                response = requests.get(url, headers={
                    'Authorization': f'Bearer {token}',
                    'ST-App-Id': self.credentials['app_id'],
                    'ST-App-Key': self.credentials['app_key']
                })
                response.raise_for_status()
                
                result = response.json()
                page_items = result.get("data", [])
                continue_from = result.get("continueFrom")
                has_more = result.get("hasMore", False)
                
                if not page_items:
                    break
                
                # Escribir cada item como línea separada (newline-delimited)
                for item in page_items:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
                
                total_records += len(page_items)
                #print(f"📄 Batch: {len(page_items)} registros (total: {total_records})")
                
                # Salir si no hay más datos
                if not has_more or not continue_from:
                    break
        
        return total_records, continue_from

# Cache para nombres de tablas (evita consultas repetidas)
_table_name_cache = {}

def get_standardized_table_name(endpoint):
    """
    Obtiene el nombre estandarizado de la tabla desde metadata_consolidated_tables.
    Si no se encuentra en la tabla de metadata, usa normalización por defecto.
    
    Args:
        endpoint: Nombre del endpoint (ej: "business-units", "job-types")
    
    Returns:
        Nombre estandarizado de la tabla
    """
    # Usar cache si ya se consultó antes
    cache_key = endpoint
    if cache_key in _table_name_cache:
        return _table_name_cache[cache_key]
    
    try:
        # Consultar tabla de metadata
        client = bigquery.Client(project=METADATA_PROJECT)
        # Consulta que busca por endpoint.name, retorna table_name directamente
        query = f"""
            SELECT table_name
            FROM `{METADATA_PROJECT}.{METADATA_DATASET}.{METADATA_TABLE}`
            WHERE endpoint.name = @endpoint
            LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("endpoint", "STRING", endpoint)
            ]
        )
        
        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())
        
        if results:
            table_name = results[0].table_name
            _table_name_cache[cache_key] = table_name
            print(f"📋 Tabla estandarizada desde metadata: {endpoint} -> {table_name}")
            return table_name
        else:
            # Si no se encuentra en metadata, usar normalización por defecto
            print(f"⚠️  Endpoint '{endpoint}' no encontrado en metadata, usando normalización por defecto")
            normalized = _normalize_table_name_fallback(endpoint)
            _table_name_cache[cache_key] = normalized
            return normalized
            
    except Exception as e:
        # En caso de error, usar normalización por defecto
        print(f"⚠️  Error consultando metadata para '{endpoint}': {str(e)}. Usando normalización por defecto")
        normalized = _normalize_table_name_fallback(endpoint)
        _table_name_cache[cache_key] = normalized
        return normalized

def _normalize_table_name_fallback(endpoint):
    """
    Función de respaldo para normalizar nombres cuando no se encuentra en metadata.
    Convierte guiones y slashes a underscores.
    """
    normalized = endpoint.replace("/", "_").replace("-", "_")
    normalized = re.sub(r'_+', '_', normalized)
    normalized = normalized.strip('_')
    return normalized

def ensure_bucket_exists(project_id, region="US"):
    bucket_name = f"{project_id}_servicetitan"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name, location=region)
        print(f"✅ Bucket creado: {bucket_name}")
    else:
        print(f"Bucket ya existe: {bucket_name}")
    return bucket_name

def upload_to_bucket(bucket_name, project_id, local_file, dest_blob_name):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_filename(local_file)
    print(f"📤 Subido a gs://{bucket_name}/{dest_blob_name}")

def process_company(row):
    # Obtener credenciales y datos
    company_id = row.company_id
    company_name = row.company_name
    company_new_name = row.company_new_name
    app_id = row.app_id
    client_id = row.client_id
    client_secret = row.client_secret
    tenant_id = row.tenant_id
    app_key = row.app_key
    project_id = row.company_project_id  # Usar directamente el campo project_id
    print(f"\n{'='*80}\n🏢 Procesando compañía: {company_name} (ID: {company_id}) | project_id: {project_id}")
    # 1. Crear/verificar bucket
    bucket_name = ensure_bucket_exists(project_id)
    # 2. Instanciar cliente ServiceTitan
    st_client = ServiceTitanAuth(app_id, client_id, client_secret, tenant_id, app_key)
    # 3. Descargar y subir datos de cada endpoint
    for api_url_base, api_data, table_name in ENDPOINTS:
        print(f"\n🔄 Descargando endpoint: {api_data}")
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Usar table_name directamente desde metadata para nombrar archivos JSON
            filename_ts = f"servicetitan_{table_name}_{timestamp}.json"
            filename_alias = f"servicetitan_{table_name}.json"
            
            # Detectar si es endpoint export (usa from/continueFrom) o normal (usa page)
            if api_data.startswith("export/"):
                # Endpoint EXPORT: usa continueFrom para paginación
                #print(f"📤 [EXPORT MODE] Usando paginación con continueFrom")
                total, continue_token = st_client.get_data_export(api_url_base, api_data, filename_alias)
                print(f"📊 [EXPORT MODE] Total registros descargados: {total}")
                if continue_token:
                    print(f"🔖 [EXPORT MODE] Token para próxima ejecución: {continue_token}")
                # Copiar para archivo con timestamp
                shutil.copy2(filename_alias, filename_ts)
            else:
                # Endpoint NORMAL: carga en memoria con paginación page=
                #print(f"📥 [NORMAL MODE] Usando paginación con page=")
                data = st_client.get_data(api_url_base, api_data)
                print(f"📊 [NORMAL MODE] Total registros descargados: {len(data)}")
                with open(filename_ts, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                with open(filename_alias, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Subir ambos archivos al bucket
            upload_to_bucket(bucket_name, project_id, filename_ts, filename_ts)
            upload_to_bucket(bucket_name, project_id, filename_alias, filename_alias)
            # Borrar archivos locales
            os.remove(filename_ts)
            os.remove(filename_alias)
            print(f"✅ Endpoint {api_data} procesado y archivos subidos.")
        except Exception as e:
            print(f"❌ Error en endpoint {api_data}: {str(e)}")

def main():
    # Detectar si estamos en modo paralelo (Cloud Run Jobs con múltiples tareas)
    # Cloud Run Jobs establece estas variables de entorno automáticamente:
    # CLOUD_RUN_TASK_INDEX: índice de la tarea actual (0-based)
    # CLOUD_RUN_TASK_COUNT: número total de tareas
    task_index = int(os.environ.get('CLOUD_RUN_TASK_INDEX', '0'))
    task_count = int(os.environ.get('CLOUD_RUN_TASK_COUNT', '1'))
    is_parallel = task_count > 1
    
    if is_parallel:
        print(f"\n{'='*80}")
        print(f"🚀 MODO PARALELO ACTIVADO")
        print(f"   Tarea: {task_index + 1}/{task_count}")
        print(f"{'='*80}")
    
    print(f"🔍 Proyecto detectado para companies: {PROJECT_SOURCE}")
    print(f"🔍 Proyecto para metadata: {METADATA_PROJECT}")
    print("Conectando a BigQuery para obtener compañías...")
    if is_parallel:
        print(f"🔄 Procesamiento paralelo: Tarea {task_index + 1} de {task_count}")
    
    # Usar PROJECT_SOURCE tanto para el cliente como para las queries
    # BigQuery Client acepta project_name (como "platform-partners-pro")
    client = bigquery.Client(project=PROJECT_SOURCE)
    query = f"""
        SELECT * FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_fivetran_status = TRUE
        ORDER BY company_id
    """
    results = list(client.query(query).result())  # Convertir a lista para poder contar
    total = len(results)
    
    # En modo paralelo, dividir las compañías entre las tareas
    if is_parallel:
        # Calcular qué compañías procesa esta tarea
        companies_per_task = total // task_count
        remainder = total % task_count
        
        # Las primeras tareas procesan una compañía extra si hay resto
        start_idx = task_index * companies_per_task + min(task_index, remainder)
        end_idx = start_idx + companies_per_task + (1 if task_index < remainder else 0)
        
        # Filtrar compañías para esta tarea
        results = results[start_idx:end_idx]
        total_assigned = len(results)
        
        print(f"📊 Total de compañías: {total}")
        print(f"📊 Compañías asignadas a esta tarea: {total_assigned} (índices {start_idx+1}-{end_idx} de {total})")
    else:
        total_assigned = total
        print(f"📊 Total de compañías a procesar: {total}")
    
    print(f"{'='*80}\n")
    
    procesadas = 0
    for idx, row in enumerate(results, 1):
        try:
            if is_parallel:
                print(f"\n[{idx}/{total_assigned}] Procesando compañía: {row.company_name}")
            process_company(row)
            procesadas += 1
        except Exception as e:
            print(f"❌ Error procesando compañía {row.company_name}: {str(e)}")
    
    print(f"\n{'='*80}")
    if is_parallel:
        print(f"🏁 Resumen Tarea {task_index + 1}/{task_count}: {procesadas}/{total_assigned} compañías procesadas exitosamente.")
        print(f"📊 Total global: {total} compañías distribuidas en {task_count} tareas")
    else:
        print(f"🏁 Resumen: {procesadas}/{total} compañías procesadas exitosamente.")

if __name__ == "__main__":
    main()
