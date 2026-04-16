"""
Módulo común con funciones compartidas para scripts st2json.
Este módulo contiene todas las funciones que son usadas tanto por
servicetitan_all_st_to_json.py (producción) como por servicetitan_st_to_json.py (pruebas).
"""

import requests
from requests.auth import HTTPBasicAuth
import json
import os
import time
import re
import gzip
import logging
from google.cloud import bigquery, storage

# Configurar logging para suprimir mensajes innecesarios de urllib3/requests sobre gzip
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("google.auth").setLevel(logging.ERROR)  # Suprimir warnings de autenticación
logging.getLogger("google.auth.transport").setLevel(logging.ERROR)  # Suprimir warnings de autenticación

# Suprimir advertencias específicas de quota project
import warnings
warnings.filterwarnings("ignore", message=".*quota project.*", category=UserWarning)
warnings.filterwarnings("ignore", message=".*end user credentials.*", category=UserWarning)

# Configuración de BigQuery
def get_project_source():
    """
    Obtiene el proyecto del ambiente actual.
    Prioridad:
    1. Variable de entorno GCP_PROJECT (establecida por Cloud Run Jobs)
    2. Variable de entorno GOOGLE_CLOUD_PROJECT
    3. Proyecto por defecto del cliente BigQuery
    4. Fallback hardcoded según ambiente detectado
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

def get_bigquery_project_id():
    """
    Obtiene el project_id real para usar en queries SQL.
    En PRO, GCP_PROJECT contiene "platform-partners-pro" (project_name),
    pero necesitamos usar "constant-height-455614-i0" (project_id) en las queries.
    """
    project_source = get_project_source()
    
    # Si estamos en PRO y recibimos el project_name, usar el project_id real
    if project_source == "platform-partners-pro":
        return "constant-height-455614-i0"
    
    # Para otros ambientes, project_name = project_id
    return project_source

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

def get_balanced_tasks(bq_client, results, task_count, task_index):
    """
    Distribuye las compañías entre las tareas de Cloud Run usando un algoritmo
    Greedy para balancear la carga basada en métricas históricas de duración.
    
    Args:
        bq_client: Cliente de BigQuery para obtener métricas.
        results: Lista de filas (Row objects) con las compañías activas.
        task_count: Número total de tareas (bins).
        task_index: Índice de la tarea actual (0-based).
    
    Returns:
        Lista de filas (Row objects) asignadas a esta tarea.
    """
    if task_count <= 1:
        return results

    # 1. Obtener pesos (duración total acumulada) de BigQuery
    # IMPORTANTE: Se agrupa por company_id para sumar la duración de todos sus endpoints
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

    # 2. Asignar peso por defecto a compañías sin historial (promedio o 60s)
    fallback_weight = sum(weights.values()) / len(weights) if weights else 60.0
    
    # Preparar lista de compañías con su peso detectado o fallback
    company_data = []
    for row in results:
        cid = int(row.company_id)
        w = weights.get(cid, fallback_weight)
        company_data.append({'row': row, 'weight': w})

    # 3. Algoritmo Greedy Bin Packing
    company_data.sort(key=lambda x: x['weight'], reverse=True)
    
    bins = [[] for _ in range(task_count)]
    bin_weights = [0.0] * task_count
    
    for item in company_data:
        min_bin_idx = bin_weights.index(min(bin_weights))
        bins[min_bin_idx].append(item['row'])
        bin_weights[min_bin_idx] += item['weight']
    
    assigned_companies = bins[task_index]
    
    total_w = sum(bin_weights)
    avg_w   = total_w / task_count if task_count > 0 else 0
    this_w  = bin_weights[task_index]
    diff_pct = ((this_w - avg_w) / avg_w * 100) if avg_w > 0 else 0
    
    print(f"\n⚖️  BALANCEO INTELIGENTE (Greedy Partitioning)")
    print(f"   Tarea actual: {task_index + 1} de {task_count}")
    print(f"   Carga estimada t{task_index+1}: {this_w:.1f}s (Dif vs promedio: {diff_pct:+.1f}%)")
    print(f"   Compañías asignadas: {len(assigned_companies)}")
    
    assigned_companies.sort(key=lambda x: x.company_id)
    
    return assigned_companies

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

    def get_data_streaming(self, api_url_base, api_data, output_file):
        """
        Método para endpoints grandes - escribe directamente a archivo para evitar problemas de memoria.
        Usa paginación estándar (page/pageSize) pero escribe cada página directamente al archivo.
        """
        page_size = 5000
        total_records = 0
        page = 1
        
        # Determinar qué parámetros acepta el endpoint (solo en primera página)
        use_active_param = True
        use_pagination = True
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('[\n')
            first_item = True
            
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
                        timeout=(30, 600),
                        stream=True
                    )
                    if response.status_code == 200:
                        success = True
                    elif response.status_code == 404:
                        use_active_param = False
                
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
                        timeout=(30, 600),
                        stream=True
                    )
                    if response.status_code == 200:
                        success = True
                    elif response.status_code == 404:
                        use_pagination = False
                        use_active_param = False
                
                # Estrategia 3: Intentar sin parámetros
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
                        timeout=(30, 600),
                        stream=True
                    )
                    if response.status_code == 200:
                        success = True
                
                # Para páginas siguientes
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
                        timeout=(30, 600),
                        stream=True
                    )
                    if response.status_code == 200:
                        success = True
                
                if not success:
                    response.raise_for_status()
                
                # Procesar respuesta
                try:
                    response_content = response.raw.read()
                    content_encoding = response.headers.get('Content-Encoding', '').lower()
                    if content_encoding == 'gzip':
                        response_content = gzip.decompress(response_content)
                    response_text = response_content.decode('utf-8')
                    result = json.loads(response_text)
                except Exception as e:
                    error_msg = f"Error procesando respuesta en página {page}: {str(e)}"
                    print(f"❌ {error_msg}")
                    raise ValueError(error_msg) from e
                
                # Extraer items de la página
                if isinstance(result, list):
                    page_items = result
                else:
                    page_items = result.get("data", [])
                
                if not page_items:
                    break
                
                # Escribir items directamente al archivo
                for item in page_items:
                    if not first_item:
                        f.write(',\n')
                    json.dump(item, f, ensure_ascii=False, indent=2)
                    first_item = False
                
                total_records += len(page_items)
                
                # Si no estamos usando paginación, solo una llamada
                if not use_pagination:
                    break
                
                # Si estamos usando paginación, verificar si hay más páginas
                if len(page_items) < page_size:
                    break
                page += 1
            
            f.write('\n]')
        
        return total_records


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
