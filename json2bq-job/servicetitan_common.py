"""
Módulo común con funciones compartidas para scripts json2bq.
Este módulo contiene todas las funciones que son usadas tanto por
servicetitan_all_json_to_bq.py (producción) como por servicetitan_json_to_bq.py (pruebas).
"""

import os
import json
import re
import time
import logging
from datetime import datetime, timezone
from google.cloud import bigquery, storage

# Configurar logging para suprimir mensajes innecesarios
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
    3. Proyecto desde gcloud config (para ejecución local) - MÁS CONFIABLE
    4. Proyecto desde Application Default Credentials
    5. Proyecto del cliente BigQuery
    6. Fallback: usar DEV (más seguro que QUA)
    """
    # Cloud Run Jobs establece GCP_PROJECT automáticamente
    project = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    if project:
        return project
    
    # PRIORIDAD: Intentar obtener desde gcloud config (para ejecución local)
    # Esto es más confiable porque refleja el proyecto activo del usuario
    try:
        import subprocess
        result = subprocess.run(
            ['gcloud', 'config', 'get-value', 'project'],
            capture_output=True,
            text=True,
            timeout=3
        )
        if result.returncode == 0 and result.stdout.strip():
            project = result.stdout.strip()
            if project:
                # Validar que sea uno de los proyectos conocidos
                known_projects = [
                    'platform-partners-des',  # DEV
                    'platform-partners-qua',  # QUA
                    'platform-partners-pro',  # PRO (project name)
                    'constant-height-455614-i0'  # PRO (project id)
                ]
                if project in known_projects:
                    return project
                # Si no es conocido, igualmente usarlo (puede ser un proyecto de prueba)
                return project
    except Exception as e:
        # Si gcloud no está disponible o hay error, continuar con otros métodos
        pass
    
    # Intentar obtener desde Application Default Credentials
    try:
        from google.auth import default
        credentials, project = default()
        if project:
            # Validar que sea uno de los proyectos conocidos
            known_projects = [
                'platform-partners-des',
                'platform-partners-qua',
                'platform-partners-pro',
                'constant-height-455614-i0'
            ]
            if project in known_projects:
                return project
    except:
        pass
    
    # Intentar detectar desde el cliente BigQuery
    try:
        client = bigquery.Client()
        detected_project = client.project
        if detected_project:
            # Validar que sea uno de los proyectos conocidos
            known_projects = [
                'platform-partners-des',
                'platform-partners-qua',
                'platform-partners-pro',
                'constant-height-455614-i0'
            ]
            if detected_project in known_projects:
                return detected_project
    except:
        pass
    
    # Último recurso: usar DEV (más seguro que QUA como fallback)
    return "platform-partners-des"  # Fallback a DEV

def get_bigquery_project_id():
    """
    Obtiene el project_id real para usar en queries SQL y operaciones de BigQuery.
    En PRO, GCP_PROJECT contiene "platform-partners-pro" (project_name),
    pero necesitamos usar "constant-height-455614-i0" (project_id) en las operaciones.
    """
    project_source = get_project_source()
    
    # Si estamos en PRO y recibimos el project_name, usar el project_id real
    if project_source == "platform-partners-pro":
        return "constant-height-455614-i0"
    
    # Para otros ambientes, project_name = project_id
    return project_source

# Configuración para logging centralizado
LOGS_DATASET = "logs"
LOGS_TABLE = "etl_servicetitan"

def get_logs_project():
    """Obtiene el proyecto para logging dinámicamente"""
    # Usar el mismo proyecto que se está usando para las operaciones
    project = get_bigquery_project_id()
    
    # Mostrar mensaje informativo sobre el proyecto detectado (solo una vez)
    if not hasattr(get_logs_project, '_project_shown'):
        detected_from = "variable de entorno"
        if not os.environ.get('GCP_PROJECT') and not os.environ.get('GOOGLE_CLOUD_PROJECT'):
            detected_from = "gcloud config o credenciales"
        print(f"🔍 Proyecto detectado para logs: {project} (desde {detected_from})")
        get_logs_project._project_shown = True
    
    return project

# Configuración para tabla de metadata (SIEMPRE centralizada en pph-central)
METADATA_PROJECT = "pph-central"
METADATA_DATASET = "management"
METADATA_TABLE = "metadata_consolidated_tables"

def load_endpoints_from_metadata():
    """
    Carga los endpoints automáticamente desde metadata_consolidated_tables.
    Solo carga endpoints con silver_use_bronze = TRUE (endpoints que este ETL maneja,
    diferenciándolos de los que maneja Fivetran).
    Retorna tuplas (endpoint_name, table_name) donde:
    - endpoint_name: endpoint.name (usado para logging)
    - table_name: nombre de la tabla (usado para archivos JSON y tablas BigQuery)
    
    Returns:
        Lista de tuplas [(endpoint_name, table_name), ...]
    """
    try:
        client = bigquery.Client(project=METADATA_PROJECT)
        query = f"""
            SELECT 
                endpoint.name,
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
        
        endpoints = [(row.name, row.table_name) for row in results]
        print(f"✅ Total endpoints cargados desde metadata: {len(endpoints)}")
        return endpoints
        
    except Exception as e:
        print(f"⚠️  Error cargando endpoints desde metadata: {str(e)}")
        print(f"⚠️  Usando lista de endpoints por defecto (hardcoded)")
        # Fallback a lista por defecto si hay error
        # Retornar tuplas de 2 elementos: (endpoint_name, table_name)
        # IMPORTANTE: table_name debe coincidir con el usado en st2json-job
        return [
            ("business-units", "business_unit"),
            ("job-types", "job_type"),
            ("technicians", "technician"),
            ("employees", "employee"),
            ("campaigns", "campaign"),
            ("jobs/timesheets", "timesheet"),
            ("purchase-orders", "purchase_order"),
            ("returns", "return"),
            ("vendors", "vendor"),
            ("export/job-canceled-logs", "job_canceled_log"),
            ("jobs/cancel-reasons", "job_cancel_reason")
        ]

# Función para convertir a snake_case
def to_snake_case(name):
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower()

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

def log_event_bq(company_id=None, company_name=None, project_id=None, endpoint=None, 
                event_type="INFO", event_title="", event_message="", info=None, source="servicetitan_json_to_bigquery"):
    """Inserta un evento en la tabla de logs centralizada."""
    try:
        # Obtener proyecto de logs dinámicamente para usar el proyecto correcto
        logs_project = get_logs_project()
        
        # Debug: mostrar qué proyecto se está usando para logs
        if os.environ.get('DEBUG_LOGS', '').lower() == 'true':
            print(f"🔍 DEBUG: Usando proyecto para logs: {logs_project}")
            print(f"🔍 DEBUG: GCP_PROJECT={os.environ.get('GCP_PROJECT')}")
            print(f"🔍 DEBUG: GOOGLE_CLOUD_PROJECT={os.environ.get('GOOGLE_CLOUD_PROJECT')}")
        
        client = bigquery.Client(project=logs_project)
        table_id = f"{LOGS_DATASET}.{LOGS_TABLE}"
        
        row = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "company_id": str(company_id) if company_id else None,
            "company_name": company_name,
            "project_id": project_id,
            "endpoint": endpoint,
            "event_type": event_type,
            "event_title": event_title,
            "event_message": event_message,
            "source": source,
            "info": json.dumps(info) if info else None
        }
        
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            print(f"❌ Error insertando log en BigQuery: {errors}")
    except Exception as e:
        print(f"❌ Error en logging: {str(e)}")

def fix_nested_value(value, field_path="", known_array_fields=None):
    """Función recursiva para corregir valores anidados.
    IMPORTANTE: Campos dentro de STRUCT preservan camelCase (como vienen de la fuente).
    Solo corrige: objetos a arrays cuando es necesario, NULL a [] para campos REPEATED.
    NO convierte nombres de campos dentro de STRUCT a snake_case."""
    if known_array_fields is None:
        known_array_fields = set()
    
    # Si es None, verificar si este campo debería ser un array
    if value is None:
        # Extraer el nombre del campo del path (último componente)
        field_name = field_path.split('.')[-1] if field_path else ""
        if field_name in known_array_fields:
            return []  # Convertir NULL a array vacío para campos REPEATED
        return None
    
    # Si es un diccionario/objeto (STRUCT)
    if isinstance(value, dict):
        # Verificar si este campo debería ser un array (por nombre común)
        # serialNumbers debería ser array
        field_name_lower = field_path.lower() if field_path else ""
        if 'serial' in field_name_lower and 'number' in field_name_lower:
            # Si es un objeto pero debería ser array, convertir a array vacío
            return []
        
        # PRINCIPIO BRONZE: Preservar nombres originales (camelCase) - NO convertir a snake_case
        # Solo procesar recursivamente para corregir NULLs y arrays anidados
        fixed_dict = {}
        
        for k, v in value.items():
            # Preservar nombre original 'k' (camelCase) - NO convertir a snake_case
            nested_path = f"{field_path}.{k}" if field_path else k
            
            # Verificar si este campo anidado necesita procesamiento especial
            nested_field_lower = nested_path.lower()
            
            # Procesar recursivamente para corregir NULLs y arrays, pero preservar nombres
            if 'serial' in nested_field_lower and 'number' in nested_field_lower and isinstance(v, dict):
                # serialNumbers como objeto: convertir a array vacío
                fixed_dict[k] = []  # Preservar nombre original
            else:
                # Procesar recursivamente pero preservar nombre original
                fixed_dict[k] = fix_nested_value(v, nested_path, known_array_fields)
        
        return fixed_dict
    
    # Si es una lista (ARRAY), procesar cada elemento recursivamente
    # IMPORTANTE: Cuando el array contiene STRUCT, preserva camelCase dentro de esos STRUCT
    if isinstance(value, list):
        # Procesar recursivamente cada elemento del array (preserva camelCase dentro de STRUCT)
        return [fix_nested_value(item, field_path, known_array_fields) for item in value]
    
    # Para otros tipos, retornar tal cual
    return value

def fix_json_format(local_path, temp_path, repeated_fields=None):
    """Transforma el JSON a formato newline-delimited y snake_case.
    IMPORTANTE: Campos de nivel superior → snake_case, campos dentro de STRUCT → camelCase (preservar fuente).
    Soporta tanto JSON array como newline-delimited JSON.
    Si se proporciona repeated_fields, convierte NULL a [] para esos campos.
    También corrige campos anidados que deberían ser arrays pero vienen como objetos.
    
    Para archivos grandes (>100MB), usa procesamiento streaming para evitar problemas de memoria."""
    
    # Detectar tamaño del archivo
    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    LARGE_FILE_THRESHOLD_MB = 100  # Usar streaming para archivos >100MB
    
    if file_size_mb > LARGE_FILE_THRESHOLD_MB:
        # Usar procesamiento streaming para archivos grandes
        return fix_json_format_streaming(local_path, temp_path, repeated_fields)
    
    # Procesamiento en memoria para archivos pequeños (más rápido)
    with open(local_path, 'r', encoding='utf-8') as f:
        first_char = f.read(1)
        f.seek(0)
        
        if first_char == '[':
            # JSON array tradicional
            json_data = json.load(f)
        else:
            # Newline-delimited JSON
            json_data = [json.loads(line) for line in f if line.strip()]
    
    # Detectar campos array (usando snake_case para campos de nivel superior)
    if repeated_fields:
        array_fields = set(repeated_fields)
        # También detectar campos array en los datos para casos adicionales
        detected_array_fields = set()
        sample_size = min(100, len(json_data))
        for item in json_data[:sample_size]:
            for key, value in item.items():
                if isinstance(value, list):
                    detected_array_fields.add(to_snake_case(key))  # snake_case para nivel superior
        array_fields = array_fields | detected_array_fields
    else:
        # Detectar campos que son arrays - usando snake_case para nivel superior
        detected_array_fields = set()
        sample_size = min(100, len(json_data))
        for item in json_data[:sample_size]:
            for key, value in item.items():
                if isinstance(value, list):
                    detected_array_fields.add(to_snake_case(key))  # snake_case para nivel superior
        array_fields = detected_array_fields
    
    # IMPORTANTE: Campos de nivel superior → snake_case, campos dentro de STRUCT → camelCase
    # Transformar y limpiar
    with open(temp_path, 'w', encoding='utf-8') as f:
        for item in json_data:
            new_item = {}
            for k, v in item.items():
                snake_key = to_snake_case(k)  # snake_case para campos de nivel superior
                
                # Procesar recursivamente: preserva camelCase dentro de STRUCT
                fixed_value = fix_nested_value(v, snake_key, array_fields)
                
                # Si el campo es un array y viene como NULL, convertir a array vacío
                if snake_key in array_fields and fixed_value is None:
                    new_item[snake_key] = []
                else:
                    new_item[snake_key] = fixed_value
            f.write(json.dumps(new_item) + '\n')

def fix_json_format_streaming(local_path, temp_path, repeated_fields=None):
    """Versión streaming de fix_json_format para archivos grandes.
    Procesa el archivo línea por línea o item por item sin cargar todo en memoria."""
    
    # Detectar campos array primero (usando una muestra pequeña)
    array_fields = set()
    if repeated_fields:
        array_fields = set(repeated_fields)
    
    # Leer una muestra para detectar campos array (solo primeras líneas/chars)
    sample_items = []
    sample_size = 1024 * 1024  # Leer primeros 1MB para muestra
    with open(local_path, 'r', encoding='utf-8') as f:
        first_char = f.read(1)
        f.seek(0)
        
        if first_char == '[':
            # JSON array tradicional - leer solo una muestra pequeña
            sample_content = f.read(sample_size)
            # Buscar items JSON en la muestra
            bracket_pos = sample_content.find('[')
            if bracket_pos != -1:
                depth = 0
                item_start = None
                in_string = False
                escape_next = False
                
                for i, char in enumerate(sample_content[bracket_pos+1:], start=bracket_pos+1):
                    if escape_next:
                        escape_next = False
                        continue
                    
                    if char == '\\':
                        escape_next = True
                        continue
                    
                    if char == '"' and not escape_next:
                        in_string = not in_string
                        continue
                    
                    if in_string:
                        continue
                    
                    if char == '[':
                        depth += 1
                    elif char == ']':
                        if depth == 0:
                            break
                        depth -= 1
                    elif char == '{' and depth == 0:
                        item_start = i
                    elif char == '}' and depth == 0 and item_start is not None:
                        try:
                            item_str = sample_content[item_start:i+1].strip()
                            if item_str and len(sample_items) < 100:
                                sample_items.append(json.loads(item_str))
                        except:
                            pass
                        item_start = None
        else:
            # Newline-delimited JSON - leer primeras 100 líneas
            for i, line in enumerate(f):
                if i >= 100:
                    break
                if line.strip():
                    try:
                        sample_items.append(json.loads(line))
                    except:
                        pass
    
    # Detectar campos array de la muestra
    detected_array_fields = set()
    for item in sample_items:
        for key, value in item.items():
            if isinstance(value, list):
                detected_array_fields.add(to_snake_case(key))
    array_fields = array_fields | detected_array_fields
    
    # Obtener tamaño del archivo para mostrar progreso
    file_size = os.path.getsize(local_path)
    file_size_mb = file_size / (1024 * 1024)
    print(f"📊 Procesando archivo grande ({file_size_mb:.2f} MB) con streaming...")
    
    # Procesar archivo completo de forma streaming
    start_time = time.time()
    items_processed = 0
    bytes_processed = 0
    last_progress_time = start_time
    
    with open(local_path, 'r', encoding='utf-8') as f_in, open(temp_path, 'w', encoding='utf-8') as f_out:
        first_char = f_in.read(1)
        f_in.seek(0)
        
        if first_char == '[':
            # JSON array tradicional - procesar item por item usando parser streaming
            # Leer en chunks y parsear objetos JSON completos
            # Usar chunks más grandes para archivos muy grandes (mejor rendimiento)
            chunk_size = 256 * 1024 if file_size_mb > 500 else 64 * 1024  # 256KB para archivos >500MB, 64KB para otros
            buffer = ""
            depth = 0
            item_start = None
            in_string = False
            escape_next = False
            found_start = False
            
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                
                bytes_processed += len(chunk)
                buffer += chunk
                
                i = 0
                while i < len(buffer):
                    char = buffer[i]
                    
                    if escape_next:
                        escape_next = False
                        i += 1
                        continue
                    
                    if char == '\\':
                        escape_next = True
                        i += 1
                        continue
                    
                    if char == '"' and not escape_next:
                        in_string = not in_string
                        i += 1
                        continue
                    
                    if in_string:
                        i += 1
                        continue
                    
                    if not found_start and char == '[':
                        found_start = True
                        i += 1
                        continue
                    
                    if not found_start:
                        i += 1
                        continue
                    
                    if char == '[':
                        depth += 1
                    elif char == ']':
                        if depth == 0:
                            # Fin del array - procesar último item si existe
                            if item_start is not None:
                                try:
                                    item_str = buffer[item_start:i].strip().rstrip(',').strip()
                                    if item_str:
                                        item = json.loads(item_str)
                                        new_item = transform_item(item, array_fields)
                                        f_out.write(json.dumps(new_item) + '\n')
                                        items_processed += 1
                                except:
                                    pass
                            buffer = buffer[i+1:]
                            break
                        depth -= 1
                    elif char == '{' and depth == 0:
                        item_start = i
                    elif char == '}' and depth == 0 and item_start is not None:
                        try:
                            item_str = buffer[item_start:i+1].strip()
                            if item_str:
                                item = json.loads(item_str)
                                new_item = transform_item(item, array_fields)
                                f_out.write(json.dumps(new_item) + '\n')
                                items_processed += 1
                                
                                # Mostrar progreso cada 10,000 items o cada 30 segundos
                                current_time = time.time()
                                if items_processed % 10000 == 0 or (current_time - last_progress_time) >= 30:
                                    progress_pct = (bytes_processed / file_size * 100) if file_size > 0 else 0
                                    elapsed = current_time - start_time
                                    print(f"⏳ Progreso: {items_processed:,} items procesados ({progress_pct:.1f}% del archivo, {elapsed:.1f}s)")
                                    last_progress_time = current_time
                        except:
                            pass
                        item_start = None
                        # Limpiar buffer hasta este punto para ahorrar memoria
                        buffer = buffer[i+1:]
                        i = -1  # Resetear índice después de limpiar
                    
                    i += 1
        else:
            # Newline-delimited JSON - procesar línea por línea (más eficiente)
            for line_num, line in enumerate(f_in, 1):
                if line.strip():
                    try:
                        item = json.loads(line)
                        new_item = transform_item(item, array_fields)
                        f_out.write(json.dumps(new_item) + '\n')
                        items_processed += 1
                        bytes_processed += len(line.encode('utf-8'))
                        
                        # Mostrar progreso cada 10,000 líneas o cada 30 segundos
                        current_time = time.time()
                        if items_processed % 10000 == 0 or (current_time - last_progress_time) >= 30:
                            progress_pct = (bytes_processed / file_size * 100) if file_size > 0 else 0
                            elapsed = current_time - start_time
                            print(f"⏳ Progreso: {items_processed:,} items procesados ({progress_pct:.1f}% del archivo, {elapsed:.1f}s)")
                            last_progress_time = current_time
                    except Exception as e:
                        # Continuar con la siguiente línea si hay error
                        pass
    
    total_time = time.time() - start_time
    print(f"✅ Transformación completada: {items_processed:,} items procesados en {total_time:.1f}s ({items_processed/total_time:.0f} items/seg)")

def transform_item(item, array_fields):
    """Transforma un item individual a snake_case en nivel superior, preserva camelCase en STRUCT."""
    new_item = {}
    for k, v in item.items():
        snake_key = to_snake_case(k)  # snake_case para campos de nivel superior
        
        # Procesar recursivamente: preserva camelCase dentro de STRUCT
        fixed_value = fix_nested_value(v, snake_key, array_fields)
        
        # Si el campo es un array y viene como NULL, convertir a array vacío
        if snake_key in array_fields and fixed_value is None:
            new_item[snake_key] = []
        else:
            new_item[snake_key] = fixed_value
    return new_item

def validate_json_file(file_path, max_lines_to_check=1000):
    """
    Valida rápidamente que un archivo JSON esté bien formado.
    Para archivos grandes, solo valida las primeras líneas.
    
    Args:
        file_path: Ruta al archivo JSON
        max_lines_to_check: Número máximo de líneas a validar (para archivos grandes)
    
    Returns:
        tuple: (is_valid: bool, error_message: str or None, json_type: 'array' or 'ndjson' or None)
    """
    try:
        # Verificar que el archivo existe y no está vacío
        if not os.path.exists(file_path):
            return (False, "Archivo no existe", None)
        
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            return (False, "Archivo JSON vacío", None)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            first_char = f.read(1)
            if not first_char:  # Archivo vacío después de leer
                return (False, "Archivo JSON vacío", None)
            f.seek(0)
            
            if first_char == '[':
                # JSON array tradicional - validar que sea JSON válido
                try:
                    # Para archivos grandes, solo leer una muestra
                    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                    if file_size_mb > 100:
                        # Archivo grande: validar solo estructura básica (primeros y últimos caracteres)
                        f.seek(0, 2)  # Ir al final
                        file_size = f.tell()
                        f.seek(0)
                        # Leer primeros 1KB y últimos 1KB
                        first_chunk = f.read(1024)
                        if file_size > 2048:
                            f.seek(file_size - 1024)
                            last_chunk = f.read(1024)
                            # Validar que empiece con [ y termine con ]
                            if not first_chunk.strip().startswith('['):
                                return (False, "Archivo JSON array no comienza con '['", None)
                            if not last_chunk.strip().endswith(']'):
                                return (False, "Archivo JSON array no termina con ']'", None)
                        else:
                            # Archivo pequeño: validar completo
                            f.seek(0)
                            json.load(f)
                    else:
                        # Archivo pequeño: validar completo
                        json.load(f)
                    return (True, None, 'array')
                except json.JSONDecodeError as e:
                    return (False, f"JSON array mal formado: {str(e)}", None)
            else:
                # Newline-delimited JSON - validar primeras líneas
                lines_checked = 0
                for line_num, line in enumerate(f, 1):
                    if line.strip():
                        try:
                            json.loads(line)
                            lines_checked += 1
                            if lines_checked >= max_lines_to_check:
                                break  # Ya validamos suficientes líneas
                        except json.JSONDecodeError as e:
                            return (False, f"Línea {line_num} mal formada (NDJSON): {str(e)}", None)
                
                if lines_checked == 0:
                    return (False, "Archivo JSON vacío o sin líneas válidas", None)
                
                return (True, None, 'ndjson')
    except UnicodeDecodeError as e:
        return (False, f"Error de encoding UTF-8: {str(e)}", None)
    except Exception as e:
        return (False, f"Error validando archivo JSON: {str(e)}", None)

def upload_to_bucket(bucket_name, project_id, local_file, dest_blob_name):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_filename(local_file)
    print(f"📤 Subido a gs://{bucket_name}/{dest_blob_name}")

def _schema_field_to_sql(field):
    """Convierte un SchemaField de BigQuery a definición SQL para ALTER TABLE"""
    field_name = field.name
    field_type = field.field_type
    
    # Si es STRUCT, construir la definición recursivamente
    if field_type == 'STRUCT':
        fields_sql = ', '.join([_schema_field_to_sql(f) for f in field.fields])
        struct_def = f"STRUCT<{fields_sql}>"
    else:
        struct_def = field_type
    
    # Agregar mode (NULLABLE, REQUIRED, REPEATED)
    mode = field.mode or 'NULLABLE'
    if mode == 'REPEATED':
        return f"{field_name} ARRAY<{struct_def}>"
    elif mode == 'REQUIRED':
        return f"{field_name} {struct_def} NOT NULL"
    else:
        return f"{field_name} {struct_def}"

def load_json_to_staging_with_error_handling(
    bq_client, temp_fixed, temp_json, table_ref_staging, 
    project_id, table_name, table_staging, dataset_staging,
    load_start, log_event_callback=None, 
    company_id=None, company_name=None, endpoint_name=None
):
    """
    Carga un archivo JSON a BigQuery staging con detección y corrección automática de errores.
    
    Esta función maneja:
    - Errores de tipo de dato (type_mismatch): corrige esquema automáticamente
    - Campos REPEATED con NULL: limpia datos
    - Campos anidados incorrectos: limpia datos
    
    Args:
        bq_client: Cliente de BigQuery
        temp_fixed: Ruta al archivo JSON transformado (newline-delimited)
        temp_json: Ruta al archivo JSON original (por si necesita re-transformar)
        table_ref_staging: Referencia a la tabla staging
        project_id: ID del proyecto
        table_name: Nombre de la tabla
        table_staging: Nombre de la tabla staging
        dataset_staging: Nombre del dataset staging
        load_start: Tiempo de inicio (time.time())
        log_event_callback: Función para logging (opcional)
        company_id, company_name, endpoint_name: Para logging (opcionales)
    
    Returns:
        tuple: (success: bool, load_time: float, error_message: str or None)
    """
    # Obtener esquema autodetectado de una muestra para usar en la carga
    schema = None
    try:
        # Leer primeras líneas para obtener esquema
        sample_file = f"/tmp/sample_schema_{project_id}_{table_name}.json"
        with open(temp_fixed, 'r', encoding='utf-8') as f_in:
            with open(sample_file, 'w', encoding='utf-8') as f_out:
                # Leer primeras 100 líneas para obtener esquema
                for i, line in enumerate(f_in):
                    if i >= 100:
                        break
                    f_out.write(line)
        
        # Cargar muestra para obtener esquema autodetectado
        sample_table_ref = bq_client.dataset(dataset_staging).table(f"{table_staging}_schema_sample")
        sample_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        try:
            sample_load_job = bq_client.load_table_from_file(
                open(sample_file, "rb"),
                sample_table_ref,
                job_config=sample_config
            )
            sample_load_job.result()
            sample_table = bq_client.get_table(sample_table_ref)
            schema = sample_table.schema
            bq_client.delete_table(sample_table_ref, not_found_ok=True)
            if os.path.exists(sample_file):
                os.remove(sample_file)
        except Exception as sample_error:
            # Si falla la muestra, continuar con autodetect normal
            schema = None
            bq_client.delete_table(sample_table_ref, not_found_ok=True)
            if os.path.exists(sample_file):
                os.remove(sample_file)
    except Exception as schema_error:
        # Si hay error obteniendo esquema, continuar con autodetect normal
        schema = None
    
    # Configurar job_config con esquema corregido o autodetect
    if schema:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    else:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    
    # Intentar cargar directamente
    load_job = None
    try:
        load_job = bq_client.load_table_from_file(
            open(temp_fixed, "rb"),
            table_ref_staging,
            job_config=job_config
        )
        load_job.result()
        load_time = time.time() - load_start
        return (True, load_time, None)
    except Exception as e:
        error_msg = str(e)
        problematic_field = None
        needs_fix = False
        fix_type = None  # 'repeated', 'nested', 'type_mismatch'
        
        # Intentar obtener errores detallados del job de BigQuery
        # El mensaje de excepción puede ser genérico, pero el job tiene errores detallados
        detailed_errors = []
        if load_job:
            try:
                # Esperar a que el job termine (si no ha terminado)
                try:
                    load_job.result()
                except:
                    pass
                
                # Obtener errores del job
                if hasattr(load_job, 'errors') and load_job.errors:
                    detailed_errors = load_job.errors
                elif hasattr(load_job, 'job_id'):
                    # Obtener el job completo para acceder a los errores
                    try:
                        job = bq_client.get_job(load_job.job_id)
                        if hasattr(job, 'errors') and job.errors:
                            detailed_errors = job.errors
                    except:
                        pass
            except:
                pass
        
        # Construir mensaje de error más detallado
        if detailed_errors:
            error_details = []
            for err in detailed_errors:
                err_msg = err.get('message', '') if isinstance(err, dict) else str(err)
                error_details.append(err_msg)
                # Extraer campo del mensaje detallado si está disponible
                if 'Field:' in err_msg and not problematic_field:
                    field_match = re.search(r'Field:\s*([\w_]+)', err_msg, re.IGNORECASE)
                    if field_match:
                        problematic_field = field_match.group(1)
            
            # Agregar detalles al mensaje de error y mostrarlo
            if error_details:
                detailed_msg = "\n".join([f"  • {detail}" for detail in error_details[:5]])
                print(f"❌ Error detallado de BigQuery:\n{detailed_msg}")
                error_msg = f"{error_msg}\n\n📋 Detalles del error:\n{detailed_msg}"
        
        # Intentar extraer campo REPEATED del mensaje de error
        # Formato 1: "Field: permissions; Value: NULL"
        match = re.search(r'Field:\s*(\w+);\s*Value:\s*NULL', error_msg, re.IGNORECASE)
        
        # Formato 2: "JSON object specified for non-record field: items.serialNumbers"
        match2 = re.search(r'non-record field:\s*([\w.]+)', error_msg, re.IGNORECASE)
        
        # Formato 3: Error de tipo de dato - puede estar en múltiples niveles del mensaje
        # "Could not convert value 'string_value: "S72-85922420"' to integer. Field: job_number"
        # También puede estar en: "JSON parsing error... Could not convert... Field: job_number"
        match3 = re.search(r'Could not convert.*?Field:\s*([\w_]+)', error_msg, re.IGNORECASE | re.DOTALL)
        
        # Formato 4: "JSON table encountered too many errors" - buscar en los detalles anidados
        # El error real puede estar en los detalles del error, buscar cualquier "Field: X"
        # El mensaje puede tener múltiples niveles: "JSON table encountered too many errors... JSON parsing error... Field: job_number"
        if ('JSON table encountered too many errors' in error_msg or 'JSON parsing error' in error_msg) and not match3:
            # Buscar el campo problemático en el mensaje completo (puede estar en cualquier parte)
            # Buscar todos los campos mencionados y usar el último (más probable que sea el problemático)
            all_fields = re.findall(r'Field:\s*([\w_]+)', error_msg, re.IGNORECASE)
            if all_fields:
                # Usar el último campo encontrado (generalmente el más específico)
                problematic_field = all_fields[-1]
                needs_fix = True
                fix_type = 'type_mismatch'
                print(f"🔍 Error de tipo de dato detectado (desde errores anidados): {problematic_field}")
                print(f"🔧 Corrigiendo esquema: convirtiendo campo {problematic_field} a STRING")
                match3 = type('Match', (), {'group': lambda self, n: problematic_field})()  # Crear objeto match simulado
        
        if match:
            problematic_field = match.group(1)
            needs_fix = True
            fix_type = 'repeated'
            print(f"🔍 Campo REPEATED detectado del error: {problematic_field}")
            print(f"🧹 Limpiando datos: convirtiendo NULL a [] para campo {problematic_field}")
        elif match2:
            problematic_field = match2.group(1)
            needs_fix = True
            fix_type = 'nested'
            print(f"🔍 Campo anidado con tipo incorrecto detectado: {problematic_field}")
            print(f"🧹 Limpiando datos: corrigiendo campo {problematic_field} que viene como objeto pero debería ser array")
        elif match3:
            problematic_field = match3.group(1)
            needs_fix = True
            fix_type = 'type_mismatch'
            print(f"🔍 Error de tipo de dato detectado: {problematic_field}")
            print(f"🔧 Corrigiendo esquema: convirtiendo campo {problematic_field} a STRING")
        
        if needs_fix and problematic_field:
            if fix_type == 'type_mismatch':
                # Error de tipo de dato: corregir esquema
                try:
                    print(f"🔧 Intentando corregir esquema para campo {problematic_field}...")
                    
                    # Paso 1: Obtener esquema de una muestra pequeña
                    sample_file = f"/tmp/sample_{project_id}_{table_name}.json"
                    with open(temp_fixed, 'r', encoding='utf-8') as f_in:
                        with open(sample_file, 'w', encoding='utf-8') as f_out:
                            for i, line in enumerate(f_in):
                                if i >= 100:
                                    break
                                f_out.write(line)
                    
                    # Cargar muestra para obtener esquema autodetectado
                    sample_table_ref = bq_client.dataset(dataset_staging).table(f"{table_staging}_sample")
                    sample_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        autodetect=True,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                    )
                    
                    try:
                        sample_load_job = bq_client.load_table_from_file(
                            open(sample_file, "rb"),
                            sample_table_ref,
                            job_config=sample_config
                        )
                        sample_load_job.result()
                        sample_table = bq_client.get_table(sample_table_ref)
                        schema = sample_table.schema
                        bq_client.delete_table(sample_table_ref, not_found_ok=True)
                        if os.path.exists(sample_file):
                            os.remove(sample_file)
                    except Exception as sample_error:
                        schema = None
                        bq_client.delete_table(sample_table_ref, not_found_ok=True)
                        if os.path.exists(sample_file):
                            os.remove(sample_file)
                        raise ValueError(f"No se pudo obtener esquema de muestra: {str(sample_error)}")
                    
                    # Paso 2: Corregir el campo problemático a STRING en el esquema
                    if schema:
                        corrected_schema = []
                        field_found = False
                        for field in schema:
                            if field.name == problematic_field:
                                corrected_field = bigquery.SchemaField(
                                    field.name,
                                    'STRING',
                                    mode=field.mode,
                                    fields=field.fields,
                                    description=field.description
                                )
                                corrected_schema.append(corrected_field)
                                field_found = True
                                print(f"✅ Campo {problematic_field} convertido de {field.field_type} a STRING en esquema")
                            else:
                                corrected_schema.append(field)
                        
                        if not field_found:
                            raise ValueError(f"Campo {problematic_field} no encontrado en esquema autodetectado")
                        
                        # Paso 3: Borrar tabla staging y cargar archivo completo con esquema corregido
                        bq_client.delete_table(table_ref_staging, not_found_ok=True)
                        
                        fixed_job_config = bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                            schema=corrected_schema,
                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                        )
                        
                        load_job = bq_client.load_table_from_file(
                            open(temp_fixed, "rb"),
                            table_ref_staging,
                            job_config=fixed_job_config
                        )
                        load_job.result()
                        load_time = time.time() - load_start
                        print(f"✅ Cargado a tabla staging con esquema corregido en {load_time:.1f}s")
                        return (True, load_time, None)
                    else:
                        raise ValueError("No se pudo obtener esquema para corrección automática")
                except Exception as schema_error:
                    error_msg = f"Error corrigiendo esquema: {str(schema_error)}"
                    print(f"❌ {error_msg}")
                    if log_event_callback:
                        log_event_callback(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error corrigiendo esquema",
                            event_message=f"Error corrigiendo esquema para campo {problematic_field}: {str(schema_error)}"
                        )
                    return (False, time.time() - load_start, error_msg)
            else:
                # Limpiar datos con el campo detectado (REPEATED y nested)
                field_name = problematic_field.split('.')[-1] if '.' in problematic_field else problematic_field
                print(f"🔧 Campo a limpiar: {field_name}")
                fix_json_format(temp_json, temp_fixed, repeated_fields={field_name})
                
                # Reintentar carga
                try:
                    load_job = bq_client.load_table_from_file(
                        open(temp_fixed, "rb"),
                        table_ref_staging,
                        job_config=job_config
                    )
                    load_job.result()
                    load_time = time.time() - load_start
                    print(f"✅ Cargado a tabla staging: {dataset_staging}.{table_staging} (después de limpieza) en {load_time:.1f}s")
                    return (True, load_time, None)
                except Exception as retry_error:
                    error_msg = f"Error cargando a tabla staging después de limpiar {problematic_field}: {str(retry_error)}"
                    print(f"❌ {error_msg}")
                    if log_event_callback:
                        log_event_callback(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error cargando a staging (después de limpieza)",
                            event_message=error_msg
                        )
                    return (False, time.time() - load_start, error_msg)
        else:
            # Error no reconocido o no se pudo detectar el campo problemático
            if log_event_callback:
                log_event_callback(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Error cargando a staging",
                    event_message=f"Error cargando a tabla staging: {error_msg}"
                )
            return (False, time.time() - load_start, error_msg)
