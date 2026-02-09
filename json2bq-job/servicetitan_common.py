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
logging.getLogger("google.auth").setLevel(logging.WARNING)
logging.getLogger("google.auth.transport").setLevel(logging.WARNING)

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
    return "platform-partners-qua"  # Fallback por defecto

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

# Configuración para logging centralizado (usar project_id real)
def get_logs_project():
    """Obtiene el proyecto para logging"""
    return get_bigquery_project_id()

LOGS_PROJECT = get_logs_project()
LOGS_DATASET = "logs"
LOGS_TABLE = "etl_servicetitan"

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
        client = bigquery.Client(project=LOGS_PROJECT)
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
