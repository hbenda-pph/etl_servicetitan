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
    
    Returns:
        Lista de tuplas [(endpoint_name, table_name), ...]
    """
    try:
        client = bigquery.Client(project=METADATA_PROJECT)
        query = f'''
            SELECT DISTINCT 
                endpoint.name AS endpoint_name,
                table_name
            FROM `{METADATA_PROJECT}.{METADATA_DATASET}.{METADATA_TABLE}`
            WHERE endpoint IS NOT NULL
              AND endpoint.name IS NOT NULL
              AND table_name IS NOT NULL
              AND active = TRUE
              AND silver_use_bronze = TRUE
            ORDER BY endpoint.name
        '''
        results = client.query(query).result()
        endpoints = [(row.endpoint_name, row.table_name) for row in results]
        print(f"📋 Cargados {len(endpoints)} endpoints desde metadata")
        return endpoints
    except Exception as e:
        print(f"⚠️  Error cargando endpoints desde metadata: {str(e)}")
        return []

def to_snake_case(name):
    """Convierte un nombre de camelCase o PascalCase a snake_case"""
    # Insertar underscore antes de mayúsculas seguidas de minúsculas
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Insertar underscore antes de mayúsculas que siguen a minúsculas o números
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

def get_standardized_table_name(endpoint):
    """
    Obtiene el nombre estandarizado de la tabla desde metadata_consolidated_tables.
    Si no se encuentra en metadata, usa normalización por defecto.
    """
    # Cache para evitar consultas repetidas
    if not hasattr(get_standardized_table_name, '_cache'):
        get_standardized_table_name._cache = {}
    
    cache_key = endpoint.lower()
    if cache_key in get_standardized_table_name._cache:
        return get_standardized_table_name._cache[cache_key]
    
    try:
        client = bigquery.Client(project=METADATA_PROJECT)
        query = f'''
            SELECT DISTINCT table_name
            FROM `{METADATA_PROJECT}.{METADATA_DATASET}.{METADATA_TABLE}`
            WHERE LOWER(endpoint_name) = LOWER(@endpoint)
            AND silver_use_bronze = TRUE
            AND table_name IS NOT NULL
            LIMIT 1
        '''
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("endpoint", "STRING", endpoint)
            ]
        )
        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())
        
        if results:
            table_name = results[0].table_name
            get_standardized_table_name._cache[cache_key] = table_name
            print(f"📋 Tabla estandarizada desde metadata: {endpoint} -> {table_name}")
            return table_name
        else:
            # Si no se encuentra en metadata, usar normalización por defecto
            print(f"⚠️  Endpoint '{endpoint}' no encontrado en metadata, usando normalización por defecto")
            normalized = _normalize_table_name_fallback(endpoint)
            get_standardized_table_name._cache[cache_key] = normalized
            return normalized
            
    except Exception as e:
        # En caso de error, usar normalización por defecto
        print(f"⚠️  Error consultando metadata para '{endpoint}': {str(e)}. Usando normalización por defecto")
        normalized = _normalize_table_name_fallback(endpoint)
        get_standardized_table_name._cache[cache_key] = normalized
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
    """
    Función recursiva para corregir valores anidados.
    - Convierte NULL a [] para campos array conocidos
    - Corrige objetos que deberían ser arrays (ej: serialNumbers)
    - Preserva nombres originales (camelCase) dentro de STRUCT
    """
    if value is None:
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
    if repeated_fields is None:
        repeated_fields = set()
        for item in json_data[:1000]:  # Muestra de primeros 1000 items
            for k, v in item.items():
                snake_key = to_snake_case(k)
                if isinstance(v, list):
                    repeated_fields.add(snake_key)
    
    # Transformar cada item
    transformed_items = []
    for item in json_data:
        transformed_item = transform_item(item, repeated_fields)
        transformed_items.append(transformed_item)
    
    # Escribir como newline-delimited JSON
    with open(temp_path, 'w', encoding='utf-8') as f:
        for item in transformed_items:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    print(f"✅ Transformación completada: {len(transformed_items):,} items procesados")

def fix_json_format_streaming(local_path, temp_path, repeated_fields=None):
    """Versión streaming de fix_json_format para archivos grandes.
    Procesa línea por línea para evitar cargar todo en memoria.
    Usa json.JSONDecoder para parsear JSON arrays de forma incremental."""
    
    items_processed = 0
    start_time = time.time()
    last_progress_time = start_time
    
    # Detectar campos array en una muestra
    if repeated_fields is None:
        repeated_fields = set()
        with open(local_path, 'r', encoding='utf-8') as f:
            first_char = f.read(1)
            f.seek(0)
            
            if first_char == '[':
                # JSON array - leer primeros items usando decoder incremental
                decoder = json.JSONDecoder()
                buffer = ""
                items = []
                chunk_size = 1024 * 1024  # 1MB chunks
                
                # Leer primer chunk
                chunk = f.read(chunk_size)
                if chunk:
                    buffer = chunk
                    idx = 0
                    # Saltar el '[' inicial
                    if buffer[idx] == '[':
                        idx += 1
                    
                    # Parsear items hasta tener 100 o terminar el chunk
                    while idx < len(buffer) and len(items) < 100:
                        # Buscar el siguiente item
                        buffer = buffer[idx:].lstrip()
                        if not buffer or buffer[0] == ']':
                            break
                        
                        try:
                            obj, idx = decoder.raw_decode(buffer)
                            items.append(obj)
                            # Avanzar después del item (saltar coma si existe)
                            buffer = buffer[idx:].lstrip()
                            if buffer and buffer[0] == ',':
                                buffer = buffer[1:].lstrip()
                            idx = 0
                        except (ValueError, json.JSONDecodeError):
                            # Necesitamos más datos, pero ya tenemos suficientes items
                            if len(items) >= 10:
                                break
                            break
            else:
                # Newline-delimited JSON - leer primeras líneas
                items = []
                for i, line in enumerate(f):
                    if i >= 100:
                        break
                    try:
                        items.append(json.loads(line.strip()))
                    except:
                        pass
        
        # Detectar campos array
        for item in items:
            if isinstance(item, dict):
                for k, v in item.items():
                    snake_key = to_snake_case(k)
                    if isinstance(v, list):
                        repeated_fields.add(snake_key)
    
    # Procesar archivo completo
    with open(local_path, 'r', encoding='utf-8') as f_in:
        with open(temp_path, 'w', encoding='utf-8') as f_out:
            first_char = f_in.read(1)
            f_in.seek(0)
            
            if first_char == '[':
                # JSON array - usar decoder incremental para parsear streaming
                decoder = json.JSONDecoder()
                buffer = ""
                chunk_size = 64 * 1024  # 64KB chunks para balance entre memoria y eficiencia
                array_started = False
                
                # Leer y procesar en chunks
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk and not buffer:
                        break
                    
                    if chunk:
                        buffer += chunk
                    
                    # Si es el primer chunk, saltar el '[' inicial
                    if not array_started and buffer:
                        buffer = buffer.lstrip()
                        if buffer and buffer[0] == '[':
                            buffer = buffer[1:].lstrip()
                            array_started = True
                        elif buffer and buffer[0] == ']':
                            # Array vacío
                            break
                    
                    # Parsear todos los items completos en el buffer
                    while buffer:
                        buffer = buffer.lstrip()
                        if not buffer or buffer[0] == ']':
                            break
                        
                        try:
                            obj, consumed = decoder.raw_decode(buffer)
                            transformed = transform_item(obj, repeated_fields)
                            f_out.write(json.dumps(transformed, ensure_ascii=False) + '\n')
                            items_processed += 1
                            
                            # Avanzar después del item
                            buffer = buffer[consumed:].lstrip()
                            # Saltar coma si existe
                            if buffer and buffer[0] == ',':
                                buffer = buffer[1:].lstrip()
                            
                            # Mostrar progreso cada 10 segundos
                            current_time = time.time()
                            if current_time - last_progress_time >= 10:
                                elapsed = current_time - start_time
                                rate = items_processed / elapsed if elapsed > 0 else 0
                                print(f"🔄 Procesando... {items_processed:,} items ({rate:.0f} items/seg)")
                                last_progress_time = current_time
                        except (ValueError, json.JSONDecodeError) as e:
                            # Item incompleto, necesitamos más datos del siguiente chunk
                            # Si no hay más chunk, puede ser un error real
                            if not chunk:
                                if items_processed == 0:
                                    print(f"⚠️  Error parseando JSON: {str(e)[:200]}")
                                break
                            break
                    
                    # Si no hay más datos y el buffer está vacío o solo tiene ']', terminamos
                    if not chunk:
                        break
            else:
                # Newline-delimited JSON - más simple
                for line in f_in:
                    if line.strip():
                        try:
                            item = json.loads(line)
                            transformed = transform_item(item, repeated_fields)
                            f_out.write(json.dumps(transformed, ensure_ascii=False) + '\n')
                            items_processed += 1
                            
                            # Mostrar progreso cada 10 segundos
                            current_time = time.time()
                            if current_time - last_progress_time >= 10:
                                elapsed = current_time - start_time
                                rate = items_processed / elapsed if elapsed > 0 else 0
                                print(f"🔄 Procesando... {items_processed:,} items ({rate:.0f} items/seg)")
                                last_progress_time = current_time
                        except Exception as e:
                            # Log error pero continuar
                            if items_processed == 0:
                                print(f"⚠️  Error parseando línea: {str(e)[:100]}")
                            pass
    
    total_time = time.time() - start_time
    if items_processed == 0:
        print(f"❌ ERROR: No se procesaron items. El archivo puede estar vacío o mal formado.")
    else:
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

def align_schemas_before_merge(bq_client, staging_table, final_table, project_id, dataset_final, table_final):
    """
    Verifica y corrige incompatibilidades de esquema entre staging y final ANTES del MERGE.
    
    Esta función:
    1. Compara los tipos de datos de campos comunes entre staging y final
    2. Si hay incompatibilidades (ej: INTEGER vs STRING, TIMESTAMP vs STRING), corrige el esquema de final
    3. Retorna True si se hicieron correcciones, False si no hay incompatibilidades
    
    Args:
        bq_client: Cliente de BigQuery
        staging_table: Tabla de staging (objeto Table)
        final_table: Tabla final (objeto Table)
        project_id: ID del proyecto
        dataset_final: Nombre del dataset final
        table_final: Nombre de la tabla final
    
    Returns:
        tuple: (needs_correction: bool, corrections_made: list, error_message: str or None)
    """
    corrections_made = []
    staging_schema = staging_table.schema
    final_schema = final_table.schema
    
    # Crear diccionarios para acceso rápido
    staging_fields = {f.name: f for f in staging_schema if not f.name.startswith('_etl_')}
    final_fields = {f.name: f for f in final_schema if not f.name.startswith('_etl_')}
    
    # Encontrar campos comunes con tipos incompatibles
    incompatible_fields = []
    
    for field_name in staging_fields:
        if field_name in final_fields:
            staging_field = staging_fields[field_name]
            final_field = final_fields[field_name]
            
            # Comparar tipos (ignorar STRUCT por ahora, se manejan después del MERGE)
            if staging_field.field_type != final_field.field_type:
                # Tipos incompatibles detectados
                incompatible_fields.append({
                    'name': field_name,
                    'staging_type': staging_field.field_type,
                    'final_type': final_field.field_type,
                    'staging_field': staging_field,
                    'final_field': final_field
                })
    
    if not incompatible_fields:
        return (False, [], None)
    
    # Mostrar incompatibilidades detectadas
    print(f"🔍 Incompatibilidades de esquema detectadas ({len(incompatible_fields)} campos):")
    for inc in incompatible_fields:
        print(f"  • {inc['name']}: staging={inc['staging_type']}, final={inc['final_type']}")
    
    # Corregir cada incompatibilidad
    print(f"🔧 Corrigiendo esquema de tabla final para alinearlo con staging...")
    
    for inc in incompatible_fields:
        field_name = inc['name']
        staging_field = inc['staging_field']
        new_type = inc['staging_type']
        old_type = inc['final_type']
        
        try:
            # Para tipos simples (no STRUCT), usar ALTER TABLE para cambiar el tipo
            if staging_field.field_type != 'STRUCT':
                # BigQuery no permite cambiar tipos directamente con ALTER TABLE
                # Necesitamos usar una estrategia diferente:
                # 1. Agregar columna temporal con el tipo correcto
                # 2. Copiar datos con CAST
                # 3. Eliminar columna vieja
                # 4. Renombrar columna temporal
                
                # Simplificación: actualizar el esquema directamente (solo funciona si la tabla está vacía o BigQuery lo permite)
                # Si la tabla tiene datos, necesitamos una estrategia más compleja
                
                # Intentar actualizar el esquema directamente
                updated_schema = []
                for field in final_schema:
                    if field.name == field_name:
                        # Reemplazar con el campo de staging
                        updated_schema.append(staging_field)
                    else:
                        updated_schema.append(field)
                
                final_table.schema = updated_schema
                bq_client.update_table(final_table, ['schema'])
                
                corrections_made.append({
                    'field': field_name,
                    'old_type': old_type,
                    'new_type': new_type
                })
                print(f"  ✅ Campo {field_name} actualizado: {old_type} → {new_type}")
            else:
                # STRUCT: más complejo, se manejará después del MERGE si es necesario
                print(f"  ⚠️  Campo {field_name} es STRUCT, se manejará después del MERGE si es necesario")
        
        except Exception as e:
            error_msg = f"Error corrigiendo campo {field_name}: {str(e)}"
            print(f"  ❌ {error_msg}")
            return (True, corrections_made, error_msg)
    
    if corrections_made:
        print(f"✅ Esquema corregido: {len(corrections_made)} campos actualizados")
        return (True, corrections_made, None)
    else:
        return (False, [], None)

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
                # Obtener errores del job (el job ya terminó con error)
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
                    
                    # Estrategia simplificada: crear esquema directamente con el campo corregido
                    # Obtener esquema actual de staging (si existe) o crear uno nuevo
                    try:
                        current_staging_table = bq_client.get_table(table_ref_staging)
                        current_schema = list(current_staging_table.schema)
                    except:
                        # Tabla no existe, necesitamos inferir esquema de una muestra
                        sample_file = f"/tmp/sample_{project_id}_{table_name}.json"
                        with open(temp_fixed, 'r', encoding='utf-8') as f_in:
                            with open(sample_file, 'w', encoding='utf-8') as f_out:
                                for i, line in enumerate(f_in):
                                    if i >= 50:  # Solo primeras 50 líneas para inferir esquema
                                        break
                                    f_out.write(line)
                        
                        # Cargar muestra para obtener esquema autodetectado
                        sample_table_ref = bq_client.dataset(dataset_staging).table(f"{table_staging}_sample")
                        sample_config = bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                            autodetect=True,
                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                        )
                        
                        sample_load_job = bq_client.load_table_from_file(
                            open(sample_file, "rb"),
                            sample_table_ref,
                            job_config=sample_config
                        )
                        sample_load_job.result()
                        sample_table = bq_client.get_table(sample_table_ref)
                        current_schema = list(sample_table.schema)  # Convertir a lista para poder modificar
                        bq_client.delete_table(sample_table_ref, not_found_ok=True)
                        if os.path.exists(sample_file):
                            os.remove(sample_file)
                    
                    # Crear nuevo SchemaField con el tipo corregido a STRING
                    corrected_field_new = bigquery.SchemaField(
                        name=problematic_field,
                        field_type='STRING',  # Siempre corregir a STRING
                        mode='NULLABLE',
                        description=None,
                        fields=None
                    )
                    
                    # Reemplazar o agregar el campo corregido en el esquema
                    updated_schema = []
                    field_replaced = False
                    for field in current_schema:
                        if field.name == problematic_field:
                            # Reemplazar con el campo corregido
                            updated_schema.append(corrected_field_new)
                            field_replaced = True
                        else:
                            updated_schema.append(field)
                    
                    if not field_replaced:
                        # Campo no existe en esquema, agregarlo
                        updated_schema.append(corrected_field_new)
                    
                    # Actualizar o crear tabla staging con esquema corregido
                    staging_table_obj = bigquery.Table(table_ref_staging, schema=updated_schema)
                    try:
                        bq_client.update_table(staging_table_obj, ['schema'])
                    except:
                        # Tabla no existe, crearla
                        bq_client.create_table(staging_table_obj)
                    
                    print(f"✅ Campo {problematic_field} convertido a STRING en esquema")
                    
                    # Reintentar carga con esquema corregido
                    job_config.schema = updated_schema
                    load_job_retry = bq_client.load_table_from_file(
                        open(temp_fixed, "rb"),
                        table_ref_staging,
                        job_config=job_config
                    )
                    load_job_retry.result()
                    load_time = time.time() - load_start
                    return (True, load_time, None)
                except Exception as schema_error:
                    return (False, 0, f"Error corrigiendo esquema: {str(schema_error)}")
            elif fix_type == 'repeated':
                # Campo REPEATED con NULL: re-transformar datos
                print(f"🧹 Re-transformando datos para limpiar NULLs en campo REPEATED {problematic_field}...")
                # Esta lógica ya está en fix_json_format, solo necesitamos re-ejecutarla
                # Por ahora, retornar error para que se maneje en el nivel superior
                return (False, 0, f"Campo REPEATED {problematic_field} tiene valores NULL. Se requiere limpieza de datos.")
            elif fix_type == 'nested':
                # Campo anidado incorrecto: re-transformar datos
                print(f"🧹 Re-transformando datos para corregir campo anidado {problematic_field}...")
                return (False, 0, f"Campo anidado {problematic_field} tiene tipo incorrecto. Se requiere limpieza de datos.")
        
        # Si no se pudo corregir, retornar error
        load_time = time.time() - load_start
        return (False, load_time, error_msg)

def execute_merge_or_insert(
    bq_client, staging_table, final_table, project_id, dataset_final, table_final,
    dataset_staging, table_staging, merge_start, log_event_callback=None,
    company_id=None, company_name=None, endpoint_name=None
):
    """
    Ejecuta MERGE o INSERT directo dependiendo de si la tabla final está vacía.
    
    Esta función:
    - Verifica si la tabla final está vacía (primera carga)
    - Si está vacía: usa INSERT directo (más eficiente)
    - Si tiene datos: usa MERGE incremental con soft delete
    
    Args:
        bq_client: Cliente de BigQuery
        staging_table: Tabla de staging (objeto Table)
        final_table: Tabla final (objeto Table)
        project_id: ID del proyecto
        dataset_final: Nombre del dataset final
        table_final: Nombre de la tabla final
        dataset_staging: Nombre del dataset staging
        table_staging: Nombre de la tabla staging
        merge_start: Tiempo de inicio (time.time())
        log_event_callback: Función para logging (opcional)
        company_id, company_name, endpoint_name: Para logging (opcionales)
    
    Returns:
        tuple: (success: bool, merge_time: float, error_message: str or None)
    """
    staging_schema = staging_table.schema
    final_schema = final_table.schema
    
    # Obtener nombres de columnas (excluyendo campos ETL y id)
    staging_cols = {col.name for col in staging_schema if col.name != 'id' and not col.name.startswith('_etl_')}
    
    # Verificar si la tabla final está vacía (primera carga)
    is_first_load = final_table.num_rows == 0
    
    if is_first_load:
        # Primera carga: usar INSERT directo (más eficiente y evita problemas de MERGE)
        print(f"📥 Primera carga detectada (tabla vacía). Usando INSERT directo en lugar de MERGE...")
        
        # Para INSERT, usar todas las columnas de staging (excepto ETL) + campos ETL
        insert_cols = [col.name for col in staging_schema if not col.name.startswith('_etl_')]
        insert_cols_with_etl = insert_cols + ['_etl_synced', '_etl_operation']
        insert_values = [f'S.{col.name}' for col in staging_schema if not col.name.startswith('_etl_')]
        insert_values_with_etl = insert_values + ['CURRENT_TIMESTAMP()', "'INSERT'"]
        
        insert_sql = f'''
            INSERT INTO `{project_id}.{dataset_final}.{table_final}` (
                {', '.join(insert_cols_with_etl)}
            )
            SELECT 
                {', '.join(insert_values_with_etl)}
            FROM `{project_id}.{dataset_staging}.{table_staging}` S
        '''
        
        try:
            query_job = bq_client.query(insert_sql)
            query_job.result()
            # Obtener número de filas insertadas desde staging
            from google.cloud.bigquery import TableReference
            table_ref_staging = TableReference.from_string(f"{project_id}.{dataset_staging}.{table_staging}")
            staging_table_refresh = bq_client.get_table(table_ref_staging)
            rows_inserted = staging_table_refresh.num_rows
            merge_time = time.time() - merge_start
            print(f"✅ INSERT directo ejecutado: {dataset_final}.{table_final} poblado con {rows_inserted:,} filas en {merge_time:.1f}s")
            return (True, merge_time, None)
        except Exception as e:
            error_msg = str(e)
            merge_time = time.time() - merge_start
            print(f"❌ INSERT directo falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {error_msg}")
            if log_event_callback:
                log_event_callback(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Error en INSERT directo",
                    event_message=f"Error en INSERT directo: {error_msg}"
                )
            return (False, merge_time, error_msg)
    else:
        # Tabla tiene datos: usar MERGE incremental
        print(f"🔄 Tabla tiene datos. Usando MERGE incremental...")
        
        # Construir UPDATE SET solo con columnas comunes
        update_set = ', '.join([f'T.{col} = S.{col}' for col in sorted(staging_cols)])
        
        # Para INSERT, usar todas las columnas de staging (excepto ETL)
        insert_cols = [col.name for col in staging_schema if not col.name.startswith('_etl_')]
        insert_values = [f'S.{col.name}' for col in staging_schema if not col.name.startswith('_etl_')]
        
        # MERGE incremental a tabla final con Soft Delete y campos ETL
        merge_sql = f'''
            MERGE `{project_id}.{dataset_final}.{table_final}` T
            USING `{project_id}.{dataset_staging}.{table_staging}` S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET 
                {update_set},
                T._etl_synced = CURRENT_TIMESTAMP(),
                T._etl_operation = 'UPDATE'
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(insert_cols)},
                _etl_synced, _etl_operation
            ) VALUES (
                {', '.join(insert_values)},
                CURRENT_TIMESTAMP(), 'INSERT'
            )
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
                T._etl_synced = CURRENT_TIMESTAMP(),
                T._etl_operation = 'DELETE'
        '''
        
        try:
            query_job = bq_client.query(merge_sql)
            query_job.result()
            merge_time = time.time() - merge_start
            print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado en {merge_time:.1f}s")
            return (True, merge_time, None)
        except Exception as e:
            error_msg = str(e)
            merge_time = time.time() - merge_start
            print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {error_msg}")
            if log_event_callback:
                log_event_callback(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Error en MERGE",
                    event_message=f"Error en MERGE: {error_msg}"
                )
            return (False, merge_time, error_msg)
