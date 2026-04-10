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
        Lista de tuplas [(endpoint_name, table_name, use_merge, is_production), ...]
        use_merge=False       → usa CREATE OR REPLACE TABLE (snapshot completo) en lugar de MERGE.
        is_production=False → endpoint en desarrollo: fuerza CREATE OR REPLACE TABLE siempre,
                                ignorando use_merge, para mantener schema limpio.
    """
    try:
        client = bigquery.Client(project=METADATA_PROJECT)
        query = f'''
            SELECT DISTINCT 
                endpoint.name AS endpoint_name,
                table_name,
                COALESCE(use_merge, TRUE) AS use_merge,
                COALESCE(is_production, FALSE) AS is_production
            FROM `{METADATA_PROJECT}.{METADATA_DATASET}.{METADATA_TABLE}`
            WHERE endpoint IS NOT NULL
              AND endpoint.name IS NOT NULL
              AND table_name IS NOT NULL
              AND active = TRUE
              AND silver_use_bronze = TRUE
            ORDER BY endpoint.name
        '''
        results = client.query(query).result()
        endpoints = [(row.endpoint_name, row.table_name, row.use_merge, row.is_production) for row in results]
        overwrite_eps  = [t for _, t, m, c in endpoints if not m]
        dev_eps        = [t for _, t, m, c in endpoints if not c]
        if dev_eps:
            print(f"📋 Cargados {len(endpoints)} endpoints | OVERWRITE: {overwrite_eps or 'ninguno'} | En desarrollo (CREATE OR REPLACE): {dev_eps}")
        else:
            print(f"📋 Cargados {len(endpoints)} endpoints | OVERWRITE: {overwrite_eps or 'ninguno'}")
        return endpoints
    except Exception as e:
        print(f"⚠️ [load_endpoints_from_metadata] Error cargando endpoints desde metadata: {str(e)}")
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
            print(f"⚠️ [get_standardized_table_name] Endpoint '{endpoint}' no encontrado en metadata, usando normalización por defecto")
            normalized = _normalize_table_name_fallback(endpoint)
            get_standardized_table_name._cache[cache_key] = normalized
            return normalized
            
    except Exception as e:
        # En caso de error, usar normalización por defecto
        print(f"⚠️ [get_standardized_table_name] Error consultando metadata para '{endpoint}': {str(e)}. Usando normalización por defecto")
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
            print(f"❌ [log_event_bq] Error insertando log en BigQuery: {errors}")
    except Exception as e:
        print(f"❌ [log_event_bq] Error en logging: {str(e)}")

def fix_nested_value(value, field_path="", known_array_fields=None):
    """
    Función recursiva para corregir valores anidados.
    - Convierte NULL a [] para campos array conocidos
    - Corrige objetos que deberían ser arrays (ej: serialNumbers)
    - Preserva nombres originales (camelCase) dentro de STRUCT
    """
    snake_path = to_snake_case(field_path) if field_path else ""
    path_parts = snake_path.split('.')
    is_array_field = known_array_fields and any(part in known_array_fields for part in path_parts)

    if value is None:
        if is_array_field:
            return []
        return None
    
    # Si es un diccionario/objeto (STRUCT)
    if isinstance(value, dict):
        field_name_lower = field_path.lower() if field_path else ""
        
        # 1. Regla hardcodeada (serialNumbers)
        if 'serial' in field_name_lower and 'number' in field_name_lower:
            return []

        # PRINCIPIO BRONZE: Preservar nombres originales (camelCase) - NO convertir a snake_case
        # Procesar recursivamente para corregir NULLs y arrays, preservando nombres
        fixed_dict = {}
        for k, v in value.items():
            nested_path = f"{field_path}.{k}" if field_path else k
            fixed_dict[k] = fix_nested_value(v, nested_path, known_array_fields)
            
        # 2. Regla dinámica: si está explícitamente en known_array_fields,
        # significa que BQ esperaba un array (o un field simple) pero vino como objeto {}
        if is_array_field:
            if not fixed_dict:
                return []
            else:
                return [fixed_dict]  # Fallback a un array de este objeto si no está vacío
                
        return fixed_dict
    
    # Si es una lista (ARRAY), procesar cada elemento recursivamente
    # IMPORTANTE: Cuando el array contiene STRUCT, preserva camelCase dentro de esos STRUCT
    if isinstance(value, list):
        processed_list = []
        for item in value:
            # BigQuery no soporta arrays anidados (ARRAY de ARRAYs).
            # Si detectamos un array dentro de este array, lo convertimos a string o lo omitimos si está vacío
            if isinstance(item, list):
                if not item:
                    continue  # Omitir arrays anidados vacíos (ej: [[]])
                import json as _json
                processed_list.append(_json.dumps(item))
            else:
                # Agregar sufijo '[]' a la ruta para evitar que reglas de known_array_fields se activen
                # doblemente en los elementos hijos del array y creen Array of Arrays
                array_item_path = f"{field_path}[]" if field_path else "[]"
                processed_list.append(fix_nested_value(item, array_item_path, known_array_fields))
        return processed_list
    
    # Para otros tipos, retornar tal cual
    return value

def fix_json_format(local_path, temp_path, repeated_fields=None, stringify_fields=None, bronze_type_map=None):
    """Transforma el JSON a formato newline-delimited y snake_case.
    IMPORTANTE: Campos de nivel superior → snake_case, campos dentro de STRUCT → camelCase (preservar fuente).
    Soporta tanto JSON array como newline-delimited JSON.
    Si se proporciona repeated_fields, convierte NULL a [] para esos campos.
    Si se proporciona stringify_fields, convierte el contenido a un JSON string.
    Si se proporciona bronze_type_map, coerciona tipos en origen para evitar doble carga a staging.
    También corrige campos anidados que deberían ser arrays pero vienen como objetos.
    
    Para archivos grandes (>100MB), usa procesamiento streaming para evitar problemas de memoria."""
    
    # Detectar tamaño del archivo
    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    # Umbral para usar streaming en lugar de cargar todo en memoria.
    # 200MB es el límite seguro por task: con N tasks paralelas en Cloud Run,
    # N * 200MB * 2 (fuente + transformado) debe caber en la RAM disponible.
    # Archivos como gross_pay_items (~950MB) DEBEN usar streaming para evitar OOM.
    STREAMING_THRESHOLD_MB = 200

    if file_size_mb > STREAMING_THRESHOLD_MB:
        print(f"🌊 Archivo grande ({file_size_mb:.2f} MB) → modo streaming (evita OOM en Cloud Run)")
        return fix_json_format_streaming(local_path, temp_path, repeated_fields, stringify_fields, bronze_type_map)
    
    # Procesamiento en memoria - SIMPLIFICADO Y ROBUSTO
    print(f"📖 Cargando JSON completo en memoria...")
    with open(local_path, 'r', encoding='utf-8') as f:
        first_char = f.read(1)
        f.seek(0)
        
        if first_char == '[':
            # JSON array tradicional
            json_data = json.load(f)
        else:
            # Newline-delimited JSON
            json_data = [json.loads(line) for line in f if line.strip()]
    
    total_items = len(json_data)
    print(f"✅ JSON cargado: {total_items:,} items en memoria")
    
    # Detectar campos array (usando snake_case para campos de nivel superior)
    if repeated_fields is None:
        repeated_fields = set()
    elif not isinstance(repeated_fields, set):
        repeated_fields = set(repeated_fields)
        
    sample_size = min(1000, total_items)
    # Detectar campos array Y campos con tipos mixtos (ej: location_zip que viene como "-" en algunos registros)
    # Los campos mixed-type (num + string no-numérico) se fuerzan a STRING para evitar doble carga a staging
    field_types = {}  # {snake_key: set de tipos Python vistos}
    if stringify_fields is None:
        stringify_fields = set()
    elif not isinstance(stringify_fields, set):
        stringify_fields = set(stringify_fields)

    for item in json_data[:sample_size]:
        if isinstance(item, dict):
            for k, v in item.items():
                snake_key = to_snake_case(k)
                if isinstance(v, list):
                    repeated_fields.add(snake_key)
                elif v is not None:
                    # Registrar qué tipos de valores tiene este campo
                    if snake_key not in field_types:
                        field_types[snake_key] = {'has_numeric': False, 'has_non_numeric_str': False}
                    if isinstance(v, (int, float)):
                        field_types[snake_key]['has_numeric'] = True
                    elif isinstance(v, str) and v.strip():
                        # String no vacío: verificar si es puramente numérico
                        try:
                            float(v)
                            field_types[snake_key]['has_numeric'] = True
                        except ValueError:
                            field_types[snake_key]['has_non_numeric_str'] = True

    # Forzar a STRING campos que tienen mix de numérico + string no-numérico
    auto_stringified = []
    for snake_key, types in field_types.items():
        if types['has_numeric'] and types['has_non_numeric_str'] and snake_key not in stringify_fields:
            stringify_fields.add(snake_key)
            auto_stringified.append(snake_key)
    if auto_stringified:
        print(f"🔍 Campos con tipos mixtos detectados (forzados a STRING): {sorted(auto_stringified)}")
    
    # Transformar cada item con progreso
    print(f"🔄 Transformando {total_items:,} items a snake_case...")
    transformed_items = []
    items_processed = 0
    start_transform = time.time()
    last_progress = start_transform
    
    for item in json_data:
        transformed_item = transform_item(item, repeated_fields, stringify_fields, bronze_type_map)
        transformed_items.append(transformed_item)
        items_processed += 1
        
        # Mostrar progreso cada 1000 items o cada 5 segundos
        current_time = time.time()
        if (items_processed % 1000 == 0) or (current_time - last_progress >= 5):
            last_progress = current_time
    
    # Escribir como newline-delimited JSON
    print(f"💾 Escribiendo archivo transformado...")
    with open(temp_path, 'w', encoding='utf-8') as f:
        for item in transformed_items:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    transform_time = time.time() - start_transform
    print(f"✅ Transformación completada: {len(transformed_items):,} items procesados en {transform_time:.1f}s ({len(transformed_items)/transform_time:.0f} items/seg)")

def fix_json_format_streaming(local_path, temp_path, repeated_fields=None, stringify_fields=None, bronze_type_map=None):
    """Versión streaming de fix_json_format para archivos grandes.
    Procesa línea por línea para evitar cargar todo en memoria.
    Usa json.JSONDecoder para parsear JSON arrays de forma incremental."""
    
    items_processed = 0
    start_time = time.time()
    last_progress_time = start_time
    
    # Asegurar que el archivo de salida esté limpio (por si una ejecución previa falló)
    # Esto es diferente de la tabla staging - este es un archivo local en disco
    if os.path.exists(temp_path):
        try:
            os.remove(temp_path)
        except Exception:
            pass  # Continuar si no se puede eliminar
    
    # Detectar campos array en una muestra
    if repeated_fields is None:
        repeated_fields = set()
    elif not isinstance(repeated_fields, set):
        repeated_fields = set(repeated_fields)
        
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
    
    # Detectar campos array Y campos con tipos mixtos en la muestra
    if stringify_fields is None:
        stringify_fields = set()
    elif not isinstance(stringify_fields, set):
        stringify_fields = set(stringify_fields)

    field_types = {}  # {snake_key: set de tipos vistos}
    for item in items:
        if isinstance(item, dict):
            for k, v in item.items():
                snake_key = to_snake_case(k)
                if isinstance(v, list):
                    repeated_fields.add(snake_key)
                elif v is not None:
                    if snake_key not in field_types:
                        field_types[snake_key] = {'has_numeric': False, 'has_non_numeric_str': False}
                    if isinstance(v, (int, float)):
                        field_types[snake_key]['has_numeric'] = True
                    elif isinstance(v, str) and v.strip():
                        try:
                            float(v)
                            field_types[snake_key]['has_numeric'] = True
                        except ValueError:
                            field_types[snake_key]['has_non_numeric_str'] = True

    # Forzar a STRING campos que tienen mix de numérico + string no-numérico
    auto_stringified = []
    for snake_key, types in field_types.items():
        if types['has_numeric'] and types['has_non_numeric_str'] and snake_key not in stringify_fields:
            stringify_fields.add(snake_key)
            auto_stringified.append(snake_key)
    if auto_stringified:
        print(f"🔍 Campos con tipos mixtos detectados (forzados a STRING): {sorted(auto_stringified)}")
    
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
                items_since_last_progress = 0  # Inicializar UNA VEZ fuera del loop de chunks
                
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
                    parse_attempts = 0
                    max_parse_attempts = 1000  # Evitar loops infinitos
                    
                    while buffer and parse_attempts < max_parse_attempts:
                        parse_attempts += 1
                        buffer = buffer.lstrip()
                        if not buffer or buffer[0] == ']':
                            break
                        
                        try:
                            obj, consumed = decoder.raw_decode(buffer)
                            transformed = transform_item(obj, repeated_fields, stringify_fields, bronze_type_map)
                            f_out.write(json.dumps(transformed, ensure_ascii=False) + '\n')
                            items_processed += 1
                            items_since_last_progress += 1
                            
                            # Avanzar después del item
                            buffer = buffer[consumed:].lstrip()
                            # Saltar coma si existe
                            if buffer and buffer[0] == ',':
                                buffer = buffer[1:].lstrip()
                            
                            # Resetear contador de intentos después de parsear exitosamente
                            parse_attempts = 0
                            
                            # Actualizar contadores de progreso sin imprimir para evitar logs excesivos
                            current_time = time.time()
                            if (current_time - last_progress_time >= 10) or (items_since_last_progress >= 1000):
                                last_progress_time = current_time
                                items_since_last_progress = 0
                        except (ValueError, json.JSONDecodeError) as e:
                            # Item incompleto o mal formado
                            # Si no hay más chunk, intentar avanzar manualmente para no quedarse atascado
                            if not chunk:
                                # Intentar avanzar saltando caracteres hasta encontrar una coma o ']'
                                # Esto ayuda a recuperarse de items mal formados
                                found_separator = False
                                for i, char in enumerate(buffer[:1000]):  # Buscar en próximos 1000 caracteres
                                    if char == ',' or char == ']':
                                        buffer = buffer[i+1:].lstrip()
                                        found_separator = True
                                        break
                                
                                if not found_separator:
                                    # No se encontró separador, probablemente fin del archivo
                                    if items_processed == 0:
                                        print(f"⚠️ [fix_json_format_streaming] Error parseando JSON: {str(e)[:200]}")
                                    break
                            else:
                                # Hay más datos, esperar al siguiente chunk
                                break
                    
                    # Si no hay más datos y el buffer está vacío o solo tiene ']', terminamos
                    if not chunk:
                        # Verificar si quedó algo en el buffer antes de terminar
                        buffer_remaining = buffer.lstrip()
                        if buffer_remaining and buffer_remaining != ']':
                            # Hay datos sin procesar - intentar parsear uno más
                            try:
                                obj, consumed = decoder.raw_decode(buffer_remaining.rstrip(',').rstrip(']').strip())
                                transformed = transform_item(obj, repeated_fields, stringify_fields, bronze_type_map)
                                f_out.write(json.dumps(transformed, ensure_ascii=False) + '\n')
                                items_processed += 1
                            except:
                                pass  # Ignorar si no se puede parsear
                        break
            else:
                # Newline-delimited JSON - más simple
                for line in f_in:
                    if line.strip():
                        try:
                            item = json.loads(line)
                            transformed = transform_item(item, repeated_fields, stringify_fields, bronze_type_map)
                            f_out.write(json.dumps(transformed, ensure_ascii=False) + '\n')
                            items_processed += 1
                            
                            # Actualizar contadores de progreso sin imprimir para evitar logs excesivos
                            current_time = time.time()
                            if current_time - last_progress_time >= 10:
                                last_progress_time = current_time
                        except Exception as e:
                            # Log error pero continuar
                            if items_processed == 0:
                                print(f"⚠️ [fix_json_format_streaming] Error parseando línea: {str(e)[:100]}")
                            pass
    
    total_time = time.time() - start_time
    if items_processed == 0:
        print(f"❌ [fix_json_format_streaming] ERROR: No se procesaron items. El archivo puede estar vacío o mal formado.")
    else:
        print(f"✅ Transformación completada: {items_processed:,} items procesados en {total_time:.1f}s ({items_processed/total_time:.0f} items/seg)")

def transform_item(item, array_fields, stringify_fields=None, bronze_type_map=None):
    """
    Transforma un item individual a snake_case en nivel superior, preserva camelCase en STRUCT.
    
    Si se proporciona bronze_type_map ({campo: tipo_bq}), aplica coerción de tipos durante
    la transformación: valores string que no pueden convertirse al tipo esperado se convierten
    a NULL. Esto evita que BigQuery rechace el archivo y fuerce una doble carga a staging.
    
    El bronze_type_map se construye dinámicamente desde el schema real de bronze, sin hardcodeo.
    """
    # Tipos numéricos y booleanos que deben coercionarse si el valor viene como string
    _NUMERIC_INT  = frozenset({'INT64', 'INTEGER', 'INT', 'SMALLINT', 'BIGINT', 'BYTEINT'})
    _NUMERIC_FLOAT = frozenset({'FLOAT64', 'FLOAT', 'NUMERIC', 'BIGNUMERIC'})
    _BOOL_TYPES   = frozenset({'BOOL', 'BOOLEAN'})

    new_item = {}
    for k, v in item.items():
        snake_key = to_snake_case(k)  # snake_case para campos de nivel superior
        
        # Procesar recursivamente: preserva camelCase dentro de STRUCT
        fixed_value = fix_nested_value(v, snake_key, array_fields)
        
        if stringify_fields and snake_key in stringify_fields:
            # Forzar campo a JSON string (por autodetección de problemas en ejecuciones previas)
            new_item[snake_key] = json.dumps(fixed_value, ensure_ascii=False) if fixed_value is not None else None

        elif snake_key in array_fields and fixed_value is None:
            new_item[snake_key] = []

        elif (
            bronze_type_map
            and snake_key in bronze_type_map
            and isinstance(fixed_value, str)
            and fixed_value != ''
        ):
            # Coerción genérica basada en el schema real de bronze:
            # Si el valor es un string pero bronze espera un tipo numérico/bool,
            # intentar convertir. Si falla → NULL (no error de BQ).
            expected = bronze_type_map[snake_key].upper()
            if expected in _NUMERIC_INT:
                try:
                    new_item[snake_key] = int(float(fixed_value))  # float() primero tolera "3.0"
                except (ValueError, TypeError):
                    new_item[snake_key] = None  # ej: "-", "N/A", "" → NULL
            elif expected in _NUMERIC_FLOAT:
                try:
                    new_item[snake_key] = float(fixed_value)
                except (ValueError, TypeError):
                    new_item[snake_key] = None
            elif expected in _BOOL_TYPES:
                lower_v = fixed_value.lower()
                if lower_v in ('true', '1', 'yes'):
                    new_item[snake_key] = True
                elif lower_v in ('false', '0', 'no'):
                    new_item[snake_key] = False
                else:
                    new_item[snake_key] = None
            else:
                new_item[snake_key] = fixed_value  # DATE, TIMESTAMP, STRING, STRUCT → sin tocar

        else:
            new_item[snake_key] = fixed_value
            
    # Inyectar [] para campos array que no estaban presentes en el item
    if array_fields:
        for array_field in array_fields:
            if '.' not in array_field and array_field not in new_item:
                new_item[array_field] = []
                
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

def clean_bq_error(e):
    """Limpia el mensaje de error de BigQuery quitando Location, Job ID y URL verbose."""
    import re
    error_str = str(e)
    # Suprimir la URL gigante de la API usando regex
    error_str = re.sub(r'(?:GET|POST|PUT|DELETE)\s+https?://[^\s]+:\s*', '', error_str)
    
    # Cortar en el primer salto de párrafo antes de Location:
    for separator in ['\n\nLocation:', '\nLocation:', 'Location:']:
        if separator in error_str:
            error_str = error_str.split(separator)[0].rstrip()
            break
            
    # Quitar cualquier línea que contenga "Job ID:" o "Location:" para estar súper seguros
    lines = []
    for l in error_str.splitlines():
        if l.strip().startswith('Job ID:') or l.strip().startswith('Location:'):
            continue
        # Limpiar si viene concatenado en la misma linea como "reason: invalidQuery, location: query"
        l = re.sub(r',\s*location:\s*[\w]+', '', l)
        l = re.sub(r';\s*reason:\s*[\w]+', '', l)
        if l.strip():
            lines.append(l)
    return '\n'.join(lines).rstrip()

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
    
    if field_type == 'STRUCT' or field_type == 'RECORD':
        fields_sql = ', '.join([_schema_field_to_sql(f) for f in field.fields])
        struct_def = f"STRUCT<{fields_sql}>"
    else:
        # Mapear tipos que BQ API devuelve pero que ALTER TABLE no acepta literalmente
        BQ_TYPE_MAP = {
            'FLOAT': 'FLOAT64',
            'INTEGER': 'INT64',
            'BOOLEAN': 'BOOL',
        }
        struct_def = BQ_TYPE_MAP.get(field_type, field_type)
    
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
        return (False, [], None, {})
    
    # BigQuery NO permite cambiar tipos de columnas existentes via PATCH/update_table.
    # Usamos SAFE_CAST en el MERGE (se hace en execute_merge_or_insert).
    # Aquí solo reportamos las incompatibilidades.
    print(f"🔍 Incompatibilidades de esquema detectadas ({len(incompatible_fields)} campos):")
    for inc in incompatible_fields:
        print(f"  • {inc['name']}: staging={inc['staging_type']}, final={inc['final_type']}")

    type_mismatches = {}
    for inc in incompatible_fields:
        field_name = inc['name']
        if inc['staging_field'].field_type == 'STRUCT':
            print(f"  ⚠️ [align_schemas_before_merge] Campo {field_name} es STRUCT con tipo distinto, se ignora.")
            continue
        type_mismatches[field_name] = {'staging': inc['staging_type'], 'final': inc['final_type']}
        print(f"  ⚠️ [align_schemas_before_merge] Campo {field_name}: se usará SAFE_CAST en el MERGE (staging={inc['staging_type']}, final={inc['final_type']}).")

    return (bool(type_mismatches), list(type_mismatches.keys()), None, type_mismatches)

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
            # Abrir archivo de muestra de forma segura
            with open(sample_file, "rb") as sample_f:
                sample_load_job = bq_client.load_table_from_file(
                    sample_f,
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
        # Abrir archivo de forma segura con context manager
        with open(temp_fixed, "rb") as f:
            load_job = bq_client.load_table_from_file(
                f,
                table_ref_staging,
                job_config=job_config
            )
        load_job.result()
        
        load_time = time.time() - load_start
        print(f"✅ Carga a staging completada en {load_time:.1f}s")
        return (True, load_time, None)
    except Exception as e:
        import re
        error_msg_raw = str(e)
        error_msg = clean_bq_error(error_msg_raw)
        problematic_field = None
        needs_fix = False
        fix_type = None  # 'repeated', 'nested', 'type_mismatch'
        
        # Limpiar verbosidad de BigQuery para los logs
        clean_errors = []
        
        # Intentar obtener errores detallados del job de BigQuery
        detailed_errors = []
        if load_job:
            try:
                if hasattr(load_job, 'errors') and load_job.errors:
                    detailed_errors = load_job.errors
                elif hasattr(load_job, 'job_id'):
                    try:
                        job = bq_client.get_job(load_job.job_id)
                        if hasattr(job, 'errors') and job.errors:
                            detailed_errors = job.errors
                    except:
                        pass
            except:
                pass
        
        # Analizar cada error y construir mensajes limpios
        pos_match = None
        if detailed_errors:
            for err in detailed_errors:
                err_msg = err.get('message', '') if isinstance(err, dict) else str(err)
                
                # Omitir errores redudantes y no descriptivos
                if "encountered too many errors, giving up" in err_msg:
                    continue
                
                # Extraer posición de bytes si está disponible ("row starting at position X")
                if not pos_match:
                    pos_match = re.search(r'position\s+(\d+)', err_msg, re.IGNORECASE)
                
                # Extraer campo del mensaje detallado si está disponible
                if 'Field:' in err_msg and not problematic_field:
                    field_match = re.search(r'Field:\s*([\w_]+)', err_msg, re.IGNORECASE)
                    if field_match:
                        problematic_field = field_match.group(1)
                
                if err_msg not in clean_errors:
                    clean_errors.append(err_msg)
        
        # Manejo especial: extraer la fila exacta JSON usando la posición de bytes
        problematic_row_preview = ""
        if pos_match:
            try:
                byte_pos = int(pos_match.group(1))
                with open(temp_fixed, 'rb') as f_preview:
                    f_preview.seek(byte_pos)
                    # Leer la línea en esa posición
                    bad_line = f_preview.readline().decode('utf-8', errors='replace').strip()
                    if bad_line:
                        # Truncar a 200 caracteres para evitar spam excesivo
                        preview_str = bad_line[:200] + "..." if len(bad_line) > 200 else bad_line
                        problematic_row_preview = f"\n  ▶️ Fila JSON problemática (aprox): {preview_str}"
            except Exception:
                pass

        # Construir mensaje de error más limpio
        if clean_errors:
            error_msg = "\n".join([f"  • {msg}" for msg in clean_errors])
            # Imprimir al usuario de manera limpia
            print(f"⚠️ [load_json_to_staging_with_error_handling] Error BQ detectado (intentando auto-corrección):")
            print(f"{error_msg}{problematic_row_preview}")
            
        # Manejo de archivo JSON sin esquema (totalmente vacío)
        if 'Schema has no fields' in error_msg:
            return (False, time.time() - load_start, "Archivo JSON vacío o sin campos válidos (Schema has no fields)")
        
        # Detectar campo problemático. PRIORIDAD: repeated > nested > type_mismatch
        # "too many errors" es un envoltorio — ya contiene uno de los formatos internos.
        match = re.search(r'Field:\s*(\w+);\s*Value:\s*NULL', error_msg, re.IGNORECASE)
        match2 = re.search(r'non-record field:\s*([\w.]+)', error_msg, re.IGNORECASE)
        match3 = re.search(r'Could not convert.*?Field:\s*([\w_]+)', error_msg, re.IGNORECASE | re.DOTALL)
        match4 = re.search(r'Invalid (?:date|datetime|time|timestamp).*?Field:\s*([\w_]+)', error_msg, re.IGNORECASE)

        # Fallback: buscar campo en mensajes de "too many errors" cuando no hubo match directo
        if not match and not match2 and not match3 and not match4:
            if 'JSON table encountered too many errors' in error_msg or 'JSON parsing error' in error_msg:
                all_fields = re.findall(r'Field:\s*([\w_]+)', error_msg, re.IGNORECASE)
                if all_fields:
                    problematic_field = all_fields[-1]
                    needs_fix = True
                    fix_type = 'repeated'

        if match:
            problematic_field = match.group(1)
            needs_fix = True
            fix_type = 'repeated'
        elif match2:
            problematic_field = match2.group(1)
            needs_fix = True
            fix_type = 'nested'
        elif match3:
            problematic_field = match3.group(1)
            needs_fix = True
            fix_type = 'type_mismatch'
        elif match4:
            problematic_field = match4.group(1)
            needs_fix = True
            fix_type = 'type_mismatch'

        if needs_fix and problematic_field:
            strategy_labels = {'repeated': 'stringify', 'nested': 'stringify', 'type_mismatch': 'corregir tipo a STRING'}
            print(f"🔧 Auto-corrección: campo '{problematic_field}' | estrategia: {strategy_labels.get(fix_type, fix_type)}")
            if fix_type == 'type_mismatch':
                # Error de tipo de dato: corregir esquema
                # Estrategia simplificada: eliminar tabla staging y recrearla con esquema corregido
                try:
                    print(f"🔧 Intentando corregir esquema para campo {problematic_field}...")
                    
                    # Eliminar tabla staging si existe (para empezar limpio)
                    try:
                        bq_client.delete_table(table_ref_staging, not_found_ok=True)
                        print(f"🧹 Tabla staging eliminada para recrear con esquema corregido")
                    except Exception as delete_error:
                        print(f"⚠️ [load_json_to_staging_with_error_handling] Error eliminando tabla staging: {str(delete_error)[:100]}")
                    
                    # Crear esquema mínimo con solo el campo problemático como STRING
                    # BigQuery inferirá el resto de los campos automáticamente
                    corrected_field = bigquery.SchemaField(
                        name=problematic_field,
                        field_type='STRING',  # Siempre corregir a STRING
                        mode='NULLABLE'
                        # No pasar fields=None, dejar que use el valor por defecto
                    )
                    
                    # Estrategia: inferir esquema de una muestra pequeña, corregir el campo problemático,
                    # y luego cargar todos los datos con el esquema corregido
                    sample_file = f"/tmp/sample_{project_id}_{table_name}_schema.json"
                    sample_table_ref = bq_client.dataset(dataset_staging).table(f"{table_staging}_sample_schema")
                    
                    try:
                        # Helper recursivo para buscar y reemplazar un campo (incluso dentro de RECORD/STRUCT)
                        def _replace_field_recurse(schema_fields, target_name):
                            updated = []
                            found = False
                            for f in schema_fields:
                                if f.field_type in ('RECORD', 'STRUCT'):
                                    sub_updated, sub_found = _replace_field_recurse(f.fields, target_name)
                                    if sub_found:
                                        found = True
                                        updated.append(bigquery.SchemaField(
                                            name=f.name, field_type=f.field_type, mode=f.mode,
                                            description=f.description, fields=sub_updated
                                        ))
                                    else:
                                        updated.append(f)
                                elif f.name == target_name:
                                    found = True
                                    updated.append(bigquery.SchemaField(
                                        name=f.name, field_type='STRING', mode=f.mode,
                                        description=f.description
                                    ))
                                else:
                                    updated.append(f)
                            return updated, found

                        # Crear muestra pequeña (primeras 100 líneas) para inferir esquema
                        with open(temp_fixed, 'r', encoding='utf-8') as f_in:
                            with open(sample_file, 'w', encoding='utf-8') as f_out:
                                for i, line in enumerate(f_in):
                                    if i >= 100:  # Solo primeras 100 líneas
                                        break
                                    f_out.write(line)
                        
                        # Cargar muestra con autodetect para inferir esquema completo
                        sample_config = bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                            autodetect=True,
                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                        )
                        
                        # Abrir archivo de muestra de forma segura
                        with open(sample_file, "rb") as sample_f:
                            sample_load_job = bq_client.load_table_from_file(
                                sample_f,
                                sample_table_ref,
                                job_config=sample_config
                            )
                            sample_load_job.result()
                        
                        # Obtener esquema inferido
                        sample_table = bq_client.get_table(sample_table_ref)
                        if sample_table and sample_table.schema:
                            # Construir esquema corregido de forma recursiva
                            updated_schema, field_found = _replace_field_recurse(sample_table.schema, problematic_field)
                            
                            if not field_found:
                                # Campo no estaba en el esquema inferido, agregarlo al nivel superior
                                updated_schema.append(corrected_field)
                            
                            print(f"✅ Esquema inferido y corregido: campo {problematic_field} forzado a STRING (nested support)")
                        else:
                            # Si no se pudo inferir, usar solo el campo corregido
                            updated_schema = [corrected_field]
                            print(f"⚠️ [load_json_to_staging_with_error_handling] No se pudo inferir esquema completo, usando solo campo {problematic_field} como STRING")
                        
                        # Limpiar tabla de muestra
                        bq_client.delete_table(sample_table_ref, not_found_ok=True)
                        
                    except Exception as sample_error:
                        print(f"⚠️ [load_json_to_staging_with_error_handling] Error infiriendo esquema de muestra: {str(sample_error)[:200]}")
                        # Si falla, usar solo el campo corregido
                        updated_schema = [corrected_field]
                    finally:
                        if os.path.exists(sample_file):
                            try:
                                os.remove(sample_file)
                            except:
                                pass
                    
                    # Crear tabla staging con esquema corregido completo
                    staging_table_obj = bigquery.Table(table_ref_staging, schema=updated_schema)
                    bq_client.create_table(staging_table_obj)
                    print(f"✅ Tabla staging recreada con esquema corregido ({len(updated_schema)} campos)")
                    
                    # Cargar todos los datos con el esquema corregido
                    # Si hay más errores de tipo, detectarlos y corregirlos recursivamente
                    max_retries = 3  # Máximo 3 intentos de corrección
                    retry_count = 0
                    current_schema = updated_schema
                    
                    while retry_count < max_retries:
                        try:
                            job_config_final = bigquery.LoadJobConfig(
                                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                schema=current_schema,
                                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                max_bad_records=0
                            )
                            
                            # Cargar archivo - BigQuery necesita que el archivo esté abierto
                            with open(temp_fixed, "rb") as f:
                                load_job_final = bq_client.load_table_from_file(
                                    f,
                                    table_ref_staging,
                                    job_config=job_config_final
                                )
                            # Esperar resultado fuera del context manager
                            load_job_final.result()
                            
                            load_time = time.time() - load_start
                            print(f"✅ Datos cargados exitosamente con esquema corregido")
                            return (True, load_time, None)
                            
                        except Exception as load_error:
                            error_msg = clean_bq_error(load_error)
                            
                            # Intentar obtener detalles del error del job
                            another_field = None
                            try:
                                if hasattr(load_job_final, 'errors') and load_job_final.errors:
                                    for err in load_job_final.errors:
                                        err_str = str(err)
                                        # Buscar campo problemático en el error
                                        match = re.search(r"Could not convert.*?Field:\s*([\w_]+)", err_str, re.IGNORECASE | re.DOTALL)
                                        if match:
                                            another_field = match.group(1)
                                            break
                            except:
                                pass
                            
                            # Si no se encontró en errors, buscar en el mensaje completo
                            if not another_field:
                                # Buscar todos los campos mencionados en el error
                                all_fields = re.findall(r'Field:\s*([\w_]+)', error_msg, re.IGNORECASE)
                                if all_fields:
                                    # Usar el último campo encontrado (generalmente el más específico)
                                    another_field = all_fields[-1]
                                else:
                                    # Intentar buscar con el patrón "Could not convert"
                                    match = re.search(r"Could not convert.*?Field:\s*([\w_]+)", error_msg, re.IGNORECASE | re.DOTALL)
                                    if match:
                                        another_field = match.group(1)
                            
                            # Si encontramos otro campo problemático y no es el mismo que ya corregimos
                            if another_field and another_field != problematic_field and retry_count < max_retries - 1:
                                # Verificar que no hayamos corregido este campo ya
                                already_corrected = any(f.name == another_field and f.field_type == 'STRING' for f in current_schema)
                                
                                if not already_corrected:
                                    print(f"🔍 Detectado otro campo problemático: {another_field}")
                                    # Corregir este campo también
                                    another_corrected_field = bigquery.SchemaField(
                                        name=another_field,
                                        field_type='STRING',
                                        mode='NULLABLE'
                                    )
                                    # Actualizar esquema recursivamente
                                    new_schema, field_replaced = _replace_field_recurse(current_schema, another_field)
                                    
                                    if not field_replaced:
                                        new_schema.append(another_corrected_field)
                                    
                                    current_schema = new_schema
                                    # Actualizar tabla con nuevo esquema
                                    staging_table_obj = bigquery.Table(table_ref_staging, schema=current_schema)
                                    try:
                                        bq_client.update_table(staging_table_obj, ['schema'])
                                    except:
                                        bq_client.delete_table(table_ref_staging, not_found_ok=True)
                                        bq_client.create_table(staging_table_obj)
                                    
                                    print(f"✅ Campo {another_field} también convertido a STRING")
                                    retry_count += 1
                                    continue
                            
                            # Si no encontramos campo específico pero hay error, intentar usar autodetect con max_bad_records
                            if not another_field and retry_count < max_retries - 1:
                                print(f"⚠️ [load_json_to_staging_with_error_handling] Error genérico detectado, intentando con autodetect y max_bad_records=10...")
                                try:
                                    # Eliminar tabla y recrear con autodetect permitiendo algunos errores
                                    bq_client.delete_table(table_ref_staging, not_found_ok=True)
                                    job_config_autodetect = bigquery.LoadJobConfig(
                                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                        autodetect=True,
                                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                        max_bad_records=10  # Permitir algunos errores para ver qué campos fallan
                                    )
                                    with open(temp_fixed, "rb") as f:
                                        load_job_autodetect = bq_client.load_table_from_file(
                                            f,
                                            table_ref_staging,
                                            job_config=job_config_autodetect
                                        )
                                    # Esperar resultado fuera del context manager
                                    load_job_autodetect.result()
                                    
                                    # Si llegamos aquí, la carga fue exitosa con autodetect
                                    load_time = time.time() - load_start
                                    print(f"✅ Datos cargados exitosamente con autodetect (algunos registros pueden haberse omitido)")
                                    return (True, load_time, None)
                                except Exception as autodetect_error:
                                    print(f"⚠️ [load_json_to_staging_with_error_handling] Autodetect también falló: {str(autodetect_error)[:200]}")
                                    retry_count += 1
                                    continue
                            
                            # Si no hay más campos para corregir o alcanzamos el máximo, lanzar error
                            raise
                except Exception as schema_error:
                    import traceback
                    error_trace = traceback.format_exc()
                    print(f"❌ [load_json_to_staging_with_error_handling] Error completo en corrección de esquema:\n{error_trace}")
                    return (False, 0, f"Error corrigiendo esquema: {str(schema_error)}")
            elif fix_type in ['repeated', 'nested']:
                # Campo REPEATED/NESTED problemático: la estrategia más robusta es stringify.
                # Intentar inyectar `[]` no resuelve porque BigQuery autodetect puede inferir el campo como
                # RECORD REPEATED REQUIRED (si las primeras filas tienen datos), y luego rechazar cualquier
                # fila donde el campo sea null o esté ausente.
                # Stringify lo convierte a STRING NULLABLE, que BQ siempre acepta.
                print(f"🧹 Re-transformando datos para corregir campo {problematic_field} (convirtiendo a STRING)...")
                try:
                    fix_json_format(temp_json, temp_fixed, stringify_fields={problematic_field})
                    
                    # Limpiar tabla en caso de que existiera parcialmente
                    bq_client.delete_table(table_ref_staging, not_found_ok=True)
                    
                    print(f"🔄 Reintentando carga a staging después de corregir datos...")
                    
                    # CRÍTICO: Usar un nuevo job_config con autodetect=True. 
                    # El antiguo job_config podría tener un 'schema' estático (calculado antes
                    # de hacer el stringify) que entra en conflicto con nuestro nuevo formato texto.
                    retry_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        autodetect=True,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                    )
                    
                    with open(temp_fixed, "rb") as f2:
                        retry_job = bq_client.load_table_from_file(
                            f2,
                            table_ref_staging,
                            job_config=retry_config
                        )
                    retry_job.result()
                    
                    load_time = time.time() - load_start
                    print(f"✅ Cargado a tabla staging exitosamente después de limpieza de datos")
                    return (True, load_time, None)
                except Exception as retry_error:
                    error_msg = f"Error reintentando carga tras limpieza de datos para {problematic_field}: {clean_bq_error(retry_error)}"
                    try:
                        if 'retry_job' in locals() and hasattr(retry_job, 'errors') and retry_job.errors:
                            error_details = json.dumps(retry_job.errors, indent=2)
                            error_msg += f"\nDetalles del error en BigQuery:\n{error_details}"
                    except:
                        pass
                    print(f"❌ [load_json_to_staging_with_error_handling] {error_msg}")
                    return (False, 0, error_msg)
        
        # Si no se pudo corregir, retornar error
        load_time = time.time() - load_start
        return (False, load_time, error_msg)

def execute_merge_or_insert(
    bq_client, staging_table, final_table, project_id, dataset_final, table_final,
    dataset_staging, table_staging, merge_start, log_event_callback=None,
    company_id=None, company_name=None, endpoint_name=None, type_mismatches=None,
    use_merge=True, temp_fixed=None, is_production=True
):
    """
    Ejecuta MERGE, INSERT directo, o CREATE OR REPLACE TABLE dependiendo de la configuración.
    
    Modos:
    - is_production=False : fuerza CREATE OR REPLACE TABLE (schema siempre desde staging).
                              Úsalo durante desarrollo activo de un endpoint.
    - use_merge=False       : CREATE OR REPLACE TABLE (snapshot completo, endpoint consolidado).
    - use_merge=True + tabla vacía  : INSERT directo (primera carga).
    - use_merge=True + tabla llena  : MERGE incremental con soft delete.
    
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
        use_merge: Si False, usa CREATE OR REPLACE TABLE (snapshot completo consolidado)
        temp_fixed: Ruta al archivo NDJSON transformado
        is_production: Si False, fuerza CREATE OR REPLACE TABLE para mantener schema limpio
                         durante período de desarrollo activo del endpoint.
    
    Returns:
        tuple: (success: bool, merge_time: float, error_message: str or None)
    """
    # ─── MODO DESARROLLO (is_production=False) ────────────────────────────────
    # Si el endpoint NO está consolidado, forzar CREATE OR REPLACE TABLE siempre.
    # Esto garantiza que el schema de bronze siempre refleje el schema actual de staging,
    # eliminando deuda de schema acumulada por cambios de código durante el desarrollo.
    if not is_production:
        print(f"🔧 Endpoint en DESARROLLO (is_production=False): forzando CREATE OR REPLACE TABLE para {dataset_final}.{table_final}")
        use_merge = False  # Delegar al bloque OVERWRITE a continuación

    # ─── MODO OVERWRITE (use_merge=False) ──────────────────────────────────────
    # Para endpoints que la API devuelve como snapshot completo (ej: gross_pay_items).
    # Usa CREATE OR REPLACE TABLE: una sola operación server-side que reemplaza
    # schema + datos en un paso. Ignora diferencias staging-bronze por diseño.
    if not use_merge:
        print(f"♻️  Modo OVERWRITE activado para {dataset_final}.{table_final}")
        try:
            # Una sola operación server-side: reemplaza schema + datos en BigQuery.
            # No requiere TRUNCATE previo, no tiene type mismatch, no necesita ALTER.
            overwrite_sql = f"""
                CREATE OR REPLACE TABLE `{project_id}.{dataset_final}.{table_final}`
                AS
                SELECT
                    *,
                    CURRENT_TIMESTAMP() AS _etl_synced,
                    'OVERWRITE' AS _etl_operation
                FROM `{project_id}.{dataset_staging}.{table_staging}`
            """
            query_job = bq_client.query(overwrite_sql)
            query_job.result()

            overwrite_time = time.time() - merge_start
            staging_refresh = bq_client.get_table(staging_table.reference)
            rows_written = staging_refresh.num_rows
            print(f"✅ OVERWRITE ejecutado: {dataset_final}.{table_final} reemplazado con {rows_written:,} filas en {overwrite_time:.1f}s")
            return (True, overwrite_time, None)

        except Exception as e:
            error_msg = clean_bq_error(e)
            overwrite_time = time.time() - merge_start
            print(f"❌ [execute_merge_or_insert] OVERWRITE falló para {dataset_final}.{table_final} después de {overwrite_time:.1f}s: {error_msg}")
            if log_event_callback:
                log_event_callback(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Error en OVERWRITE",
                    event_message=f"Error en OVERWRITE: {error_msg}"
                )
            return (False, overwrite_time, error_msg)

    staging_schema = staging_table.schema
    final_schema = final_table.schema
    
    # Obtener nombres de columnas (excluyendo campos ETL y id)
    staging_cols = {col.name for col in staging_schema if col.name != 'id' and not col.name.startswith('_etl_')}
    final_cols = {col.name for col in final_schema if col.name != 'id' and not col.name.startswith('_etl_')}
    new_cols = staging_cols - final_cols  # Columnas nuevas en staging que no están en final
    
    # Si hay columnas nuevas, agregarlas al esquema de la tabla final usando ALTER TABLE
    if new_cols:
        try:
            print(f"🆕 Columnas nuevas detectadas: {sorted(new_cols)}. Agregando al esquema de tabla final...")
            # Obtener esquema completo de staging (sin campos ETL)
            new_schema_fields = [col for col in staging_schema if col.name in new_cols]
            
            # Agregar cada nueva columna usando ALTER TABLE
            # BigQuery requiere una sentencia ALTER TABLE por columna (secuencial)
            # Esto evita el bug de BigQuery donde update_table borra campos anidados
            added_cols = []
            failed_cols = []
            for new_field in new_schema_fields:
                try:
                    field_def = _schema_field_to_sql(new_field)
                    alter_sql = f"ALTER TABLE `{project_id}.{dataset_final}.{table_final}` ADD COLUMN IF NOT EXISTS {field_def}"
                    bq_client.query(alter_sql).result()
                    print(f"  ✨ Campo {new_field.name} agregado a {dataset_final}.{table_final}")
                    added_cols.append(new_field.name)
                except Exception as e:
                    print(f"  ⚠️ [execute_merge_or_insert] No se pudo agregar {new_field.name}: {clean_bq_error(e)}")
                    failed_cols.append(new_field.name)
            if added_cols:
                print(f"✅ Columnas agregadas al esquema: {sorted(added_cols)}")
            if failed_cols:
                print(f"⚠️ [execute_merge_or_insert] Columnas que no se pudieron agregar (se ignorarán en MERGE): {sorted(failed_cols)}")
        except Exception as schema_error:
            print(f"⚠️ [execute_merge_or_insert] Error al actualizar esquema: {str(schema_error)}")
    
    # Verificar si la tabla final está vacía (primera carga)
    is_first_load = final_table.num_rows == 0
    
    if is_first_load:
        # Primera carga: usar INSERT directo (más eficiente y evita problemas de MERGE)
        print(f"📥 Primera carga detectada (tabla vacía). Usando INSERT directo en lugar de MERGE...")
        
        # Para INSERT, usar todas las columnas de staging explícitamente, pero sin forzar 'id' si no existe
        staging_has_id = any(col.name == 'id' for col in staging_schema)
        if staging_has_id:
            cols_list = ['id'] + sorted(list(staging_cols))
        else:
            cols_list = sorted(list(staging_cols))
            print(f"⚠️ [execute_merge_or_insert] Tabla staging no tiene columna 'id', se omitirá en el INSERT.")
        
        insert_cols_with_etl = cols_list + ['_etl_synced', '_etl_operation']
        
        # Usar SAFE_CAST si hay mismatches definidos (rara vez pero posible en primer load si final fue precreado)
        type_mismatches = type_mismatches or {}
        def _col_insert_expr(col):
            if col in type_mismatches:
                final_type = type_mismatches[col]['final']
                BQ_ALIASES = {'INTEGER': 'INT64', 'FLOAT': 'FLOAT64', 'BOOLEAN': 'BOOL'}
                final_type_sql = BQ_ALIASES.get(final_type, final_type)
                return f'SAFE_CAST(S.{col} AS {final_type_sql})'
            return f'S.{col}'
            
        insert_values_with_etl = [_col_insert_expr(col) for col in cols_list] + ['CURRENT_TIMESTAMP()', "'INSERT'"]
        
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
            error_msg = clean_bq_error(e)
            merge_time = time.time() - merge_start
            print(f"❌ [execute_merge_or_insert] INSERT directo falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {error_msg}")
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
        
        # RECALCULAR columnas finales disponibles (por si algún ALTER TABLE falló)
        final_table_refresh = bq_client.get_table(final_table.reference)
        final_cols_actual = {col.name for col in final_table_refresh.schema if col.name != 'id' and not col.name.startswith('_etl_')}
        
        # INTERSECCIÓN SEGURA: Solo actualizar e insertar columnas que REALMENTE existen en ambas tablas
        safe_cols = sorted(list(staging_cols.intersection(final_cols_actual)))
        
        # Verificar que 'id' existe en la tabla final Y EN STAGING (prerequisito del MERGE)
        final_has_id = any(col.name == 'id' for col in final_table_refresh.schema)
        staging_has_id = any(col.name == 'id' for col in staging_schema)
        
        if not final_has_id and staging_has_id:
            # Intentar agregarlo vía ALTER TABLE a la tabla final si staging sí lo tiene
            staging_id_field = next((col for col in staging_schema if col.name == 'id'), None)
            if staging_id_field:
                try:
                    id_sql_type = _schema_field_to_sql(staging_id_field)
                    alter_id_sql = f"ALTER TABLE `{project_id}.{dataset_final}.{table_final}` ADD COLUMN IF NOT EXISTS {id_sql_type}"
                    bq_client.query(alter_id_sql).result()
                    print(f"  ✨ Campo 'id' agregado a {dataset_final}.{table_final} para habilitar MERGE")
                    final_has_id = True
                except Exception as alter_id_err:
                    print(f"⚠️ [execute_merge_or_insert] No se pudo agregar 'id' a {dataset_final}.{table_final}: {clean_bq_error(alter_id_err)}")
            
        if not final_has_id or not staging_has_id:
            # Sin 'id' en uno de los dos lados: hacer TRUNCATE + INSERT (reemplazo total). Apropiado para tablas de settings.
            reason = "no tiene 'id' en staging" if not staging_has_id else "no tiene 'id' en la tabla final"
            print(f"\u26a0\ufe0f [execute_merge_or_insert] Tabla {dataset_final}.{table_final} {reason}. Usando TRUNCATE + INSERT (reemplazo total).")
            try:
                truncate_sql = f"TRUNCATE TABLE `{project_id}.{dataset_final}.{table_final}`"
                bq_client.query(truncate_sql).result()
                
                # Para TRUNCATE+INSERT, usar columnas seguras sin 'id' (ya que falta en al menos uno de los lados)
                trunc_cols = safe_cols
                
                # Reutilizar type_mismatches para castear en el INSERT
                type_mismatches = type_mismatches or {}
                def _col_trunc_insert_expr(col):
                    if col in type_mismatches:
                        final_type = type_mismatches[col]['final']
                        BQ_ALIASES = {'INTEGER': 'INT64', 'FLOAT': 'FLOAT64', 'BOOLEAN': 'BOOL'}
                        final_type_sql = BQ_ALIASES.get(final_type, final_type)
                        return f'SAFE_CAST(S.{col} AS {final_type_sql})'
                    return f'S.{col}'
                    
                insert_trunc_sql = f'''
                    INSERT INTO `{project_id}.{dataset_final}.{table_final}` (
                        {', '.join(trunc_cols)}, _etl_synced, _etl_operation
                    )
                    SELECT {', '.join([_col_trunc_insert_expr(c) for c in trunc_cols])},
                           CURRENT_TIMESTAMP(), 'INSERT'
                    FROM `{project_id}.{dataset_staging}.{table_staging}` S
                '''
                bq_client.query(insert_trunc_sql).result()
                merge_time = time.time() - merge_start
                print(f"✅ TRUNCATE+INSERT ejecutado: {dataset_final}.{table_final} reemplazado en {merge_time:.1f}s")
                return (True, merge_time, None)
            except Exception as trunc_err:
                merge_time = time.time() - merge_start
                err = clean_bq_error(trunc_err)
                print(f"❌ [execute_merge_or_insert] TRUNCATE+INSERT falló para {dataset_final}.{table_final}: {err}")
                return (False, merge_time, err)

        # Funciones para obtener la expresión a evaluar (S.col o SAFE_CAST)
        type_mismatches = type_mismatches or {}
        def _col_val_expr(col):
            if col in type_mismatches:
                final_type = type_mismatches[col]['final']
                BQ_ALIASES = {'INTEGER': 'INT64', 'FLOAT': 'FLOAT64', 'BOOLEAN': 'BOOL'}
                final_type_sql = BQ_ALIASES.get(final_type, final_type)
                return f'SAFE_CAST(S.{col} AS {final_type_sql})'
            return f'S.{col}'
            
        # UPDATE solo lleva {col} = {expr} (BQ no permite alias 'T.' en la izquierda)
        def _col_update_expr(col):
            return f'{col} = {_col_val_expr(col)}'
            
        update_set = ', '.join([_col_update_expr(col) for col in safe_cols])
        
        # Para INSERT, usar columnas seguras, agregando 'id' solo si existe
        if staging_has_id:
            cols_list = ['id'] + safe_cols
        else:
            cols_list = safe_cols
            
        insert_cols = cols_list
        # VALUES list usa puramente _col_val_expr
        insert_values = [_col_val_expr(col) if col != 'id' else 'S.id' for col in cols_list]
        
        # MERGE incremental a tabla final con Soft Delete y campos ETL
        merge_sql = f'''
            MERGE `{project_id}.{dataset_final}.{table_final}` T
            USING `{project_id}.{dataset_staging}.{table_staging}` S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET 
                {update_set},
                _etl_synced = CURRENT_TIMESTAMP(),
                _etl_operation = 'UPDATE'
            WHEN NOT MATCHED THEN INSERT (
                {', '.join(insert_cols)},
                _etl_synced, _etl_operation
            ) VALUES (
                {', '.join(insert_values)},
                CURRENT_TIMESTAMP(), 'INSERT'
            )
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
                _etl_synced = CURRENT_TIMESTAMP(),
                _etl_operation = 'DELETE'
        '''
        
        try:
            query_job = bq_client.query(merge_sql)
            query_job.result()
            merge_time = time.time() - merge_start
            print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado en {merge_time:.1f}s")
            return (True, merge_time, None)
        except Exception as e:
            error_msg = clean_bq_error(e)
            merge_time = time.time() - merge_start
            print(f"❌ [execute_merge_or_insert] MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {error_msg}")
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
