import os
import json
import re
import time
from datetime import datetime, timezone
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

# Configuración de BigQuery
# Detectar proyecto automáticamente desde variable de entorno o metadata del service account
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

PROJECT_SOURCE = get_project_source()
DATASET_NAME = "settings"
TABLE_NAME = "companies"

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

PROJECT_ID_FOR_QUERY = get_bigquery_project_id()  # Para usar en queries SQL (project_id real)

# Configuración para logging centralizado (usar project_id real)
LOGS_PROJECT = PROJECT_ID_FOR_QUERY
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

# Cargar endpoints automáticamente desde metadata
ENDPOINTS = load_endpoints_from_metadata()

# Función para convertir a snake_case
def to_snake_case(name):
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower()

# Configuración para tabla de metadata
METADATA_PROJECT = "pph-central"
METADATA_DATASET = "management"
METADATA_TABLE = "metadata_consolidated_tables"

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
                event_type="INFO", event_title="", event_message="", info=None):
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
            "source": "servicetitan_all_json_to_bigquery",
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
    También corrige campos anidados que deberían ser arrays pero vienen como objetos."""
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

def detectar_cambios_reales(staging_df, final_df, project_id, dataset_final, table_final):
    """Detecta si hay cambios reales entre staging y tabla final"""
    try:
        if final_df.empty:
            return 'INSERT'  # Primera carga
        
        # Obtener campos relevantes (excluyendo campos ETL)
        campos_relevantes = [col.name for col in staging_df.columns if not col.name.startswith('_etl_')]
        
        cambios_detectados = []
        
        for _, staging_row in staging_df.iterrows():
            id_val = staging_row['id']
            
            # Buscar en tabla final
            final_rows = final_df[final_df['id'] == id_val]
            
            if final_rows.empty:
                cambios_detectados.append('INSERT')
            else:
                final_row = final_rows.iloc[0]
                
                # Comparar campos relevantes
                hay_cambios = False
                for campo in campos_relevantes:
                    if str(staging_row[campo]) != str(final_row[campo]):
                        hay_cambios = True
                        break
                
                if hay_cambios:
                    cambios_detectados.append('UPDATE')
                else:
                    cambios_detectados.append('SYNC')
        
        # Determinar operación principal
        if 'UPDATE' in cambios_detectados:
            return 'UPDATE'
        elif 'INSERT' in cambios_detectados:
            return 'INSERT'
        else:
            return 'SYNC'
            
    except Exception as e:
        print(f"⚠️  Error detectando cambios: {str(e)}")
        return 'UPDATE'  # Por defecto, asumir UPDATE    

def process_company(row):
    company_id = row.company_id
    company_name = row.company_name
    project_id = row.company_project_id
    company_start_time = time.time()
    
    # Log de inicio de procesamiento de compañía (solo una vez por compañía, no por endpoint)
    print(f"\n{'='*80}\n🏢 Procesando compañía: {company_name} (company_id: {company_id}) | project_id: {project_id}")
    bucket_name = f"{project_id}_servicetitan"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    for endpoint_name, table_name in ENDPOINTS:
        endpoint_start_time = time.time()
        # Usar table_name directamente desde metadata para archivos JSON y tablas
        json_filename = f"servicetitan_{table_name}.json"
        temp_json = f"/tmp/{project_id}_{table_name}.json"
        temp_fixed = f"/tmp/fixed_{project_id}_{table_name}.json"
        
        print(f"\n📦 Endpoint: {endpoint_name} (tabla: {table_name})")
        
        # Descargar archivo JSON del bucket
        try:
            download_start = time.time()
            blob = bucket.blob(json_filename)
            if not blob.exists():
                print(f"⚠️  Archivo no encontrado: {json_filename} en bucket {bucket_name}")
                # Paso 4: MERGE no ejecutado (archivo faltante)
                merge_time = 0.0  # No hubo intento de MERGE
                print(f"❌ MERGE con Soft Delete no ejecutado para bronze.{table_name}: archivo {json_filename} no encontrado en bucket")
                log_event_bq(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="WARNING",
                    event_title="Archivo no encontrado",
                    event_message=f"Archivo {json_filename} no encontrado en bucket {bucket_name}. MERGE no ejecutado."
                )
                # Paso 5: Endpoint completado con errores
                endpoint_time = time.time() - endpoint_start_time
                print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                continue
            blob.download_to_filename(temp_json)
            download_time = time.time() - download_start
            file_size_mb = os.path.getsize(temp_json) / (1024 * 1024)
            print(f"⬇️  Descargado {json_filename} ({file_size_mb:.2f} MB) en {download_time:.1f}s")
        except Exception as e:
            print(f"❌ Error descargando {json_filename}: {str(e)}")
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error descargando archivo",
                event_message=f"Error descargando {json_filename}: {str(e)}"
            )
            # Paso 4: MERGE no ejecutado (error en descarga)
            merge_time = 0.0  # No hubo intento de MERGE
            print(f"❌ MERGE con Soft Delete no ejecutado para bronze.{table_name}: error descargando archivo - {str(e)}")
            # Paso 5: Endpoint completado con errores
            endpoint_time = time.time() - endpoint_start_time
            print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
            continue
        
        # Transformar a newline-delimited y snake_case (primera pasada)
        try:
            transform_start = time.time()
            fix_json_format(temp_json, temp_fixed)
            transform_time = time.time() - transform_start
            print(f"🔄 Transformado a newline-delimited y snake_case en {transform_time:.1f}s")
        except Exception as e:
            print(f"❌ Error transformando {json_filename}: {str(e)}")
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error transformando archivo",
                event_message=f"Error transformando {json_filename}: {str(e)}"
            )
            # Paso 4: MERGE no ejecutado (error en transformación)
            merge_time = 0.0  # No hubo intento de MERGE
            print(f"❌ MERGE con Soft Delete no ejecutado para bronze.{table_name}: error transformando archivo - {str(e)}")
            # Paso 5: Endpoint completado con errores
            endpoint_time = time.time() - endpoint_start_time
            print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
            continue
        
        # Cargar a tabla staging en BigQuery
        load_start = time.time()
        bq_client = bigquery.Client(project=project_id)
        dataset_staging = "staging"
        dataset_final = "bronze"
        # Usar table_name directamente desde metadata (coincide con el nombre del archivo JSON)
        table_staging = table_name
        table_final = table_name
        table_ref_staging = bq_client.dataset(dataset_staging).table(table_staging)
        table_ref_final = bq_client.dataset(dataset_final).table(table_final)
        
        # Asegurar que el dataset staging existe
        try:
            bq_client.get_dataset(f"{project_id}.{dataset_staging}")
        except NotFound:
            dataset = bigquery.Dataset(f"{project_id}.{dataset_staging}")
            dataset.location = "US"
            bq_client.create_dataset(dataset)
            print(f"🆕 Dataset {dataset_staging} creado en proyecto {project_id}")
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        # Intentar cargar directamente
        try:
            load_job = bq_client.load_table_from_file(
                open(temp_fixed, "rb"),
                table_ref_staging,
                job_config=job_config
            )
            load_job.result()
            load_time = time.time() - load_start
            # print(f"✅ Cargado a tabla staging: {dataset_staging}.{table_staging} en {load_time:.1f}s")
        except Exception as e:
            error_msg = str(e)
            problematic_field = None
            needs_fix = False
            
            # Intentar extraer campo REPEATED del mensaje de error
            # Formato 1: "Field: permissions; Value: NULL" (puede estar en cualquier parte del mensaje)
            # Formato 1b: "Only optional fields can be set to NULL. Field: permissions; Value: NULL"
            match = re.search(r'Field:\s*(\w+);\s*Value:\s*NULL', error_msg, re.IGNORECASE)
            
            # Formato 2: "JSON object specified for non-record field: items.serialNumbers"
            # Este error indica que un campo que debería ser array viene como objeto
            match2 = re.search(r'non-record field:\s*([\w.]+)', error_msg, re.IGNORECASE)
            
            if match:
                problematic_field = match.group(1)
                needs_fix = True
                print(f"🔍 Campo REPEATED detectado del error: {problematic_field}")
                print(f"🧹 Limpiando datos: convirtiendo NULL a [] para campo {problematic_field}")
            elif match2:
                problematic_field = match2.group(1)
                needs_fix = True
                print(f"🔍 Campo anidado con tipo incorrecto detectado: {problematic_field}")
                print(f"🧹 Limpiando datos: corrigiendo campo {problematic_field} que viene como objeto pero debería ser array")
            
            if needs_fix and problematic_field:
                # Limpiar datos con el campo detectado
                # Si es un campo anidado (ej: items.serialNumbers), extraer solo el nombre del campo
                field_name = problematic_field.split('.')[-1] if '.' in problematic_field else problematic_field
                # Usar nombre original (puede ser camelCase o snake_case dependiendo de la fuente)
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
                    # print(f"✅ Cargado a tabla staging: {dataset_staging}.{table_staging} (después de limpieza) en {load_time:.1f}s")
                except Exception as retry_error:
                    log_event_bq(
                        company_id=company_id,
                        company_name=company_name,
                        project_id=project_id,
                        endpoint=endpoint_name,
                        event_type="ERROR",
                        event_title="Error cargando a staging (después de limpieza)",
                        event_message=f"Error cargando a tabla staging después de limpiar {problematic_field}: {str(retry_error)}"
                    )
                    print(f"❌ Error cargando a tabla staging después de limpieza: {str(retry_error)}")
                    # Paso 4: MERGE no ejecutado (error cargando a staging)
                    merge_time = 0.0  # No hubo intento de MERGE
                    print(f"❌ MERGE con Soft Delete no ejecutado para bronze.{table_name}: error cargando a staging después de limpieza - {str(retry_error)}")
                    # Paso 5: Endpoint completado con errores
                    endpoint_time = time.time() - endpoint_start_time
                    print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                    continue
            else:
                log_event_bq(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Error cargando a staging",
                    event_message=f"Error cargando a tabla staging: {error_msg}"
                )
                print(f"❌ Error cargando a tabla staging: {error_msg}")
                # Paso 4: MERGE no ejecutado (error cargando a staging)
                merge_time = 0.0  # No hubo intento de MERGE
                print(f"❌ MERGE con Soft Delete no ejecutado para bronze.{table_name}: error cargando a staging - {error_msg}")
                # Paso 5: Endpoint completado con errores
                endpoint_time = time.time() - endpoint_start_time
                print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                continue
        
        # Asegurar que la tabla final existe
        try:
            bq_client.get_table(table_ref_final)
            # print(f"✅ Tabla final {dataset_final}.{table_final} ya existe.")
        except NotFound:
            schema = bq_client.get_table(table_ref_staging).schema
            # AGREGAR CAMPOS ETL AL ESQUEMA (SOLO 2 CAMPOS)
            campos_etl = [
                bigquery.SchemaField("_etl_synced", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_etl_operation", "STRING", mode="REQUIRED")
            ]
            schema_completo = list(schema) + campos_etl
            
            table = bigquery.Table(table_ref_final, schema=schema_completo)
            bq_client.create_table(table)
            print(f"🆕 Tabla final {dataset_final}.{table_final} creada con esquema ETL.")
            
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="INFO",
                event_title="Tabla final creada",
                event_message=f"Tabla final {dataset_final}.{table_final} creada automáticamente con campos ETL"
            )
        
        # MERGE incremental a tabla final con Soft Delete y campos ETL
        merge_start = time.time()
        merge_success = False
        merge_error_msg = None
        
        # OPTIMIZACIÓN: Obtener esquemas de ambas tablas en una sola operación cuando sea posible
        schema_start = time.time()
        staging_table = bq_client.get_table(table_ref_staging)
        final_table = bq_client.get_table(table_ref_final)
        schema_time = time.time() - schema_start
        if schema_time > 1:
            print(f"⏱️  Obtención de esquemas: {schema_time:.1f}s")
        staging_schema = staging_table.schema
        final_schema = final_table.schema
        
        # Obtener nombres de columnas (excluyendo campos ETL y id)
        staging_cols = {col.name for col in staging_schema if col.name != 'id' and not col.name.startswith('_etl_')}
        final_cols = {col.name for col in final_schema if col.name != 'id' and not col.name.startswith('_etl_')}
        common_cols = staging_cols & final_cols  # Intersección: columnas en ambas tablas
        new_cols = staging_cols - final_cols  # Columnas nuevas en staging que no están en final
        
        # OPTIMIZACIÓN: Solo verificar STRUCTs si hay columnas nuevas o si el MERGE falla
        # No hacer esta verificación costosa en cada ejecución si no es necesario
        structs_to_merge = {}
        # Solo verificar STRUCTs si realmente hay diferencias potenciales (columnas nuevas)
        # o si el MERGE falla (se manejará en el bloque de errores)
        
        # Si hay columnas nuevas, agregarlas al esquema de la tabla final
        # IMPORTANTE: Usar ALTER TABLE ADD COLUMN en lugar de reconstruir el esquema completo
        # para evitar perder campos anidados en STRUCTs existentes
        if new_cols:
            try:
                print(f"🆕 Columnas nuevas detectadas: {sorted(new_cols)}. Agregando al esquema de tabla final...")
                final_table = bq_client.get_table(table_ref_final)
                
                # Obtener esquema completo de staging (sin campos ETL)
                new_schema_fields = [col for col in staging_schema if col.name in new_cols]
                
                # Agregar cada nueva columna usando ALTER TABLE
                # BigQuery requiere una sentencia ALTER TABLE por columna (secuencial)
                for new_field in new_schema_fields:
                    try:
                        field_def = _schema_field_to_sql(new_field)
                        alter_sql = f"ALTER TABLE `{project_id}.{dataset_final}.{table_final}` ADD COLUMN IF NOT EXISTS {field_def}"
                        query_job = bq_client.query(alter_sql)
                        query_job.result()
                        print(f"  ✅ Columna {new_field.name} agregada al esquema")
                    except Exception as e:
                        # Si falla ALTER TABLE, loguear y continuar sin agregar la columna
                        # El MERGE manejará cualquier problema de esquema
                        print(f"  ⚠️  No se pudo agregar {new_field.name} con ALTER TABLE: {str(e)}")
                        print(f"  ⚠️  Continuando sin agregar esta columna. El MERGE manejará cualquier problema de esquema.")
                
                print(f"✅ Esquema actualizado. Columnas agregadas: {sorted(new_cols)}")
            except Exception as schema_error:
                # Si hay un error crítico al actualizar el esquema, loguear y continuar
                print(f"⚠️  Error al actualizar esquema: {str(schema_error)}")
                print(f"⚠️  Continuando con el MERGE. Si falla, se manejará en el bloque de errores del MERGE.")
        
        # Asegurar que campos ETL existan antes de construir MERGE SQL
        final_table_refresh = bq_client.get_table(table_ref_final)
        has_etl_synced = any(col.name == '_etl_synced' for col in final_table_refresh.schema)
        has_etl_operation = any(col.name == '_etl_operation' for col in final_table_refresh.schema)
        
        if not has_etl_synced or not has_etl_operation:
            print(f"🔧 Agregando campos ETL faltantes antes del MERGE...")
            try:
                if not has_etl_synced:
                    alter_sql1 = f"ALTER TABLE `{project_id}.{dataset_final}.{table_final}` ADD COLUMN IF NOT EXISTS _etl_synced TIMESTAMP"
                    bq_client.query(alter_sql1).result()
                    print(f"  ✅ Campo _etl_synced agregado")
                if not has_etl_operation:
                    alter_sql2 = f"ALTER TABLE `{project_id}.{dataset_final}.{table_final}` ADD COLUMN IF NOT EXISTS _etl_operation STRING"
                    bq_client.query(alter_sql2).result()
                    print(f"  ✅ Campo _etl_operation agregado")
                
                # CRÍTICO: Verificar que los campos se agregaron correctamente antes de continuar
                final_table_refresh = bq_client.get_table(table_ref_final)
                has_etl_synced = any(col.name == '_etl_synced' for col in final_table_refresh.schema)
                has_etl_operation = any(col.name == '_etl_operation' for col in final_table_refresh.schema)
                
                if not has_etl_synced or not has_etl_operation:
                    raise Exception(f"CRÍTICO: Campos ETL no se pudieron agregar. _etl_synced: {has_etl_synced}, _etl_operation: {has_etl_operation}. No se puede continuar con MERGE.")
                    
            except Exception as etl_error:
                merge_success = False
                merge_error_msg = f"Error agregando campos ETL antes del MERGE: {str(etl_error)}"
                merge_time = time.time() - merge_start
                print(f"❌ MERGE con Soft Delete no ejecutado para {dataset_final}.{table_final} después de {merge_time:.1f}s: {merge_error_msg}")
                log_event_bq(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Error agregando campos ETL",
                    event_message=merge_error_msg
                )
                endpoint_time = time.time() - endpoint_start_time
                print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                continue  # Continuar al siguiente endpoint
        
        # Construir UPDATE SET solo con columnas comunes (ahora incluye las nuevas)
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
            merge_success = True
            print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado en {merge_time:.1f}s")
            
            # Solo borrar tabla staging si el MERGE fue exitoso
            delete_start = time.time()
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
            delete_time = time.time() - delete_start
            # if delete_time > 0.5:
            #     print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada en {delete_time:.1f}s")
            # else:
            #     print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada")
            
            # Paso 5: Endpoint completado (solo cuando MERGE es exitoso)
            endpoint_time = time.time() - endpoint_start_time
            print(f"✅ Endpoint {endpoint_name} completado en {endpoint_time:.1f}s total")
        except Exception as e:
            error_msg = str(e)
            merge_error_msg = error_msg
            merge_time = time.time() - merge_start
            # Registrar paso 4 (MERGE) con error
            print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {error_msg}")
            
            # Detectar error de incompatibilidad de STRUCT (incluye ARRAY<STRUCT>)
            # Formato: "Value of type STRUCT<...> cannot be assigned to T.address, which has type STRUCT<...>"
            # Formato: "Value of type ARRAY<STRUCT<...>> cannot be assigned to T.items, which has type ARRAY<STRUCT<...>>"
            struct_match = re.search(r'cannot be assigned to T\.(\w+), which has type (?:ARRAY<)?STRUCT', error_msg)
            
            # Detectar error de cambio de tipo de campo (INT64 vs STRING, etc.)
            # Formato: "Value of type INT64 cannot be assigned to T.purchase_order_id, which has type STRING"
            type_mismatch = re.search(r'Value of type (\w+) cannot be assigned to T\.(\w+), which has type (\w+)', error_msg)
            
            if struct_match:
                problematic_struct_field = struct_match.group(1)
                print(f"🔍 Campo STRUCT con esquema incompatible detectado: {problematic_struct_field}")
                print(f"🔧 Actualizando esquema de campo STRUCT {problematic_struct_field} en tabla final...")
                
                # Obtener el campo STRUCT actualizado de staging
                staging_table = bq_client.get_table(table_ref_staging)
                final_table = bq_client.get_table(table_ref_final)
                
                # Encontrar el campo en staging
                new_struct_field = None
                for field in staging_table.schema:
                    if field.name == problematic_struct_field:
                        new_struct_field = field
                        break
                
                if new_struct_field:
                    # Fusionar campos del STRUCT: preservar campos existentes que no están en staging
                    # y agregar/actualizar campos nuevos de staging
                    old_struct_field = None
                    for field in final_table.schema:
                        if field.name == problematic_struct_field:
                            old_struct_field = field
                            break
                    
                    # CRÍTICO: Si hay incompatibilidad de STRUCT, fusionar campos preservando AMBOS
                    # La tabla final puede tener campos mixtos (camelCase y snake_case del error anterior)
                    # Necesitamos preservar TODOS los campos existentes para evitar "Field missing in new schema"
                    print(f"⚠️  Incompatibilidad de STRUCT detectada en {problematic_struct_field}.")
                    print(f"📋 Fusionando esquemas preservando TODOS los campos (final puede tener mixtos).")
                    print(f"   Final tiene campos: {[f.name for f in old_struct_field.fields] if old_struct_field.fields else 'None'}")
                    print(f"   Staging tiene campos: {[f.name for f in new_struct_field.fields] if new_struct_field.fields else 'None'}")
                    
                    if old_struct_field and new_struct_field and old_struct_field.fields and new_struct_field.fields:
                        # Fusionar campos: preservar TODOS los del final + agregar nuevos de staging
                        old_fields_dict = {f.name: f for f in old_struct_field.fields}
                        new_fields_dict = {f.name: f for f in new_struct_field.fields}
                        
                        merged_fields = []
                        # Paso 1: Preservar TODOS los campos del final primero (evita "Field missing")
                        for old_field in old_struct_field.fields:
                            if old_field.name in new_fields_dict:
                                # Existe en ambos: usar el de staging (más actualizado)
                                merged_fields.append(new_fields_dict[old_field.name])
                            else:
                                # Solo en final: PRESERVARLO (evita error "missing in new schema")
                                merged_fields.append(old_field)
                        
                        # Paso 2: Agregar campos nuevos de staging que no están en final
                        for new_field in new_struct_field.fields:
                            if new_field.name not in old_fields_dict:
                                merged_fields.append(new_field)
                        
                        # Crear campo STRUCT fusionado
                        if old_struct_field.mode == 'REPEATED' or new_struct_field.mode == 'REPEATED':
                            merged_struct_field = bigquery.SchemaField(
                                problematic_struct_field,
                                'RECORD',
                                mode='REPEATED',
                                fields=merged_fields,
                                description=old_struct_field.description
                            )
                        else:
                            merged_struct_field = bigquery.SchemaField(
                                problematic_struct_field,
                                old_struct_field.field_type,
                                mode=old_struct_field.mode,
                                fields=merged_fields,
                                description=old_struct_field.description
                            )
                    else:
                        # Fallback: usar esquema de staging
                        merged_struct_field = new_struct_field
                    
                    # Reemplazar el campo en el esquema final
                    updated_schema = []
                    for field in final_table.schema:
                        if field.name == problematic_struct_field:
                            updated_schema.append(merged_struct_field)
                        elif not field.name.startswith('_etl_'):
                            updated_schema.append(field)
                    
                    # Mantener campos ETL al final
                    etl_fields = [col for col in final_table.schema if col.name.startswith('_etl_')]
                    final_table.schema = updated_schema + etl_fields
                    try:
                        bq_client.update_table(final_table, ['schema'])
                        print(f"✅ Esquema de campo STRUCT {problematic_struct_field} actualizado.")
                    except Exception as schema_error:
                        print(f"❌ Error actualizando esquema STRUCT: {str(schema_error)}")
                        merge_success = False
                        merge_error_msg = f"Error actualizando esquema STRUCT {problematic_struct_field}: {str(schema_error)}. No se pudo ejecutar MERGE."
                        merge_time = time.time() - merge_start
                        # Paso 4: MERGE no ejecutado (indicar el motivo)
                        print(f"❌ MERGE con Soft Delete no ejecutado para {dataset_final}.{table_final} después de {merge_time:.1f}s: {merge_error_msg}")
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error actualizando esquema STRUCT",
                            event_message=merge_error_msg
                        )
                        # Paso 5: Endpoint completado con errores
                        endpoint_time = time.time() - endpoint_start_time
                        print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                        continue  # Continuar al siguiente endpoint
                    
                    # CRÍTICO: Los datos existentes tienen estructura antigua (camelCase)
                    # No podemos hacer MERGE directo - usar COPY + agregar campos ETL + UPDATE
                    print(f"🔄 Migrando datos de staging a final (incompatibilidad de STRUCT detectada)...")
                    try:
                        # Paso 1: Copiar datos de staging a final (reemplaza completamente)
                        # Esto reemplaza datos antiguos con camelCase por nuevos con snake_case
                        copy_job = bq_client.copy_table(
                            table_ref_staging,
                            table_ref_final,
                            job_config=bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
                        )
                        copy_job.result()
                        print(f"✅ Datos copiados de staging a final.")
                        
                        # Paso 2: Agregar campos ETL si no existen (COPY los eliminó)
                        final_table_check = bq_client.get_table(table_ref_final)
                        has_etl_synced = any(col.name == '_etl_synced' for col in final_table_check.schema)
                        has_etl_operation = any(col.name == '_etl_operation' for col in final_table_check.schema)
                        
                        if not has_etl_synced or not has_etl_operation:
                            print(f"🔧 Agregando campos ETL que se perdieron en COPY...")
                            try:
                                # BigQuery requiere ADD COLUMN por separado
                                if not has_etl_synced:
                                    alter_sql1 = f"ALTER TABLE `{project_id}.{dataset_final}.{table_final}` ADD COLUMN IF NOT EXISTS _etl_synced TIMESTAMP"
                                    bq_client.query(alter_sql1).result()
                                    print(f"  ✅ Campo _etl_synced agregado")
                                if not has_etl_operation:
                                    alter_sql2 = f"ALTER TABLE `{project_id}.{dataset_final}.{table_final}` ADD COLUMN IF NOT EXISTS _etl_operation STRING"
                                    bq_client.query(alter_sql2).result()
                                    print(f"  ✅ Campo _etl_operation agregado")
                                
                                # Verificar que los campos se agregaron correctamente
                                final_table_check = bq_client.get_table(table_ref_final)
                                has_etl_synced = any(col.name == '_etl_synced' for col in final_table_check.schema)
                                has_etl_operation = any(col.name == '_etl_operation' for col in final_table_check.schema)
                                
                                if not has_etl_synced or not has_etl_operation:
                                    raise Exception(f"Campos ETL no se pudieron agregar correctamente. _etl_synced: {has_etl_synced}, _etl_operation: {has_etl_operation}")
                                    
                            except Exception as etl_error:
                                raise Exception(f"Error agregando campos ETL: {str(etl_error)}")
                        
                        # Paso 3: Actualizar campos ETL en registros nuevos (solo si los campos existen)
                        update_etl_sql = f'''
                            UPDATE `{project_id}.{dataset_final}.{table_final}`
                            SET 
                                _etl_synced = CURRENT_TIMESTAMP(),
                                _etl_operation = 'INSERT'
                            WHERE _etl_synced IS NULL
                        '''
                        etl_job = bq_client.query(update_etl_sql)
                        etl_job.result()
                        
                        merge_time = time.time() - merge_start
                        merge_success = True
                        print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado en {merge_time:.1f}s (migración completa)")
                        
                        bq_client.delete_table(table_ref_staging, not_found_ok=True)
                        
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="SUCCESS",
                            event_title="MERGE exitoso (después de actualizar STRUCT)",
                            event_message=f"MERGE ejecutado exitosamente después de actualizar esquema de {problematic_struct_field} (migración completa de datos)"
                        )
                        
                        # Paso 5: Endpoint completado
                        endpoint_time = time.time() - endpoint_start_time
                        print(f"✅ Endpoint {endpoint_name} completado en {endpoint_time:.1f}s total")
                    except Exception as retry_error:
                        merge_success = False
                        merge_error_msg = f"Error en migración después de actualizar {problematic_struct_field}: {str(retry_error)}"
                        merge_time = time.time() - merge_start
                        print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {merge_error_msg}")
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error en MERGE (después de actualizar STRUCT)",
                            event_message=merge_error_msg,
                            info={"merge_sql": merge_sql}
                        )
                        endpoint_time = time.time() - endpoint_start_time
                        print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                        continue  # Continuar al siguiente endpoint
                else:
                    log_event_bq(
                        company_id=company_id,
                        company_name=company_name,
                        project_id=project_id,
                        endpoint=endpoint_name,
                        event_type="ERROR",
                        event_title="Error en MERGE",
                        event_message=f"Error en MERGE: campo STRUCT {problematic_struct_field} no encontrado en staging. {error_msg}",
                        info={"merge_sql": merge_sql}
                    )
                    merge_success = False
                    merge_time = time.time() - merge_start
                    print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: campo STRUCT {problematic_struct_field} no encontrado en staging")
                    endpoint_time = time.time() - endpoint_start_time
                    print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                    continue  # Continuar al siguiente endpoint
            elif type_mismatch:
                # Campo cambió de tipo (ej: INT64 -> STRING)
                new_type = type_mismatch.group(1)
                problematic_field = type_mismatch.group(2)
                old_type = type_mismatch.group(3)
                
                print(f"🔍 Campo con tipo incompatible detectado: {problematic_field} (staging: {new_type}, final: {old_type})")
                print(f"🔧 Actualizando tipo de campo {problematic_field} en tabla final...")
                
                # Obtener el campo actualizado de staging
                staging_table = bq_client.get_table(table_ref_staging)
                final_table = bq_client.get_table(table_ref_final)
                
                # Encontrar el campo en staging
                new_field = None
                for field in staging_table.schema:
                    if field.name == problematic_field:
                        new_field = field
                        break
                
                if new_field:
                    # Reemplazar el campo en el esquema final
                    updated_schema = []
                    for field in final_table.schema:
                        if field.name == problematic_field:
                            updated_schema.append(new_field)
                        elif not field.name.startswith('_etl_'):
                            updated_schema.append(field)
                    
                    # Mantener campos ETL al final
                    etl_fields = [col for col in final_table.schema if col.name.startswith('_etl_')]
                    final_table.schema = updated_schema + etl_fields
                    try:
                        bq_client.update_table(final_table, ['schema'])
                        print(f"✅ Tipo de campo {problematic_field} actualizado de {old_type} a {new_type}.")
                    except Exception as schema_error:
                        merge_success = False
                        merge_error_msg = f"Error actualizando esquema de tipo para {problematic_field}: {str(schema_error)}. No se pudo ejecutar MERGE."
                        merge_time = time.time() - merge_start
                        print(f"❌ MERGE con Soft Delete no ejecutado para {dataset_final}.{table_final} después de {merge_time:.1f}s: {merge_error_msg}")
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error actualizando esquema de tipo",
                            event_message=merge_error_msg
                        )
                        endpoint_time = time.time() - endpoint_start_time
                        print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                        continue  # Continuar al siguiente endpoint
                    
                    # Reintentar MERGE
                    try:
                        query_job = bq_client.query(merge_sql)
                        query_job.result()
                        merge_time = time.time() - merge_start
                        merge_success = True
                        print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado en {merge_time:.1f}s")
                        
                        bq_client.delete_table(table_ref_staging, not_found_ok=True)
                        # print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada.")
                        
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="SUCCESS",
                            event_title="MERGE exitoso (después de actualizar tipo)",
                            event_message=f"MERGE ejecutado exitosamente después de actualizar tipo de {problematic_field} de {old_type} a {new_type}"
                        )
                        
                        # Paso 5: Endpoint completado
                        endpoint_time = time.time() - endpoint_start_time
                        print(f"✅ Endpoint {endpoint_name} completado en {endpoint_time:.1f}s total")
                    except Exception as retry_error:
                        merge_success = False
                        merge_error_msg = f"Error en MERGE después de actualizar tipo de {problematic_field}: {str(retry_error)}"
                        merge_time = time.time() - merge_start
                        print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {merge_error_msg}")
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error en MERGE (después de actualizar tipo)",
                            event_message=merge_error_msg,
                            info={"merge_sql": merge_sql}
                        )
                        endpoint_time = time.time() - endpoint_start_time
                        print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                else:
                    log_event_bq(
                        company_id=company_id,
                        company_name=company_name,
                        project_id=project_id,
                        endpoint=endpoint_name,
                        event_type="ERROR",
                        event_title="Error en MERGE",
                        event_message=f"Error en MERGE: campo {problematic_field} no encontrado en staging. {error_msg}",
                        info={"merge_sql": merge_sql}
                    )
                    merge_success = False
                    merge_time = time.time() - merge_start
                    print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: campo {problematic_field} no encontrado en staging")
                    endpoint_time = time.time() - endpoint_start_time
                    print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
            else:
                # Detectar error de esquema faltante (ej: "Field address.isMilitary is missing in new schema")
                schema_missing_match = re.search(r'Field\s+([\w.]+)\s+is missing in new schema', error_msg, re.IGNORECASE)
                
                if schema_missing_match:
                    missing_field_path = schema_missing_match.group(1)
                    print(f"🔍 Campo faltante en esquema detectado: {missing_field_path}")
                    
                    # Extraer el nombre del campo STRUCT (parte antes del punto)
                    if '.' in missing_field_path:
                        struct_field_name = missing_field_path.split('.')[0]
                        print(f"🔧 Intentando fusionar campos del STRUCT {struct_field_name}...")
                        
                        # Obtener esquemas
                        staging_table = bq_client.get_table(table_ref_staging)
                        final_table = bq_client.get_table(table_ref_final)
                        
                        # Encontrar el campo STRUCT en ambos
                        staging_struct_field = None
                        final_struct_field = None
                        
                        for field in staging_table.schema:
                            if field.name == struct_field_name:
                                staging_struct_field = field
                                break
                        
                        for field in final_table.schema:
                            if field.name == struct_field_name:
                                final_struct_field = field
                                break
                        
                        if staging_struct_field and final_struct_field and \
                           staging_struct_field.field_type == 'STRUCT' and final_struct_field.field_type == 'STRUCT':
                            # Fusionar campos del STRUCT
                            old_fields_dict = {f.name: f for f in final_struct_field.fields}
                            new_fields_dict = {f.name: f for f in staging_struct_field.fields}
                            
                            merged_fields = []
                            # Preservar todos los campos existentes en final
                            for old_field in final_struct_field.fields:
                                if old_field.name in new_fields_dict:
                                    merged_fields.append(new_fields_dict[old_field.name])
                                else:
                                    merged_fields.append(old_field)
                            
                            # Agregar campos nuevos de staging
                            for new_field in staging_struct_field.fields:
                                if new_field.name not in old_fields_dict:
                                    merged_fields.append(new_field)
                            
                            # Crear nuevo campo STRUCT fusionado
                            merged_struct_field = bigquery.SchemaField(
                                struct_field_name,
                                'STRUCT',
                                mode=final_struct_field.mode,
                                fields=merged_fields,
                                description=final_struct_field.description
                            )
                            
                            # Actualizar esquema
                            updated_schema = []
                            for field in final_table.schema:
                                if field.name == struct_field_name:
                                    updated_schema.append(merged_struct_field)
                                elif not field.name.startswith('_etl_'):
                                    updated_schema.append(field)
                            
                            etl_fields = [col for col in final_table.schema if col.name.startswith('_etl_')]
                            final_table.schema = updated_schema + etl_fields
                            try:
                                bq_client.update_table(final_table, ['schema'])
                                print(f"✅ Esquema de STRUCT {struct_field_name} fusionado. Campo {missing_field_path} preservado.")
                            except Exception as schema_error:
                                merge_success = False
                                merge_error_msg = f"Error actualizando esquema STRUCT fusionado {struct_field_name}: {str(schema_error)}. No se pudo ejecutar MERGE."
                                merge_time = time.time() - merge_start
                                print(f"❌ MERGE con Soft Delete no ejecutado para {dataset_final}.{table_final} después de {merge_time:.1f}s: {merge_error_msg}")
                                log_event_bq(
                                    company_id=company_id,
                                    company_name=company_name,
                                    project_id=project_id,
                                    endpoint=endpoint_name,
                                    event_type="ERROR",
                                    event_title="Error actualizando esquema STRUCT fusionado",
                                    event_message=merge_error_msg
                                )
                                endpoint_time = time.time() - endpoint_start_time
                                print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                                continue  # Continuar al siguiente endpoint
                            
                            # Reintentar MERGE
                            try:
                                query_job = bq_client.query(merge_sql)
                                query_job.result()
                                merge_time = time.time() - merge_start
                                merge_success = True
                                print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado en {merge_time:.1f}s")
                                bq_client.delete_table(table_ref_staging, not_found_ok=True)
                                print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada.")
                                log_event_bq(
                                    company_id=company_id,
                                    company_name=company_name,
                                    project_id=project_id,
                                    endpoint=endpoint_name,
                                    event_type="SUCCESS",
                                    event_title="MERGE exitoso (después de fusionar STRUCT)",
                                    event_message=f"MERGE ejecutado exitosamente después de fusionar esquema de {struct_field_name}"
                                )
                                
                                # Paso 5: Endpoint completado
                                endpoint_time = time.time() - endpoint_start_time
                                print(f"✅ Endpoint {endpoint_name} completado en {endpoint_time:.1f}s total")
                            except Exception as retry_error:
                                merge_success = False
                                merge_error_msg = f"Error en MERGE después de fusionar {struct_field_name}: {str(retry_error)}"
                                merge_time = time.time() - merge_start
                                print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {merge_error_msg}")
                                log_event_bq(
                                    company_id=company_id,
                                    company_name=company_name,
                                    project_id=project_id,
                                    endpoint=endpoint_name,
                                    event_type="ERROR",
                                    event_title="Error en MERGE (después de fusionar STRUCT)",
                                    event_message=merge_error_msg
                                )
                                endpoint_time = time.time() - endpoint_start_time
                                print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                                continue  # Continuar al siguiente endpoint
                        else:
                            log_event_bq(
                                company_id=company_id,
                                company_name=company_name,
                                project_id=project_id,
                                endpoint=endpoint_name,
                                event_type="ERROR",
                                event_title="Error en MERGE",
                                event_message=f"Error en MERGE: no se pudo fusionar campo {missing_field_path}. {error_msg}",
                                info={"merge_sql": merge_sql}
                            )
                            merge_success = False
                            merge_time = time.time() - merge_start
                            print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: no se pudo fusionar campo {missing_field_path}")
                            endpoint_time = time.time() - endpoint_start_time
                            print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                            continue  # Continuar al siguiente endpoint
                    else:
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error en MERGE",
                            event_message=f"Error en MERGE o borrado de staging: {error_msg}. La tabla staging NO se borra para depuración.",
                            info={"merge_sql": merge_sql}
                        )
                        merge_success = False
                        merge_time = time.time() - merge_start
                        print(f"❌ MERGE con Soft Delete falló para {dataset_final}.{table_final} después de {merge_time:.1f}s: {error_msg} (la tabla staging NO se borra para depuración)")
                        endpoint_time = time.time() - endpoint_start_time
                        print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                        continue  # Continuar al siguiente endpoint
        
        # Borrar archivos temporales
        try:
            os.remove(temp_json)
            os.remove(temp_fixed)
        except Exception:
            pass
    
    # Resumen de tiempo por compañía
    company_elapsed = time.time() - company_start_time
    print(f"\n{'='*80}")
    print(f"✅ Compañía {company_name} (company_id: {company_id}) completada en {company_elapsed:.1f} segundos ({company_elapsed/60:.1f} minutos)")
    print(f"{'='*80}")

def main():
    # Tiempo límite del job (40 minutos = 2400 segundos)
    JOB_TIMEOUT_SECONDS = 40 * 60
    start_time = time.time()
    
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
    
    # Log de inicio del proceso ETL
    log_event_bq(
        event_type="INFO",
        event_title="Inicio proceso ETL",
        event_message=f"Iniciando proceso ETL de ServiceTitan para todas las compañías activas" + 
                     (f" (Tarea {task_index + 1}/{task_count})" if is_parallel else "")
    )

    print(f"\n{'='*80}")
    print("Conectando a BigQuery para obtener compañías...")
    print(f"⏱️  Tiempo límite del job: {JOB_TIMEOUT_SECONDS // 60} minutos")
    if is_parallel:
        print(f"🔄 Procesamiento paralelo: Tarea {task_index + 1} de {task_count}")
    # Crear cliente sin especificar project (usa el del service account = project_id real)
    # Usar PROJECT_ID_FOR_QUERY (project_id real) en la query SQL
    client = bigquery.Client()  # Usa el project del service account automáticamente
    query = f'''
        SELECT * FROM `{PROJECT_ID_FOR_QUERY}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_bigquery_status = TRUE
        ORDER BY company_id
    '''
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
    start_company_time = start_time
    
    for idx, row in enumerate(results, 1):
        # Verificar tiempo transcurrido antes de procesar cada compañía
        elapsed_time = time.time() - start_time
        remaining_time = JOB_TIMEOUT_SECONDS - elapsed_time
        
        # Si quedan menos de 5 minutos, loguear advertencia
        if remaining_time < 300:  # 5 minutos
            print(f"⚠️  ADVERTENCIA: Quedan {remaining_time // 60:.1f} minutos antes del timeout")
            log_event_bq(
                event_type="WARNING",
                event_title="Advertencia de timeout",
                event_message=f"Quedan {remaining_time // 60:.1f} minutos antes del timeout. Procesando compañía {idx}/{total}: {row.company_name}"
            )
        
        # Si el tiempo se agotó, lanzar excepción con mensaje claro
        if elapsed_time >= JOB_TIMEOUT_SECONDS:
            elapsed_minutes = elapsed_time / 60
            log_event_bq(
                event_type="ERROR",
                event_title="Timeout del job",
                event_message=f"Job interrumpido por timeout después de {elapsed_minutes:.1f} minutos. Procesadas {procesadas}/{total} compañías. Última compañía procesada: {row.company_name if idx > 1 else 'ninguna'}"
            )
            print(f"\n{'='*80}")
            print(f"⏱️  TIMEOUT: Job interrumpido después de {elapsed_minutes:.1f} minutos")
            print(f"📊 Progreso: {procesadas}/{total} compañías procesadas exitosamente")
            print(f"🔄 El job se reiniciará automáticamente desde el inicio")
            print(f"{'='*80}")
            raise TimeoutError(f"Job timeout después de {elapsed_minutes:.1f} minutos. Procesadas {procesadas}/{total} compañías.")
        
        # Log del progreso
        elapsed_minutes = elapsed_time / 60
        if is_parallel:
            # Calcular índice global para mostrar progreso total
            companies_per_task_base = len(results) // task_count if task_count > 0 else 0
            remainder = len(results) % task_count if task_count > 0 else 0
            start_idx = task_index * companies_per_task_base + min(task_index, remainder)
            global_idx = start_idx + idx
            print(f"\n📊 Progreso: Compañía {idx}/{total_assigned} (global: ~{global_idx}) | Tiempo: {elapsed_minutes:.1f} min | Restante: {remaining_time // 60:.1f} min")
        else:
            print(f"\n📊 Progreso: Compañía {idx}/{total_assigned} | Tiempo: {elapsed_minutes:.1f} min | Restante: {remaining_time // 60:.1f} min")
        
        try:
            company_start_time = time.time()
            process_company(row)
            procesadas += 1
            # company_elapsed = time.time() - company_start_time
            # print(f"✅ Compañía {row.company_name} procesada en {company_elapsed:.1f} segundos")  # Duplicado, ya se muestra al final de process_company
        except TimeoutError:
            # Re-lanzar timeout para que se propague
            raise
        except Exception as e:
            log_event_bq(
                company_id=row.company_id,
                company_name=row.company_name,
                project_id=row.company_project_id,
                event_type="ERROR",
                event_title="Error procesando compañía",
                event_message=f"Error procesando compañía {row.company_name}: {str(e)}"
            )
            print(f"❌ Error procesando compañía {row.company_name}: {str(e)}")
            # Continuar con la siguiente compañía
    
    # Log de fin del proceso ETL
    total_time = time.time() - start_time
    total_minutes = total_time / 60
    task_info = f" (Tarea {task_index + 1}/{task_count})" if is_parallel else ""
    log_event_bq(
        event_type="SUCCESS",
        event_title="Fin proceso ETL",
        event_message=f"Proceso ETL completado{task_info}. {procesadas}/{total_assigned} compañías procesadas exitosamente en {total_minutes:.1f} minutos."
    )
    
    print(f"\n{'='*80}")
    if is_parallel:
        print(f"🏁 Resumen Tarea {task_index + 1}/{task_count}: {procesadas}/{total_assigned} compañías procesadas en {total_minutes:.1f} minutos.")
    else:
        print(f"🏁 Resumen: {procesadas}/{total_assigned} compañías procesadas exitosamente en {total_minutes:.1f} minutos.")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()