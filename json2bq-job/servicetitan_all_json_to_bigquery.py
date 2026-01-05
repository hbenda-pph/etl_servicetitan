import os
import json
import re
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

# Configuración para logging centralizado (usar proyecto detectado)
LOGS_PROJECT = PROJECT_SOURCE
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
    Convierte objetos a arrays cuando el campo debería ser array (ej: serialNumbers).
    También convierte NULL a [] para campos que están en known_array_fields."""
    if known_array_fields is None:
        known_array_fields = set()
    
    # Si es None, verificar si este campo debería ser un array
    if value is None:
        # Extraer el nombre del campo del path (último componente)
        field_name = field_path.split('.')[-1] if field_path else ""
        if field_name in known_array_fields:
            return []  # Convertir NULL a array vacío para campos REPEATED
        return None
    
    # Si es un diccionario/objeto
    if isinstance(value, dict):
        # Verificar si este campo debería ser un array (por nombre común)
        # serialNumbers, serial_numbers, etc. deberían ser arrays
        field_name_lower = field_path.lower() if field_path else ""
        if 'serial' in field_name_lower and 'number' in field_name_lower:
            # Si es un objeto pero debería ser array, convertir a array vacío
            return []
        
        # Si no, procesar recursivamente
        fixed_dict = {}
        for k, v in value.items():
            snake_key = to_snake_case(k)
            nested_path = f"{field_path}.{snake_key}" if field_path else snake_key
            
            # Verificar si este campo anidado debería ser array pero viene como objeto
            nested_field_lower = nested_path.lower()
            if 'serial' in nested_field_lower and 'number' in nested_field_lower and isinstance(v, dict):
                # Convertir objeto a array vacío
                fixed_dict[snake_key] = []
            else:
                fixed_dict[snake_key] = fix_nested_value(v, nested_path, known_array_fields)
        return fixed_dict
    
    # Si es una lista, procesar cada elemento
    if isinstance(value, list):
        return [fix_nested_value(item, field_path, known_array_fields) for item in value]
    
    # Para otros tipos, retornar tal cual
    return value

def fix_json_format(local_path, temp_path, repeated_fields=None):
    """Transforma el JSON a formato newline-delimited y snake_case.
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
    
    # Detectar campos que son arrays en al menos un registro (nivel superior)
    detected_array_fields = set()
    for item in json_data:
        for key, value in item.items():
            if isinstance(value, list):
                detected_array_fields.add(to_snake_case(key))
    
    # Combinar campos detectados con los proporcionados (si hay)
    array_fields = detected_array_fields
    if repeated_fields:
        array_fields = array_fields | set(repeated_fields)
    
    # Transformar y limpiar
    with open(temp_path, 'w', encoding='utf-8') as f:
        for item in json_data:
            new_item = {}
            for k, v in item.items():
                snake_key = to_snake_case(k)
                
                # Procesar recursivamente para corregir campos anidados
                # Pasar array_fields para que fix_nested_value pueda convertir NULL a [] cuando corresponda
                fixed_value = fix_nested_value(v, snake_key, array_fields)
                
                # Si el campo es un array y viene como NULL (después del procesamiento recursivo),
                # convertir a array vacío como fallback adicional
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
    
    # Log de inicio de procesamiento de compañía
    log_event_bq(
        company_id=company_id,
        company_name=company_name,
        project_id=project_id,
        event_type="INFO",
        event_title="Inicio procesamiento compañía",
        event_message=f"Iniciando procesamiento de {company_name} (ID: {company_id})"
    )
    
    print(f"\n{'='*80}\n🏢 Procesando compañía: {company_name} (ID: {company_id}) | project_id: {project_id}")
    bucket_name = f"{project_id}_servicetitan"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    for endpoint_name, table_name in ENDPOINTS:
        # Usar table_name directamente desde metadata para archivos JSON y tablas
        json_filename = f"servicetitan_{table_name}.json"
        temp_json = f"/tmp/{project_id}_{table_name}.json"
        temp_fixed = f"/tmp/fixed_{project_id}_{table_name}.json"
        
        # Log de inicio de procesamiento de endpoint
        log_event_bq(
            company_id=company_id,
            company_name=company_name,
            project_id=project_id,
            endpoint=endpoint_name,
            event_type="INFO",
            event_title="Inicio procesamiento endpoint",
            event_message=f"Procesando endpoint {endpoint_name} (tabla: {table_name}) para {company_name}"
        )
        
        # Descargar archivo JSON del bucket
        try:
            blob = bucket.blob(json_filename)
            if not blob.exists():
                log_event_bq(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="WARNING",
                    event_title="Archivo no encontrado",
                    event_message=f"Archivo {json_filename} no encontrado en bucket {bucket_name}"
                )
                print(f"⚠️  Archivo no encontrado: {json_filename} en bucket {bucket_name}")
                continue
            blob.download_to_filename(temp_json)
            print(f"⬇️  Descargado {json_filename} de gs://{bucket_name}")
        except Exception as e:
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error descargando archivo",
                event_message=f"Error descargando {json_filename}: {str(e)}"
            )
            print(f"❌ Error descargando {json_filename}: {str(e)}")
            continue
        
        # Transformar a newline-delimited y snake_case (primera pasada)
        try:
            fix_json_format(temp_json, temp_fixed)
            print(f"🔄 Transformado a newline-delimited y snake_case: {temp_fixed}")
        except Exception as e:
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error transformando archivo",
                event_message=f"Error transformando {json_filename}: {str(e)}"
            )
            print(f"❌ Error transformando {json_filename}: {str(e)}")
            continue
        
        # Cargar a tabla staging en BigQuery
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
            print(f"✅ Cargado a tabla staging: {dataset_staging}.{table_staging}")
            
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="SUCCESS",
                event_title="Carga a staging exitosa",
                event_message=f"Archivo cargado exitosamente a {dataset_staging}.{table_staging}"
            )
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
                # Convertir a snake_case para asegurar consistencia
                field_name_snake = to_snake_case(field_name)
                print(f"🔧 Campo a limpiar (snake_case): {field_name_snake}")
                fix_json_format(temp_json, temp_fixed, repeated_fields={field_name_snake})
                
                # Reintentar carga
                try:
                    load_job = bq_client.load_table_from_file(
                        open(temp_fixed, "rb"),
                        table_ref_staging,
                        job_config=job_config
                    )
                    load_job.result()
                    print(f"✅ Cargado a tabla staging: {dataset_staging}.{table_staging} (después de limpieza)")
                    
                    log_event_bq(
                        company_id=company_id,
                        company_name=company_name,
                        project_id=project_id,
                        endpoint=endpoint_name,
                        event_type="SUCCESS",
                        event_title="Carga a staging exitosa (después de limpieza)",
                        event_message=f"Archivo cargado exitosamente a {dataset_staging}.{table_staging} después de limpiar campo {problematic_field}"
                    )
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
                continue
        
        # Asegurar que la tabla final existe
        try:
            bq_client.get_table(table_ref_final)
            print(f"✅ Tabla final {dataset_final}.{table_final} ya existe.")
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
        # Obtener esquemas de ambas tablas
        staging_schema = bq_client.get_table(table_ref_staging).schema
        final_schema = bq_client.get_table(table_ref_final).schema
        
        # Obtener nombres de columnas (excluyendo campos ETL y id)
        staging_cols = {col.name for col in staging_schema if col.name != 'id' and not col.name.startswith('_etl_')}
        final_cols = {col.name for col in final_schema if col.name != 'id' and not col.name.startswith('_etl_')}
        common_cols = staging_cols & final_cols  # Intersección: columnas en ambas tablas
        new_cols = staging_cols - final_cols  # Columnas nuevas en staging que no están en final
        
        # Verificar si hay STRUCTs que necesitan fusión (campos comunes que son STRUCT)
        structs_to_merge = {}
        for col_name in common_cols:
            staging_field = next((f for f in staging_schema if f.name == col_name), None)
            final_field = next((f for f in final_schema if f.name == col_name), None)
            if staging_field and final_field and staging_field.field_type == 'STRUCT' and final_field.field_type == 'STRUCT':
                # Verificar si hay diferencias en los campos anidados
                staging_subfields = {f.name for f in staging_field.fields}
                final_subfields = {f.name for f in final_field.fields}
                if staging_subfields != final_subfields:
                    structs_to_merge[col_name] = (staging_field, final_field)
        
        # Si hay columnas nuevas, agregarlas al esquema de la tabla final
        # IMPORTANTE: Usar ALTER TABLE ADD COLUMN en lugar de reconstruir el esquema completo
        # para evitar perder campos anidados en STRUCTs existentes
        if new_cols:
            try:
                print(f"🆕 Columnas nuevas detectadas: {sorted(new_cols)}. Agregando al esquema de tabla final...")
                final_table = bq_client.get_table(table_ref_final)
                
                # Obtener esquema completo de staging (sin campos ETL)
                new_schema_fields = [col for col in staging_schema if col.name in new_cols]
                
                # Agregar cada nueva columna individualmente usando ALTER TABLE
                # Esto preserva todos los campos existentes, incluyendo campos anidados en STRUCTs
                for new_field in new_schema_fields:
                    try:
                        # Construir SQL ALTER TABLE para agregar la columna
                        # Necesitamos convertir el SchemaField a definición SQL
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
        
        # Construir UPDATE SET solo con columnas comunes (ahora incluye las nuevas)
        update_set = ', '.join([f'T.{col} = S.{col}' for col in sorted(staging_cols)])
        
        # Para INSERT, usar todas las columnas de staging (excepto ETL)
        insert_cols = [col.name for col in staging_schema if not col.name.startswith('_etl_')]
        insert_values = [f'S.{col.name}' for col in staging_schema if not col.name.startswith('_etl_')]
        
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
            print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado.")
            
            # Solo borrar tabla staging si el MERGE fue exitoso
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
            print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada.")
            
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="SUCCESS",
                event_title="MERGE exitoso",
                event_message=f"MERGE con Soft Delete ejecutado exitosamente y tabla staging eliminada"
            )
        except Exception as e:
            error_msg = str(e)
            # Detectar error de incompatibilidad de STRUCT
            # Formato: "Value of type STRUCT<...> cannot be assigned to T.address, which has type STRUCT<...>"
            struct_match = re.search(r'cannot be assigned to T\.(\w+), which has type STRUCT', error_msg)
            
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
                    
                    if old_struct_field and old_struct_field.field_type == 'STRUCT' and new_struct_field.field_type == 'STRUCT':
                        # Fusionar campos del STRUCT: combinar campos existentes con nuevos
                        old_fields_dict = {f.name: f for f in old_struct_field.fields}
                        new_fields_dict = {f.name: f for f in new_struct_field.fields}
                        
                        # Combinar: campos existentes primero, luego nuevos campos
                        merged_fields = []
                        # Agregar campos existentes (preservar orden y campos que no están en staging)
                        for old_field in old_struct_field.fields:
                            if old_field.name in new_fields_dict:
                                # Campo existe en ambos: usar el de staging (puede tener cambios)
                                merged_fields.append(new_fields_dict[old_field.name])
                            else:
                                # Campo solo existe en final: preservarlo
                                merged_fields.append(old_field)
                        
                        # Agregar campos nuevos de staging que no están en final
                        for new_field in new_struct_field.fields:
                            if new_field.name not in old_fields_dict:
                                merged_fields.append(new_field)
                        
                        # Crear nuevo campo STRUCT con campos fusionados
                        merged_struct_field = bigquery.SchemaField(
                            problematic_struct_field,
                            'STRUCT',
                            mode=old_struct_field.mode,
                            fields=merged_fields,
                            description=old_struct_field.description
                        )
                    else:
                        # Si no es STRUCT o no se puede fusionar, usar el campo de staging directamente
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
                    bq_client.update_table(final_table, ['schema'])
                    print(f"✅ Esquema de campo STRUCT {problematic_struct_field} actualizado.")
                    
                    # Reintentar MERGE
                    try:
                        query_job = bq_client.query(merge_sql)
                        query_job.result()
                        print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado.")
                        
                        bq_client.delete_table(table_ref_staging, not_found_ok=True)
                        print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada.")
                        
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="SUCCESS",
                            event_title="MERGE exitoso (después de actualizar STRUCT)",
                            event_message=f"MERGE ejecutado exitosamente después de actualizar esquema de {problematic_struct_field}"
                        )
                    except Exception as retry_error:
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error en MERGE (después de actualizar STRUCT)",
                            event_message=f"Error en MERGE después de actualizar {problematic_struct_field}: {str(retry_error)}",
                            info={"merge_sql": merge_sql}
                        )
                        print(f"❌ Error en MERGE después de actualizar STRUCT: {str(retry_error)}")
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
                    print(f"❌ Error en MERGE: campo STRUCT {problematic_struct_field} no encontrado en staging")
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
                    bq_client.update_table(final_table, ['schema'])
                    print(f"✅ Tipo de campo {problematic_field} actualizado de {old_type} a {new_type}.")
                    
                    # Reintentar MERGE
                    try:
                        query_job = bq_client.query(merge_sql)
                        query_job.result()
                        print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado.")
                        
                        bq_client.delete_table(table_ref_staging, not_found_ok=True)
                        print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada.")
                        
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="SUCCESS",
                            event_title="MERGE exitoso (después de actualizar tipo)",
                            event_message=f"MERGE ejecutado exitosamente después de actualizar tipo de {problematic_field} de {old_type} a {new_type}"
                        )
                    except Exception as retry_error:
                        log_event_bq(
                            company_id=company_id,
                            company_name=company_name,
                            project_id=project_id,
                            endpoint=endpoint_name,
                            event_type="ERROR",
                            event_title="Error en MERGE (después de actualizar tipo)",
                            event_message=f"Error en MERGE después de actualizar tipo de {problematic_field}: {str(retry_error)}",
                            info={"merge_sql": merge_sql}
                        )
                        print(f"❌ Error en MERGE después de actualizar tipo: {str(retry_error)}")
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
                    print(f"❌ Error en MERGE: campo {problematic_field} no encontrado en staging")
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
                            bq_client.update_table(final_table, ['schema'])
                            print(f"✅ Esquema de STRUCT {struct_field_name} fusionado. Campo {missing_field_path} preservado.")
                            
                            # Reintentar MERGE
                            try:
                                query_job = bq_client.query(merge_sql)
                                query_job.result()
                                print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado.")
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
                            except Exception as retry_error:
                                log_event_bq(
                                    company_id=company_id,
                                    company_name=company_name,
                                    project_id=project_id,
                                    endpoint=endpoint_name,
                                    event_type="ERROR",
                                    event_title="Error en MERGE (después de fusionar STRUCT)",
                                    event_message=f"Error en MERGE después de fusionar {struct_field_name}: {str(retry_error)}"
                                )
                                print(f"❌ Error en MERGE después de fusionar STRUCT: {str(retry_error)}")
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
                            print(f"❌ Error en MERGE: no se pudo fusionar campo {missing_field_path}")
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
                        print(f"❌ Error en MERGE o borrado de staging: {error_msg} (la tabla staging NO se borra para depuración)")
                        continue  # Continuar al siguiente endpoint
        
        # Borrar archivos temporales
        try:
            os.remove(temp_json)
            os.remove(temp_fixed)
        except Exception:
            pass
    
    # Log de fin de procesamiento de compañía
    log_event_bq(
        company_id=company_id,
        company_name=company_name,
        project_id=project_id,
        event_type="SUCCESS",
        event_title="Fin procesamiento compañía",
        event_message=f"Procesamiento completado para {company_name}"
    )

def main():
    # Log de inicio del proceso ETL
    log_event_bq(
        event_type="INFO",
        event_title="Inicio proceso ETL",
        event_message="Iniciando proceso ETL de ServiceTitan para todas las compañías activas"
    )
    
    print("Conectando a BigQuery para obtener compañías...")
    client = bigquery.Client(project=PROJECT_SOURCE)
    query = f'''
        SELECT * FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_bigquery_status = TRUE
        ORDER BY company_id
    '''
    results = client.query(query).result()
    total = 0
    procesadas = 0
    
    for row in results:
        total += 1
        try:
            process_company(row)
            procesadas += 1
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
    
    # Log de fin del proceso ETL
    log_event_bq(
        event_type="SUCCESS",
        event_title="Fin proceso ETL",
        event_message=f"Proceso ETL completado. {procesadas}/{total} compañías procesadas exitosamente."
    )
    
    print(f"\n{'='*80}")
    print(f"🏁 Resumen: {procesadas}/{total} compañías procesadas exitosamente.")

if __name__ == "__main__":
    main()