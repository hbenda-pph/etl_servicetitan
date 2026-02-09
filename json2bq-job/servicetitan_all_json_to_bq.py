import os
import json
import re
import time
from datetime import datetime, timezone
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

# Importar funciones comunes
from servicetitan_common import (
    get_project_source,
    get_bigquery_project_id,
    load_endpoints_from_metadata,
    log_event_bq,
    fix_nested_value,
    fix_json_format,
    transform_item,
    _schema_field_to_sql,
    to_snake_case,
    load_json_to_staging_with_error_handling,
    LOGS_PROJECT,
    LOGS_DATASET,
    LOGS_TABLE
)

PROJECT_SOURCE = get_project_source()
PROJECT_ID_FOR_QUERY = get_bigquery_project_id()
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Cargar endpoints automáticamente desde metadata
ENDPOINTS = load_endpoints_from_metadata()

# Wrapper para log_event_bq con source correcto
def log_event_bq_all(*args, **kwargs):
    """Wrapper para log_event_bq con source='servicetitan_all_json_to_bq'"""
    kwargs.setdefault('source', 'servicetitan_all_json_to_bq')
    return log_event_bq(*args, **kwargs)

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
        
        print(f"\n📦 ENDPOINT: {endpoint_name} (tabla: {table_name}) company {company_id}")
        
        # Descargar archivo JSON del bucket
        try:
            download_start = time.time()
            blob = bucket.blob(json_filename)
            if not blob.exists():
                print(f"⚠️  Archivo no encontrado: {json_filename} en bucket {bucket_name}")
                # Paso 4: MERGE no ejecutado (archivo faltante)
                merge_time = 0.0  # No hubo intento de MERGE
                print(f"❌ MERGE con Soft Delete no ejecutado para bronze.{table_name}: archivo {json_filename} no encontrado en bucket")
                log_event_bq_all(
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
            log_event_bq_all(
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
            file_size_mb = os.path.getsize(temp_json) / (1024 * 1024)
            if file_size_mb > 100:
                print(f"🔄 Transformando archivo grande ({file_size_mb:.2f} MB) a newline-delimited y snake_case (esto puede tomar varios minutos)...")
            fix_json_format(temp_json, temp_fixed)
            transform_time = time.time() - transform_start
            if file_size_mb <= 100:
                print(f"🔄 Transformado a newline-delimited y snake_case en {transform_time:.1f}s")
        except Exception as e:
            print(f"❌ Error transformando {json_filename}: {str(e)}")
            log_event_bq_all(
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
        
        # Limpiar staging al inicio para evitar conflictos de esquemas de ejecuciones anteriores
        try:
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
        except Exception:
            pass  # Continuar si hay error al borrar
        
        # Usar función común para cargar con manejo automático de errores
        success, load_time, error_msg = load_json_to_staging_with_error_handling(
            bq_client=bq_client,
            temp_fixed=temp_fixed,
            temp_json=temp_json,
            table_ref_staging=table_ref_staging,
            project_id=project_id,
            table_name=table_name,
            table_staging=table_staging,
            dataset_staging=dataset_staging,
            load_start=load_start,
            log_event_callback=log_event_bq_all,
            company_id=company_id,
            company_name=company_name,
            endpoint_name=endpoint_name
        )
        
        if not success:
            print(f"❌ Error cargando a tabla staging: {error_msg}")
            # Paso 4: MERGE no ejecutado (error cargando a staging)
            merge_time = 0.0  # No hubo intento de MERGE
            print(f"❌ MERGE con Soft Delete no ejecutado para bronze.{table_name}: error cargando a staging - {error_msg}")
            # Paso 5: Endpoint completado con errores
            endpoint_time = time.time() - endpoint_start_time
            print(f"❌ Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
            continue
        
        # Carga exitosa, continuar con el proceso
        # (load_time ya está calculado por la función común)
        
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
            
            log_event_bq_all(
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
                log_event_bq_all(
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
                        log_event_bq_all(
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
                        
                        log_event_bq_all(
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
                        log_event_bq_all(
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
                    log_event_bq_all(
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
                        log_event_bq_all(
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
                        
                        log_event_bq_all(
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
                        log_event_bq_all(
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
                    log_event_bq_all(
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
                                log_event_bq_all(
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
                                log_event_bq_all(
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
                                log_event_bq_all(
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
                            log_event_bq_all(
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
                        log_event_bq_all(
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
    log_event_bq_all(
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
            log_event_bq_all(
                event_type="WARNING",
                event_title="Advertencia de timeout",
                event_message=f"Quedan {remaining_time // 60:.1f} minutos antes del timeout. Procesando compañía {idx}/{total}: {row.company_name}"
            )
        
        # Si el tiempo se agotó, lanzar excepción con mensaje claro
        if elapsed_time >= JOB_TIMEOUT_SECONDS:
            elapsed_minutes = elapsed_time / 60
            log_event_bq_all(
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
            log_event_bq_all(
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
    log_event_bq_all(
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
