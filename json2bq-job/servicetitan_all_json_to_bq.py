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
    validate_json_file,
    align_schemas_before_merge,
    execute_merge_or_insert,
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
        print(f"⚠️ [detectar_cambios_reales] Error detectando cambios: {str(e)}")
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
    
    endpoints_count = 0
    for endpoint_name, table_name, use_merge, is_production in ENDPOINTS:
        endpoints_count += 1
        endpoint_start_time = time.time()
        json_filename = f"servicetitan_{table_name}.json"
        temp_json = f"/tmp/{project_id}_{table_name}.json"
        temp_fixed = f"/tmp/fixed_{project_id}_{table_name}.json"
        
        if not is_production:
            merge_label = "DEV"
        elif not use_merge:
            merge_label = "OVERWRITE"
        else:
            merge_label = "MERGE"
        print(f"\n📦 ENDPOINT: {endpoint_name} (tabla: {table_name}) [{merge_label}] company {company_id}")
        
        # Descargar archivo JSON del bucket
        try:
            download_start = time.time()
            blob = bucket.blob(json_filename)
            if not blob.exists():
                print(f"⚠️ [process_company] Archivo no encontrado: {json_filename} en bucket {bucket_name}")
                # Paso 4: MERGE no ejecutado (archivo faltante)
                merge_time = 0.0  # No hubo intento de MERGE
                print(f"❌ [process_company] MERGE con Soft Delete no ejecutado para bronze.{table_name}: archivo {json_filename} no encontrado en bucket")
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
                print(f"❌ [process_company] Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                continue
            blob.download_to_filename(temp_json)
            download_time = time.time() - download_start
            file_size_mb = os.path.getsize(temp_json) / (1024 * 1024)
            print(f"⬇️  Descargado {json_filename} ({file_size_mb:.2f} MB) en {download_time:.1f}s")
            
            # Validar JSON inmediatamente después de descargar
            print(f"🔍 Validando estructura JSON...")
            is_valid, validation_error, json_type = validate_json_file(temp_json)
            if not is_valid:
                print(f"❌ [process_company] ARCHIVO JSON MAL FORMADO: {validation_error}")
                print(f"❌ [process_company] El archivo {json_filename} está corrupto o mal generado por el job anterior (st2json)")
                log_event_bq_all(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Archivo JSON mal formado",
                    event_message=f"Archivo {json_filename} está mal formado: {validation_error}. Revisar job st2json."
                )
                # Paso 4: MERGE no ejecutado (archivo corrupto)
                merge_time = 0.0
                print(f"❌ [process_company] MERGE con Soft Delete no ejecutado para bronze.{table_name}: archivo JSON mal formado")
                # Paso 5: Endpoint completado con errores
                endpoint_time = time.time() - endpoint_start_time
                print(f"❌ [process_company] Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
                continue
            print(f"✅ JSON válido (tipo: {json_type})")
        except Exception as e:
            print(f"❌ [process_company] Error descargando {json_filename}: {str(e)}")
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
            print(f"❌ [process_company] MERGE con Soft Delete no ejecutado para bronze.{table_name}: error descargando archivo - {str(e)}")
            # Paso 5: Endpoint completado con errores
            endpoint_time = time.time() - endpoint_start_time
            print(f"❌ [process_company] Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
            continue
        
        # Transformar a newline-delimited y snake_case
        # Si bronze ya existe, construir bronze_type_map para coerción de tipos
        # en una sola pasada (evita doble carga a staging por type mismatch).
        try:
            transform_start = time.time()
            file_size_mb = os.path.getsize(temp_json) / (1024 * 1024)
            if file_size_mb > 100:
                print(f"🔄 Transformando archivo grande ({file_size_mb:.2f} MB) a newline-delimited y snake_case (esto puede tomar varios minutos)...")
            bronze_type_map = None
            try:
                _bq_tmp = bigquery.Client(project=project_id)
                _bronze = _bq_tmp.get_table(f"{project_id}.bronze.{table_name}")
                bronze_type_map = {
                    f.name: f.field_type for f in _bronze.schema
                    if f.field_type not in ('RECORD', 'STRUCT') and f.mode != 'REPEATED'
                }
            except Exception:
                pass  # Primera carga o error → sin coerción, BQ autodetecta
            fix_json_format(temp_json, temp_fixed, bronze_type_map=bronze_type_map)
            transform_time = time.time() - transform_start
            if file_size_mb <= 100:
                print(f"🔄 Transformado a newline-delimited y snake_case en {transform_time:.1f}s")
        except Exception as e:
            print(f"❌ [process_company] Error transformando {json_filename}: {str(e)}")
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
            print(f"❌ [process_company] MERGE con Soft Delete no ejecutado para bronze.{table_name}: error transformando archivo - {str(e)}")
            # Paso 5: Endpoint completado con errores
            endpoint_time = time.time() - endpoint_start_time
            print(f"❌ [process_company] Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
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
            print(f"❌ [process_company] Error cargando a tabla staging: {error_msg}")
            # Paso 4: MERGE no ejecutado (error cargando a staging)
            merge_time = 0.0  # No hubo intento de MERGE
            print(f"❌ [process_company] MERGE con Soft Delete no ejecutado para bronze.{table_name}: error cargando a staging - {error_msg}")
            # Paso 5: Endpoint completado con errores
            endpoint_time = time.time() - endpoint_start_time
            print(f"❌ [process_company] Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
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
        # MERGE/INSERT (usando función común unificada y blindada)
        merge_start = time.time()
        staging_table = bq_client.get_table(table_ref_staging)
        final_table = bq_client.get_table(table_ref_final)
        
        # Verificar y corregir incompatibilidades de esquema (tipos de datos)
        print(f"🔍 Verificando compatibilidad de esquemas entre staging y final...")
        needs_correction, corrections_made, alignment_error, type_mismatches = align_schemas_before_merge(
            bq_client=bq_client,
            staging_table=staging_table,
            final_table=final_table,
            project_id=project_id,
            dataset_final=dataset_final,
            table_final=table_final
        )
        
        if alignment_error:
            print(f"❌ [process_company] Error alineando esquemas: {clean_bq_error(alignment_error)}")
            log_event_bq_all(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error alineando esquemas",
                event_message=f"Error alineando esquemas antes del MERGE/INSERT: {alignment_error}"
            )
            
        if needs_correction:
            final_table = bq_client.get_table(table_ref_final)
            
        # Ejecutar MERGE, INSERT o OVERWRITE (la función común ahora maneja ALTER TABLE internamente)
        merge_success, merge_time, merge_error_msg = execute_merge_or_insert(
            bq_client=bq_client,
            staging_table=staging_table,
            final_table=final_table,
            project_id=project_id,
            dataset_final=dataset_final,
            table_final=table_final,
            dataset_staging=dataset_staging,
            table_staging=table_staging,
            merge_start=merge_start,
            log_event_callback=log_event_bq_all,
            company_id=company_id,
            company_name=company_name,
            endpoint_name=endpoint_name,
            type_mismatches=type_mismatches,
            use_merge=use_merge,
            temp_fixed=temp_fixed,
            is_production=is_production
        )
        
        if merge_success:
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
            
            endpoint_time = time.time() - endpoint_start_time
            print(f"✅ Endpoint {endpoint_name} completado en {endpoint_time:.1f}s total")
        else:
            print(f"❌ [process_company] Error en MERGE/INSERT o borrado de staging: {clean_bq_error(merge_error_msg)} (la tabla staging NO se borra para depuración)")
            endpoint_time = time.time() - endpoint_start_time
            print(f"❌ [process_company] Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
        
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
    
    return True, endpoints_count, 0, 0, None

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
            print(f"⚠️ [main] ADVERTENCIA: Quedan {remaining_time // 60:.1f} minutos antes del timeout")
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
            print(f"❌ [main] Error procesando compañía {row.company_name}: {str(e)}")
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
