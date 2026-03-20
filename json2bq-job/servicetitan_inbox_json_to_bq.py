"""
ETL INBOX: JSON (Cloud Storage) → BigQuery

Refactorizado para quedar alineado con `servicetitan_json_to_bq.py`,
reutilizando `servicetitan_common.py` del job json2bq.

Salvedades INBOX:
- la tabla de configuración se lee desde `pph-inbox.settings.companies`
- procesa múltiples compañías activas (company_bigquery_status = TRUE)
- cada compañía escribe en su propio proyecto temporal (usa `company_project_id`)
"""

import os
import re
import time
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

from servicetitan_common import (
    load_endpoints_from_metadata,
    log_event_bq,
    fix_json_format,
    load_json_to_staging_with_error_handling,
    validate_json_file,
    align_schemas_before_merge,
    execute_merge_or_insert,
)

# Fuente de configuración INBOX
PROJECT_ID_FOR_QUERY = "pph-inbox"
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Endpoints desde metadata (centralizada)
ENDPOINTS = load_endpoints_from_metadata()

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
    # Cada compañía INBOX tiene su propio proyecto temporal (Estrategia A)
    project_id = row.company_project_id
    if not project_id:
        raise ValueError(f"company_project_id vacío para company_id={company_id}. Primero crea el proyecto INBOX por compañía.")
    
    # Log de inicio de procesamiento de compañía
    log_event_bq(
        company_id=company_id,
        company_name=company_name,
        project_id=project_id,
        event_type="INFO",
        event_title="Inicio procesamiento compañía",
        event_message=f"Iniciando procesamiento de {company_name} (ID: {company_id})"
    )
    
    print(f"\n{'='*80}\n🏢 Procesando compañía INBOX: {company_name} (ID: {company_id}) | project_id: {project_id}")
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
            # Intentar extraer campo REPEATED del mensaje de error
            # Formato: "Field: permissions; Value: NULL" (puede estar en cualquier parte del mensaje)
            match = re.search(r'Field:\s*(\w+);\s*Value:\s*NULL', error_msg, re.IGNORECASE)
            
            if match:
                problematic_field = match.group(1)
                print(f"🔍 Campo REPEATED detectado del error: {problematic_field}")
                print(f"🧹 Limpiando datos: convirtiendo NULL a [] para campo {problematic_field}")
                
                # Limpiar datos con el campo detectado
                fix_json_format(temp_json, temp_fixed, repeated_fields={problematic_field})
                
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
        
        # Si hay columnas nuevas, agregarlas al esquema de la tabla final
        if new_cols:
            print(f"🆕 Columnas nuevas detectadas: {sorted(new_cols)}. Actualizando esquema de tabla final...")
            # Obtener esquema completo de staging (sin campos ETL)
            new_schema_fields = [col for col in staging_schema if col.name in new_cols]
            final_table = bq_client.get_table(table_ref_final)
            
            # Separar campos ETL del resto del esquema para mantenerlos al final
            etl_fields = [col for col in final_table.schema if col.name.startswith('_etl_')]
            non_etl_fields = [col for col in final_table.schema if not col.name.startswith('_etl_')]
            
            # Reconstruir esquema: campos normales + nuevas columnas + campos ETL
            final_table.schema = non_etl_fields + new_schema_fields + etl_fields
            bq_client.update_table(final_table, ['schema'])
            print(f"✅ Esquema actualizado. Columnas agregadas: {sorted(new_cols)} (campos ETL mantenidos al final)")
        
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
                    # Reemplazar el campo en el esquema final
                    updated_schema = []
                    for field in final_table.schema:
                        if field.name == problematic_struct_field:
                            updated_schema.append(new_struct_field)
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
        event_message="Iniciando proceso ETL de ServiceTitan para compañía INBOX"
    )
    
    # INBOX (multi-compañía): itera todas las compañías activas en pph-inbox.settings.companies
    print("Conectando a BigQuery para obtener compañías INBOX...")
    client = bigquery.Client(project=PROJECT_ID_FOR_QUERY)
    query = f'''
        SELECT * FROM `{PROJECT_ID_FOR_QUERY}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_bigquery_status = TRUE
          AND company_project_id IS NOT NULL
        ORDER BY company_id
    '''
    results = list(client.query(query).result())
    
    if not results:
        log_event_bq(
            event_type="ERROR",
            event_title="No se encontró compañía INBOX",
            event_message="No se encontró ninguna compañía INBOX activa en la tabla companies (con company_project_id)."
        )
        print("❌ No se encontró ninguna compañía INBOX activa en la tabla companies (con company_project_id).")
        return
    
    total = len(results)
    print(f"📊 Se encontraron {total} compañía(s) INBOX activa(s) para procesar\n")

    failed = 0
    for idx, row in enumerate(results, 1):
        try:
            print(f"📊 Procesando compañía {idx} de {total}")
            process_company(row)
        except Exception as e:
            failed += 1
            log_event_bq(
                company_id=row.company_id,
                company_name=row.company_name,
                project_id=row.company_project_id,
                event_type="ERROR",
                event_title="Error procesando compañía",
                event_message=f"Error procesando compañía INBOX {row.company_name} (ID: {row.company_id}): {str(e)}"
            )
            print(f"❌ Error procesando compañía INBOX {row.company_name} (ID: {row.company_id}): {str(e)}")

        print(f"\n{'='*80}")

    # Log de fin del proceso ETL
    log_event_bq(
        event_type="SUCCESS" if failed == 0 else "WARNING",
        event_title="Fin proceso ETL",
        event_message=f"Proceso ETL INBOX finalizado. Total: {total}, fallidas: {failed}."
    )

    print(f"🏁 Procesamiento INBOX completado. Total: {total}, fallidas: {failed}.")

if __name__ == "__main__":
    main()