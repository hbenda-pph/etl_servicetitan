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
        print(f"⚠️ [detectar_cambios_reales] Error detectando cambios: {str(e)}")
        return 'UPDATE'  # Por defecto, asumir UPDATE    

def process_company(row):
    company_id = row.company_id
    company_name = row.company_name
    # Cada compañía INBOX tiene su propio proyecto temporal (Estrategia A)
    project_id = row.company_project_id
    if not project_id:
        raise ValueError(f"company_project_id vacío para company_id={company_id}. Primero crea el proyecto INBOX por compañía.")
    
    # Log de inicio de procesamiento de compañía [COMENTADO]
    # log_event_bq(
    #     company_id=company_id,
    #     company_name=company_name,
    #     project_id=project_id,
    #     event_type="INFO",
    #     event_title="Inicio procesamiento compañía",
    #     event_message=f"Iniciando procesamiento de {company_name} (ID: {company_id})"
    # )
    
    print(f"\n{'='*80}\n🏢 Procesando compañía INBOX: {company_name} (ID: {company_id}) | project_id: {project_id}")
    bucket_name = f"{project_id}_servicetitan"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    for endpoint_name, table_name in ENDPOINTS:
        endpoint_start_time = time.time()
        # Usar table_name directamente desde metadata para archivos JSON y tablas
        json_filename = f"servicetitan_{table_name}.json"
        temp_json = f"/tmp/{project_id}_{table_name}.json"
        temp_fixed = f"/tmp/fixed_{project_id}_{table_name}.json"
        
        # Log de inicio de procesamiento de endpoint [COMENTADO]
        # log_event_bq(
        #     company_id=company_id,
        #     company_name=company_name,
        #     project_id=project_id,
        #     endpoint=endpoint_name,
        #     event_type="INFO",
        #     event_title="Inicio procesamiento endpoint",
        #     event_message=f"Procesando endpoint {endpoint_name} (tabla: {table_name}) para {company_name}"
        # )
        
        # Descargar archivo JSON del bucket
        try:
            blob = bucket.blob(json_filename)
            if not blob.exists():
                # log_event_bq( [COMENTADO]
                #     company_id=company_id,
                #     company_name=company_name,
                #     project_id=project_id,
                #     endpoint=endpoint_name,
                #     event_type="WARNING",
                #     event_title="Archivo no encontrado",
                #     event_message=f"Archivo {json_filename} no encontrado en bucket {bucket_name}"
                # )
                print(f"⚠️ [process_company] Archivo no encontrado: {json_filename} en bucket {bucket_name}")
                continue
            blob.download_to_filename(temp_json)
            print(f"⬇️  Descargado {json_filename} de gs://{bucket_name}")
        except Exception as e:
            # log_event_bq( [COMENTADO]
            #     company_id=company_id,
            #     company_name=company_name,
            #     project_id=project_id,
            #     endpoint=endpoint_name,
            #     event_type="ERROR",
            #     event_title="Error descargando archivo",
            #     event_message=f"Error descargando {json_filename}: {str(e)}"
            # )
            print(f"❌ [process_company] Error descargando {json_filename}: {str(e)}")
            continue
        
        # Transformar a newline-delimited y snake_case (primera pasada)
        try:
            fix_json_format(temp_json, temp_fixed)
            print(f"🔄 Transformado a newline-delimited y snake_case: {temp_fixed}")
        except Exception as e:
            # log_event_bq( [COMENTADO]
            #     company_id=company_id,
            #     company_name=company_name,
            #     project_id=project_id,
            #     endpoint=endpoint_name,
            #     event_type="ERROR",
            #     event_title="Error transformando archivo",
            #     event_message=f"Error transformando {json_filename}: {str(e)}"
            # )
            print(f"❌ [process_company] Error transformando {json_filename}: {str(e)}")
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
        
        # Usar la función común de carga que contiene todas las correcciones heurísticas 
        # y generación de vistas previas de errores JSON para logs
        load_start = time.time()
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
            log_event_callback=None,
            company_id=company_id,
            company_name=company_name,
            endpoint_name=endpoint_name
        )
        
        if not success:
            print(f"❌ [process_company] Error cargando a tabla staging: {error_msg}")
            merge_time = 0.0
            print(f"❌ [process_company] MERGE con Soft Delete no ejecutado para bronze.{table_name}: error cargando a staging - {error_msg}")
            endpoint_time = time.time() - endpoint_start_time
            print(f"❌ [process_company] Endpoint {endpoint_name} completado con errores en {endpoint_time:.1f}s total")
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
            
            # log_event_bq( [COMENTADO]
            #     company_id=company_id,
            #     company_name=company_name,
            #     project_id=project_id,
            #     endpoint=endpoint_name,
            #     event_type="INFO",
            #     event_title="Tabla final creada",
            #     event_message=f"Tabla final {dataset_final}.{table_final} creada automáticamente con campos ETL"
            # )
        
        # MERGE/INSERT (usando función común ahora potenciada)
        merge_start = time.time()
        staging_table = bq_client.get_table(table_ref_staging)
        final_table = bq_client.get_table(table_ref_final)
        
        # Verificar y corregir incompatibilidades de esquema (tipos incompatibles)
        print(f"🔍 Verificando compatibilidad de esquemas entre staging y final...")
        needs_correction, corrections_made, alignment_error = align_schemas_before_merge(
            bq_client=bq_client,
            staging_table=staging_table,
            final_table=final_table,
            project_id=project_id,
            dataset_final=dataset_final,
            table_final=table_final
        )
        
        if alignment_error:
            print(f"❌ [process_company] Error alineando esquemas: {alignment_error}")
            
        if needs_correction:
            final_table = bq_client.get_table(table_ref_final)
            
        # Usar función común para ejecutar MERGE o INSERT con Schema Evolution supercargado
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
            log_event_callback=None, # INBOX usa logs comentados actualmente
            company_id=company_id,
            company_name=company_name,
            endpoint_name=endpoint_name
        )
        
        if merge_success:
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
            print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada.")
        else:
            print(f"❌ [process_company] Error en MERGE/INSERT o borrado de staging: {merge_error_msg} (la tabla staging NO se borra para depuración)")
        
        # Borrar archivos temporales
        try:
            os.remove(temp_json)
            os.remove(temp_fixed)
        except Exception:
            pass
    
    # Log de fin de procesamiento de compañía [COMENTADO]
    # log_event_bq(
    #     company_id=company_id,
    #     company_name=company_name,
    #     project_id=project_id,
    #     event_type="SUCCESS",
    #     event_title="Fin procesamiento compañía",
    #     event_message=f"Procesamiento completado para {company_name}"
    # )

def main():
    # Log de inicio del proceso ETL [COMENTADO]
    # log_event_bq(
    #     event_type="INFO",
    #     event_title="Inicio proceso ETL",
    #     event_message="Iniciando proceso ETL de ServiceTitan para compañía INBOX"
    # )
    
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
        # log_event_bq( [COMENTADO]
        #     event_type="ERROR",
        #     event_title="No se encontró compañía INBOX",
        #     event_message="No se encontró ninguna compañía INBOX activa en la tabla companies (con company_project_id)."
        # )
        print("❌ [main] No se encontró ninguna compañía INBOX activa en la tabla companies (con company_project_id).")
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
            # log_event_bq( [COMENTADO]
            #     company_id=row.company_id,
            #     company_name=row.company_name,
            #     project_id=row.company_project_id,
            #     event_type="ERROR",
            #     event_title="Error procesando compañía",
            #     event_message=f"Error procesando compañía INBOX {row.company_name} (ID: {row.company_id}): {str(e)}"
            # )
            print(f"❌ [main] Error procesando compañía INBOX {row.company_name} (ID: {row.company_id}): {str(e)}")

        print(f"\n{'='*80}")

    # Log de fin del proceso ETL [COMENTADO]
    # log_event_bq(
    #     event_type="SUCCESS" if failed == 0 else "WARNING",
    #     event_title="Fin proceso ETL",
    #     event_message=f"Proceso ETL INBOX finalizado. Total: {total}, fallidas: {failed}."
    # )

    print(f"🏁 Procesamiento INBOX completado. Total: {total}, fallidas: {failed}.")

if __name__ == "__main__":
    main()