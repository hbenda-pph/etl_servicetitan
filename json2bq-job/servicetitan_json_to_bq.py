"""
Script de prueba para procesar datos de ServiceTitan JSON a BigQuery.
Permite procesar una sola compañía y/o un solo endpoint para pruebas locales.

Uso:
    python servicetitan_json_to_bq.py --company-id 1
    python servicetitan_json_to_bq.py --company-id 1 --endpoint "gross-pay-items"
    python servicetitan_json_to_bq.py --company-id 1 --endpoint "gross-pay-items" --dry-run
"""

import argparse
import os
import time
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

# Importar funciones comunes
from servicetitan_common import (
    get_project_source,
    get_bigquery_project_id,
    load_endpoints_from_metadata,
    log_event_bq,
    fix_json_format,
    _schema_field_to_sql,
    LOGS_PROJECT,
    LOGS_DATASET,
    LOGS_TABLE
)

# Configuración
PROJECT_SOURCE = get_project_source()
PROJECT_ID_FOR_QUERY = get_bigquery_project_id()
DATASET_NAME = "settings"
TABLE_NAME = "companies"


def process_company(row, endpoints_filter=None, dry_run=False):
    """
    Procesa una compañía, opcionalmente filtrando por endpoint.
    
    Args:
        row: Row de BigQuery con datos de la compañía
        endpoints_filter: Lista de endpoints a procesar (None = todos)
        dry_run: Si True, solo muestra qué haría sin ejecutar
    """
    company_id = row.company_id
    company_name = row.company_name
    project_id = row.company_project_id
    company_start_time = time.time()
    
    print(f"\n{'='*80}")
    print(f"🏢 Procesando compañía: {company_name} (company_id: {company_id}) | project_id: {project_id}")
    if dry_run:
        print(f"🔍 MODO DRY-RUN: Solo mostrando qué se haría, sin ejecutar")
    print(f"{'='*80}")
    
    bucket_name = f"{project_id}_servicetitan"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    # Cargar endpoints
    all_endpoints = load_endpoints_from_metadata()
    
    # Filtrar endpoints si se especificó
    if endpoints_filter:
        # Buscar endpoints que coincidan (por endpoint_name o table_name)
        filtered_endpoints = []
        for endpoint_name, table_name in all_endpoints:
            if endpoint_name in endpoints_filter or table_name in endpoints_filter:
                filtered_endpoints.append((endpoint_name, table_name))
        
        if not filtered_endpoints:
            print(f"⚠️  No se encontraron endpoints que coincidan con: {endpoints_filter}")
            return
        
        endpoints_to_process = filtered_endpoints
        print(f"📋 Procesando {len(endpoints_to_process)} endpoint(s) filtrado(s)")
    else:
        endpoints_to_process = all_endpoints
        print(f"📋 Procesando todos los endpoints ({len(endpoints_to_process)})")
    
    for endpoint_name, table_name in endpoints_to_process:
        endpoint_start_time = time.time()
        json_filename = f"servicetitan_{table_name}.json"
        temp_json = f"/tmp/{project_id}_{table_name}.json"
        temp_fixed = f"/tmp/fixed_{project_id}_{table_name}.json"
        
        print(f"\n📦 ENDPOINT: {endpoint_name} (tabla: {table_name}) company {company_id}")
        
        if dry_run:
            print(f"  📋 [DRY-RUN] Se descargaría: gs://{bucket_name}/{json_filename}")
            print(f"  📋 [DRY-RUN] Se transformaría a newline-delimited y snake_case")
            print(f"  📋 [DRY-RUN] Se cargaría a: {project_id}.staging.{table_name}")
            print(f"  📋 [DRY-RUN] Se ejecutaría MERGE a: {project_id}.bronze.{table_name}")
            continue
        
        # Descargar archivo JSON del bucket
        try:
            download_start = time.time()
            blob = bucket.blob(json_filename)
            if not blob.exists():
                print(f"⚠️  Archivo no encontrado: {json_filename} en bucket {bucket_name}")
                log_event_bq(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="WARNING",
                    event_title="Archivo no encontrado",
                    event_message=f"Archivo {json_filename} no encontrado en bucket {bucket_name}",
                    source="servicetitan_json_to_bq"
                )
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
                event_message=f"Error descargando {json_filename}: {str(e)}",
                source="servicetitan_json_to_bq"
            )
            continue
        
        # Transformar a newline-delimited y snake_case
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
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error transformando archivo",
                event_message=f"Error transformando {json_filename}: {str(e)}",
                source="servicetitan_json_to_bq"
            )
            continue
        
        # Cargar a tabla staging en BigQuery
        load_start = time.time()
        bq_client = bigquery.Client(project=project_id)
        dataset_staging = "staging"
        dataset_final = "bronze"
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
        
        # Limpiar staging al inicio para evitar conflictos
        try:
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
        except Exception:
            pass
        
        # Cargar con autodetect
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        try:
            load_job = bq_client.load_table_from_file(
                open(temp_fixed, "rb"),
                table_ref_staging,
                job_config=job_config
            )
            load_job.result()
            load_time = time.time() - load_start
            print(f"✅ Cargado a tabla staging: {dataset_staging}.{table_staging} en {load_time:.1f}s")
        except Exception as e:
            print(f"❌ Error cargando a staging: {str(e)}")
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error cargando a staging",
                event_message=f"Error cargando a tabla staging: {str(e)}",
                source="servicetitan_json_to_bq"
            )
            continue
        
        # Asegurar que la tabla final existe
        try:
            bq_client.get_table(table_ref_final)
        except NotFound:
            schema = bq_client.get_table(table_ref_staging).schema
            campos_etl = [
                bigquery.SchemaField("_etl_synced", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_etl_operation", "STRING", mode="REQUIRED")
            ]
            schema_completo = list(schema) + campos_etl
            table = bigquery.Table(table_ref_final, schema=schema_completo)
            bq_client.create_table(table)
            print(f"🆕 Tabla final {dataset_final}.{table_final} creada con esquema ETL")
        
        # MERGE incremental (simplificado para pruebas)
        merge_start = time.time()
        staging_table = bq_client.get_table(table_ref_staging)
        final_table = bq_client.get_table(table_ref_final)
        staging_schema = staging_table.schema
        final_schema = final_table.schema
        
        staging_cols = {col.name for col in staging_schema if col.name != 'id' and not col.name.startswith('_etl_')}
        update_set = ', '.join([f'T.{col} = S.{col}' for col in sorted(staging_cols)])
        
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
            merge_time = time.time() - merge_start
            print(f"🔀 MERGE ejecutado: {dataset_final}.{table_final} actualizado en {merge_time:.1f}s")
            
            # Limpiar staging
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
            
            endpoint_time = time.time() - endpoint_start_time
            print(f"✅ Endpoint {endpoint_name} completado en {endpoint_time:.1f}s total")
        except Exception as e:
            print(f"❌ Error en MERGE: {str(e)}")
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error en MERGE",
                event_message=f"Error en MERGE: {str(e)}",
                source="servicetitan_json_to_bq"
            )
        
        # Borrar archivos temporales
        try:
            os.remove(temp_json)
            os.remove(temp_fixed)
        except Exception:
            pass
    
    company_elapsed = time.time() - company_start_time
    print(f"\n{'='*80}")
    print(f"✅ Compañía {company_name} completada en {company_elapsed:.1f} segundos ({company_elapsed/60:.1f} minutos)")
    print(f"{'='*80}")


def main():
    parser = argparse.ArgumentParser(
        description='Script de prueba para procesar datos de ServiceTitan JSON a BigQuery',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Procesar todos los endpoints de la compañía 1
  python servicetitan_json_to_bq.py --company-id 1
  
  # Procesar solo un endpoint específico
  python servicetitan_json_to_bq.py --company-id 1 --endpoint "gross-pay-items"
  
  # Modo dry-run (solo mostrar qué haría)
  python servicetitan_json_to_bq.py --company-id 1 --endpoint "gross-pay-items" --dry-run
        """
    )
    
    parser.add_argument(
        '--company-id',
        type=int,
        required=True,
        help='ID de la compañía a procesar'
    )
    
    parser.add_argument(
        '--endpoint',
        type=str,
        nargs='+',
        help='Endpoint(s) específico(s) a procesar (puede ser endpoint_name o table_name). Si no se especifica, procesa todos.'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Modo dry-run: solo muestra qué haría sin ejecutar'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*80}")
    print("🔍 Script de Prueba: ServiceTitan JSON → BigQuery")
    print(f"{'='*80}")
    print(f"📋 Compañía ID: {args.company_id}")
    if args.endpoint:
        print(f"📋 Endpoint(s): {', '.join(args.endpoint)}")
    else:
        print(f"📋 Endpoint(s): TODOS")
    print(f"📋 Modo: {'DRY-RUN' if args.dry_run else 'EJECUTAR'}")
    print(f"🔍 Proyecto detectado: {PROJECT_SOURCE}")
    print(f"🔍 Project ID para queries: {PROJECT_ID_FOR_QUERY}")
    print(f"{'='*80}\n")
    
    # Conectar a BigQuery y obtener la compañía
    print("Conectando a BigQuery para obtener compañía...")
    # Usar PROJECT_ID_FOR_QUERY explícitamente para evitar errores de detección automática
    client = bigquery.Client(project=PROJECT_ID_FOR_QUERY)
    query = f"""
        SELECT * FROM `{PROJECT_ID_FOR_QUERY}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_id = @company_id
        AND company_bigquery_status = TRUE
        LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company_id", "INT64", args.company_id)
        ]
    )
    
    results = list(client.query(query, job_config=job_config).result())
    
    if not results:
        print(f"❌ No se encontró compañía con ID {args.company_id} o no está activa (company_bigquery_status = TRUE)")
        return
    
    row = results[0]
    
    # Procesar compañía
    try:
        process_company(row, endpoints_filter=args.endpoint, dry_run=args.dry_run)
        print(f"\n{'='*80}")
        print(f"✅ Procesamiento completado")
        print(f"{'='*80}")
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ Error procesando compañía: {str(e)}")
        print(f"{'='*80}")
        raise


if __name__ == "__main__":
    main()
