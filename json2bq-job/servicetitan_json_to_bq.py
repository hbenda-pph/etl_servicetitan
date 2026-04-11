"""
Script de prueba para procesar datos de ServiceTitan JSON a BigQuery.
Permite procesar una sola compañía o todas las compañías activas, y/o un solo endpoint para pruebas locales.

Uso:
    python servicetitan_json_to_bq.py --company-id 1
    python servicetitan_json_to_bq.py --company-id 1 --endpoint "gross-pay-items"
    python servicetitan_json_to_bq.py --company-id 1 --endpoint "gross-pay-items" --dry-run
    python servicetitan_json_to_bq.py  # Procesa todas las compañías activas
    python servicetitan_json_to_bq.py --endpoint "gross-pay-items"  # Procesa todas las compañías con un endpoint específico
"""

import argparse
import os
import re
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
    load_json_to_staging_with_error_handling,
    validate_json_file,
    align_schemas_before_merge,
    execute_merge_or_insert
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
        for endpoint_name, table_name, use_merge, is_production in all_endpoints:
            if endpoint_name in endpoints_filter or table_name in endpoints_filter:
                filtered_endpoints.append((endpoint_name, table_name, use_merge, is_production))
        
        if not filtered_endpoints:
            print(f"⚠️ [process_company] No se encontraron endpoints que coincidan con: {endpoints_filter}")
            return
        
        endpoints_to_process = filtered_endpoints
        print(f"📋 Procesando {len(endpoints_to_process)} endpoint(s) filtrado(s)")
    else:
        endpoints_to_process = all_endpoints
        print(f"📋 Procesando todos los endpoints ({len(endpoints_to_process)})")
    
    for endpoint_name, table_name, use_merge, is_production in endpoints_to_process:
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
        
        if dry_run:
            print(f"  📋 [DRY-RUN] Se descargaría: gs://{bucket_name}/{json_filename}")
            print(f"  📋 [DRY-RUN] Se transformaría a newline-delimited y snake_case")
            print(f"  📋 [DRY-RUN] Se cargaría a: {project_id}.staging.{table_name}")
            print(f"  📋 [DRY-RUN] Se ejecutaría {merge_label} a: {project_id}.bronze.{table_name}")
            continue
        
        # Descargar archivo JSON del bucket
        try:
            download_start = time.time()
            blob = bucket.blob(json_filename)
            if not blob.exists():
                print(f"⚠️ [process_company] Archivo no encontrado: {json_filename} en bucket {bucket_name}")
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
            
            # Validar JSON inmediatamente después de descargar
            print(f"🔍 Validando estructura JSON...")
            is_valid, validation_error, json_type = validate_json_file(temp_json)
            if not is_valid:
                print(f"❌ [process_company] ARCHIVO JSON MAL FORMADO: {validation_error}")
                print(f"❌ [process_company] El archivo {json_filename} está corrupto o mal generado por el job anterior (st2json)")
                log_event_bq(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Archivo JSON mal formado",
                    event_message=f"Archivo {json_filename} está mal formado: {validation_error}. Revisar job st2json.",
                    source="servicetitan_json_to_bq"
                )
                continue
            print(f"✅ JSON válido (tipo: {json_type})")
        except Exception as e:
            print(f"❌ [process_company] Error descargando {json_filename}: {str(e)}")
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
        
        # Usar función común para cargar con manejo automático de errores
        def log_callback(**kwargs):
            """Wrapper para log_event_bq con source correcto"""
            kwargs.setdefault('source', 'servicetitan_json_to_bq')
            log_event_bq(**kwargs)
        
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
            log_event_callback=log_callback,
            company_id=company_id,
            company_name=company_name,
            endpoint_name=endpoint_name
        )
        
        if not success:
            print(f"❌ [process_company] Error cargando a staging: {error_msg}")
            continue
        
        if load_time:
            print(f"✅ Cargado a tabla staging: {dataset_staging}.{table_staging} en {load_time:.1f}s")
        
        # MERGE/INSERT (usando función común)
        merge_start = time.time()
        staging_table = bq_client.get_table(table_ref_staging)
        
        # SOLO aseguramos que exista y verificamos esquemas si vamos a hacer MERGE
        final_table = None
        type_mismatches = None
        
        if is_production and use_merge:
            # Asegurar que la tabla final existe antes del MERGE
            try:
                final_table = bq_client.get_table(table_ref_final)
            except NotFound:
                schema = staging_table.schema
                campos_etl = [
                    bigquery.SchemaField("_etl_synced", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("_etl_operation", "STRING", mode="REQUIRED")
                ]
                schema_completo = list(schema) + campos_etl
                table = bigquery.Table(table_ref_final, schema=schema_completo)
                final_table = bq_client.create_table(table)
                print(f"🆕 Tabla final {dataset_final}.{table_final} creada con esquema ETL")
                
            # Verificar y corregir incompatibilidades de esquema ANTES del MERGE/INSERT
            print(f"🔍 Verificando compatibilidad de esquemas entre staging y final (Modo MERGE)...")
            needs_correction, corrections_made, alignment_error, type_mismatches = align_schemas_before_merge(
                bq_client=bq_client,
                staging_table=staging_table,
                final_table=final_table,
                project_id=project_id,
                dataset_final=dataset_final,
                table_final=table_final
            )
            
            if alignment_error:
                print(f"❌ [process_company] Error alineando esquemas: {alignment_error}")
                log_event_bq(
                    company_id=company_id,
                    company_name=company_name,
                    project_id=project_id,
                    endpoint=endpoint_name,
                    event_type="ERROR",
                    event_title="Error alineando esquemas",
                    event_message=f"Error alineando esquemas antes del MERGE/INSERT: {alignment_error}",
                    source="servicetitan_json_to_bq"
                )
                
            if needs_correction:
                # Refrescar tabla final después de correcciones
                final_table = bq_client.get_table(table_ref_final)
                
        else:
            reason = "DEV Mode" if not is_production else "OVERWRITE Mode"
            print(f"⏭️  Saltando verificación de esquemas ({reason}) -> se creará/reemplazará la tabla completa")
        
        # Usar función común para ejecutar MERGE, INSERT o OVERWRITE
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
            log_event_callback=log_event_bq,
            company_id=company_id,
            company_name=company_name,
            endpoint_name=endpoint_name,
            type_mismatches=type_mismatches,
            use_merge=use_merge,
            temp_fixed=temp_fixed,
            is_production=is_production
        )
        
        if merge_success:
            # MERGE/INSERT exitoso
            # Limpiar staging
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
            
            endpoint_time = time.time() - endpoint_start_time
            print(f"✅ Endpoint {endpoint_name} completado en {endpoint_time:.1f}s total")
        else:
            # MERGE/INSERT falló
            print(f"❌ [process_company] Error en MERGE/INSERT: {merge_error_msg}")
            log_event_bq(
                company_id=company_id,
                company_name=company_name,
                project_id=project_id,
                endpoint=endpoint_name,
                event_type="ERROR",
                event_title="Error en MERGE/INSERT",
                event_message=f"Error en MERGE/INSERT: {merge_error_msg}",
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
  
  # Procesar solo un endpoint específico de la compañía 1
  python servicetitan_json_to_bq.py --company-id 1 --endpoint "gross-pay-items"
  
  # Procesar todas las compañías activas
  python servicetitan_json_to_bq.py
  
  # Procesar todas las compañías activas con un endpoint específico
  python servicetitan_json_to_bq.py --endpoint "gross-pay-items"
  
  # Modo dry-run (solo mostrar qué haría)
  python servicetitan_json_to_bq.py --company-id 1 --endpoint "gross-pay-items" --dry-run
        """
    )
    
    parser.add_argument(
        '--company-id',
        type=int,
        required=False,
        help='ID de la compañía a procesar. Si no se especifica, procesa todas las compañías activas.'
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
    print("Script de Prueba: ServiceTitan JSON -> BigQuery")
    print(f"{'='*80}")
    if args.company_id:
        print(f"📋 Compañía ID: {args.company_id}")
    else:
        print(f"📋 Compañía ID: TODAS (compañías activas)")
    if args.endpoint:
        print(f"📋 Endpoint(s): {', '.join(args.endpoint)}")
    else:
        print(f"📋 Endpoint(s): TODOS")
    print(f"📋 Modo: {'DRY-RUN' if args.dry_run else 'EJECUTAR'}")
    print(f"🔍 Proyecto detectado: {PROJECT_SOURCE}")
    print(f"🔍 Project ID para queries: {PROJECT_ID_FOR_QUERY}")
    print(f"{'='*80}\n")
    
    # Conectar a BigQuery y obtener la(s) compañía(s)
    print("Conectando a BigQuery para obtener compañía(s)...")
    # Usar PROJECT_ID_FOR_QUERY explícitamente para evitar errores de detección automática
    client = bigquery.Client(project=PROJECT_ID_FOR_QUERY)
    
    if args.company_id:
        # Procesar solo una compañía específica
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
            print(f"❌ [main] No se encontró compañía con ID {args.company_id} o no está activa (company_bigquery_status = TRUE)")
            return
        
        companies = results
    else:
        # Procesar todas las compañías activas
        query = f"""
            SELECT * FROM `{PROJECT_ID_FOR_QUERY}.{DATASET_NAME}.{TABLE_NAME}`
            WHERE company_bigquery_status = TRUE
            ORDER BY company_id
        """
        
        results = list(client.query(query).result())
        
        if not results:
            print(f"❌ [main] No se encontraron compañías activas (company_bigquery_status = TRUE)")
            return
        
        companies = results
        print(f"📊 Se encontraron {len(companies)} compañía(s) activa(s) para procesar\n")
    
    # Procesar cada compañía
    total_companies = len(companies)
    processed_count = 0
    failed_count = 0
    
    for idx, row in enumerate(companies, 1):
        try:
            print(f"\n{'#'*80}")
            print(f"📊 Procesando compañía {idx} de {total_companies}")
            print(f"{'#'*80}")
            process_company(row, endpoints_filter=args.endpoint, dry_run=args.dry_run)
            processed_count += 1
        except Exception as e:
            failed_count += 1
            print(f"\n{'='*80}")
            print(f"❌ [main] Error procesando compañía {row.company_id} ({row.company_name}): {str(e)}")
            print(f"{'='*80}")
            # Continuar con la siguiente compañía en lugar de detener todo el proceso
            continue
    
    # Resumen final
    print(f"\n{'='*80}")
    print(f"✅ Procesamiento completado")
    print(f"📊 Total de compañías: {total_companies}")
    print(f"✅ Procesadas exitosamente: {processed_count}")
    if failed_count > 0:
        print(f"❌ [main] Fallidas: {failed_count}")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
