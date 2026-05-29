"""
ETL Job: CSV (Cloud Storage) → BigQuery
=========================================
Sube archivos CSV desde el bucket {project_id}_gmail2bq hacia el dataset 'reports' de BigQuery.

Uso en Cloud Shell / Local (modo TEST):
    python main.py --mode test --company-id 1 --endpoint technician_timesheet_summary
    python main.py --mode test  # Procesa todas las compañías
"""

import argparse
import os
import time
import glob
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

# Importar funciones comunes
from servicetitan_common import (
    get_project_source,
    get_bigquery_project_id,
    log_event_bq,
    get_balanced_tasks,
    read_schema_from_layout,
    load_csv_to_bq
)

# Proyecto central (ALL / TEST): se lee dinámicamente
PROJECT_ALL = get_bigquery_project_id()

# Tablas maestras
DATASET_COMPANIES = "settings"
TABLE_COMPANIES = "companies"

# Dataset destino
DATASET_FINAL = "reports"

def _make_log_callback(source: str):
    def callback(*args, **kwargs):
        kwargs.setdefault("source", source)
        return log_event_bq(*args, **kwargs)
    return callback


def get_available_endpoints():
    """
    Busca los archivos layout_*.txt en el directorio actual
    para determinar qué tablas (endpoints) se deben procesar.
    Retorna una lista de nombres de tabla.
    """
    layouts = glob.glob("layout_*.txt")
    endpoints = []
    for layout in layouts:
        # layout_technician_timesheet_summary.txt -> technician_timesheet_summary
        name = layout.replace("layout_", "").replace(".txt", "")
        endpoints.append(name)
    return endpoints


def process_company(row, endpoints_override=None, dry_run=False, log_callback=None):
    company_id    = row.company_id
    company_name  = row.company_name
    project_id    = row.company_project_id
    company_start = time.time()

    print(f"\n{'='*80}")
    print(f"🏢 Procesando: {company_name} (ID: {company_id}) | proyecto: {project_id}")
    if dry_run:
        print("🔍 MODO DRY-RUN: Solo mostrando acciones, sin ejecutar.")
    print(f"{'='*80}")

    if not project_id:
        raise ValueError(f"company_project_id vacío para company_id={company_id}.")

    bucket_name     = f"{project_id}_gmail2bq"
    storage_client  = storage.Client(project=project_id)
    try:
        bucket = storage_client.get_bucket(bucket_name)
    except NotFound:
        print(f"⚠️  Bucket no encontrado: {bucket_name}")
        return False, 0, f"Bucket no encontrado: {bucket_name}"

    all_endpoints = get_available_endpoints()

    if endpoints_override:
        endpoints_to_process = [ep for ep in all_endpoints if ep in endpoints_override]
        if not endpoints_to_process:
            print(f"⚠️  No se encontraron layouts para: {endpoints_override}")
            return False, 0, "No se encontraron layouts"
    else:
        endpoints_to_process = all_endpoints

    endpoints_count = 0

    for table_name in endpoints_to_process:
        endpoints_count += 1
        ep_start       = time.time()
        
        # Asumimos que el nombre del CSV coincide con el nombre de la tabla
        # Se pueden probar ambos formatos
        csv_filename_1 = f"{table_name}.csv"
        csv_filename_2 = f"servicetitan_{table_name}.csv"
        
        blob = bucket.blob(csv_filename_1)
        csv_filename = csv_filename_1
        
        if not blob.exists():
            blob = bucket.blob(csv_filename_2)
            if blob.exists():
                csv_filename = csv_filename_2
            else:
                print(f"⚠️  Archivos {csv_filename_1} o {csv_filename_2} no encontrados en {bucket_name}")
                if log_callback:
                    log_callback(
                        company_id=company_id, company_name=company_name,
                        project_id=project_id, endpoint=table_name,
                        event_type="WARNING", event_title="Archivo no encontrado",
                        event_message=f"Ni {csv_filename_1} ni {csv_filename_2} encontrados en {bucket_name}."
                    )
                continue

        temp_csv = f"/tmp/{project_id}_{table_name}.csv"
        layout_file = f"layout_{table_name}.txt"

        print(f"\n📦 ENDPOINT: {table_name} | company {company_id}")

        if dry_run:
            print(f"   📋 [DRY-RUN] Descargaría: gs://{bucket_name}/{csv_filename}")
            print(f"   📋 [DRY-RUN] Cargaría esquema de {layout_file}")
            print(f"   📋 [DRY-RUN] APPEND a: {project_id}.{DATASET_FINAL}.{table_name}")
            continue

        try:
            dl_start = time.time()
            blob.download_to_filename(temp_csv)
            dl_time = time.time() - dl_start
            file_size_mb = os.path.getsize(temp_csv) / (1024 * 1024)
            print(f"⬇️  Descargado {csv_filename} ({file_size_mb:.2f} MB) en {dl_time:.1f}s")
            
            print("🔍 Cargando esquema del archivo layout...")
            schema = read_schema_from_layout(layout_file)
            
        except Exception as e:
            print(f"❌ Error preparando archivo {csv_filename}: {e}")
            continue

        bq_client = bigquery.Client(project=project_id)
        
        # Asegurar que existe el dataset reports
        try:
            bq_client.get_dataset(f"{project_id}.{DATASET_FINAL}")
        except NotFound:
            ds = bigquery.Dataset(f"{project_id}.{DATASET_FINAL}")
            ds.location = "US"
            bq_client.create_dataset(ds)
            print(f"🆕 Dataset {DATASET_FINAL} creado en {project_id}")

        table_ref_final = bq_client.dataset(DATASET_FINAL).table(table_name)
        
        # Para evitar duplicados en pruebas repetidas, si es DEV y el file es el mismo.
        # Por ahora asumimos WRITE_APPEND, dado que bajan toda la info del año y luego diario.
        # O WRITE_TRUNCATE? Dejamos WRITE_APPEND (is_append=True).
        load_start = time.time()
        success, load_time, error_msg = load_csv_to_bq(
            bq_client=bq_client,
            temp_csv_path=temp_csv,
            table_ref_final=table_ref_final,
            schema=schema,
            load_start=load_start,
            is_append=True 
        )

        if not success:
            print(f"❌ Error cargando a BigQuery: {error_msg}")
            if log_callback:
                log_callback(
                    company_id=company_id, company_name=company_name,
                    project_id=project_id, endpoint=table_name,
                    event_type="ERROR", event_title="Error cargando CSV",
                    event_message=f"Error cargando {csv_filename}: {error_msg}"
                )
        else:
            print(f"✅ Endpoint {table_name} completado en {time.time()-ep_start:.1f}s")

        try:
            os.remove(temp_csv)
        except Exception:
            pass

    company_elapsed = time.time() - company_start
    print(f"\n{'='*80}")
    print(
        f"✅ Compañía {company_name} (ID: {company_id}) | "
        f"{endpoints_count} endpoints procesados | {company_elapsed:.1f}s ({company_elapsed/60:.1f} min)"
    )
    print(f"{'='*80}")

    return True, endpoints_count, None


def run_all(args, log_callback):
    JOB_TIMEOUT_SECONDS = 2 * 3600
    start_time = time.time()

    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
    task_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))
    is_parallel = task_count > 1

    print("\nConectando a BigQuery (pph-central)...")
    client = bigquery.Client()
    query = f"""
        SELECT * FROM `{PROJECT_ALL}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
        WHERE company_bigquery_status = TRUE
        ORDER BY company_id
    """
    results = list(client.query(query).result())
    
    if is_parallel:
        results = get_balanced_tasks(client, results, task_count, task_index)

    procesadas = 0
    for row in results:
        elapsed = time.time() - start_time
        if elapsed >= JOB_TIMEOUT_SECONDS:
            print(f"⏱️  TIMEOUT alcanzado.")
            break
        
        try:
            process_company(row, log_callback=log_callback)
            procesadas += 1
        except Exception as e:
            print(f"❌ Error procesando {row.company_name}: {e}")

    print(f"🏁 Fin de corrida ALL. {procesadas} compañías procesadas.")


def run_test(args, log_callback):
    print(f"\n{'='*80}")
    print("🧪 MODO TEST: CSV → BigQuery")
    print(f"{'='*80}")

    client = bigquery.Client(project=PROJECT_ALL)

    if args.company_id:
        query = f"""
            SELECT * FROM `{PROJECT_ALL}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
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
    else:
        query = f"""
            SELECT * FROM `{PROJECT_ALL}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
            WHERE company_bigquery_status = TRUE
            ORDER BY company_id
        """
        results = list(client.query(query).result())

    if not results:
        print("❌ No se encontraron compañías.")
        return

    for idx, row in enumerate(results, 1):
        try:
            process_company(
                row,
                endpoints_override=args.endpoint,
                dry_run=args.dry_run,
                log_callback=log_callback,
            )
        except Exception as e:
            print(f"❌ Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="ETL: CSV (GCS) → BigQuery")
    parser.add_argument("--mode", type=str, choices=["all", "test"], default="test")
    parser.add_argument("--company-id", "-c", type=int, default=None)
    parser.add_argument("--endpoint", "-e", type=str, nargs="+", default=None)
    parser.add_argument("--dry-run", action="store_true", default=False)

    args = parser.parse_args()
    mode = args.mode or os.environ.get("ETL_MODE", "").lower() or "test"

    print(f"\n{'='*80}")
    print(f"🚀 ETL csv2bq | MODO: {mode.upper()}")
    print(f"{'='*80}\n")

    log_cb = _make_log_callback(f"etl_csv2bq_{mode}")

    if mode == "all":
        run_all(args, log_cb)
    elif mode == "test":
        run_test(args, log_cb)

if __name__ == "__main__":
    main()
