"""
ETL Job: JSON (Cloud Storage) → BigQuery
=========================================
Script unificado que reemplaza los tres scripts anteriores:
  - servicetitan_all_json_to_bq.py   → modo --mode all
  - servicetitan_inbox_json_to_bq.py → modo --mode inbox
  - servicetitan_json_to_bq.py       → modo --mode test

Uso en Cloud Shell / Local (modo TEST):
    python main.py --mode test --company-id 1 --endpoint locations
    python main.py --mode test --company-id 1 --endpoint locations --dry-run
    python main.py --mode test  # Procesa todas las compañías, todos los endpoints

El modo también puede establecerse con la variable de entorno ETL_MODE,
que es la forma en que lo inyectan los scripts build_deploy.sh:
    ETL_MODE=all   → build_deploy.sh
    ETL_MODE=inbox → build_deploy_inbox.sh
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
    load_json_to_staging_with_error_handling,
    validate_json_file,
    align_schemas_before_merge,
    execute_merge_or_insert,
    get_balanced_tasks,
)

# =============================================================================
# CONFIGURACIÓN DE PROYECTOS Y TABLAS MAESTRAS
# =============================================================================

# Proyecto central (ALL / TEST): se lee dinámicamente del service account activo
PROJECT_ALL = get_bigquery_project_id()

# Proyecto INBOX: siempre fijo, es su propio proyecto GCP
PROJECT_INBOX = "pph-inbox"

# Tablas maestras (iguales para ambos modos)
DATASET_COMPANIES = "settings"
TABLE_COMPANIES = "companies"

# =============================================================================
# HELPERS DE LOGGING
# =============================================================================

def _make_log_callback(source: str):
    """Devuelve un wrapper de log_event_bq con el 'source' fijado."""
    def callback(*args, **kwargs):
        kwargs.setdefault("source", source)
        return log_event_bq(*args, **kwargs)
    return callback


# =============================================================================
# NÚCLEO: process_company()
# =============================================================================

def process_company(row, endpoints_override=None, dry_run=False, log_callback=None):
    """
    Procesa todos los endpoints de una compañía: descarga JSON desde GCS,
    lo transforma a NDJSON/snake_case, lo carga a staging y hace MERGE a bronze.

    Args:
        row            : Fila de BigQuery con datos de la compañía.
        endpoints_override : Lista de endpoint names/table names a procesar.
                             None = procesa todos los endpoints de metadata.
        dry_run        : Si True, solo muestra qué haría sin ejecutar nada.
        log_callback   : Función de log hacia BigQuery. None = sin logs a BQ.
    """
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
        raise ValueError(
            f"company_project_id vacío para company_id={company_id}. "
            "Verifica la tabla settings.companies."
        )

    bucket_name     = f"{project_id}_servicetitan"
    storage_client  = storage.Client(project=project_id)
    bucket          = storage_client.bucket(bucket_name)

    # ── Cargar endpoints ──────────────────────────────────────────────────────
    all_endpoints = load_endpoints_from_metadata()

    if endpoints_override:
        endpoints_to_process = [
            (ep_name, tbl_name, use_merge, is_prod)
            for ep_name, tbl_name, use_merge, is_prod in all_endpoints
            if ep_name in endpoints_override or tbl_name in endpoints_override
        ]
        if not endpoints_to_process:
            print(f"⚠️  No se encontraron endpoints que coincidan con: {endpoints_override}")
            return
        print(f"📋 Endpoints filtrados: {len(endpoints_to_process)}")
    else:
        endpoints_to_process = all_endpoints
        print(f"📋 Total de endpoints: {len(endpoints_to_process)}")

    endpoints_count = 0

    for endpoint_name, table_name, use_merge, is_production in endpoints_to_process:
        endpoints_count += 1
        ep_start       = time.time()
        json_filename  = f"servicetitan_{table_name}.json"
        temp_json      = f"/tmp/{project_id}_{table_name}.json"
        temp_fixed     = f"/tmp/fixed_{project_id}_{table_name}.json"

        print(f"\n📦 ENDPOINT: {endpoint_name} (tabla: {table_name}) | company {company_id}")

        # ── Dry-run ───────────────────────────────────────────────────────────
        if dry_run:
            print(f"   📋 [DRY-RUN] Descargaría: gs://{bucket_name}/{json_filename}")
            print(f"   📋 [DRY-RUN] Transformaría a NDJSON / snake_case")
            print(f"   📋 [DRY-RUN] Cargaría a: {project_id}.staging.{table_name}")
            print(f"   📋 [DRY-RUN] MERGE a: {project_id}.bronze.{table_name}")
            continue

        # ── 1. Descargar JSON ─────────────────────────────────────────────────
        try:
            dl_start = time.time()
            blob = bucket.blob(json_filename)
            if not blob.exists():
                print(f"⚠️  Archivo no encontrado: {json_filename} en {bucket_name}")
                if log_callback:
                    log_callback(
                        company_id=company_id, company_name=company_name,
                        project_id=project_id, endpoint=endpoint_name,
                        event_type="WARNING", event_title="Archivo no encontrado",
                        event_message=f"{json_filename} no encontrado en {bucket_name}."
                    )
                print(f"❌ MERGE no ejecutado para bronze.{table_name}: archivo faltante")
                print(f"❌ Endpoint {endpoint_name} completado con errores en {time.time()-ep_start:.1f}s")
                continue

            blob.download_to_filename(temp_json)
            dl_time      = time.time() - dl_start
            file_size_mb = os.path.getsize(temp_json) / (1024 * 1024)
            print(f"⬇️  Descargado {json_filename} ({file_size_mb:.2f} MB) en {dl_time:.1f}s")

            # Validar JSON
            print("🔍 Validando estructura JSON...")
            is_valid, validation_error, json_type = validate_json_file(temp_json)
            if not is_valid:
                print(f"❌ JSON MAL FORMADO: {validation_error}")
                if log_callback:
                    log_callback(
                        company_id=company_id, company_name=company_name,
                        project_id=project_id, endpoint=endpoint_name,
                        event_type="ERROR", event_title="JSON mal formado",
                        event_message=f"{json_filename} mal formado: {validation_error}"
                    )
                print(f"❌ Endpoint {endpoint_name} completado con errores en {time.time()-ep_start:.1f}s")
                continue
            print(f"✅ JSON válido (tipo: {json_type})")

        except Exception as e:
            print(f"❌ Error descargando {json_filename}: {e}")
            if log_callback:
                log_callback(
                    company_id=company_id, company_name=company_name,
                    project_id=project_id, endpoint=endpoint_name,
                    event_type="ERROR", event_title="Error descargando archivo",
                    event_message=f"Error descargando {json_filename}: {e}"
                )
            print(f"❌ Endpoint {endpoint_name} completado con errores en {time.time()-ep_start:.1f}s")
            continue

        # ── 2. Transformar a NDJSON / snake_case ─────────────────────────────
        try:
            tr_start = time.time()
            if file_size_mb > 100:
                print(f"🔄 Transformando archivo grande ({file_size_mb:.2f} MB)... (puede tardar)")
            fix_json_format(temp_json, temp_fixed)
            tr_time = time.time() - tr_start
            if file_size_mb <= 100:
                print(f"🔄 Transformado a NDJSON/snake_case en {tr_time:.1f}s")
        except Exception as e:
            print(f"❌ Error transformando {json_filename}: {e}")
            if log_callback:
                log_callback(
                    company_id=company_id, company_name=company_name,
                    project_id=project_id, endpoint=endpoint_name,
                    event_type="ERROR", event_title="Error transformando archivo",
                    event_message=f"Error transformando {json_filename}: {e}"
                )
            print(f"❌ Endpoint {endpoint_name} completado con errores en {time.time()-ep_start:.1f}s")
            continue

        # ── 3. Cargar a staging en BigQuery ───────────────────────────────────
        bq_client        = bigquery.Client(project=project_id)
        dataset_staging  = "staging"
        dataset_final    = "bronze"
        table_staging    = table_name
        table_final      = table_name
        table_ref_staging = bq_client.dataset(dataset_staging).table(table_staging)
        table_ref_final   = bq_client.dataset(dataset_final).table(table_final)

        # Asegurar que existe el dataset staging
        try:
            bq_client.get_dataset(f"{project_id}.{dataset_staging}")
        except NotFound:
            ds = bigquery.Dataset(f"{project_id}.{dataset_staging}")
            ds.location = "US"
            bq_client.create_dataset(ds)
            print(f"🆕 Dataset {dataset_staging} creado en {project_id}")

        # Limpiar staging previo para evitar conflictos de esquema
        try:
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
        except Exception:
            pass

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
            log_event_callback=log_callback,
            company_id=company_id,
            company_name=company_name,
            endpoint_name=endpoint_name,
        )

        if not success:
            print(f"❌ Error cargando a staging: {error_msg}")
            print(f"❌ MERGE no ejecutado para bronze.{table_name}: error en staging")
            print(f"❌ Endpoint {endpoint_name} completado con errores en {time.time()-ep_start:.1f}s")
            continue

        # ── 4. Asegurar tabla final y hacer MERGE ─────────────────────────────
        try:
            bq_client.get_table(table_ref_final)
        except NotFound:
            schema = bq_client.get_table(table_ref_staging).schema
            campos_etl = [
                bigquery.SchemaField("_etl_synced",    "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_etl_operation", "STRING",    mode="REQUIRED"),
            ]
            table = bigquery.Table(table_ref_final, schema=list(schema) + campos_etl)
            bq_client.create_table(table)
            print(f"🆕 Tabla final {dataset_final}.{table_final} creada con campos ETL.")
            if log_callback:
                log_callback(
                    company_id=company_id, company_name=company_name,
                    project_id=project_id, endpoint=endpoint_name,
                    event_type="INFO", event_title="Tabla final creada",
                    event_message=f"Tabla {dataset_final}.{table_final} creada automáticamente."
                )

        merge_start   = time.time()
        staging_table = bq_client.get_table(table_ref_staging)
        final_table   = bq_client.get_table(table_ref_final)

        print("🔍 Verificando compatibilidad de esquemas staging vs final...")
        needs_correction, corrections_made, alignment_error, type_mismatches = align_schemas_before_merge(
            bq_client=bq_client,
            staging_table=staging_table,
            final_table=final_table,
            project_id=project_id,
            dataset_final=dataset_final,
            table_final=table_final,
        )

        if alignment_error:
            print(f"❌ Error alineando esquemas: {alignment_error}")
            if log_callback:
                log_callback(
                    company_id=company_id, company_name=company_name,
                    project_id=project_id, endpoint=endpoint_name,
                    event_type="ERROR", event_title="Error alineando esquemas",
                    event_message=f"Error antes del MERGE: {alignment_error}"
                )

        if needs_correction:
            final_table = bq_client.get_table(table_ref_final)

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
            log_event_callback=log_callback,
            company_id=company_id,
            company_name=company_name,
            endpoint_name=endpoint_name,
            type_mismatches=type_mismatches,
            use_merge=use_merge,
            is_production=is_production,
        )

        if merge_success:
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
            print(f"✅ Endpoint {endpoint_name} completado en {time.time()-ep_start:.1f}s")
        else:
            print(
                f"❌ Error en MERGE/INSERT: {merge_error_msg} "
                "(staging NO se borra para depuración)"
            )
            print(f"❌ Endpoint {endpoint_name} completado con errores en {time.time()-ep_start:.1f}s")

        # Limpiar temporales
        for tmpf in (temp_json, temp_fixed):
            try:
                os.remove(tmpf)
            except Exception:
                pass

    # Resumen por compañía
    company_elapsed = time.time() - company_start
    print(f"\n{'='*80}")
    print(
        f"✅ Compañía {company_name} (ID: {company_id}) | "
        f"{endpoints_count} endpoints | {company_elapsed:.1f}s ({company_elapsed/60:.1f} min)"
    )
    print(f"{'='*80}")

    return True, endpoints_count, None


# =============================================================================
# MODO ALL — PRO / QUA / DEV  (pph-central)
# =============================================================================

def run_all(args, log_callback):
    """
    Procesa todas las compañías del consorcio desde pph-central.
    Soporta paralelismo de Cloud Run Tasks.
    """
    JOB_TIMEOUT_SECONDS = 2 * 3600  # 2 horas (según proceso_etl.csv)
    start_time = time.time()

    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
    task_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))
    is_parallel = task_count > 1

    if is_parallel:
        print(f"\n{'='*80}")
        print(f"🚀 MODO PARALELO | Tarea {task_index + 1}/{task_count}")
        print(f"{'='*80}")

    log_callback(
        event_type="INFO",
        event_title="Inicio ETL ALL",
        event_message=(
            f"Iniciando ETL ALL para todas las compañías activas"
            + (f" (Tarea {task_index+1}/{task_count})" if is_parallel else "")
        ),
    )

    print("\nConectando a BigQuery (pph-central)...")
    print(f"⏱️  Timeout del job: {JOB_TIMEOUT_SECONDS // 60} minutos")

    client = bigquery.Client()  # Usa el project del service account
    query = f"""
        SELECT * FROM `{PROJECT_ALL}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
        WHERE company_bigquery_status = TRUE
        ORDER BY company_id
    """
    results = list(client.query(query).result())
    total   = len(results)

    # Distribución de carga para modo paralelo
    if is_parallel:
        results = get_balanced_tasks(client, results, task_count, task_index)
        total_assigned = len(results)
    else:
        total_assigned = total
        print(f"📊 Total compañías a procesar: {total}")

    print(f"{'='*80}\n")

    procesadas = 0
    for idx, row in enumerate(results, 1):
        elapsed   = time.time() - start_time
        remaining = JOB_TIMEOUT_SECONDS - elapsed

        if remaining < 300:
            print(f"⚠️  Quedan {remaining//60:.1f} min antes del timeout")
            log_callback(
                event_type="WARNING",
                event_title="Advertencia timeout",
                event_message=f"Quedan {remaining//60:.1f} min. Compañía {idx}/{total_assigned}: {row.company_name}",
            )

        if elapsed >= JOB_TIMEOUT_SECONDS:
            elapsed_min = elapsed / 60
            log_callback(
                event_type="ERROR",
                event_title="Timeout del job",
                event_message=(
                    f"Job interrumpido por timeout tras {elapsed_min:.1f} min. "
                    f"Procesadas {procesadas}/{total_assigned}."
                ),
            )
            print(f"\n{'='*80}")
            print(f"⏱️  TIMEOUT: Job interrumpido después de {elapsed_min:.1f} minutos")
            print(f"📊 Progreso: {procesadas}/{total_assigned} compañías")
            print(f"{'='*80}")
            raise TimeoutError(
                f"Job timeout tras {elapsed_min:.1f} min. "
                f"Procesadas {procesadas}/{total_assigned}."
            )

        elapsed_min = elapsed / 60
        print(
            f"\n📊 Progreso: {idx}/{total_assigned} | "
            f"Tiempo: {elapsed_min:.1f} min | Restante: {remaining//60:.1f} min"
        )

        try:
            process_company(row, log_callback=log_callback)
            procesadas += 1
        except TimeoutError:
            raise
        except Exception as e:
            log_callback(
                company_id=row.company_id,
                company_name=row.company_name,
                project_id=row.company_project_id,
                event_type="ERROR",
                event_title="Error procesando compañía",
                event_message=f"Error en {row.company_name}: {e}",
            )
            print(f"❌ Error procesando {row.company_name}: {e}")

    total_min  = (time.time() - start_time) / 60
    task_info  = f" (Tarea {task_index+1}/{task_count})" if is_parallel else ""
    log_callback(
        event_type="SUCCESS",
        event_title="Fin ETL ALL",
        event_message=(
            f"ETL ALL completado{task_info}. "
            f"{procesadas}/{total_assigned} compañías en {total_min:.1f} min."
        ),
    )
    print(f"\n{'='*80}")
    print(f"🏁 Resumen{task_info}: {procesadas}/{total_assigned} compañías en {total_min:.1f} min.")
    print(f"{'='*80}")


# =============================================================================
# MODO INBOX — pph-inbox
# =============================================================================

def run_inbox(args):
    """
    Procesa las compañías candidatas (INBOX) desde pph-inbox.settings.companies.
    No usa paralelismo (volumen reducido ~5 compañías).
    No escribe logs a BigQuery (comportamiento heredado del script original).
    """
    print("\nConectando a BigQuery (pph-inbox)...")
    client = bigquery.Client(project=PROJECT_INBOX)
    query = f"""
        SELECT * FROM `{PROJECT_INBOX}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
        WHERE company_bigquery_status = TRUE
          AND company_project_id IS NOT NULL
        ORDER BY company_id
    """
    results = list(client.query(query).result())

    if not results:
        print("❌ No se encontraron compañías INBOX activas (company_bigquery_status=TRUE).")
        return

    total  = len(results)
    failed = 0
    print(f"📊 Compañías INBOX activas: {total}\n")

    for idx, row in enumerate(results, 1):
        print(f"\n{'='*80}")
        print(f"📊 INBOX: compañía {idx}/{total}")
        try:
            process_company(row, log_callback=None)  # Sin logs a BQ en INBOX (por ahora)
        except Exception as e:
            failed += 1
            print(f"❌ Error procesando INBOX {row.company_name} (ID: {row.company_id}): {e}")

    print(f"\n🏁 INBOX completado. Total: {total}, fallidas: {failed}.")


# =============================================================================
# MODO TEST — cualquier proyecto, parámetros manuales
# =============================================================================

def run_test(args, log_callback):
    """
    Modo de prueba manual en Cloud Shell o entorno local.
    Permite filtrar por compañía y/o endpoint específico.
    Soporta --dry-run para solo mostrar qué haría.
    """
    print(f"\n{'='*80}")
    print("🧪 MODO TEST: ServiceTitan JSON → BigQuery")
    print(f"{'='*80}")
    print(f"📋 Proyecto de contexto: {PROJECT_ALL}")
    if args.company_id:
        print(f"📋 Compañía ID: {args.company_id}")
    else:
        print("📋 Compañía ID: TODAS las activas")
    if args.endpoint:
        print(f"📋 Endpoint(s): {', '.join(args.endpoint)}")
    else:
        print("📋 Endpoint(s): TODOS")
    print(f"📋 Dry-run: {'SÍ' if args.dry_run else 'NO'}")
    print(f"{'='*80}\n")

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
        if not results:
            print(f"❌ Compañía {args.company_id} no encontrada o inactiva.")
            return
    else:
        query = f"""
            SELECT * FROM `{PROJECT_ALL}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
            WHERE company_bigquery_status = TRUE
            ORDER BY company_id
        """
        results = list(client.query(query).result())
        if not results:
            print("❌ No se encontraron compañías activas.")
            return
        print(f"📊 Compañías activas encontradas: {len(results)}\n")

    total      = len(results)
    procesadas = 0
    failed     = 0

    for idx, row in enumerate(results, 1):
        print(f"\n{'#'*80}")
        print(f"📊 TEST: compañía {idx}/{total}")
        print(f"{'#'*80}")
        try:
            process_company(
                row,
                endpoints_override=args.endpoint,
                dry_run=args.dry_run,
                log_callback=log_callback,
            )
            procesadas += 1
        except Exception as e:
            failed += 1
            print(f"❌ Error procesando {row.company_id} ({row.company_name}): {e}")

    print(f"\n{'='*80}")
    print(f"✅ TEST completado | Total: {total} | OK: {procesadas} | Err: {failed}")
    print(f"{'='*80}")


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL: ServiceTitan JSON (GCS) → BigQuery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modos de ejecución:
  all    → Procesa TODAS las compañías del consorcio (pph-central). Usado por Cloud Run Job.
  inbox  → Procesa compañías INBOX candidatas (pph-inbox). Usado por Cloud Run Job INBOX.
  test   → Modo manual en Cloud Shell. Permite filtrar por compañía y/o endpoint.

Ejemplos:
  # Modo TEST: una compañía, un endpoint
  python main.py --mode test --company-id 1 --endpoint locations

  # Modo TEST: verificar sin ejecutar (dry-run)
  python main.py --mode test --company-id 1 --endpoint locations --dry-run

  # Modo TEST: todas las compañías, todos los endpoints
  python main.py --mode test

  # Modo TEST: todas las compañías, un endpoint específico
  python main.py --mode test --endpoint jobs

  # Modo ALL (normalmente lo ejecuta el Cloud Run Job directamente):
  python main.py --mode all

  # Modo INBOX (normalmente lo ejecuta el Cloud Run Job INBOX):
  python main.py --mode inbox
        """,
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["all", "inbox", "test"],
        default=None,
        help=(
            "Modo de ejecución: 'all' (consorcio), 'inbox' (candidatos), 'test' (manual). "
            "Si no se especifica, se usa la variable de entorno ETL_MODE."
        ),
    )
    parser.add_argument(
        "--company-id", "-c",
        type=int,
        default=None,
        help="(Solo modo test) ID de la compañía a procesar.",
    )
    parser.add_argument(
        "--endpoint", "-e",
        type=str,
        nargs="+",
        default=None,
        help="(Solo modo test) Endpoint(s) específico(s) a procesar.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="(Solo modo test) Muestra qué haría sin ejecutar.",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # Resolver modo:
    # 1. --mode explícito (argparse)
    # 2. Variable de entorno ETL_MODE (inyectada por Cloud Run Job)
    # 3. Default: 'test' (si se corre desde Cloud Shell sin nada configurado)
    mode = args.mode or os.environ.get("ETL_MODE", "").lower() or "test"

    if mode not in ("all", "inbox", "test"):
        print(f"❌ Modo inválido: '{mode}'. Debe ser 'all', 'inbox' o 'test'.")
        raise SystemExit(1)

    print(f"\n{'='*80}")
    print(f"🚀 ETL json2bq | MODO: {mode.upper()}")
    print(f"{'='*80}\n")

    if mode == "all":
        log_cb = _make_log_callback("etl_json2bq_all")
        run_all(args, log_cb)

    elif mode == "inbox":
        run_inbox(args)

    elif mode == "test":
        log_cb = _make_log_callback("etl_json2bq_test")
        run_test(args, log_cb)


if __name__ == "__main__":
    main()
