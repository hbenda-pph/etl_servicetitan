"""
ETL Job: ServiceTitan API → JSON (Cloud Storage)
==================================================
Script unificado que reemplaza los tres scripts anteriores:
  - servicetitan_all_st_to_json.py   → modo --mode all
  - servicetitan_inbox_st_to_json.py → modo --mode inbox
  - servicetitan_st_to_json.py       → modo --mode test

Uso en Cloud Shell / Local (modo TEST):
    python main.py --mode test --company-id 1
    python main.py --mode test --company-id 1 --endpoint "gross-pay-items"
    python main.py --mode test --company-id 1 --endpoint "gross-pay-items" --dry-run

El modo también puede establecerse con la variable de entorno ETL_MODE,
que es la forma en que lo inyectan los scripts build_deploy.sh:
    ETL_MODE=all   → build_deploy.sh
    ETL_MODE=inbox → build_deploy_inbox.sh
"""

import argparse
import json
import os
import shutil
from datetime import datetime
from google.cloud import bigquery

# Importar funciones comunes
from servicetitan_common import (
    get_project_source,
    get_bigquery_project_id,
    load_endpoints_from_metadata,
    ServiceTitanAuth,
    ensure_bucket_exists,
    upload_to_bucket,
)

# =============================================================================
# CONFIGURACIÓN DE PROYECTOS Y TABLAS MAESTRAS
# =============================================================================

# Proyecto central (ALL / TEST)
PROJECT_ALL = get_bigquery_project_id()
PROJECT_SOURCE = get_project_source()

# Proyecto INBOX: siempre fijo
PROJECT_INBOX = "pph-inbox"

# Tablas maestras (iguales para ambos modos)
DATASET_COMPANIES = "settings"
TABLE_COMPANIES   = "companies"

# Endpoints grandes que requieren modo streaming
LARGE_ENDPOINTS = ["gross-pay-items"]


# =============================================================================
# NÚCLEO: process_company()
# =============================================================================

def process_company(row, endpoints_filter=None, dry_run=False):
    """
    Descarga los datos de ServiceTitan para cada endpoint de una compañía
    y los sube a su bucket en Cloud Storage.

    Args:
        row             : Fila de BigQuery con datos de la compañía.
        endpoints_filter: Lista de api_data o table_name a procesar.
                          None = procesa todos los endpoints de metadata.
        dry_run         : Si True, solo muestra qué haría sin ejecutar nada.
    """
    # Credenciales y datos de la compañía
    company_id      = row.company_id
    company_name    = row.company_name
    company_new_name = row.company_new_name
    app_id          = row.app_id
    client_id       = row.client_id
    client_secret   = row.client_secret
    tenant_id       = row.tenant_id
    app_key         = row.app_key
    project_id      = row.company_project_id

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

    # ── Setup (omitido en dry-run) ────────────────────────────────────────────
    if dry_run:
        print(f"📋 [DRY-RUN] Se crearía/verificaría bucket: {project_id}_servicetitan")
        print("📋 [DRY-RUN] Se instanciaría cliente ServiceTitan")
        bucket_name = f"{project_id}_servicetitan"
        st_client   = None
    else:
        bucket_name = ensure_bucket_exists(project_id)
        st_client   = ServiceTitanAuth(app_id, client_id, client_secret, tenant_id, app_key)

    # ── Cargar endpoints ──────────────────────────────────────────────────────
    all_endpoints = load_endpoints_from_metadata()

    if endpoints_filter:
        endpoints_to_process = [
            (api_url_base, api_data, table_name)
            for api_url_base, api_data, table_name in all_endpoints
            if api_data in endpoints_filter or table_name in endpoints_filter
        ]
        if not endpoints_to_process:
            print(f"⚠️  No se encontraron endpoints que coincidan con: {endpoints_filter}")
            return
        print(f"📋 Endpoints filtrados: {len(endpoints_to_process)}")
    else:
        endpoints_to_process = all_endpoints
        print(f"📋 Total de endpoints: {len(endpoints_to_process)}")

    # ── Procesar cada endpoint ────────────────────────────────────────────────
    for api_url_base, api_data, table_name in endpoints_to_process:
        print(f"\n🔄 {'[DRY-RUN] ' if dry_run else ''}Descargando endpoint: {api_data} (tabla: {table_name})")

        timestamp      = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_ts    = f"servicetitan_{table_name}_{timestamp}.json"
        filename_alias = f"servicetitan_{table_name}.json"

        # ── Modo DRY-RUN ─────────────────────────────────────────────────────
        if dry_run:
            if api_data.startswith("export/"):
                print("  📋 [DRY-RUN] Usaría modo EXPORT (continueFrom)")
            elif api_data in LARGE_ENDPOINTS or any(large in api_data for large in LARGE_ENDPOINTS):
                print("  📋 [DRY-RUN] Usaría modo STREAMING (archivo grande)")
            else:
                print("  📋 [DRY-RUN] Usaría modo NORMAL (paginación page=)")
            print(f"  📋 [DRY-RUN] Archivos: {filename_ts}, {filename_alias}")
            print(f"  📋 [DRY-RUN] Destino:  gs://{bucket_name}/")
            continue

        # ── Descarga real ─────────────────────────────────────────────────────
        try:
            if api_data.startswith("export/"):
                # Endpoint EXPORT: usa continueFrom para paginación
                total, continue_token = st_client.get_data_export(api_url_base, api_data, filename_alias)
                print(f"📊 [EXPORT MODE] Total registros descargados: {total}")
                if continue_token:
                    print(f"🔖 [EXPORT MODE] Token para próxima ejecución: {continue_token}")
                shutil.copy2(filename_alias, filename_ts)

            elif api_data in LARGE_ENDPOINTS or any(large in api_data for large in LARGE_ENDPOINTS):
                # Endpoint GRANDE: usar streaming para evitar problemas de memoria
                print(f"📥 [STREAMING MODE] Usando streaming para endpoint grande: {api_data}")
                total = st_client.get_data_streaming(api_url_base, api_data, filename_alias)
                print(f"📊 [STREAMING MODE] Total registros descargados: {total}")
                shutil.copy2(filename_alias, filename_ts)

            else:
                # Endpoint NORMAL: carga en memoria con paginación page=
                data = st_client.get_data(api_url_base, api_data)
                print(f"📊 [NORMAL MODE] Total registros descargados: {len(data)}")
                with open(filename_ts, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                with open(filename_alias, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

            # Subir ambos archivos al bucket
            upload_to_bucket(bucket_name, project_id, filename_ts, filename_ts)
            upload_to_bucket(bucket_name, project_id, filename_alias, filename_alias)

            # Borrar archivos locales
            try:
                os.remove(filename_ts)
                os.remove(filename_alias)
            except Exception:
                pass

            print(f"✅ Endpoint {api_data} procesado y archivos subidos.")

        except Exception as e:
            print(f"❌ Error en endpoint {api_data}: {str(e)}")


# =============================================================================
# MODO ALL — PRO / QUA / DEV  (pph-central / company_fivetran_status)
# =============================================================================

def run_all(args):
    """
    Procesa todas las compañías del consorcio desde pph-central.
    Soporta paralelismo de Cloud Run Tasks.
    Usa company_fivetran_status = TRUE como filtro.
    """
    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
    task_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))
    is_parallel = task_count > 1

    if is_parallel:
        print(f"\n{'='*80}")
        print(f"🚀 MODO PARALELO | Tarea {task_index + 1}/{task_count}")
        print(f"{'='*80}")

    print(f"🔍 Proyecto detectado para companies: {PROJECT_SOURCE}")
    print(f"🔍 Project ID para queries: {PROJECT_ALL}")
    print("Conectando a BigQuery (pph-central)...")
    if is_parallel:
        print(f"🔄 Procesamiento paralelo: Tarea {task_index + 1} de {task_count}")

    client = bigquery.Client()  # Usa el project del service account automáticamente
    query = f"""
        SELECT * FROM `{PROJECT_ALL}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
        WHERE company_fivetran_status = TRUE
        ORDER BY company_id
    """
    results = list(client.query(query).result())
    total   = len(results)

    # Partición para modo paralelo
    if is_parallel:
        companies_per_task = total // task_count
        remainder          = total % task_count
        start_idx = task_index * companies_per_task + min(task_index, remainder)
        end_idx   = start_idx + companies_per_task + (1 if task_index < remainder else 0)
        results   = results[start_idx:end_idx]
        total_assigned = len(results)
        print(f"📊 Total compañías: {total}")
        print(f"📊 Asignadas a esta tarea: {total_assigned} (índices {start_idx+1}-{end_idx} de {total})")
    else:
        total_assigned = total
        print(f"📊 Total compañías a procesar: {total}")

    print(f"{'='*80}\n")

    procesadas = 0
    for idx, row in enumerate(results, 1):
        try:
            if is_parallel:
                print(f"\n[{idx}/{total_assigned}] Procesando compañía: {row.company_name}")
            process_company(row)
            procesadas += 1
        except Exception as e:
            print(f"❌ Error procesando compañía {row.company_name}: {str(e)}")

    print(f"\n{'='*80}")
    if is_parallel:
        print(f"🏁 Resumen Tarea {task_index+1}/{task_count}: {procesadas}/{total_assigned} compañías procesadas.")
        print(f"📊 Total global: {total} compañías distribuidas en {task_count} tareas")
    else:
        print(f"🏁 Resumen: {procesadas}/{total} compañías procesadas exitosamente.")


# =============================================================================
# MODO INBOX — pph-inbox  (company_fivetran_status)
# =============================================================================

def run_inbox(args):
    """
    Procesa las compañías candidatas (INBOX) desde pph-inbox.settings.companies.
    No usa paralelismo (volumen reducido ~5 compañías).
    Usa company_fivetran_status = TRUE como filtro (igual que ALL).
    """
    print("Conectando a BigQuery (pph-inbox)...")
    client = bigquery.Client(project=PROJECT_INBOX)
    query = f"""
        SELECT * FROM `{PROJECT_INBOX}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
        WHERE company_fivetran_status = TRUE
          AND company_project_id IS NOT NULL
        ORDER BY company_id
    """
    results = list(client.query(query).result())

    if not results:
        print("❌ No se encontró ninguna compañía INBOX activa (company_fivetran_status=TRUE).")
        return

    total = len(results)
    print(f"📊 Se encontraron {total} compañía(s) INBOX activa(s) para procesar\n")

    for idx, row in enumerate(results, 1):
        try:
            print(f"📊 Procesando compañía {idx} de {total}")
            process_company(row)
        except Exception as e:
            print(f"❌ Error procesando compañía INBOX {row.company_name} (ID: {row.company_id}): {str(e)}")
        print(f"\n{'='*80}")

    print("🏁 Procesamiento INBOX completado.")


# =============================================================================
# MODO TEST — una sola compañía, parámetros manuales
# =============================================================================

def run_test(args):
    """
    Modo de prueba manual en Cloud Shell o entorno local.
    Si se especifica --company-id, procesa solo esa compañía.
    Si no se especifica, procesa TODAS las compañías activas (igual que run_all).
    Permite filtrar por endpoint y usar --dry-run.
    """
    print(f"\n{'='*80}")
    print("🧪 MODO TEST: ServiceTitan ST → JSON")
    print(f"{'='*80}")
    if args.company_id:
        print(f"📋 Compañía ID: {args.company_id}")
    else:
        print("📋 Compañía ID: TODAS las activas")
    if args.endpoint:
        print(f"📋 Endpoint(s): {', '.join(args.endpoint)}")
    else:
        print("📋 Endpoint(s): TODOS")
    print(f"📋 Dry-run: {'SÍ' if args.dry_run else 'NO'}")
    print(f"🔍 Proyecto detectado: {PROJECT_SOURCE}")
    print(f"🔍 Project ID para queries: {PROJECT_ALL}")
    print(f"{'='*80}\n")

    print("Conectando a BigQuery para obtener compañía(s)...")
    client = bigquery.Client(project=PROJECT_ALL)

    if args.company_id:
        # Procesar UNA sola compañía
        query = f"""
            SELECT * FROM `{PROJECT_ALL}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
            WHERE company_id = @company_id
              AND company_fivetran_status = TRUE
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("company_id", "INT64", args.company_id)
            ]
        )
        results = list(client.query(query, job_config=job_config).result())
        if not results:
            print(f"❌ No se encontró compañía con ID {args.company_id} o no está activa (company_fivetran_status = TRUE)")
            return
    else:
        # Procesar TODAS las compañías activas
        query = f"""
            SELECT * FROM `{PROJECT_ALL}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
            WHERE company_fivetran_status = TRUE
            ORDER BY company_id
        """
        results = list(client.query(query).result())
        if not results:
            print("❌ No se encontraron compañías activas (company_fivetran_status = TRUE)")
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
            process_company(row, endpoints_filter=args.endpoint, dry_run=args.dry_run)
            procesadas += 1
        except Exception as e:
            failed += 1
            print(f"❌ Error procesando {row.company_id} ({row.company_name}): {str(e)}")

    print(f"\n{'='*80}")
    print(f"✅ TEST completado | Total: {total} | OK: {procesadas} | Err: {failed}")
    print(f"{'='*80}")


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL: ServiceTitan API → JSON (Cloud Storage)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modos de ejecución:
  all    → Procesa TODAS las compañías del consorcio (pph-central). Usado por Cloud Run Job.
  inbox  → Procesa compañías INBOX candidatas (pph-inbox). Usado por Cloud Run Job INBOX.
  test   → Modo manual en Cloud Shell. Requiere --company-id.

Ejemplos:
  # Modo TEST: una compañía, todos los endpoints
  python main.py --mode test --company-id 1

  # Modo TEST: una compañía, un endpoint específico
  python main.py --mode test --company-id 1 --endpoint "gross-pay-items"

  # Modo TEST: verificar sin ejecutar (dry-run)
  python main.py --mode test --company-id 1 --endpoint "gross-pay-items" --dry-run

  # Modo ALL (normalmente lo ejecuta el Cloud Run Job):
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
        help="(Requerido en modo test) ID de la compañía a procesar.",
    )
    parser.add_argument(
        "--endpoint", "-e",
        type=str,
        nargs="+",
        default=None,
        help="(Solo modo test) Endpoint(s) específico(s) a procesar (api_data o table_name).",
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
    print(f"🚀 ETL st2json | MODO: {mode.upper()}")
    print(f"{'='*80}\n")

    if mode == "all":
        run_all(args)
    elif mode == "inbox":
        run_inbox(args)
    elif mode == "test":
        run_test(args)


if __name__ == "__main__":
    main()

