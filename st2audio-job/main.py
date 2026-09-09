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
    ServiceTitanAuth,
    ensure_audio_bucket_exists,
    upload_to_bucket,
    upload_audio_stream_to_bucket,
    parse_duration_seconds,
    get_balanced_tasks,
)
from google.cloud import storage

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


# =============================================================================
# PUNTO 1: CONSULTA DE LLAMADAS PENDIENTES (LEFT JOIN)
# =============================================================================

def get_pending_calls(client, project_id, limit=None):
    """
    Obtiene las llamadas que tienen duración > 0 y que aún no han sido
    procesadas o cuyo estatus anterior fue fallido/dañado (status < 0).
    
    Args:
        client     : Cliente de BigQuery.
        project_id : Project ID de la compañía.
        limit      : Límite opcional de llamadas a retornar (para pruebas).
        
    Returns:
        Lista de filas con llamadas pendientes.
    """
    dataset_suffix = project_id.replace("-", "_")
    table_call = f"`{project_id}.servicetitan_{dataset_suffix}.call`"
    table_recordings = f"`{project_id}.bronze.call_recordings`"
    limit_clause = f"LIMIT {limit}" if limit else ""

    # Verificar si la tabla de recordings ya existe
    try:
        client.get_table(f"{project_id}.bronze.call_recordings")
        recordings_exists = True
    except Exception:
        recordings_exists = False

    if recordings_exists:
        query = f"""
            SELECT 
                c.lead_call_id,
                c.id,
                c.lead_call_received_on,
                c.lead_call_duration,
                c.lead_call_call_type,
                c.lead_call_direction,
                c.lead_call_recording_url
            FROM {table_call} c
            LEFT JOIN {table_recordings} r
                ON c.lead_call_id = r.lead_call_id
            WHERE (r.lead_call_id IS NULL OR r.status < 0)
              AND (c.lead_call_duration IS NOT NULL AND c.lead_call_duration != '00:00:00')
              AND (c._fivetran_deleted IS FALSE OR c._fivetran_deleted IS NULL)
            ORDER BY c.lead_call_received_on DESC
            {limit_clause}
        """
    else:
        query = f"""
            SELECT 
                c.lead_call_id,
                c.id,
                c.lead_call_received_on,
                c.lead_call_duration,
                c.lead_call_call_type,
                c.lead_call_direction,
                c.lead_call_recording_url
            FROM {table_call} c
            WHERE (c.lead_call_duration IS NOT NULL AND c.lead_call_duration != '00:00:00')
              AND (c._fivetran_deleted IS FALSE OR c._fivetran_deleted IS NULL)
            ORDER BY c.lead_call_received_on DESC
            {limit_clause}
        """

    return list(client.query(query).result())


# =============================================================================
# NÚCLEO: process_company() — PUNTOS 1, 2 Y 3
# =============================================================================

def process_company(row, dry_run=False, limit=None):
    """
    Puntos 1, 2 y 3:
    1. Consulta BigQuery (LEFT JOIN) para obtener llamadas pendientes.
    2. Realiza petición HTTP binaria a ServiceTitan Telecom API para cada llamada.
    3. Transmite el audio binario directamente a Cloud Storage y genera el JSONL
       de metadata para la tabla bronze.call_recordings.

    Args:
        row     : Fila de BigQuery con datos y credenciales de la compañía.
        dry_run : Si True, solo muestra qué haría sin ejecutar llamadas HTTP.
        limit   : Límite opcional de llamadas a procesar.
    """
    company_id       = row.company_id
    company_name     = row.company_name
    company_new_name = row.company_new_name
    app_id           = row.app_id
    client_id        = row.client_id
    client_secret    = row.client_secret
    tenant_id        = row.tenant_id
    app_key          = row.app_key
    project_id       = row.company_project_id

    print(f"\n{'='*80}")
    print(f"🏢 Procesando Audios: {company_name} (ID: {company_id}) | Proyecto: {project_id}")
    if dry_run:
        print("🔍 MODO DRY-RUN: Solo mostrando acciones, sin ejecutar.")
    print(f"{'='*80}")

    if not project_id:
        raise ValueError(
            f"company_project_id vacío para company_id={company_id}. "
            "Verifica la tabla settings.companies."
        )

    # ── Setup ────────────────────────────────────────────────────────────────
    if dry_run:
        bucket_name    = f"{project_id}_audio"
        st_client      = None
        storage_client = None
    else:
        bucket_name    = ensure_audio_bucket_exists(project_id)
        st_client      = ServiceTitanAuth(app_id, client_id, client_secret, tenant_id, app_key)
        storage_client = storage.Client(project=project_id)

    bq_client = bigquery.Client(project=project_id)

    # ── Punto 1: Consulta SQL de llamadas pendientes ─────────────────────────
    print(f"📋 Consultando llamadas pendientes para {company_name}...")
    try:
        pending_calls = get_pending_calls(bq_client, project_id, limit=limit)
    except Exception as e:
        print(f"❌ Error al consultar llamadas pendientes: {str(e)}")
        return

    total_calls = len(pending_calls)
    print(f"📊 Total llamadas pendientes a procesar: {total_calls}")

    if total_calls == 0:
        print("✅ No hay llamadas pendientes de descarga para esta compañía.")
        return

    # Preparar archivo de metadata local JSONL
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    metadata_filename_ts    = f"/tmp/servicetitan_call_recordings_{timestamp}.jsonl"
    metadata_filename_alias = f"/tmp/servicetitan_call_recordings.jsonl"

    metadata_records = []
    audios_guardados = 0
    sin_audio_count  = 0
    errores_count    = 0

    # ── Punto 2 & 3: Extracción HTTP, Streaming a GCS y Metadata ─────────────
    for idx, call in enumerate(pending_calls, 1):
        lead_call_id     = call.lead_call_id
        duration_str     = call.lead_call_duration
        duration_sec     = parse_duration_seconds(duration_str)
        call_received_on = call.lead_call_received_on

        print(f"\n🔄 [{idx}/{total_calls}] Llamada ID: {lead_call_id} | Duración: {duration_str}")

        if dry_run:
            print(f"  📋 [DRY-RUN] Solicitaría GET /telecom/v2/tenant/{tenant_id}/calls/{lead_call_id}/recording")
            print(f"  📋 [DRY-RUN] Destino GCS: gs://{bucket_name}/{lead_call_id}.mp3")
            continue

        dest_blob_name = f"{lead_call_id}.mp3"
        gcs_uri        = None
        file_size      = None
        content_type   = None
        status         = -1
        http_code      = None
        error_msg      = None

        try:
            response = st_client.get_call_recording(lead_call_id)
            http_code = response.status_code

            if http_code == 200:
                # ── Audio disponible: streaming directo a GCS ────────────────
                file_size = upload_audio_stream_to_bucket(
                    storage_client=storage_client,
                    bucket_name=bucket_name,
                    dest_blob_name=dest_blob_name,
                    response_stream=response,
                    content_type="audio/mpeg"
                )
                gcs_uri = f"gs://{bucket_name}/{dest_blob_name}"
                content_type = "audio/mpeg"
                status = 0  # 0: Almacenado / Listo para ML
                audios_guardados += 1
                print(f"  ✅ Audio guardado en gs://{bucket_name}/{dest_blob_name} ({file_size} bytes)")

            elif http_code in (404, 204):
                # ── Sin audio en ServiceTitan ────────────────────────────────
                status = -2  # -2: Sin audio / No disponible
                dest_blob_name = None
                error_msg = f"Audio not available in ServiceTitan (HTTP {http_code})"
                sin_audio_count += 1
                print(f"  ⚠️  Llamada sin grabación en ServiceTitan (HTTP {http_code})")

            else:
                # ── Error HTTP temporal (5xx, 429, etc.) ──────────────────────
                status = -1  # -1: Error temporal de extracción
                dest_blob_name = None
                error_msg = f"HTTP {http_code}: {response.text[:200]}"
                errores_count += 1
                print(f"  ❌ Error HTTP {http_code} al solicitar grabación")

        except Exception as e:
            status = -1
            dest_blob_name = None
            error_msg = str(e)[:300]
            errores_count += 1
            print(f"  ❌ Excepción descargando llamada {lead_call_id}: {error_msg}")

        # Construir registro de metadata para call_recordings
        record = {
            "lead_call_id": lead_call_id,
            "id": call.id,
            "company_id": company_id,
            "gcs_uri": gcs_uri,
            "file_name": dest_blob_name,
            "file_size_bytes": file_size,
            "content_type": content_type,
            "call_received_on": call_received_on.isoformat() if call_received_on else None,
            "call_duration": duration_str,
            "call_duration_seconds": duration_sec,
            "call_direction": call.lead_call_direction,
            "call_type": call.lead_call_call_type,
            "status": status,
            "http_status_code": http_code,
            "error_message": error_msg,
            "retry_count": 0,
            "_etl_extracted_at": datetime.utcnow().isoformat()
        }
        metadata_records.append(record)

    # ── Guardar y subir archivo de metadata JSONL a GCS ──────────────────────
    if not dry_run and metadata_records:
        with open(metadata_filename_ts, "w", encoding="utf-8") as f_ts, \
             open(metadata_filename_alias, "w", encoding="utf-8") as f_alias:
            for rec in metadata_records:
                line = json.dumps(rec, ensure_ascii=False) + "\n"
                f_ts.write(line)
                f_alias.write(line)

        # Subir metadata al bucket de la compañía
        upload_to_bucket(bucket_name, project_id, metadata_filename_ts, f"metadata/{os.path.basename(metadata_filename_ts)}")
        upload_to_bucket(bucket_name, project_id, metadata_filename_alias, f"metadata/{os.path.basename(metadata_filename_alias)}")

        try:
            os.remove(metadata_filename_ts)
            os.remove(metadata_filename_alias)
        except Exception:
            pass

        print(f"\n📊 Resumen Extracción {company_name}:")
        print(f"   🎵 Audios descargados (status=0) : {audios_guardados}")
        print(f"   ⚠️  Sin audio (status=-2)         : {sin_audio_count}")
        print(f"   ❌ Errores (status=-1)           : {errores_count}")
        print(f"   📝 Metadata generada y subida    : {len(metadata_records)} registros")




# =============================================================================
# OBTENCIÓN DE COMPAÑÍAS ACTIVAS
# =============================================================================

def fetch_active_companies(client, project_id, company_id=0):
    """
    Obtiene la lista de compañías activas desde settings.companies.
    
    Args:
        client     : Cliente de BigQuery.
        project_id : Proyecto donde reside settings.companies.
        company_id : ID específico (>0) o 0/None para procesar todas.
        
    Returns:
        Lista de filas con datos y credenciales de las compañías.
    """
    if company_id and int(company_id) > 0:
        query = f"""
            SELECT * FROM `{project_id}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
            WHERE company_fivetran_status = TRUE
              AND company_id = @company_id
              AND company_project_id IS NOT NULL
            ORDER BY company_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("company_id", "INT64", int(company_id))
            ]
        )
        return list(client.query(query, job_config=job_config).result())
    else:
        query = f"""
            SELECT * FROM `{project_id}.{DATASET_COMPANIES}.{TABLE_COMPANIES}`
            WHERE company_fivetran_status = TRUE
              AND company_project_id IS NOT NULL
            ORDER BY company_id
        """
        return list(client.query(query).result())


# =============================================================================
# MODO ALL — PRO / QUA / DEV  (pph-central / company_fivetran_status)
# =============================================================================

def run_all(args):
    """
    Procesa compañías del consorcio desde pph-central.
    Soporta filtro por --company-id y paralelismo de Cloud Run Tasks.
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
    results = fetch_active_companies(client, PROJECT_ALL, args.company_id)
    total   = len(results)

    if not results:
        print(f"❌ No se encontraron compañías activas para company_id={args.company_id} en {PROJECT_ALL}")
        return

    # Distribución de carga para modo paralelo (solo si hay más de 1 compañía)
    if is_parallel and len(results) > 1:
        results = get_balanced_tasks(client, results, task_count, task_index)
        total_assigned = len(results)
    else:
        total_assigned = total
        print(f"📊 Total compañías a procesar: {total}")

    print(f"{'='*80}\n")

    procesadas = 0
    for idx, row in enumerate(results, 1):
        try:
            if is_parallel:
                print(f"\n[{idx}/{total_assigned}] Procesando compañía: {row.company_name} (ID: {row.company_id})")
            process_company(row)
            procesadas += 1
        except Exception as e:
            print(f"❌ Error procesando compañía {row.company_name} (ID: {row.company_id}): {str(e)}")

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
    Soporta filtro opcional por --company-id.
    """
    print("Conectando a BigQuery (pph-inbox)...")
    client = bigquery.Client(project=PROJECT_INBOX)
    results = fetch_active_companies(client, PROJECT_INBOX, args.company_id)

    if not results:
        print(f"❌ No se encontró ninguna compañía INBOX activa (company_id={args.company_id}).")
        return

    total = len(results)
    print(f"📊 Se encontraron {total} compañía(s) INBOX activa(s) para procesar\n")

    for idx, row in enumerate(results, 1):
        try:
            print(f"📊 Procesando compañía {idx} de {total}: {row.company_name} (ID: {row.company_id})")
            process_company(row)
        except Exception as e:
            print(f"❌ Error procesando compañía INBOX {row.company_name} (ID: {row.company_id}): {str(e)}")
        print(f"\n{'='*80}")

    print("🏁 Procesamiento INBOX completado.")


# =============================================================================
# MODO TEST — una sola compañía o todas, parámetros manuales
# =============================================================================

def run_test(args):
    """
    Modo de prueba manual en Cloud Shell o entorno local.
    Si se especifica --company-id (>0), procesa solo esa compañía.
    Si es 0 o no se especifica, procesa TODAS las compañías activas.
    Permite filtrar por endpoint y usar --dry-run.
    """
    print(f"\n{'='*80}")
    print("🧪 MODO TEST: ServiceTitan ST → Audio")
    print(f"{'='*80}")
    if args.company_id and args.company_id > 0:
        print(f"📋 Compañía ID: {args.company_id}")
    else:
        print("📋 Compañía ID: TODAS las activas (0)")
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
    results = fetch_active_companies(client, PROJECT_ALL, args.company_id)

    if not results:
        print(f"❌ No se encontraron compañías activas (company_id={args.company_id})")
        return

    total      = len(results)
    procesadas = 0
    failed     = 0

    print(f"📊 Compañías activas encontradas: {total}\n")

    for idx, row in enumerate(results, 1):
        print(f"\n{'#'*80}")
        print(f"📊 TEST: compañía {idx}/{total}: {row.company_name} (ID: {row.company_id})")
        print(f"{'#'*80}")
        try:
            process_company(row, dry_run=args.dry_run, limit=args.limit)
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
        description="ETL: ServiceTitan Telecom API → Audio (Cloud Storage)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Modos de ejecución:
  all    → Procesa compañías del consorcio (pph-central). Usado por Cloud Run Job.
  inbox  → Procesa compañías INBOX candidatas (pph-inbox). Usado por Cloud Run Job INBOX.
  test   → Modo manual en Cloud Shell. Permite especificar --company-id.

Ejemplos:
  # Procesar solo compañía ID 1 (modo test)
  python main.py --mode test --company-id 1

  # Procesar solo 5 llamadas de prueba
  python main.py --mode test --company-id 1 --limit 5

  # Procesar TODAS las compañías activas (modo test)
  python main.py --mode test --company-id 0

  # Verificar sin ejecutar (dry-run)
  python main.py --mode test --company-id 1 --dry-run

  # Modo ALL (Cloud Run Job)
  python main.py --mode all
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
        default=0,
        help="ID de la compañía a procesar (ej: 1). Si es 0 o no se especifica, procesa todas las activas.",
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="(Solo modo test) Límite máximo de llamadas a procesar por compañía.",
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

    # Resolver company_id si no vino por CLI pero está en entorno
    if not args.company_id and os.environ.get("COMPANY_ID"):
        try:
            args.company_id = int(os.environ.get("COMPANY_ID"))
        except ValueError:
            args.company_id = 0

    if mode not in ("all", "inbox", "test"):
        print(f"❌ Modo inválido: '{mode}'. Debe ser 'all', 'inbox' o 'test'.")
        raise SystemExit(1)

    print(f"\n{'='*80}")
    print(f"🚀 ETL st2audio | MODO: {mode.upper()} | COMPANY_ID: {args.company_id if args.company_id > 0 else 'TODAS'}")
    print(f"{'='*80}\n")

    if mode == "all":
        run_all(args)
    elif mode == "inbox":
        run_inbox(args)
    elif mode == "test":
        run_test(args)


if __name__ == "__main__":
    main()


