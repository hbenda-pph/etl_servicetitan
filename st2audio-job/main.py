"""
ETL Job: ServiceTitan Telecom API → Audio (Cloud Storage & BigQuery)
====================================================================
Descarga audios de llamadas desde ServiceTitan Telecom API (/telecom/v2/tenant/{tenant}/calls/{id}/recording),
transmite el binario directamente a Cloud Storage (gs://{project_id}_audio/{lead_call_id}.mp3) y
registra los metadatos y estatus en BigQuery (`bronze.call_recordings`).

Uso en Cloud Shell / Local (modo TEST):
    python main.py --mode test --company-id 1
    python main.py --mode test --company-id 1 --limit 5
    python main.py --mode test --company-id 1 --limit 5 --dry-run

Modos disponibles:
    ETL_MODE=all   → build_deploy.sh (Cloud Run Job consorcio)
    ETL_MODE=inbox → build_deploy_inbox.sh (Cloud Run Job candidatos)
    ETL_MODE=test  → Ejecución local/manual
"""

import argparse
import json
import os
import shutil
from datetime import datetime
from google.cloud import bigquery, storage

from servicetitan_common import (
    get_project_source,
    get_bigquery_project_id,
    ServiceTitanAuth,
    ensure_audio_bucket_exists,
    ensure_call_recordings_table_exists,
    upload_to_bucket,
    parse_duration_seconds,
    get_balanced_tasks,
)

# =============================================================================
# CONFIGURACIÓN DE PROYECTOS Y TABLAS MAESTRAS
# =============================================================================

PROJECT_ALL = get_bigquery_project_id()
PROJECT_SOURCE = get_project_source()
PROJECT_INBOX = "pph-inbox"

DATASET_COMPANIES = "settings"
TABLE_COMPANIES = "companies"


# =============================================================================
# CONSULTA DE LLAMADAS PENDIENTES (LEFT JOIN)
# =============================================================================

def get_pending_calls(client, project_id, limit=None):
    """
    Obtiene las llamadas que tienen duración > 0 y que aún no han sido
    procesadas o cuyo estatus anterior fue fallido/dañado (status < 0).
    """
    dataset_suffix = project_id.replace("-", "_")
    table_call = f"`{project_id}.servicetitan_{dataset_suffix}.call`"
    table_recordings = f"`{project_id}.bronze.call_recordings`"
    limit_clause = f"LIMIT {limit}" if limit else ""

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
    return list(client.query(query).result())


# =============================================================================
# PERSISTENCIA EN BIGQUERY (MERGE IDEMPOTENTE)
# =============================================================================

def save_call_recordings_to_bigquery(bq_client, project_id, records):
    """
    Inserta o actualiza los registros de auditoría en bronze.call_recordings
    usando una tabla temporal y MERGE para garantizar idempotencia.
    """
    if not records:
        return

    timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    temp_table_id = f"{project_id}.bronze._stg_call_rec_{timestamp_str}"
    target_table_id = f"{project_id}.bronze.call_recordings"

    schema = [
        bigquery.SchemaField("lead_call_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("gcs_uri", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("file_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("file_size_bytes", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("content_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("status", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("http_status_code", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("retry_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("_etl_synced", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("_etl_operation", "STRING", mode="NULLABLE"),
    ]

    # 1. Cargar registros en tabla temporal de staging
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = bq_client.load_table_from_json(records, temp_table_id, job_config=job_config)
    load_job.result()

    # 2. Ejecutar MERGE hacia bronze.call_recordings
    merge_sql = f"""
    MERGE `{target_table_id}` T
    USING `{temp_table_id}` S
    ON T.lead_call_id = S.lead_call_id
    WHEN MATCHED THEN UPDATE SET
      id = S.id,
      gcs_uri = S.gcs_uri,
      file_name = S.file_name,
      file_size_bytes = S.file_size_bytes,
      content_type = S.content_type,
      status = S.status,
      http_status_code = S.http_status_code,
      error_message = S.error_message,
      retry_count = COALESCE(T.retry_count, 0) + 1,
      _etl_synced = S._etl_synced,
      _etl_operation = 'UPDATE'
    WHEN NOT MATCHED THEN INSERT (
      lead_call_id, id, gcs_uri, file_name, file_size_bytes, content_type,
      status, http_status_code, error_message, retry_count, _etl_synced, _etl_operation
    ) VALUES (
      S.lead_call_id, S.id, S.gcs_uri, S.file_name, S.file_size_bytes, S.content_type,
      S.status, S.http_status_code, S.error_message, 0, S._etl_synced, 'INSERT'
    );
    """
    bq_client.query(merge_sql).result()

    # 3. Eliminar tabla temporal de staging
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    print(f"  💾 {len(records)} registros guardados/sincronizados en `{target_table_id}`")


# =============================================================================
# NÚCLEO: process_company()
# =============================================================================

def process_company(row, dry_run=False, limit=None):
    """
    1. Asegura bucket `{project_id}_audio` y tabla `{project_id}.bronze.call_recordings`.
    2. Consulta BigQuery (LEFT JOIN) para obtener llamadas pendientes.
    3. Realiza petición HTTP binaria a Telecom API para cada llamada.
    4. Transmite el stream a Cloud Storage (`gs://{project_id}_audio/{lead_call_id}.mp3`).
    5. Guarda metadatos y estatus en BigQuery (`bronze.call_recordings`).
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
    bq_client = bigquery.Client(project=project_id)

    if dry_run:
        bucket_name    = f"{project_id}_audio"
        st_client      = None
        storage_client = None
    else:
        # Validar / crear bucket y tabla destino
        bucket_name = ensure_audio_bucket_exists(project_id)
        ensure_call_recordings_table_exists(bq_client, project_id)
        st_client      = ServiceTitanAuth(app_id, client_id, client_secret, tenant_id, app_key)
        storage_client = storage.Client(project=project_id)

    # ── Consulta SQL de llamadas pendientes ──────────────────────────────────
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

    metadata_records = []
    audios_guardados = 0
    sin_audio_count  = 0
    errores_count    = 0
    synced_timestamp = datetime.utcnow().isoformat()

    # ── Extracción HTTP, Streaming a GCS y Armado de Metadata ─────────────────
    for idx, call in enumerate(pending_calls, 1):
        lead_call_id = call.lead_call_id
        duration_str = call.lead_call_duration

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
                file_size = upload_to_bucket(
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
                status = -2  # -2: Sin audio en ST / No disponible
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

        # Estructura normalizada de 12 campos para bronze.call_recordings
        record = {
            "lead_call_id": int(lead_call_id),
            "id": int(call.id) if call.id is not None else None,
            "gcs_uri": gcs_uri,
            "file_name": dest_blob_name,
            "file_size_bytes": file_size,
            "content_type": content_type,
            "status": int(status),
            "http_status_code": int(http_code) if http_code else None,
            "error_message": error_msg,
            "retry_count": 0,
            "_etl_synced": synced_timestamp,
            "_etl_operation": "INSERT"
        }
        metadata_records.append(record)

    # ── Guardar en BigQuery y backup local/GCS ───────────────────────────────
    if not dry_run and metadata_records:
        # 1. Guardar/Actualizar directamente en BigQuery bronze.call_recordings
        save_call_recordings_to_bigquery(bq_client, project_id, metadata_records)

        # 2. Respaldo JSONL en GCS (metadata/)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        metadata_filename_ts = f"/tmp/servicetitan_call_recordings_{timestamp}.jsonl"
        with open(metadata_filename_ts, "w", encoding="utf-8") as f_ts:
            for rec in metadata_records:
                f_ts.write(json.dumps(rec, ensure_ascii=False) + "\n")

        metadata_blob = storage_client.bucket(bucket_name).blob(f"metadata/{os.path.basename(metadata_filename_ts)}")
        metadata_blob.upload_from_filename(metadata_filename_ts)
        print(f"  📤 Respaldo metadata subido a gs://{bucket_name}/metadata/{os.path.basename(metadata_filename_ts)}")
        try:
            os.remove(metadata_filename_ts)
        except Exception:
            pass

        print(f"\n📊 Resumen Extracción {company_name}:")
        print(f"   🎵 Audios descargados (status=0) : {audios_guardados}")
        print(f"   ⚠️  Sin audio (status=-2)         : {sin_audio_count}")
        print(f"   ❌ Errores (status=-1)           : {errores_count}")
        print(f"   📝 Registros guardados en BQ     : {len(metadata_records)}")


# =============================================================================
# OBTENCIÓN DE COMPAÑÍAS ACTIVAS
# =============================================================================

def fetch_active_companies(client, project_id, company_id=0):
    """
    Obtiene la lista de compañías activas desde settings.companies.
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
# MODOS DE EJECUCIÓN
# =============================================================================

def run_all(args):
    """Modo ALL: Procesa compañías del consorcio (pph-central)."""
    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
    task_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))
    is_parallel = task_count > 1

    print(f"🔍 Proyecto detectado: {PROJECT_SOURCE} | Project ID queries: {PROJECT_ALL}")
    client = bigquery.Client()
    results = fetch_active_companies(client, PROJECT_ALL, args.company_id)
    total = len(results)

    if not results:
        print(f"❌ No se encontraron compañías activas para company_id={args.company_id} en {PROJECT_ALL}")
        return

    if is_parallel and len(results) > 1:
        results = get_balanced_tasks(client, results, task_count, task_index)
        total_assigned = len(results)
    else:
        total_assigned = total
        print(f"📊 Total compañías a procesar: {total}")

    procesadas = 0
    for idx, row in enumerate(results, 1):
        try:
            if is_parallel:
                print(f"\n[{idx}/{total_assigned}] Procesando compañía: {row.company_name} (ID: {row.company_id})")
            process_company(row, limit=args.limit)
            procesadas += 1
        except Exception as e:
            print(f"❌ Error procesando compañía {row.company_name} (ID: {row.company_id}): {str(e)}")

    print(f"\n{'='*80}")
    print(f"🏁 Resumen: {procesadas}/{total_assigned} compañías procesadas exitosamente.")


def run_inbox(args):
    """Modo INBOX: Procesa compañías candidatas desde pph-inbox."""
    client = bigquery.Client(project=PROJECT_INBOX)
    results = fetch_active_companies(client, PROJECT_INBOX, args.company_id)

    if not results:
        print(f"❌ No se encontró ninguna compañía INBOX activa (company_id={args.company_id}).")
        return

    total = len(results)
    print(f"📊 Se encontraron {total} compañía(s) INBOX activa(s)\n")

    for idx, row in enumerate(results, 1):
        try:
            print(f"📊 Procesando compañía {idx} de {total}: {row.company_name} (ID: {row.company_id})")
            process_company(row, limit=args.limit)
        except Exception as e:
            print(f"❌ Error procesando compañía INBOX {row.company_name} (ID: {row.company_id}): {str(e)}")


def run_test(args):
    """Modo TEST: Ejecución manual con filtros y soporte para dry-run."""
    print(f"\n{'='*80}")
    print("🧪 MODO TEST: ServiceTitan Telecom API → Audio")
    print(f"{'='*80}")
    if args.company_id and args.company_id > 0:
        print(f"📋 Compañía ID: {args.company_id}")
    else:
        print("📋 Compañía ID: TODAS las activas (0)")
    if args.limit:
        print(f"📋 Límite de llamadas por compañía: {args.limit}")
    print(f"📋 Dry-run: {'SÍ' if args.dry_run else 'NO'}")
    print(f"🔍 Proyecto detectado: {PROJECT_SOURCE} | Project ID queries: {PROJECT_ALL}")
    print(f"{'='*80}\n")

    client = bigquery.Client(project=PROJECT_ALL)
    results = fetch_active_companies(client, PROJECT_ALL, args.company_id)

    if not results:
        print(f"❌ No se encontraron compañías activas (company_id={args.company_id})")
        return

    total = len(results)
    procesadas = 0
    failed = 0

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
# CLI & ENTRYPOINT
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL: ServiceTitan Telecom API → Audio (Cloud Storage & BigQuery)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["all", "inbox", "test"],
        default=None,
        help="Modo de ejecución: 'all', 'inbox', 'test'.",
    )
    parser.add_argument(
        "--company-id", "-c",
        type=int,
        default=0,
        help="ID de la compañía a procesar (ej: 1). Si es 0, procesa todas las activas.",
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="(Solo modo test) Límite de llamadas a procesar por compañía.",
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
    mode = args.mode or os.environ.get("ETL_MODE", "").lower() or "test"

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
