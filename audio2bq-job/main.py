"""
ETL Job: Audio (Cloud Storage) → Vertex AI Gemini 2.5 Flash → BigQuery Silver
=============================================================================
Script unificado estandarizado con la arquitectura de json2bq-job y st2audio-job:
  - Modo ALL   (--mode all)   → Cloud Run Job consorcio (pph-central / balanceado por tareas)
  - Modo INBOX (--mode inbox) → Cloud Run Job candidatos (pph-inbox)
  - Modo TEST  (--mode test)  → Ejecución manual / Cloud Shell

Uso en Cloud Shell / Local (modo TEST):
    python main.py --mode test --company-id 1 --limit 1000 --batch-size 25
    python main.py --mode test --company-id 1 --limit 10 --dry-run
    python main.py --mode test

El modo también puede establecerse con la variable de entorno ETL_MODE:
    ETL_MODE=all   → build_deploy.sh
    ETL_MODE=inbox → build_deploy_inbox.sh
"""

import os
import sys
import time
import argparse
from datetime import datetime, timezone
from google.cloud import bigquery

# Garantizar compatibilidad de encoding en Windows console (emojis / unicode)
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr and hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

from servicetitan_common import (
    get_project_source,
    get_bigquery_project_id,
    init_vertex_ai,
    process_audio_with_gemini,
    ensure_silver_call_recordings_table_exists,
    flush_silver_records_and_update_bronze,
    get_balanced_tasks,
)

# =============================================================================
# CONFIGURACIÓN DE PROYECTOS Y TABLAS MAESTRAS
# =============================================================================

# Proyecto central (ALL / TEST): se lee dinámicamente del ambiente activo
PROJECT_ALL = get_bigquery_project_id()
PROJECT_SOURCE = get_project_source()

# Proyecto INBOX: siempre fijo, es su propio proyecto GCP
PROJECT_INBOX = "pph-inbox"

# Tablas maestras (iguales para ambos modos)
DATASET_COMPANIES = "settings"
TABLE_COMPANIES = "companies"


# =============================================================================
# CONSULTA DE COMPAÑÍAS ACTIVAS
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


def format_duration(seconds: float) -> str:
    mins, secs = divmod(int(seconds), 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"


# =============================================================================
# NÚCLEO: process_company()
# =============================================================================

def process_company(
    row,
    bq_client: bigquery.Client,
    limit: int = None,
    batch_size: int = 25,
    location: str = "us-central1",
    dry_run: bool = False
):
    """
    Procesa todos los audios pendientes (bronze.call_recordings status=0) de una compañía:
    llama a Vertex AI Gemini 2.5 Flash, inserta en silver.tb_call_recordings y actualiza status en bronze.
    """
    company_id = row.company_id
    company_name = row.company_name
    target_project = row.company_project_id
    
    print(f"\n" + "=" * 80, flush=True)
    print(f"🏢 Procesando Empresa #{company_id}: {company_name} | Proyecto: {target_project}", flush=True)
    if dry_run:
        print("🔍 MODO DRY-RUN: Solo mostrando registros, sin llamar a Vertex AI ni BigQuery.", flush=True)
    print("=" * 80, flush=True)
    
    if not target_project:
        print(f"⚠️ company_project_id vacío para company_id={company_id}. Saltando.", flush=True)
        return
        
    # 1. Asegurar tabla silver.tb_call_recordings
    if not dry_run:
        ensure_silver_call_recordings_table_exists(bq_client, target_project)
        
    # 2. Inicializar modelo Vertex AI apuntando al proyecto donde vive el bucket de audio
    model = None
    if not dry_run:
        print(f"🤖 Inicializando Vertex AI Gemini 2.5 Flash en {target_project} ({location})...", flush=True)
        model = init_vertex_ai(project_id=target_project, location=location)
        
    # 3. Consultar registros candidatos en bronze.call_recordings (status = 0)
    limit_clause = f"LIMIT {limit}" if limit else ""
    query_candidates = f"""
    SELECT *
    FROM `{target_project}.bronze.call_recordings`
    WHERE status = 0
      AND gcs_uri IS NOT NULL
    ORDER BY lead_call_id ASC
    {limit_clause}
    """
    
    try:
        query_job = bq_client.query(query_candidates)
        candidates = list(query_job.result())
    except Exception as e:
        print(f"❌ Error al consultar candidatos en {target_project}.bronze.call_recordings: {e}", flush=True)
        return
        
    total_candidates = len(candidates)
    if total_candidates == 0:
        print(f"✨ No hay audios pendientes de procesar (status = 0) en {target_project}.", flush=True)
        return
        
    print(f"🎯 Total audios pendientes a procesar con IA: {total_candidates}", flush=True)
    if dry_run:
        print(f"🔍 [DRY-RUN] Simulación completa. Registros encontrados: {total_candidates}.", flush=True)
        return
        
    start_time = time.time()
    processed_count = 0
    success_count = 0
    error_count = 0
    total_tokens_in = 0
    total_tokens_out = 0
    
    success_batch = []
    failed_batch = []
    
    for row_cand in candidates:
        r_dict = dict(row_cand)
        lead_call_id = r_dict["lead_call_id"]
        gcs_uri = r_dict["gcs_uri"]
        
        # Mapeo flexible de IDs (pre y post script de reparación):
        if "call_id" in r_dict:
            rec_id = r_dict.get("id")
            call_id = r_dict.get("call_id")
        else:
            rec_id = None
            call_id = r_dict.get("id")
        
        try:
            # Procesar con Vertex AI Gemini 2.5 Flash
            ai_result = process_audio_with_gemini(model, gcs_uri)
            
            record = {
                "id": rec_id,
                "lead_call_id": lead_call_id,
                "call_id": call_id,
                "transcription": ai_result["transcription"],
                "summary": ai_result["summary"],
                "call_outcome": ai_result["call_outcome"],
                "lost_reason_category": ai_result["lost_reason_category"],
                "appointment_booked": ai_result["appointment_booked"],
                "price_resistance_detected": ai_result["price_resistance_detected"],
                "competitor_mentioned": ai_result["competitor_mentioned"],
                "customer_sentiment": ai_result["customer_sentiment"],
                "customer_sentiment_score": ai_result["customer_sentiment_score"],
                "urgency_level": ai_result["urgency_level"],
                "service_requested_category": ai_result["service_requested_category"],
                "csr_handling_score": ai_result["csr_handling_score"],
                "key_issues": ai_result["key_issues"],
                "tokens_input": ai_result["tokens_input"],
                "tokens_output": ai_result["tokens_output"],
                "model_version": "gemini-2.5-flash",
                "transcribed_at": datetime.now(timezone.utc).isoformat()
            }
            success_batch.append(record)
            success_count += 1
            total_tokens_in += ai_result["tokens_input"]
            total_tokens_out += ai_result["tokens_output"]
            
            outcome_str = f"[{ai_result['call_outcome']}]"
            booked_str = "📅 Booked: Yes" if ai_result["appointment_booked"] else "❌ Booked: No"
            sentiment_str = f"😊 Sent: {ai_result['customer_sentiment']}"
            
        except Exception as e:
            error_count += 1
            failed_batch.append({
                "lead_call_id": lead_call_id,
                "error_message": str(e)
            })
            outcome_str = "❌ AI_ERROR"
            booked_str = f"Err: {str(e)[:35]}"
            sentiment_str = ""
            
        processed_count += 1
        elapsed = time.time() - start_time
        speed = processed_count / elapsed if elapsed > 0 else 0
        eta_seconds = (total_candidates - processed_count) / speed if speed > 0 else 0
        percent = (processed_count / total_candidates) * 100
        
        # Log en tiempo real
        print(
            f"[{processed_count}/{total_candidates}] ({percent:5.1f}%) "
            f"Call: {lead_call_id} | {outcome_str:18} | {booked_str:15} | {sentiment_str:18} | "
            f"⚡ {speed:4.2f} aud/s | ⏳ ETA: {format_duration(eta_seconds)}",
            flush=True
        )
        
        # Flush por lotes si alcanzamos el batch_size
        if (len(success_batch) + len(failed_batch)) >= batch_size:
            print(f"   💾 Realizando flush a BigQuery ({len(success_batch)} exitosos, {len(failed_batch)} fallidos)...", flush=True)
            flush_silver_records_and_update_bronze(bq_client, target_project, success_batch, failed_batch)
            success_batch.clear()
            failed_batch.clear()
            
    # Flush final de remanentes
    if success_batch or failed_batch:
        print(f"   💾 Flush final a BigQuery ({len(success_batch)} exitosos, {len(failed_batch)} fallidos)...", flush=True)
        flush_silver_records_and_update_bronze(bq_client, target_project, success_batch, failed_batch)
        success_batch.clear()
        failed_batch.clear()
        
    total_elapsed = time.time() - start_time
    avg_speed = processed_count / total_elapsed if total_elapsed > 0 else 0
    print("\n" + "-" * 80, flush=True)
    print(f"🏁 RESUMEN PROCESAMIENTO IA - EMPRESA #{company_id} ({company_name}):", flush=True)
    print(f"   • Total procesados: {processed_count}/{total_candidates}", flush=True)
    print(f"   • Exitosos (status 1): {success_count}", flush=True)
    print(f"   • Errores (status -3): {error_count}", flush=True)
    print(f"   • Tokens In / Out: {total_tokens_in:,} / {total_tokens_out:,}", flush=True)
    print(f"   • Tiempo total: {format_duration(total_elapsed)} ({avg_speed:4.2f} audios/seg)", flush=True)
    print("-" * 80 + "\n", flush=True)


# =============================================================================
# MODOS DE EJECUCIÓN
# =============================================================================

def run_all(args):
    """Modo ALL: Procesa compañías del consorcio (pph-central) con balanceo si hay paralelismo."""
    task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
    task_count = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))
    is_parallel = task_count > 1

    print(f"🔍 Proyecto detectado: {PROJECT_SOURCE} | Project ID queries: {PROJECT_ALL}")
    client = bigquery.Client(project=PROJECT_ALL)
    results = fetch_active_companies(client, PROJECT_ALL, args.company_id)
    total = len(results)

    if not results:
        print(f"❌ No se encontraron compañías activas en {PROJECT_ALL}")
        return

    if is_parallel and len(results) > 1:
        results = get_balanced_tasks(client, results, task_count, task_index)
        total_assigned = len(results)
    else:
        total_assigned = total

    task_info = f" (Tarea {task_index+1}/{task_count} — {total_assigned} asignadas)" if is_parallel else ""
    print(f"\n📊 Total compañías a procesar: {total_assigned}{task_info}\n")

    for idx, row in enumerate(results, 1):
        try:
            process_company(
                row=row,
                bq_client=client,
                limit=args.limit,
                batch_size=args.batch_size,
                location=args.location,
                dry_run=args.dry_run
            )
        except Exception as e:
            print(f"❌ Error procesando {row.company_name} (ID: {row.company_id}): {e}")


def run_inbox(args):
    """Modo INBOX: Procesa compañías candidatas desde pph-inbox.settings.companies."""
    print(f"\nConectando a BigQuery ({PROJECT_INBOX})...")
    client = bigquery.Client(project=PROJECT_INBOX)
    results = fetch_active_companies(client, PROJECT_INBOX, args.company_id)

    if not results:
        print("❌ No se encontraron compañías INBOX activas.")
        return

    total = len(results)
    print(f"📊 Compañías INBOX activas: {total}\n")

    for idx, row in enumerate(results, 1):
        print(f"📊 INBOX: compañía {idx}/{total}")
        try:
            process_company(
                row=row,
                bq_client=client,
                limit=args.limit,
                batch_size=args.batch_size,
                location=args.location,
                dry_run=args.dry_run
            )
        except Exception as e:
            print(f"❌ Error procesando INBOX {row.company_name} (ID: {row.company_id}): {e}")


def run_test(args):
    """Modo TEST: Ejecución manual desde Cloud Shell o local."""
    print(f"\n{'='*80}")
    print("🧪 MODO TEST: Audio -> Vertex AI Gemini 2.5 Flash -> BigQuery Silver")
    print(f"{'='*80}")
    print(f"📋 Proyecto de contexto: {PROJECT_ALL}")
    print(f"📋 Compañía ID: {args.company_id if args.company_id else 'TODAS las activas'}")
    print(f"📋 Límite por empresa: {args.limit if args.limit else 'Sin límite'}")
    print(f"📋 Batch size: {args.batch_size}")
    print(f"📋 Dry-run: {'SÍ' if args.dry_run else 'NO'}")
    print(f"{'='*80}\n")

    client = bigquery.Client(project=PROJECT_ALL)
    results = fetch_active_companies(client, PROJECT_ALL, args.company_id)

    if not results:
        print(f"❌ No se encontraron compañías activas en {PROJECT_ALL}.")
        return

    print(f"📊 Compañías activas encontradas: {len(results)}\n")
    for row in results:
        process_company(
            row=row,
            bq_client=client,
            limit=args.limit,
            batch_size=args.batch_size,
            location=args.location,
            dry_run=args.dry_run
        )


# =============================================================================
# MAIN / CLI PARSER
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="ETL: Audio (GCS) -> Vertex AI Gemini 2.5 Flash -> BigQuery Silver",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["all", "inbox", "test"],
        default=None,
        help="Modo de ejecución: 'all', 'inbox', 'test'. Si no se indica, se usa la variable ETL_MODE.",
    )
    parser.add_argument(
        "--company-id", "-c",
        type=int,
        default=None,
        help="ID de la compañía a procesar (opcional).",
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Límite máximo de audios a procesar por empresa.",
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=25,
        help="Tamaño de lote para hacer flush a BigQuery (default: 25).",
    )
    parser.add_argument(
        "--location",
        type=str,
        default="us-central1",
        help="Región de Vertex AI (default: us-central1).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Muestra qué registros se procesarían sin llamar a Vertex AI ni modificar BigQuery.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    mode = args.mode or os.environ.get("ETL_MODE", "").lower() or "test"

    if mode not in ("all", "inbox", "test"):
        print(f"❌ Modo inválido: '{mode}'. Debe ser 'all', 'inbox' o 'test'.")
        raise SystemExit(1)

    print(f"\n{'='*80}")
    print(f"🚀 ETL audio2bq | MODO: {mode.upper()}")
    print(f"{'='*80}\n")

    if mode == "all":
        run_all(args)
    elif mode == "inbox":
        run_inbox(args)
    elif mode == "test":
        run_test(args)


if __name__ == "__main__":
    main()
