"""
Script principal para el Job audio2bq-job (Procesamiento de Audios con Vertex AI Gemini 2.5 Flash -> BigQuery).

Lee los registros pendientes de {company_project}.bronze.call_recordings (status = 0),
envía el audio a Vertex AI (Gemini 2.5 Flash) para transcripción y extracción de features para ML,
guarda los resultados en {company_project}.silver.tb_call_recordings,
y actualiza el estado en {company_project}.bronze.call_recordings (status = 1 o -3).

Uso:
    python main.py --mode test --company-id 1 --limit 5
    python main.py --mode all --batch-size 25
    python main.py --mode all --company-id 1 --limit 100
"""

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
)


def parse_arguments():
    parser = argparse.ArgumentParser(description="ETL Audio to BigQuery (audio2bq-job) con Vertex AI Gemini 2.5 Flash")
    parser.add_argument("--mode", choices=["all", "inbox", "test"], default="test",
                        help="Modo de ejecución: all (todas las empresas), inbox (empresas activas), test (pruebas)")
    parser.add_argument("--company-id", type=int, default=None,
                        help="ID de empresa específica a procesar (opcional)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Límite máximo de audios a procesar por empresa (ej. 5 para test)")
    parser.add_argument("--batch-size", type=int, default=25,
                        help="Tamaño de lote para hacer flush a BigQuery (default: 25)")
    parser.add_argument("--location", type=str, default="us-central1",
                        help="Región de Vertex AI (default: us-central1)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Modo simulación: consulta registros pero no llama a la IA ni modifica BigQuery")
    return parser.parse_args()


def get_target_companies(bq_client: bigquery.Client, project_source: str, mode: str, company_id: int = None):
    """
    Obtiene la lista de empresas objetivo desde settings.companies en project_source.
    """
    table_ref = f"`{project_source}.settings.companies`"
    if mode == "inbox":
        table_ref = "`pph-inbox.settings.companies`"
        
    query = f"""
    SELECT * FROM {table_ref}
    WHERE (company_fivetran_status = TRUE OR company_fivetran_status IS NULL)
    """
    if company_id:
        query += f" AND company_id = {company_id}"
        
    query += " ORDER BY company_id ASC"
    
    rows = bq_client.query(query).result()
    companies = []
    for row in rows:
        r = dict(row)
        companies.append({
            "company_id": r.get("company_id"),
            "company_name": r.get("company_name", f"Company {r.get('company_id')}"),
            "tenant_id": r.get("company_tenant_id") or r.get("tenant_id"),
            "project_id": r.get("company_project_id") or r.get("project_id")
        })
    return [c for c in companies if c["project_id"]]


def format_duration(seconds: float) -> str:
    mins, secs = divmod(int(seconds), 60)
    hrs, mins = divmod(mins, 60)
    if hrs > 0:
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"


def process_company_audios(
    company: dict,
    bq_client: bigquery.Client,
    model,
    limit: int = None,
    batch_size: int = 25,
    dry_run: bool = False
):
    company_id = company["company_id"]
    company_name = company["company_name"]
    target_project = company["project_id"]
    
    print(f"\n" + "=" * 80, flush=True)
    print(f"🏢 Procesando Empresa #{company_id}: {company_name} (Proyecto: {target_project})", flush=True)
    print("=" * 80, flush=True)
    
    # 1. Asegurar tabla silver.tb_call_recordings
    if not dry_run:
        ensure_silver_call_recordings_table_exists(bq_client, target_project)
        
    # 2. Consultar registros candidatos en bronze.call_recordings (status = 0)
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
        print(f"🔍 [DRY-RUN] Modo simulación activado. Registros encontrados: {total_candidates}. Fin.", flush=True)
        return
        
    start_time = time.time()
    processed_count = 0
    success_count = 0
    error_count = 0
    total_tokens_in = 0
    total_tokens_out = 0
    
    success_batch = []
    failed_batch = []
    
    for row in candidates:
        r_dict = dict(row)
        lead_call_id = r_dict["lead_call_id"]
        gcs_uri = r_dict["gcs_uri"]
        
        # Mapeo flexible de IDs (pre y post script de reparación):
        if "call_id" in r_dict:
            rec_id = r_dict.get("id")
            call_id = r_dict.get("call_id")
        else:
            rec_id = None
            call_id = r_dict.get("id")
        
        proc_start = time.time()
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
            booked_str = f"Err: {str(e)[:40]}"
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


def main():
    args = parse_arguments()
    project_source = get_project_source()
    bq_project_id = get_bigquery_project_id()
    
    print("=" * 80, flush=True)
    print("🚀 INICIANDO AUDIO2BQ-JOB (Vertex AI Gemini 2.5 Flash -> BigQuery Silver)", flush=True)
    print(f"   • Fuente Metadata: {project_source}", flush=True)
    print(f"   • Proyecto BigQuery: {bq_project_id}", flush=True)
    print(f"   • Modo: {args.mode}", flush=True)
    print(f"   • Company ID: {args.company_id if args.company_id else 'Todas'}", flush=True)
    print(f"   • Límite por empresa: {args.limit if args.limit else 'Sin límite'}", flush=True)
    print(f"   • Batch size: {args.batch_size}", flush=True)
    print(f"   • Vertex Location: {args.location}", flush=True)
    print(f"   • Dry Run: {args.dry_run}", flush=True)
    print("=" * 80, flush=True)
    
    bq_client = bigquery.Client()
    
    # Obtener empresas a procesar
    companies = get_target_companies(bq_client, project_source, args.mode, args.company_id)
    if not companies:
        print("⚠️ No se encontraron empresas activas para procesar.", flush=True)
        return
        
    print(f"📋 Empresas encontradas para procesar: {len(companies)}", flush=True)
    
    for company in companies:
        target_project = company["project_id"]
        print(f"🤖 Inicializando Vertex AI Gemini 2.5 Flash en proyecto: {target_project} ({args.location})...", flush=True)
        model = init_vertex_ai(project_id=target_project, location=args.location)
        
        process_company_audios(
            company=company,
            bq_client=bq_client,
            model=model,
            limit=args.limit,
            batch_size=args.batch_size,
            dry_run=args.dry_run
        )
        
    print("✨ audio2bq-job finalizado exitosamente.", flush=True)


if __name__ == "__main__":
    main()
