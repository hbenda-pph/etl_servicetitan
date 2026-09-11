"""
Módulo común con funciones compartidas para el ETL de Procesamiento de Audios con Vertex AI y BigQuery (audio2bq-job).
Se encarga de:
1. Conectar con Vertex AI (Gemini 2.5 Flash) para transcripción y extracción de features para ML.
2. Crear y mantener la tabla {project_id}.silver.tb_call_recordings (con partición y clusterización optimizada).
3. Actualizar el estado de procesamiento en {project_id}.bronze.call_recordings.
"""

import os
import time
import json
import logging
import warnings
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

from google.cloud import bigquery, storage
import vertexai
from vertexai.generative_models import GenerativeModel, Part, GenerationConfig

# Suprimir advertencias y configurar logging
logging.getLogger("google.auth").setLevel(logging.ERROR)
logging.getLogger("google.auth.transport").setLevel(logging.ERROR)

warnings.filterwarnings("ignore", message=".*quota project.*", category=UserWarning)
warnings.filterwarnings("ignore", message=".*end user credentials.*", category=UserWarning)


# Configuración central de metadata
METADATA_PROJECT = "pph-central"
METADATA_DATASET = "management"
METADATA_TABLE = "metadata_consolidated_tables"


def get_project_source() -> str:
    """
    Obtiene el proyecto del ambiente actual para consultas maestras (settings.companies).
    Prioridad:
    1. Variable de entorno GCP_PROJECT (establecida por Cloud Run Jobs)
    2. Variable de entorno GOOGLE_CLOUD_PROJECT
    3. Proyecto por defecto del cliente BigQuery (si es un proyecto central)
    4. Fallback 'pph-central'
    """
    project = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if project:
        if project not in ("platform-partners-pro", "constant-height-455614-i0", "platform-partners-qua", "platform-partners-des", "pph-central", "pph-inbox"):
            return "pph-central"
        return project
    
    try:
        import subprocess
        res = subprocess.run(["gcloud", "config", "get-value", "project"], capture_output=True, text=True, timeout=3)
        if res.returncode == 0 and res.stdout.strip():
            proj = res.stdout.strip()
            if proj in ("platform-partners-pro", "constant-height-455614-i0", "platform-partners-qua", "platform-partners-des", "pph-central", "pph-inbox"):
                return proj
    except Exception:
        pass
    
    return "pph-central"


def get_bigquery_project_id() -> str:
    """
    Obtiene el project_id real para usar en queries SQL.
    En PRO, GCP_PROJECT contiene 'platform-partners-pro',
    pero se usa 'constant-height-455614-i0' (project_id) en las queries.
    """
    project_source = get_project_source()
    if project_source == "platform-partners-pro":
        return "constant-height-455614-i0"
    return project_source


def get_balanced_tasks(bq_client, results, task_count, task_index):
    """
    Distribuye las compañías entre las tareas de Cloud Run usando un algoritmo
    Greedy para balancear la carga basada en métricas históricas de duración.
    """
    if task_count <= 1:
        return results

    weights = {}
    try:
        query = f"""
            SELECT company_id, SUM(actual_duration) as total_duration
            FROM `{METADATA_PROJECT}.management.etl_monitoring_snapshot`
            WHERE updated_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
            GROUP BY company_id
        """
        query_job = bq_client.query(query)
        for row in query_job.result():
            weights[int(row.company_id)] = float(row.total_duration)
    except Exception as e:
        print(f"⚠️  [get_balanced_tasks] No se pudieron obtener pesos históricos: {str(e)[:100]}")

    fallback_weight = sum(weights.values()) / len(weights) if weights else 60.0
    
    company_data = []
    for row in results:
        cid = int(row.company_id)
        w = weights.get(cid, fallback_weight)
        company_data.append({'row': row, 'weight': w})

    company_data.sort(key=lambda x: x['weight'], reverse=True)
    
    bins = [[] for _ in range(task_count)]
    bin_weights = [0.0] * task_count
    
    for item in company_data:
        min_bin_idx = bin_weights.index(min(bin_weights))
        bins[min_bin_idx].append(item['row'])
        bin_weights[min_bin_idx] += item['weight']
    
    assigned_companies = bins[task_index]
    assigned_companies.sort(key=lambda x: x.company_id)
    return assigned_companies


def init_vertex_ai(project_id: str, location: str = "us-central1") -> GenerativeModel:
    """
    Inicializa el cliente de Vertex AI y retorna el modelo GenerativeModel configurado.
    """
    vertexai.init(project=project_id, location=location)
    
    # System instruction para análisis de llamadas de ServiceTitan
    system_instruction = (
        "You are an expert conversational AI and data extraction assistant for home service companies (HVAC, Plumbing, Electrical). "
        "Your task is to analyze audio call recordings between Customer Service Representatives (Agents) and Customers. "
        "You must return the analysis strictly as a valid JSON object matching the requested schema."
    )
    
    model = GenerativeModel(
        model_name="gemini-2.5-flash",
        system_instruction=[system_instruction]
    )
    return model


ANALYSIS_PROMPT = """Analyze this audio recording of a customer service call and provide a detailed analysis in JSON format.

JSON Schema requirements:
{
  "transcription": "Verbatim transcription in English. Clearly format dialogue with 'Agent: <text>' and 'Customer: <text>' separated by newlines.",
  "summary": "Concise summary (2-3 sentences) of what occurred during the call.",
  "call_outcome": "One of: 'Booked', 'Lost Opportunity', 'Inquiry Only', 'Follow-up Existing Job', 'Vendor/Spam', 'Unclear'",
  "lost_reason_category": "If call_outcome is 'Lost Opportunity', categorize reason as one of: 'Price Objection', 'No Availability', 'Out of Service Area', 'Competitor Shopping', 'Customer Hesitation', 'Emergency Not Handled', 'Other'. If not lost, put 'None'.",
  "appointment_booked": boolean (true if an appointment/job/estimate was confirmed and scheduled, false otherwise),
  "price_resistance_detected": boolean (true if customer expressed hesitation, objection, or asked repeatedly about dispatch fee or pricing),
  "competitor_mentioned": boolean (true if customer mentioned getting other quotes or named a competitor),
  "customer_sentiment": "One of: 'Positive', 'Neutral', 'Negative', 'Frustrated'",
  "customer_sentiment_score": float between -1.0 (extremely negative/angry) and 1.0 (extremely positive/satisfied),
  "urgency_level": "One of: 'Emergency', 'High', 'Medium', 'Low'",
  "service_requested_category": "One of: 'HVAC Repair', 'HVAC Replacement/Install', 'Plumbing Emergency', 'Plumbing General', 'Electrical', 'Maintenance/Tune-up', 'Water Heater', 'Other'",
  "csr_handling_score": integer from 1 (poor handling/dismissive) to 5 (excellent customer service and booking attempt),
  "key_issues": ["array", "of", "key", "topics", "or", "objections", "mentioned"]
}

Output ONLY valid JSON. Do not include markdown code fence formatting like ```json.
"""


def process_audio_with_gemini(model: GenerativeModel, gcs_uri: str) -> Dict[str, Any]:
    """
    Envía el audio desde GCS a Gemini 2.5 Flash y obtiene la transcripción + features para ML.
    """
    audio_part = Part.from_uri(uri=gcs_uri, mime_type="audio/mpeg")
    
    config = GenerationConfig(
        response_mime_type="application/json",
        temperature=0.2,
    )
    
    start_time = time.time()
    response = model.generate_content(
        [audio_part, ANALYSIS_PROMPT],
        generation_config=config
    )
    elapsed_time = time.time() - start_time
    
    response_text = response.text.strip()
    # Limpiar posibles delimitadores markdown si el modelo los devuelve
    if response_text.startswith("```json"):
        response_text = response_text[7:]
    if response_text.startswith("```"):
        response_text = response_text[3:]
    if response_text.endswith("```"):
        response_text = response_text[:-3]
    response_text = response_text.strip()
    
    parsed = json.loads(response_text)
    
    # Extraer métricas de tokens si están disponibles
    tokens_input = 0
    tokens_output = 0
    if hasattr(response, 'usage_metadata') and response.usage_metadata:
        tokens_input = getattr(response.usage_metadata, 'prompt_token_count', 0) or 0
        tokens_output = getattr(response.usage_metadata, 'candidates_token_count', 0) or 0
    
    return {
        "transcription": parsed.get("transcription", ""),
        "summary": parsed.get("summary", ""),
        "call_outcome": parsed.get("call_outcome", "Unclear"),
        "lost_reason_category": parsed.get("lost_reason_category", "None"),
        "appointment_booked": bool(parsed.get("appointment_booked", False)),
        "price_resistance_detected": bool(parsed.get("price_resistance_detected", False)),
        "competitor_mentioned": bool(parsed.get("competitor_mentioned", False)),
        "customer_sentiment": parsed.get("customer_sentiment", "Neutral"),
        "customer_sentiment_score": float(parsed.get("customer_sentiment_score", 0.0)),
        "urgency_level": parsed.get("urgency_level", "Medium"),
        "service_requested_category": parsed.get("service_requested_category", "Other"),
        "csr_handling_score": int(parsed.get("csr_handling_score", 3)),
        "key_issues": parsed.get("key_issues", []) if isinstance(parsed.get("key_issues"), list) else [],
        "tokens_input": tokens_input,
        "tokens_output": tokens_output,
        "elapsed_seconds": elapsed_time
    }


def ensure_silver_call_recordings_table_exists(bq_client: bigquery.Client, target_project_id: str) -> str:
    """
    Crea la tabla {target_project_id}.silver.tb_call_recordings si no existe,
    con partición mensual por _etl_synced y clusterización por lead_call_id, call_outcome, lost_reason_category, appointment_booked.
    """
    dataset_ref = f"{target_project_id}.silver"
    table_id = f"{dataset_ref}.tb_call_recordings"
    
    # Asegurar dataset silver
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = "US"
        bq_client.create_dataset(ds, exists_ok=True)
        print(f"📦 Dataset creado: {dataset_ref}")
        
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
        call_recording_id INT64 OPTIONS(description="ID propio incremental de la grabación (referencia a bronze.call_recordings.id)"),
        lead_call_id INT64 OPTIONS(description="Identificador principal de la llamada en Telecom"),
        call_id INT64 OPTIONS(description="ID de la llamada en la tabla calls de ServiceTitan"),
        
        -- Transcripción y Resumen
        transcription STRING OPTIONS(description="Transcripción completa y diarizada (Agent / Customer)"),
        summary STRING OPTIONS(description="Resumen de la conversación generado por Gemini"),
        
        -- Features para Machine Learning (Clasificación de Pérdida / Conversión)
        call_outcome STRING OPTIONS(description="Resultado de la llamada: Booked, Lost Opportunity, Inquiry, Follow-up, Vendor/Spam"),
        lost_reason_category STRING OPTIONS(description="Categoría del motivo de pérdida: Price Objection, No Availability, Competitor, etc."),
        appointment_booked BOOL OPTIONS(description="Indica si la llamada resultó en cita agendada"),
        price_resistance_detected BOOL OPTIONS(description="Indica si el cliente mostró resistencia al precio"),
        competitor_mentioned BOOL OPTIONS(description="Indica si el cliente mencionó competencia"),
        customer_sentiment STRING OPTIONS(description="Sentimiento general: Positive, Neutral, Negative, Frustrated"),
        customer_sentiment_score FLOAT64 OPTIONS(description="Puntaje de sentimiento (-1.0 a 1.0)"),
        urgency_level STRING OPTIONS(description="Nivel de urgencia detectado: Emergency, High, Medium, Low"),
        service_requested_category STRING OPTIONS(description="Categoría del servicio solicitado (HVAC, Plumbing, Electrical, etc.)"),
        csr_handling_score INT64 OPTIONS(description="Evaluación de la atención del agente (1 a 5)"),
        key_issues ARRAY<STRING> OPTIONS(description="Lista de temas clave u objeciones mencionadas"),
        
        -- Metadatos técnicos
        tokens_input INT64 OPTIONS(description="Tokens de entrada procesados por Vertex AI"),
        tokens_output INT64 OPTIONS(description="Tokens de salida generados por Vertex AI"),
        model_version STRING OPTIONS(description="Versión del modelo de IA utilizado"),
        transcribed_at TIMESTAMP OPTIONS(description="Timestamp exacto de la transcripción"),
        _etl_synced TIMESTAMP OPTIONS(description="Timestamp de sincronización ETL"),
        _etl_operation STRING OPTIONS(description="Tipo de operación ETL: INSERT / UPDATE")
    )
    PARTITION BY TIMESTAMP_TRUNC(_etl_synced, MONTH)
    CLUSTER BY lead_call_id, call_outcome, lost_reason_category, appointment_booked;
    """
    
    query_job = bq_client.query(ddl)
    query_job.result()
    return table_id


def flush_silver_records_and_update_bronze(
    bq_client: bigquery.Client,
    target_project_id: str,
    success_records: List[Dict[str, Any]],
    failed_records: List[Dict[str, Any]]
) -> None:
    """
    Inserta/actualiza los registros procesados en silver.tb_call_recordings
    y actualiza el estado en bronze.call_recordings (status = 1 para éxito, status = -3 para error).
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    
    # 1. Inserción en silver.tb_call_recordings para los registros exitosos
    if success_records:
        silver_table = f"{target_project_id}.silver.tb_call_recordings"
        
        rows_to_insert = []
        for r in success_records:
            rows_to_insert.append({
                "call_recording_id": r.get("call_recording_id"),
                "lead_call_id": r["lead_call_id"],
                "call_id": r.get("call_id"),
                "transcription": r["transcription"],
                "summary": r["summary"],
                "call_outcome": r["call_outcome"],
                "lost_reason_category": r["lost_reason_category"],
                "appointment_booked": r["appointment_booked"],
                "price_resistance_detected": r["price_resistance_detected"],
                "competitor_mentioned": r["competitor_mentioned"],
                "customer_sentiment": r["customer_sentiment"],
                "customer_sentiment_score": r["customer_sentiment_score"],
                "urgency_level": r["urgency_level"],
                "service_requested_category": r["service_requested_category"],
                "csr_handling_score": r["csr_handling_score"],
                "key_issues": r["key_issues"],
                "tokens_input": r["tokens_input"],
                "tokens_output": r["tokens_output"],
                "model_version": r.get("model_version", "gemini-2.5-flash"),
                "transcribed_at": r.get("transcribed_at", now_iso),
                "_etl_synced": now_iso,
                "_etl_operation": "INSERT"
            })
            
        errors = bq_client.insert_rows_json(silver_table, rows_to_insert)
        if errors:
            print(f"⚠️ Error al insertar filas en {silver_table}: {errors[:2]}")
            
    # 2. Actualizar status en bronze.call_recordings
    # Status 1 = Transcrito exitosamente
    if success_records:
        success_ids = [r["lead_call_id"] for r in success_records]
        update_success_sql = f"""
        UPDATE `{target_project_id}.bronze.call_recordings`
        SET status = 1,
            error_message = NULL,
            _etl_synced = CURRENT_TIMESTAMP(),
            _etl_operation = 'UPDATE'
        WHERE lead_call_id IN UNNEST(@ids)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("ids", "INT64", success_ids)
            ]
        )
        bq_client.query(update_success_sql, job_config=job_config).result()
        
    # Status -3 = Error en procesamiento de IA
    if failed_records:
        for f in failed_records:
            err_msg = str(f.get("error_message", "AI processing error"))[:500]
            update_fail_sql = f"""
            UPDATE `{target_project_id}.bronze.call_recordings`
            SET status = -3,
                error_message = @err_msg,
                _etl_synced = CURRENT_TIMESTAMP(),
                _etl_operation = 'UPDATE'
            WHERE lead_call_id = @lead_call_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("err_msg", "STRING", err_msg),
                    bigquery.ScalarQueryParameter("lead_call_id", "INT64", f["lead_call_id"]),
                ]
            )
            bq_client.query(update_fail_sql, job_config=job_config).result()
