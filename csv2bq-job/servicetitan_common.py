"""
Módulo común con funciones compartidas para el ETL csv2bq.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone
from google.cloud import bigquery, storage

# Configurar logging
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("google.auth").setLevel(logging.ERROR)
logging.getLogger("google.auth.transport").setLevel(logging.ERROR)

import warnings
warnings.filterwarnings("ignore", message=".*quota project.*", category=UserWarning)
warnings.filterwarnings("ignore", message=".*end user credentials.*", category=UserWarning)


def get_project_source():
    """Obtiene el proyecto del ambiente actual."""
    project = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    if project:
        return project
    
    try:
        import subprocess
        result = subprocess.run(
            ['gcloud', 'config', 'get-value', 'project'],
            capture_output=True,
            text=True,
            timeout=3
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception:
        pass
    
    return "platform-partners-des"


def get_bigquery_project_id():
    project_source = get_project_source()
    if project_source == "platform-partners-pro":
        return "constant-height-455614-i0"
    return project_source


LOGS_DATASET = "logs"
LOGS_TABLE = "etl_csv2bq"

def get_logs_project():
    project = get_bigquery_project_id()
    if not hasattr(get_logs_project, '_project_shown'):
        print(f"🔍 Proyecto detectado para logs: {project}")
        get_logs_project._project_shown = True
    return project


def log_event_bq(company_id=None, company_name=None, project_id=None, endpoint=None, 
                event_type="INFO", event_title="", event_message="", info=None, source="csv2bq_job"):
    """Inserta un evento en la tabla de logs centralizada."""
    try:
        logs_project = get_logs_project()
        client = bigquery.Client(project=logs_project)
        table_id = f"{LOGS_DATASET}.{LOGS_TABLE}"
        
        row = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "company_id": str(company_id) if company_id else None,
            "company_name": company_name,
            "project_id": project_id,
            "endpoint": endpoint,
            "event_type": event_type,
            "event_title": event_title,
            "event_message": event_message,
            "source": source,
            "info": json.dumps(info) if info else None
        }
        
        errors = client.insert_rows_json(table_id, [row])
        if errors:
            print(f"❌ [log_event_bq] Error insertando log en BigQuery: {errors}")
    except Exception as e:
        print(f"❌ [log_event_bq] Error en logging: {str(e)}")


def get_balanced_tasks(bq_client, results, task_count, task_index):
    """
    Distribuye las compañías entre las tareas de Cloud Run para paralelismo.
    """
    if task_count <= 1:
        return results

    # Como no tenemos historico de tiempos de CSV aún, asignamos round-robin básico.
    # En el futuro se puede adaptar a leer métricas históricas reales.
    bins = [[] for _ in range(task_count)]
    for i, row in enumerate(results):
        bins[i % task_count].append(row)
    
    assigned_companies = bins[task_index]
    
    print(f"\n⚖️  BALANCEO (Round Robin)")
    print(f"   Tarea actual: {task_index + 1} de {task_count}")
    print(f"   Compañías asignadas: {len(assigned_companies)}")
    
    assigned_companies.sort(key=lambda x: x.company_id)
    return assigned_companies


def read_schema_from_layout(layout_path):
    """
    Lee un archivo de layout (ej. layout_technician_timesheet_summary.txt)
    y lo convierte en una lista de bigquery.SchemaField.
    Formato esperado por línea: campo:TIPO
    """
    schema = []
    if not os.path.exists(layout_path):
        raise FileNotFoundError(f"Archivo de layout no encontrado: {layout_path}")
        
    with open(layout_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Puede tener una coma al final
            if line.endswith(","):
                line = line[:-1]
                
            parts = line.split(":")
            if len(parts) == 2:
                name = parts[0].strip()
                bq_type = parts[1].strip()
                schema.append(bigquery.SchemaField(name, bq_type))
            else:
                print(f"⚠️ Línea de layout ignorada (formato inválido): {line}")
                
    return schema


def load_csv_to_bq(
    bq_client, temp_csv_path, table_ref_final, 
    schema, load_start, is_append=True
):
    """
    Carga un archivo CSV a BigQuery definiendo explícitamente el esquema
    y saltando la fila de encabezados.
    """
    write_disp = bigquery.WriteDisposition.WRITE_APPEND if is_append else bigquery.WriteDisposition.WRITE_TRUNCATE
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Saltar el encabezado problemático
        schema=schema,
        write_disposition=write_disp,
        allow_quoted_newlines=True
    )

    print(f"📤 Cargando CSV a la tabla {table_ref_final.dataset_id}.{table_ref_final.table_id}...")
    try:
        with open(temp_csv_path, "rb") as f:
            load_job = bq_client.load_table_from_file(
                f,
                table_ref_final,
                job_config=job_config
            )
        load_job.result()
        
        load_time = time.time() - load_start
        print(f"✅ Carga a BigQuery completada en {load_time:.1f}s")
        return True, load_time, None
    except Exception as e:
        error_msg = str(e)
        # Extraer detalles adicionales del job si existen
        if load_job and hasattr(load_job, "errors") and load_job.errors:
            error_msg += f" Detalles: {json.dumps(load_job.errors)}"
        return False, time.time() - load_start, error_msg
