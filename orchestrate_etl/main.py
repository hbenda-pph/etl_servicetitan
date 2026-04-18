from google.cloud import run_v2, bigquery
import functions_framework
import time
import os
import math
from datetime import datetime

def get_total_workload_weight():
    """
    Calcula la duración total estimada de todas las compañías activas
    basándose en el historial de los últimos 7 días.
    """
    try:
        # PPH-CENTRAL es el origen de las métricas de monitoreo
        client = bigquery.Client(project="pph-central")
        query = """
            SELECT SUM(actual_duration) as total_duration
            FROM `pph-central.management.etl_monitoring_snapshot`
            WHERE updated_at >= CURRENT_TIMESTAMP() - INTERVAL 7 DAY
        """
        results = list(client.query(query).result())
        if results and results[0].total_duration:
            return float(results[0].total_duration)
        
        # Fallback si no hay métricas: asume ~40 minutos de carga total
        return 2400.0 
    except Exception as e:
        print(f"⚠️ Error calculando peso total: {e}")
        return 2400.0


@functions_framework.http
def orchestrate_etl_jobs(request):
    """Orquesta la ejecución secuencial de los Jobs de ETL."""
    
    start_time_all = datetime.now()
    
    # Detectar proyecto desde variable de entorno o usar fallback
    project_id = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    # Mapeo de project_name a project_id (solo necesario para PRO)
    project_mapping = {
        "platform-partners-pro": "constant-height-455614-i0",
        "platform-partners-des": "platform-partners-des",
        "platform-partners-qua": "platform-partners-qua"
    }
    
    if project_id in project_mapping:
        project_id = project_mapping[project_id]
    
    if not project_id:
        project_id = "platform-partners-des"  # Fallback
        
    region = "us-east1"
    
    # Nombres de los Jobs
    if project_id == "constant-height-455614-i0":
        job1_name = "etl-st2json-job"
        job2_name = "etl-json2bq-job"
    elif project_id == "platform-partners-qua":
        job1_name = "etl-st2json-job-qua"
        job2_name = "etl-json2bq-job-qua"
    else:
        job1_name = "etl-st2json-job-dev"
        job2_name = "etl-json2bq-job-dev"
    
    client = run_v2.JobsClient()
    
    # --- CÁLCULO DE ESCALABILIDAD DINÁMICA ---
    total_weight = get_total_workload_weight()
    n_ideal = math.ceil(total_weight / 600)
    
    # Aplicar límites razonables
    st2json_tasks = min(max(3, n_ideal), 20)
    json2bq_tasks = min(max(4, n_ideal), 25)
    
    print(f"⚖️ Orquestación Iniciada - Ambiente: {project_id}")
    print(f"⚖️ Peso total estimado: {total_weight/60:.1f} minutos")
    print(f"⚖️ Configuración: st2json={st2json_tasks} tasks, json2bq={json2bq_tasks} tasks")
    
    try:
        # --- JOB 1: EXTRACCIÓN ---
        start_time_j1 = datetime.now()
        print(f"🚀 [1/2] Iniciando Job 1: Extracción ({job1_name})")
        
        job1_parent = f"projects/{project_id}/locations/{region}/jobs/{job1_name}"
        request1 = {
            "name": job1_parent,
            "overrides": {"task_count": st2json_tasks}
        }
        operation1 = client.run_job(request=request1)
        
        print("⏳ Polling Job 1...")
        operation1.result(timeout=3600)  # Espera activa con timeout ampliado
        
        duration_j1 = (datetime.now() - start_time_j1).total_seconds()
        print(f"✅ Job 1 completado en {duration_j1/60:.1f} minutos")
        
        # Margen de seguridad para GCS
        print("⏳ Esperando 30s para consistencia de archivos en GCS...")
        time.sleep(30)
        
        # --- JOB 2: CARGA ---
        start_time_j2 = datetime.now()
        print(f"🚀 [2/2] Iniciando Job 2: Carga a BigQuery ({job2_name})")
        
        job2_parent = f"projects/{project_id}/locations/{region}/jobs/{job2_name}"
        request2 = {
            "name": job2_parent,
            "overrides": {"task_count": json2bq_tasks}
        }
        operation2 = client.run_job(request=request2)
        
        print("⏳ Polling Job 2...")
        operation2.result(timeout=3600)
        
        duration_j2 = (datetime.now() - start_time_j2).total_seconds()
        print(f"✅ Job 2 completado en {duration_j2/60:.1f} minutos")
        
        total_duration = (datetime.now() - start_time_all).total_seconds()
        print(f"🎉 Orquestación finalizada exitosamente en {total_duration/60:.1f} minutos")
        
        return {
            "status": "success",
            "total_duration_minutes": total_duration/60,
            "job1_duration_minutes": duration_j1/60,
            "job2_duration_minutes": duration_j2/60
        }, 200
        
    except Exception as e:
        error_msg = f"❌ Error en la orquestación: {str(e)}"
        print(error_msg)
        return {"status": "error", "message": str(e)}, 500