from google.cloud import run_v2
import functions_framework
import time
import os

@functions_framework.http
def orchestrate_etl_jobs(request):
    """Orquesta la ejecución secuencial de los Jobs de ETL."""
    
    # Detectar proyecto desde variable de entorno o usar fallback
    # Cloud Functions establece GCP_PROJECT automáticamente
    project_id = os.environ.get('GCP_PROJECT') or os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    # Si no hay variable de entorno, detectar desde el ambiente
    if not project_id:
        # Intentar detectar desde el service account o usar fallback
        # En PRO, el project_id es "constant-height-455614-i0"
        # En DEV/QUA, project_id = project_name
        project_id = "platform-partners-des"  # Fallback a DEV
    
    # Mapeo de project_name a project_id (solo necesario para PRO)
    project_mapping = {
        "platform-partners-pro": "constant-height-455614-i0",
        "platform-partners-des": "platform-partners-des",
        "platform-partners-qua": "platform-partners-qua"
    }
    
    # Si recibimos project_name, convertir a project_id
    if project_id in project_mapping:
        project_id = project_mapping[project_id]
    
    region = "us-east1"
    
    # Nombres de los Jobs (sin sufijo en PRO, con sufijo en DEV/QUA)
    if project_id == "constant-height-455614-i0":
        # PRO: nombres sin sufijo
        job1_name = "etl-st2json-job"
        job2_name = "etl-json2bq-job"
    elif project_id == "platform-partners-des":
        # DEV: nombres con sufijo -dev
        job1_name = "etl-st2json-job-dev"
        job2_name = "etl-json2bq-job-dev"
    elif project_id == "platform-partners-qua":
        # QUA: nombres con sufijo -qua
        job1_name = "etl-st2json-job-qua"
        job2_name = "etl-json2bq-job-qua"
    else:
        # Fallback: asumir formato PRO
        job1_name = "etl-st2json-job"
        job2_name = "etl-json2bq-job"
    
    client = run_v2.JobsClient()
    
    try:
        # Ejecutar Job 1 (extracción de API)
        print("🚀 Iniciando Job 1: Extracción de datos de ServiceTitan API")
        job1_parent = f"projects/{project_id}/locations/{region}/jobs/{job1_name}"
        operation1 = client.run_job(name=job1_parent)
        
        # Esperar a que Job 1 termine
        print("⏳ Esperando a que Job 1 termine...")
        operation1.result()  # Esto bloquea hasta que termine
        print("✅ Job 1 completado exitosamente")
        
        # Esperar 60 segundos adicionales para asegurar que los archivos estén listos
        time.sleep(60)
        
        # Ejecutar Job 2 (procesamiento de JSONs)
        print("🚀 Iniciando Job 2: Procesamiento de JSONs a BigQuery")
        job2_parent = f"projects/{project_id}/locations/{region}/jobs/{job2_name}"
        operation2 = client.run_job(name=job2_parent)
        
        # Esperar a que Job 2 termine
        print("⏳ Esperando a que Job 2 termine...")
        operation2.result()
        print("✅ Job 2 completado exitosamente")
        
        return "Orchestration completada exitosamente", 200
        
    except Exception as e:
        print(f"❌ Error en la orquestación: {str(e)}")
        return f"Error: {str(e)}", 500