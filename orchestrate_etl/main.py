from google.cloud import run_v2
import functions_framework
import time

@functions_framework.http
def orchestrate_etl_jobs(request):
    """Orquesta la ejecución secuencial de los Jobs de ETL."""
    
    # Configuración
    project_id = "platform-partners-des"  # Tu proyecto
    region = "us-east1"
    
    # Nombres de los Jobs
    job1_name = "etl-st2json-job"  # Job que extrae de API
    job2_name = "etl-json2bq-job"  # Job que procesa JSONs
    
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