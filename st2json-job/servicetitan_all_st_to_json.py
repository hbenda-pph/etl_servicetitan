import json
from datetime import datetime
import os
import shutil
from google.cloud import bigquery

# Importar funciones comunes
from servicetitan_common import (
    get_project_source,
    get_bigquery_project_id,
    load_endpoints_from_metadata,
    ServiceTitanAuth,
    ensure_bucket_exists,
    upload_to_bucket
)

PROJECT_SOURCE = get_project_source()  # Para logging/información
PROJECT_ID_FOR_QUERY = get_bigquery_project_id()  # Para usar en queries SQL (project_id real)
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Cargar endpoints automáticamente desde metadata
ENDPOINTS = load_endpoints_from_metadata()

# Lista de endpoints grandes que requieren streaming
LARGE_ENDPOINTS = ["gross-pay-items"]

def process_company(row):
    # Obtener credenciales y datos
    company_id = row.company_id
    company_name = row.company_name
    company_new_name = row.company_new_name
    app_id = row.app_id
    client_id = row.client_id
    client_secret = row.client_secret
    tenant_id = row.tenant_id
    app_key = row.app_key
    project_id = row.company_project_id  # Usar directamente el campo project_id
    print(f"\n{'='*80}\n🏢 Procesando compañía: {company_name} (ID: {company_id}) | project_id: {project_id}")
    # 1. Crear/verificar bucket
    bucket_name = ensure_bucket_exists(project_id)
    # 2. Instanciar cliente ServiceTitan
    st_client = ServiceTitanAuth(app_id, client_id, client_secret, tenant_id, app_key)
    # 3. Descargar y subir datos de cada endpoint
    for api_url_base, api_data, table_name in ENDPOINTS:
        print(f"\n🔄 Descargando endpoint: {api_data}")
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Usar table_name directamente desde metadata para nombrar archivos JSON
            filename_ts = f"servicetitan_{table_name}_{timestamp}.json"
            filename_alias = f"servicetitan_{table_name}.json"
            
            # Detectar si es endpoint export (usa from/continueFrom) o normal (usa page)
            if api_data.startswith("export/"):
                # Endpoint EXPORT: usa continueFrom para paginación
                #print(f"📤 [EXPORT MODE] Usando paginación con continueFrom")
                total, continue_token = st_client.get_data_export(api_url_base, api_data, filename_alias)
                print(f"📊 [EXPORT MODE] Total registros descargados: {total}")
                if continue_token:
                    print(f"🔖 [EXPORT MODE] Token para próxima ejecución: {continue_token}")
                # Copiar para archivo con timestamp
                shutil.copy2(filename_alias, filename_ts)
            elif api_data in LARGE_ENDPOINTS or any(large in api_data for large in LARGE_ENDPOINTS):
                # Endpoint GRANDE: usar streaming para evitar problemas de memoria
                print(f"📥 [STREAMING MODE] Usando streaming para endpoint grande: {api_data}")
                total = st_client.get_data_streaming(api_url_base, api_data, filename_alias)
                print(f"📊 [STREAMING MODE] Total registros descargados: {total}")
                # Copiar para archivo con timestamp
                shutil.copy2(filename_alias, filename_ts)
            else:
                # Endpoint NORMAL: carga en memoria con paginación page=
                #print(f"📥 [NORMAL MODE] Usando paginación con page=")
                data = st_client.get_data(api_url_base, api_data)
                print(f"📊 [NORMAL MODE] Total registros descargados: {len(data)}")
                with open(filename_ts, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                with open(filename_alias, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            
            # Subir ambos archivos al bucket
            upload_to_bucket(bucket_name, project_id, filename_ts, filename_ts)
            upload_to_bucket(bucket_name, project_id, filename_alias, filename_alias)
            # Borrar archivos locales
            os.remove(filename_ts)
            os.remove(filename_alias)
            print(f"✅ Endpoint {api_data} procesado y archivos subidos.")
        except Exception as e:
            print(f"❌ Error en endpoint {api_data}: {str(e)}")

def main():
    # Detectar si estamos en modo paralelo (Cloud Run Jobs con múltiples tareas)
    # Cloud Run Jobs establece estas variables de entorno automáticamente:
    # CLOUD_RUN_TASK_INDEX: índice de la tarea actual (0-based)
    # CLOUD_RUN_TASK_COUNT: número total de tareas
    task_index = int(os.environ.get('CLOUD_RUN_TASK_INDEX', '0'))
    task_count = int(os.environ.get('CLOUD_RUN_TASK_COUNT', '1'))
    is_parallel = task_count > 1
    
    if is_parallel:
        print(f"\n{'='*80}")
        print(f"🚀 MODO PARALELO ACTIVADO")
        print(f"   Tarea: {task_index + 1}/{task_count}")
        print(f"{'='*80}")
    
    print(f"🔍 Proyecto detectado para companies: {PROJECT_SOURCE}")
    print(f"🔍 Project ID para queries: {PROJECT_ID_FOR_QUERY}")
    print("Conectando a BigQuery para obtener compañías...")
    if is_parallel:
        print(f"🔄 Procesamiento paralelo: Tarea {task_index + 1} de {task_count}")
    
    # Crear cliente sin especificar project (usa el del service account = project_id real)
    # Usar PROJECT_ID_FOR_QUERY (project_id real) en la query SQL
    client = bigquery.Client()  # Usa el project del service account automáticamente
    query = f"""
        SELECT * FROM `{PROJECT_ID_FOR_QUERY}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_fivetran_status = TRUE
        ORDER BY company_id
    """
    results = list(client.query(query).result())  # Convertir a lista para poder contar
    total = len(results)
    
    # En modo paralelo, dividir las compañías entre las tareas
    if is_parallel:
        # Calcular qué compañías procesa esta tarea
        companies_per_task = total // task_count
        remainder = total % task_count
        
        # Las primeras tareas procesan una compañía extra si hay resto
        start_idx = task_index * companies_per_task + min(task_index, remainder)
        end_idx = start_idx + companies_per_task + (1 if task_index < remainder else 0)
        
        # Filtrar compañías para esta tarea
        results = results[start_idx:end_idx]
        total_assigned = len(results)
        
        print(f"📊 Total de compañías: {total}")
        print(f"📊 Compañías asignadas a esta tarea: {total_assigned} (índices {start_idx+1}-{end_idx} de {total})")
    else:
        total_assigned = total
        print(f"📊 Total de compañías a procesar: {total}")
    
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
        print(f"🏁 Resumen Tarea {task_index + 1}/{task_count}: {procesadas}/{total_assigned} compañías procesadas exitosamente.")
        print(f"📊 Total global: {total} compañías distribuidas en {task_count} tareas")
    else:
        print(f"🏁 Resumen: {procesadas}/{total} compañías procesadas exitosamente.")

if __name__ == "__main__":
    main()
