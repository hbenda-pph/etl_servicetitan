import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import os
import gc
import psutil
from google.cloud import bigquery, storage

# Configuración de BigQuery
PROJECT_SOURCE = "platform-partners-qua"
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Endpoints a consultar
ENDPOINTS = [
    ("settings/v2/tenant", "business-units"),
    ("jpm/v2/tenant/", "job-types"),
    ("settings/v2/tenant", "technicians"),
    ("settings/v2/tenant", "employees"),
    ("marketing/v2/tenant", "campaigns"),
    ("timesheets/v2/tenant", "activities"),
    ("payroll/v2/tenant", "jobs/timesheets"),    
    ("sales/v2/tenant", "estimates")
]
#    ("timesheets/v2/tenant", "timesheets"),

# Funciones de monitoreo de memoria (simplificadas)
def log_memory_usage():
    """Log current memory usage"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024
    return memory_mb

# Clase de autenticación y descarga
class ServiceTitanAuth:
    #Development:
    #AUTH_URL = "https://auth-integration.servicetitan.io/connect/token"
    #BASE_API_URL = "https://api-integration.servicetitan.io"

    #Production:
    AUTH_URL = "https://auth.servicetitan.io/connect/token"
    BASE_API_URL = "https://api.servicetitan.io"

    def __init__(self, app_id, client_id, client_secret, tenant_id, app_key):
        self.credentials = {
            'app_id': app_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'tenant_id': tenant_id,
            'app_key': app_key
        }
    
    def get_access_token(self):
        response = requests.post(
            self.AUTH_URL,
            auth=HTTPBasicAuth(self.credentials['client_id'], self.credentials['client_secret']),
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'ST-App-Key': self.credentials['app_key']
            },
            data={
                'grant_type': 'client_credentials',
                'client_id': self.credentials['client_id'],
                'client_secret': self.credentials['client_secret']
            }
        )
        response.raise_for_status()
        return response.json()['access_token']
    
    def get_data_streaming(self, api_url_base, api_data, output_file):
        """Stream data directly to file to avoid memory accumulation"""
        token = self.get_access_token()
        tenant_id = self.credentials['tenant_id']
        page = 1
        page_size = 5000
        total_records = 0
        max_memory_mb = 2000  # Límite de memoria en MB
        
        # Open file for writing
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('[\n')
            first_page = True
            
            while True:
                # Verificar memoria antes de cada página (silencioso)
                current_memory = log_memory_usage()
                if current_memory > max_memory_mb:
                    gc.collect()
                
                url = f"{self.BASE_API_URL}/{api_url_base}/{tenant_id}/{api_data}?page={page}&pageSize={page_size}&active=Any"
                response = requests.get(
                    url,
                    headers={
                        'Authorization': f'Bearer {token}',
                        'ST-App-Id': self.credentials['app_id'],
                        'ST-App-Key': self.credentials['app_key']
                    }
                )
                response.raise_for_status()
                data = response.json()
                page_data = data.get("data", [])
                
                # Write data directly to file
                for i, record in enumerate(page_data):
                    if not first_page or i > 0:
                        f.write(',\n')
                    json.dump(record, f, ensure_ascii=False, indent=2)
                    first_page = False
                
                total_records += len(page_data)
                
                if len(page_data) < page_size:
                    break
                page += 1
                
                # Force garbage collection every 5 pages for estimates endpoint
                if api_data == "estimates" and page % 5 == 0:
                    gc.collect()
                elif page % 10 == 0:
                    gc.collect()
            
            f.write('\n]')
        
        print(f"✅ Total de registros procesados: {total_records:,}")
        return total_records

def ensure_bucket_exists(project_id, region="US"):
    bucket_name = f"{project_id}_servicetitan"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name, location=region)
    return bucket_name

def upload_to_bucket(bucket_name, project_id, local_file, dest_blob_name):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_filename(local_file)
    print(f"📤 Subido a gs://{bucket_name}/{dest_blob_name}")

def process_company(row):
    # Obtener credenciales y datos
    company_id = row.company_id
    company_name = row.company_name
    app_id = row.app_id
    client_id = row.client_id
    client_secret = row.client_secret
    tenant_id = row.tenant_id
    app_key = row.app_key
    project_id = row.company_project_id
    
    print(f"🏢 Procesando: {company_name} (ID: {company_id})")
    
    # 1. Crear/verificar bucket
    bucket_name = ensure_bucket_exists(project_id)
    
    # 2. Instanciar cliente ServiceTitan
    st_client = ServiceTitanAuth(app_id, client_id, client_secret, tenant_id, app_key)
    
    # 3. Descargar y subir datos de cada endpoint
    for api_url_base, api_data in ENDPOINTS:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_api_data = api_data.replace("/","_") 
            filename_ts = f"servicetitan_{filename_api_data}_{timestamp}.json"
            filename_alias = f"servicetitan_{filename_api_data}.json"

            # Usar streaming para evitar acumulación de memoria
            total_records = st_client.get_data_streaming(api_url_base, api_data, filename_ts)
            
            # Copiar archivo para el alias (sin timestamp)
            import shutil
            shutil.copy2(filename_ts, filename_alias)
            
            # Subir ambos archivos al bucket
            upload_to_bucket(bucket_name, project_id, filename_ts, filename_ts)
            upload_to_bucket(bucket_name, project_id, filename_alias, filename_alias)
            
            # Borrar archivos locales inmediatamente
            os.remove(filename_ts)
            os.remove(filename_alias)
            
            # Forzar garbage collection después de cada endpoint
            gc.collect()
            
            print(f"✅ {company_name} - {api_data}: {total_records:,} registros procesados")
            
        except Exception as e:
            print(f"❌ {company_name} - {api_data}: ERROR - {str(e)}")
            # Limpiar archivos temporales en caso de error
            for temp_file in [filename_ts, filename_alias]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            gc.collect()

def main():
    print("🚀 Iniciando proceso ETL ServiceTitan...")
    
    client = bigquery.Client(project=PROJECT_SOURCE)
    query = f"""
        SELECT * FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_fivetran_status = TRUE
        ORDER BY company_id
    """
    results = client.query(query).result()
    total = 0
    procesadas = 0
    
    for row in results:
        total += 1
        try:
            process_company(row)
            procesadas += 1
            gc.collect()
            
        except Exception as e:
            print(f"❌ Error procesando compañía {row.company_name}: {str(e)}")
            gc.collect()
    
    print(f"\n🏁 Resumen: {procesadas}/{total} compañías procesadas exitosamente.")

if __name__ == "__main__":
    main()