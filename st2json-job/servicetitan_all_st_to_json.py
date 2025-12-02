import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import os
import time
import shutil
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
    ("payroll/v2/tenant", "jobs/timesheets"),
    ("inventory/v2/tenant", "purchase-orders"),
    ("inventory/v2/tenant", "returns"),
    ("inventory/v2/tenant", "vendors"),    
    ("jpm/v2/tenant", "export/job-canceled-logs"),
    ("jpm/v2/tenant", "job-cancel-reasons")    
]
# Endpoints deshabilitados temporalmente (causan OOM):
#    ("payroll/v2/tenant", "payrolls"),    
#    ("sales/v2/tenant", "estimates")
#    ("timesheets/v2/tenant", "timesheets"),
#    ("timesheets/v2/tenant", "activities"),


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
        self._token = None
        self._token_time = 0

    def get_access_token(self):
        # Renovar si pasaron más de 10 minutos
        if self._token and (time.time() - self._token_time) < 600:
            return self._token
        
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
        self._token = response.json()['access_token']
        self._token_time = time.time()
        return self._token

    def get_data(self, api_url_base, api_data):
        """Método estándar para endpoints pequeños - retorna lista en memoria"""
        page_size = 5000
        all_data = []
        page = 1
        
        while True:
            token = self.get_access_token()
            tenant_id = self.credentials['tenant_id']
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
            page_items = response.json().get("data", [])
            
            if not page_items:
                break
            all_data.extend(page_items)
            
            if len(page_items) < page_size:
                break
            page += 1
        
        return all_data

    def get_data_export(self, api_url_base, api_data, output_file, continue_from=None):
        """Método para endpoints /export/ - usa from/continueFrom en vez de paginación"""
        total_records = 0
        
        with open(output_file, 'w', encoding='utf-8') as f:
            while True:
                token = self.get_access_token()
                tenant_id = self.credentials['tenant_id']
                
                # Construir URL con o sin token de continuación
                if continue_from:
                    url = f"{self.BASE_API_URL}/{api_url_base}/{tenant_id}/{api_data}?from={continue_from}"
                else:
                    url = f"{self.BASE_API_URL}/{api_url_base}/{tenant_id}/{api_data}"
                
                response = requests.get(url, headers={
                    'Authorization': f'Bearer {token}',
                    'ST-App-Id': self.credentials['app_id'],
                    'ST-App-Key': self.credentials['app_key']
                })
                response.raise_for_status()
                
                result = response.json()
                page_items = result.get("data", [])
                continue_from = result.get("continueFrom")
                has_more = result.get("hasMore", False)
                
                if not page_items:
                    break
                
                # Escribir cada item como línea separada (newline-delimited)
                for item in page_items:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
                
                total_records += len(page_items)
                #print(f"📄 Batch: {len(page_items)} registros (total: {total_records})")
                
                # Salir si no hay más datos
                if not has_more or not continue_from:
                    break
        
        return total_records, continue_from

def ensure_bucket_exists(project_id, region="US"):
    bucket_name = f"{project_id}_servicetitan"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket_name, location=region)
        print(f"✅ Bucket creado: {bucket_name}")
    else:
        print(f"Bucket ya existe: {bucket_name}")
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
    for api_url_base, api_data in ENDPOINTS:
        print(f"\n🔄 Descargando endpoint: {api_data}")
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_api_data = api_data.replace("/","_") 
            filename_ts = f"servicetitan_{filename_api_data}_{timestamp}.json"
            filename_alias = f"servicetitan_{filename_api_data}.json"
            
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
    print("Conectando a BigQuery para obtener compañías...")
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
        except Exception as e:
            print(f"❌ Error procesando compañía {row.company_name}: {str(e)}")
    print(f"\n{'='*80}")
    print(f"🏁 Resumen: {procesadas}/{total} compañías procesadas exitosamente.")

if __name__ == "__main__":
    main()
