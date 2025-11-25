import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import os
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
    ("jpm/v2/tenant", "export/job-canceled-logs")
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
    def get_data(self, api_url_base, api_data):
        token = self.get_access_token()
        tenant_id = self.credentials['tenant_id']
        all_data = []
        page = 1
        page_size = 5000
        prev_total = 0
        while True:
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
            page_items = data.get("data", [])
            if not page_items:
                break  # sin datos en esta página
            all_data.extend(page_items)
            # romper si no hay crecimiento para evitar paginación defectuosa
            if len(all_data) == prev_total:
                break
            prev_total = len(all_data)
            # última página detectada por tamaño parcial
            if len(page_items) < page_size:
                break
            page += 1
        return all_data

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
            data = st_client.get_data(api_url_base, api_data)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_api_data = api_data.replace("/","_") 
            filename_ts = f"servicetitan_{filename_api_data}_{timestamp}.json"
            filename_alias = f"servicetitan_{filename_api_data}.json"
            # Guardar localmente ambos archivos
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
