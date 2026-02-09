import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import os
from google.cloud import bigquery, storage

# Configuración
PROJECT_SOURCE = "platform-partners-qua"
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Clase de autenticación
class ServiceTitanAuth:
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
        """Stream data directly to file to avoid memory issues"""
        token = self.get_access_token()
        tenant_id = self.credentials['tenant_id']
        page = 1
        page_size = 1000  # Smaller page size for estimates
        total_records = 0
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('[\n')
            first_page = True
            
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
                page_data = data.get("data", [])
                
                # Write data directly to file
                for i, record in enumerate(page_data):
                    if not first_page or i > 0:
                        f.write(',\n')
                    json.dump(record, f, ensure_ascii=False, indent=2)
                    first_page = False
                
                total_records += len(page_data)
                print(f"Página {page}: {len(page_data)} registros")
                
                if len(page_data) < page_size:
                    break
                page += 1
            
            f.write('\n]')
        
        return total_records

def upload_to_bucket(bucket_name, project_id, local_file, dest_blob_name):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_filename(local_file)
    print(f"Subido: gs://{bucket_name}/{dest_blob_name}")

def process_estimates_for_company(company_id):
    """Procesar solo estimates para una compañía específica"""
    print(f"Procesando estimates para company_id: {company_id}")
    
    # Conectar a BigQuery
    client = bigquery.Client(project=PROJECT_SOURCE)
    query = f"""
        SELECT * FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_id = {company_id}
        AND company_fivetran_status = TRUE
    """
    
    results = client.query(query).result()
    rows = list(results)
    
    if not rows:
        print(f"❌ No se encontró la compañía con ID: {company_id}")
        return
    
    row = rows[0]
    print(f"✅ Compañía encontrada: {row.company_name} (ID: {row.company_id})")
    
    # Configurar cliente ServiceTitan
    st_client = ServiceTitanAuth(
        row.app_id, 
        row.client_id, 
        row.client_secret, 
        row.tenant_id, 
        row.app_key
    )
    
    # Configurar bucket
    bucket_name = f"{row.company_project_id}_servicetitan"
    
    # Procesar solo estimates
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename_ts = f"servicetitan_estimates_{timestamp}.json"
    filename_alias = "servicetitan_estimates.json"
    
    try:
        print("Descargando estimates...")
        total_records = st_client.get_data_streaming("sales/v2/tenant", "estimates", filename_ts)
        
        print(f"Total registros: {total_records:,}")
        
        # Copiar archivo
        import shutil
        shutil.copy2(filename_ts, filename_alias)
        
        # Subir archivos
        upload_to_bucket(bucket_name, row.company_project_id, filename_ts, filename_ts)
        upload_to_bucket(bucket_name, row.company_project_id, filename_alias, filename_alias)
        
        # Limpiar archivos locales
        os.remove(filename_ts)
        os.remove(filename_alias)
        
        print(f"✅ Estimates procesados exitosamente: {total_records:,} registros")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        # Limpiar archivos en caso de error
        for temp_file in [filename_ts, filename_alias]:
            if os.path.exists(temp_file):
                os.remove(temp_file)

if __name__ == "__main__":
    # Cambiar por el company_id que necesitas
    company_id = 27  # Cambiar aquí por el company_id específico
    process_estimates_for_company(company_id)
