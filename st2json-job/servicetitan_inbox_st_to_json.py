"""
ETL INBOX: ServiceTitan → JSON (Cloud Storage)

Refactorizado para reutilizar `servicetitan_common` como el script normal,
con salvedades INBOX:
- procesa múltiples compañías desde `pph-inbox.settings.companies`
- cada compañía escribe en su propio proyecto temporal (usa `company_project_id`)
"""

import json
import os
import shutil
from datetime import datetime

from google.cloud import bigquery

from servicetitan_common import (
    ServiceTitanAuth,
    ensure_bucket_exists,
    load_endpoints_from_metadata,
    upload_to_bucket,
)

# Fuente de configuración INBOX
PROJECT_SOURCE = "pph-inbox"
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Lista de endpoints grandes que requieren streaming (alineado con script normal)
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
    # Cada compañía INBOX tiene su propio proyecto temporal
    project_id = row.company_project_id
    if not project_id:
        raise ValueError(
            f"company_project_id vacío para company_id={company_id}. "
            f"Primero crea/asigna el proyecto INBOX por compañía."
        )

    print(f"\n{'='*80}")
    print(f"🏢 Procesando compañía INBOX: {company_name} (ID: {company_id}) | project_id: {project_id}")
    print(f"{'='*80}")

    # 1) Crear/verificar bucket
    bucket_name = ensure_bucket_exists(project_id)

    # 2) Cliente ServiceTitan
    st_client = ServiceTitanAuth(app_id, client_id, client_secret, tenant_id, app_key)

    # 3) Endpoints (desde metadata centralizada)
    endpoints = load_endpoints_from_metadata()

    # 4) Descarga y subida por endpoint
    for api_url_base, api_data, table_name in endpoints:
        print(f"\n🔄 Descargando endpoint: {api_data} (tabla: {table_name})")

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_ts = f"servicetitan_{table_name}_{timestamp}.json"
            filename_alias = f"servicetitan_{table_name}.json"

            if api_data.startswith("export/"):
                total, continue_token = st_client.get_data_export(api_url_base, api_data, filename_alias)
                print(f"📊 [EXPORT MODE] Total registros descargados: {total}")
                if continue_token:
                    print(f"🔖 [EXPORT MODE] Token para próxima ejecución: {continue_token}")
                shutil.copy2(filename_alias, filename_ts)
            elif api_data in LARGE_ENDPOINTS or any(large in api_data for large in LARGE_ENDPOINTS):
                print(f"📥 [STREAMING MODE] Usando streaming para endpoint grande: {api_data}")
                total = st_client.get_data_streaming(api_url_base, api_data, filename_alias)
                print(f"📊 [STREAMING MODE] Total registros descargados: {total}")
                shutil.copy2(filename_alias, filename_ts)
            else:
                data = st_client.get_data(api_url_base, api_data)
                print(f"📊 [NORMAL MODE] Total registros descargados: {len(data)}")
                with open(filename_ts, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                with open(filename_alias, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

            upload_to_bucket(bucket_name, project_id, filename_ts, filename_ts)
            upload_to_bucket(bucket_name, project_id, filename_alias, filename_alias)

            # Limpieza
            try:
                os.remove(filename_ts)
                os.remove(filename_alias)
            except Exception:
                pass

            print(f"✅ Endpoint {api_data} procesado y archivos subidos.")
        except Exception as e:
            print(f"❌ Error en endpoint {api_data}: {str(e)}")

def main():
    # INBOX (multi-compañía): itera todas las compañías activas en pph-inbox.settings.companies
    print("Conectando a BigQuery para obtener compañías INBOX...")
    client = bigquery.Client(project=PROJECT_SOURCE)
    query = f"""
        SELECT * FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_fivetran_status = TRUE
          AND company_project_id IS NOT NULL
        ORDER BY company_id
    """
    results = list(client.query(query).result())
    
    if not results:
        print("❌ No se encontró ninguna compañía INBOX activa en la tabla companies (con company_project_id).")
        return
    
    total = len(results)
    print(f"📊 Se encontraron {total} compañía(s) INBOX activa(s) para procesar\n")

    for idx, row in enumerate(results, 1):
        try:
            print(f"📊 Procesando compañía {idx} de {total}")
            process_company(row)
        except Exception as e:
            print(f"❌ Error procesando compañía INBOX {row.company_name} (ID: {row.company_id}): {str(e)}")

        print(f"\n{'='*80}")
    print(f"🏁 Procesamiento INBOX completado.")

if __name__ == "__main__":
    main()