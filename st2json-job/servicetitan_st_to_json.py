"""
Script de prueba para procesar datos de ServiceTitan a JSON.
Permite procesar una sola compañía y/o un solo endpoint para pruebas locales.

Uso:
    python servicetitan_st_to_json.py --company-id 1
    python servicetitan_st_to_json.py --company-id 1 --endpoint "gross-pay-items"
    python servicetitan_st_to_json.py --company-id 1 --endpoint "gross-pay-items" --dry-run
"""

import argparse
import json
import os
import shutil
from datetime import datetime
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

# Configuración
PROJECT_SOURCE = get_project_source()
PROJECT_ID_FOR_QUERY = get_bigquery_project_id()
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Lista de endpoints grandes que requieren streaming
LARGE_ENDPOINTS = ["gross-pay-items"]


def process_company(row, endpoints_filter=None, dry_run=False):
    """
    Procesa una compañía, opcionalmente filtrando por endpoint.
    
    Args:
        row: Row de BigQuery con datos de la compañía
        endpoints_filter: Lista de endpoints a procesar (None = todos)
        dry_run: Si True, solo muestra qué haría sin ejecutar
    """
    # Obtener credenciales y datos
    company_id = row.company_id
    company_name = row.company_name
    company_new_name = row.company_new_name
    app_id = row.app_id
    client_id = row.client_id
    client_secret = row.client_secret
    tenant_id = row.tenant_id
    app_key = row.app_key
    project_id = row.company_project_id
    
    print(f"\n{'='*80}")
    print(f"🏢 Procesando compañía: {company_name} (ID: {company_id}) | project_id: {project_id}")
    if dry_run:
        print(f"🔍 MODO DRY-RUN: Solo mostrando qué se haría, sin ejecutar")
    print(f"{'='*80}")
    
    if dry_run:
        print(f"📋 [DRY-RUN] Se crearía/verificaría bucket: {project_id}_servicetitan")
        print(f"📋 [DRY-RUN] Se instanciaría cliente ServiceTitan")
    else:
        # 1. Crear/verificar bucket
        bucket_name = ensure_bucket_exists(project_id)
        # 2. Instanciar cliente ServiceTitan
        st_client = ServiceTitanAuth(app_id, client_id, client_secret, tenant_id, app_key)
    
    # 3. Cargar endpoints
    all_endpoints = load_endpoints_from_metadata()
    
    # Filtrar endpoints si se especificó
    if endpoints_filter:
        # Buscar endpoints que coincidan (por api_data o table_name)
        filtered_endpoints = []
        for api_url_base, api_data, table_name in all_endpoints:
            if api_data in endpoints_filter or table_name in endpoints_filter:
                filtered_endpoints.append((api_url_base, api_data, table_name))
        
        if not filtered_endpoints:
            print(f"⚠️  No se encontraron endpoints que coincidan con: {endpoints_filter}")
            return
        
        endpoints_to_process = filtered_endpoints
        print(f"📋 Procesando {len(endpoints_to_process)} endpoint(s) filtrado(s)")
    else:
        endpoints_to_process = all_endpoints
        print(f"📋 Procesando todos los endpoints ({len(endpoints_to_process)})")
    
    # 4. Descargar y subir datos de cada endpoint
    for api_url_base, api_data, table_name in endpoints_to_process:
        print(f"\n🔄 {'[DRY-RUN] ' if dry_run else ''}Descargando endpoint: {api_data} (tabla: {table_name})")
        
        if dry_run:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_ts = f"servicetitan_{table_name}_{timestamp}.json"
            filename_alias = f"servicetitan_{table_name}.json"
            
            if api_data.startswith("export/"):
                print(f"  📋 [DRY-RUN] Usaría modo EXPORT (continueFrom)")
            elif api_data in LARGE_ENDPOINTS or any(large in api_data for large in LARGE_ENDPOINTS):
                print(f"  📋 [DRY-RUN] Usaría modo STREAMING (archivo grande)")
            else:
                print(f"  📋 [DRY-RUN] Usaría modo NORMAL (paginación page=)")
            
            print(f"  📋 [DRY-RUN] Archivos generados: {filename_ts}, {filename_alias}")
            print(f"  📋 [DRY-RUN] Se subirían a: gs://{project_id}_servicetitan/")
            continue
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Usar table_name directamente desde metadata para nombrar archivos JSON
            filename_ts = f"servicetitan_{table_name}_{timestamp}.json"
            filename_alias = f"servicetitan_{table_name}.json"
            
            # Detectar si es endpoint export (usa from/continueFrom) o normal (usa page)
            if api_data.startswith("export/"):
                # Endpoint EXPORT: usa continueFrom para paginación
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
    parser = argparse.ArgumentParser(
        description='Script de prueba para procesar datos de ServiceTitan a JSON',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Procesar todos los endpoints de la compañía 1
  python servicetitan_st_to_json.py --company-id 1
  
  # Procesar solo un endpoint específico
  python servicetitan_st_to_json.py --company-id 1 --endpoint "gross-pay-items"
  
  # Modo dry-run (solo mostrar qué haría)
  python servicetitan_st_to_json.py --company-id 1 --endpoint "gross-pay-items" --dry-run
        """
    )
    
    parser.add_argument(
        '--company-id',
        type=int,
        required=True,
        help='ID de la compañía a procesar'
    )
    
    parser.add_argument(
        '--endpoint',
        type=str,
        nargs='+',
        help='Endpoint(s) específico(s) a procesar (puede ser api_data o table_name). Si no se especifica, procesa todos.'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Modo dry-run: solo muestra qué haría sin ejecutar'
    )
    
    args = parser.parse_args()
    
    print(f"\n{'='*80}")
    print("🔍 Script de Prueba: ServiceTitan ST → JSON")
    print(f"{'='*80}")
    print(f"📋 Compañía ID: {args.company_id}")
    if args.endpoint:
        print(f"📋 Endpoint(s): {', '.join(args.endpoint)}")
    else:
        print(f"📋 Endpoint(s): TODOS")
    print(f"📋 Modo: {'DRY-RUN' if args.dry_run else 'EJECUTAR'}")
    print(f"🔍 Proyecto detectado: {PROJECT_SOURCE}")
    print(f"🔍 Project ID para queries: {PROJECT_ID_FOR_QUERY}")
    print(f"{'='*80}\n")
    
    # Conectar a BigQuery y obtener la compañía
    print("Conectando a BigQuery para obtener compañía...")
    client = bigquery.Client()
    query = f"""
        SELECT * FROM `{PROJECT_ID_FOR_QUERY}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_id = @company_id
        AND company_fivetran_status = TRUE
        LIMIT 1
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company_id", "INT64", args.company_id)
        ]
    )
    
    results = list(client.query(query, job_config=job_config).result())
    
    if not results:
        print(f"❌ No se encontró compañía con ID {args.company_id} o no está activa (company_fivetran_status = TRUE)")
        return
    
    row = results[0]
    
    # Procesar compañía
    try:
        process_company(row, endpoints_filter=args.endpoint, dry_run=args.dry_run)
        print(f"\n{'='*80}")
        print(f"✅ Procesamiento completado")
        print(f"{'='*80}")
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ Error procesando compañía: {str(e)}")
        print(f"{'='*80}")
        raise


if __name__ == "__main__":
    main()
