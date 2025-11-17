import os
import json
import re
from datetime import datetime, timezone
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound

# Configuración de BigQuery para la tabla de compañías INBOX
PROJECT_SOURCE = "pph-inbox"
DATASET_NAME = "settings"
TABLE_NAME = "companies"

# Endpoints y nombres de archivo a procesar (versión reducida para Free Trial)
ENDPOINTS = [
    "job-types",
    "technicians",
    "campaigns",
    "jobs_timesheets",
    "purchase-orders",
    "returns"
]

# Función para convertir a snake_case
def to_snake_case(name):
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def fix_json_format(local_path, temp_path):
    """Transforma el JSON a formato newline-delimited y snake_case."""
    with open(local_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    with open(temp_path, 'w', encoding='utf-8') as f:
        for item in json_data:
            new_item = {to_snake_case(k): v for k, v in item.items()}
            f.write(json.dumps(new_item) + '\n')

def upload_to_bucket(bucket_name, project_id, local_file, dest_blob_name):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(dest_blob_name)
    blob.upload_from_filename(local_file)
    print(f"📤 Subido a gs://{bucket_name}/{dest_blob_name}")

def detectar_cambios_reales(staging_df, final_df, project_id, dataset_final, table_final):
    """Detecta si hay cambios reales entre staging y tabla final"""
    try:
        if final_df.empty:
            return 'INSERT'  # Primera carga
        
        # Obtener campos relevantes (excluyendo campos ETL)
        campos_relevantes = [col.name for col in staging_df.columns if not col.name.startswith('_etl_')]
        
        cambios_detectados = []
        
        for _, staging_row in staging_df.iterrows():
            id_val = staging_row['id']
            
            # Buscar en tabla final
            final_rows = final_df[final_df['id'] == id_val]
            
            if final_rows.empty:
                cambios_detectados.append('INSERT')
            else:
                final_row = final_rows.iloc[0]
                
                # Comparar campos relevantes
                hay_cambios = False
                for campo in campos_relevantes:
                    if str(staging_row[campo]) != str(final_row[campo]):
                        hay_cambios = True
                        break
                
                if hay_cambios:
                    cambios_detectados.append('UPDATE')
                else:
                    cambios_detectados.append('SYNC')
        
        # Determinar operación principal
        if 'UPDATE' in cambios_detectados:
            return 'UPDATE'
        elif 'INSERT' in cambios_detectados:
            return 'INSERT'
        else:
            return 'SYNC'
            
    except Exception as e:
        print(f"⚠️  Error detectando cambios: {str(e)}")
        return 'UPDATE'  # Por defecto, asumir UPDATE    

def process_company(row):
    company_id = row.company_id
    company_name = row.company_name
    project_id = "pph-inbox"  # Proyecto fijo para INBOX
    
    print(f"\n{'='*80}\n🏢 Procesando compañía INBOX: {company_name} (ID: {company_id}) | project_id: {project_id}")
    bucket_name = f"{project_id}_servicetitan"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    
    for endpoint in ENDPOINTS:
        json_filename = f"servicetitan_{endpoint}.json"
        temp_json = f"/tmp/{project_id}_{endpoint}.json"
        temp_fixed = f"/tmp/fixed_{project_id}_{endpoint}.json"
        
        # Descargar archivo JSON del bucket
        try:
            blob = bucket.blob(json_filename)
            if not blob.exists():
                print(f"⚠️  Archivo no encontrado: {json_filename} en bucket {bucket_name}")
                continue
            blob.download_to_filename(temp_json)
            print(f"⬇️  Descargado {json_filename} de gs://{bucket_name}")
        except Exception as e:
            print(f"❌ Error descargando {json_filename}: {str(e)}")
            continue
        
        # Transformar a newline-delimited y snake_case
        try:
            fix_json_format(temp_json, temp_fixed)
            print(f"🔄 Transformado a newline-delimited y snake_case: {temp_fixed}")
        except Exception as e:
            print(f"❌ Error transformando {json_filename}: {str(e)}")
            continue
        
        # Cargar a tabla staging en BigQuery
        bq_client = bigquery.Client(project=project_id)
        dataset_staging = "staging"
        dataset_final = "bronze"
        table_staging = endpoint
        table_final = endpoint
        table_ref_staging = bq_client.dataset(dataset_staging).table(table_staging)
        table_ref_final = bq_client.dataset(dataset_final).table(table_final)
        
        # Asegurar que el dataset staging existe
        try:
            bq_client.get_dataset(f"{project_id}.{dataset_staging}")
        except NotFound:
            dataset = bigquery.Dataset(f"{project_id}.{dataset_staging}")
            dataset.location = "US"
            bq_client.create_dataset(dataset)
            print(f"🆕 Dataset {dataset_staging} creado en proyecto {project_id}")
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        try:
            load_job = bq_client.load_table_from_file(
                open(temp_fixed, "rb"),
                table_ref_staging,
                job_config=job_config
            )
            load_job.result()
            print(f"✅ Cargado a tabla staging: {dataset_staging}.{table_staging}")
        except Exception as e:
            print(f"❌ Error cargando a tabla staging: {str(e)}")
            continue
        
        # Asegurar que la tabla final existe
        try:
            bq_client.get_table(table_ref_final)
            print(f"✅ Tabla final {dataset_final}.{table_final} ya existe.")
        except NotFound:
            schema = bq_client.get_table(table_ref_staging).schema
            # AGREGAR CAMPOS ETL AL ESQUEMA (SOLO 2 CAMPOS)
            campos_etl = [
                bigquery.SchemaField("_etl_synced", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("_etl_operation", "STRING", mode="REQUIRED")
            ]
            schema_completo = list(schema) + campos_etl
            
            table = bigquery.Table(table_ref_final, schema=schema_completo)
            bq_client.create_table(table)
            print(f"🆕 Tabla final {dataset_final}.{table_final} creada con esquema ETL.")
        
        # MERGE incremental a tabla final con Soft Delete y campos ETL
        merge_sql = f'''
            MERGE `{project_id}.{dataset_final}.{table_final}` T
            USING `{project_id}.{dataset_staging}.{table_staging}` S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET 
                {', '.join([f'T.{col.name} = S.{col.name}' for col in bq_client.get_table(table_ref_staging).schema if col.name != 'id'])},
                T._etl_synced = CURRENT_TIMESTAMP(),
                T._etl_operation = 'UPDATE'
            WHEN NOT MATCHED THEN INSERT (
                {', '.join([col.name for col in bq_client.get_table(table_ref_staging).schema])},
                _etl_synced, _etl_operation
            ) VALUES (
                {', '.join([f'S.{col.name}' for col in bq_client.get_table(table_ref_staging).schema])},
                CURRENT_TIMESTAMP(), 'INSERT'
            )
            WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
                T._etl_synced = CURRENT_TIMESTAMP(),
                T._etl_operation = 'DELETE'
        '''
        
        try:
            query_job = bq_client.query(merge_sql)
            query_job.result()
            print(f"🔀 MERGE con Soft Delete ejecutado: {dataset_final}.{table_final} actualizado.")
            
            # Solo borrar tabla staging si el MERGE fue exitoso
            bq_client.delete_table(table_ref_staging, not_found_ok=True)
            print(f"🗑️  Tabla staging {dataset_staging}.{table_staging} eliminada.")
        except Exception as e:
            print(f"❌ Error en MERGE o borrado de staging: {str(e)} (la tabla staging NO se borra para depuración)")
        
        # Borrar archivos temporales
        try:
            os.remove(temp_json)
            os.remove(temp_fixed)
        except Exception:
            pass

def main():
    print("Conectando a BigQuery para obtener compañías INBOX...")
    client = bigquery.Client(project=PROJECT_SOURCE)
    query = f'''
        SELECT * FROM `{PROJECT_SOURCE}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_bigquery_status = TRUE
        ORDER BY company_id
    '''
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
    print(f"🏁 Resumen INBOX: {procesadas}/{total} compañías procesadas exitosamente.")

if __name__ == "__main__":
    main()