from google.cloud import bigquery

# Configuración
PROJECT_ID = "platform-partners-des"
DATASET_ID = "logs"
TABLE_ID = "etl_servicetitan"

def create_logs_table():
    """Crea el dataset 'logs' y la tabla 'etl_servicetitan' para centralizar logs del ETL."""
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # 1. Crear dataset 'logs' si no existe
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        print(f"✅ Dataset {DATASET_ID} ya existe en proyecto {PROJECT_ID}")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Ajusta la región según necesites
        client.create_dataset(dataset)
        print(f"🆕 Dataset {DATASET_ID} creado en proyecto {PROJECT_ID}")
    
    # 2. Definir esquema de la tabla
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("company_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("company_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("project_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("endpoint", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("info", "STRING", mode="NULLABLE"),
    ]
    
    # 3. Crear tabla si no existe
    table_ref = dataset_ref.table(TABLE_ID)
    try:
        client.get_table(table_ref)
        print(f"✅ Tabla {DATASET_ID}.{TABLE_ID} ya existe")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"🆕 Tabla {DATASET_ID}.{TABLE_ID} creada con esquema completo")
    
    print(f"\n📋 Esquema de la tabla {DATASET_ID}.{TABLE_ID}:")
    for field in schema:
        print(f"  - {field.name}: {field.field_type} ({field.mode})")
    
    print(f"\n✅ Tabla de logs lista para usar en: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

if __name__ == "__main__":
    create_logs_table() 
