"""
Script para limpiar columnas duplicadas en tablas afectadas por el error de conversión.
Detecta columnas duplicadas (camelCase y snake_case) y ayuda a migrarlas.

USO:
    python fix_duplicate_columns.py --project PROJECT_ID --dataset DATASET_NAME --table TABLE_NAME [--dry-run]
"""

import argparse
import re
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def to_snake_case(name):
    """Convierte camelCase a snake_case"""
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def to_camel_case(name):
    """Convierte snake_case a camelCase"""
    components = name.split('_')
    return components[0] + ''.join(x.capitalize() for x in components[1:])

def detect_duplicates(client, project_id, dataset_name, table_name):
    """Detecta columnas duplicadas (camelCase y snake_case)"""
    table_ref = client.dataset(dataset_name, project=project_id).table(table_name)
    
    try:
        table = client.get_table(table_ref)
        columns = [col.name for col in table.schema if not col.name.startswith('_etl_')]
        
        snake_case_cols = {}
        camel_case_cols = {}
        
        for col in columns:
            # Detectar snake_case
            if re.match(r'^[a-z][a-z0-9_]*$', col):
                camel_equiv = to_camel_case(col)
                if camel_equiv != col:
                    snake_case_cols[col] = camel_equiv
            # Detectar camelCase
            elif re.match(r'^[a-z][a-zA-Z0-9]*$', col):
                snake_equiv = to_snake_case(col)
                if snake_equiv != col:
                    camel_case_cols[col] = snake_equiv
        
        # Encontrar duplicados
        duplicates = []
        for snake_col, camel_equiv in snake_case_cols.items():
            if camel_equiv in columns:
                duplicates.append({
                    'snake_case': snake_col,
                    'camelCase': camel_equiv,
                    'recommendation': 'snake_case'  # Mantener snake_case por defecto
                })
        
        return duplicates, columns
    except NotFound:
        print(f"❌ Tabla {project_id}.{dataset_name}.{table_name} no encontrada")
        return [], []

def fix_duplicates(client, project_id, dataset_name, table_name, dry_run=True, keep_format='snake_case'):
    """Limpia columnas duplicadas"""
    duplicates, all_columns = detect_duplicates(client, project_id, dataset_name, table_name)
    
    if not duplicates:
        print(f"✅ No se encontraron columnas duplicadas en {dataset_name}.{table_name}")
        return
    
    print(f"\n{'='*80}")
    print(f"📋 Tabla: {project_id}.{dataset_name}.{table_name}")
    print(f"🔍 Columnas duplicadas detectadas: {len(duplicates)}")
    print(f"{'='*80}\n")
    
    for dup in duplicates:
        print(f"  • {dup['snake_case']} (snake_case) <-> {dup['camelCase']} (camelCase)")
    
    if dry_run:
        print(f"\n🔍 DRY RUN - No se realizarán cambios")
        print(f"💡 Recomendación: Mantener {keep_format}")
        if keep_format == 'snake_case':
            print(f"   Eliminar: {', '.join([d['camelCase'] for d in duplicates])}")
        else:
            print(f"   Eliminar: {', '.join([d['snake_case'] for d in duplicates])}")
        print(f"\n   Ejecutar sin --dry-run para aplicar cambios")
        return
    
    print(f"\n🔧 Aplicando corrección (manteniendo {keep_format})...")
    
    # Construir ALTER TABLE para eliminar columnas duplicadas
    if keep_format == 'snake_case':
        cols_to_drop = [dup['camelCase'] for dup in duplicates]
    else:
        cols_to_drop = [dup['snake_case'] for dup in duplicates]
    
    if cols_to_drop:
        # BigQuery requiere un DROP COLUMN por sentencia
        for col in cols_to_drop:
            alter_sql = f"ALTER TABLE `{project_id}.{dataset_name}.{table_name}` DROP COLUMN IF EXISTS {col}"
            try:
                if not dry_run:
                    query_job = client.query(alter_sql)
                    query_job.result()
                    print(f"  ✅ Columna eliminada: {col}")
            except Exception as e:
                print(f"  ❌ Error eliminando {col}: {str(e)}")
    
    print(f"\n✅ Limpieza completada")

def main():
    parser = argparse.ArgumentParser(description='Limpia columnas duplicadas en tablas BigQuery')
    parser.add_argument('--project', required=True, help='Project ID de BigQuery')
    parser.add_argument('--dataset', required=True, help='Dataset name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--dry-run', action='store_true', help='Solo detectar, no hacer cambios')
    parser.add_argument('--keep-format', choices=['snake_case', 'camelCase'], default='snake_case',
                       help='Formato a mantener (default: snake_case)')
    
    args = parser.parse_args()
    
    client = bigquery.Client(project=args.project)
    fix_duplicates(client, args.project, args.dataset, args.table, args.dry_run, args.keep_format)

if __name__ == "__main__":
    main()
