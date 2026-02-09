"""
Script para limpiar columnas duplicadas en TODOS los proyectos/compañías afectadas.
Procesa automáticamente todas las compañías activas.

USO:
    python fix_all_projects_duplicates.py [--dry-run] [--dataset bronze] [--keep-format snake_case]
"""

import argparse
import re
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Configuración (igual que el script principal)
PROJECT_SOURCE = "platform-partners-qua"  # Ajustar si es necesario
DATASET_NAME = "settings"
TABLE_NAME = "companies"

def get_bigquery_project_id():
    """Obtiene el project_id real para usar en queries SQL"""
    project_source = PROJECT_SOURCE
    if project_source == "platform-partners-pro":
        return "constant-height-455614-i0"
    return project_source

def to_snake_case(name):
    """Convierte camelCase a snake_case"""
    name = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def to_camel_case(name):
    """Convierte snake_case a camelCase"""
    components = name.split('_')
    return components[0] + ''.join(x.capitalize() for x in components[1:])

def detect_duplicates_in_table(client, project_id, dataset_name, table_name):
    """Detecta columnas duplicadas en una tabla"""
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
                    'camelCase': camel_equiv
                })
        
        return duplicates
    except NotFound:
        return None
    except Exception as e:
        print(f"    ⚠️  Error accediendo tabla: {str(e)}")
        return None

def fix_table_duplicates(client, project_id, dataset_name, table_name, dry_run=True, keep_format='snake_case'):
    """Limpia columnas duplicadas en una tabla"""
    duplicates = detect_duplicates_in_table(client, project_id, dataset_name, table_name)
    
    if duplicates is None:
        return False  # Tabla no encontrada o error
    
    if not duplicates:
        return None  # Sin duplicados
    
    # Determinar qué columnas eliminar
    if keep_format == 'snake_case':
        cols_to_drop = [dup['camelCase'] for dup in duplicates]
    else:
        cols_to_drop = [dup['snake_case'] for dup in duplicates]
    
    if not dry_run:
        for col in cols_to_drop:
            alter_sql = f"ALTER TABLE `{project_id}.{dataset_name}.{table_name}` DROP COLUMN IF EXISTS {col}"
            try:
                query_job = client.query(alter_sql)
                query_job.result()
            except Exception as e:
                print(f"      ❌ Error eliminando {col}: {str(e)}")
                return False
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Limpia columnas duplicadas en TODOS los proyectos/compañías')
    parser.add_argument('--dry-run', action='store_true', help='Solo detectar, no hacer cambios')
    parser.add_argument('--dataset', default='bronze', help='Dataset a revisar (default: bronze)')
    parser.add_argument('--keep-format', choices=['snake_case', 'camelCase'], default='snake_case',
                       help='Formato a mantener (default: snake_case)')
    parser.add_argument('--tables', nargs='+', help='Tablas específicas a revisar (default: todas)')
    
    args = parser.parse_args()
    
    # Obtener lista de compañías activas
    PROJECT_ID_FOR_QUERY = get_bigquery_project_id()
    client = bigquery.Client(project=PROJECT_ID_FOR_QUERY)
    
    query = f'''
        SELECT company_id, company_name, company_project_id 
        FROM `{PROJECT_ID_FOR_QUERY}.{DATASET_NAME}.{TABLE_NAME}`
        WHERE company_bigquery_status = TRUE
        ORDER BY company_id
    '''
    
    companies = list(client.query(query).result())
    total_companies = len(companies)
    
    print(f"\n{'='*80}")
    print(f"🔍 Buscando columnas duplicadas en {total_companies} compañías")
    print(f"📊 Dataset: {args.dataset}")
    print(f"🔧 Modo: {'DRY RUN (solo detectar)' if args.dry_run else 'EJECUTAR (eliminar duplicados)'}")
    print(f"📋 Formato a mantener: {args.keep_format}")
    print(f"{'='*80}\n")
    
    affected_companies = []
    affected_tables = []
    
    for idx, company in enumerate(companies, 1):
        company_id = company.company_id
        company_name = company.company_name
        project_id = company.company_project_id
        
        print(f"[{idx}/{total_companies}] 🏢 {company_name} (project: {project_id})")
        
        # Obtener tablas del dataset
        if args.tables:
            table_list = args.tables
        else:
            # Listar todas las tablas del dataset
            try:
                dataset_ref = client.dataset(args.dataset, project=project_id)
                tables = list(client.list_tables(dataset_ref))
                table_list = [table.table_id for table in tables]
            except Exception as e:
                print(f"  ❌ Error listando tablas: {str(e)}")
                continue
        
        company_affected = False
        
        for table_name in table_list:
            duplicates = detect_duplicates_in_table(client, project_id, args.dataset, table_name)
            
            if duplicates is None:
                continue  # Tabla no existe o error
            
            if duplicates:
                company_affected = True
                affected_tables.append({
                    'project_id': project_id,
                    'company_name': company_name,
                    'dataset': args.dataset,
                    'table': table_name,
                    'duplicates_count': len(duplicates)
                })
                
                print(f"  ⚠️  Tabla {table_name}: {len(duplicates)} columnas duplicadas")
                if not args.dry_run:
                    result = fix_table_duplicates(client, project_id, args.dataset, table_name, dry_run=False, keep_format=args.keep_format)
                    if result:
                        print(f"    ✅ Columnas eliminadas")
                    else:
                        print(f"    ❌ Error al eliminar")
        
        if company_affected:
            affected_companies.append({
                'company_id': company_id,
                'company_name': company_name,
                'project_id': project_id
            })
        
        if not company_affected and not args.tables:
            print(f"  ✅ Sin problemas detectados")
    
    # Resumen
    print(f"\n{'='*80}")
    print(f"📊 RESUMEN")
    print(f"{'='*80}")
    print(f"Compañías afectadas: {len(affected_companies)}/{total_companies}")
    print(f"Tablas afectadas: {len(affected_tables)}")
    
    if affected_tables:
        print(f"\n📋 Tablas con duplicados:")
        for table_info in affected_tables:
            print(f"  • {table_info['project_id']}.{table_info['dataset']}.{table_info['table']} ({table_info['duplicates_count']} duplicados)")
    
    if args.dry_run and affected_tables:
        print(f"\n💡 Ejecutar sin --dry-run para limpiar duplicados")
    
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
