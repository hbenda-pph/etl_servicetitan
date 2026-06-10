"""
Report Catalog Discovery
=========================
Descubre todos los reportes disponibles en la Reporting API de ServiceTitan
para cada compañía activa y almacena los resultados en:
  pph-central.management.report_catalog

Recorre un árbol de 3 niveles por compañía:
  Nivel 1 → GET /report-categories              (categorías del tenant)
  Nivel 2 → GET /report-category/{cat}/reports  (reportes de cada categoría)
  Nivel 3 → GET /report-category/{cat}/reports/{id}  (params + fields de cada reporte)

El campo is_active queda en FALSE por defecto. El usuario activa manualmente
los reportes deseados con UPDATE en BigQuery y rellena: table_name, from_param,
to_param, history_from.

En re-ejecuciones, los campos de configuración del usuario (is_active, table_name,
from_param, to_param, history_from) se PRESERVAN via MERGE.

Uso:
  python report_catalog.py                          # Todas las compañías activas
  python report_catalog.py --company-id 1          # Solo compañía ID=1
  python report_catalog.py --category payroll      # Solo categoría específica
  python report_catalog.py --dry-run               # Sin escribir a BigQuery
  python report_catalog.py --company-id 1 --dry-run
"""

import argparse
import json
import io
import os
import time
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone
from google.cloud import bigquery

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

METADATA_PROJECT  = "pph-central"
CATALOG_DATASET   = "management"
CATALOG_TABLE     = "report_catalog"
CATALOG_TABLE_TMP = "report_catalog_tmp"
COMPANIES_DATASET = "settings"
COMPANIES_TABLE   = "companies"

# URLs de ServiceTitan (Producción)
ST_AUTH_URL = "https://auth.servicetitan.io/connect/token"
ST_BASE_URL = "https://api.servicetitan.io"

# Schema de la tabla report_catalog para el load job
CATALOG_SCHEMA = [
    bigquery.SchemaField("company_id",         "INT64"),
    bigquery.SchemaField("report_category",    "STRING"),
    bigquery.SchemaField("report_id",          "INT64"),
    bigquery.SchemaField("report_name",        "STRING"),
    bigquery.SchemaField("report_description", "STRING"),
    bigquery.SchemaField("report_modified_on", "TIMESTAMP"),
    bigquery.SchemaField("parameters",         "JSON"),
    bigquery.SchemaField("fields",             "JSON"),
    bigquery.SchemaField("is_active",          "BOOL"),
    bigquery.SchemaField("table_name",         "STRING"),
    bigquery.SchemaField("history_from",       "DATE"),
    bigquery.SchemaField("discovered_at",      "TIMESTAMP"),
]


# =============================================================================
# AUTENTICACIÓN SERVICETITAN
# Mismo patrón que st2json-job/servicetitan_common.py → ServiceTitanAuth
# =============================================================================

class ServiceTitanReportingAuth:
    """
    Cliente de autenticación y llamadas a la Reporting API de ServiceTitan.
    Replica el patrón de ServiceTitanAuth del st2json-job.
    Token se renueva automáticamente cada 10 minutos.
    """

    def __init__(self, app_id, client_id, client_secret, tenant_id, app_key):
        self.app_id        = str(app_id)
        self.client_id     = str(client_id)
        self.client_secret = str(client_secret)
        self.tenant_id     = str(tenant_id)
        self.app_key       = str(app_key)
        self._token        = None
        self._token_time   = 0

    def get_access_token(self):
        """Obtiene o renueva el token OAuth2."""
        if self._token and (time.time() - self._token_time) < 600:
            return self._token

        response = requests.post(
            ST_AUTH_URL,
            auth=HTTPBasicAuth(self.client_id, self.client_secret),
            headers={
                'Content-Type': 'application/x-www-form-urlencoded',
                'ST-App-Key': self.app_key
            },
            data={
                'grant_type':    'client_credentials',
                'client_id':     self.client_id,
                'client_secret': self.client_secret
            },
            timeout=30
        )
        response.raise_for_status()
        self._token      = response.json()['access_token']
        self._token_time = time.time()
        return self._token

    def _get(self, path, params=None):
        """Llamada GET autenticada. Retorna el JSON parseado."""
        token = self.get_access_token()
        url   = f"{ST_BASE_URL}/{path}"
        response = requests.get(
            url,
            headers={
                'Authorization': f'Bearer {token}',
                'ST-App-Id':     self.app_id,
                'ST-App-Key':    self.app_key,
                'Accept':        'application/json'
            },
            params=params,
            timeout=(30, 120)
        )
        response.raise_for_status()
        return response.json()

    # ── Endpoint #1 ───────────────────────────────────────────────────────────
    def get_report_categories(self):
        """
        GET /reporting/v2/tenant/{tenant}/report-categories
        Retorna lista de categorías disponibles para este tenant.
        """
        path   = f"reporting/v2/tenant/{self.tenant_id}/report-categories"
        result = self._get(path)
        if isinstance(result, list):
            return result
        return result.get('data', [])

    def get_category_reports(self, category):
        """
        GET /reporting/v2/tenant/{tenant}/report-category/{cat}/reports
        Retorna lista de reportes disponibles en esta categoría, manejando paginación.
        """
        path   = f"reporting/v2/tenant/{self.tenant_id}/report-category/{category}/reports"
        all_reports = []
        page = 1
        page_size = 50
        
        while True:
            result = self._get(path, params={'page': page, 'pageSize': page_size})
            if isinstance(result, list):
                # Si la API retorna directamente una lista, no hay paginación estándar
                all_reports.extend(result)
                break
                
            data = result.get('data', [])
            all_reports.extend(data)
            
            if not result.get('hasMore', False):
                break
            page += 1
            
        return all_reports

    # ── Endpoint #3 ───────────────────────────────────────────────────────────
    def get_report_details(self, category, report_id):
        """
        GET /reporting/v2/tenant/{tenant}/report-category/{cat}/reports/{id}
        Retorna descripción completa: parámetros de entrada y campos de salida.
        """
        path = (
            f"reporting/v2/tenant/{self.tenant_id}"
            f"/report-category/{category}/reports/{report_id}"
        )
        return self._get(path)


# =============================================================================
# BIGQUERY — HELPERS
# =============================================================================

def get_active_companies(bq_client, company_id_filter=None):
    """
    Lee compañías activas desde pph-central.settings.companies.
    Usa company_bigquery_status = TRUE como filtro.
    """
    where = "WHERE company_bigquery_status = TRUE"
    if company_id_filter:
        where += f" AND company_id = {int(company_id_filter)}"

    query = f"""
        SELECT
            company_id,
            company_name,
            tenant_id,
            app_id,
            client_id,
            client_secret,
            app_key,
            company_project_id
        FROM `{METADATA_PROJECT}.{COMPANIES_DATASET}.{COMPANIES_TABLE}`
        {where}
        ORDER BY company_id
    """
    return list(bq_client.query(query).result())


def ensure_catalog_table_exists(bq_client):
    """Crea la tabla report_catalog si no existe."""
    table_id = f"{METADATA_PROJECT}.{CATALOG_DATASET}.{CATALOG_TABLE}"
    try:
        bq_client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=CATALOG_SCHEMA)
        table.description = (
            "Catálogo de reportes disponibles en la Reporting API de ServiceTitan "
            "por compañía. Poblada por orchestrate_etl/report_catalog.py."
        )
        bq_client.create_table(table)
        print(f"🆕 Tabla {CATALOG_TABLE} creada en {METADATA_PROJECT}.{CATALOG_DATASET}")


def write_catalog_records(bq_client, records, dry_run=False):
    """
    Escribe los registros descubiertos en report_catalog usando MERGE:
      - Actualiza campos de descubrimiento si el registro ya existe
      - PRESERVA is_active, table_name, from_param, to_param, history_from
        (configuración del usuario)
      - Inserta nuevos registros con is_active = FALSE

    Estrategia: carga a tabla temporal → MERGE a tabla final → elimina temporal.
    """
    if not records:
        print("   ⚠️  Sin registros para escribir.")
        return

    if dry_run:
        print(f"   📋 [DRY-RUN] {len(records)} registros (no se escribe en BigQuery)")
        return

    tmp_table_id  = f"{METADATA_PROJECT}.{CATALOG_DATASET}.{CATALOG_TABLE_TMP}"
    main_table_id = f"{METADATA_PROJECT}.{CATALOG_DATASET}.{CATALOG_TABLE}"

    # ── 1. Cargar registros a tabla temporal ──────────────────────────────────
    ndjson   = '\n'.join(json.dumps(r, ensure_ascii=False, default=str) for r in records)
    file_obj = io.BytesIO(ndjson.encode('utf-8'))

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=CATALOG_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    load_job = bq_client.load_table_from_file(file_obj, tmp_table_id, job_config=job_config)
    load_job.result()
    print(f"   📥 {len(records)} registros cargados en tabla temporal")

    # ── 2. MERGE: actualiza existentes, inserta nuevos ────────────────────────
    merge_sql = f"""
        MERGE `{main_table_id}` T
        USING `{tmp_table_id}` S
        ON  T.company_id      = S.company_id
        AND T.report_category = S.report_category
        AND T.report_id       = S.report_id

        -- Actualizar campos de descubrimiento, preservar configuración del usuario
        WHEN MATCHED THEN UPDATE SET
            T.report_name        = S.report_name,
            T.report_description = S.report_description,
            T.report_modified_on = S.report_modified_on,
            T.parameters         = S.parameters,
            T.fields             = S.fields,
            T.discovered_at      = S.discovered_at
            -- is_active, table_name, history_from se PRESERVAN

        -- Insertar nuevos registros con is_active=FALSE
        WHEN NOT MATCHED THEN INSERT (
            company_id,
            report_category, report_id, report_name,
            report_description, report_modified_on,
            parameters, fields,
            is_active, table_name, history_from,
            discovered_at
        ) VALUES (
            S.company_id,
            S.report_category, S.report_id, S.report_name,
            S.report_description, S.report_modified_on,
            S.parameters, S.fields,
            FALSE, NULL, NULL,
            S.discovered_at
        )
    """
    merge_job = bq_client.query(merge_sql)
    merge_job.result()
    print(f"   ✅ MERGE completado en {CATALOG_TABLE}")

    # ── 3. Eliminar tabla temporal ─────────────────────────────────────────────
    bq_client.delete_table(tmp_table_id, not_found_ok=True)


# =============================================================================
# PROCESO PRINCIPAL DE DESCUBRIMIENTO (árbol 3 niveles)
# =============================================================================

def discover_company(company, category_filter=None):
    """
    Recorre el árbol de reporting para una compañía:
      Nivel 1: Categorías
      Nivel 2: Reportes de cada categoría
      Nivel 3: Parámetros + Fields de cada reporte

    Retorna lista de registros listos para escribir en report_catalog.
    """
    company_id   = company.company_id
    company_name = company.company_name
    tenant_id    = company.tenant_id

    print(f"\n{'='*70}")
    print(f"🏢 [{company_id}] {company_name}  (Tenant: {tenant_id})")
    print(f"{'='*70}")

    try:
        auth = ServiceTitanReportingAuth(
            app_id        = company.app_id,
            client_id     = company.client_id,
            client_secret = company.client_secret,
            tenant_id     = tenant_id,
            app_key       = company.app_key
        )
    except Exception as e:
        print(f"   ❌ Error inicializando autenticación: {e}")
        return []

    # ── NIVEL 1: Categorías de reportes ──────────────────────────────────────
    try:
        categories = auth.get_report_categories()
    except Exception as e:
        print(f"   ❌ Error en Endpoint #1 (report-categories): {e}")
        return []

    # Normalizar: categoría puede ser un string o un dict
    def get_cat_id(c):
        return c if isinstance(c, str) else c.get('id', str(c))
        
    def get_cat_name(c):
        return c if isinstance(c, str) else c.get('name', str(c))

    if category_filter:
        categories = [c for c in categories if get_cat_id(c).lower() == category_filter.lower() or get_cat_name(c).lower() == category_filter.lower()]
        print(f"   📂 Categorías (filtro='{category_filter}'): {len(categories)} encontradas")
    else:
        print(f"   📂 Categorías encontradas: {len(categories)}")

    if not categories:
        print(f"   ⚠️  Sin categorías para procesar.")
        return []

    records       = []
    total_reports = 0
    now_ts        = datetime.now(timezone.utc).isoformat()

    for category in categories:
        cat_id = get_cat_id(category)
        cat_name = get_cat_name(category)
        print(f"\n   📁 Categoría: {cat_name} (ID: {cat_id})")

        # ── NIVEL 2: Reportes de esta categoría ──────────────────────────────
        try:
            reports = auth.get_category_reports(cat_id)
        except Exception as e:
            print(f"      ❌ Error en Endpoint #2 (category-reports) para '{cat_id}': {e}")
            continue

        print(f"      📄 Reportes: {len(reports)}")

        for report in reports:
            report_id   = report.get('id')
            report_name = report.get('name', '')

            if not report_id:
                print(f"      ⚠️  Reporte sin ID, se omite: {report}")
                continue

            # ── NIVEL 3: Detalles del reporte (parámetros + fields) ──────────
            try:
                details     = auth.get_report_details(cat_id, report_id)
                parameters  = details.get('parameters', [])
                fields      = details.get('fields', [])
                modified_on = details.get('modifiedOn')
                description = details.get('description')

                print(f"      ✅ [{report_id}] {report_name}"
                      f"  |  params: {len(parameters)}  |  fields: {len(fields)}")

                records.append({
                    "company_id":         company_id,
                    "report_category":    cat_id,
                    "report_id":          int(report_id),
                    "report_name":        report_name,
                    "report_description": description,
                    "report_modified_on": modified_on,
                    "parameters":         parameters,
                    "fields":             fields,
                    # Campos de configuración: NULL en descubrimiento, el usuario los rellena
                    "is_active":    False,
                    "table_name":   None,
                    "from_param":   None,
                    "to_param":     None,
                    "history_from": None,
                    "discovered_at": now_ts,
                })
                total_reports += 1

            except Exception as e:
                print(f"      ❌ Error en Endpoint #3 para [{report_id}] {report_name}: {e}")
                continue

    print(f"\n   📊 Total reportes descubiertos: {total_reports}")
    return records


# =============================================================================
# MAIN
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Report Catalog Discovery — Descubre reportes de la Reporting API de ServiceTitan",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python report_catalog.py                          # Todas las compañías activas
  python report_catalog.py --company-id 1          # Solo compañía ID=1
  python report_catalog.py --category payroll      # Solo categoría 'payroll'
  python report_catalog.py --dry-run               # Sin escribir en BigQuery
  python report_catalog.py --company-id 1 --dry-run

Después de correr, activa los reportes deseados en BigQuery:
  UPDATE `pph-central.management.report_catalog`
  SET is_active=TRUE, table_name='...', from_param='From', to_param='To', history_from='2025-01-01'
  WHERE report_name LIKE '%Timesheet%' AND is_active=FALSE;
        """
    )
    parser.add_argument(
        "--company-id", "-c", type=int, default=None,
        help="Procesar solo esta compañía (por company_id)"
    )
    parser.add_argument(
        "--category", type=str, default=None,
        help="Filtrar por nombre de categoría (ej: payroll)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=False,
        help="Ejecutar sin escribir en BigQuery (solo muestra resultados)"
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print(f"\n{'='*70}")
    print(f"🔍 REPORT CATALOG DISCOVERY")
    if args.dry_run:
        print(f"   ⚠️  MODO DRY-RUN: no se escribe en BigQuery")
    if args.company_id:
        print(f"   🏢 Compañía filtro: ID={args.company_id}")
    if args.category:
        print(f"   📁 Categoría filtro: {args.category}")
    print(f"{'='*70}")

    bq_client = bigquery.Client(project=METADATA_PROJECT)

    # Asegurar que la tabla report_catalog existe
    if not args.dry_run:
        ensure_catalog_table_exists(bq_client)

    # Obtener compañías a procesar
    companies = get_active_companies(bq_client, company_id_filter=args.company_id)
    if not companies:
        print(f"\n❌ No se encontraron compañías activas.")
        return

    print(f"\n📊 Compañías a procesar: {len(companies)}\n")

    all_records   = []
    grand_total   = 0
    errors_count  = 0

    for idx, company in enumerate(companies, 1):
        print(f"\n[{idx}/{len(companies)}]", end="")
        try:
            records = discover_company(company, category_filter=args.category)
            all_records.extend(records)
            grand_total += len(records)
        except Exception as e:
            errors_count += 1
            print(f"\n   ❌ Error crítico procesando {company.company_name}: {e}")

    # Escribir todos los registros al final (un solo MERGE para todas las compañías)
    print(f"\n{'='*70}")
    print(f"💾 Escribiendo {grand_total} registros en BigQuery...")
    write_catalog_records(bq_client, all_records, dry_run=args.dry_run)

    print(f"\n{'='*70}")
    print(f"🏁 Descubrimiento completado")
    print(f"   Compañías procesadas: {len(companies) - errors_count}")
    print(f"   Reportes descubiertos: {grand_total}")
    if errors_count:
        print(f"   ⚠️  Compañías con error: {errors_count}")
    print(f"{'='*70}")

    if not args.dry_run and grand_total > 0:
        print(f"""
💡 Próximo paso — Activa los reportes en BigQuery:

   SELECT c.company_name, r.report_category, r.report_id, r.report_name,
          r.is_active, r.table_name
   FROM `{METADATA_PROJECT}.{CATALOG_DATASET}.{CATALOG_TABLE}` r
   JOIN `{METADATA_PROJECT}.{COMPANIES_DATASET}.{COMPANIES_TABLE}` c
     ON r.company_id = c.company_id
   ORDER BY c.company_name, r.report_category, r.report_name;

   UPDATE `{METADATA_PROJECT}.{CATALOG_DATASET}.{CATALOG_TABLE}`
   SET
     is_active    = TRUE,
     table_name   = 'tu_tabla_destino',
     from_param   = 'From',
     to_param     = 'To',
     history_from = DATE '2025-01-01'
   WHERE report_name LIKE '%Timesheet%'
     AND is_active = FALSE;
""")


if __name__ == "__main__":
    main()
