-- =============================================================================
-- Tabla: pph-central.management.report_catalog
-- Propósito: Catálogo de reportes disponibles en la Reporting API de ServiceTitan
--            por compañía. Poblada por el proceso orchestrate_etl/report_catalog.py.
--
-- Campos de descubrimiento (auto): company_id, tenant_id, report_category,
--   report_id, report_name, parameters, fields, discovered_at
-- Campos de configuración (usuario): is_active, table_name, from_param,
--   to_param, history_from
-- =============================================================================

CREATE TABLE IF NOT EXISTS `pph-central.management.report_catalog` (

  -- ── Identificación ──────────────────────────────────────────────────────
  company_id          INT64     NOT NULL,   -- ID de la compañía

  -- ── Datos del Reporte (descubiertos automáticamente) ────────────────────
  report_category     STRING    NOT NULL,   -- Categoría del reporte (ej: "payroll")
  report_id           INT64     NOT NULL,   -- ID del reporte en este tenant
  report_name         STRING,               -- Nombre del reporte
  report_description  STRING,               -- Descripción (puede ser null)
  report_modified_on  TIMESTAMP,            -- Última modificación del reporte

  -- ── Schema del Reporte (Almacenado como JSON) ───────────────────────────
  parameters          JSON,     -- JSON: [{"name": "From", "dataType": "String", ...}]
  fields              JSON,     -- JSON: [{"name": "Id", "dataType": "Number", ...}]

  -- ── Configuración (el USUARIO llena estos campos) ───────────────────────
  is_active           BOOL,                 -- TRUE = el ETL lo extrae. Default: FALSE
  table_name          STRING,               -- Tabla destino en BQ (ej: "technician_timesheet_daily")
  history_from        DATE,                 -- Fecha inicio del backfill histórico (ej: 2025-01-01)

  -- ── Auditoría ────────────────────────────────────────────────────────────
  discovered_at       TIMESTAMP             -- Cuándo fue descubierto/actualizado este registro

)
OPTIONS (
  description = "Catálogo de reportes disponibles en la Reporting API de ServiceTitan por compañía. Poblada por orchestrate_etl/report_catalog.py. El usuario activa reportes con is_active=TRUE y configura table_name y history_from."
);


-- =============================================================================
-- Query de ejemplo para activar un reporte después del descubrimiento:
-- =============================================================================
/*
UPDATE `pph-central.management.report_catalog`
SET
  is_active    = TRUE,
  table_name   = 'technician_timesheet_daily',
  history_from = DATE '2025-01-01'
WHERE
  report_name LIKE '%Technician Timesheet%'
  AND is_active = FALSE;
*/


-- =============================================================================
-- Query útil: Ver catálogo completo con conteo de reportes por compañía
-- =============================================================================
/*
SELECT
  company_name,
  report_category,
  COUNT(*) AS total_reportes,
  COUNTIF(is_active) AS activos
FROM `pph-central.management.report_catalog`
GROUP BY company_name, report_category
ORDER BY company_name, report_category;
*/
