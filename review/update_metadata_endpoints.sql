-- =============================================================================
-- SCRIPT SQL PARA ACTUALIZAR metadata_consolidated_tables
-- Agrega/Actualiza endpoints de ServiceTitan con campo endpoint STRUCT
-- =============================================================================
-- NOTA: El campo se llama "endpoint" (no endpoint_metadata)
--       El subcampo se llama "submodule" (no prefix)
--       "tenant" es una variable multitenant, NO es un submodule

-- Ejemplo: Agregar/Actualizar un nuevo endpoint
-- Reemplaza los valores según necesites

MERGE `pph-central.management.metadata_consolidated_tables` T
USING (
  SELECT 
    STRUCT(
      'settings' AS module,              -- Ej: 'settings', 'jpm', 'marketing', 'payroll', 'inventory'
      'v2' AS version,                   -- Versión de la API (generalmente 'v2')
      NULL AS submodule,                 -- Solo algunos endpoints tienen submodule (ej: 'jobs')
      'business-units' AS name,          -- Nombre del endpoint (ej: 'business-units', 'job-types')
      'normal' AS endpoint_type          -- 'normal' o 'export' (tipo de paginación)
    ) AS endpoint,
    'business_units' AS table_name,     -- Nombre estandarizado de la tabla (sin prefijos)
    TRUE AS active
) S
ON T.table_name = S.table_name
WHEN MATCHED THEN
  UPDATE SET
    endpoint = S.endpoint,
    active = S.active
WHEN NOT MATCHED THEN
  INSERT (
    endpoint,
    table_name,
    active
  )
  VALUES (
    S.endpoint,
    S.table_name,
    S.active
  );

-- =============================================================================
-- EJEMPLOS DE USO:
-- =============================================================================

-- Ejemplo 1: Agregar endpoint "business-units"
/*
MERGE `pph-central.management.metadata_consolidated_tables` T
USING (
  SELECT 
    STRUCT(
      'settings' AS module,
      'v2' AS version,
      NULL AS submodule,              -- Sin submodule
      'business-units' AS name,
      'normal' AS endpoint_type
    ) AS endpoint,
    'business_units' AS table_name,
    TRUE AS active
) S
ON T.table_name = S.table_name
WHEN MATCHED THEN
  UPDATE SET
    endpoint = S.endpoint,
    active = S.active
WHEN NOT MATCHED THEN
  INSERT (
    endpoint,
    table_name,
    active
  )
  VALUES (
    S.endpoint,
    S.table_name,
    S.active
  );
*/

-- Ejemplo 2: Agregar múltiples endpoints a la vez
-- NOTA: Nombres de tablas sin prefijos (como Fivetran)
/*
INSERT INTO `pph-central.management.metadata_consolidated_tables`
  (endpoint, table_name, active)
VALUES
  (STRUCT('settings' AS module, 'v2' AS version, NULL AS submodule, 'business-units' AS name, 'normal' AS endpoint_type), 'business_units', TRUE),
  (STRUCT('jpm' AS module, 'v2' AS version, NULL AS submodule, 'job-types' AS name, 'normal' AS endpoint_type), 'job_types', TRUE),
  (STRUCT('settings' AS module, 'v2' AS version, NULL AS submodule, 'technicians' AS name, 'normal' AS endpoint_type), 'technicians', TRUE),
  (STRUCT('settings' AS module, 'v2' AS version, NULL AS submodule, 'employees' AS name, 'normal' AS endpoint_type), 'employees', TRUE),
  (STRUCT('marketing' AS module, 'v2' AS version, NULL AS submodule, 'campaigns' AS name, 'normal' AS endpoint_type), 'campaigns', TRUE),
  (STRUCT('payroll' AS module, 'v2' AS version, 'jobs' AS submodule, 'jobs/timesheets' AS name, 'normal' AS endpoint_type), 'timesheets', TRUE),
  (STRUCT('inventory' AS module, 'v2' AS version, NULL AS submodule, 'purchase-orders' AS name, 'normal' AS endpoint_type), 'purchase_orders', TRUE),
  (STRUCT('inventory' AS module, 'v2' AS version, NULL AS submodule, 'returns' AS name, 'normal' AS endpoint_type), 'returns', TRUE),
  (STRUCT('inventory' AS module, 'v2' AS version, NULL AS submodule, 'vendors' AS name, 'normal' AS endpoint_type), 'vendors', TRUE),
  (STRUCT('jpm' AS module, 'v2' AS version, NULL AS submodule, 'export/job-canceled-logs' AS name, 'export' AS endpoint_type), 'export_job_canceled_logs', TRUE),
  (STRUCT('jpm' AS module, 'v2' AS version, 'jobs' AS submodule, 'jobs/cancel-reasons' AS name, 'normal' AS endpoint_type), 'cancel_reasons', TRUE);


INSERT INTO `pph-central.management.metadata_consolidated_tables`
  (endpoint, table_name, active, silver_use_bronze)
VALUES
  (STRUCT('payroll' AS module, 'v2' AS version, NULL AS submodule, 'payroll-settings' AS name, 'normal' AS endpoint_type), 'payroll_settings', TRUE, TRUE),
  (STRUCT('payroll' AS module, 'v2' AS version, NULL AS submodule, 'gross-pay-items' AS name, 'normal' AS endpoint_type), 'gross_pay_items', TRUE, TRUE)

*/

-- =============================================================================
-- CONSULTA PARA VERIFICAR ENDPOINTS EXISTENTES:
-- =============================================================================
/*
SELECT 
  endpoint.module AS module,
  endpoint.version AS version,
  endpoint.submodule AS submodule,
  endpoint.name AS endpoint_name,
  endpoint.endpoint_type AS endpoint_type,
  table_name,
  active
FROM `pph-central.management.metadata_consolidated_tables`
WHERE endpoint IS NOT NULL
ORDER BY endpoint.module, endpoint.name;
*/

-- =============================================================================
-- DESACTIVAR UN ENDPOINT (sin borrarlo):
-- =============================================================================
/*
UPDATE `pph-central.management.metadata_consolidated_tables`
SET active = FALSE
WHERE table_name = 'nombre_tabla_a_desactivar';
*/

-- =============================================================================
-- INSERT SIMPLE: non-job-timesheets
-- =============================================================================
INSERT INTO `pph-central.management.metadata_consolidated_tables`
  (endpoint, table_name, active)
VALUES
  (STRUCT('payroll' AS module, 'v2' AS version, NULL AS submodule, 'non-job-timesheets' AS name, 'normal' AS endpoint_type), 'non_job_timesheets', TRUE);

-- =============================================================================
-- UPDATE MASIVO: Actualizar registros existentes con endpoint
-- =============================================================================
-- NOTA: 
-- - "tenant" NO es un submodule, es una variable multitenant
-- - Solo algunos endpoints tienen submodule (ej: "jobs" para jobs/timesheets)
-- - Nombres de tablas sin prefijos (timesheets en lugar de jobs_timesheets)

INSERT INTO `pph-central.management.metadata_consolidated_tables` (endpoint, table_name, active)
VALUES (STRUCT('payroll' AS module, 'v2' AS version, NULL AS submodule, 'non-job-timesheets' AS name, 'normal' AS endpoint_type), 'non_job_timesheets', TRUE);

UPDATE `pph-central.management.metadata_consolidated_tables` SET silver_use_bronze = TRUE WHERE table_name = 'job_cancel_reason';
UPDATE `pph-central.management.metadata_consolidated_tables` SET silver_use_bronze = TRUE WHERE table_name = 'non_job_timesheets';

UPDATE `pph-central.management.metadata_consolidated_tables` SET table_name = 'job_canceled_log' WHERE table_name = 'export_job_canceled_log';
UPDATE `pph-central.management.metadata_consolidated_tables` SET table_name = 'timesheet' WHERE table_name = 'jobs_timesheet';
 
select table_name from  `pph-central.management.metadata_consolidated_tables` T WHERE T.silver_use_bronze order by table_name -- T.endpoint IS NULL 

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('settings' AS module, 'v2' AS version, NULL AS submodule, 'business-units' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'business_unit';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('marketing' AS module, 'v2' AS version, NULL AS submodule, 'campaigns' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'campaign';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('jpm' AS module, 'v2' AS version, NULL AS submodule, 'job-cancel-reasons' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'job_cancel_reason';  

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('jpm' AS module, 'v2' AS version, 'export' AS submodule, 'job-canceled-logs' AS name, 'export' AS endpoint_type)
WHERE table_name = 'job_canceled_log';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('jpm' AS module, 'v2' AS version, NULL AS submodule, 'job-types' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'job_type';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('payroll' AS module, 'v2' AS version, NULL AS submodule, 'non-job-timesheets' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'non_job_timesheets';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('inventory' AS module, 'v2' AS version, NULL AS submodule, 'purchase-orders' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'purchase_order';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('inventory' AS module, 'v2' AS version, NULL AS submodule, 'returns' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'return';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('settings' AS module, 'v2' AS version, NULL AS submodule, 'technicians' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'technician';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('payroll' AS module, 'v2' AS version, 'jobs' AS submodule, 'timesheets' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'timesheet';

UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint = STRUCT('inventory' AS module, 'v2' AS version, NULL AS submodule, 'vendors' AS name, 'normal' AS endpoint_type)
WHERE table_name = 'vendor';