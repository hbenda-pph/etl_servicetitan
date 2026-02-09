-- =============================================================================
-- ALTER TABLE: Renombrar campos endpoint_metadata y prefix
-- =============================================================================
-- Este script renombra:
-- 1. endpoint_metadata → endpoint
-- 2. endpoint.prefix → endpoint.submodule

-- Paso 1: Renombrar el campo STRUCT completo
ALTER TABLE `pph-central.management.metadata_consolidated_tables`
RENAME COLUMN endpoint_metadata TO endpoint;

-- Paso 2: Renombrar el campo interno prefix → submodule
-- Nota: BigQuery no permite renombrar campos dentro de STRUCT directamente,
-- necesitamos recrear el STRUCT con el nuevo nombre

-- Primero, crear una columna temporal con la nueva estructura
ALTER TABLE `pph-central.management.metadata_consolidated_tables`
ADD COLUMN IF NOT EXISTS endpoint_new STRUCT<
  module STRING,
  version STRING,
  submodule STRING,              -- Renombrado de prefix
  name STRING,
  endpoint_type STRING
>;

-- Copiar datos de endpoint a endpoint_new, mapeando prefix → submodule
UPDATE `pph-central.management.metadata_consolidated_tables`
SET endpoint_new = STRUCT(
  endpoint.module,
  endpoint.version,
  endpoint.prefix AS submodule,  -- Mapear prefix a submodule
  endpoint.name,
  endpoint.endpoint_type
)
WHERE endpoint IS NOT NULL;

-- Eliminar la columna antigua
ALTER TABLE `pph-central.management.metadata_consolidated_tables`
DROP COLUMN IF EXISTS endpoint;

-- Renombrar endpoint_new a endpoint
ALTER TABLE `pph-central.management.metadata_consolidated_tables`
RENAME COLUMN endpoint_new TO endpoint;

