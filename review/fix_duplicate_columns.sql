-- Script para detectar y ayudar a limpiar columnas duplicadas (camelCase vs snake_case)
-- Ejecutar en BigQuery para cada tabla afectada

-- PASO 1: Detectar columnas duplicadas en una tabla específica
-- Reemplazar 'PROJECT_ID', 'DATASET_NAME', 'TABLE_NAME' con valores reales

SELECT 
    column_name,
    data_type,
    CASE 
        WHEN column_name REGEXP '^[a-z][a-z0-9_]*$' THEN 'snake_case'
        WHEN column_name REGEXP '^[a-z][a-zA-Z0-9]*$' THEN 'camelCase'
        ELSE 'other'
    END as naming_convention,
    -- Detectar si hay una versión duplicada en otro formato
    CASE 
        WHEN column_name REGEXP '^[a-z][a-z0-9_]*$' THEN 
            -- Buscar camelCase equivalente (ej: invoice_id -> invoiceId)
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                column_name, '_id', 'Id'), '_on', 'On'), '_name', 'Name'), '_code', 'Code'),
                '_type', 'Type'), '_number', 'Number'), '_date', 'Date'), '_total', 'Total'),
                '_tax', 'Tax'), '_', '')
        ELSE 
            -- Buscar snake_case equivalente (camelCase -> snake_case es más complejo, requeriría regex inverso)
            NULL
    END as potential_duplicate
FROM `PROJECT_ID.DATASET_NAME.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'TABLE_NAME'
  AND NOT column_name LIKE '_etl_%'
ORDER BY column_name;

-- PASO 2: Para limpiar, elegir UNA de estas opciones:

-- OPCIÓN A: Eliminar columnas camelCase y quedarse con snake_case (RECOMENDADO)
-- Esto asume que snake_case es el formato deseado para campos de nivel superior

/*
ALTER TABLE `PROJECT_ID.DATASET_NAME.TABLE_NAME`
DROP COLUMN IF EXISTS invoiceId,
DROP COLUMN IF EXISTS jobId,
DROP COLUMN IF EXISTS projectId;
-- Agregar todas las columnas camelCase que quieras eliminar
*/

-- OPCIÓN B: Si prefieres camelCase para todo (NO recomendado, pero si decides hacerlo)
-- Primero migrar datos, luego eliminar snake_case:
/*
UPDATE `PROJECT_ID.DATASET_NAME.TABLE_NAME`
SET 
    invoiceId = invoice_id,
    jobId = job_id,
    projectId = project_id
WHERE invoice_id IS NOT NULL OR job_id IS NOT NULL OR project_id IS NOT NULL;

ALTER TABLE `PROJECT_ID.DATASET_NAME.TABLE_NAME`
DROP COLUMN IF EXISTS invoice_id,
DROP COLUMN IF EXISTS job_id,
DROP COLUMN IF EXISTS project_id;
*/

-- OPCIÓN C: Script Python para hacer la migración de forma segura
-- Ver fix_duplicate_columns.py
