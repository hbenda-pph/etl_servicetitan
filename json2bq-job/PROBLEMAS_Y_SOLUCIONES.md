# Análisis de Problemas - Endpoint gross-pay-items

## Problemas Detectados

### 1. **Parsing Incremental Falla** (Item ~5,343)
- **Síntoma**: El parsing se atasca alrededor del item 5,343, procesando muy lento (1 item/seg)
- **Causa**: `json.JSONDecoder.raw_decode()` no maneja bien archivos grandes con items complejos
- **Solución intentada**: Mejorar lógica de recuperación de errores, buscar separadores manualmente
- **Resultado**: No funcionó, sigue atascándose

### 2. **Error de Esquema** (location_zip)
- **Síntoma**: BigQuery infiere `location_zip` como INTEGER pero viene como STRING ("Tester")
- **Causa**: BigQuery autodetect infiere tipos basándose en primeros registros
- **Solución intentada**: Detectar error, corregir esquema automáticamente
- **Resultado**: Funciona pero es complejo

### 3. **Archivo Temporal Parcial**
- **Síntoma**: Si ejecución previa falla, archivo temporal queda con datos parciales
- **Causa**: No se limpia al inicio
- **Solución**: Eliminar archivo temporal si existe antes de empezar
- **Resultado**: ✅ Resuelto

### 4. **Progreso Mostrándose Línea por Línea**
- **Síntoma**: Muestra progreso cada item en lugar de cada 1000 items
- **Causa**: `items_since_last_progress` se reseteaba en cada chunk
- **Solución**: Inicializar fuera del loop de chunks
- **Resultado**: ✅ Resuelto

## Análisis Real

**Hecho clave**: El usuario pudo cargar manualmente el JSON a BigQuery y funcionó (14,511 registros).

**Conclusión**: El problema NO es:
- ❌ El JSON (está bien formado)
- ❌ BigQuery (puede procesarlo)
- ❌ El esquema (BigQuery lo infiere correctamente)

**El problema ES**:
- ✅ NUESTRA transformación está complicando las cosas innecesariamente
- ✅ El parsing incremental no es necesario para 906MB
- ✅ La transformación a snake_case puede estar causando problemas

## Solución Propuesta: SIMPLIFICAR

1. **Para archivos < 1GB**: Cargar JSON completo en memoria (ya implementado)
2. **Cargar directamente a BigQuery** sin tanta transformación intermedia
3. **Dejar que BigQuery maneje** la inferencia de esquema y corrección de tipos
