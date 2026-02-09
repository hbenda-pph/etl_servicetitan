# Optimizaciones para ETL JSON2BQ Job

## 🚀 Opciones de Optimización Disponibles

### 1. **Paralelismo con Cloud Run Jobs** ⭐ RECOMENDADO

Cloud Run Jobs soporta ejecutar múltiples tareas en paralelo, cada una procesando un subconjunto de compañías.

#### Configuración

Edita `build_deploy.sh` y ajusta estas variables:

```bash
PARALLELISM="3"  # Número de tareas ejecutándose simultáneamente
TASKS="3"         # Total de tareas (cada una procesa ~10 compañías si hay 30)
```

#### Cómo Funciona

- **30 compañías, 3 tareas**: Cada tarea procesa ~10 compañías
- **Tarea 1**: Compañías 1-10
- **Tarea 2**: Compañías 11-20  
- **Tarea 3**: Compañías 21-30

#### Ventajas

✅ **Reducción de tiempo total**: Si cada compañía toma 2 minutos, 30 compañías = 60 minutos secuencial. Con 3 tareas paralelas = ~20 minutos

✅ **Mejor uso de recursos**: Múltiples CPUs trabajando simultáneamente

✅ **Escalable**: Puedes ajustar `TASKS` según el número de compañías

#### Desventajas

⚠️ **Costo**: Más recursos = más costo (pero tiempo total menor)

⚠️ **Complejidad**: Logs distribuidos entre múltiples tareas

#### Ejemplo de Configuración

```bash
# Para 30 compañías, usar 3-5 tareas
PARALLELISM="3"
TASKS="3"

# Para 50+ compañías, usar 5-10 tareas
PARALLELISM="5"
TASKS="5"
```

---

### 2. **Aumentar Recursos (CPU/Memoria)**

#### Configuración Actual
```bash
MEMORY="4Gi"
CPU="4"
```

#### Opciones

| Configuración | CPU | Memoria | Uso |
|--------------|-----|---------|-----|
| **Actual** | 4 | 4Gi | Bueno para procesamiento normal |
| **Alta** | 8 | 8Gi | Para transformaciones JSON complejas |
| **Muy Alta** | 16 | 16Gi | Para archivos JSON muy grandes (>100MB) |

#### Cuándo Aumentar

- ✅ Si el MERGE es lento (más CPU ayuda)
- ✅ Si hay errores de memoria (OOM)
- ✅ Si la transformación JSON es lenta

#### Cómo Aplicar

Edita `build_deploy.sh`:
```bash
MEMORY="8Gi"
CPU="8"
```

---

### 3. **Aumentar Timeout**

#### Configuración Actual
```bash
TASK_TIMEOUT="2400"  # 40 minutos
```

#### Opciones

- **60 minutos**: `TASK_TIMEOUT="3600"`
- **90 minutos**: `TASK_TIMEOUT="5400"`
- **120 minutos**: `TASK_TIMEOUT="7200"`

⚠️ **Nota**: Aumentar timeout no resuelve el problema de rendimiento, solo da más tiempo. Mejor usar paralelismo.

---

### 4. **Clustering en BigQuery**

#### ¿Ayuda?

**Respuesta corta**: **NO mucho** para este caso de uso.

#### Por qué

- ✅ El MERGE ya usa `ON T.id = S.id`, que es eficiente
- ✅ BigQuery indexa automáticamente por `id` en MERGE
- ✅ Clustering ayuda principalmente en queries con WHERE, no en MERGE
- ❌ La carga de JSON desde staging no se beneficia del clustering

#### Cuándo Considerar Clustering

- Si haces queries frecuentes con filtros por fecha u otros campos
- Si las tablas son muy grandes (>100GB) y necesitas optimizar queries

#### Cómo Aplicar (si decides hacerlo)

```sql
ALTER TABLE `project.dataset.table`
SET OPTIONS (
  description="Tabla con clustering",
  clustering_fields=["id", "_etl_synced"]
);
```

---

## 📊 Comparación de Estrategias

| Estrategia | Reducción Tiempo | Costo | Complejidad | Recomendación |
|------------|------------------|-------|------------|---------------|
| **Paralelismo (3 tareas)** | 60% | +200% | Media | ⭐⭐⭐⭐⭐ |
| **Aumentar CPU a 8** | 20-30% | +100% | Baja | ⭐⭐⭐ |
| **Aumentar Memoria a 8Gi** | 10-15% | +100% | Baja | ⭐⭐ |
| **Aumentar Timeout** | 0% | 0% | Baja | ⭐ (solo si necesario) |
| **Clustering** | <5% | 0% | Media | ⭐ (no recomendado) |

---

## 🎯 Recomendación Final

### Para 30 compañías que toman ~40 minutos:

1. **Usar Paralelismo**: `PARALLELISM="3"`, `TASKS="3"`
   - Tiempo estimado: ~15 minutos
   - Costo: ~3x por ejecución, pero tiempo total menor

2. **Mantener recursos actuales**: CPU=4, Memory=4Gi
   - Si hay problemas de memoria, aumentar a 8Gi

3. **Timeout**: Mantener 40 minutos (2400s)
   - Con paralelismo, no debería ser necesario aumentar

### Para 50+ compañías:

1. **Aumentar paralelismo**: `PARALLELISM="5"`, `TASKS="5"`
2. **Considerar más CPU**: `CPU="8"` si el MERGE es lento
3. **Timeout**: Aumentar a 60 minutos si es necesario

---

## 🔧 Cómo Aplicar Cambios

1. Edita `build_deploy.sh` con los valores deseados
2. Ejecuta: `./build_deploy.sh pro` (o el ambiente correspondiente)
3. El script aplicará automáticamente los cambios

---

## 📝 Notas Importantes

- **Paralelismo**: Cloud Run Jobs divide automáticamente las compañías entre tareas
- **Logs**: Cada tarea genera sus propios logs, revisa todos para monitoreo completo
- **Errores**: Si una tarea falla, las otras continúan (mejor resiliencia)
- **Costo**: Paralelismo = más recursos, pero tiempo total menor = mejor ROI

