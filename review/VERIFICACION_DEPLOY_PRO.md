# Verificación Pre-Deploy a PRO

## ✅ Verificación de 3 Puntos Críticos

### 1. Parámetro PRO en build_deploy.sh

**Respuesta**: El script **NO requiere** el parámetro obligatorio, pero **ES RECOMENDABLE** pasarlo explícitamente.

#### Comportamiento:

- **Con parámetro**: `./build_deploy.sh pro` → Usa PRO directamente ✅
- **Sin parámetro**: Detecta automáticamente desde `gcloud config get-value project`
  - Si proyecto activo = `platform-partners-pro` → Detecta PRO ✅
  - Si proyecto activo = otro/no reconocido → **Usa DEV por defecto** ⚠️

#### Recomendación:

```bash
# ✅ SEGURO: Pasar parámetro explícitamente
./build_deploy.sh pro

# ⚠️ RIESGOSO: Sin parámetro (puede usar DEV si proyecto no está configurado)
./build_deploy.sh
```

**Conclusión**: Para deploy a PRO, **SIEMPRE usar**: `./build_deploy.sh pro`

---

### 2. Scheduler (Cloud Scheduler)

**Respuesta**: ✅ **SÍ, el scheduler se crea/actualiza SOLO en PRO**. En DEV/QUA se desactiva/elimina.

#### Lógica en build_deploy.sh (líneas 208-247):

```bash
if [ "$ENVIRONMENT" = "pro" ]; then
    # Solo en producción: crear/actualizar scheduler
    # Crea o actualiza el scheduler cada 6 horas
else
    # En dev/qua: desactivar o eliminar scheduler si existe
    # Pausa o elimina el scheduler
fi
```

#### Comportamiento por Ambiente:

| Ambiente | Acción Scheduler |
|----------|------------------|
| **PRO** | ✅ Crea/Actualiza scheduler (cada 6 horas) |
| **DEV** | ❌ Desactiva/Elimina scheduler si existe |
| **QUA** | ❌ Desactiva/Elimina scheduler si existe |

**Conclusión**: ✅ El scheduler solo se activa en PRO. En DEV/QUA se desactiva automáticamente.

---

### 3. Tablas de Referencia por Ambiente

**Respuesta**: ✅ **SÍ, las tablas de referencia están por ambiente**, excepto metadata que es centralizada.

#### Tabla `companies` (settings.companies):

- **Origen**: `{PROJECT_SOURCE}.settings.companies`
- **PROJECT_SOURCE**: Se detecta automáticamente desde variable de entorno `GCP_PROJECT`
- **GCP_PROJECT**: Se establece en `build_deploy.sh` línea 164: `--set-env-vars GCP_PROJECT=${PROJECT_ID}`

#### Flujo:

1. **Deploy a PRO**:
   - `PROJECT_ID = "platform-partners-pro"`
   - `GCP_PROJECT = "platform-partners-pro"` (variable de entorno)
   - Lee de: `platform-partners-pro.settings.companies` ✅

2. **Deploy a DEV**:
   - `PROJECT_ID = "platform-partners-des"`
   - `GCP_PROJECT = "platform-partners-des"` (variable de entorno)
   - Lee de: `platform-partners-des.settings.companies` ✅

3. **Deploy a QUA**:
   - `PROJECT_ID = "platform-partners-qua"`
   - `GCP_PROJECT = "platform-partners-qua"` (variable de entorno)
   - Lee de: `platform-partners-qua.settings.companies` ✅

#### Tabla `metadata_consolidated_tables`:

- **Origen**: `pph-central.management.metadata_consolidated_tables`
- **Hardcoded**: Siempre centralizada (no cambia por ambiente)
- **Razón**: Metadata compartida entre todos los ambientes

**Conclusión**: ✅ Cada ambiente usa su propia tabla `companies`. Metadata es centralizada.

---

## 📋 Resumen Final

| Punto | Estado | Acción Requerida |
|-------|--------|------------------|
| **1. Parámetro PRO** | ⚠️ Opcional pero recomendado | Usar: `./build_deploy.sh pro` |
| **2. Scheduler** | ✅ Correcto | Solo se crea en PRO automáticamente |
| **3. Tablas por ambiente** | ✅ Correcto | Cada ambiente usa su propia tabla `companies` |

---

## 🚀 Comando Recomendado para Deploy a PRO

```bash
cd json2bq-job
./build_deploy.sh pro
```

Este comando:
- ✅ Usa PRO explícitamente (sin depender de proyecto activo)
- ✅ Crea/actualiza el scheduler en PRO
- ✅ Configura `GCP_PROJECT=platform-partners-pro` para que lea de la tabla correcta
- ✅ Aplica paralelismo (3 tareas) si está configurado

---

## ⚠️ Verificaciones Post-Deploy

Después del deploy, verificar:

1. **Job creado/actualizado**:
   ```bash
   gcloud run jobs describe etl-json2bq-job --region=us-east1 --project=platform-partners-pro
   ```

2. **Scheduler activo**:
   ```bash
   gcloud scheduler jobs describe etl-json2bq-schedule --location=us-east1 --project=platform-partners-pro
   ```

3. **Variable de entorno GCP_PROJECT**:
   - Verificar en la configuración del job que `GCP_PROJECT=platform-partners-pro`

4. **Tabla companies**:
   - Verificar que el job lee de `platform-partners-pro.settings.companies`

