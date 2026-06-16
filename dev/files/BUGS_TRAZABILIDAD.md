# MIDAS — Trazabilidad de Bugs y Fixes
> Documento de registro técnico. Cada entrada incluye: síntoma, causa raíz, fix aplicado, archivos afectados y verificación.

---

## BUG-001 — `NameError: name '__file__' is not defined` en Databricks spark_python_task

| Campo | Detalle |
|---|---|
| **Fecha** | 2026-02-27 |
| **Severidad** | Crítica (bloqueante) |
| **Ambiente** | Databricks Dev (`adb-1161217326944529.9.azuredatabricks.net`) |
| **Job afectado** | `midas_load_transform_data` — tarea `actualizar_ordenes_calidad` |
| **Branch** | `refactor-midas` |
| **Reportado por** | Sergio (iData) — ejecución real en Dev |

### Síntoma

El Job 1 falló en la primera tarea con el siguiente traceback:

```
NameError: name '__file__' is not defined
File /Workspace/Users/ssuarecr@contratista.epm.co/.bundle/midas_agent/dev/files/src/midas/main_ingestion.py, line 6
    _src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
```

### Causa Raíz

**`__file__` no es una variable global en el contexto de ejecución de Databricks.**

Cuando Databricks ejecuta un `spark_python_task`, el script no se lanza como un proceso Python nativo (`python script.py`). Internamente usa un runner basado en **`ipykernel`** (el mismo motor que los notebooks), donde el código se interpreta pero `__file__` nunca se asigna como variable global del módulo.

Esto contrasta con el comportamiento estándar de Python:
- `python main_ingestion.py` → `__file__` = ruta absoluta del script ✅
- Databricks `spark_python_task` (ipykernel) → `__file__` no definido ❌

El patrón afectado era el bloque de `sys.path` que se agregó durante el refactoring para que Databricks pudiera encontrar el paquete `midas` bajo `src/`:

```python
# CÓDIGO ORIGINAL (fallaba en Databricks):
_src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)
```

### Archivos Afectados

El mismo patrón existía en los 4 runners del proyecto:

| Archivo | Línea fallida |
|---|---|
| `src/midas/main_ingestion.py` | 6 |
| `src/midas/main_transform.py` | 6 |
| `src/midas/main_tools.py` | 6 |
| `src/midas/main_inference.py` | 6 |

### Fix Aplicado

Reemplazo del acceso directo a `__file__` por un bloque `try/except` que usa `sys.argv[0]` como fallback:

```python
# FIX (funciona en Databricks ipykernel Y en ejecución local):
try:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
_src_path = os.path.join(_script_dir, "..")
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)
```

**¿Por qué `sys.argv[0]` funciona?**
En un `spark_python_task`, Databricks sí pasa la ruta completa del script como primer argumento (`sys.argv[0]`). Ej:
```
/Workspace/.../files/src/midas/main_ingestion.py
```
Esto permite reconstruir la ruta a `src/` de forma idéntica a como lo haría `__file__`.

**Contextos cubiertos por el fix:**

| Contexto | Rama ejecutada | Resultado |
|---|---|---|
| `python main_ingestion.py` (local) | `__file__` definido → rama `try` | ✅ |
| `pytest` (importación normal) | `__file__` definido → rama `try` | ✅ |
| Databricks `spark_python_task` (ipykernel) | `__file__` indefinido → rama `except` con `sys.argv[0]` | ✅ |

### Verificación

**Tests locales post-fix:**
```
11 passed in 0.22s
```
Todos los tests siguen pasando. El bloque `try` se ejecuta (porque `__file__` sí existe en importación normal), el comportamiento no cambia.

**Verificación en Databricks:** Re-deploy + re-run del Job 1 → BUG-001 resuelto (el Job avanzó más allá de la línea 6). Nuevo error encontrado: ver BUG-002.

---

## BUG-002 — `PERMISSION_DENIED: MANAGE` en Unity Catalog al crear tablas Bronze

| Campo | Detalle |
|---|---|
| **Fecha** | 2026-02-27 |
| **Severidad** | Crítica (bloqueante) |
| **Tipo** | Permisos en Unity Catalog — **NO es un bug de código** |
| **Ambiente** | Databricks Dev (`adb-1161217326944529.9.azuredatabricks.net`) |
| **Job afectado** | `midas_load_transform_data` — tarea `actualizar_ordenes_calidad` |
| **Branch** | `refactor-midas` |
| **Reportado por** | Sergio (iData) — ejecución real en Dev |

### Síntoma

```
[UNAUTHORIZED_ACCESS] Unauthorized access:
PERMISSION_DENIED: User does not have MANAGE on Table
'epm_datalabs_catalog_dllo.facturacion.midas_ordenes_calidad_pendientes_bronze'. SQLSTATE: 42501
```

El job falla en `ingestion.py:39` al ejecutar `.saveAsTable("midas_ordenes_calidad_pendientes_bronze")` con `mode("overwrite")` y `overwriteSchema("true")`.

### Causa Raíz

**Las tablas `midas_*_bronze` ya existen en el esquema `facturacion`** (creadas previamente por otro usuario, probablemente Jonatan Londoño durante el desarrollo original). El proceso Delta de creación/reemplazo de tablas ocurre en dos fases:

1. **Fase Delta** — `CreateDeltaTableCommand.handleCommit()`: escribe los datos en el storage de Unity Catalog. Esta fase **sí tiene permisos** (el usuario puede leer y escribir en el volumen).
2. **Fase UC Metadata** — `CreateDeltaTableCommand.runPostCommitUpdates() → updateCatalog() → alterTable()`: actualiza el Unity Catalog con el nuevo esquema de la tabla. Esta fase **requiere ser el owner** de la tabla existente o tener el privilegio `MANAGE`.

El usuario `ssuarecr@contratista.epm.co` **no es el owner** de las tablas que ya existen, por lo que Unity Catalog rechaza la operación de metadata.

### Análisis adicional: isolación Dev

El `databricks.yml` en el target `dev` no sobreescribe `schema_name`, por lo que el job escribe en el mismo esquema `facturacion` que se usa en producción. Esto es un **riesgo de arquitectura**: en Dev se debería usar un esquema separado (ej: `facturacion_dev`) para evitar conflictos con datos de otros ambientes.

### Fix Requerido — Acción de Franz/EPM

Este bug **requiere intervención del administrador de Unity Catalog (Franz)**, no es un cambio de código.

**Opción A — Recomendada: Otorgar permisos al usuario sobre el esquema**
```sql
-- Ejecutar con usuario admin en el catálogo epm_datalabs_catalog_dllo
GRANT ALL PRIVILEGES ON SCHEMA epm_datalabs_catalog_dllo.facturacion
  TO `ssuarecr@contratista.epm.co`;
```

**Opción B — Alternativa: Eliminar las tablas existentes para que Sergio las cree desde cero**
```sql
-- El owner actual (Jonatan u otro) debe ejecutar esto:
DROP TABLE IF EXISTS epm_datalabs_catalog_dllo.facturacion.midas_ordenes_calidad_pendientes_bronze;
DROP TABLE IF EXISTS epm_datalabs_catalog_dllo.facturacion.midas_datos_basicos_producto_bronze;
DROP TABLE IF EXISTS epm_datalabs_catalog_dllo.facturacion.midas_datos_lecturas_producto_bronze;
DROP TABLE IF EXISTS epm_datalabs_catalog_dllo.facturacion.midas_datos_consumos_producto_bronze;
DROP TABLE IF EXISTS epm_datalabs_catalog_dllo.facturacion.midas_datos_ordenes_previa_critica_bronze;
DROP TABLE IF EXISTS epm_datalabs_catalog_dllo.facturacion.midas_datos_cometarios_ordenes_bronze;
DROP TABLE IF EXISTS epm_datalabs_catalog_dllo.facturacion.midas_datos_cuentas_cobro_bronze;
DROP TABLE IF EXISTS epm_datalabs_catalog_dllo.facturacion.midas_datos_detalle_cargos_bronze;
```

**Opción C — Recomendada para correcta separación Dev/Prod: Esquema separado para Dev**

En el `databricks.yml`, en el target `dev`, agregar:
```yaml
dev:
  variables:
    schema_name: facturacion_dev   # <- Esquema aislado, propiedad de Sergio
```
Y solicitar a Franz la creación de `epm_datalabs_catalog_dllo.facturacion_dev` con Sergio como owner.

### Iteración 2 del fix — Nuevo error tras aplicar TRUNCATE + append

Al re-correr el job con el fix de TRUNCATE + append se encontró un nuevo error:

```
AnalysisException: Cannot create or update table because the child column(s) `id_orden`
of primary key `pk_midas_ordenes_calidad_pendientes_bronze` cannot be set to nullable.
File ingestion.py:60 → COMMENT ON TABLE ...
```

**Causa**: `mode("append").saveAsTable()` en Spark Connect reescribe el schema Delta del DataFrame entrante (donde `id_orden` es nullable en Parquet) sobre el schema de la tabla existente, removiendo el atributo NOT NULL. Cuando se intenta ejecutar cualquier DDL posterior (`COMMENT ON TABLE`), Unity Catalog detecta la inconsistencia (columna nullable pero con PK constraint) y rechaza la operación.

### Fix Final — `insertInto(overwrite=True)` + metadatos solo en creación

**Cambio en `src/midas/ingestion.py`:**

```python
# Para tabla EXISTENTE (carga diaria):
df_spark.write.insertInto(full_table_name, overwrite=True)
# No ejecutar ningún DDL de metadatos

# Para tabla NUEVA (primera ejecución):
df_spark.write.format("delta").saveAsTable(full_table_name)
# Luego aplicar COMMENT, SET NOT NULL, ADD CONSTRAINT
```

**¿Por qué `insertInto(overwrite=True)` resuelve ambos problemas?**

| Operación | MANAGE requerido | Modifica schema/NOT NULL | Preserva PK constraint |
|---|---|---|---|
| `mode("overwrite").saveAsTable()` | ✅ Sí (BUG-002) | Sí | No |
| `TRUNCATE` + `mode("append").saveAsTable()` | No | Sí (BUG-003) | Causa inconsistencia |
| `insertInto(overwrite=True)` | No (solo MODIFY) | **No** | **Sí** |

**Principio de diseño correcto**: Los metadatos (descripciones, comentarios, PK constraints) son parte del diseño de la tabla, no de la carga diaria. Se aplican una sola vez en la creación y no deben re-ejecutarse en cada refresh.

### Opción C + Service Principal — Solución final adoptada (2026-03-01)

El approach de creación manual del schema fue descartado porque Sergio no tiene `CREATE SCHEMA` en el catálogo (BUG-004). La solución definitiva combina:

1. **`run_as: service_principal_name`** en el target `dev` del `databricks.yml` — los Jobs corren como SP, que sí tiene `CREATE SCHEMA`.
2. **`ensure_schema_exists(catalog, schema)`** añadido en `DataIngestor` — ejecuta `CREATE SCHEMA IF NOT EXISTS` una sola vez antes del loop de tablas. En cargas diarias es un no-op.

```python
# ingestion.py — nuevo método
def ensure_schema_exists(self, catalog: str, schema: str):
    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# main_ingestion.py — llamada antes del loop
ingestor.ensure_schema_exists(args.destination_catalog, args.destination_schema)
for config in tables_config:
    ingestor.load_parquet_to_delta(...)
```

```yaml
# databricks.yml — dev target
run_as:
  service_principal_name: <APP-ID-DEL-SP-DEV>  # Franz proporciona este valor
```

### Estado

- [x] Fix de código aplicado (`insertInto` + metadata solo en creación) — 2026-02-27
- [x] `databricks.yml` actualizado con `schema_name: facturacion_dev` para Dev — 2026-02-27
- [x] `ingestion.py` — método `ensure_schema_exists` añadido — 2026-03-01
- [x] `databricks.yml` — `run_as` placeholder añadido al target `dev` — 2026-03-01
- [x] Tests locales: 12/12 pasando
- [ ] **Pendiente Franz**: Proporcionar APP-ID del SP con permisos en `epm_datalabs_catalog_dllo` Dev
- [ ] Reemplazar `<APP-ID-DEL-SP-DEV>` en `databricks.yml` con el valor real
- [ ] Re-deploy + re-run Job 1

---

## BUG-003 — `AnalysisException: PrimaryKeyColumnsNullableException` tras TRUNCATE + append

| Campo | Detalle |
|---|---|
| **Fecha** | 2026-02-27 |
| **Severidad** | Crítica (bloqueante) |
| **Ambiente** | Databricks Dev (`adb-1161217326944529.9.azuredatabricks.net`) |
| **Job afectado** | `midas_load_transform_data` — tarea `actualizar_ordenes_calidad` |
| **Branch** | `refactor-midas` |
| **Reportado por** | Sergio (iData) — ejecución real en Dev |
| **Descubierto en** | Iteración 2 del fix de BUG-002 |

### Síntoma

Al re-correr el job con la estrategia TRUNCATE + `mode("append").saveAsTable()` (intento de fix del BUG-002), se obtuvo el siguiente error:

```
AnalysisException: Cannot create or update table because the child column(s) `id_orden`
of primary key `pk_midas_ordenes_calidad_pendientes_bronze` cannot be set to nullable.
File ingestion.py:60 → COMMENT ON TABLE ...
```

El error no ocurrió en el `saveAsTable` en sí, sino al ejecutar la siguiente sentencia DDL (`COMMENT ON TABLE`).

### Causa Raíz

**`mode("append").saveAsTable()` en Spark Connect reescribe el schema Delta** del DataFrame entrante sobre el schema de la tabla existente. Dado que el DataFrame proviene de un archivo Parquet donde las columnas son **nullable por defecto**, Spark elimina el atributo `NOT NULL` de la columna `id_orden` en el metadata Delta de la tabla.

Resultado: La tabla queda en un **estado inconsistente** — tiene un `PRIMARY KEY constraint` que referencia a `id_orden`, pero la columna ahora es nullable. Unity Catalog detecta esta inconsistencia al intentar ejecutar cualquier DDL posterior (`COMMENT ON TABLE`, `ALTER TABLE`, etc.) y rechaza la operación.

**Cadena causal completa:**

```
TRUNCATE TABLE (borra filas)
  → mode("append").saveAsTable()
    → Spark Connect reescribe schema Delta con Parquet schema (nullable)
      → id_orden pierde NOT NULL en Delta metadata
        → Estado inconsistente: PK existe pero columna es nullable
          → COMMENT ON TABLE falla con PrimaryKeyColumnsNullableException
```

### Por qué el fix de BUG-002 (TRUNCATE + append) no era correcto

La idea era evitar el `MANAGE` requerido por `overwrite`, pero `mode("append").saveAsTable()` hace algo más que insertar filas: **actualiza el schema de la tabla** en el catálogo con el schema del DataFrame. Esto no sólo no requería MANAGE, sino que introducía corrupción silenciosa del schema.

### Fix Aplicado

Mismo fix que resolvió BUG-002 definitivamente: **`insertInto(overwrite=True)`**.

Esta operación es una **escritura de datos pura**:
- NO modifica el schema Delta de la tabla
- NO toca columnas nullability
- NO requiere MANAGE (solo MODIFY)
- Preserva PK constraints, comentarios y el schema completo

```python
# Para tabla EXISTENTE — no toca metadatos, preserva schema y constraints:
df_spark.write.insertInto(full_table_name, overwrite=True)
```

Los DDL de metadatos (COMMENT, SET NOT NULL, ADD CONSTRAINT) se mueven completamente a la rama de **primera creación** de la tabla, donde se ejecutan una sola vez.

### Archivos Modificados

| Archivo | Cambio |
|---|---|
| `src/midas/ingestion.py` | `insertInto(overwrite=True)` para tablas existentes; metadatos solo en `else` (creación) |
| `tests/test_ingestion.py` | Test `test_load_parquet_existing_table` verifica que NO se llamen DDL en carga diaria |

### Verificación

**Tests locales post-fix:**
```
12 passed in 0.22s
```

**Verificación en Databricks:** Pendiente re-run tras resolución de BUG-004 (CREATE SCHEMA).

### Estado

- [x] Fix de código aplicado (`insertInto` + metadata solo en creación) — 2026-02-27
- [x] Test unitario que verifica el comportamiento correcto — 2026-02-27
- [ ] Verificación en Databricks Dev (bloqueado por BUG-004)

---

## BUG-004 — `PERMISSION_DENIED: User does not have CREATE SCHEMA` en Unity Catalog

| Campo | Detalle |
|---|---|
| **Fecha** | 2026-02-27 |
| **Severidad** | Bloqueante (impide ejecución en Dev) |
| **Tipo** | Permisos en Unity Catalog — **NO es un bug de código** |
| **Ambiente** | Databricks Dev (`adb-1161217326944529.9.azuredatabricks.net`) |
| **Catálogo afectado** | `epm_datalabs_catalog_dllo` |
| **Branch** | `refactor-midas` |
| **Reportado por** | Sergio (iData) — notebook en Databricks Dev |

### Síntoma

Al intentar crear el schema aislado para Dev (Opción C de BUG-002) ejecutando en un notebook:

```sql
CREATE SCHEMA IF NOT EXISTS epm_datalabs_catalog_dllo.facturacion_dev;
```

Se obtuvo:

```
[UNAUTHORIZED_ACCESS] Unauthorized access:
PERMISSION_DENIED: User does not have CREATE SCHEMA on Catalog 'epm_datalabs_catalog_dllo'. SQLSTATE: 42501
```

### Causa Raíz

El usuario `ssuarecr@contratista.epm.co` es un **usuario contratista** y no tiene el privilegio `CREATE SCHEMA` en el catálogo `epm_datalabs_catalog_dllo`. Este privilegio requiere ser `CATALOG ADMIN` o tener el grant explícito `CREATE SCHEMA ON CATALOG`.

En Unity Catalog, la jerarquía de privilegios es:
```
Catálogo (CREATE SCHEMA)  ← Sergio no tiene este nivel
  └─ Schema (CREATE TABLE, MODIFY, SELECT)  ← Sergio podría tener si el schema existiera
       └─ Tabla (MANAGE, MODIFY, SELECT, etc.)
```

### Fix Requerido — Acción de Franz/EPM

Este bug **requiere intervención de Franz** (administrador del catálogo). Hay dos opciones:

**Opción A — Franz crea el schema y asigna owner a Sergio (Recomendada)**
```sql
-- Franz ejecuta en el catálogo de Dev:
CREATE SCHEMA IF NOT EXISTS epm_datalabs_catalog_dllo.facturacion_dev;
ALTER SCHEMA epm_datalabs_catalog_dllo.facturacion_dev OWNER TO `ssuarecr@contratista.epm.co`;
```
Con esto, Sergio será owner del schema y podrá crear tablas, modificarlas y gestionarlas sin intervención adicional.

**Opción B — Franz otorga CREATE SCHEMA al usuario**
```sql
-- Franz ejecuta:
GRANT CREATE SCHEMA ON CATALOG epm_datalabs_catalog_dllo TO `ssuarecr@contratista.epm.co`;
```
Esto es más amplio y permite a Sergio crear cualquier schema en el catálogo, lo que puede ser excesivo para un contratista.

**Recomendación:** Opción A — es más restringida (Sergio solo tiene control sobre `facturacion_dev`) y sigue el principio de mínimo privilegio.

### Contexto — ¿Por qué se necesita `facturacion_dev`?

El `databricks.yml` fue actualizado (fix BUG-002, Opción C) para que el ambiente Dev use `schema_name: facturacion_dev` en lugar de `facturacion`. Esto:
1. Evita conflictos de permisos con tablas de producción/Jonatan en el schema `facturacion`
2. Da a Sergio control total sobre su propio schema de desarrollo
3. Aísla correctamente los ambientes Dev y Prod

Sin `facturacion_dev`, el Job 1 fallará al intentar crear o acceder a tablas en ese schema.

### Pasos para Desbloquear

1. **Sergio contacta a Franz** y le comparte este documento (o los SQLs de la Opción A)
2. **Franz ejecuta** el SQL de creación + asignación de owner
3. **Sergio re-deploya** el bundle: `databricks bundle deploy --target dev`
4. **Sergio re-ejecuta** el Job 1: `databricks bundle run midas_load_transform_data --target dev`

### Solución adoptada — Service Principal como `run_as` (2026-03-01)

La creación manual del schema fue descartada. En su lugar, se adoptó el patrón de Service Principal:
- El SP tiene `CREATE SCHEMA` → crea `facturacion_dev` automáticamente en primer run via `ensure_schema_exists()`
- Elimina la dependencia de privilegios sobre el usuario contratista
- Alineado con el diseño de producción (prod ya usa `run_as: service_principal_name`)

### Estado

- [x] `databricks.yml` actualizado con `schema_name: facturacion_dev` — 2026-02-27
- [x] `databricks.yml` — `run_as` placeholder añadido al target `dev` — 2026-03-01
- [x] `ingestion.py` — `ensure_schema_exists` crea el schema automáticamente — 2026-03-01
- [ ] **Pendiente Franz**: Proporcionar APP-ID del Service Principal con permisos en Dev UC
- [ ] Reemplazar `<APP-ID-DEL-SP-DEV>` en `databricks.yml` con el valor real
- [ ] Re-deploy + re-run Job 1

---

*Documento mantenido por: Sergio (iData) — Proyecto MIDAS para EPM*
