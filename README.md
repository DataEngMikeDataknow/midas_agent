# midas_data_platform

Bundle de **ingeniería de datos** del sistema multiagente **MIDAS** (EPM,
Gerencia Experiencia Usuario Cliente — Facturación).

Su responsabilidad es **exclusivamente** la cadena de datos:

```
Oracle ──(JDBC ojdbc11)──▶ Parquet (Volume UC) ──▶ Bronze (Delta) ──▶ Silver (Delta)
```

El **agente** (serving del modelo, deploy MLOps e inferencia batch) vive en el
bundle separado **`midas_agent`**. Este repositorio ya no contiene nada del
agente.

---

## Flujo

```
                        job: midas_bronze_silver_<target>   (07:00 America/Bogota)
   ┌─────────────────────────────────────────────────────────────────────────────┐
   │                                                                               │
   │  1) extraer_datos_oracle                                                      │
   │     Oracle ──JDBC(ojdbc11/JayDeBeApi, driver-side)──▶ *.parquet en Volume UC  │
   │                                                                               │
   │  2) actualizar_ordenes_calidad                                                │
   │     Parquet ──insertInto(overwrite=True)──▶ 8 tablas *_bronze                 │
   │                                                                               │
   │  3) bronze_to_silver                                                          │
   │     *_bronze ──transformaciones──▶ tablas *_silver                            │
   │                                                                               │
   └─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
              Silver consumida por el bundle `midas_agent` (inferencia)
```

**Contrato entre bundles:** este bundle produce las Silver; la inferencia del
agente debe correr **después de ~09:00** o validar en `midas_log_cargas` que la
corrida del día quedó en estado `EXITOSO` antes de arrancar. No hay acoplamiento
directo de jobs entre bundles.

---

## Ambientes

| Target | Rama       | Workspace host                                          | Catálogo destino               | Service Principal                      | DSN Oracle                                |
|--------|------------|---------------------------------------------------------|--------------------------------|----------------------------------------|-------------------------------------------|
| `dllo` | desarrollo | `adb-1161217326944529.9.azuredatabricks.net`            | `epm_datalabs_catalog_dllo`    | `2080d313-3074-4adf-8265-916ef7645dc2` | `epm-to34.corp.epm.com.co:1521/SFUAT`     |
| `uat`  | pruebas    | `adb-6159907084216583.3.azuredatabricks.net`            | `epm_datalake_catalog_np`      | `c0b3fdca-baec-4bab-9f08-c6e222d921ae` | `epm-to34.corp.epm.com.co:1521/SFUAT`     |
| `pdn`  | produccion | `adb-5768199714870840.0.azuredatabricks.net`            | `epm_datalake_catalog_prod`    | `1db15634-31b7-4f92-a976-fc8920216152` | `epm-po34.corp.epm.com.co:1522/SFPDN`     |

- Schema destino: `facturacion` en los tres ambientes.
- `dllo` corre sobre un **cluster compartido existente** (`existing_cluster_id`)
  porque el SP no puede crear clusters. `uat`/`pdn` usan **job clusters
  efímeros** (`SINGLE_USER`).
- Schedule pausado en `dllo` (`PAUSED`), activo en `uat`/`pdn` (`UNPAUSED`).

---

## Conexión a Oracle

### Por qué JDBC (ojdbc11) y no `python-oracledb`
La cuenta Oracle de PRODUCCIÓN usa verificador de contraseña **10G**, no
soportado por el modo *thin* de `python-oracledb` → falla con **`DPY-3015`**.
Resetear el verifier fue descartado (cuenta transaccional compartida). El driver
JDBC de Oracle (`ojdbc11`) sí soporta el verifier 10G.

### Por qué NO `spark.read.format("jdbc")`
En clusters Unity Catalog, el datasource JDBC de Spark exige `SELECT ON ANY
FILE` → **`INSUFFICIENT_PERMISSIONS (42501)`**. En su lugar, la conexión es
**driver-side**: `JayDeBeApi` + `JPype1` arrancan una JVM en el driver y abren
el socket a Oracle directamente con `ojdbc11` (Java puro: sin Oracle Instant
Client, sin `libaio`). UC no intercepta ese tráfico.

### Requisitos de runtime
- **DBR `16.4.x-scala2.12` en los tres targets** (= JDK 17). El jar `ojdbc11`
  está compilado para JDK 11+/17. Un cluster en DBR 15.4 (JDK 8) produce
  **SIGSEGV (exit 139)** al arrancar la JVM de JPype (crash nativo, no error
  Python).
- Job clusters efímeros (`uat`/`pdn`) con `data_security_mode: SINGLE_USER` +
  `single_user_name = <service_principal>` (modo correcto para JVM-en-proceso;
  no rompe UC).
- Librerías `JayDeBeApi` y `JPype1`: declaradas como `libraries` de la task
  `extraer_datos_oracle` en `uat`/`pdn`; **preinstaladas** por el admin en el
  cluster compartido de `dllo`.

### Ubicación del jar y GRANTs
- El jar vive en un Volume UC; la ruta se pasa como variable
  `oracle_jdbc_jar_path` y debe coincidir **carácter por carácter** con la
  ubicación real:
  - np:   `/Volumes/epm_datalake_vol_np/facturacion_vol/facturacion_bronze_vol/_oracle_client/ojdbc11-23.26.2.0.0.jar`
  - prod: `/Volumes/epm_datalake_vol_prod/facturacion_vol/facturacion_bronze_vol/_oracle_client/ojdbc11-23.26.2.0.0.jar`
- El SP necesita `READ VOLUME` (jar) y `WRITE VOLUME` (Parquet), además de
  `USE CATALOG`/`USE SCHEMA`/`CREATE TABLE`/`SELECT`/`MODIFY` en las tablas
  destino/control/log y `READ` del secret scope.
- El usuario Oracle (`SQL_EPMBOTPD05`) va **explícito** como parámetro; solo la
  contraseña vive en el secret scope.

### Diagnóstico de errores de conexión

| Síntoma                                      | Causa probable                                            |
|----------------------------------------------|-----------------------------------------------------------|
| `DPY-6005` / timeout TCP                      | Red/firewall/ruta de subnet (NO credenciales)             |
| `ORA-01017`                                   | Credenciales inválidas (usuario/contraseña)               |
| `ORA-12514`                                   | Service name / listener incorrecto                        |
| `SIGSEGV` (exit 139)                          | Mismatch runtime/JDK — usar DBR 16.4 (JDK 17)             |
| `EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE`   | Objetos Java sin convertir a tipos Python (ver más abajo) |

`src/midas/db/database.py` hace un **diagnóstico de red** (DNS + TCP) antes de
conectar, para distinguir un problema de firewall de uno de credenciales.

---

## Notas operativas

- **Bronze siempre con `insertInto(full_table, overwrite=True)`.** Nunca
  `saveAsTable` con overwrite/overwriteSchema ni `TRUNCATE`+append sobre tablas
  existentes: eso exige `MANAGE` y destruye PKs/NOT NULL/comentarios.
- **Binds nombrados de Oracle (`:param`)**: `queries.py` los conserva. JDBC solo
  entiende `?` posicionales; la conversión se hace en runtime en `database.py`,
  reemplazando **solo** los nombres presentes en el dict de parámetros y
  repitiendo el valor por cada ocurrencia. Los literales tipo `'HH24:MI:SS'` no
  se tocan.
- **Paridad de tipos Parquet**: `JayDeBeApi` devuelve `NUMBER`/`DATE`/`CLOB`
  como objetos Java (JPype). `database.py` los convierte a tipos Python nativos
  usando la escala del metadata (`cursor.description[i][5]`) para que el schema
  Parquet sea idéntico al que producía `python-oracledb` (`insertInto` es
  estricto contra las Bronze existentes).
- **Plano de control con `job_name`**: `midas_control_cargas` y
  `midas_log_cargas` son COMPARTIDAS con otros jobs (p. ej. `vera_framework`).
  Todos los runners de este bundle usan `job_name = "midas_bronze"` para
  discriminar sus filas. Ver `sql/seed_control_cargas.sql`.

---

## Prerrequisitos de plataforma (gestionar con EPM)

1. Subir `ojdbc11-23.26.2.0.0.jar` a `_oracle_client/` del Volume de cada
   ambiente (np y prod), con el nombre exacto de la variable.
2. GRANTs del SP por ambiente: `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`,
   `SELECT`/`MODIFY` en tablas destino/control/log, `READ VOLUME` (jar),
   `WRITE VOLUME` (Parquet) y `READ` del secret scope.
3. Habilitación de red por workspace/subnet hacia Oracle:
   `dllo`/`uat` → `epm-to34:1521`; **`pdn` → `epm-po34:1522`** (cada subnet
   requiere su propia apertura; que `uat` funcione no implica `pdn`).
4. Instalar `JayDeBeApi` y `JPype1` en el cluster compartido de `dllo`
   (`0722-211855-e1a090ph`).
5. Sembrar/migrar `job_name` en `midas_control_cargas` de cada ambiente con
   `sql/seed_control_cargas.sql`.

---

## Estructura

```
databricks.yml                 Definición del bundle (targets dllo/uat/pdn, job por target)
pipeline/deploy-bundle.yml     CI/CD Azure DevOps (desarrollo→dllo, pruebas→uat, produccion→pdn)
sql/seed_control_cargas.sql    Seed/migración de midas_control_cargas (con job_name)
src/midas/
  db/database.py               Conector Oracle JDBC (ojdbc11 + JayDeBeApi + JPype1)
  db/queries.py                Consultas SQL (binds nombrados :param)
  db/processing.py             Extracción Oracle → Parquet
  framework/control_cargas.py  Cliente del plano de control (job_name)
  framework/chain_runner.py    Orquestación de la cadena de extracción
  ingestion.py                 Parquet → Bronze (insertInto)
  transformations.py           Bronze → Silver
  main_data_fetcher.py         Runner tarea 1 (extracción)
  main_ingestion.py            Runner tarea 2 (bronze)
  main_transform.py            Runner tarea 3 (silver)
tests/                         Suite de pruebas (mockea Spark/JVM)
```

---

## Cómo correr los tests

```bash
pip install -r requirements-test.txt
PYTHONPATH=.:src pytest tests/ -v
```

Los tests **mockean** Spark y la JVM (no requieren cluster ni Oracle ni
`JayDeBeApi`/`JPype1` instalados). `tests/test_jdbc_database.py` cubre la
conversión de binds y de tipos Java→Python; `tests/test_job_name.py` cubre el
filtrado por `job_name` en el plano de control.

> **Nota:** No hacer `databricks bundle deploy` desde local salvo con
> credenciales del SP. El despliegue lo gestiona `pipeline/deploy-bundle.yml`.
