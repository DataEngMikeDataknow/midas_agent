# Componentes y repositorio

## Baseline documentado
- Rama base: `feature/midas_data_platform`
- Referencia operativa: estado al 2026-07-09

## Estructura del repositorio

```text
midas_data_platform/
|-- databricks.yml                      # definicion del bundle (dllo/uat/pdn)
|-- pyproject.toml                       # empaquetado (no genera wheel para el runtime)
|-- requirements-test.txt                # dependencias de test
|-- README.md
|-- BUGS_TRAZABILIDAD.md                 # historial de bugs de ingenieria de datos
|-- pipeline/
|   `-- deploy-bundle.yml                # CI/CD Azure DevOps
|-- docs/
|   |-- 00_INDICE.md ... 09_ANEXOS.md
|   `-- adr/0001-bundles-separados-vera-midas.md
|-- notebooks/
|   |-- 00_creacion_objetos_midas.py     # task crear_objetos
|   |-- 10_extraer_datos_oracle.py       # task extraer_datos_oracle (%pip + main_data_fetcher)
|   |-- 90_exploracion_campos_caso2.py   # exploracion Caso 2 (read-only, no es del job)
|   |-- check_conectividad_oracle.py     # job de diagnostico
|   `-- validacion_control_cargas.py     # validacion del plano de control
|-- sql/                                 # (eliminado: seed reemplazado por crear_objetos)
|-- src/
|   |-- __init__.py
|   `-- midas/
|       |-- db/
|       |   |-- database.py              # conector JDBC ojdbc11 (JayDeBeApi/JPype)
|       |   |-- queries.py               # queries Oracle (binds :param)
|       |   `-- processing.py            # extraccion -> Parquet
|       |-- framework/
|       |   |-- control_cargas.py        # cliente del plano de control (job_name)
|       |   `-- chain_runner.py          # orquestador de la cadena
|       |-- ingestion.py                 # Parquet -> Bronze (insertInto)
|       |-- transformations.py           # Bronze -> Silver
|       |-- main_data_fetcher.py         # runner: extraccion
|       |-- main_ingestion.py            # runner: bronze
|       `-- main_transform.py            # runner: silver
`-- tests/
    |-- test_jdbc_database.py            # conversion binds y tipos Java->Python
    |-- test_job_name.py                 # filtrado por job_name
    |-- test_runners.py                  # runners ingestion/transform
    |-- test_ingestion.py
    |-- test_transformations.py
    `-- test_framework_control.py
```

## Inventario por componente

### `databricks.yml`
Fuente de verdad del bundle:
- targets dllo/uat/pdn (job definido POR target)
- variables Oracle/JDBC, catalogo, Volume, scopes, cluster
- job `midas_bronze_silver` (4 tasks) y `midas_check_conectividad` (uat/pdn)
- permisos, notificaciones y schedule

### `pipeline/deploy-bundle.yml`
Pipeline CD (Azure DevOps):
- mapeo rama -> target: `desarrollo->dllo`, `pruebas->uat`, `produccion->pdn`
- 3 stages independientes; cada uno instala CLI + Terraform 1.9.8 y hace validate + deploy
- credenciales por variable group `databricks-MIDAS`

### `notebooks/00_creacion_objetos_midas.py`
Task `crear_objetos`. DDL idempotente de `midas_control_cargas` / `midas_log_cargas`
(con `job_name` en el CREATE), migracion guardada, bootstrap MERGE de 8 filas
`FULL_CHAINED`, `query_padre_id` y verificacion que hace `raise` si no hay 8 activas.

### `notebooks/10_extraer_datos_oracle.py`
Task `extraer_datos_oracle`. Wrapper delgado: `%pip install JayDeBeApi JPype1` +
`restartPython()`, resuelve `src/` en `sys.path`, arma `sys.argv` desde los
`base_parameters` y llama a `main_data_fetcher.main()`. Es `notebook_task` porque un
`spark_python_task` no puede ejecutar `%pip` y la plataforma no permite instalar libs
a nivel de cluster (mismo patron que `vera_framework`).

### `notebooks/check_conectividad_oracle.py`
Preflight de conectividad (job `midas_check_conectividad`). Copia del de `vera_framework`.

### `src/midas/db/database.py`
Conector Oracle JDBC ojdbc11 via JayDeBeApi + JPype1. Contrato de interfaz:
`init_database()`, `execute_query(query, params)`, `close_pool()`. Convierte binds
`:param` a `?` y objetos Java a tipos Python.

### `src/midas/db/queries.py`
Las queries Oracle (binds nombrados). No se modifican; la conversion se hace en runtime.

### `src/midas/db/processing.py`
Ejecuta cada query y escribe el Parquet correspondiente en el Volume.

### `src/midas/framework/control_cargas.py`
Cliente del plano de control. Filtra por `job_name`; escribe log por columnas explicitas
(id_log es IDENTITY).

### `src/midas/framework/chain_runner.py`
Orquestador delgado: envuelve `processing.py` con el cliente de control y aborta al primer fallo.

### `src/midas/ingestion.py` / `transformations.py`
Carga Bronze (`insertInto`) y construccion de Silver (Spark SQL).

### `src/midas/main_*.py`
Runners `spark_python_task` con el patron BUG-001 (`__file__` -> `sys.argv[0]`) y armado de `sys.path`.

## Componentes activos vs legacy

### Activos
- Runners y librerias de `src/midas/`
- Notebooks `00_creacion_objetos_midas.py` y `check_conectividad_oracle.py`
- Pipeline `pipeline/deploy-bundle.yml`

### Eliminado respecto a la version previa
- Todo lo del agente: `agent/`, `deploy/`, `batch_inference/`, `training/`,
  `src/midas/agent/`, `main_tools/deploy/inference/endpoint_permissions.py`
- Extractor legacy `data_fetcher/`
- `sql/seed_control_cargas.sql` (reemplazado por `crear_objetos`)
- `azure-pipelines.yml` y `pipelines/deploy-cicd.yml` (del agente)

## Dependencias operativas

### Databricks
- Databricks CLI
- Terraform 1.9.8 (instalado explicito en el pipeline)
- workspace host y Service Principal por ambiente
- Runtime `16.4.x-scala2.12` (JDK 17)

### Python (runtime del job)
- `pandas`, `pyarrow`
- `JayDeBeApi`, `JPype1` (driver JDBC; se instalan por `%pip` en la primera celda de
  `notebooks/10_extraer_datos_oracle.py`, igual en dllo/uat/pdn. NINGUN target declara
  `libraries`: la plataforma EPM no permite instalar libs a nivel de cluster)

### Python (tests / CI)
- `pytest`, `pytest-cov`, `mock`, `pandas`, `numpy`
- NO se instalan JayDeBeApi/JPype1 en CI: los tests los mockean

### Oracle
- jar `ojdbc11-23.26.2.0.0.jar` en el Volume de cada ambiente
- usuario `SQL_EPMBOTPD05` (explicito) + contraseña en secret scope

## Desalineaciones detectadas
- `tests/test_ingestion.py::test_ensure_schema_exists` esta desfasado respecto al
  `ingestion.py` refactorizado (espera `CREATE SCHEMA IF NOT EXISTS`, el codigo hace
  `SHOW SCHEMAS ... LIKE` + create condicional). Falla preexistente, ajena a la migracion;
  ambos archivos se mantienen intactos por acuerdo. Ver 05 (deuda tecnica).
- Coexisten nombres historicos (`source_base_path: midas_agent`) por continuidad de las Bronze.

## Autocritica del repositorio
- El repo quedo enfocado y sin huellas del agente, pero conserva un test stale heredado.
- La estructura es entendible para quien conoce Databricks; para terceros requiere transferencia previa.
