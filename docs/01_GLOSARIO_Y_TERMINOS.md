# Glosario y terminos

## Proposito
Reduce la dependencia de contexto informal. Permite que una persona nueva entienda
el lenguaje tecnico y funcional usado en `midas_data_platform`.

## Terminos del dominio

### MIDAS
Sistema que analiza ordenes de calidad de facturacion. Se compone de dos bundles:
`midas_data_platform` (ingenieria de datos, este repo) y `midas_agent` (agente LLM).

### Orden de calidad
Registro de negocio que representa un caso a revisar por diferencias o anomalias de
facturacion. Es el eje de la tabla `midas_ordenes_calidad_pendientes_*`.

### Orden de critica / previa
Orden operativa asociada a validaciones, correcciones o revisiones previas sobre
consumos, lecturas o causales.

### Cadena (FULL_CHAINED)
Modelo de carga de este bundle: las 12 extracciones se ejecutan en orden porque cada
paso puede alimentar al siguiente (p. ej. de ordenes pendientes se derivan los
servicios suscritos que parametrizan lecturas/consumos).

### Capas Bronze / Silver
- **Bronze**: copia tipada y gobernada de los Parquet extraidos (una tabla por query).
- **Silver**: tablas derivadas/enriquecidas que consume el agente.
- Este bundle llega **hasta Silver**. No produce Gold ni CSV.

## Terminos de plataforma

### Databricks Asset Bundles (DAB)
Empaquetado declarativo de jobs, variables y configuracion de despliegue en `databricks.yml`.

### Unity Catalog (UC)
Capa de gobierno de datos donde viven catalogos, schemas, tablas Delta y Volumes.

### Volume (UC)
Almacenamiento gobernado dentro de UC. Aqui se guardan los Parquet intermedios y el
jar `ojdbc11`. Las rutas usan prefijo `/Volumes/...`.

### Service Principal (SP)
Identidad no humana que ejecuta el job (`run_as`) y el deploy. Uno por ambiente.

### JDBC / ojdbc11
Driver Java oficial de Oracle. Se usa en modo *thin* desde el driver de Spark.

### JayDeBeApi / JPype1
Librerias Python que levantan una JVM en el driver y permiten hablar JDBC como codigo
plano (sin `spark.read.format("jdbc")`, sin Oracle Instant Client).

### Plano de control
Las tablas `midas_control_cargas` (configuracion de que cargar) y `midas_log_cargas`
(bitacora de ejecuciones). Son **compartidas** con otros jobs.

### job_name
Discriminador de filas en el plano de control. Este bundle usa `job_name = 'midas_bronze'`;
`vera_framework` usa `job_name = 'vera_framework'`.

### Secret scope
Almacen de secretos de Databricks. Aqui vive la **contraseña** Oracle (el usuario va explicito).

## Terminos de pipeline

### CI/CD
Pipeline de despliegue definido en `pipeline/deploy-bundle.yml` (Azure DevOps).

### dllo / uat / pdn
Los tres targets del bundle:
- dllo: desarrollo (rama `desarrollo`)
- uat: pruebas / calidad (rama `pruebas`)
- pdn: produccion (rama `produccion`)

> Nota de nomenclatura: la version previa usaba `dev/uat/prod`. Este bundle usa
> `dllo/uat/pdn` para alinearse con `vera_framework`.

## Periodicidad

### Job programado
`midas_bronze_silver_<target>` corre diariamente a las **07:00** hora Bogota
(`PAUSED` en dllo, `UNPAUSED` en uat/pdn).

### Job no programado
`midas_check_conectividad_<target>` (solo uat/pdn): diagnostico on-demand de red y
credenciales hacia Oracle. Sin schedule.

## Glosario rapido de componentes del repo
- `databricks.yml`: definicion del bundle (targets, jobs, variables).
- `notebooks/00_creacion_objetos_midas.py`: crea/siembra el plano de control (task `crear_objetos`).
- `notebooks/check_conectividad_oracle.py`: preflight de conectividad Oracle.
- `src/midas/db/`: conector JDBC, queries y extraccion a Parquet.
- `src/midas/framework/`: cliente del plano de control y orquestador de la cadena.
- `src/midas/main_*.py`: runners (`spark_python_task`) de cada paso.
- `pipeline/deploy-bundle.yml`: CI/CD.

## Autocritica
- El proyecto mezcla terminos funcionales y tecnicos en castellano e ingles.
- Coexisten nombres historicos (`source_base_path: midas_agent` en los Volumes) con la
  nueva nomenclatura del bundle; se conservan por continuidad de las tablas Bronze.
