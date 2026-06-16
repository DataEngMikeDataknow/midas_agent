# Glosario y terminos

## Proposito
Este documento reduce la dependencia de contexto informal. Debe permitir que una persona nueva entienda el lenguaje tecnico y funcional usado en MIDAS.

## Terminos del dominio

### MIDAS
Nombre del flujo/solucion que analiza ordenes de calidad de facturacion y produce una decision automatizada o semiautomatica.

### Orden de calidad
Registro de negocio que representa un caso a revisar por diferencias o anomalas de facturacion.

### Orden de critica
Orden operativa asociada a validaciones, correcciones o revisiones previas sobre consumos, lecturas o causales.

### Acueducto / Alcantarillado
Servicios publicos comparados por el agente para determinar si existe o no una inconsistencia que requiera ajuste.

### Sin ajuste
Decision del agente cuando los datos no justifican una correccion.

### Con ajuste
Decision del agente cuando los datos indican que debe corregirse o nivelarse el consumo / facturacion.

### Investigacion manual requerida
Decision de seguridad cuando la informacion no es suficiente, es inconsistente o no permite una conclusion automatica confiable.

## Terminos de plataforma

### Databricks Asset Bundles (DAB)
Empaquetado declarativo de jobs, variables y configuracion de despliegue en `databricks.yml`.

### Unity Catalog (UC)
Capa de gobierno de datos donde viven catalogos, schemas, tablas, funciones SQL y modelos registrados.

### Serving Endpoint
Endpoint HTTP administrado por Databricks que expone el agente para invocaciones online.

### Service Principal (SP)
Identidad no humana usada por pipelines y jobs para autenticarse y ejecutar acciones.

### Bundle root_path
Ruta en el workspace donde Databricks almacena el bundle desplegado.

### MLflow Experiment
Ubicacion donde se registran runs de despliegue del modelo.

### UC SQL Function
Funcion SQL registrada en Unity Catalog. En MIDAS se usan como tools del agente:
- `get_hist_fact`
- `get_ordenes_critica`

### ResponsesAgent
Interfaz de MLflow para agentes con entrada/salida estilo Responses API.

### External Location
Ubicacion externa de storage usada para publicar resultados CSV.

## Terminos de pipeline

### CI
Pipeline de build, tests y quality gate definido en `azure-pipelines.yml`.

### CD
Pipeline de despliegue definido en `pipelines/deploy-cicd.yml`.

### DEV / UAT / PROD
- DEV: desarrollo y pruebas funcionales
- UAT: validacion previa a produccion
- PROD: ambiente productivo

## Periodicidad

### Job programado
`midas_load_transform_data` tiene cron diaria a las 08:30 hora Bogota.

### Jobs no programados
- `midas_agent_deploy_ops`: se ejecuta desde pipeline o manualmente
- `midas_agent_inference`: se dispara al final de la carga diaria y tambien puede correrse de forma aislada

## Glosario rapido de componentes del repo
- `src/midas/`: runners y logica de procesamiento en Databricks
- `src/midas/agent/`: tools y prompt del agente
- `agent/agent.py`: implementacion empaquetada del agente para serving
- `data_fetcher/`: extractor Oracle -> parquet
- `tests/`: pruebas unitarias y smoke manual del endpoint

## Autocritica
- El proyecto mezcla terminos funcionales y tecnicos en castellano e ingles.
- Algunas convenciones no estan normalizadas entre codigo, pipelines y bundle.
- La documentacion intenta estandarizar el lenguaje, pero el repositorio aun conserva nombres historicos y abreviados.
