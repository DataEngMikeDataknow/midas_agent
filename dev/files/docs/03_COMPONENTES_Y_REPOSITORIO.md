# Componentes y repositorio

## Baseline documentado
- Rama base: `pruebas`
- Referencia operativa: estado al 2026-04-09

## Estructura del repositorio

```text
Midas/
|-- agent/
|   `-- agent.py
|-- data_fetcher/
|   |-- main.py
|   `-- src/
|-- docs/
|-- pipelines/
|   `-- deploy-cicd.yml
|-- src/
|   `-- midas/
|       |-- main_ingestion.py
|       |-- main_transform.py
|       |-- main_tools.py
|       |-- main_deploy.py
|       |-- main_inference.py
|       `-- agent/
|-- tests/
|-- azure-pipelines.yml
`-- databricks.yml
```

## Inventario por componente

### `databricks.yml`
Fuente de verdad del bundle:
- targets dev/uat/prod
- variables de catalogo, endpoint y paths
- jobs y permisos
- cluster compartido

### `pipelines/deploy-cicd.yml`
Pipeline CD:
- despliegue manual a DEV
- despliegue automatico/manual a UAT
- PROD bloqueado
- instala Terraform explicito para soportar DABs

### `azure-pipelines.yml`
Pipeline CI:
- pytest
- coverage
- SonarQube
- despliegue de DEV para ramas `feature/*`

### `src/midas/*.py`
Runners productivos del bundle:
- `main_ingestion.py`
- `main_transform.py`
- `main_tools.py`
- `main_deploy.py`
- `main_inference.py`

### `src/midas/agent/tools.py`
Define las SQL Functions de Unity Catalog usadas por el agente.

### `src/midas/agent/prompts.py`
Contiene el prompt fuente de negocio usado por el agente.

### `agent/agent.py`
Implementacion empaquetada del agente para MLflow / Serving.

### `data_fetcher/`
Proceso externo al bundle para obtener parquet desde Oracle.

### `tests/`
Pruebas unitarias y smoke manual del endpoint.

## Componentes activos vs legacy

### Activos
- Python runners en `src/midas/`
- agente en `agent/agent.py`
- prompt en `src/midas/agent/prompts.py`
- extractor `data_fetcher/`
- pipelines `azure-pipelines.yml` y `pipelines/deploy-cicd.yml`

### Legacy eliminable o ya eliminado
- notebooks historicos de entrenamiento, deploy e inferencia
- duplicados de codigo no usados por el bundle
- caches locales (`.databricks`, `.ruff_cache`, `.pytest_cache`)

## Dependencias operativas

### Databricks
- Databricks CLI v2
- Terraform 1.5.5 para comandos bundle
- workspace host por ambiente
- Service Principal por ambiente

### Python
- pytest y pytest-cov para CI
- mlflow
- databricks-sdk
- databricks-openai
- databricks-vectorsearch
- backoff

### Oracle / data_fetcher
- variables `.env`
- driver `oracledb`
- opcionalmente Oracle Instant Client

## Desalineaciones detectadas

### Tests desfasados
- `tests/test_deploy.py` aun valida `databricks.agents.deploy()`, pero `main_deploy.py` ya no usa ese flujo.
- `tests/test_inference.py` aun espera `ai_query`, pero `main_inference.py` invoca el endpoint por HTTP con una UDF.

### Duplicidad parcial de prompts
Existe prompt en `src/midas/agent/prompts.py` y una copia legacy en `agent/prompts.py`. La implementacion operativa usa la version bajo `src/midas/agent/`.

### Drift entre CI y CD
- `pipelines/deploy-cicd.yml` instala Terraform explicitamente.
- `azure-pipelines.yml` no lo instala.
- En agentes sin cache o sin acceso saliente a HashiCorp, esto puede romper `bundle validate/deploy` en CI aunque CD funcione.

## Autocritica del repositorio
- El repositorio ya no depende de notebooks para el flujo productivo, pero aun conserva huellas historicas y workarounds temporales.
- La estructura actual es entendible para quien conoce Databricks, pero no es todavia una plataforma completamente endurecida para terceros sin transferencia previa.
