# Arquitectura y flujos

## Vista general
MIDAS no es solo un agente. Es una cadena completa:
1. extraccion Oracle -> parquet
2. ingesta parquet -> tablas bronze
3. transformacion bronze -> silver
4. creacion de SQL Functions
5. despliegue del agente a un serving endpoint
6. inferencia batch
7. publicacion de resultados

## Arquitectura end to end

```mermaid
flowchart LR
    A[Oracle / fuentes operativas] --> B[data_fetcher]
    B --> C[Parquet en almacenamiento]
    C --> D[midas_load_transform_data]
    D --> E[Tablas bronze]
    E --> F[Transformaciones silver]
    F --> G[SQL Functions UC]
    G --> H[midas_agent_deploy_ops]
    H --> I[MLflow + UC Model]
    I --> J[Serving Endpoint]
    J --> K[midas_agent_inference]
    K --> L[Tabla gold]
    K --> M[CSV en External Location]
```

## Arquitectura logica del bundle

```mermaid
flowchart TD
    A[databricks.yml] --> B[midas_load_transform_data]
    A --> C[midas_agent_deploy_ops]
    A --> D[midas_agent_inference]

    B --> B1[main_ingestion.py]
    B --> B2[main_transform.py]
    B --> B3[main_tools.py]
    B --> D

    C --> C1[main_deploy.py]
    C1 --> C2[MLflow register_model]
    C1 --> C3[Serving endpoint create/update]

    D --> D1[main_inference.py]
    D1 --> J[HTTP invocations al endpoint]
```

## Flujo funcional del agente

```mermaid
sequenceDiagram
    participant Batch as Batch inference
    participant EP as Serving endpoint
    participant LLM as databricks-gpt-oss-120b
    participant UC1 as get_hist_fact
    participant UC2 as get_ordenes_critica

    Batch->>EP: POST /invocations con order_id
    EP->>LLM: prompt + input
    LLM->>UC1: get_hist_fact(order_id)
    UC1-->>LLM: historico de facturacion
    alt requiere validacion operativa
        LLM->>UC2: get_ordenes_critica(ss, periodo)
        UC2-->>LLM: comentarios / ordenes
    end
    LLM-->>EP: JSON final
    EP-->>Batch: respuesta del agente
```

## Flujo por ambientes

```mermaid
flowchart LR
    F[feature/*] --> D[desarrollo]
    D --> P[pruebas]
    P --> R[produccion]

    CI[azure-pipelines.yml] --> F
    CD[pipelines/deploy-cicd.yml] --> D
    CD --> P
    CD --> R
```

## Componentes operativos

### Extraccion
- `data_fetcher/main.py`
- Usa credenciales Oracle por `.env`
- Genera archivos parquet locales

### Ingestion
- `src/midas/main_ingestion.py`
- Carga parquet a tablas bronze
- Garantiza schema destino

### Transformacion
- `src/midas/main_transform.py`
- Construye tablas silver a partir de bronze

### Tools
- `src/midas/main_tools.py`
- Registra funciones SQL en UC

### Deploy del agente
- `src/midas/main_deploy.py`
- Registra modelo en MLflow/UC
- Crea o actualiza endpoint

### Inference batch
- `src/midas/main_inference.py`
- Invoca el endpoint por HTTP
- Escribe Delta y CSV

## Programacion y periodicidad

### Programacion activa
`midas_load_transform_data`:
- cron: `0 30 8 * * ?`
- timezone: `America/Bogota`
- estado: `UNPAUSED`

### Encadenamiento
`midas_load_transform_data` termina disparando `midas_agent_inference` via `run_job_task`.

### Ejecucion por pipeline
`midas_agent_deploy_ops` corre desde Azure DevOps despues del `bundle deploy`.

## Decisiones de diseno relevantes

### Root path compartido
Se usa `/Shared/bundles/${bundle.name}/${bundle.target}` para evitar dependencia de carpetas personales.

### Cluster por job
Se usa `new_cluster` compartido por el bundle en vez de `existing_cluster_id`, para facilitar UAT/PROD y desacoplar el flujo de un cluster manual.

### Endpoint versionado en DEV
El endpoint de DEV quedo como `agente_ordenes_calidad_dev_v3` por conflictos previos de permisos con endpoints heredados.

## Autocritica de arquitectura
- La arquitectura operativa esta funcional, pero no completamente homogenea entre ambientes.
- `main_deploy.py` conserva un workaround de compatibilidad con `databricks-sdk` en lugar de un flujo limpio y definitivo con `databricks.agents`.
- PROD no esta listo como ambiente real; aun contiene placeholders de catalogo y storage.
