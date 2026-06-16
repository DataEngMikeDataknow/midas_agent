# MIDAS - Archivos añadidos para Etapa 4

Copia el contenido de este ZIP sobre la raíz del repositorio extraído. La raíz esperada es la que contiene `dev/files/`.

## Archivos y rutas exactas

| Ruta destino | Propósito |
|---|---|
| `dev/files/config/midas_cases.json` | Configuración de casuísticas habilitadas |
| `dev/files/config/midas_decision_catalog.json` | Catálogo común de decisiones y estados técnicos |
| `dev/files/src/midas/agent_framework/__init__.py` | Exposición del framework Stage 4 |
| `dev/files/src/midas/agent_framework/contracts.py` | Contratos de decisión, evidencia y estados |
| `dev/files/src/midas/agent_framework/case_config.py` | Loader de configuración por casuística |
| `dev/files/src/midas/agent_framework/case_router.py` | Router de órdenes hacia casuísticas |
| `dev/files/src/midas/agent_framework/prompt_loader.py` | Loader de prompts markdown |
| `dev/files/src/midas/agent_framework/decision_validator.py` | Validador de salida JSON del agente |
| `dev/files/src/midas/agents/variacion_significativa/__init__.py` | Paquete de la primera casuística Stage 4 |
| `dev/files/src/midas/agents/variacion_significativa/prompt.md` | Prompt maestro de variación significativa |
| `dev/files/src/midas/agents/variacion_significativa/rules.json` | Reglas y umbrales placeholder versionables |
| `dev/files/src/midas/agents/variacion_significativa/schema.json` | Contrato JSON esperado del agente |
| `dev/files/src/midas/agents/variacion_significativa/tools.py` | Builder de SQL Functions específicas |
| `dev/files/src/midas/main_tools_stage4.py` | Runner para crear SQL Functions Stage 4 |
| `dev/files/src/midas/main_deploy_stage4.py` | Runner para registrar/desplegar el agente Stage 4 |
| `dev/files/src/midas/main_inference_stage4.py` | Runner de inferencia batch resiliente |
| `dev/files/agent/stage4_agent.py` | Agente MLflow/ResponsesAgent configurable |
| `dev/files/resources/jobs/stage4_databricks_job_snippet.yml` | Snippet para integrar al `databricks.yml` |
| `dev/files/docs/10_ETAPA_4_AGENTE_INTELIGENTE_DESDE_CERO.md` | Documentación técnica de la etapa |
| `dev/files/tests/stage4/test_decision_validator_stage4.py` | Tests del validador JSON |
| `dev/files/tests/stage4/test_case_router_stage4.py` | Tests del router por casuística |

## Integración mínima recomendada

1. Copiar los archivos.
2. Ejecutar tests locales del framework:

```bash
cd dev/files
pytest tests/stage4 -q
```

3. Crear SQL Functions Stage 4 en Databricks:

```bash
python ./src/midas/main_tools_stage4.py \
  --catalog <catalog> \
  --schema <schema> \
  --pipeline_sp <service-principal-id> \
  --grant_account_users false
```

4. Integrar manualmente el contenido de:

```text
resources/jobs/stage4_databricks_job_snippet.yml
```

en `dev/files/databricks.yml`.

## Nota importante

Este ZIP no modifica `databricks.yml` automáticamente para evitar romper los jobs actuales. Incluye un snippet listo para revisión y pegado controlado.
