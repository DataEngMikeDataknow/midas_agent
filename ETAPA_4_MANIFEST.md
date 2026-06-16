# MIDAS - Proyecto completo con Etapa 4 integrada

Este ZIP fue generado a partir de la base `midas_agent.zip` entregada por el usuario. Se conservaron los archivos existentes y se añadieron/modificaron únicamente los componentes necesarios para la Etapa 4: construcción del nuevo agente inteligente desde cero.

## Cambios principales

- Se agregó una arquitectura extensible por casuística en `dev/files/src/midas/agent_framework/`.
- Se agregó la primera casuística Stage 4: `variacion_significativa_mes_anterior`.
- Se agregó agente MLflow/ResponsesAgent en `dev/files/agent/stage4_agent.py`.
- Se agregaron scripts de tools, deploy e inferencia batch Stage 4.
- Se actualizó `dev/files/databricks.yml` con variables y jobs Stage 4.
- Se agregaron pruebas unitarias Stage 4 en `dev/files/tests/stage4/`.
- Se agregó documentación técnica en `dev/files/docs/10_ETAPA_4_AGENTE_INTELIGENTE_DESDE_CERO.md`.

## Validación local realizada

```bash
cd midas_agent/dev/files
PYTHONPATH=src pytest tests/stage4 -q
```

Resultado esperado: `5 passed`.

## Nota

La integración real con Databricks requiere validar permisos, Unity Catalog, SQL Functions, Mosaic AI Serving Endpoint, variables del target y disponibilidad de tablas Silver/Gold en el workspace de EPM.
