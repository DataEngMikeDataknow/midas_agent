# Etapa 4 — Construcción del nuevo agente inteligente

Esta entrega agrega la capa de ciencia de datos para MIDAS sin reemplazar la
ingeniería de datos existente.

## Alcance implementado

- Prompt maestro versionado en Markdown.
- Prompt de clasificación.
- Catálogo de decisiones funcionales.
- Catálogo de estados técnicos.
- Contrato JSON de salida.
- Validador de respuesta.
- Parser robusto de salida del LLM.
- Registro de casuísticas.
- Tools SQL de Unity Catalog para variación significativa.
- Agente Stage4 desplegable de forma independiente.
- Inferencia batch Stage4 con logs estructurados.

## Flujo

```text
Silver tables
  ↓
SQL Functions Unity Catalog
  ↓
agent/stage4_agent.py
  ↓
main_stage4_inference.py
  ↓
Gold table
```

## Archivos principales

- `agent/stage4_agent.py`
- `src/midas/main_stage4_tools.py`
- `src/midas/main_stage4_deploy.py`
- `src/midas/main_stage4_inference.py`
- `src/midas/agent/cases/variacion_significativa/prompt.md`
- `src/midas/agent/cases/variacion_significativa/schema.json`
- `src/midas/agent/cases/variacion_significativa/rules.yaml`

## Pendientes funcionales

- Ajustar reglas con la documentación real del analista Luis Eduardo.
- Confirmar texto real de la actividad de variación significativa en SPS/EPM.
- Validar nombres de columnas finales en Silver.
- Construir dataset de evaluación de 40.000 registros.
- Comparar contra decisión de analista.
- Iterar prompt con discrepancias.
