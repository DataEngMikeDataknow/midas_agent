# Etapa 4 - Construcción del nuevo agente inteligente desde cero

## Objetivo

Agregar una capa modular para construir el agente de la casuística **variación significativa contra el mes anterior**, sin romper el agente actual de diferencia acueducto-alcantarillado.

La propuesta del proyecto indica que el sistema debe evolucionar hacia un enfoque multiagente/multi-casuística en Databricks/Mosaic AI, con resultado final en tablas Databricks y sin comportamiento conversacional para usuarios funcionales.

## Qué agrega esta etapa

1. Configuración por casuística.
2. Prompt versionable en markdown.
3. SQL Functions específicas para el contexto de variación significativa.
4. Agente Stage 4 configurable por tools.
5. Inferencia batch resiliente por orden.
6. Separación entre decisión funcional y estado técnico.
7. Validación JSON estricta antes de escribir en Gold.
8. Tests unitarios iniciales.

## Decisión de arquitectura

No se recomienda iniciar con muchos modelos independientes. Se recomienda iniciar con:

- un modelo base aprobado por EPM;
- múltiples agentes/casuísticas configurables;
- tools y prompts especializados;
- una salida Gold común y auditable.

Esto reduce costo, complejidad operativa y riesgo de gobierno.

## Componentes nuevos

| Componente | Ruta | Propósito |
|---|---|---|
| Framework de agentes | `src/midas/agent_framework/` | Contratos, routing, config y validación |
| Casuística VSMA | `src/midas/agents/variacion_significativa/` | Prompt, reglas, schema y SQL tools |
| Agente MLflow | `agent/stage4_agent.py` | Agente ResponsesAgent configurable |
| SQL Functions Stage 4 | `src/midas/main_tools_stage4.py` | Crea tools UC para el nuevo agente |
| Deploy Stage 4 | `src/midas/main_deploy_stage4.py` | Registra y despliega el agente nuevo |
| Inferencia Stage 4 | `src/midas/main_inference_stage4.py` | Ejecuta batch inference resiliente |
| Snippet DAB | `resources/jobs/stage4_databricks_job_snippet.yml` | Fragmento para integrar al `databricks.yml` |

## Contrato Gold recomendado

La tabla `midas_predicciones_agente_stage4_gold` debería conservar, como mínimo:

- `id_orden`
- `servicio_suscrito`
- `contrato`
- `ciclo`
- `actividad`
- `servicio`
- `case_id`
- `decision_payload`
- `business_decision`
- `technical_status`
- `requires_human_review`
- `confidence_score`
- `cause_category`
- `error_code`
- `error_message`
- `run_id`
- `fecha_inferencia`

## Manejo de errores

Una orden fallida no debe detener el lote completo.

- Si falla una orden individual: se marca con `technical_status` de error y `requires_human_review=true`.
- Si falla el endpoint: se reintenta y, si persiste, queda `PENDIENTE_REINTENTO` o `ERROR_ENDPOINT`.
- Si el JSON del agente no es válido: queda `ERROR_VALIDACION_JSON`.
- Una falla técnica no debe convertirse en decisión funcional.

## Pendientes funcionales con EPM

Antes de endurecer la etapa 4, se debe confirmar:

1. Nombre/código exacto de la actividad para variación significativa.
2. Umbrales oficiales de variación por servicio.
3. Catálogo final de decisiones de negocio.
4. Criterios de visita, ajuste, sin novedad y revisión manual.
5. Estructura final de la tabla Gold.
6. Dataset histórico validado por analista para medir desempeño.
7. Criterio de aceptación de la POC.
