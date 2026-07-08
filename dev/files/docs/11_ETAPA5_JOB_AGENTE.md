# Etapa 5 - Job productivo del agente MIDAS Órdenes de Calidad

## Objetivo

Implementar el flujo productivo para ejecutar diariamente el agente inteligente de órdenes de calidad en Databricks, registrando resultados en tabla Gold, logs operativos y métricas de inferencia.

El alcance está alineado con la propuesta funcional del proyecto: el resultado del análisis debe quedar disponible en Databricks para consulta del equipo EPM y el agente no opera como chatbot conversacional, sino como proceso batch gobernado.

## Flujo de ejecución

1. Validar parámetros del Job: `fecha_proceso`, `catalog`, `schema`, `ambiente`, `limite_ordenes`, `modo_ejecucion`.
2. Crear o validar tablas Delta de salida.
3. Crear SQL Functions controladas para herramientas del agente.
4. Consultar órdenes pendientes desde Unity Catalog.
5. Armar contexto por orden desde las ocho tablas Bronze.
6. Ejecutar reglas determinísticas obligatorias.
7. Ejecutar LLM si el modo no es `rules-only` y existe endpoint.
8. Validar JSON final.
9. Persistir en Gold, logs y métricas.
10. Registrar eventos de inicio, finalización o fallo del Job.

## Script productivo

Archivo principal:

```bash
src/midas/main_stage5_job.py
```

Parámetros principales:

```bash
--fecha_proceso YYYY-MM-DD
--catalog epm_datalabs_catalog_dllo
--schema facturacion
--ambiente dev|qa|uat|prod
--limite_ordenes 300
--modo_ejecucion dry-run|persistente|rules-only
--model_endpoint agente_ordenes_calidad_dev_v3
--max_retries 3
--crear_sql_functions true
```

## Ejecución manual en Databricks

```bash
python src/midas/main_stage5_job.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-06-23 \
  --limite_ordenes 300 \
  --modo_ejecucion persistente \
  --model_endpoint agente_ordenes_calidad_dev_v3 \
  --max_retries 3
```

## Ejecución batch de validación

Para validar 40.000 registros sin consumo masivo de LLM:

```bash
python src/midas/main_stage5_job.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-06-23 \
  --limite_ordenes 40000 \
  --modo_ejecucion rules-only \
  --max_retries 3
```

## Evaluación contra analista

Después de una corrida persistente, si existe una tabla con decisión del analista:

```bash
python src/midas/main_stage4_evaluation.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-06-23 \
  --run_id <RUN_ID_GENERADO> \
  --analyst_table epm_datalabs_catalog_dllo.facturacion.midas_decisiones_analista_gold \
  --analyst_order_column orden_id \
  --analyst_label_column categoria_analista
```

Métricas calculadas:

- Accuracy contra analista.
- Porcentaje de JSON válido.
- Tasa de revisión humana.
- Distribución de categorías.
- Discrepancias para refinamiento del prompt.

## Asset Bundle

Se agregó el Job:

```text
midas_stage5_agent_job_daily
```

El trigger diario está definido a las 09:00 America/Bogota y queda inicialmente pausado para activación controlada por EPM:

```yaml
schedule:
  quartz_cron_expression: "0 0 9 * * ?"
  timezone_id: "America/Bogota"
  pause_status: PAUSED
```

## Validaciones recomendadas

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run midas_stage5_agent_job_daily -t dev
```

## Criterios de aceptación

- El Job corre con parámetros por ambiente.
- Crea o valida tablas de salida.
- Registra logs de inicio, fin y error.
- Procesa 100 a 300 órdenes/día en modo persistente.
- Permite prueba batch de 40.000 registros en modo `rules-only`.
- Persiste resultados en Gold.
- Calcula métricas de inferencia.
- Permite evaluación contra decisión del analista.
