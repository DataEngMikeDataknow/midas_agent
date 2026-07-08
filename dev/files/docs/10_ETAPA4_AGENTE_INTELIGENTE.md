# Etapa 4 — Construcción del nuevo agente inteligente MIDAS

## 1. Análisis de la arquitectura actual

El repositorio ya sigue una arquitectura Databricks-oriented con estos bloques:

- `src/midas/db`: extracción Oracle mediante queries parametrizadas y escritura Parquet.
- `src/midas/framework`: control de cargas y logging de la cadena Bronze.
- `src/midas/ingestion.py`: carga Parquet hacia Delta/Unity Catalog.
- `src/midas/transformations.py`: materialización Silver.
- `src/midas/agent`: prompts y SQL Functions usadas como tools del agente legacy.
- `src/midas/main_*`: runners ejecutables desde `spark_python_task` en Databricks Jobs.
- `databricks.yml`: Databricks Asset Bundle con jobs de carga, despliegue e inferencia.

La Etapa 4 se implementa respetando ese patrón: paquete Python dentro de `src/midas`, runners `main_stage4_*`, SQL functions en Unity Catalog, tablas Delta Gold y documentación en `docs`/`sql`. No se crea una aplicación aislada ni una estructura paralela fuera del repo.

## 2. Estructura agregada

```text
src/midas/stage4/
  constants.py          # query_keys, tablas, categorías, decisiones
  config.py             # parámetros de ejecución dev/qa/prod
  models.py             # entidades de dominio flexibles
  data_access.py        # Data Access Layer sobre Unity Catalog
  normalization.py      # normalización Bronze -> dominio
  rules.py              # reglas determinísticas y guardrails
  schemas.py            # JSON schema + DDL tablas Gold/logs/métricas
  validator.py          # validación estricta de salida
  prompts.py            # system prompt y prompt de clasificación
  llm.py                # cliente Databricks Serving
  agent_orchestrator.py # orquestación de inferencia
  persistence.py        # persistencia Delta
  evaluation.py         # métricas batch y comparación analista
  tools_sql.py          # SQL Functions controladas

src/midas/main_stage4_tools.py
src/midas/main_stage4_agent.py
agent/agent_stage4.py
sql/stage4_tables.sql
sql/stage4_sql_functions.sql
tests/test_stage4_*.py
```

## 3. Flujo operativo

1. El job lee órdenes desde `midas_ordenes_calidad_pendientes_bronze`.
2. Para cada orden obtiene producto, lecturas, consumos, crítica previa, comentarios, cuentas de cobro y detalle de cargos.
3. Normaliza el contexto a entidades de dominio.
4. Ejecuta reglas determinísticas obligatorias:
   - datos mínimos;
   - variación contra mes anterior;
   - señales de lectura, PNO, constante, reclamo, obra/cambio;
   - guardrails de revisión humana.
5. Si hay endpoint LLM y no es hard stop, llama al agente con prompt cerrado y schema obligatorio.
6. Valida el JSON. Si falla, reintenta una reparación una vez.
7. Si vuelve a fallar, genera fallback auditable con `REQUIERE_REVISION_HUMANA`.
8. Persiste resultados en Gold, logs funcionales/técnicos y métricas de inferencia.

## 4. Fuentes consumidas

Todas las lecturas se hacen desde el catálogo/esquema parametrizado. Para DLLO:

```text
epm_datalabs_catalog_dllo.facturacion.midas_ordenes_calidad_pendientes_bronze
epm_datalabs_catalog_dllo.facturacion.midas_datos_basicos_producto_bronze
epm_datalabs_catalog_dllo.facturacion.midas_datos_lecturas_producto_bronze
epm_datalabs_catalog_dllo.facturacion.midas_datos_consumos_producto_bronze
epm_datalabs_catalog_dllo.facturacion.midas_datos_ordenes_previa_critica_bronze
epm_datalabs_catalog_dllo.facturacion.midas_datos_comentarios_ordenes_bronze
epm_datalabs_catalog_dllo.facturacion.midas_datos_cuentas_cobro_bronze
epm_datalabs_catalog_dllo.facturacion.midas_datos_detalle_cargos_bronze
```

Nota: el repositorio legacy tiene una referencia con typo `midas_datos_cometarios_ordenes_bronze`; la nueva capa usa el nombre correcto y mantiene fallback en `data_access.py` para lecturas programáticas.

## 5. Ejecución

### Crear SQL Functions controladas

```bash
databricks bundle run midas_stage4_agent_quality_orders -t dev
```

O manualmente:

```bash
python src/midas/main_stage4_tools.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion
```

### Dry-run de 100 órdenes

```bash
python src/midas/main_stage4_agent.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-06-23 \
  --limite_ordenes 100 \
  --modo_ejecucion dry-run \
  --model_endpoint agente_ordenes_calidad_dev_v3
```

### Persistente diario

```bash
python src/midas/main_stage4_agent.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-06-23 \
  --limite_ordenes 300 \
  --modo_ejecucion persistente \
  --model_endpoint agente_ordenes_calidad_dev_v3
```

### Prueba batch 40.000 registros

```bash
python src/midas/main_stage4_agent.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-06-23 \
  --limite_ordenes 40000 \
  --modo_ejecucion rules-only
```

Para 40.000 registros con LLM se debe usar endpoint con throughput aprobado y particionar por lotes operativos; no se recomienda ejecutarlo desde driver sin control de cuota.

## 6. Decisiones arquitectónicas

- **Data Access cerrado por query_key**: impide SQL injection y evita que el modelo genere consultas arbitrarias.
- **Dominio flexible**: las fuentes Bronze pueden cambiar capitalización o columnas; los modelos mantienen payload crudo y helpers tolerantes.
- **Reglas antes del LLM**: lo determinístico se resuelve fuera del modelo. El LLM se usa para síntesis, clasificación fina y explicación.
- **Schema estricto**: no se persiste salida inválida como si fuera decisión válida.
- **Reintento único de reparación**: controla costo y evita loops.
- **Fallback conservador**: ante error técnico o datos insuficientes, nunca se aprueba automáticamente.
- **Logs y métricas separados**: los resultados Gold son consumibles por negocio; logs/métricas son para operación MLOps.

## 7. Métricas mínimas

- `porcentaje_json_valido`
- `tasa_revision_humana`
- `latencia_promedio_ms`
- `latencia_p95_ms`
- `errores_herramienta`
- `accuracy` contra analista, si se entrega etiqueta histórica
- reporte de discrepancias por `orden_id`

## 8. Conventional commits sugeridos

```text
feat(stage4): agregar arquitectura modular del agente de ordenes de calidad
feat(stage4): implementar data access layer sobre unity catalog
feat(stage4): agregar reglas deterministicas para variacion significativa
feat(stage4): agregar prompts y validacion estricta de json final
feat(stage4): persistir resultados gold logs y metricas de inferencia
feat(stage4): agregar sql functions controladas para tools del agente
feat(stage4): agregar runner databricks para dry-run y modo persistente
test(stage4): cubrir schema reglas data access y prompts
docs(stage4): documentar ejecucion y decisiones arquitectonicas
```

## Actualización Etapa 4 v1.1.0

Se reforzó el prompt maestro con separación explícita entre reglas obligatorias y reglas interpretativas por IA. El agente queda restringido a inferencia batch, sin comportamiento conversacional, y debe emitir exclusivamente JSON validado.

### Componentes implementados

- `src/midas/stage4/prompts.py`: prompt maestro, prompt de clasificación y prompt de reparación de JSON.
- `src/midas/stage4/rules.py`: reglas determinísticas obligatorias para datos insuficientes, variación significativa, reclamos, PNO, error de lectura, constante e instalación/cambio.
- `src/midas/stage4/agent_orchestrator.py`: orquestación de contexto, reglas, LLM, validación, retry de JSON y guardrails.
- `src/midas/stage4/evaluation.py`: métricas batch, accuracy contra analista y discrepancias.
- `src/midas/main_stage4_evaluation.py`: runner de evaluación contra tabla de decisión del analista.

### Few-shots funcionales incluidos

Los few-shots quedaron dentro del prompt de clasificación como patrones guía para:

1. Variación con cambio de medidor o instalación.
2. Variación significativa sin soporte.
3. Reclamo/PQR relacionado.
4. Constante mal configurada.
5. Datos históricos insuficientes.
6. Posible error de lectura.

Cuando EPM entregue el documento detallado del analista Luis Eduardo con ejemplos reales, estos few-shots deben reemplazarse o complementarse con casos reales anonimizados.

### Evaluación y refinamiento

El ciclo recomendado es:

1. Ejecutar `rules-only` sobre 40.000 registros para validar cobertura, tiempos y calidad del contrato de datos.
2. Ejecutar `dry-run` con 5 a 20 registros para validar LLM y JSON.
3. Ejecutar `persistente` con 100 a 300 órdenes.
4. Comparar contra decisión del analista usando `main_stage4_evaluation.py`.
5. Revisar discrepancias y ajustar prompt/reglas.
6. Versionar prompt como `stage4-vX.Y.Z`.
