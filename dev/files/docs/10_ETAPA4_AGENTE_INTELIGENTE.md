# Etapa 4 - Construcción del nuevo agente inteligente MIDAS

## Alcance implementado

Esta refactorización implementa la Etapa 4 para la casuística prioritaria:

`993 - VARIACION SIGNIFICATIVA CONTRA EL MES ANTERIOR`

El agente quedó diseñado como componente batch en Databricks. No es conversacional. Recibe un contrato de entrada JSON, aplica reglas determinísticas, opcionalmente llama un endpoint LLM/Mosaic AI y persiste resultados Gold, logs y métricas.

## Mapa real de datos usado

El análisis funcional/técnico validó que la fuente principal de entrada es:

- `facturacion.midas_ordenes_calidad_pendientes_silver`

La llave central del modelo es:

- `servicio_suscrito`

La fuente principal para explicar la casuística 993 es:

- `facturacion.midas_datos_lecturas_producto_bronze`

Fuentes complementarias:

- `facturacion.midas_datos_detalle_solicitudes_silver`
- `facturacion.midas_historial_critica_silver`
- `facturacion.midas_datos_consumos_producto_bronze` como evidencia secundaria

No se usa `resultado_ia` como salida final porque no contiene el contrato trazable requerido para una inferencia productiva.

## Tablas creadas por la Etapa 4

### Entrada del agente

- `midas_contexto_agente_993_v0`: contexto estructurado por orden.
- `midas_agent_input_993_v0`: JSON limpio de entrada para el agente.

### Salidas

- `midas_resultado_agente_ordenes_calidad_gold`: resultado final del agente.
- `midas_logs_agente_ordenes_calidad`: logs técnicos y funcionales por orden.
- `midas_metricas_agente_ordenes_calidad`: métricas de ejecución.
- `midas_evaluacion_agente_ordenes_calidad`: comparación contra analista cuando exista tabla de referencia.

## Contrato de entrada del agente

El JSON de entrada se divide en:

- `orden`: datos no personales de la orden.
- `consumo_principal`: consumo facturado actual/anterior, promedio 6m, variaciones y límites.
- `lectura`: lectura anterior/actual, diferencia, constante, PNO y observaciones.
- `calidad_dato`: flags de consumo extremo, inconsistencia y revisión obligatoria.
- `antecedentes`: solicitudes y críticas.
- `evidencia_adicional`: consumos por tipo para no perder anomalías secundarias.

Por gobierno de datos no se envían al LLM:

- `nombre_cliente`
- `identificacion`
- `direccion`
- `pagina`

## Reglas obligatorias

Antes del LLM se aplican reglas duras:

| Regla | Resultado |
|---|---|
| `requiere_revision_por_calidad_dato = true` | Revisión humana |
| `flag_consumo_extremo = true` | Revisión humana |
| `existe_consumo_extremo_en_algun_tipo = true` | Revisión humana |
| `flag_lectura_inconsistente = true` | Posible error de lectura / revisión |
| `tiene_pno = true` | Posible PNO |
| Solicitud con reclamo/PQR/queja/recurso | Escalar |
| Consumo anterior cero y sin promedio útil | Datos insuficientes |
| `flag_fuera_limites = true` | Señal fuerte de variación, revisar |

## Prompt maestro

El prompt maestro está en:

`src/midas/stage4/prompts.py`

Incluye:

- rol del agente;
- contrato real de entrada;
- reglas obligatorias;
- reglas interpretativas;
- categorías permitidas;
- decisiones permitidas;
- schema JSON final;
- few-shots funcionales iniciales para validar con documento de Luis.

## Categorías de clasificación

- `NORMAL`
- `VARIACION_SIGNIFICATIVA_JUSTIFICADA`
- `VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA`
- `CONSTANTE_MAL_CONFIGURADA`
- `POSIBLE_ERROR_LECTURA`
- `POSIBLE_PNO`
- `ESTACIONALIDAD`
- `OBRA_NUEVA_O_CAMBIO_INSTALACION`
- `RECLAMO_RELACIONADO`
- `DATOS_INSUFICIENTES`
- `REQUIERE_REVISION_HUMANA`

## Ejecución recomendada

### 1. Crear / refrescar contexto y probar reglas

```bash
python src/midas/main_stage4_agent.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-07-09 \
  --limite_ordenes 37 \
  --input_mode context \
  --build_context true \
  --modo_ejecucion rules-only
```

### 2. Persistir resultados rules-only

```bash
python src/midas/main_stage4_agent.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-07-09 \
  --limite_ordenes 37 \
  --input_mode context \
  --build_context true \
  --modo_ejecucion persistente
```

### 3. Ejecutar con endpoint LLM

```bash
python src/midas/main_stage4_agent.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-07-09 \
  --limite_ordenes 37 \
  --input_mode context \
  --build_context true \
  --modo_ejecucion persistente \
  --model_endpoint agente_ordenes_calidad_dev_v3
```

### 4. Prueba de 40.000 registros

La prueba de 40.000 registros debe iniciar en `rules-only` o con particionamiento/cuota aprobada del endpoint:

```bash
python src/midas/main_stage4_agent.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-07-09 \
  --limite_ordenes 40000 \
  --input_mode context \
  --build_context true \
  --modo_ejecucion rules-only
```

## Evaluación contra analista

Cuando exista tabla de etiquetas del analista:

```bash
python src/midas/main_stage4_evaluation.py \
  --catalog epm_datalabs_catalog_dllo \
  --schema facturacion \
  --ambiente dev \
  --fecha_proceso 2026-07-09 \
  --run_id <RUN_ID> \
  --analyst_table facturacion.resultado_ia \
  --analyst_order_column SOLIC \
  --analyst_label_column CATEGORIA_HALLAZGO
```

Nota: `resultado_ia` se usa como referencia histórica/analista, no como tabla final del nuevo agente.
