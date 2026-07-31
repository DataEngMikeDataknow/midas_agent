## Handoff — 2026-07-30

**Qué se tocó:** `src/midas/db/queries.py`, `src/midas/db/processing.py`,
`src/midas/framework/chain_runner.py`, `src/midas/main_ingestion.py`,
`notebooks/00_creacion_objetos_midas.py`, `docs/semantica_campos_caso2.md`.
Commits `bf40368`..`79480cc` (2026-07-29 → 2026-07-30), ya commiteados por el usuario.

**Qué cambió de fondo:** Bronze del Caso 2 pasó de un modelo con **dimensiones materializadas**
a uno **sin ellas**: la matriz de "estado de corte facturable" ahora se resuelve inline (dos
subconsultas correlacionadas) directamente en `midas_datos_basicos_producto_bronze`, y el roster
de servicios por contrato se retiró por completo porque ya era derivable de la misma tabla. A
cambio se agregaron los **periodos traducidos** (año/mes/ciclo o fechas de inicio/fin) en las 7
tablas que antes solo traían el id crudo de periodo, una tabla nueva de **Pérdidas No
Operacionales**, y una 4ª rama en la query de órdenes críticas que trae la **orden de decisión
del analista** (actividad 7400027) — la pieza que cerraba el hallazgo de negocio del caso
7400027. El plano de control quedó en **12 cargas**: partiendo de las 13 que dejó la poda previa
(T2, ver commits anteriores), esta v3 retira 2 (la dimensión de facturable y el roster de
contrato) y agrega 1 (PNO).

**¿Esto revierte o reemplaza algo anterior?** Sí. Reemplaza el modelo con dimensiones de la v2
(`midas_dim_estado_corte_facturable_bronze` y `midas_datos_servicios_contrato_bronze`): ambas se
**desactivaron** en el control (no se borraron, para conservar trazabilidad en
`midas_log_cargas`), y el `DROP TABLE` correspondiente quedó redactado pero **sin ejecutar** en
`scripts/migracion_v3_limpieza.sql`. La decisión de retirar el roster (`servicios_contrato`) se
tomó tras verificar con datos reales de `dllo` que las 782 filas que un `LEFT ANTI JOIN` había
señalado como "huérfanas" eran en realidad residuo de corridas anteriores con
`abortar_en_fallo=False`, no información nueva. El Caso 1 **no se tocó**: sus 8 queries y su
patrón de materialización siguen intactos.

**¿Necesita un ADR nuevo?** No para esta capa Bronze — las decisiones (por qué no hay
dimensiones, por qué se retiró el roster, por qué la 4ª rama y no una tabla nueva) están
documentadas inline en el propio código y en `docs/semantica_campos_caso2.md`. Sí se abrió un
ADR nuevo, pero es de la capa **Silver** que se construyó encima
(`docs/adr/0002-grano-historial-consumo.md`, sobre el grano de `historial_consumo`), no de este
handoff de Bronze.

**Queda abierto:**
- `scripts/migracion_v3_limpieza.sql` (los `DROP TABLE` de las 2 tablas retiradas) sigue **sin
  ejecutar**, a la espera de que alguien confirme que ya no hace falta ningún rollback.
- El notebook `notebooks/31_validacion_v3.py` corrió en `dllo` con resultado **85 OK / 12
  REVISAR / 0 FALLA / 1 N/A**. Los 12 REVISAR no bloquean, pero incluyen: cobertura de periodos
  en `detalle_cargos` al 81,24% (no 100%), y que la comparación contra la corrida previa no dio
  idéntica (esperable: hay datos nuevos entre corridas).
  El `explorador_modelo_bronze_caso2` (V11, autocontenido contra Oracle real) validó puntualmente
  la rama 4 antes de escribirla; no quedó como suite recurrente.
- Sobre esta v3 de Bronze ya se construyó la capa Silver completa del Caso 2 (13 objetos, ver
  `docs/contrato_silver.md`); ese trabajo consumió y depende de todo lo descrito aquí, así que
  cualquier ajuste posterior a Bronze v3 debe revisarse contra ese contrato antes de aplicarse.
- Pendientes de negocio que Bronze deja para que Silver/el agente resuelvan (no bloquean Bronze,
  pero condicionan features corriente abajo): el concepto `87` como cargo de consumo normal es
  inferencia sobre capturas de pantalla, no dato confirmado; y `estado_pno` en la Bronze de PNO
  va crudo, sin verificar si tiene catálogo asociado.

---

### Contexto ampliado (para quien no siguió la sesión)

**Punto de partida:** al cierre de la v2, Bronze tenía 21 cargas: la cadena original del Caso 1
(8) más una capa de "dimensiones de referencia" (9) pensada para resolver códigos-descripción
por join, más 4 promociones del Caso 2. Esa capa de dimensiones nunca llegó a ser la única
fuente de verdad — las queries de origen ya traían la resolución inline en varios casos — y
mantenerla implicaba una sincronización redundante.

**Qué motivó el giro a "cero dimensiones" (invariante I11):** con el modelo de dimensiones, un
consumidor tenía dos caminos para llegar al mismo dato (inline en la query vs. join a la
dimensión), y nada garantizaba que coincidieran. La v3 fuerza un solo camino: todo
código-descripción se resuelve con una subconsulta correlacionada en el punto de uso. Esto
redujo el plano de control de 21 a 12 cargas.

**Por qué la rama 4 y no una tabla nueva (el giro más importante de la sesión):** la
investigación del caso 7400027/"ORDEN DECISIÓN ANALISTA" pasó por dos hipótesis. La primera
propuesta agregaba dos tablas Bronze nuevas para capturar esa actividad y sus comentarios. Tras
una verificación visual del usuario contra el modelo real (que esta actividad es estructuralmente
idéntica a las otras 3 ramas de `QUERY_ORDENES_CRITICA_PEVIA`, solo que sin filtro de tipo de
consumo porque `or_order_activity` no lo tiene), se descartaron esas dos tablas por completo y en
su lugar se agregó una 4ª rama `UNION` a la query existente. Esto generó un bug real durante la
implementación — la 4ª rama no podía filtrar por tipo de consumo, así que el driver de Python
producía filas duplicadas al iterar `(periodo, servicio, tipo_consumo)` — resuelto con
`drop_duplicates()` después del `concat` en `processing.py`, con dos tests de regresión.

**Riesgo de encadenamiento descubierto y corregido:** la rama 4 devuelve `tipo_consumo = NULL`
por construcción (no aplica). El código de comentarios (`run_query_comentarios_ordenes`) hacía
`row.TIPO_CONSUMO.split('-')` sin guardia, y ese paso corre con `abortar_en_fallo=True` dentro de
la cadena del Caso 1 — un `AttributeError` ahí habría abortado *toda* la extracción, no solo la
pieza del Caso 2. Se agregó el guard `is None or pd.isna(...)` antes del `.split()`.

**Estado de los tests:** 100 pasan / 1 falla preexistente (`test_ensure_schema_exists`, ajena a
este trabajo, no se toca por acuerdo previo) al cierre de esta v3 de Bronze. La suite creció
después con el trabajo de Silver hasta 114/1.
