# Handoff — Ajuste quirúrgico: racionalización de dimensiones (v3)

| | |
|---|---|
| **Fecha** | 2026-07-22 (posterior a `HANDOFF_2026-07-22_bronze_caso2.md`) |
| **Alcance** | Ajuste **pre-commit** sobre el trabajo Bronze Caso 2. Nada estaba commiteado ni desplegado |
| **Rama / HEAD** | `feature/midas_data_platform` · `9224b6b` |
| **Estado** | **Sin commit** y **sin deploy** |
| **Tests** | **56 pasan / 1 preexistente** (`test_ensure_schema_exists`, intocable) |
| **Control plane** | **21 → 13 cargas** |

---

## 1. La decisión

El Caso 1 **nunca materializó catálogos**: los resuelve **inline** en las queries de extracción
con subconsultas correlacionadas —
`(SELECT escocodi||'-'||escodesc FROM estacort WHERE escocodi = sesuesco) AS estado_corte` —
por lo que las Bronze de datos **ya traen código y descripción juntos**.

Las 9 dimensiones creadas en v1/v2 eran, salvo una, **redundantes con ese patrón**. Este era el
momento más barato para podarlas: nada commiteado ni desplegado, y el costo de operarlas sería
permanente.

| Dim | Destino | Razón |
|---|---|---|
| `midas_dim_estado_corte_facturable_bronze` | **SE QUEDA** | No es una etiqueta: es una **matriz de decisión** (S/N por estado × servicio). El agente la consulta como **regla** y necesita las combinaciones **completas**, no solo las presentes en los datos del día. Negocio pidió que fuera dinámica |
| `tipo_consumo`, `observacion_lectura`, `calificacion`, `concepto`, `causal_cargo`, `metodo_calculo`, `tipo_solicitud`, `estado_investigacion` | **ELIMINADAS** | Redundantes: el código ya llega traducido inline en las Bronze de datos |

**Upgrade path documentado (no implementado):** si el científico de datos necesitara **enumerar**
catálogos completos (para prompts o tools), la solución es **UNA** tabla genérica
`midas_dim_catalogos_bronze (catalogo, codigo, descripcion)` con `UNION ALL` — no volver a nueve
tablas.

---

## 2. Modelo resultante — 13 cargas

| Grupo | tipo_carga | Orden | Cantidad |
|---|---|---|---|
| Dimensión facturable | `QUERY_FULL_OVERWRITE` | 1 | 1 |
| Cadena Caso 1 | `FULL_CHAINED` | 11–18 | 8 |
| Promociones Caso 2 | `FULL_CHAINED` | 21–24 | 4 |

`midas_parametros` **no cambia** (10 parámetros).

---

## 3. Cambios por archivo

| Archivo | Cambio |
|---|---|
| `src/midas/db/queries.py` | **T1:** `QUERY_CONSUMOS_CONTRATO` +`calificacion` inline; `QUERY_INVESTIGACION_CONSUMO` + `tipo_consumo` inline y **+`estado_investigacion_desc`** (desde `pe_invest_cons_state`), manteniendo el código crudo. **T2:** eliminadas las 8 constantes `QUERY_DIM_*`; queda la de facturable + nota del patrón inline y upgrade path |
| `src/midas/db/processing.py` | Eliminadas 8 `run_query_dim_*` y sus entradas de Parquet; queda `run_query_dim_estado_corte_facturable` |
| `src/midas/framework/chain_runner.py` | Eliminados 8 `PASO_DIM_*` y la lista `_DIMS`; queda un solo paso de dimensión (sigue con `abortar_en_fallo=False`) |
| `notebooks/00_creacion_objetos_midas.py` | Seed de 21 → **13** filas; `N_ESPERADO` sigue calculándose solo (mecanismo intacto) |
| `src/midas/main_ingestion.py` | `build_tables_config()` y `_QUERY_KEY` de 21 → **13** |
| `notebooks/30_validacion_midas.py` | **T3:** bloque "Catálogos one-off" — F1b `pe_invest_cons_state`, **F1c `ps_package_type`** (nueva, insumo para el mapeo de negocio). Read-only, no crean tablas |
| `tests/test_caso2_bronze.py` | Conteos 21 → 13; **test negativo nuevo** (`test_dims_podadas_no_existen`); fixture estructural de facturable **intacto** |
| `README.md` | 13 cargas + sección "Por qué UNA sola dimensión" + upgrade path |
| `docs/semantica_campos_caso2.md` | §0.3 nueva con la decisión; referencias a dims podadas corregidas a "resuelto inline" |

---

## 4. Verificación (criterios de aceptación)

| # | Criterio | Resultado |
|---|---|---|
| 1 | Control plane con exactamente 13 cargas | ✅ `SEED = 13`; `tables_config = 13`; `_QUERY_KEY = 13` |
| 2 | Toda categórica de las 4 tablas nuevas llega como código-descripción | ✅ Roster (espejo) ya resolvía; consumos_contrato +`calificacion`; investigación +`tipo_consumo`/+`estado_investigacion_desc`. **`QUERY_DETALLE_SOLICITUDES` ya resolvía todo inline** (`package_type`, `package_status`, `reception_type`, `vendor`, `organizat_area_id`) → **sin excepción que documentar** |
| 3 | Cero referencias huérfanas | ✅ grep limpio fuera de `tests/` (allí son intencionales: lista de nombres podados del test negativo) |
| 4 | Tests en verde, fixture intacto, sin regresiones | ✅ 56 pasan / 1 preexistente |
| 5 | Docs cuentan la historia (9→1 y upgrade path) | ✅ README + `semantica_campos_caso2.md` §0.3 |

**Invariantes:** I3 OK · `main_data_fetcher.py` byte-idéntico · Opción B (0 `libraries:`) ·
queries del Caso 1 **intactas** · estándar de nombres · `job_name` en toda fila.

**Preservado del handoff anterior (no se tocó):** adopción de solicitudes con
`_adaptar_solicitudes_a_bronze`, hallazgo `consumption_period = id_periodo_consumo`,
`QUERY_SERVICIOS_CONTRATO` espejo, `QUERY_CONSUMOS_CONTRATO`, `abortar_en_fallo=False`,
`midas_parametros`, PK compuesta en `ingestion.py`.

---

## 5. Riesgo de GRANTs — **reducido**

Antes: 8 catálogos nuevos que podían fallar por permisos. Ahora:

| Objeto | Situación |
|---|---|
| Catálogos de la cadena (`estacort`, `tipocons`, `obselect`, `calivaco`, `concepto`, `causcarg`, `mecacons`…) | **Ya concedidos** — los usa el Caso 1 inline desde siempre |
| `confesco` + `servicio` | **Nuevos** (matriz facturable). Si falta el GRANT, la dim queda FALLIDA **no-fatal** |
| `pe_invest_cons_state` | Nuevo, pero **inline** dentro de la query de investigación |
| `ps_package_type` | Ya lo usa `QUERY_DETALLE_SOLICITUDES`; además se consulta **one-off** en el notebook |

---

## 6. Próximos pasos

1. **Commit** (v1 + v2 + este ajuste) y **deploy** a dllo.
2. Verificar en `midas_log_cargas`: **13 cargas**, dim facturable poblada, promociones cargando.
3. Correr `notebooks/30_validacion_midas.py` celdas **F1b/F1c** para llevar los catálogos
   completos a negocio (mapeo de `package_type_id` — PENDIENTE-NEG).
4. Silver del Caso 2 (fuera de alcance de esta entrega).

**PENDIENTE-NEG sin cambios:** mapeo `package_type_id`, marca en `obselect` que modifica el
consumo, regla "SS vigente", calificación/conceptos (diferidos al caso-por-caso con el DS).
