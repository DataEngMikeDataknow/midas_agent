# Handoff técnico — Bronze Caso 2 (`midas_data_platform`)

| | |
|---|---|
| **Periodo cubierto** | 2026-07-21 14:23 GMT-5 → 2026-07-22 |
| **Disparador** | Ejecución de `PROMPT_CLAUDE_CODE_bronze_caso2.md` (v1) y luego su **v2** |
| **Repo / rama** | `midas_data_platform` · `feature/midas_data_platform` |
| **HEAD al cierre** | `9224b6b` ("Remueve instalación libraries") |
| **Estado del working tree** | **TODO sin commitear** (decisión del usuario) |
| **Tests** | **55 pasan / 1 fallo preexistente** (`test_ensure_schema_exists`, intocable por acuerdo) |
| **Deploys** | **Ninguno**. El bundle en Databricks aún corre la versión anterior |

---

## 1. Resumen ejecutivo

Se completó la **capa Bronze del Caso 2** ("993 - VARIACIÓN SIGNIFICATIVA CONTRA EL MES
ANTERIOR"): se cerraron las brechas A1–A4 identificadas en la Fase 1, se agregó un bloque de
**9 dimensiones de referencia** y una **tabla de parámetros** que saca los códigos "mágicos"
del código. El plano de control pasó de **8 a 21 cargas**.

Además se **validó contra datos reales en dllo** (4 chequeos) y eso resolvió 3 pendientes de
negocio/modelo y corrigió 2 supuestos equivocados.

**Ninguna query ni tabla del Caso 1 fue modificada** (frontera contractual: la integración con
el Caso 1 es del cliente).

---

## 2. Cronología del trabajo

| # | Hito | Resultado |
|---|---|---|
| 1 | **Ejecución v1 (F0–F6)** | 8 dims, promociones A1–A4, `midas_parametros`, tests, docs, notebook de validación |
| 2 | **1ª corrida de validación en dllo** | F5 OK; **F2 falló por bug mío**; F4 mal planteado; F1b falló por bundle desactualizado |
| 3 | **Corrección del notebook** | Notebook hecho **autocontenido** (SQL inline) + 2 bugs corregidos |
| 4 | **Ejecución v2 (reconciliación + deltas 1–5)** | +1 dim, roster reescrito como espejo, semántica de investigación resuelta, fixture oficial |
| 5 | **2ª corrida de validación** | **F5, F2, F4, F1b los 4 resueltos** → decisión de adopción de la Bronze de solicitudes |

---

## 3. Descubrimientos (con evidencia)

### 3.1 Las órdenes del Caso 2 ya llegan a Bronze  ✅
`QUERY_ORDENES_PENDIENTES` filtra **solo** por `task_type_id = 883`, **sin `activity_id`**.
Confirmado con datos: actividades presentes en `midas_ordenes_calidad_pendientes_bronze` =
**`993, 1019, 1013, 995, 996, 1012, 1120`**.
→ **No se creó ninguna query de entrada nueva.** El filtro por caso vive aguas abajo (Silver /
registro de agentes) leyendo `midas_parametros`. Descubrimiento lateral: hay **al menos 5
casuísticas más** entrando por el mismo camino.

### 3.2 La Bronze de solicitudes existía "huérfana" y con otro schema  ✅
`midas_datos_detalle_solicitudes_bronze` **ya existía** pero no estaba en la cadena ni en el
control. Su schema es el **mismo dato** de `QUERY_DETALLE_SOLICITUDES` con **nombres en
español** y una columna extra al inicio: `servicio_suscrito` — que **la query no devuelve**
(es el bind `:p_servicio_suscrito`).

| Bronze (posicional) | Origen en la query |
|---|---|
| `servicio_suscrito` | bind `:p_servicio_suscrito` (no venía en el SELECT) |
| `id_solicitud` | `package_id` |
| `usuario` | `subscriber` |
| `tipo_solicitud` | `package_type` |
| `fecha_solicitud` | `request_date` |
| `estado_solicitud` | `package_status` |
| `fecha_atencion_solicitud` | `attention_date` |
| `comentario` | `comment_` |
| `medio_recepcion` | `reception_type` |
| `analista` | `vendor` |
| `area_organizacional` | `organizat_area_id` |

**Riesgo detectado:** recrear la tabla "tal cual la query" la habría dejado **sin
`servicio_suscrito`**, que es justamente la `columna_join` declarada en el control.

### 3.3 `consumption_period` ≡ `id_periodo_consumo` (no `id_periodo_facturacion`)  ✅
Evidencia: **21 valores en común** con `id_periodo_consumo` sobre los mismos SS; **0**
coincidencias con `id_periodo_facturacion`. Formatos: 10 dígitos (`1210131820`) vs 3–4 dígitos
(`938`, `1077`).
→ El join de investigación es por **SS + `id_periodo_consumo`**.

### 3.4 Semántica de `INVEST_CONS_STATE_ID` resuelta  ✅
Catálogo `PE_INVEST_CONS_STATE` (3 estados), columnas `invest_cons_state_id` + `description`
confirmadas:

| Valor | Significado |
|---|---|
| 1 | EN INVESTIGACIÓN (abierta) |
| 2 | IMPUTABLE AL CLIENTE |
| 3 | IMPUTABLE A LA EMPRESA |

**Corrige un supuesto de v1** que decía que el estado 2 parecía "terminal/cerrada": 2 y 3 son
**resoluciones** (a quién se imputa), no cierre.

### 3.5 `QUERY_DATOS_CONSUMOS` no era reutilizable para el roster
Exige **también el periodo**, que no se tiene para los SS hermanos del contrato. Por eso se
creó `QUERY_CONSUMOS_CONTRATO` (nueva, sobre `conssesu` por SS + ventana de 6 meses), que es
exactamente lo validado en la Fase 1. **No se tocó la query existente.**

### 3.6 Bugs propios encontrados y corregidos
- **`mo_motive.product_id` puede ser NULL** y ese grupo domina en volumen: mi query de anclaje
  ordenaba por `count(*)` y devolvía el grupo NULL → `int(None)`. Corregido con `IS NOT NULL`.
- **Comparación de periodos por muestras de SS distintos** daba falso negativo. Corregido a
  cruce de conjuntos completos sobre los **mismos SS**.
- El notebook de validación dependía de código nuevo aún **no desplegado** → se hizo
  **autocontenido** (SQL inline).

---

## 4. Cambios por archivo

### Modificados
| Archivo | Cambio |
|---|---|
| `src/midas/db/queries.py` | +13 queries **nuevas**: 9 `QUERY_DIM_*`, `QUERY_SERVICIOS_CONTRATO` (espejo de `QUERY_DATOS_BASICOS`), `QUERY_CONSUMOS_CONTRATO`, `QUERY_INVESTIGACION_CONSUMO`. **Las 8 de la cadena + `QUERY_DETALLE_SOLICITUDES` intactas** |
| `src/midas/db/processing.py` | +12 funciones de extracción; `servicio_suscrito` materializado en solicitudes; `_adaptar_solicitudes_a_bronze` (mapeo posicional al schema existente) |
| `src/midas/framework/chain_runner.py` | Dimensiones como pasos raíz + 4 promociones; `_ejecutar_paso(abortar_en_fallo=…)` para que lo nuevo **no tumbe la cadena del Caso 1** |
| `src/midas/ingestion.py` | `primary_key` acepta **PK compuesta** (list), retrocompatible con str |
| `src/midas/main_ingestion.py` | `build_tables_config()` a nivel de módulo (21 tablas, testeable) + `_QUERY_KEY` |
| `notebooks/00_creacion_objetos_midas.py` | Seed metadata-driven con **N dinámico** (21) + tabla **`midas_parametros`** |
| `README.md` | Tablas de la cadena Caso 1+2, dims, parámetros, notebooks |

### Nuevos
| Archivo | Qué es |
|---|---|
| `notebooks/30_validacion_midas.py` | Validación **read-only** en dllo (F5/F2/F4/F1b). Autocontenido |
| `docs/semantica_campos_caso2.md` | Fuente de verdad de la semántica del Caso 2 + PENDIENTE-NEG |
| `tests/test_caso2_bronze.py` | 23 tests: binds, `tables_config`, control, fixture facturable, mapeo de solicitudes |

### Sin versionar de antes (siguen untracked)
`docs/00_INDICE.md` … `docs/09_ANEXOS.md`, `notebooks/90_exploracion_campos_caso2.py`.

---

## 5. Modelo Bronze resultante — 21 cargas (`job_name = 'midas_bronze'`)

| Grupo | tipo_carga | Orden | Tablas |
|---|---|---|---|
| **Dimensiones (9)** | `QUERY_FULL_OVERWRITE` | 1–9 | `midas_dim_{estado_corte_facturable, tipo_consumo, observacion_lectura, calificacion, concepto, causal_cargo, metodo_calculo, tipo_solicitud, estado_investigacion}_bronze` |
| **Cadena Caso 1 (8)** | `FULL_CHAINED` | 11–18 | Las 8 originales (intactas) |
| **Promociones Caso 2 (4)** | `FULL_CHAINED` | 21–24 | `detalle_solicitudes` (A1), `servicios_contrato` (A2), `consumos_contrato` (A3), `investigacion_consumo` (A4) |

Más `midas_parametros` (10 parámetros): `task_type=883`, `activity_caso1=1019`,
**`activity_caso2=993`**, `activity_critica=102010`, `activity_decision_analista=7400027`,
`estado_orden_anulada=12`, `tipo_comentario=4002`, `metodo_calculo_facturado=4`,
`ventana_meses_historia=6`, `ventana_meses_observaciones=3`.

---

## 6. Decisiones tomadas

| # | Decisión | Justificación |
|---|---|---|
| D1 | Caso 2 = `activity_id 993`; **sin query de entrada nueva** | La query ya trae todas las actividades (validado con datos) |
| D2 | Extender `job_name = midas_bronze` (un solo job) | Comparte la misma query raíz que el Caso 1 |
| D3 | **ADOPTAR** la Bronze de solicitudes (no recrear) | Negocio: "se va a usar"; recrear la dejaría sin `servicio_suscrito` (= `columna_join`). Mapeo en capa Python, **sin tocar el SQL** |
| D4 | **No commitear** | Decisión del usuario |
| — | Dims/promos con `abortar_en_fallo=False` | Un fallo en lo nuevo (p. ej. GRANT faltante) no debe tumbar la cadena probada del Caso 1 |
| — | Roster = **espejo** de `QUERY_DATOS_BASICOS` | Indicación de Jonatan; consistencia de schema y menos deuda |
| — | Dimensiones con **códigos crudos** | El taller de semántica se difirió a trabajo caso-por-caso con el DS; nada bloquea Bronze |

---

## 7. Calidad e invariantes (verificado al cierre)

- ✅ `pytest`: **55 pasan / 1 fallo preexistente** (no se tocó, por acuerdo)
- ✅ **I3**: sin `spark.read.format("jdbc")` en `src/`
- ✅ **I10**: conector sin cambios (int/float, jamás `Decimal`)
- ✅ **I2**: `insertInto(overwrite=True)`; metadatos solo en creación
- ✅ **Opción B**: **0** declaraciones de `libraries:` en `databricks.yml`
- ✅ `main_data_fetcher.py` **byte-idéntico**
- ✅ Las 8 queries de la cadena y `QUERY_DETALLE_SOLICITUDES` **sin modificar**
- ✅ Estándar `midas_<dominio>_<capa>`; `job_name` en toda fila nueva de control

---

## 8. Riesgos y notas para el despliegue

1. **Hay que desplegar el bundle.** Los cambios tocan `queries.py`, `processing.py`,
   `chain_runner.py`, `main_ingestion.py` y `00_creacion_objetos_midas.py`. El workspace corre
   la versión anterior. (`notebooks/30_validacion_midas.py` es independiente a propósito.)
2. **Primera carga de solicitudes:** la Bronze existe, así que `insertInto` es **posicional**.
   El mapeo está validado contra el schema reportado, pero un tipo incompatible (p. ej.
   `fecha_solicitud` STRING vs DATE) dejaría el paso **FALLIDO no-fatal** — visible en
   `midas_log_cargas`, sin tumbar la cadena.
3. **Volumen de A3:** `consumos_contrato` multiplica las queries por ~4.6 SS/contrato. Está
   acotado a 6 meses; monitorear `filas_leidas`/duración en el log.
4. **GRANTs de los catálogos nuevos** (`confesco`, `obselect`, `calivaco`, `concepto`,
   `causcarg`, `mecacons`, `ps_package_type`, `pe_invest_cons_state`): si falta alguno, esa
   dimensión queda FALLIDA no-fatal.
5. **Destino de deploy:** los 3 targets usan `root_path: /Shared/bundles/${bundle.name}/${bundle.target}`.
   El path personal `/Workspace/Users/mpalomin.../midas_agent` visto en logs es una copia
   manual, no el target del bundle.

---

## 9. Próximos pasos

1. **Commitear** el trabajo (v1 + v2 + ajuste de adopción) y **desplegar** a dllo.
2. Correr el job y verificar en `midas_log_cargas`: **21 cargas**, dims pobladas,
   solicitudes cargando con el schema adoptado.
3. Confirmar `midas_parametros` sembrada (10 filas).
4. **Silver del Caso 2** (fuera del alcance de esta entrega): usar
   `activity_caso2 = 993` desde `midas_parametros` para filtrar; join de investigación por
   **SS + `id_periodo_consumo`**; comparación multi-servicio por **contrato**.

---

## 10. PENDIENTE-NEG (abierto)

| Tema | Estado |
|---|---|
| Mapeo de `package_type_id` (reclamo/reconexión/reinstalación/suspensión) | No se tocó; dim cargada cruda |
| Marca en `obselect` que distingue observaciones que **modifican** el consumo | Abierta |
| Regla de "SS vigente" del contrato (¿incluir retirados?) | Sin respuesta; default: se traen **todos** con fechas, vigencia en Silver |
| Calificación y conceptos "habitual/raro" | **Diferidos** a trabajo caso-por-caso con el científico de datos |

**Resueltos en este periodo:** semántica de `INVEST_CONS_STATE_ID`, formato de
`consumption_period`, confesco oficial, adopción de la Bronze de solicitudes, actividad del
Caso 2 (993) confirmada con datos.

---

## 11. Referencias

- `docs/semantica_campos_caso2.md` — semántica de negocio del Caso 2 (fuente de verdad)
- `docs/adr/0001-bundles-separados-vera-midas.md` — ADR de bundles separados
- `notebooks/30_validacion_midas.py` — validaciones read-only (F5/F2/F4/F1b)
- `README.md` — tablas de la cadena, dims y parámetros
