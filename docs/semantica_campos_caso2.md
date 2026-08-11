# Semántica de campos — Caso 2 (variación significativa de consumo)

> Fuente de verdad de la **semántica** del Caso 2. Sale de la reunión de negocio del
> 2026-07-16 (Jonatan Londoño — dueño del modelo Oracle; Luis "Lucho" Sánchez — analista;
> Lina Ángel — líder funcional). La **ingeniería de datos** trae los datos crudos a Bronze;
> la interpretación/umbrales que se listan aquí se cablean en **Silver / `midas_parametros`**,
> nunca en las queries de extracción (invariante I10 y política metadata-driven).

## 0. Decisión arquitectónica del caso (D1)
- El Caso 2 es la **actividad `activity_id = 993` — "993 - VARIACIÓN SIGNIFICATIVA CONTRA
  EL MES ANTERIOR"** (nombre oficial, confirmado por Jonatan). Sale de las mismas
  `or_order`/`or_order_activity` y llega a nivel de **servicio suscrito** (igual que el Caso 1).
- NO se crea una query Oracle nueva de entrada: la cadena ya trae TODAS las órdenes de calidad
  pendientes (`task_type_id = 883`, todas las actividades) y **la separación por caso
  (1019 vs 993) se hace aguas abajo** (Silver / registro de agentes), usando `midas_parametros`
  (`activity_caso1` / `activity_caso2`).
- Verificado en código: `QUERY_ORDENES_PENDIENTES` filtra solo por `task_type_id = 883`
  (sin `activity_id`).
- **CONFIRMADO CON DATOS (dllo, 21-22 jul 2026)** — `30_validacion_midas.py` celda F5:
  actividades presentes en `midas_ordenes_calidad_pendientes_c2_bronze` =
  **`993, 1019, 1013, 995, 996, 1012, 1120`**. Las órdenes del Caso 2 (993) **ya llegan a
  Bronze** con la cadena actual, junto con las del Caso 1 (1019) y otras casuísticas.
  → No se toca la query de entrada; el filtro por actividad vive aguas abajo.

## 0.1 Alcance y fronteras (contexto v2)
- **Alcance:** framework de gestión + agente **específicamente para el Caso 2 (993)**. La
  integración con el agente del Caso 1 (bundle legacy) es del equipo del **cliente**. Todo lo
  construido es genérico/escalable (control con `job_name`, `midas_parametros`, dimensiones)
  para que el cliente pueda integrar después. La cadena y queries del Caso 1 quedan **intactas**
  (invariante + frontera contractual).
- **Teradata (HIDRO/GDE/GASPAR) FUERA del framework de ingestión.** Se evalúa un **servidor
  MCP** para consulta bajo demanda del agente, en etapas posteriores. NO hay conectores,
  queries, filas de control ni docs de ingesta de Teradata en este bundle.

## 0.2 Consumo en investigación — semántica RESUELTA (era PENDIENTE en v1)
`PE_INVEST_CONSUM` se filtra por `product_id` (= servicio suscrito). `INVEST_CONS_STATE_ID`
**define si se le cobra o no al usuario**; el catálogo es `PE_INVEST_CONS_STATE`, resuelto
**inline** en la query (columna `estado_investigacion_desc`, junto al código crudo):

| Valor | Significado |
|---|---|
| 1 | EN INVESTIGACIÓN (abierta) |
| 2 | IMPUTABLE AL CLIENTE |
| 3 | IMPUTABLE A LA EMPRESA |

- "Tiene consumo en investigación (abierta)" = estado **1**. Los estados **2/3 son
  resoluciones** (a quién se imputa), **no** "cerrada/terminal".
- La Bronze `midas_datos_investigacion_consumo_bronze` trae el estado **crudo**; el filtro por
  estado se hace en Silver.
- **Periodo — CONFIRMADO (dllo 21-22 jul 2026, celda F4):** `consumption_period` **equivale a
  `id_periodo_consumo`** (21 valores en común sobre los mismos SS; **0** coincidencias con
  `id_periodo_facturacion`). → El join de investigación es por **SS + `id_periodo_consumo`**,
  nunca por `id_periodo_facturacion`. Referencia de formato: 10 dígitos (`1210131820`) vs
  `id_periodo_facturacion` de 3–4 dígitos (`938`, `1077`)
  (`notebooks/30_validacion_midas.py`, celda F4).

## 0.3 Racionalización de dimensiones (v3) — por qué UNA sola
El Caso 1 nunca materializó catálogos: los resuelve **inline** en las queries de extracción con
subconsultas correlacionadas (`(SELECT escocodi||'-'||escodesc FROM estacort WHERE escocodi = sesuesco)`),
por lo que las Bronze de datos **ya traen código y descripción juntos**. Materializar catálogos
de `tipo_consumo`, `observacion_lectura`, `calificacion`, `concepto`, `causal_cargo`,
`metodo_calculo`, `tipo_solicitud` y `estado_investigacion` era **redundante** y habría que
operarlos para siempre → **se podaron antes del commit** (nunca se desplegaron).

**Única dimensión que se queda: `midas_dim_estado_corte_facturable_bronze`.** No es una
etiqueta, es una **matriz de decisión** (S/N por estado × servicio, desde `confesco`): el agente
la consulta como **regla** y necesita las combinaciones **completas**, no solo las presentes en
los datos del día; y negocio pidió explícitamente que fuera dinámica.

Resultado: control de **21 → 13 cargas** (1 dim + 8 cadena + 4 promociones). `midas_parametros`
no cambia. Las 4 queries nuevas resuelven inline todos sus códigos categóricos.

> **Upgrade path** (no implementar hoy): si el DS necesitara enumerar catálogos completos para
> prompts/tools → **una** tabla genérica `midas_dim_catalogos_bronze (catalogo, codigo,
> descripcion)` con `UNION ALL`, no volver a nueve tablas.
> Mientras tanto, los catálogos completos se consultan **one-off** (read-only) en
> `notebooks/30_validacion_midas.py` (celdas F1b `pe_invest_cons_state`, F1c `ps_package_type`).

## 1. Estado de corte facturable  (CERRADO)
- Facturable NO es una lista plana: es una relación **(estado_corte × servicio)** en la tabla
  `confesco`, columna `coecfact` (S/N).
- Se carga **dinámico** a la dimensión `midas_dim_estado_corte_facturable_bronze` con el query
  entregado por Jonatan (`QUERY_DIM_ESTADO_CORTE_FACTURABLE`). NO se queman códigos.
- Servicios: 101 AGUA POTABLE, 103 ALCANTARILLADO, 501 GAS NATURAL REGULADO, 701 ENERGÍA.
- Ejemplos **S** (facturable): 1-Conexión, 4-Orden suspensión total, 5-Suspensión total,
  6-Orden de reconexión, 91, 93, 94-Inicio retiro voluntario, 97, 99-Convenio de pago, 100,
  107, 122.
- Ejemplos **N** (no facturable): 92-Retiro definitivo, 95-Retiro voluntario, 101-Retiro por
  cancelación deuda, 110-Retirado sin instalación, 111, 112, 113-Con siniestro aprobado, 970,
  96 (solo energía).
- Notas de negocio: el 94 dura ~3 meses esperando cobros remanentes y luego pasa a 95 (ya no
  factura). El 113 es catástrofe/siniestro.

## 2. Observación de lectura  (CERRADO en semántica; marca en `obselect` PENDIENTE)
- `0` = sin novedad (lector vio el consumo dentro de límites; no critica).
- `30` = desviación significativa (lector confirmó la lectura y reportó la causa).
- `23` / vacía = instalación vacía → el sistema **no cobra**.
- `2` = imposibilidad de acceso → cobra **promedio** ese mes y **recalcula** al mes siguiente.
- `25` = medidor parado → un analista cobra promedio y remite al negocio.
- `9` = servicio directo (sin medidor) → se calcula por otras vías; el promedio no se modifica.
- `7` (display desenergizado) vs `5` (destruido/dañado): los lectores los **confunden**. El 7
  real = suspendido por no pago → **no se cobra** (se desambigua con estado_corte suspendido).
  El 5 real = **sí se cobra**.
- La observación 2 (segundo slot) es complemento (perro bravo, habitada/vacía, etc.).
- **Conclusión estructural:** hay observaciones que MODIFICAN el consumo cobrado y otras que no.
  → **PENDIENTE-NEG (A4 de modelo):** la marca exacta en `obselect` que las distingue sigue
  abierta. El código+descripción de la observación ya llega inline en la Bronze de lecturas;
  el flag se define luego.

## 3. Conceptos y causales  (CERRADO)
- **Conceptos habituales:** 692 pago (anula con signo crédito), 90 consumo energía, 87 consumo
  gas/acueducto/alcantarillado, 39 cargo fijo, 701+ intereses de mora, subsidios estratos 1-3,
  contribuciones 5-6, 614 redondeo de decimales, 530 alumbrado.
- **No habituales:** saldos a favor (703 generación, 725 aplicación).
- Hay ~1342 conceptos: imposible clasificarlos todos; la clasificación útil es la del TOP + la
  regla por causal.
- **Insight de Lina (orienta Silver, no Bronze):** el análisis central es aislar la causal
  **`-1` (consumo normal)**; las demás causales (74-PNO/fraude con recuperación de hasta 5
  meses, 30-paso a diferido, 73-abono a diferido, 25-reclamos, etc.) son el **argumento** que
  justifica la variación. **PNO real** se ve por **causal 74 + programa 307**, NO por el campo
  `pno` de lecturas (99.5% nulo, confirmado en Fase 1).
- Concepto y causal ya llegan como código-descripción inline en la Bronze de cargos; la marca
  "habitual/esperado" se mantiene como parámetro de negocio.

## 4. Análisis a nivel de contrato  (CERRADO)
- El análisis se hace **a nivel de contrato**: se comparan los "hermanitos" (agua/energía/gas)
  del mismo contrato. Si los tres bajan → casa desocupada; si solo uno → anomalía.
- Habilitado en Bronze por `midas_datos_servicios_contrato_bronze` (roster) +
  `midas_datos_consumos_contrato_bronze` (consumos de cada SS del contrato).
- **Roster = espejo de `QUERY_DATOS_BASICOS`** (indicación de Jonatan): `QUERY_SERVICIOS_CONTRATO`
  usa los mismos joins/catálogos/alias que datos_básicos, cambiando el filtro de instalación por
  `sesususc = :p_contrato`. Así el schema del roster es consistente con `datos_basicos_producto`.

## 4.1 Taller de semántica: resto DIFERIDO deliberadamente
La 1ª sesión entregó estado_corte/confesco (§1) y la investigación (§0.2). El **resto**
(calificación, conceptos, observaciones relevantes) se construirá **caso por caso junto al
científico de datos** al armar el árbol de decisión. Consecuencia: las dimensiones se cargan con
los **códigos crudos** y **ninguna carga Bronze queda bloqueada** esperando semántica.

## 5. PENDIENTE-NEG (para el trabajo caso-por-caso con el DS / próximos correos)
| Tema | Estado | Nota |
|---|---|---|
| **Calificación** (normal/anomalía/proceso) | Diferido (caso-por-caso con DS) | Bronze carga `calivaco` cruda |
| **Conceptos "habitual/raro"** | Diferido (caso-por-caso con DS) | Bronze carga `concepto`/`causcarg` crudos |
| **`package_type_id`** (reclamo/reconexión/reinstalación/suspensión) | No se tocó | Bronze carga `ps_package_type` cruda; el mapeo va a parámetros |
| **Marca en `obselect`** (¿modifica consumo?) | Abierta | Define si se expone un flag `obs_modifica_consumo` |
| **Regla "SS vigente"** del contrato (¿incluir retirados?) | No se tocó | Default técnico: se traen TODOS (incl. retirados) con fechas; vigencia en Silver |

> **Resuelto (ya no PENDIENTE), validado en dllo 21-22 jul 2026:**
> - Semántica de `INVEST_CONS_STATE_ID` (§0.2) y catálogo `PE_INVEST_CONS_STATE`
>   (3 estados; columnas `invest_cons_state_id`+`description` confirmadas — celda F1b).
> - **Formato de `consumption_period` = `id_periodo_consumo`** (celda F4).
> - Actividades en Bronze: 993 y 1019 presentes (celda F5).
> - confesco oficial (§1).
> - **Bronze de solicitudes: ADOPTADA** (negocio: "se va a usar"). Ver §7.

## 7. Bronze de solicitudes — decisión de adopción (validada en dllo 21-22 jul 2026)

> **SUPERADA por el fork `c2` (2026-08-11).** La adopción se revirtió: este bundle dejó de
> escribir el objeto compartido y construye el suyo, `midas_datos_detalle_solicitudes_c2_bronze`.
> El motivo es que la convivencia tuvo un costo real — dos escritores sobre el mismo objeto, y un
> `CREATE OR REPLACE` ajeno que dejó R5 en cero sobre 3.152 filas con los datos sanos en la tabla.
> El análisis de abajo se conserva porque explica el mapeo de columnas, que sigue vigente.

La tabla original ya existía (huérfana). Su schema **no es
incompatible de fondo**: es el mismo dato de `QUERY_DETALLE_SOLICITUDES` con **nombres en
español** y una columna extra al inicio, `servicio_suscrito`, que **la query no devuelve**
(es el bind `:p_servicio_suscrito`).

**Decisión: ADOPTAR la tabla existente** (no recrear), porque negocio confirmó que se usa y
porque recrearla tal cual la query dejaría la Bronze **sin `servicio_suscrito`**, que es
justamente la `columna_join` declarada en el control.

Implementación (capa Python, **sin modificar el SQL**): `processing.run_query_detalle_solicitudes`
inserta `servicio_suscrito` y `processing._adaptar_solicitudes_a_bronze` renombra/reordena al
schema posicional de la Bronze (`insertInto` es posicional):

| Bronze (posición) | Origen en la query |
|---|---|
| `servicio_suscrito` | bind `:p_servicio_suscrito` (materializado) |
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

PK del control/ingesta: `[servicio_suscrito, id_solicitud]`.
> Nota de negocio: para el Caso 1, el campo `comentario` requerirá interpretación por LLM —
> eso es **capa agente**, fuera del alcance de ingeniería de datos.

## 6. Códigos "mágicos" (ahora en `midas_parametros`)
Sembrados por `notebooks/00_creacion_objetos_midas.py` (dominio.clave = valor):
`orden.task_type_ordenes_calidad=883`, `orden.activity_caso1=1019`,
`orden.activity_caso2=993`, `orden.activity_critica=102010`,
`orden.activity_decision_analista=7400027`, `orden.estado_orden_anulada=12`,
`comentario.tipo_comentario=4002`, `consumo.metodo_calculo_facturado=4`,
`ventana.ventana_meses_historia=6`, `ventana.ventana_meses_observaciones=3`.
> Aún NO los consumen las queries existentes (refactor aparte): la separación de casos se hace
> aguas abajo leyendo esta tabla.

---

# V3 — Reunión de reglas de negocio del 2026-07-28

Participantes: Jonatan Londoño (dueño del modelo Oracle), Luis Eduardo Sánchez (analista),
Lina Ángel (líder funcional).

## V3.0 Estado de la implementación

| Regla | Qué es | Estado |
|---|---|---|
| **R1** | Eliminar la dimensión de facturable; el join va inline en `QUERY_DATOS_BASICOS` | ✅ **Implementada** |
| **R2** | Eliminar `servicios_contrato`; recablear A3 sobre `datos_basicos` | ✅ **Implementada** |
| **R3** | Traducción de periodos por join contra `pericose` / `perifact` | ✅ **Implementada** |
| **R4** | Nueva Bronze de Pérdidas No Operacionales | ✅ **Implementada** (pendiente verificar el mapeo del SS) |

**Resultado: 12 cargas activas**, todas `FULL_CHAINED`. Objetivo de la v3 alcanzado.

### R2 — la verificación tardó tres pasos en interpretarse bien

El `LEFT ANTI JOIN` obligatorio (§3.2) exigía 0 filas y devolvió **782**. La primera lectura
fue que la premisa era falsa y R2 quedó bloqueada. Las verificaciones de seguimiento
cambiaron la conclusión:

| Verificación | Resultado |
|---|---|
| SS huérfanos | 782 |
| …con un hermano de contrato que sí está en `datos_basicos` | **0 de 782** |
| …que aparecen en `ordenes_pendientes` | **0 de 782** |
| Contratos implicados | **181**, ninguno en `ordenes_pendientes` ni en `datos_basicos` |

Esos 782 no eran servicios hermanos legítimos: eran **residuo de corridas anteriores**.

El mecanismo es concreto y estaba a la vista en el código: `run_query_servicios_contrato`
itera los contratos **de `datos_basicos` de la misma corrida**, así que en un run consistente
todo contrato del roster tiene que estar en básicos. Que 181 contratos no lo estuvieran solo
puede significar que las dos tablas venían de corridas distintas. Y A2 se ejecutaba con
`abortar_en_fallo=False`: si fallaba, `datos_basicos` se sobrescribía fresco mientras el
roster conservaba los datos viejos.

**Retirar la tabla elimina esa clase de inconsistencia de raíz**, además de cumplir I12.

### Cómo se obtiene ahora el roster

```sql
SELECT * FROM midas_datos_basicos_producto_c2_bronze
 WHERE contrato = (SELECT contrato FROM midas_datos_basicos_producto_c2_bronze
                    WHERE servicio_suscrito = :ss_de_la_orden)
```

Y A3 (`run_query_consumos_contrato`) recibe ahora `(df_datos_basicos, df_ordenes_pendientes)`
e itera los SS de los **contratos de las órdenes**. El filtro por contrato es deliberado: la
instalación es un superconjunto que traería SS de contratos ajenos (otros clientes del mismo
predio), inflando el volumen y contaminando el análisis multi-servicio del Caso 13.

> **Vigilar en la primera corrida:** el log de A3 reporta `N SS por contrato vs M SS por
> instalación`. Una caída fuerte de filas en `consumos_contrato` respecto al valor previo
> significaría que el filtro quedó mal acotado.

### Verificaciones ejecutadas el 2026-07-29

| Verificación | Esperado | Obtenido | Efecto |
|---|---|---|---|
| Duplicados `(coeccodi, coecserv)` en `confesco` | 0 | **0** | R1 desbloqueada: la subconsulta escalar no puede dar `ORA-01427` |
| `LEFT ANTI JOIN` roster vs. básicos | 0 | 782 → **residuo** | R2 implementada |
| Total de registros en `fm_possible_ntl` | > 0 | **151.173** | La tabla tiene volumen real |
| …con `normalized_prod_id` válido en `servsusc` | — | **27.459 (18%)**, 24.090 SS distintos | El mapeo SS **es correcto**; ver §V3.2 |

## V3.1 R3 — Los dos periodos

| Concepto | Código | Tabla maestra | Qué significa |
|---|---|---|---|
| **Periodo de consumo** | 10 dígitos (`PECSCONS`) | `pericose` | Ventana en que el cliente **consumió**. `PECSFECI`…`PECSFECF` son los días entre lectura anterior y actual |
| **Periodo de facturación** | 4 dígitos (`PEFACODI`) | `perifact` | Ventana en que EPM **cobra** ese consumo. Trae año, mes y ciclo |

Los ciclos existen porque no se puede leer a todos los clientes el mismo día: el periodo de
consumo **se desplaza** y no coincide con el mes calendario. Un mismo periodo de facturación
puede contener cargos de **varios** periodos de consumo — ese es el mecanismo de las
recuperaciones (§V3.4).

**Forma del join:** subconsulta escalar en todos los casos, nunca inner join. Un maestro
faltante devuelve `NULL` y **no** puede hacer desaparecer filas de consumo o de cargos.

### Columnas agregadas (todas AL FINAL, invariante I13)

| Tabla | Columnas nuevas |
|---|---|
| `midas_datos_lecturas_producto_c2_bronze` | `anio_facturacion`, `mes_facturacion`, `ciclo_facturacion` |
| `midas_datos_consumos_producto_c2_bronze` | `fecha_ini_consumo`, `fecha_fin_consumo` |
| `midas_datos_ordenes_previa_critica_c2_bronze` | `fecha_ini_consumo`, `fecha_fin_consumo` |
| `midas_datos_cuentas_cobro_c2_bronze` | `id_periodo_consumo`, `fecha_ini_consumo`, `fecha_fin_consumo` |
| `midas_datos_detalle_cargos_c2_bronze` | `fecha_ini_consumo`, `fecha_fin_consumo`, `anio_facturacion`, `mes_facturacion` |
| `midas_datos_consumos_contrato_bronze` | `fecha_ini_consumo`, `fecha_fin_consumo`, `anio_facturacion`, `mes_facturacion` |
| `midas_datos_investigacion_consumo_bronze` | `fecha_ini_consumo`, `fecha_fin_consumo` |

**Decisión sobre `cuentas_cobro`.** El prompt suponía que no aplicaba porque "una cuenta
agrupa varios periodos de consumo". La CTE de la propia query lo contradice: fija **un**
`pecscons` por `pefacodi` (`pecscons = pefapecs`), y `valor_periodo` / `valor_recuperado`
se calculan comparando `cargpeco` contra ese valor. Sin exponerlo, esas dos columnas que ya
se entregan son inauditables. Se expone: es aditivo y sale del CTE a coste cero.

**Decisión sobre `detalle_cargos`.** El prompt pedía derivar `anio_facturacion` /
`mes_facturacion` desde `cargos.CARGPEFA`. Existen **dos rutas** al periodo de facturación de
un cargo: `CARGPEFA` (el periodo propio del cargo) y `factura`+`cuencobr` (la que ya usaba la
columna `id_periodo_facturacion`). **Negocio decidió mantener la ruta ya implementada**, la de
`factura`+`cuencobr`, porque está en producción y funciona. Consecuencia: `anio/mes_facturacion`
son **siempre consistentes** con `id_periodo_facturacion` de la misma fila.

> **PENDIENTE-NEG:** queda sin explorar si `CARGPEFA` difiere de esa ruta en los cargos de
> recuperación. Si difiriera, sería una señal adicional — pero hoy no se extrae.

**Watch-item del formato de `consumption_period`.** Si `fecha_ini_consumo` sale `NULL` en
todas las filas de investigación, `consumption_period` no es un `PECSCONS`. Umbral esperado
de resolución: **> 95%**. Por debajo → reportar, no corregir a ciegas.

## V3.2 R4 — Pérdidas No Operacionales

`midas_datos_perdidas_no_operacionales_bronze`, orden 25, `FULL_CHAINED` por
`servicio_suscrito`, PK `id_pno`. Espeja el driver de A1 (solicitudes).

**Complementa, no reemplaza** la señal de PNO que ya existe en cargos (`causal = 74` +
`programa = 307`): cargos **detecta** que hubo una PNO; esta tabla **explica** cuál fue la
irregularidad y en qué ventana.

**`normalized_prod_id` = servicio suscrito: CONFIRMADO.** El muestreo inicial (0 de 6 SS con
expediente) no era concluyente — una PNO es un expediente de fraude y es raro por naturaleza.
La prueba por dominio de valores lo resolvió: de 151.173 registros, **27.459 tienen un
`normalized_prod_id` que existe en `servsusc`** (24.090 SS distintos). Si la columna fuera otro
identificador, la coincidencia habría sido ~0. El join es correcto.

> El 82% restante referencia servicios que ya no están en `servsusc` (retirados o migrados).
> Es esperable en un histórico de fraude y **no invalida** el join: solo significa que la
> Bronze traerá expedientes únicamente de los SS vivos que entren por la cadena.

Verificaciones pendientes:


1. `status` — averiguar si tiene catálogo; si lo tiene, resolver inline (I11).
2. `comment_` — probable CLOB; confirmar que `database.py` lo convierte a `str` (I10).
3. Volumen — filas totales y SS distintos; evaluar ventana temporal **solo con confirmación**.
4. GRANT sobre el submodelo `FM_*` — si falta, el paso falla sin tumbar la cadena.

### Fuentes de PNO conocidas y NO extraídas

Jonatan entregó tres queries; solo se implementó la de la rejilla principal. Las otras dos
quedan documentadas para no redescubrirlas:

- **Actas de recuperación** — `epm_actareco` / `audit_epm_actareco`, cruzadas por
  `acrenuse` (número de servicio).
- **Consumos de pérdida** — `fm_preinvoice_pno`.

## V3.3 Métodos de consumo y agregación

`COSSMECC = 4` (consumo facturado) es **el único que se cobra**. Los demás (`1` medido,
`2` corregido, `3` estimado…) son información sobre el **origen** del consumo.

`COSSCOCA` **se agrega**: el consumo de un periodo puede venir en varias filas y hay que
sumarlo. Caso típico: cambio de medidor dentro del periodo, una fila por `COSSELME`.

> **Riesgo de pipeline:** cualquier deduplicación por `(servicio_suscrito, id_periodo_consumo)`
> sin `tipo_consumo` ni `medidor` en la llave rompe esto — y además hace desaparecer la
> energía reactiva.

## V3.4 Recuperaciones: el patrón está en `CARGDOSO`

Una recuperación es un cargo del **mismo periodo de facturación** con **periodo de consumo
distinto**, y su documento soporte trae el token `PR`:

```
CO-PR-202606-TC-0007   <- recuperación
CO-202606-TC-0007      <- cargo normal
```

Las unidades recuperadas están en `CARGUNID`. Con R3 esto queda computable en Silver.

## V3.5 Unidades: desde cargos, no desde consumos

Jonatan (29:09) pidió tomar las unidades de cargos, porque es lo que realmente se cobró.
`CONSSESU` puede quedar desactualizado si hubo ajuste con recuperación al mes siguiente.

## V3.6 `CARGPROG` (programa)

`5 = FGCA` es el proceso normal de facturación (tarifa × unidades + subsidios y
contribuciones). **Cualquier otro programa** es un cargo inyectado por otra funcionalidad
(PNO, Fénix…). Es la base de la regla dura del Caso 17, "otros cobros".

## V3.7 `COSSFUFA` (función de cálculo)

Indica si el consumo salió por **diferencia de lecturas** o por **promedio de los últimos 6
meses**. Información adicional, no criterio de decisión.

## V3.8 Fecha de retiro comodín

`SESUFERE = 31/12/4732` significa **servicio activo** (comodín de Open). No es un dato real.
Relevante para el filtro de "SS histórico", que sí detiene el análisis.

## V3.9 Órdenes de decisión de analista

Se obtienen de `midas_datos_ordenes_previa_critica_c2_bronze` filtrando por **servicio suscrito +
tipo de trabajo + actividad**. Jonatan corrigió en vivo que **la llave de esa tabla es el
servicio suscrito**, no el `order_id`. Ya está en `midas_parametros` como
`activity_decision_analista = 7400027`.

> Nota de contraste con lo verificado en `dllo`: hoy **no llega ninguna orden 7400027** a esa
> Bronze (0 de 287 filas). `QUERY_ORDENES_CRITICA_PEVIA` limita la primera rama a
> `activity_id = 102010` y su lista de `task_type_id` incluye `10037` pero no `10038`. El
> filtro descrito arriba solo funcionará cuando esa brecha de extracción se cierre.

## V3.10 Órdenes de calidad pendientes: estado y comentario no son informativos

Siempre están abiertas y sin comentario, por definición (aún no se han cerrado). **No
construir features sobre esos dos campos.**

## V3.11 PENDIENTE-NEG: `COSSFLLI`

En los screenshots aparece una columna `COSSFLLI` con valores `S`/`N` que **distingue las dos
filas del mismo periodo** y no está en el diccionario del proyecto. Hipótesis: marca de "línea
facturada". Si se confirma, podría ser un filtro más limpio que `COSSMECC = 4`.

**No implementar nada sobre esta columna.** No viene hoy en
`midas_datos_consumos_producto_c2_bronze` (13 columnas, verificado).

## V3.12 Parámetro nuevo

`ventana_periodos_analisis = 8` en `midas_parametros`. MIDAS v1 mira 8 periodos hacia atrás y
el analista usa ~6; Jonatan pidió explícitamente que fuera parametrizable.

> Sigue vigente la deuda: `midas_parametros` está sembrada pero **ninguna query la consume
> todavía**; los códigos y ventanas siguen cableados.


---

# Orden de decisión del analista (7400027) — el ground truth del agente

**No hace falta ninguna tabla nueva.** Es la **misma pantalla** "Órdenes de Crítica y Previa"
que ya extrae `QUERY_ORDENES_CRITICA_PEVIA`, con las mismas columnas (`ORDEN`, `PER. CONS.`,
`TIPO TRABAJO`, `ACTIVIDAD`, `FECHA CREACION`, `FECHA LEGALIZACION`, `ESTADO`,
`QUIEN LEGALIZA`) y su panel de comentarios. Lo único que pasaba es que el filtro
`activity_id = 102010` de la rama 1 dejaba esas filas afuera.

## Los tres hechos verificados en Oracle (2026-07-29)

| Verificación | Resultado |
|---|---|
| Combinaciones `activity_id` × `task_type_id` | **una sola**: `7400027` va siempre con `10038` |
| Volumen | **106.545 órdenes** sobre **70.554 SS**, entre 2025-05-15 y 2026-07-10 |
| Órdenes con `102010` y `7400027` a la vez | **0** |

El tercero explica por qué nunca aparecían: son órdenes Oracle distintas, así que el filtro
de la rama 1 las excluía por completo.

## La solución: RAMA 4 del UNION

Se agrega una cuarta rama a `QUERY_ORDENES_CRITICA_PEVIA`. Las filas caen en
`midas_datos_ordenes_previa_critica_c2_bronze` — **mismo schema, 12 columnas, sin cambios** — y
sus comentarios fluyen solos hacia `midas_datos_cometarios_ordenes_c2_bronze` por la cadena
existente, porque `QUERY_COMENTARIOS_ORDENES` **no filtra por tipo de comentario**: su primera
rama es `WHERE oc.order_id = :p_id_orden` y trae todos los tipos.

### Tres decisiones de implementación

**No se engancha a `PE_INVEST_CONSUM`.** Las ramas 2 y 3 unen por
`oa.package_id = p.investigate_request`. Usar esa vía supondría que toda decisión cuelga de
una solicitud de investigación, cosa **no verificada**. La rama 4 ataca directo por
`or_order_activity.product_id`.

**Ventana `o.created_date BETWEEN pefafimo AND pefaffmo`.** No es decorativa:
`run_query_ordenes_critica_previa` itera ~8 combinaciones de periodo por SS, y sin la ventana
la misma orden se repetiría en cada una. Mismo patrón que ya usa la rama 3 con
`p.register_date`. **Efecto lateral asumido:** solo llegan las decisiones creadas dentro de
los periodos de facturación analizados.

**`tipo_consumo` va NULL.** La orden de decisión no expone uno propio. Bronze es réplica
fiel: no se fabrica el valor de la iteración en curso.

### El NULL obligó a blindar el paso de comentarios

`run_query_comentarios_ordenes` hacía `row.TIPO_CONSUMO.split('-')[0]`. Con `TIPO_CONSUMO`
NULL eso lanza `AttributeError`, y como ese paso corre con `abortar_en_fallo=True`
**habría tumbado la cadena entera del Caso 1**. Se agregó la guarda correspondiente.

## Consecuencia asumida: los conteos del Caso 1 cambian

Es la **única excepción consciente** a la invariante I13 y al criterio de "cero filas de
diferencia". Afecta a dos tablas:

- `midas_datos_ordenes_previa_critica_c2_bronze` — suma las filas de decisión.
- `midas_datos_cometarios_ordenes_c2_bronze` — suma sus comentarios.

Las otras seis tablas del Caso 1 deben seguir idénticas. Las filas nuevas son precisamente lo
que se buscaba, así que el aumento es la señal de éxito, no un defecto — pero hay que
verificarlo tabla por tabla, no aceptarlo en bloque.

## Los códigos 4048 / 4049 / 4050

Se observaron en `or_order_comment.comment_type_id` de las órdenes `7400027`
(`CONSUMO IMPUTABLE AL CLIENTE`, `NO IMPUTABLE AL CLIENTE`, `CREAR ORDEN DE CALIBRACIÓN`,
más `1111 GENERAL`). **No se implementó nada sobre ellos.** Cuando la rama 4 esté desplegada
y las órdenes empiecen a llegar, se confirma con datos reales qué tipos traen. `PENDIENTE-NEG`
hasta entonces.

## Total de cargas: 12

Sin cambios. El arreglo es una rama más en una tabla que ya existe.
