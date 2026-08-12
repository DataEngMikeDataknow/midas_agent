# Handoff — MIDAS `midas_data_platform` · Fork `c2` y gobierno de schema
_Generado: 2026-08-11 (hora local del usuario, America/Bogota)_

---

## 1. Objetivo

Plataforma de datos de EPM (Empresas Públicas de Medellín) que alimenta a un **agente
automatizado** encargado del **Caso 2: actividad 993 — Variación Significativa Contra el Mes
Anterior**.

El proceso de negocio real: un usuario reclama que le llegó una factura muy alta y se abre una
orden 993. Hoy un analista entra al sistema comercial Oracle FLEX y revisa a mano el histórico
de consumo, las lecturas del medidor, los cargos de la factura, los trámites del cliente y las
órdenes previas, para decidir si el cobro fue correcto o si hay que ajustar. El objetivo es que
el agente haga ese recorrido con los mismos datos, estructurados y con su significado explícito.

El flujo es `Oracle FLEX → Parquet (Volume UC) → Bronze (Delta) → Silver (Delta)`, orquestado
por un plano de control metadata-driven en `midas_control_cargas` / `midas_log_cargas`,
discriminados por `job_name`.

**Frontera que define todo el diseño:** Silver publica **hechos**, el agente emite **juicios**.
No existe ninguna columna tipo `cierre_sugerido` o `requiere_ajuste`. La razón es doble: el
veredicto es del agente, y estas tablas son el patrón de medida con el que se le evalúa — el
evaluador no puede ser parte de lo evaluado.

**Repositorio activo:** `C:\Users\migue\Desktop\ProyectoMidas\midas_agent`, rama
`feature/midas_data_platform`. El repo de Vera en
`C:\Users\migue\Desktop\IngestionDatabricksBundleEPM-main` es **estrictamente de solo lectura**.

**Giro de esta sesión:** dejamos de compartir 14 objetos (9 Bronze + 5 Silver) con el equipo del
Caso 1 — construimos nuestra propia copia `_c2_` de cada uno — porque un `CREATE OR REPLACE
TABLE` ajeno sobre un objeto compartido dejó una feature entera (R5) en cero durante días sin que
nada fallara. Ese fork destapó un segundo problema más profundo: `crear_objetos` nunca gobernaba
el schema de Bronze, y eso dejó pasar un `UNRESOLVED_COLUMN` en producción. Ambos quedan resueltos
en el estado actual de este handoff, con una excepción sin commitear (ver §2).

---

## 2. Estado Actual

### Qué funciona

- **El fork `c2` corrió de punta a punta en `dllo` y quedó validado.** [Validado] — corrida
  `82b6740a…` (2026-08-11, 20:38–20:39), 13 objetos Silver EXITOSO, 0 FALLIDO. Los 15 `FALLIDO`
  que aparecían en la bitácora del día pertenecen a 3 corridas *anteriores* durante la propia
  transición del `UNRESOLVED_COLUMN` (confirmado por el usuario, desglose por `run_id`).
- **V3b — el criterio de aceptación real del fork — da 0 objetos.** [Validado] — ningún objeto
  del Caso 1 recibió una escritura nuestra en la corrida buena. Este chequeo es nuevo en esta
  sesión y es el único que mide directamente "dejamos de pisar al Caso 1"; nada más lo hacía.
- **R5 (reconexión/suspensión) reprodujo los números exactos del 2026-08-03.** [Validado] — 175
  con reconexión, 201 con suspensión, 640 con `fecha_ultima_reconexion` poblada. Era el pendiente
  más viejo de esta línea de trabajo: la causa raíz terminó siendo un problema de **orden de
  ejecución** en el seed de Silver (`detalle_solicitudes` en orden 53, DIEZ posiciones después de
  `features_consumo` en 43, que la lee), agravado por un segundo escritor externo que hacía
  `CREATE OR REPLACE` sobre la tabla compartida. El fork elimina ambas causas de raíz.
- **El concepto `545` entró**: `conceptos_consumo_medido = 87,90,545,546`. [Validado] —
  confirmado en `midas_parametros`. El `+29%` esperado en `unidades_consumo_cobradas` **no se
  puede medir limpio en esta corrida**: el total subió solo ~10% (3.415.873 → 3.757.461), pero
  esta corrida movió el parámetro y una extracción fresca de Oracle a la vez, así que la
  magnitud no es comparable contra la medición original.
- **Los Parquet de la cadena Caso 1 estaban STALE, y ya no lo están.** [Validado] — el volcado de
  schema del 2026-08-11 mostró que 6 de las 8 tablas Bronze de la cadena no traían las columnas
  que sus propias queries de Oracle proyectan desde v3 (`estado_corte_facturable`,
  `anio_facturacion`, `fecha_ini_consumo`…). No era un escritor externo: era una extracción vieja.
  Corregido reconstruyendo cada DDL como `columnas del Parquet + cola declarada en
  _MIGRACION_V3`, verificado a mano contra las queries de Oracle, y confirmado en `dllo` por
  V2/V2.1 del notebook `31` sin un solo desvío.
- **`crear_objetos` ahora gobierna el schema de Bronze.** [Validado] — 12 DDL en
  `src/midas/sql/bronze/ddl/`, ejecutados como primer bloque de `crear_objetos`. `ingestion.py`
  ya NO crea tablas: falla explícitamente si una no existe, y compara nombre **y orden** de
  columnas antes de cada `insertInto` (que es posicional y antes no lo verificaba nadie).
- **Órdenes `7400027` (ground truth del analista) llegan con datos.** [Validado] — 26 en `dllo`,
  con sus 26 comentarios en `datos_cometarios_ordenes`. Esto **resuelve** el `[Supuesto]` más
  viejo del handoff anterior y **hace innecesario ejecutar
  `notebooks/35_verificacion_oracle_7400027.py`**: su única pregunta era justo esta.
- **145 tests pasan.** [Validado] — con 1 fallo preexistente (`test_ensure_schema_exists`) que
  por acuerdo explícito **no se toca**; sigue deseleccionado en `pipeline/deploy-bundle.yml`.
- **La convivencia de dos escritores sobre `detalle_solicitudes` está resuelta**, no solo
  "identificada". [Validado] — V3 del notebook `34` da 1 sola identidad
  (`mpalomin@contratista.epm.co`) en la corrida buena. La pregunta pendiente del handoff anterior
  ("quién es el segundo escritor") queda sin objeto: el fork eliminó la tabla compartida.

### Qué falta / está incompleto

- **Sin commitear: la limpieza de 7 PRIMARY KEY que los datos contradecían.** Es lo más urgente
  de este handoff. El bloque V8 del notebook `31` midió unicidad real contra las 12 PK
  declaradas en los DDL nuevos y **7 eran falsas** (p.ej. `lecturas_producto_c2`: PK declarada
  `servicio_suscrito`, pero 3.180 filas para solo 331 valores distintos). Ya está: DDL corregido
  con el grano real documentado, `ALTER TABLE ... DROP CONSTRAINT` agregado a `crear_objetos`
  (imprescindible: `CREATE TABLE IF NOT EXISTS` no toca una tabla que ya existe, y esas 7 ya se
  crearon CON la constraint en la corrida buena), notebook `31` realineado (V8 ahora exige
  unicidad real de las 5 que sí la tienen y sube a `FALLA`; V8b nueva mide, sin exigir, las 7 sin
  PK), y un test que impide que el DDL y el `ALTER` se vuelvan a separar. **Todo verificado
  localmente y con tests en verde, pero NUNCA se corrió contra `dllo`** — falta ejecutar
  `crear_objetos` una vez más para que el `DROP CONSTRAINT` aterrice.
- **`docs/contrato_silver.md` sigue desactualizado.** Sigue diciendo "44 columnas" en
  `features_consumo` (hoy 49), y no refleja ninguno de los cambios del fork `c2` en la sección de
  Nivel 1 más allá de lo ya corregido puntualmente (ver §3). Es deuda heredada del handoff
  anterior, no tocada en esta sesión salvo los fragmentos específicos sobre la tabla adoptada.
- **Comentarios de columna perdidos en las 4 copias heredadas del Caso 1.** No estaba previsto:
  un `CREATE OR REPLACE TABLE AS SELECT` **sí propaga** los `COMMENT` de la tabla fuente, así
  que cada copia `_c2_silver` heredó solo los comentarios que trae nuestro DDL de Bronze — no
  los que el equipo del Caso 1 había puesto sobre sus propias tablas Silver originales.
  Resultado medido: `basicos_producto` 2/23, `ordenes_calidad_pendientes` 3/25,
  `historial_critica` 4/13, `historial_facturacion` 0/23. Se arregla enriqueciendo los `COMMENT`
  del DDL de Bronze (se propagan solos); requiere que negocio o el diccionario confirmen el
  significado de las columnas hoy sin comentar.
- **`cargos=1, features=0` para el concepto `899`.** Persiste sin investigar a fondo desde hace
  varias corridas. Hipótesis sin confirmar: esa única línea tiene `id_periodo_consumo` nulo y la
  filtra `midas_features_consumo_silver.sql:108`.
- **7 tablas con salto de volumen >10% en V2 del notebook `34`.** Esperado — es consecuencia
  directa de reemplazar Parquet stale por una extracción fresca — pero no se hizo una revisión
  tabla por tabla que lo confirme explícitamente.
- **Deuda de Prioridad 2, ya reencuadrada por lo aprendido esta sesión**: proyectar
  `alfanumerica` en `QUERY_DATOS_LECTURA`, `job_name` en el DDL de `midas_log_cargas`, consumir
  `midas_parametros` desde las queries de Oracle. El ítem de "PK compuestas en 6 tablas Bronze"
  del handoff anterior queda **descartado, no pendiente**: la medición real mostró que la mayoría
  de esas tablas no tiene ninguna combinación de columnas verificablemente única, así que
  "declarar PK compuesta" no era el arreglo correcto — ver §4 y §5.
- **Cuatro preguntas de negocio sin resolver**, sin cambios esta sesión: si la energía reactiva
  entra a las unidades de consumo (`conceptos_consumo_reactivo` sigue INACTIVO), si el concepto
  `899` debe contar como consumo base, el umbral de vuelta falsa con la fórmula nueva
  (`tolerancia_vuelta_falsa` sigue INACTIVO), y el reparto de `delta_valor_pct` por tipo.
- **La historia de git sigue conteniendo PII** (commit `79f15d6`, 768 KB). Sin cambios; decisión
  pendiente del dueño del repositorio con seguridad y legal.
- **El gate de calidad del pipeline sigue sin confirmarse con un build real.** [Supuesto], sin
  cambios esta sesión.

---

## 3. Archivos de esta sesión

Del más reciente al más antiguo. Todo bajo `feature/midas_data_platform`.

- **7 `.sql` en `src/midas/sql/bronze/ddl/`** (2026-08-11, sin commitear) — PK retirada donde los
  datos la contradicen (`lecturas_producto_c2`, `consumos_producto_c2`,
  `ordenes_previa_critica_c2`, `cometarios_ordenes_c2`, `detalle_cargos_c2`,
  `consumos_contrato`, `investigacion_consumo`), con el grano observado documentado inline.
  Reemplazan a la versión commiteada en `e212aba`/`367d84c`, que declaraba las 12 PK sin haberlas
  verificado contra datos reales.
- **`notebooks/00_creacion_objetos_midas.py`** (2026-08-11, sin commitear) — bloque
  `_PK_FALSAS_RETIRADAS` con los 7 `ALTER TABLE ... DROP CONSTRAINT IF EXISTS`. Se suma a los
  cambios ya commiteados de esta misma sesión: los 12 DDL de Bronze, `_RETIRADAS_C2` (desactiva
  las 14 filas del plano de control que apuntaban a objetos del Caso 1), y `detalle_solicitudes`
  pasada de `SILVER_TABLE_ADOPTADA` a `SILVER_TABLE`.
- **`notebooks/31_validacion_v3.py`** (2026-08-11, sin commitear la última ronda) — `PK_DECLARADA`
  reducida a las 5 tablas realmente únicas, `SIN_PK_MEDIDAS` nueva (bloque V8b) para las 7 sin PK,
  V8 sube de `REVISAR` a `FALLA`. Antes en esta misma sesión: fix del bug
  `WRONG_NUM_COLUMNS` en `F.greatest` (11 de 12 PK eran de una sola columna, y `greatest` exige
  mínimo dos — reemplazado por un OR acumulado), y corrección del contrato de
  `perdidas_no_operacionales_bronze` que no incluía `estado_pno_desc` (agregada el 2026-08-05,
  nunca reflejada aquí).
- **`tests/test_transformations.py`** (2026-08-11, sin commitear) — test nuevo que exige que la
  lista de `_PK_FALSAS_RETIRADAS` y los DDL sin PK coincidan exactamente; verificado que falla si
  alguien los desalinea.
- **`notebooks/34_validacion_corrida_endurecimiento.py`** (2026-08-11, commit `367d84c`) — V3
  reencuadrado (la tabla ya no es "adoptada"), **V3b nueva**: el único chequeo que mide
  directamente si algún objeto del Caso 1 se escribió en la corrida, con el corte derivado de los
  datos (la escritura más temprana sobre un objeto `c2`) en vez de una fecha cableada a mano —
  corregido después de que la primera versión usara un timestamp fijo que podía desalinearse con
  el huso horario de `DESCRIBE HISTORY`. También corregida la expectativa **invertida** de V1
  ("el total debe ser el menor del historial"): era cierta para el cambio causal→concepto del
  08-03 y falsa desde que el 545 empezó a *sumar* unidades.
- **`notebooks/32_validacion_silver_caso2.py`** (2026-08-11, commit `367d84c`/`7917470`) —
  eliminada la exención `ADOPTADAS` que eximía a `detalle_solicitudes_c2_silver` del chequeo de
  comentarios (ya no aplica: la tabla es nuestra y declara sus 11); S9 renombrado de "Caso 1
  intacto" a "las copias heredadas conservan su forma".
- **12 `.sql` en `src/midas/sql/bronze/ddl/`** (creados 2026-08-11, commit `e212aba`) — primera
  versión, generados mecánicamente desde un volcado real de schema (Parquet + tabla) más la cola
  de `_MIGRACION_V3`, no escritos a mano. Ver `scripts/volcado_schema_bronze.py`.
- **`src/midas/sql/silver/ddl/midas_datos_detalle_solicitudes_c2_silver.sql`** (2026-08-11,
  commit `e212aba`) — primera versión, escrito a mano con los 11 `COMMENT`. Reemplaza el
  concepto de "tabla adoptada sin DDL propio" que tenía el objeto compartido.
- **`src/midas/ingestion.py`** (2026-08-11, commit `e212aba`) — `load_parquet_to_delta` ya no
  crea tablas (falla explícito si falta el DDL); nueva
  `_verificar_columnas_posicionales` que compara nombre y orden antes de cada `insertInto`.
- **9 archivos `_c2_` renombrados** (Bronze y Silver, commit `ab6f397`) — rename mecánico
  verificado con control positivo y negativo (249 reemplazos, cero nombres viejos
  sobrevivientes). Reemplazan los nombres compartidos con el Caso 1 en todo `src/`,
  `notebooks/` y `tests/`.
- **`docs/07_MANUAL_TECNICO.md`, `docs/contrato_silver.md`, `docs/semantica_campos_caso2.md`**
  (2026-08-11, commit `ab6f397`) — fragmentos puntuales sobre la tabla adoptada corregidos
  (marcados como "superados por el fork `c2`" en vez de reescritos, para no perder el porqué
  original). `contrato_silver.md` **sigue** con la deuda de "44 columnas" heredada del handoff
  anterior — no se tocó esa sección.
- **`src/midas/silver_params.py` y `notebooks/00_creacion_objetos_midas.py`** (2026-08-11,
  commit `a8a53ea`) — escape de comillas simples corregido de `''` (duplicado, estándar SQL que
  Spark NO soporta — SPARK-20837) a `\'` (backslash, el que Spark sí reconoce). Causó un
  `PARSE_SYNTAX_ERROR` en `crear_objetos` porque un `COMMENT` con comilla admite un solo token
  string; en un `VALUES` el error quedaba invisible porque Spark concatena literales adyacentes.

---

## 4. Decisiones Tomadas

> Esta sección prioriza el contexto de negocio y de datos que motivó cada decisión.

### Fuentes de datos y contexto de negocio

**Oracle FLEX** es el sistema comercial de EPM: facturación, medidores, órdenes de trabajo,
trámites del cliente. Lo operan los analistas de facturación; es la fuente de verdad del negocio
y **nosotros solo leemos**.

Las 12 extracciones se ejecutan **en cadena** (`FULL_CHAINED`): cada paso alimenta al siguiente.
De las órdenes de calidad pendientes salen los servicios suscritos; de esos, los datos básicos;
de ahí lecturas, consumos, cuentas de cobro y cargos.

El consumidor es el **agente del Caso 2**, que vive en otro bundle (`midas_agent`) y **todavía no
consume nada de esta capa** — eso es lo que hizo posible renombrar los 14 objetos compartidos sin
coordinación externa. El Caso 1 (actividad 1019, diferencia acueducto-alcantarillado) es de otro
equipo.

**El fork `c2` reemplaza la "frontera contractual".** Hasta esta sesión, la relación con el Caso 1
era: 8 tablas Bronze de la cadena se comparten (mismas queries, mismo destino), y 4 tablas Silver
+ 1 tabla "adoptada" (`detalle_solicitudes`) también. Esa convivencia tuvo un costo medible: dos
procesos escribiendo `detalle_solicitudes_silver` al mismo tiempo, y un `CREATE OR REPLACE TABLE`
del otro equipo que dejó R5 (reconexión/suspensión) en cero durante días sin que nada fallara —
las banderas booleanas seguían poblándose con `false`, indistinguible de "no hubo ninguna" hasta
que se comparó contra un `COUNT` de no-nulos. Ahora este bundle construye **su propia copia**
`midas_<nombre>_c2_<capa>` de los 14 objetos compartidos, con SQL verbatim salvo los nombres
(para que el contenido siga siendo demostrablemente el mismo que produce el Caso 1), y deja de
escribir los originales. Las tablas viejas **no se borran**: siguen siendo del otro equipo.

### Diccionario de datos / conocimiento semántico

**Los nombres de FLEX no se interpretan solos** (`cosscoca`, `leemfame`, `sesunuse`), por eso cada
columna de Silver lleva su `COMMENT`.

- **Servicio suscrito** es la unidad de análisis. Un **contrato** agrupa varios servicios (agua,
  energía, gas) del mismo predio.
- **`tipo_consumo`**: `3` activa, `6` reactiva. El **concepto del cargo** distingue:
  `90/545/546` activa, `93/547/548` reactiva. [Validado] — catálogo completo de 143 conceptos.
- **`metodo_calculo = 4`** es el único método que se cobra: es el filtro que define qué entra a
  `historial_consumo`.
- **`calificacion = 1` (NORMAL)** es el único valor normal de consumo. [Validado].
- **`estado_pno`**: `R` en inspección, `E` excluido, `F` fraude confirmado, `N` fraude no
  detectado, `P` pendiente. Sin tabla catálogo en Oracle: se resuelve con `CASE` explícito.
- **`fecha_retiro` usa el comodín `31/12/4732`** del sistema Open: "sin fecha de retiro", no una
  fecha real.
- **`documento_soporte`** tiene forma `CO-PR-202606-...`. El token de recuperación se detecta por
  parseo posicional, nunca `LIKE '%PR%'`.
- **La orden de decisión del analista (`7400027`)** es el *ground truth* del agente. Es la misma
  rejilla de "Órdenes de Crítica y Previa" que ya se extrae. [Validado] esta sesión — 26 órdenes
  llegan con sus 26 comentarios en `dllo`; `102010` y `7400027` son mutuamente excluyentes.
- **Un `PRIMARY KEY` en Unity Catalog es una afirmación, no una restricción impuesta.** No falla
  al violarse; solo miente. Descubierto esta sesión de forma dura: 7 de 12 tablas Bronze
  declaraban una PK que sus propios datos contradecían (p.ej. `detalle_cargos_c2`: 21.844 filas
  para 4.480 `id_cuenta_cobro`, un factor de ~5x). El `NOT NULL` de la misma columna **sí** se
  hace cumplir, y esa asimetría es la razón por la que "arreglar" una PK agregando una columna al
  grano puede **romper la carga**: si esa columna nueva viene nula en algún porcentaje de filas
  (como `medidor` en `lecturas`, ~24%), el `NOT NULL` que la PK exige la rechaza. Silver puede
  sustituir el nulo por un centinela (`'(sin medidor)'`); Bronze, al ser réplica fiel de FLEX, no.
- **Los Parquet de una cadena de extracción pueden quedar desactualizados sin que nada lo
  anuncie.** 6 de 8 tablas Bronze de la cadena Caso 1 tenían Parquet anteriores a los cambios de
  v3 (2026-07-28), y eso solo se detectó al construir un DDL explícito que dejó de tolerar
  columnas faltantes.

### Configuración de plataforma

- **Databricks Asset Bundle** con targets `dllo` / `uat` / `pdn`. **El código llega por un Git
  folder** conectado a la rama, no por `databricks bundle deploy`. Ciclo real: **commit + push →
  pull en el Git folder → correr (o Repair run) el job**.
- **Oracle vía JDBC driver-side** (JayDeBeApi + JPype1 + ojdbc11), nunca
  `spark.read.format("jdbc")`. Runtime 16.4 (JDK 17) emparejado con ojdbc11.
- **Cero dimensiones materializadas** (invariante I11). El código-descripción se resuelve inline
  con subconsultas correlacionadas dentro de la query de Oracle.
- **Cero umbrales cableados** (invariante I17). Todo valor de negocio vive en `midas_parametros`.
- **El MERGE de parámetros es insert-if-missing a propósito**: si sobrescribiera valores, se
  perdería cualquier ajuste que negocio haga por `UPDATE`. Consecuencia operativa medida en vivo
  esta sesión: **cambiar el valor de una clave existente en el seed de Python NO la cambia en un
  ambiente donde ya existe**. `conceptos_consumo_medido` se corrigió en el código el 2026-08-05
  pero siguió con el valor viejo en `dllo` hasta que se detectó por auditoría de diffs y se agregó
  un `UPDATE` explícito condicionado al valor viejo exacto (`_CORRECCIONES_VALOR`).
- **`insertInto(overwrite=True)` es POSICIONAL.** Ahora hay una guarda explícita
  (`_verificar_columnas_posicionales`) que compara nombre y orden de columnas antes de escribir y
  aborta con un mensaje legible si difieren, en vez de dejar que corra los valores en silencio.
- **El DDL de Silver es `CREATE TABLE IF NOT EXISTS`**, que contra una tabla existente no hace
  nada. Lo mismo se descubrió esta sesión para las PRIMARY KEY: **quitar una constraint del DDL
  no la quita de una tabla que ya existe**. Hace falta un `ALTER TABLE ... DROP CONSTRAINT`
  explícito, igual que `_MIGRACION_V3`/`_MIGRACION_SILVER` existen para las columnas.
- **En Unity Catalog la PRIMARY KEY es informativa** (no se hace cumplir), pero el **`NOT NULL`
  sí**. Ver el punto del diccionario de datos arriba — esta sesión lo convirtió de advertencia
  teórica a incidente medido con números concretos.
- **El schema de Bronze ahora lo declara el repo, no el Parquet.** `crear_objetos` ejecuta los 12
  DDL de `src/midas/sql/bronze/ddl/` como primer paso; `ingestion.py` ya no crea tablas. Antes el
  schema salía de `saveAsTable` sobre lo que trajera el Parquet de turno — y ese Parquet podía ser
  de una corrida vieja, que es exactamente lo que produjo el `UNRESOLVED_COLUMN` de esta semana.
- **Spark NO soporta `''` (comilla duplicada) como escape de comilla simple**, a pesar de ser el
  estándar SQL (ticket SPARK-20837, resuelto como "Incomplete", nunca arreglado). El escape
  correcto es `\'`. En un `VALUES` el error es invisible porque Spark concatena literales
  adyacentes; en un `COMMENT` (que admite un solo token string) revienta con
  `PARSE_SYNTAX_ERROR`. Afectaba a `_sql_val` y `_literal_sql` desde el 2026-07-24, sin que
  nadie lo notara hasta que una columna con comilla (`'tolerancia'`) cayó por primera vez en un
  `COMMENT`.

### Información validada vs. supuestos

**[Validado]**

- Grano único en las tres tablas Silver nuevas: `historial_consumo` 3.180 = 3.180,
  `features_consumo` 3.152 = 3.152, cero nulos en las claves.
- **Cuadre del consumo al 100,00%**.
- R5 con señal real: **175** reconexión, **201** suspensión, **640** con `fecha_ultima_reconexion`
  poblada — reproducido en `dllo` esta sesión sobre datos frescos, coincide exactamente con la
  medición del 2026-08-03.
- El concepto `545` está activo en `conceptos_consumo_medido` en `dllo`.
- 26 órdenes `7400027` llegan con sus 26 comentarios asociados; no hay ningún problema en la
  rama 4 de `QUERY_ORDENES_CRITICA_PEVIA`.
- **Los Parquet de 6 tablas de la cadena Caso 1 estaban desactualizados** respecto a lo que sus
  queries de Oracle proyectan desde v3 — confirmado columna por columna (V2.1 del notebook 31).
- **7 de 12 PRIMARY KEY declaradas en los DDL de Bronze eran falsas** — medido con `COUNT(*)` vs
  `COUNT(DISTINCT pk)` real en `dllo`, números exactos documentados en cada DDL.
- El fork `c2` no dejó ningún objeto del Caso 1 con escrituras nuestras posteriores al corte
  (V3b, notebook 34).
- Un solo escritor sobre `detalle_solicitudes_c2_silver` tras el fork.

**[Supuesto]**

- **La magnitud del `+29%` esperado en `unidades_consumo_cobradas`.** Solo se puede confirmar con
  una corrida que aísle el cambio del parámetro de la extracción fresca de Oracle — esta sesión
  movió ambas cosas a la vez.
- **Que `cargos=1, features=0` para el 899 sea un filtro esperado por `id_periodo_consumo` nulo.**
  No verificado con una query directa.
- **Que el gate de CI bloquee de verdad.** Sin cambios esta sesión.
- Las cuatro preguntas de negocio abiertas (reactiva, 899 como consumo base, umbral de vuelta
  falsa, reparto de `delta_valor_pct`).

### Cómo este contexto moldeó el código

- **"Una PK que los datos no cumplen es peor que ninguna"** (ya escrito en el DDL de
  `historial_cargos_silver` antes de esta sesión) → se aplicó retroactivamente a las 7 tablas
  Bronze que lo violaban, en vez de tratarlas como una anomalía aislada de `historial_cargos`.
- **"El evaluador no puede ser parte de lo evaluado"** → no hay columnas de veredicto en Silver;
  las features publican medidas, no conclusiones.
- **Convivir con un escritor externo tiene costo medible, no solo teórico** → el fork `c2` no fue
  una preferencia de limpieza: se disparó porque la convivencia rompió R5 en producción sin que
  nada fallara.
- **Un DDL describe el estado deseado; una migración lo alcanza** → aplica igual a columnas nuevas
  (`_MIGRACION_V3`/`_MIGRACION_SILVER`, patrón preexistente) y a constraints que se retiran
  (`_PK_FALSAS_RETIRADAS`, patrón nuevo esta sesión): ninguno de los dos casos lo resuelve un
  `CREATE TABLE IF NOT EXISTS` por sí solo.
- **Un parámetro sin confirmar no puede producir un número inventado** → guarda explícita sobre el
  parámetro en toda bandera que dependa de uno.
- **Los códigos negativos existen** → `REGEXP_EXTRACT` en todas partes, nunca `SPLIT`.

---

## 5. Intentos Fallidos

No repetir ninguno de estos.

| Enfoque probado | Por qué se descartó |
|---|---|
| **Recrear la tabla `datos_basicos_producto` desde el Parquet actual** para explicar el `UNRESOLVED_COLUMN` | Se verificó que ni la tabla ni el Parquet tenían la columna: no fue un escritor externo recreando el schema, fue una extracción vieja. Reconstruir el DDL desde "Parquet + cola de `_MIGRACION_V3`" fue lo que funcionó. |
| **Declarar PK compuesta en las 7 tablas con duplicados** (plan inicial de esta sesión) | Al menos 2 de las 7 (`consumos_producto`, `detalle_cargos`) no tienen ninguna combinación de columnas disponibles que sea única — está medido, no es hipótesis. Y para `lecturas`, la columna que sí completa el grano único (`medidor`) viene nula en ~24% de las filas, así que agregarla a la PK haría fallar la carga por el `NOT NULL` que exige. |
| **Fijar el corte temporal de V3b con una fecha cableada a mano** | Un timestamp escrito a mano se compara contra el huso horario que reporte `DESCRIBE HISTORY`, y si no coinciden el chequeo da `OK` sin haber medido nada. Se reemplazó por un corte derivado de los datos: la escritura más temprana de la corrida sobre un objeto `c2`. |
| **Asumir "el total debe bajar" como expectativa fija en V1** | Válida solo para la transición causal→concepto del 08-03. El concepto `545` **sube** el total. Una expectativa cableada para una sola transición no es un chequeo, es una trampa que da falsos `REVISAR`. |
| **`F.greatest(*cols)` para detectar "alguna columna nula"** | Exige mínimo 2 argumentos; 11 de las 12 PK declaradas eran de una sola columna. Reemplazado por un OR acumulado (`F.lit(False)` + `|=`), que funciona igual con 1 o más columnas. |
| **Duplicar la comilla simple (`''`) como escape SQL** | Es el estándar SQL, pero Spark no lo soporta (SPARK-20837, nunca resuelto): lo lexa como dos literales adyacentes. Funciona "por accidente" en un `VALUES` (Spark los concatena) pero revienta en un `COMMENT`. El escape correcto es `\'`. |
| **Confiar en que `saveAsTable` cree las tablas Bronze con el schema correcto** | El schema salía de lo que trajera el Parquet de turno, sin garantía de que reflejara el estado actual de la query de Oracle. Ahora `crear_objetos` gobierna el schema desde DDL explícito, e `ingestion.py` falla si la tabla no existe en vez de crearla. |
| **Dos tablas Bronze nuevas** para la orden de decisión del analista | Es la misma rejilla "Órdenes de Crítica y Previa" que ya se extrae. |
| **Unir lecturas ⋈ consumos por medidor** | `cosselme` es `elmeidem` mientras `lecturas.medidor` es `elmecodi`: no son joinables. |
| **`FIRST()` para desambiguar** un atributo con varios valores | Valor arbitrario y no determinista presentado como dato. |
| **`causal_cod = -1` para aislar el consumo** en cargos | Es el 99% de las líneas: no aislaba nada. |
| **`information_schema.tables.last_altered`** como señal de frescura | No registra escrituras de datos, solo cambios de definición. |
| **`DROP TABLE` de `detalle_solicitudes_silver`** | En su momento se adoptó en vez de recrear; esta sesión la reemplazó por completo con el fork `c2`, así que ya no aplica ninguna de las dos alternativas anteriores. |

---

## 6. Próximos Pasos

En orden de prioridad.

1. **Commitear y correr `crear_objetos` una vez más en `dllo`** para que aterrice el `ALTER TABLE
   ... DROP CONSTRAINT` sobre las 7 PK falsas. Sin esto, el repo dice una cosa y `dllo` sigue
   afirmando otra — exactamente la deriva que costó esta sesión completa detectar. Los archivos
   están listos y verificados localmente; solo falta el commit + push + pull + corrida.
   ```
   git add -A && git commit -m "Retira las 7 PK que los datos contradicen y las documenta"
   ```
2. **Correr el notebook `31` después del paso 1** y confirmar: V8 en `OK` sobre las 5 PK reales,
   V8b informativo sobre las 7 sin PK, sin ningún `FALLA`.
3. **Aislar la medición del `+29%`** de `unidades_consumo_cobradas`: correr `bronze_to_silver` una
   vez más sin cambiar el parámetro ni refrescar Oracle, para que el *time travel* del notebook
   `34` compare exactamente el efecto del concepto `545` y nada más.
4. **Investigar el enriquecimiento de `COMMENT` de Bronze** para recuperar los comentarios
   perdidos en las 4 copias Silver heredadas del Caso 1 (`basicos_producto` 2/23,
   `ordenes_calidad_pendientes` 3/25, `historial_critica` 4/13, `historial_facturacion` 0/23).
   Requiere que negocio o el diccionario confirmen el significado de las columnas hoy sin
   comentar.
5. **Investigar `cargos=1, features=0` para el concepto `899`.** Confirmar o descartar la
   hipótesis del `id_periodo_consumo` nulo con una query directa.
6. **Actualizar `docs/contrato_silver.md`**: 49 columnas en `features_consumo`, y reflejar el
   fork `c2` en la sección de objetos Nivel 1 más allá de los fragmentos puntuales ya corregidos.
7. **Revisar tabla por tabla los 7 saltos de volumen >10%** de V2 (notebook 34) para confirmar
   explícitamente que son consecuencia de la extracción fresca y no otra causa.
8. **Llevar a negocio** las cuatro preguntas abiertas: energía reactiva en unidades de consumo
   (`conceptos_consumo_reactivo` INACTIVO), si el `899` cuenta como consumo base, umbral de
   vuelta falsa con la fórmula nueva (`tolerancia_vuelta_falsa` INACTIVO), y reparto de
   `delta_valor_pct` por tipo.
9. **Deuda de Prioridad 2 reencuadrada**: proyectar `alfanumerica` en `QUERY_DATOS_LECTURA`,
   `job_name` en el DDL de `midas_log_cargas`, consumir `midas_parametros` desde las queries de
   Oracle. El ítem de "PK compuestas" queda descartado por lo aprendido esta sesión (§5).
10. **Decidir sobre la PII en la historia de git.** Requiere seguridad y legal; sin cambios.
    Detalle en `docs/INCIDENTE_2026-08-03_pii_en_notebook.md`.
11. **Una corrida verde desde el bundle desplegado**, no solo desde el Git folder, antes de
    promover a `uat`. Sin cambios esta sesión.
