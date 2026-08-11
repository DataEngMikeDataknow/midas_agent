# Handoff — MIDAS `midas_data_platform` · Caso 2 (variación significativa de consumo)
_Generado: 2026-08-10 (hora local del usuario, America/Bogota)_

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

---

## 2. Estado Actual

### Qué funciona

- **12 tablas Bronze y 13 objetos Silver** (8 tablas + 5 vistas) construyéndose de punta a
  punta. [Validado] — corrida del 2026-08-03 23:27–23:29 UTC, con bitácora completa y sin
  registros `FALLIDO`.
- **Notebook `32` en 0 FALLA / 93 OK / 7 REVISAR.** [Validado] — ejecutado sobre `dllo`.
- **Notebook `34` (validación de corrida) sin FALLA reales.** [Validado] — las 2 FALLA de su
  primera ejecución resultaron ser defectos de sus propios chequeos, ya corregidos.
- **137 tests pasan.** [Validado] — con 1 fallo preexistente (`test_ensure_schema_exists`)
  que por acuerdo explícito **no se toca**; está deseleccionado en el pipeline de CI.
- **Gate de calidad en el pipeline**: stage `Calidad` (tests + gate de PII) del que dependen
  los tres despliegues. [Supuesto] — el YAML es válido y las condiciones tienen `succeeded()`
  explícito, pero **no se ha ejecutado un build real** que lo confirme.
- **Árbol de trabajo limpio**, todo commiteado en `540ac25` (2026-08-06, "Ajuste features
  silver").

### Qué falta / está incompleto

- **`notebooks/35_verificacion_oracle_7400027.py` no se ha ejecutado.** Es lo que decide si la
  rama 4 de `QUERY_ORDENES_CRITICA_PEVIA` tiene un problema o simplemente no hay datos.
- **`docs/contrato_silver.md` quedó desactualizado.** Dice "44 columnas" en
  `features_consumo` (hoy son **49**), no menciona R7 (`tuvo_consumo_alto_historico`) ni el
  concepto `545`, y su sección de columnas en `NULL` refleja el estado anterior al 2026-08-05.
  La recencia es inequívoca: el código y el handoff del 08-05 son posteriores.
- **Los cambios del 2026-08-05 no se han corrido en Databricks.** Requieren `crear_objetos`
  (siembra parámetros, desactiva el `5097`, aplica los `ALTER`) y luego repair de
  `bronze_to_silver`.
- **Deuda de Prioridad 2 sin empezar**: PK compuestas en 6 tablas Bronze, proyectar
  `alfanumerica`, `job_name` en el DDL de `midas_log_cargas`, consumir `midas_parametros` desde
  las queries de Oracle.
- **La historia de git sigue conteniendo PII.** El working tree está limpio, pero el commit
  `79f15d6` conserva 768 KB de outputs con datos de clientes. Decisión pendiente del dueño del
  repositorio con seguridad y legal.

---

## 3. Archivos de esta sesión

Del más reciente al más antiguo.

- **`docs/HANDOFF_2026-08-05_definiciones_negocio.md`** (2026-08-05) — Handoff de las
  definiciones de negocio: lista de conceptos corregida, fórmula de vuelta falsa, R7, siete
  parámetros confirmados. Primera versión, no reemplaza nada.
- **`notebooks/35_verificacion_oracle_7400027.py`** (2026-08-05) — Notebook autocontenido
  contra Oracle que separa dos hipótesis sobre por qué no llegan las órdenes `7400027`.
  Primera versión. **Sin ejecutar.**
- **`Downloads/Justificacion_modelo_Silver_Caso2.md`** (2026-08-04) — Documento de
  justificación de los 13 objetos Silver para revisión con negocio, arquitectura y DBA.
  Primera versión.
- **`docs/HANDOFF_2026-08-04_aprendizajes_validacion.md`** (2026-08-04) — Los tres errores de
  instrumentación del notebook `34` y el patrón detrás de ellos. Primera versión.
- **`Diagramas/Midas modelo silver caso 2.drawio`** (2026-08-04) — Cuatro correcciones de
  drift. Reemplaza a la versión generada el 2026-08-03, que mostraba 44 columnas y marcaba en
  gris tres columnas de R5 que ya tienen datos.
- **`notebooks/34_validacion_corrida_endurecimiento.py`** (2026-08-03, corregido 08-04) —
  Valida **la corrida**, no el contrato de la capa. Primera versión; sus tres chequeos se
  reescribieron el 08-04.
- **`docs/HANDOFF_2026-08-03_endurecimiento.md`** (2026-08-03) — Findings de auditoría, fases
  A y B. Primera versión.
- **`docs/INCIDENTE_2026-08-03_pii_en_notebook.md`** (2026-08-03) — Alcance de la exposición
  de PII, comando de reescritura y consecuencias. Primera versión.
- **`scripts/check_notebook_outputs.py`** y **`.pre-commit-config.yaml`** (2026-08-03) — Gate
  que rechaza `.ipynb` con outputs. Primeras versiones.
- **`tests/test_frescura_datos.py`** (2026-08-03) — Tests del hallazgo F02. Primera versión.
- **`notebooks/33_recorrido_silver_caso2.py`** y **`notebooks/32_validacion_silver_caso2.py`**
  (2026-07-31) — Recorrido explicativo y validación automática de la capa. Primeras versiones.
- **`docs/contrato_silver.md`** y **`docs/adr/0002-grano-historial-consumo.md`** (2026-07-31)
  — Contrato de la capa y decisión de grano. Primeras versiones. **El contrato quedó
  desactualizado** (ver Estado Actual).
- **`notebooks/91_explorador_modelo_bronze_caso2.ipynb`** (2026-08-03) — Se le limpiaron los
  outputs: 768.931 → 195.532 bytes, con las 91 celdas y los 111.923 caracteres de código
  intactos. Reemplaza a la versión commiteada en `79f15d6`, que contenía PII.

---

## 4. Decisiones Tomadas

> Esta sección prioriza el contexto de negocio y de datos que motivó cada decisión.

### Fuentes de datos y contexto de negocio

**Oracle FLEX** es el sistema comercial de EPM: facturación, medidores, órdenes de trabajo,
trámites del cliente. Lo operan los analistas de facturación; es la fuente de verdad del
negocio y **nosotros solo leemos**.

Las 12 extracciones se ejecutan **en cadena** (`FULL_CHAINED`): cada paso alimenta al
siguiente. De las órdenes de calidad pendientes salen los servicios suscritos; de esos, los
datos básicos; de ahí lecturas, consumos, cuentas de cobro y cargos.

El consumidor es el **agente del Caso 2**, que vive en otro bundle (`midas_agent`). El Caso 1
(actividad 1019, diferencia acueducto-alcantarillado) es de otro equipo y sus cuatro tablas
Silver son **frontera contractual**: se preservan verbatim.

### Diccionario de datos / conocimiento semántico

**Los nombres de FLEX no se interpretan solos** (`cosscoca`, `leemfame`, `sesunuse`), por eso
cada columna de Silver lleva su `COMMENT`.

- **Servicio suscrito** es la unidad de análisis. Un **contrato** agrupa varios servicios
  (agua, energía, gas) del mismo predio. Sirve para descartar que el problema sea del predio y
  no del servicio: si subieron dos servicios a la vez, la causa no está en un medidor.
- **`tipo_consumo`**: `3` activa, `6` reactiva. La factura **no distingue** activa de reactiva
  a nivel de cuenta de cobro, pero el **concepto del cargo sí**: `90/545/546` son activa,
  `93/547/548` reactiva. [Validado] — catálogo completo de 143 conceptos consultado el
  2026-08-05.
- **`metodo_calculo = 4`** es el único método que se cobra. No es columna en Silver: es el
  **filtro** que define qué entra a `historial_consumo`.
- **`calificacion = 1` (NORMAL)** es el único valor normal de consumo. [Validado] — catálogo
  de 31 valores, 3.828 filas, confirmado por negocio.
- **`estado_pno`**: `R` en inspección, `E` excluido, `F` fraude confirmado, `N` fraude no
  detectado, `P` pendiente. Catálogo entregado por negocio el 2026-08-05. En Oracle **no
  existe tabla catálogo** para este campo, así que se resuelve con un `CASE` explícito.
  [Validado] — en `dllo` solo aparece `F` (12 filas).
- **`fecha_retiro` usa el comodín `31/12/4732`** del sistema Open, que significa "sin fecha de
  retiro", no una fecha real. Se publica `fecha_retiro_es_comodin` para que nadie lo
  interprete mal.
- **El límite inferior de consumo suele ser cero**, así que "dentro de límites" por sí solo no
  discrimina nada. Solo el superior es señal útil.
- **`documento_soporte`** tiene forma `CO-PR-202606-...`. El token de recuperación se detecta
  por **parseo posicional**, nunca con `LIKE '%PR%'`, que daría positivo con cualquier texto
  que contenga esas letras.
- **La orden de decisión del analista (`7400027`)** es el *ground truth* del agente: el
  comentario contiene su justificación final. Es la **misma rejilla** de "Órdenes de Crítica y
  Previa" que ya se extrae. [Validado] — `102010` y `7400027` son mutuamente excluyentes; cero
  órdenes tienen ambas.

### Configuración de plataforma

- **Databricks Asset Bundle** con targets `dllo` / `uat` / `pdn`. **El código llega por un Git
  folder** conectado a la rama, no por `databricks bundle deploy`. El ciclo real es
  **commit + push → pull en el Git folder → correr (o Repair run) el job**. [Validado] — los
  stack traces muestran `/Workspace/Users/mpalomin@contratista.epm.co/midas_agent/`, no el
  `root_path` del `databricks.yml`.
- **Oracle vía JDBC driver-side** (JayDeBeApi + JPype1 + ojdbc11), nunca
  `spark.read.format("jdbc")`. La URL va sin `//` después del `@`. Runtime 16.4 (JDK 17)
  emparejado con ojdbc11: un JDK 8 produce SIGSEGV.
- **Cero dimensiones materializadas** (invariante I11). El código-descripción se resuelve
  **inline** con subconsultas correlacionadas dentro de la query de Oracle. Motivo: una
  dimensión da dos caminos al mismo dato sin garantía de que coincidan.
- **Cero umbrales cableados** (invariante I17). Todo valor de negocio vive en
  `midas_parametros`. Cambiar un umbral es un `UPDATE`, no un despliegue.
- **El MERGE de parámetros es insert-if-missing a propósito**: si sobrescribiera valores, se
  perdería cualquier ajuste que negocio haga por `UPDATE`. Consecuencia operativa: **quitar un
  parámetro del seed NO lo desactiva** en un ambiente donde ya existe; hace falta un `UPDATE`
  explícito.
- **`insertInto(overwrite=True)` es POSICIONAL.** Toda columna nueva en Bronze va **al final**;
  meterla en medio corre todos los valores siguientes **sin lanzar error**.
- **El DDL de Silver es `CREATE TABLE IF NOT EXISTS`**, que contra una tabla existente no hace
  nada. Por eso existe `_MIGRACION_SILVER`, que además aplica `ALTER COLUMN ... COMMENT`:
  `ALTER ADD COLUMNS` agrega la columna **muda**.
- **En Unity Catalog la PRIMARY KEY es informativa** (no se hace cumplir), pero el **`NOT NULL`
  sí**. Esa es la razón real del centinela `'(sin medidor)'`.

### Información validada vs. supuestos

**[Validado]**

- Grano único en las tres tablas nuevas: `historial_consumo` 3.180 filas = 3.180 combinaciones,
  `features_consumo` 3.152 = 3.152, cero nulos en las claves. — notebook `32`, bloque S4.
- **Cuadre del consumo al 100,00%**: `consumo_facturado_periodo` coincide con la suma por
  medidor en las 3.180 filas. Respalda directamente el ADR 0002. — notebook `32`, S5.
- **28 periodos con más de un medidor** (3.180 − 3.152). Es la señal cruda del Caso 3/4.
- **420 de 3.071 grupos (13,7%) tienen calificación ambigua** bajo método 4. Con `FIRST()`,
  esos 420 habrían recibido un valor arbitrario. — Genie, 2026-08-03.
- **R5 con señal real**: 175 periodos con reconexión intersectando, 201 con suspensión, 640 con
  fecha de reconexión poblada. — consulta directa, 2026-08-03.
- **Los 7 códigos de `package_type_id`** confirmados contra el catálogo real de 14 valores:
  `15`, `42`, `56`, `288`, `289`, `300`, `100207`. — Genie, 2026-08-05.
- **La causal `-1` es el 99% de las líneas de cargo** (21.596 de 21.844): no aísla el consumo.
- **El catálogo de calificación no contiene el `5097`**; existe `5091-MEDIDOR NO CONFORME
  CALIBRACION`, el opuesto semántico.
- **29,7% de las filas no tiene límite superior usable** (898 nulas + 45 en cero de 3.180).
- **`es_recuperacion` = `documento_tiene_token_recuperacion` = 476 filas**: el segundo
  componente no descarta ninguna fila dentro del conjunto del token.
- **PNO: 7 servicios con cargo, 10 con expediente, 6 en ambos.** La coincidencia parcial es
  hallazgo de negocio, no defecto.
- **La tabla `midas_datos_detalle_solicitudes_silver` tiene 11 columnas** y las mismas que su
  Bronze. — `DESCRIBE`, 2026-08-03.

**[Supuesto]**

- **Que la rama 4 de `7400027` funcione.** El código existe y tiene un `drop_duplicates` que
  solo aparece si devuelve filas, pero Bronze tiene **cero** órdenes con esa actividad. Falta
  correr el notebook `35`.
- **Que el `estado_pno_desc` se pueble bien.** El `CASE` está escrito pero no se ha ejecutado
  contra Oracle.
- **Que las features de R7 den valores razonables.** El SQL resuelve y las columnas están en
  el DDL y la migración, pero no se ha corrido la carga.
- **Que las propiedades de timeout JDBC funcionen.** Requieren pasar un dict en vez de la
  lista `[user, password]`; hay que verificarlo en UAT.
- **Quién es el segundo escritor de `detalle_solicitudes_silver`.** El 2026-08-04 bronze y
  silver se escribieron con 0,8 minutos de diferencia, consistente con nuestra corrida, pero
  el 2026-07-31 hubo una escritura a las 14:01 que no fue nuestra.
- **Que el gate de CI bloquee de verdad.** El YAML es válido pero no se ha ejecutado un build.

### Cómo este contexto moldeó el código

- **"El evaluador no puede ser parte de lo evaluado"** → no hay columnas de veredicto en
  Silver, y las features publican medidas (`n_medidores_periodo`, `desviacion_vs_promedio_pct`)
  en vez de conclusiones.
- **La serie del medidor no siempre se actualiza en Oracle** → R1 publica **cuatro medidas
  complementarias** en vez de una bandera. Un `medidor_cambio_detectado_por_serie = false`
  **no descarta** el cambio.
- **La Bronze de consumos no proyecta el medidor, y `cosselme` (`elmeidem`) no es joinable con
  `lecturas.medidor` (`elmecodi`)** → lecturas es la espina dorsal y consumos entra agregado
  al periodo. Ver ADR 0002.
- **Un parámetro sin confirmar no puede producir un número inventado** → guarda explícita
  sobre el parámetro en toda bandera que dependa de uno. Sin ella, `CASE WHEN <NULL> THEN true
  ELSE false END` devuelve **`false`**, un negativo fabricado que se lee como hecho confirmado.
- **"La tolerancia no es un porcentaje, es precedente del propio servicio"** (negocio,
  2026-08-05) → R7 se implementó como ventana `UNBOUNDED PRECEDING AND 1 PRECEDING`, no como
  umbral.
- **Un fallo de Oracle no puede parecerse a un resultado vacío** → `execute_query` lanza,
  `save_to_parquet` escribe los vacíos con columnas, y una guarda de frescura rechaza cualquier
  Parquet anterior a la corrida.
- **Los códigos negativos existen** (`-1` causal en el 99% de las líneas, `-26-Fénix`,
  `-36-FGRA`) → `REGEXP_EXTRACT(x, '^\\s*(-?[0-9]+)', 1)` en todas partes, nunca
  `SPLIT(x,'-')[0]`, que devuelve **cadena vacía** con negativos.

---

## 5. Intentos Fallidos

No repetir ninguno de estos.

| Enfoque probado | Por qué se descartó |
|---|---|
| **Dos tablas Bronze nuevas** para la orden de decisión del analista | Una captura real confirmó que es la **misma rejilla** "Órdenes de Crítica y Previa" que ya se extrae. La corrección correcta es una rama en la query existente |
| **Unir lecturas ⋈ consumos por medidor** | Imposible: consumos no proyecta el medidor, y `cosselme` es `elmeidem` mientras `lecturas.medidor` es `elmecodi` |
| **`FIRST()` para desambiguar** un atributo con varios valores | Devuelve un valor **arbitrario y no determinista** presentado como dato. Peor que un `NULL`, que es auditable |
| **`causal_cod = -1` para aislar el consumo** en cargos | Esa causal es el **99% de las líneas**. No aislaba nada: sumaba cargo fijo, alumbrado y contribuciones con el consumo |
| **Buscar conceptos "cuya descripción contenga CONSUMO"** | Se comió el `545-CONS ENERGÍA ACTIV FUERA PUNTA` — 992.655 unidades, el 2º mayor — porque dice `CONS` abreviado |
| **`consumo_facturado / 10^dígitos`** como ratio de vuelta falsa | Mide contra el **rango del medidor**, que solo coincide con la vuelta si la lectura anterior fuera cero. Las 44 filas con consumo negativo daban todas menos de 0,067 y ningún umbral detectaba nada |
| **`information_schema.tables.last_altered`** como señal de frescura | Registra cambios de **definición**, no escrituras de datos. Un `insertInto` no lo mueve. Usar `DESCRIBE HISTORY` |
| **`current_date()`** para filtrar la corrida en la bitácora | El job corre de tarde-noche en UTC y la validación se hace después: cruzar la medianoche daba cero registros |
| **Comparar la última versión Delta contra la penúltima** | Asume una sola corrida desde el cambio. Con varias, ambas son posteriores y dan idénticas |
| **`CASE WHEN <cond NULL> THEN true ELSE false END`** y **`false AND NULL`** | Devuelven **`false`**, no `NULL`. Fabricaban negativos que se leían como hechos confirmados |
| **Pedir permisos `MANAGE`** para resolver `EXPECT_VIEW_NOT_TABLE` | El error nunca fue de permisos: era un conflicto de tipo. El pipeline ya sobrescribe otras cuatro tablas del mismo autor |
| **`DROP TABLE` de `detalle_solicitudes_silver`** | Se **adoptó** en su lugar: la tabla es del modelo original, con consumidores propios. Su schema manda |
| **Declarar una PK en `historial_cargos`** | Oracle no expone un id único de línea de cargo. Una PK falsa es peor que ninguna |

---

## 6. Próximos Pasos

En orden de prioridad.

1. **Correr `crear_objetos` y luego repair de `bronze_to_silver`** en `dllo`, para que aterricen
   los cambios del 2026-08-05: siete parámetros nuevos, desactivación del `5097`, `ALTER` de
   las cuatro columnas de R7, lista de conceptos corregida y fórmula de vuelta falsa. **Sin
   `crear_objetos` la carga falla**, porque `BY NAME` no encuentra las columnas nuevas.
2. **Ejecutar `notebooks/35_verificacion_oracle_7400027.py`.** Decide si la rama 4 tiene un
   problema o simplemente no hay datos para la población que carga la cadena. No tocar la query
   hasta tener ese resultado.
3. **Correr `32` y `34`** tras el paso 1. Esperado: 0 FALLA. En `34`, la comparación por *time
   travel* debería mostrar que `unidades_consumo_cobradas` **sube ~29%** por la entrada del
   concepto `545`.
4. **Actualizar `docs/contrato_silver.md`**: 49 columnas en `features_consumo`, sección de R7,
   lista de conceptos `87/90/545/546`, y la sección de columnas en `NULL` (hoy solo queda
   `flag_vuelta_falsa`).
5. **Llevar a negocio** cuatro preguntas abiertas: si la energía **reactiva** entra a las
   unidades de consumo (parámetro `conceptos_consumo_reactivo` sembrado **inactivo**); si el
   concepto `899` (sin legalizar) debe contar como consumo base; el umbral de vuelta falsa
   **con la fórmula nueva**; y el reparto de `delta_valor_pct` por tipo, que ya es viable
   porque el 96,9% de las cuentas tiene más de un concepto.
6. **Deuda de Prioridad 2**, en este orden: PK compuestas —empezando por `consumos_contrato` e
   `investigacion_consumo`, que **aún no existen en ningún ambiente** y corregirlas ahora cuesta
   cero—, proyectar `alfanumerica` en `QUERY_DATOS_LECTURA`, `job_name` en el DDL de
   `midas_log_cargas`, y consumir `midas_parametros` desde las queries de Oracle.
7. **Identificar al segundo escritor** de `midas_datos_detalle_solicitudes_silver`. El camino
   está: `DESCRIBE HISTORY` trae `userName`. Es conversación pendiente con el dueño del modelo
   legacy.
8. **Decidir sobre la PII en la historia de git.** No es de ingeniería: requiere seguridad y
   legal. El alcance, el comando y las consecuencias están en
   `docs/INCIDENTE_2026-08-03_pii_en_notebook.md`.
9. **Una corrida verde desde el bundle desplegado**, no solo desde el Git folder, antes de
   promover a `uat`. Hoy se valida una ruta y se promueve otra.
