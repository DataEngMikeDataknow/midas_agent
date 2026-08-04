## Handoff — 2026-08-03

**Qué se tocó:** `src/midas/db/database.py`, `src/midas/db/processing.py`,
`src/midas/main_ingestion.py`, `src/midas/silver_params.py`,
`src/midas/sql/silver/{load,ddl,view}/*.sql`, `notebooks/00_creacion_objetos_midas.py`,
`notebooks/32_validacion_silver_caso2.py`, `notebooks/33_recorrido_silver_caso2.py`,
`notebooks/check_conectividad_oracle.py`, `notebooks/91_explorador_modelo_bronze_caso2.ipynb`,
`databricks.yml`, `pipeline/deploy-bundle.yml`, `scripts/check_notebook_outputs.py`,
`.pre-commit-config.yaml`, `tests/*`, y ocho documentos de `docs/`.
Commits `875429c`, `1fff309`, `57a5b02`.

**Qué cambió de fondo:** La capa Silver dejó de publicar valores fabricados y empezó a
medir lo que dice medir: cuatro columnas que afirmaban `false` con un parámetro sin
confirmar ahora salen `NULL`, y las unidades de consumo pasaron de sumar el 99% de las
líneas de cargo a sumar solo los conceptos que de verdad son consumo. En paralelo, el
pipeline dejó de poder terminar en verde con datos de ayer: un fallo de Oracle ahora falla.

**¿Esto revierte o reemplaza algo anterior?** Sí, tres cosas. (1) `unidades_consumo_cobradas`
filtraba por `causal_cod = -1`, que los datos mostraron ser el 99% de las líneas: se
reemplazó por un filtro por concepto. (2) `execute_query` devolvía un DataFrame vacío ante
un error; ahora lanza, y el test que **exigía** ese comportamiento se invirtió. (3) Los
parámetros `tipo_solicitud_reconexion` y `tipo_solicitud_suspension` pasaron de inactivos a
activos, porque el propio dato de Bronze confirmó los códigos.

**¿Necesita un ADR nuevo?** No. Las decisiones de hoy son correcciones dentro de
invariantes ya establecidos (I14 medidas no veredictos, I17 cero umbrales cableados) o
respuestas a hallazgos de auditoría. El `ADR 0002` sigue vigente y de hecho quedó
respaldado: el cuadre de consumo dio **100,00%**.

**Queda abierto:** la reescritura de la historia de git por la PII (decisión del dueño del
repo, no ejecutada); pinear dependencias (`F07`, requiere corrida en UAT); verificar en UAT
las propiedades de timeout JDBC; identificar el segundo proceso que escribe
`midas_datos_detalle_solicitudes_silver`; y una corrida verde desde el **bundle desplegado**
—hoy se itera desde el Git folder— antes de promover a uat.

---

## Detalle por bloque

### 1. Cierre del Caso 2 con datos, no con reuniones

Genie respondió sobre el catálogo real y **cerró cuatro brechas**:

| Pregunta | Respuesta |
|---|---|
| ¿300 es reconexión o suspensión? | `300 - Reconexión por Pago` (231 filas), `56 - Suspensión por no Pago` (269) |
| ¿Existe el tipo de investigación? | `100207 - Solicitud de Investigación de Consumos` (221 filas) |
| ¿El 18,76% de cargos sin periodo es una brecha? | No: son diferidos, intereses y trabajos, que por naturaleza no cuelgan de un periodo |
| ¿`estado_pno` tiene catálogo? | Un solo valor, `F`, crudo. Hoy no discrimina nada |

La respuesta llevaba meses dentro de nuestra propia tabla: Bronze resuelve
código-descripción inline (I11) y nunca miramos la descripción. Los tres parámetros se
activaron, lo que **encendió las cuatro columnas de R5**. Verificado con datos: 175
periodos con reconexión intersectando, 201 con suspensión, 640 con fecha de reconexión.

### 2. Tres columnas mentían, y el notebook 32 las cazó

`flag_vuelta_falsa` traía 3.108 valores no nulos y las dos `*_intersecta_periodo` traían
3.152. Debían estar vacías. La causa es lógica ternaria de SQL en sus dos caras:

- `CASE WHEN <cond NULL> THEN true ELSE false END` devuelve **`false`**, no `NULL`.
- `false AND NULL` devuelve **`false`**, no `NULL`.

Con el parámetro sin confirmar, ambas construcciones **fabricaban un negativo**. La columna
afirmaba *"no hubo reconexión en el periodo"* cuando la verdad era *"no lo sabemos"*. Un
agente lo habría leído como hecho confirmado y descartado una explicación posiblemente
correcta. Corregido con guarda explícita sobre el parámetro en las tres.

### 3. Las unidades de consumo no medían consumo

`unidades_consumo_cobradas` filtraba por `causal_cod = -1`. Los datos mostraron que esa
causal es **21.596 de 21.844 líneas (99%)**: no aislaba el consumo, solo excluía las
causales especiales. Sumaba unidades de conceptos ni siquiera comparables entre sí.

Ahora filtra por concepto: `87, 90, 546, 550, 552` — sin IVA, activa, activa punta, agua
potable y residual. Quedaron fuera los nueve derivados (contribuciones, subsidio, cuotas).

Requirió infraestructura nueva: un tipo de parámetro **`INT_LIST`** (una cláusula `IN (...)`
no admite subconsulta ni bind, es concatenación literal, así que cada elemento pasa por
`int()`), y un bloque **`_MIGRACION_SILVER`** — el DDL de Silver es
`CREATE TABLE IF NOT EXISTS`, que contra una tabla existente no hace nada, y la carga usa
`BY NAME`, que falla si el SELECT trae una columna que la tabla no tiene.

### 4. El comentario que la migración no aplicaba

La primera versión de `_MIGRACION_SILVER` agregaba la columna **sin `COMMENT`**, y
re-ejecutar `crear_objetos` no lo arreglaba: decidía por existencia de columna, la columna
ya existía, y saltaba. Ahora es idempotente **sobre el comentario**, no solo sobre la
columna, con un test que compara el texto contra el DDL carácter por carácter.

### 5. Hallazgos de la auditoría externa (21 findings)

Se priorizaron por severidad × implementable hoy × no desestabilizar lo recién estabilizado.

**`F02` (Critical) — un fallo de extracción publicaba datos de ayer.** La cadena:
`execute_query` tragaba la excepción y devolvía vacío → `save_to_parquet` no escribía → el
Parquet del día anterior sobrevivía → `chain_runner` registraba EXITOSO → la ingesta
cargaba los datos de ayer. **Este bug ya se había manifestado**: los 782 "huérfanos" de
`servicios_contrato` que en la v3 atribuimos a *"residuo de corridas anteriores"* eran esto.
Quitamos la tabla y nunca arreglamos el mecanismo.

Roto en tres puntos: `execute_query` lanza; `save_to_parquet` escribe los vacíos con
columnas y lanza ante fallo de escritura; y una **guarda de frescura** en `main_ingestion`
que aborta antes de tocar Bronze si algún Parquet no es de esta corrida. La guarda es el
atajo que cubre todas las rutas sin parchear los 12 extractores.

**`F01` (Critical) — PII en outputs de notebook.** 768 KB con nombres, identificaciones,
direcciones y saldos de clientes reales. Limpiado a 195 KB con las 91 celdas de código
intactas, más un script de chequeo, hook de pre-commit y un stage `Calidad` del que ahora
dependen los tres despliegues. **La historia de git sigue conteniendo los datos**; el
alcance y el comando están en `docs/INCIDENTE_2026-08-03_pii_en_notebook.md`.

**`F09`** — timeout y reintentos en los 14 tasks de los 3 targets, más
`CONNECT_TIMEOUT`/`ReadTimeout` en la conexión JDBC. **`F20`** — el `%pip` crudo rompía
`check_conectividad_oracle` con `SyntaxError`: el job de diagnóstico no arrancaba justo
cuando hacía falta. **`F21`** — conteos obsoletos en ocho lugares de seis documentos.

**Descartados con razón:** `F04` (parcialmente obsoleto tras el fix de concepto; lo que
queda está bloqueado por negocio), `F05` (decisión previa §7.2: el patrón legacy no se
toca), `F03` (Large, reestructura lo recién estabilizado), `F16` (mal atribuido en parte:
2 de 4 apariciones son reales, y cambiarlas altera el contenido de Bronze del Caso 1).

### Estado

137 tests pasan, +14 respecto de ayer. Sigue el fallo preexistente
`test_ensure_schema_exists`, deseleccionado explícitamente en el pipeline para no dejar el
gate permanentemente rojo — con la nota de borrar el `--deselect` cuando alguien lo arregle.

---

## Puntos a validar con el negocio

### 1. El consumo sin legalizar, ¿es consumo?

El concepto `899 - CONSUMO ENERGÍA SIN LEGALIZAR` quedó **fuera** de
`unidades_consumo_cobradas` y se publica aparte, en `unidades_consumo_sin_legalizar`.

**Es un criterio nuestro, no un dato.** El razonamiento: es consumo irregular, típicamente
de recuperación; sumarlo a la línea base haría que un periodo con recuperación mostrara
unidades altas junto con valor alto, y el agente concluiría *"el aumento sí es consumo"* —
justo el falso negativo que el Caso 17 busca evitar. Hoy es una sola fila en `dllo`, así que
el impacto es nulo, pero conviene confirmarlo. Si se decide lo contrario, es un `UPDATE`.

### 2. Vuelta falsa: la pregunta cambió

Ya no es *"¿qué umbral ponemos?"*. De las 44 filas con consumo calculado negativo,
**todas** tienen `ratio_vuelta_falsa` por debajo de **0,067** (mediana 0,0007). Cualquier
umbral razonable detectaría **cero** casos.

La pregunta es: **¿no hay vueltas falsas en esta muestra, o la fórmula
`consumo_facturado / 10^dígitos` no captura lo que ustedes llaman vuelta falsa?** La columna
sigue apagada porque activarla daría un `false` en todas las filas que nadie podría validar.

### 3. `delta_valor_pct`: ahora sí se puede repartir por tipo

La cuenta de cobro es por servicio y periodo, sin distinguir activa de reactiva, así que hoy
ese porcentaje **se repite igual** en ambas. Los datos confirman que el reparto **es
viable**: el 96,9% de las cuentas (4.340 de 4.480) tiene más de un concepto. Falta el mapeo
concepto → tipo de consumo.

### 4. Los 15 conceptos de consumo: ¿está completa la lista?

Se tomaron cinco como consumo medido. Vale una confirmación de que no falta ninguno y de
que los nueve derivados —contribuciones, subsidio, cuotas de financiación— efectivamente no
deben contar como unidades de consumo.

### 5. `es_recuperacion` es hoy redundante

`documento_tiene_token_recuperacion` marca 476 líneas y `es_recuperacion` marca **476
también**: el segundo componente no descarta ni una sola fila dentro del conjunto del token.
No es un error —la conjunción sí excluye 911 cargos que difieren de periodo sin token— pero
significa que el token es hoy identificador suficiente por sí solo. ¿Es el criterio
correcto, o el segundo es una red que aún no ha tenido que actuar?

### 6. Pérdidas no operacionales: la coincidencia parcial

7 servicios tienen cargo de PNO, 10 tienen expediente, **6 están en ambos**. Es decir, hay
**un servicio con cargo pero sin expediente** y cuatro con expediente sin cargo. El modelo
lo trata como dos vías independientes a propósito, pero esa diferencia es una pregunta de
negocio, no un defecto.

### 7. El promedio de R6 usa 6 periodos, no 5

La regla pide los 5 periodos válidos más recientes mirando hasta 6 atrás; la implementación
promedia todos los válidos de la ventana. Ahora hay número: afecta a **705 de 3.152 filas
(22,4%)**. El documento de negocio ya acepta tolerancia aquí (en el caso de gas verificado
el promedio real era 28,34 y el analista trabajó con 30). Se publica
`n_periodos_usados_en_promedio` para que sea auditable fila por fila.

### 8. `estado_pno` no discrimina

Un único valor `F` en las 12 filas de `dllo`, sin descripción. ¿Existe catálogo? Si lo
hubiera, se resolvería inline en Bronze (I11), no en Silver.

### 9. Dos decisiones que no son de negocio pero necesitan dueño

- **La historia de git con PII.** Reescribirla invalida todo clon y obliga a resincronizar
  el Git folder de Databricks. Puede activar obligaciones de notificación bajo la política
  de datos personales de EPM — pregunta para seguridad y legal, no para ingeniería.
- **El segundo proceso que escribe `midas_datos_detalle_solicitudes_silver`.** Algo la
  actualizó el 31 de julio a las 14:01 y no fue nuestro pipeline. Dos escritores sobre el
  mismo objeto es un problema real; está pendiente la conversación con el dueño del legacy.
