# Contrato de la capa Silver

Qué publica Silver, con qué grano, qué significa cada objeto y **qué no garantiza**.
Ciencia de datos y el agente leen **Silver**, nunca Bronze.

Estado: `feature/midas_data_platform`, Silver Caso 2 v1.
Ambiente de referencia: `dllo`, DBR 16.4 (igual que `uat` y `pdn`).

---

## 0. Reglas que aplican a toda la capa

| # | Regla | Por qué |
|---|---|---|
| I14 | Silver publica **medidas**, nunca veredictos | No hay `cierre_sugerido` ni `requiere_ajuste`. El veredicto es del agente, y estas tablas son el *ground truth* con el que se le mide: el evaluador no puede ser parte de lo evaluado |
| I15 | El número de actividad aparece en **un solo objeto**: la vista de Nivel 2 | Un tercer caso de uso se resuelve agregando otra vista, sin tocar nada existente |
| I17 | Cero umbrales cableados | Todo umbral vive en `midas_parametros`. Cambiarlo es un `UPDATE`, no un despliegue |
| I19 | Nunca comparar contra el texto de una descripción | Se compara contra el código. Las descripciones cambian sin aviso |
| I11 | Las dimensiones se resuelven **inline** en Bronze | No hay tablas de dimensión que mantener sincronizadas |

**Un umbral sin confirmar produce `NULL`, nunca un número inventado.** El parámetro se siembra
con `activo=false`, el resolver inyecta un centinela y `TRY_CAST` lo convierte en `NULL`. Ver §5.

Toda tabla lleva `run_id` y `fecha_carga_silver`. Toda carga deja una fila en `midas_log_cargas`
con `job_name='midas_silver'`.

---

## 1. Nivel 2 — el punto de entrada

### `midas_ordenes_variacion_consumo_silver` · vista · grano: orden

Roster de órdenes **abiertas** de variación significativa de consumo, con el contexto comercial
del servicio. Es por donde arranca el agente.

El código de actividad se lee de `midas_parametros` (`orden` / `actividad_variacion_consumo` =
`993`). Es el **único** objeto de la capa que filtra por actividad.

- **Garantiza:** la orden está abierta (Bronze ya excluye estado 12 y las legalizadas).
- **No garantiza:** que exista historial de consumo para ese servicio. Un servicio nuevo puede
  tener orden y no tener periodos.
- **No prioriza y no puntúa.** Entrega el roster; el orden lo decide el agente.

---

## 2. Nivel 1 — tablas de hechos

### `midas_historial_consumo_silver` · tabla
**Grano: `(servicio_suscrito, id_periodo_consumo, tipo_consumo_cod, medidor)`** · PK declarada

La serie de consumo. La **espina dorsal son las lecturas**, no los consumos — ver
[ADR 0002](adr/0002-grano-historial-consumo.md) para el porqué.

- `medidor` usa el centinela `'(sin medidor)'` cuando Oracle no lo trae, y `medidor_desconocido`
  lo marca explícitamente para que nadie tenga que comparar contra la cadena. No es cosmético:
  la PK exige `NOT NULL`, y en Unity Catalog el `NOT NULL` **sí se hace cumplir** (la PK no; es
  informativa).
- `constante_efectiva` **no es** la constante declarada del medidor. Es
  `consumo_calculado / (lectura_actual − lectura_anterior)`, que reconstruye algebraicamente el
  factor de medida `leemfame` que Bronze no proyecta. Son dos cosas distintas y la comparación
  entre ambas es justamente la señal del Caso 23a.
- Los atributos del **acto de cálculo** (`calificacion`, `funcion_calculo`) vienen al grano
  `(SS, periodo, tipo)` y se replican en cada medidor: no son propiedad del medidor. El método
  de cálculo no es columna: es el **filtro** `metodo = 4` que define qué entra a la tabla.
- Cuando un atributo es **ambiguo** dentro del grupo, la columna sale `NULL` y su contador
  (`n_calificaciones`, `n_funciones_calculo`) dice cuántos valores distintos había; para
  `calificacion` además se publica el array `calificaciones`. Un `NULL` aquí significa
  *"hubo más de uno"*, no *"no hay dato"*.

- **No garantiza:** que `consumo_facturado_periodo` cuadre con la suma por medidor. Existe
  `cuadra_consumo_periodo` justamente para medir cuándo no cuadra.

### `midas_historial_consumo_periodo_silver` · vista · grano: `(SS, periodo, tipo)`
Colapsa el medidor. Es la que se usa cuando el medidor no importa; evita que cada consumidor
reinvente el `GROUP BY`.

### `midas_historial_cargos_silver` · tabla · grano: línea de cargo
**Sin PK, deliberadamente.** Oracle no expone un identificador único de línea de cargo, y
declarar una PK falsa sería peor que no declarar ninguna.

Cinco banderas derivadas, todas con códigos parametrizados:

| Bandera | Qué marca |
|---|---|
| `es_facturacion_normal` | El cargo viene del programa de facturación normal. Su negación es el núcleo del Caso 17 |
| `es_pno` | Cargo de pérdida no operacional (causal + programa) |
| `documento_tiene_token_recuperacion` | Componente 1 de la recuperación |
| `periodo_consumo_difiere_de_cuenta` | Componente 2 de la recuperación |
| `es_recuperacion` | Las dos anteriores a la vez |

Los dos componentes se publican **por separado a propósito**: el agente necesita ver *por qué*
se marcó, no solo *que* se marcó.

`es_recuperacion` sale de un **parseo posicional** del separador en `documento_soporte`
(`CO-PR-202606-...`), nunca de un `LIKE '%PR%'`: el `LIKE` daría positivo con cualquier texto que
contenga esas dos letras en cualquier posición.

- **No garantiza:** que `es_pno` coincida con la Silver de PNO. Son **dos vías independientes**
  (§4). En `dllo`, 6 de 7 servicios con cargo de causal 74 tienen expediente. La coincidencia
  parcial es un hallazgo de negocio, no un defecto del modelo.

### `midas_features_consumo_silver` · tabla
**Grano: `(servicio_suscrito, id_periodo_consumo, tipo_consumo_cod)`** · PK declarada · 44 columnas

Siete de las ocho reglas duras del Caso 2. La octava (macromedidor contra suma de vecinos)
depende de GDE y está fuera de alcance.

| Regla | Casos | Qué publica |
|---|---|---|
| R1 | 3, 4 | Cambio de medidor: **cuatro medidas complementarias**, no una bandera |
| R2 | 17 | Otros cobros: cargos de programa anormal, deltas de valor y de unidades |
| R3a | 23a | Constante efectiva y si **difiere entre activa y reactiva** del mismo periodo |
| R3b | 23b | Vuelta falsa: ratio contra el rango del medidor |
| R4 | 9 | Lecturas decrecientes y longitud de la racha |
| R5 | 18 | Días desde la reconexión, e intersección de solicitudes con el periodo |
| R6 | 11, 15 | Desviación contra el promedio de periodos previos |
| R8 | 5, 11, 15 | Marca de investigación |

**R1 no se puede resolver con una sola señal.** La serie del medidor no siempre se actualiza en
Oracle, así que `medidor_cambio_detectado_por_serie = false` **no descarta** el cambio. Por eso
se publican cuatro medidas y el array de medidores: el agente las combina.

**R6 — desviación conocida.** El promedio usa el frame
`6 PRECEDING AND 1 PRECEDING` con los periodos inválidos enmascarados a `NULL` (`AVG` los
ignora). Cuando los 6 son válidos, promedia **6**, no los 5 más recientes que pide la
especificación. `n_periodos_usados_en_promedio` se publica para que el cálculo sea auditable.

---

## 3. Nivel 1 — pasarelas

Sobre Bronze, **sin filtros**, con columnas explícitas (nunca `SELECT *`). Existen para que ningún
consumidor toque Bronze. Tres son vistas con comentario por columna; la cuarta
(`detalle_solicitudes`) es una **tabla adoptada** y se rige por otras reglas — ver abajo.

| Vista | Grano | Nota |
|---|---|---|
| `midas_datos_servicios_contrato_silver` | servicio suscrito | Roster por contrato: los servicios hermanos. `esta_activo` se deriva de `fecha_retiro` con el comodín `31/12/4732` |
| `midas_datos_investigacion_consumo_silver` | (SS, periodo, tipo) | El estado va **crudo** |
| `midas_datos_perdidas_no_operacionales_silver` | expediente PNO | Sin ventana temporal |

### `midas_datos_detalle_solicitudes_silver` · **tabla adoptada** · grano: solicitud

Trámites radicados. Es el único objeto de la capa que **no definimos nosotros**: la tabla ya
existía, tiene consumidores propios, y **su schema manda**. Nuestro pipeline solo refresca su
contenido desde Bronze — 11 columnas, reflejo 1:1 de su Bronze, que también fue adoptada en su
momento por la misma razón.

Consecuencias de adoptarla, que importan a quien la consuma:
- **No tiene `tipo_solicitud_cod`.** Esa derivación era nuestra cuando el objeto era una vista;
  ahora vive en `midas_features_consumo_silver`, el único que la necesitaba. Si vas a comparar
  tipos de solicitud, extrae el código con `REGEXP_EXTRACT`, nunca con `SPLIT` (devuelve cadena
  vacía con códigos negativos).
- **No tiene `run_id` ni `fecha_carga_silver`.** La trazabilidad de su carga sigue en
  `midas_log_cargas`, igual que la de todos los demás objetos.
- **No lleva DDL en el repo, y es a propósito.** Un `CREATE TABLE IF NOT EXISTS` contra una tabla
  que ya existe no hace nada y no falla: dejaría creer que el contrato es el nuestro cuando no lo
  es.
- Carga con `INSERT OVERWRITE ... BY NAME`. Si su dueño le agrega o quita una columna, **la carga
  falla**. Es lo correcto: la forma posicional escribiría los valores corridos sin avisar.

**El estado de investigación no se filtra, y es a propósito.** `2 = IMPUTABLE AL CLIENTE` y
`3 = IMPUTABLE A LA EMPRESA` son **resoluciones, no cierres**. Un supuesto previo del proyecto
daba el `2` por terminal y era falso; filtrar aquí congelaría esa confusión dentro de la capa.

---

## 4. Señales con más de una fuente

Tres señales tienen varias fuentes independientes. **Ninguna reemplaza a las otras**, y el
modelo no elige por el agente.

**Investigación** — tres fuentes:
1. `midas_datos_investigacion_consumo_silver` (el registro del proceso),
2. la solicitud tipo `100207` (el trámite radicado),
3. `funcion_calculo` / `calificacion` en el propio consumo.

La tercera es **la más fiable**: vive en la fila del consumo y no requiere join.

**PNO** — dos vías: `es_pno` en cargos **detecta**; la Silver de PNO es el **expediente** (qué
irregularidad, en qué ventana).

**Cambio de medidor** — las cuatro medidas de R1.

---

## 5. Lo que hoy sale `NULL` y por qué

Cuatro columnas salen `NULL` en toda la tabla hasta que negocio confirme el parámetro:

| Columna | Parámetro sin confirmar |
|---|---|
| `flag_vuelta_falsa` | `tolerancia_vuelta_falsa` |
| `fecha_ultima_reconexion`, `dias_desde_reconexion` | `tipo_solicitud_reconexion` |
| `solicitud_reconexion_intersecta_periodo` | `tipo_solicitud_reconexion` |
| `solicitud_suspension_intersecta_periodo` | `tipo_solicitud_suspension` |

Sobre los tipos de solicitud: el plan original los sembraba invertidos
(`suspension=300`, `reconexion=56`). Según el diccionario del proyecto es al revés —
**`300` = Reconexión por Pago**, **`56` = Suspensión por no Pago**. Se sembraron **corregidos**
y **inactivos**: preferimos un `NULL` honesto a un número calculado con el código equivocado.

Estos `NULL` **no son un defecto**. Se activan con un `UPDATE` a `midas_parametros`; no requieren
despliegue ni cambio de código.

---

## 6. `PENDIENTE-NEG` — preguntas abiertas para negocio

1. **`delta_valor_pct` está a un grano distinto al de su fuente.** La cuenta de cobro es por
   `(SS, periodo)`, **sin tipo de consumo**, así que el valor **se repite** en activa y reactiva.
   ¿Debe repartirse por tipo usando el concepto del cargo?
2. **`tolerancia_vuelta_falsa`** — qué ratio se considera vuelta falsa.
3. **Tipos de solicitud de suspensión y reconexión** — confirmar `300` y `56`.
4. **El concepto `87`** como cargo de consumo normal es **inferencia sobre capturas de pantalla**,
   no dato confirmado.
5. **`estado_pno`** va crudo: no se verificó si tiene catálogo asociado. Si lo tuviera, se
   resolvería inline en Bronze (I11), no en Silver.

---

## 7. Legacy del Caso 1

Las cuatro Silver del Caso 1 conservan su patrón `CREATE OR REPLACE TABLE` y su SQL verbatim.
Ahora se orquestan y se loguean como el resto, pero **no se reescribieron**: no era el alcance.

El único cambio de contenido es aditivo (I13): `estado_corte_facturable` y
`estado_corte_facturable_desc` **al final** de la proyección de
`midas_ordenes_calidad_pendientes_silver`. Salen del `LEFT JOIN` que ya existía, así que el
**conteo de filas no cambia**.

`midas_historial_critica_silver` **cambió de contenido** con la rama 4 de la v3: ahora mezcla las
actividades `102010`, `7400027` y `1611/1613/1677/...` sin filtrar. Se documenta, no se toca.
