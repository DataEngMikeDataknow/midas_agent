## Handoff — 2026-08-05

**Qué se tocó:** `notebooks/00_creacion_objetos_midas.py` (parámetros y migraciones),
`src/midas/db/queries.py` (`estado_pno` inline),
`src/midas/sql/silver/{ddl,load}/midas_features_consumo_silver.sql` (R7 nueva y R3b
corregida), `src/midas/sql/silver/view/midas_datos_perdidas_no_operacionales_silver.sql`,
y `notebooks/35_verificacion_oracle_7400027.py` (nuevo).

**Qué cambió de fondo:** Las respuestas de negocio del 2026-08-05 desbloquearon siete
parámetros que estaban inferidos o apagados, pero el catálogo completo de Oracle reveló que
**tres definiciones que dábamos por buenas estaban mal**: faltaba el concepto de consumo más
grande después de los dos conocidos, un parámetro activo apuntaba a un código inexistente, y
la fórmula de vuelta falsa medía contra el rango del medidor en vez de contra la vuelta.

**¿Esto revierte o reemplaza algo anterior?** Sí, tres cosas. (1) La lista
`conceptos_consumo_medido` pasó de `87,90,546,550,552` a `87,90,545,546`. (2)
`calificacion_medidor_conforme = 5097` se retira y se sustituye por
`calificacion_medidor_no_conforme = 5091`. (3) `ratio_vuelta_falsa` cambia de fórmula.

**¿Necesita un ADR nuevo?** No. Son correcciones de definición dentro de invariantes ya
establecidos (I11 catálogos inline, I17 cero umbrales cableados).

**Queda abierto:** correr el notebook `35` contra Oracle para decidir si la rama 4 de
`7400027` tiene un problema o simplemente no hay datos; llevar a negocio la separación
activa/reactiva y el tratamiento de la reactiva; y la deuda de Prioridad 2 (PK compuestas,
`alfanumerica`, `job_name` en el log).

---

## Lo que contradice el prompt de entrada

Se pidió verificar antes de actuar. Tres puntos no eran como se describían.

### `7400027` YA estaba implementado

El prompt dice *"sigue sin aplicarse"*. La **rama 4 existe** desde la v3 en
`queries.py:301-344`, con `oa.activity_id = 7400027`, y tiene el `drop_duplicates` que
corrigió un bug que **solo aparece si la rama devuelve filas**.

Lo cierto es que Bronze no tiene ninguna: 779 filas en seis actividades, ninguna es la de
decisión. Que la rama exista y no traiga nada es un problema distinto —y con dos causas
posibles— así que **no se tocó la query**. Ver la sección de verificación.

### El punto 1.5 parte de una premisa falsa

El prompt dice que *"la dimensión `midas_dim_estado_corte_facturable_bronze` ya la
materializa"*. **Esa dimensión se retiró en la v3** por el invariante I11, y el inventario
del 2026-08-04 confirmó que ya no existe físicamente.

Más importante: **la discriminación por servicio ya está resuelta**. La resolución inline en
`QUERY_DATOS_BASICOS` consulta `confesco` por **ambas** claves —`coeccodi` (estado) y
`coecserv` (servicio)—, así que el caso del `96` exclusivo de energía se resuelve solo. Y
**no existe ninguna lista `NOT IN` plana** en el SQL de la plataforma: se buscó en todo el
árbol.

Lo que Silver expone hoy y basta para ese gate: `servicio`, `estado_corte`,
`estado_corte_facturable`, `estado_corte_facturable_desc` en la vista de Nivel 2, más
`fecha_retiro`, `fecha_retiro_es_comodin` y `esta_activo` en el roster de contrato — que es
justo lo que separa *no facturable* de *no analizable*. Se confirmó con el usuario que el
gate vive en el bundle del agente y queda **fuera de alcance**.

### `calificacion_medidor_conforme` apuntaba al vacío

Estaba **activo** con valor `5097`. El catálogo real de `calificacion` tiene 31 valores y
**no contiene el 5097**. Lo que existe es `5091-MEDIDOR NO CONFORME CALIBRACION`, el opuesto
semántico. Cualquier comparación contra ese parámetro daba siempre falso, sin error.

---

## El hallazgo con más impacto: faltaba un concepto de consumo

La lista era `87, 90, 546, 550, 552`. Contra el catálogo **completo** de 143 conceptos:

| Concepto | Unidades | Estado |
|---|---|---|
| `545-CONS ENERGÍA ACTIV FUERA PUNTA` | **992.655** | **Faltaba.** 4º por valor ($693M) |
| `550`, `552` (agua potable/residual) | — | **No existen** en los datos |
| `547`, `548` (reactiva) | 65.409 | Fuera, a decisión de negocio |

**Por qué se escapó:** la consulta que armó la lista pedía conceptos *"cuya descripción
contenga la palabra CONSUMO"*. El `545` dice **`CONS ENERGÍA`**, abreviado. Un filtro por
patrón de texto se comió el segundo concepto más grande en unidades.

Es el mismo patrón que dejó el handoff del 04: **un proxy cómodo tomado por la medida real**.
Ahí fue `last_altered` por frescura; aquí fue "contiene la palabra CONSUMO" por "es consumo".

La lista quedó en `87, 90, 545, 546` y `unidades_consumo_cobradas` sube ~29%. Los reactivos
se sembraron en un parámetro aparte **inactivo**: si la reactiva entra o no a las unidades de
consumo es decisión de negocio, no técnica.

---

## La fórmula de vuelta falsa medía otra cosa

Era `consumo_facturado / 10^dígitos`, que solo tiene sentido si la lectura anterior fuera
cero. El caso extremo de `dllo` lo muestra:

> **SS 94896179** — lectura `26.483 → 43`, medidor de 5 dígitos.
> Vuelta completa = `100.000 − 26.483 + 43` = **73.560**. Facturaron **6.735**, o sea el
> sistema **sí la manejó bien**. La fórmula vieja daba `6.735 / 100.000` = **0,067**.

Por eso las 44 filas con consumo negativo tenían todas ratio menor a 0,067 y **ningún umbral
razonable detectaba nada**. No es que no hubiera casos: es que se medía contra el rango del
medidor en vez de contra la vuelta.

Ahora es `consumo_facturado / (10^dígitos − lectura_anterior + lectura_actual)`, y solo se
calcula cuando la lectura **retrocedió**. Cerca de 1 significa *"cobraron la vuelta entera"*,
que es el error a detectar. El umbral sigue **inactivo**: con la fórmula corregida hay que
volver a mirar la distribución antes de fijar el corte.

---

## R7 — el reencuadre de la "tolerancia"

Negocio respondió que **no es un porcentaje**. El criterio real es si **ese mismo servicio ya
tuvo consumos por encima de los límites en el pasado**. La tolerancia no es un margen sobre
el límite de este periodo: es **precedente del propio servicio**.

Se implementó `tuvo_consumo_alto_historico` sobre `UNBOUNDED PRECEDING AND 1 PRECEDING` —
todo el historial previo, excluyendo el periodo en análisis para que la feature no se responda
a sí misma. Se publican además `n_periodos_previos_con_limite`, `limite_superior` y
`consumo_supera_limite_actual`, porque superar el límite **teniendo** precedente pesa distinto
que superarlo por primera vez.

**El `NULL` importa aquí.** El **29,7%** de las filas (898 nulas + 45 en cero de 3.180) no
tiene límite superior usable. Para esas, *"no tuvo consumo alto"* sería un negativo
**fabricado**: la verdad es que no se puede saber. La feature sale `NULL` y el contador dice
sobre cuánta historia se evaluó.

---

## Parámetros: de inferidos a confirmados

Los **7 códigos** de `package_type_id` quedan verificados contra el catálogo real (14 valores),
no inferidos de videos:

`15` retiro por no pago · `42` reinstalación · `56` suspensión · `288` gestión PNO ·
`289` aprobación de ajustes · `300` reconexión · `100207` investigación

También: `calificacion_normal = 1` (confirmado: es el único valor normal, 3.828 filas) y
`factor_reactiva = 0.5` (confirmado sin cambios). Y `estado_pno` se resuelve **inline con un
`CASE`** —no hay tabla catálogo en Oracle para ese campo— con los cinco valores R/E/F/N/P.

> Detalle de oficio: la columna nueva va **al final** de la proyección. `insertInto` es
> posicional, y ponerla junto a `estado_pno` habría corrido todos los valores siguientes
> **sin lanzar error**.

---

## Lo que NO se tocó, y por qué

| Punto | Razón |
|---|---|
| La query de `7400027` | La rama existe. Antes de cambiar una query del Caso 1 hay que saber si el problema es el filtro de fecha o que no hay datos. Para eso está el notebook `35` |
| El gate de estado de corte | Vive en el bundle del agente. Silver ya expone todo lo necesario |
| Separar activa/reactiva en `delta_valor_pct` | El catálogo da el mapeo (`90/545/546` activa, `93/547/548` reactiva) y el reparto es viable, pero **cambia el significado de columnas ya publicadas**. Decisión del usuario: va a negocio primero |
| Prioridad 2 completa | No alcanzó el tiempo. Ninguna está bloqueada |

---

## Verificación pendiente: el notebook `35`

Separa las dos hipótesis sobre `7400027` sin tocar la query:

- **Paso 3** — ¿existen órdenes 7400027 para los servicios que carga la cadena, **sin** filtro
  de fecha? Si da cero, la lógica está bien y no hay nada que arreglar: para ver órdenes de
  decisión habría que ampliar la población de servicios, que es decisión de alcance.
- **Paso 4** — si existen, ¿cuántas caen dentro de las ventanas de periodo? Si ninguna, el
  filtro las excluye todas y el arreglo **no** es quitar la ventana —volvería a duplicar la
  orden en cada iteración— sino relacionarla por la investigación que resuelve. Es cambio de
  diseño.
- **Paso 5** — el universo completo en Oracle, para dimensionar.

---

## Estado

**137 tests pasan**, 1 deselección preexistente. Los 16 archivos SQL resuelven sus
placeholders. `features_consumo` pasa de 45 a **49 columnas**, todas con su migración
guardada y con el comentario anclado contra el DDL por un test.

Para correr: commit + push + pull → **`crear_objetos`** (siembra los parámetros nuevos,
desactiva el `5097` y aplica los `ALTER`) → **repair de `bronze_to_silver`**. La extracción de
Oracle solo hace falta si se quiere el `estado_pno_desc` poblado.
