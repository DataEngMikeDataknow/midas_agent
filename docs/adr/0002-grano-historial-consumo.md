# ADR 0002 — Las lecturas son la espina dorsal de `midas_historial_consumo_silver`

- **Estado:** aceptado
- **Fecha:** 2026-07-30
- **Contexto:** Silver Caso 2 v1, rama `feature/midas_data_platform`
- **Relacionado:** [ADR 0001](0001-bundles-separados-vera-midas.md), [contrato_silver.md](../contrato_silver.md)

## Contexto

El plan de Silver pedía `midas_historial_consumo_silver` al grano
`(servicio_suscrito, id_periodo_consumo, tipo_consumo, medidor)`, construido como
`lecturas ⋈ consumos`.

Al revisar el código real de Bronze, ese join no es posible:

1. **`midas_datos_consumos_producto_bronze` no tiene columna `medidor`.** En
   `QUERY_DATOS_CONSUMOS`, `cosselme` aparece **solo en el `ORDER BY`**; nunca se proyecta.

2. **Agregarla tampoco resuelve.** `lecturas.medidor` es `elmecodi` (el código visible) mientras
   `cosselme` es `elmeidem` (el id interno). No son joinables sin otra subconsulta escalar.

3. **La ambigüedad persistiría igual.** El CTE de lecturas hace `SUM(cosscoca)` agrupando por
   `(cosspecs, cosselme, cosstcon)` — es decir, **ya se sabía** que puede haber varias filas por
   `(periodo, medidor, tipo)`.

Al mismo tiempo, ese `SUM(cosscoca)` con `cossmecc = 4` significa que **lecturas ya entrega el
consumo al grano de medidor**. La pieza que faltaba ya estaba ahí.

## Decisión

**Lecturas es la espina dorsal.** Consumos entra **agregado** al grano
`(servicio_suscrito, id_periodo_consumo, tipo_consumo_cod)`, aportando `metodo_calculo`,
`calificacion` y `funcion_calculo`.

Esos tres son atributos del **acto de cálculo del periodo**, no propiedades del medidor. Que se
repitan en cada medidor del periodo es semánticamente correcto, no una duplicación.

`medidor` usa el centinela `'(sin medidor)'` cuando Oracle no lo trae.

### Desambiguación sin `FIRST()`

Cuando el grupo tiene más de un valor para un atributo, la columna sale `NULL` y un contador
`n_<atributo>` publica cuántos había. Nunca se usa `FIRST()`.

`FIRST()` en un grupo ambiguo devuelve un valor **arbitrario y no determinista** que se presenta
como si fuera el dato. Es peor que un `NULL`: el `NULL` es auditable y honesto, y aquí significa
específicamente *"hubo más de uno"*, no *"no hay dato"*.

## Verificación previa a fijar la PK

Medido en `dllo` sobre `midas_datos_lecturas_producto_bronze`:

| Medición | Resultado |
|---|---|
| Filas | 3.180 |
| Combinaciones distintas de `(SS, periodo, tipo, medidor)` | 3.180 |
| Duplicados | 0 |
| Filas con `medidor` nulo | 757 |

Un detalle que se prestó a confusión: se creyó que las 757 filas con `medidor` nulo podían romper
la unicidad al colapsarlas en el centinela. No es así — **Spark `GROUP BY` ya agrupa los `NULL`
entre sí**, así que el conteo de 3.180 combinaciones distintas *ya incluía* ese colapso. La prueba
que se corrió ya demostraba que el centinela preserva la unicidad.

La razón real por la que el centinela hace falta es otra: la PK exige `NOT NULL`, y en Unity
Catalog **el `NOT NULL` sí se hace cumplir** aunque la PK sea solo informativa.

## Consecuencias

**A favor**

- El grano de medidor sale de datos que **ya existen**, sin tocar Bronze ni las queries de Oracle.
- `constante_efectiva` reconstruye algebraicamente el factor de medida `leemfame`, que Bronze no
  proyecta. Es la señal del Caso 23a, y se consigue gratis.
- La vista `midas_historial_consumo_periodo_silver` cubre a quien no necesita el medidor.

**En contra**

- `consumo_facturado_periodo` (del lado de consumos) puede no cuadrar con la suma por medidor. Se
  publica `cuadra_consumo_periodo` para medirlo en vez de esconderlo.
- Si Oracle empezara a traer `elmecodi` en consumos, este ADR debería revisarse.

## El *seam*

El punto exacto donde cambiaría la decisión: **el día que `QUERY_DATOS_CONSUMOS` proyecte
`elmecodi`** (no `elmeidem`), el join directo pasa a ser viable y el CTE `consumos_periodo` de
`load/midas_historial_consumo_silver.sql` puede dejar de agregar.

Nada fuera de ese CTE depende de la decisión: el DDL, la PK y todos los consumidores siguen
iguales.

## Alternativas descartadas

- **Consumos como espina dorsal** — pierde el medidor por completo, y con él R1, R3a y R3b.
- **Agregar `elmeidem` a Bronze y resolver el join con una subconsulta escalar** — más carga sobre
  Oracle, y no elimina la ambigüedad del punto 3.
- **`FIRST()` para desambiguar** — descartado arriba.
