## Handoff — 2026-08-04

**Qué se tocó:** `notebooks/34_validacion_corrida_endurecimiento.py` (tres chequeos
reescritos) y `Diagramas/Midas modelo silver caso 2.drawio` (cuatro correcciones de drift).
Ningún cambio en `src/`, en el pipeline ni en los datos.

**Qué cambió de fondo:** No cambió el sistema, cambió lo que sabemos medir. Tres de los
chequeos que escribí ayer estaban construidos sobre premisas falsas y produjeron cuatro
alarmas sobre una corrida que estaba sana. Un chequeo que da falsos positivos es peor que
no tenerlo: entrena a quien lo lee a ignorarlo.

**¿Esto revierte o reemplaza algo anterior?** Sí, tres chequeos del notebook `34`:
la comparación de versiones Delta, el filtro por fecha de la bitácora, y —el más
importante— el uso de `information_schema.tables.last_altered` como señal de frescura.

**¿Necesita un ADR nuevo?** No. Son correcciones de instrumentación, no decisiones de
arquitectura.

**Queda abierto:** correr el `34` corregido para confirmar 0 FALLA; identificar al segundo
escritor de la tabla adoptada (el chequeo nuevo ahora lo nombra); y todo lo que ya venía
del handoff del 2026-08-03.

---

## Los tres errores de instrumentación, y qué se aprende de cada uno

### 1. `last_altered` no mide frescura de datos

**El más importante.** `information_schema.tables.last_altered` registra cuándo se alteró
la **definición** de la tabla: schema, propiedades, comentarios. Un
`insertInto(overwrite=True)` reemplaza *todas* las filas sin tocar la definición, así que
el campo **no se mueve**.

Eso hizo que tres Bronze aparecieran con fechas de hace días. La bitácora mostraba que las
tres habían cargado `EXITOSO` esa misma noche —12, 119 y 7.084 filas— dos minutos antes de
que Silver corriera. Lo que el chequeo medía era **estabilidad de esquema**, y lo estaba
llamando frescura: las tablas que "parecían frescas" eran justamente las que habían
recibido un `ALTER` reciente.

**La señal correcta es `DESCRIBE HISTORY`**, el registro de transacciones de Delta. Viene
del storage, no del metastore, y registra escrituras de verdad. De paso trae la columna
`userName`, que responde directamente la pregunta abierta sobre el segundo escritor de la
tabla adoptada — mejor que inferirlo comparando timestamps.

> **La guarda de frescura del pipeline NO comparte este defecto.** Usa `os.path.getmtime`
> sobre el archivo Parquet, que es un timestamp real del sistema de archivos. Sigue siendo
> válida. Conviene tenerlo claro para no "arreglar" algo que está bien.

### 2. Comparar contra "la versión anterior" asume que solo hubo una corrida

Para verificar que el filtro por concepto había cambiado los datos, comparé la última
versión Delta contra la penúltima. Dieron idénticas y el chequeo marcó FALLA.

La causa: el job había corrido **varias veces desde el cambio**, así que las dos últimas
versiones eran ambas posteriores. El "antes" estaba más atrás en el historial.

Ahora se recorre **todo el historial** y se busca dónde salta el valor. El chequeo dejó de
depender de cuántas veces se haya corrido el job entre el cambio y la validación.

### 3. `current_date()` rompe cuando el job cruza la medianoche UTC

El bloque de bitácora filtraba por `to_date(fecha_inicio) = current_date()`. El job corrió
a las 23:29 UTC y la validación se hizo a las 02:48 UTC del día siguiente: cero registros,
y la apariencia de que el job nunca corrió.

No es un caso raro. Un job vespertino y una validación posterior **siempre** van a caer
así. Ahora se usa una ventana móvil anclada en la última carga registrada.

---

## El patrón detrás de los tres

Los tres errores son la misma equivocación con distinta ropa: **tomé un proxy conveniente
por la medida real**.

- `last_altered` era un proxy cómodo de "cuándo se escribió".
- "la versión anterior" era un proxy cómodo de "antes del cambio".
- `current_date()` era un proxy cómodo de "esta corrida".

Los tres funcionan en el caso feliz y fallan en cuanto la realidad se sale del guion:
varias corridas el mismo día, un job que cruza medianoche, una tabla que no cambia de
esquema. Y fallan **hacia el lado ruidoso** —marcan problemas que no existen—, que es
mejor que fallar en silencio, pero igual desgasta la confianza en el instrumento.

La regla que queda: **cuando un chequeo dependa de metadata, verificar qué mide esa
metadata antes de confiar en ella.** La documentación de `last_altered` dice exactamente lo
que hace; no la leí.

## Lo que sí funcionó a la primera

Vale registrarlo, porque el contraste enseña:

- **V4 y V5 dieron OK sin ajustes.** Los seis parámetros exactos, cero columnas sin
  comentario, y R5 con señal real: 175 / 201 / 640, clavado en lo esperado.
- Esos chequeos comparan **contra valores concretos esperados**, no contra proxies. Cuando
  el chequeo dice "esta columna debe tener ~175 positivos" no hay premisa oculta que se
  pueda romper.

La diferencia entre los chequeos que fallaron y los que no es exactamente esa: los que
fallaron infieren, los que funcionaron comparan.

---

## Diagrama Silver: cuatro correcciones de drift

`Diagramas/Midas modelo silver caso 2.drawio` se generó antes de los cambios del 3 de
agosto. Estructura intacta —13 cajas, 10 aristas, XML válido— pero con contenido vencido:

| Qué decía | Qué dice ahora | Por qué |
|---|---|---|
| `44 columnas` | `45 columnas` | Se agregó `unidades_consumo_sin_legalizar` |
| R2 sin las unidades | `unidades_consumo_cobradas` y `unidades_consumo_sin_legalizar`, en azul | Son el cambio de fondo del 3 de agosto: pasaron de filtrar por causal a filtrar por concepto |
| `fecha_ultima_reconexion` y `dias_desde_reconexion` **en gris** | En negro, más `solicitud_reconexion_intersecta_periodo` | El gris significa "feature apagada por umbral sin confirmar". Los códigos 300 y 56 se confirmaron con datos y los parámetros están activos: esas columnas **ya miden** |

`flag_vuelta_falsa` sigue en gris, y ahí sí corresponde: `tolerancia_vuelta_falsa` continúa
sin confirmar por negocio.

> El diagrama es para el DBA. Que mostrara en gris —"sin datos"— tres columnas que hoy
> tienen 175, 201 y 640 valores habría llevado a una conclusión equivocada sobre el estado
> del modelo.

El diagrama de Bronze en la misma carpeta no requiere cambios: la capa Bronze no se tocó
ni el 3 ni el 4 de agosto.
