# Databricks notebook source
# MAGIC %md # 33 - Recorrido de la capa Silver (Caso 2)
# MAGIC
# MAGIC Este notebook explica **por qué** la capa Silver quedó como quedó, y te deja
# MAGIC **tocar los datos** para comprobarlo tú mismo. No es la validación automática
# MAGIC (esa es el notebook `32`, que da OK / REVISAR / FALLA): aquí cada decisión viene
# MAGIC con la celda que la hace verificable.
# MAGIC
# MAGIC | Sección | Qué encontrarás |
# MAGIC |---|---|
# MAGIC | **1** | Mapa de los 13 objetos: qué es cada uno y a qué grano |
# MAGIC | **2** | Las 11 decisiones de diseño, cada una con su evidencia consultable |
# MAGIC | **3** | Los problemas que se resolvieron: síntoma, causa real y arreglo |
# MAGIC | **4** | Recorrido interactivo de UN servicio suscrito, como lo haría el agente |
# MAGIC | **5** | Verificaciones que siguen pendientes |
# MAGIC | **6** | Preguntas abiertas para negocio (`PENDIENTE-NEG`) |
# MAGIC
# MAGIC ### Es 100% de SOLO LECTURA
# MAGIC No crea, no borra, no modifica. Ningún `DROP`, ningún `INSERT`, ningún `ALTER`.
# MAGIC Córrelo completo las veces que quieras.
# MAGIC
# MAGIC ### Cómo usarlo
# MAGIC Pon el widget `servicio_suscrito` en el servicio que quieras investigar y vuelve a
# MAGIC correr la sección 4. Si lo dejas vacío, el notebook **elige uno interesante solo**
# MAGIC (uno con más de un medidor en algún periodo, que es el caso rico).

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo", "1. Catálogo")
dbutils.widgets.text("schema_destino", "facturacion", "2. Esquema")
dbutils.widgets.text("servicio_suscrito", "", "3. Servicio suscrito (vacío = elegir uno)")

CATALOG = dbutils.widgets.get("catalog_destino").strip()
SCHEMA  = dbutils.widgets.get("schema_destino").strip()
PREFIJO = f"{CATALOG}.{SCHEMA}"

from pyspark.sql import functions as F

def tabla(nombre):
    return spark.table(f"{PREFIJO}.{nombre}")

def titulo(txt):
    print("\n" + "=" * 92); print(txt); print("=" * 92)

print(f"Recorriendo: {PREFIJO}")
print("Modo: SOLO LECTURA")

# COMMAND ----------
# MAGIC %md
# MAGIC # 1. Mapa de la capa
# MAGIC
# MAGIC La capa tiene **tres niveles**, y la diferencia entre ellos no es cosmética:
# MAGIC
# MAGIC - **Nivel 2** — el punto de entrada. Es el **único** objeto de toda la capa que
# MAGIC   filtra por número de actividad. Si mañana entra un tercer caso de uso, se agrega
# MAGIC   otra vista de Nivel 2 y **no se toca nada de lo existente**.
# MAGIC - **Nivel 1 — hechos.** Tablas materializadas con el historial y las features.
# MAGIC - **Nivel 1 — pasarelas.** Vistas sobre Bronze sin filtros, para que ningún
# MAGIC   consumidor tenga que tocar Bronze.
# MAGIC
# MAGIC Todo lo que no es Nivel 2 es **agnóstico al caso de uso**. Esa es la propiedad que
# MAGIC hace que la capa escale.

# COMMAND ----------
titulo("1. Los 13 objetos de la capa")

MAPA = [
    # (objeto, nivel, forma, grano, para qué sirve)
    ("midas_ordenes_variacion_consumo_silver", "Nivel 2", "vista", "orden",
     "Roster de órdenes abiertas de variación de consumo. Por aquí arranca el agente."),
    ("midas_historial_consumo_silver", "Nivel 1", "tabla", "SS+periodo+tipo+medidor",
     "La serie de consumo. Espina dorsal = lecturas (ver ADR 0002)."),
    ("midas_historial_consumo_periodo_silver", "Nivel 1", "vista", "SS+periodo+tipo",
     "La misma serie con el medidor colapsado, para quien no lo necesita."),
    ("midas_historial_cargos_silver", "Nivel 1", "tabla", "línea de cargo",
     "Cargos con 5 banderas derivadas. Sin PK: Oracle no da id único de línea."),
    ("midas_features_consumo_silver", "Nivel 1", "tabla", "SS+periodo+tipo",
     "7 de las 8 reglas duras del Caso 2, como MEDIDAS."),
    ("midas_datos_servicios_contrato_silver", "Nivel 1", "vista", "servicio suscrito",
     "Roster por contrato: los servicios hermanos."),
    ("midas_datos_detalle_solicitudes_silver", "Nivel 1", "tabla ADOPTADA", "solicitud",
     "Trámites. Tabla preexistente: su schema manda, solo refrescamos contenido."),
    ("midas_datos_investigacion_consumo_silver", "Nivel 1", "vista", "SS+periodo+tipo",
     "Investigaciones. El estado va CRUDO a propósito."),
    ("midas_datos_perdidas_no_operacionales_silver", "Nivel 1", "vista", "expediente PNO",
     "El EXPEDIENTE de la pérdida. La DETECCIÓN vive en es_pno de cargos."),
    ("midas_datos_basicos_producto_silver", "Legacy", "tabla", "servicio suscrito",
     "Caso 1. SQL verbatim, patrón intacto."),
    ("midas_ordenes_calidad_pendientes_silver", "Legacy", "tabla", "orden",
     "Caso 1 + 2 columnas aditivas de corte facturable."),
    ("midas_historial_critica_silver", "Legacy", "tabla", "orden de crítica",
     "Caso 1. OJO: su contenido cambió con la rama 4 de la v3."),
    ("midas_historial_facturacion_silver", "Legacy", "tabla", "cuenta de cobro",
     "Caso 1. SQL verbatim."),
]

filas = []
for obj, nivel, forma, grano, para_que in MAPA:
    try:
        n = tabla(obj).count()
    except Exception:                                          # noqa: BLE001
        n = None
    filas.append((nivel, obj, forma, grano, f"{n:,}" if n is not None else "NO EXISTE", para_que))

display(spark.createDataFrame(
    filas, "nivel string, objeto string, forma string, grano string, filas string, para_que string"))

# COMMAND ----------
# MAGIC %md
# MAGIC # 2. Las decisiones de diseño
# MAGIC
# MAGIC Once decisiones. Cada una con la celda que te deja comprobarla.

# COMMAND ----------
# MAGIC %md
# MAGIC ## D1 — Silver publica MEDIDAS, nunca veredictos
# MAGIC
# MAGIC No existe ninguna columna `cierre_sugerido`, `requiere_ajuste` ni
# MAGIC `recomendacion`. La razón es doble:
# MAGIC
# MAGIC 1. El veredicto es del **agente**, no del modelo de datos.
# MAGIC 2. Estas tablas son el **patrón de medida** con el que se evalúa al agente. Si la
# MAGIC    capa ya trajera la respuesta, estaríamos calificando al agente con la respuesta
# MAGIC    que nosotros mismos le dimos. **El evaluador no puede ser parte de lo evaluado.**
# MAGIC
# MAGIC Lo que sí publica: cuánto se desvió, cuántos medidores hubo, si la lectura bajó,
# MAGIC cuántos periodos seguidos. El juicio lo hace quien lee.

# COMMAND ----------
titulo("D1 - Ninguna columna emite veredicto")

PROHIBIDAS = ["cierre", "sugerid", "recomend", "requiere_", "veredicto", "decision", "accion"]
hallazgos = []
for obj, *_ in MAPA:
    try:
        for c in tabla(obj).columns:
            if any(p in c.lower() for p in PROHIBIDAS):
                hallazgos.append((obj, c))
    except Exception:                                          # noqa: BLE001
        pass

print(f"  Columnas con pinta de veredicto: {hallazgos if hallazgos else 'ninguna'}")
print("\n  Lo que SÍ publica features_consumo (una muestra):")
for c in ["n_medidores_periodo", "consumo_calculado_negativo", "desviacion_vs_promedio_pct",
          "n_periodos_lectura_decreciente_consecutivos", "ratio_vuelta_falsa"]:
    print(f"    - {c}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## D2 — Lecturas es la espina dorsal, no consumos
# MAGIC
# MAGIC El plan original pedía `lecturas ⋈ consumos` al grano de medidor. **No era
# MAGIC posible**: `midas_datos_consumos_producto_bronze` no proyecta el medidor
# MAGIC (`cosselme` solo aparece en el `ORDER BY`), y aunque se agregara, `lecturas.medidor`
# MAGIC es `elmecodi` mientras `cosselme` es `elmeidem` — no son joinables.
# MAGIC
# MAGIC Pero lecturas **ya trae el consumo al grano de medidor** (su CTE suma `cosscoca` con
# MAGIC `cossmecc = 4`). La pieza que faltaba ya estaba ahí.
# MAGIC
# MAGIC Consumos entra **agregado** al grano `(SS, periodo, tipo)` aportando `calificacion`
# MAGIC y `funcion_calculo`: son atributos del **acto de cálculo del periodo**, no
# MAGIC propiedades del medidor, así que replicarlos en cada medidor es correcto.
# MAGIC
# MAGIC Detalle completo: `docs/adr/0002-grano-historial-consumo.md`.

# COMMAND ----------
titulo("D2 - El grano de medidor y su costo real")

hc = tabla("midas_historial_consumo_silver")
n_grano_medidor = hc.count()
n_grano_periodo = hc.select("servicio_suscrito", "id_periodo_consumo", "tipo_consumo_cod").distinct().count()

print(f"  Filas al grano (SS, periodo, tipo, medidor): {n_grano_medidor:,}")
print(f"  Combinaciones al grano (SS, periodo, tipo):  {n_grano_periodo:,}")
print(f"  Diferencia = periodos con MÁS DE UN medidor: {n_grano_medidor - n_grano_periodo:,}")
print("\n  Esa diferencia es exactamente la señal del Caso 3/4 (cambio de medidor).")
print("  Si fuera 0, el grano de medidor no aportaría nada y sobraría la tabla.")

display(hc.groupBy("servicio_suscrito", "id_periodo_consumo", "tipo_consumo_cod")
          .agg(F.countDistinct("medidor").alias("n_medidores"),
               F.collect_set("medidor").alias("medidores"))
          .filter(F.col("n_medidores") > 1)
          .orderBy(F.col("n_medidores").desc()))

# COMMAND ----------
# MAGIC %md
# MAGIC ## D3 — Un atributo ambiguo sale `NULL`, nunca `FIRST()`
# MAGIC
# MAGIC Cuando al agrupar hay más de un valor distinto para `calificacion` o
# MAGIC `funcion_calculo`, la columna sale **`NULL`** y un contador (`n_calificaciones`,
# MAGIC `n_funciones_calculo`) dice cuántos había.
# MAGIC
# MAGIC `FIRST()` habría devuelto un valor **arbitrario y no determinista**, presentado como
# MAGIC si fuera el dato. Eso es peor que un `NULL`: el `NULL` es auditable, y aquí significa
# MAGIC específicamente *"hubo más de uno"*, no *"no hay dato"*.
# MAGIC
# MAGIC Para `calificacion` además se publica el array `calificaciones` con todos los valores.

# COMMAND ----------
titulo("D3 - NULL honesto vs valor arbitrario")

for attr, contador in (("calificacion", "n_calificaciones"),
                       ("funcion_calculo", "n_funciones_calculo")):
    amb = hc.filter(F.col(contador) > 1).count()
    incoherente = hc.filter((F.col(contador) > 1) & F.col(attr).isNotNull()).count()
    print(f"  {attr}: {amb:,} grupos ambiguos | incoherencias (ambiguo y no-nulo): {incoherente}")

print("\n  Un NULL con su contador en 1 es 'no había dato'.")
print("  Un NULL con su contador > 1 es 'había varios y no elegimos por ti'.")
display(hc.select("servicio_suscrito", "id_periodo_consumo", "tipo_consumo_cod",
                  "calificacion", "n_calificaciones", "calificaciones")
          .filter(F.col("n_calificaciones") > 1))

# COMMAND ----------
# MAGIC %md
# MAGIC ## D4 — El centinela `'(sin medidor)'` y por qué no es cosmético
# MAGIC
# MAGIC Cuando Oracle no trae el medidor, se escribe `'(sin medidor)'` y se marca
# MAGIC `medidor_desconocido = true`.
# MAGIC
# MAGIC **No es maquillaje: la PK exige `NOT NULL`, y en Unity Catalog el `NOT NULL` sí se
# MAGIC hace cumplir** (la PRIMARY KEY, en cambio, es solo informativa y no se valida).
# MAGIC
# MAGIC Un detalle que se prestó a confusión durante el diseño: se temía que colapsar los
# MAGIC nulos en un centinela rompiera la unicidad. No la rompe — **Spark `GROUP BY` ya
# MAGIC agrupa los `NULL` entre sí**, así que el conteo de combinaciones distintas que se
# MAGIC midió antes de fijar la PK *ya incluía* ese colapso.
# MAGIC
# MAGIC Se publica `medidor_desconocido` para que nadie tenga que comparar contra la cadena.

# COMMAND ----------
titulo("D4 - Centinela y unicidad del grano")

n_cent = hc.filter(F.col("medidor_desconocido")).count()
claves = ["servicio_suscrito", "id_periodo_consumo", "tipo_consumo_cod", "medidor"]
print(f"  Filas con medidor desconocido: {n_cent:,} de {n_grano_medidor:,}")
print(f"  ¿El grano sigue siendo único? {hc.select(*claves).distinct().count() == n_grano_medidor}")
print(f"  ¿Algún NULL en las claves?    {hc.filter(' OR '.join(f'{c} IS NULL' for c in claves)).count()} (UC lo impide)")
print("\n  En UC: PRIMARY KEY = informativa (NO se valida). NOT NULL = SÍ se hace cumplir.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## D5 y D6 — Cero umbrales cableados, y el parámetro sin confirmar sale `NULL`
# MAGIC
# MAGIC **D5:** ningún número de negocio vive en el código. Todos están en
# MAGIC `midas_parametros`. Cambiar "cuántos periodos entran al promedio" es un `UPDATE`,
# MAGIC **no un despliegue**.
# MAGIC
# MAGIC **D6:** los que negocio aún no confirma están sembrados con `activo = false`. El
# MAGIC resolver inyecta un centinela y `TRY_CAST` lo convierte en `NULL`.
# MAGIC
# MAGIC Es `TRY_CAST` y no `CAST` por una razón concreta: con ANSI activo —el default en DBR
# MAGIC reciente— un `CAST('__SIN_PARAMETRIZAR__' AS DOUBLE)` **lanza error y tumba la
# MAGIC carga**. Con `TRY_CAST` devuelve `NULL`, la comparación es `NULL` y la feature sale
# MAGIC `NULL`. Es la diferencia entre *"esta columna no está confirmada"* y *"la capa no
# MAGIC cargó"*.
# MAGIC
# MAGIC **Preferimos el vacío al número inventado.** Un vacío se ve y se pregunta; un número
# MAGIC calculado con el parámetro equivocado se cree.

# COMMAND ----------
titulo("D5/D6 - Parámetros y las columnas que apagan")

par = tabla("midas_parametros")
print(f"  Activos: {par.filter(F.col('activo')).count()} | Inactivos: {par.filter(~F.col('activo')).count()}")
display(par.filter(~F.col("activo")).select("dominio", "clave", "valor", "descripcion"))

fc = tabla("midas_features_consumo_silver")
n_feat = fc.count()
print("\n  Columnas que HOY salen NULL, y el parámetro del que dependen:")
for col, param in [("flag_vuelta_falsa", "tolerancia_vuelta_falsa"),
                   ("fecha_ultima_reconexion", "tipo_solicitud_reconexion"),
                   ("dias_desde_reconexion", "tipo_solicitud_reconexion"),
                   ("solicitud_reconexion_intersecta_periodo", "tipo_solicitud_reconexion"),
                   ("solicitud_suspension_intersecta_periodo", "tipo_solicitud_suspension")]:
    n = fc.filter(F.col(col).isNotNull()).count()
    print(f"    {col:42s} no-nulos={n:6,}  <- {param}")
print(f"\n  (sobre {n_feat:,} filas). Se encienden con un UPDATE, sin desplegar código.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## D7 — `REGEXP_EXTRACT`, nunca `SPLIT`, para el código de un `codigo-descripcion`
# MAGIC
# MAGIC Dos razones independientes, y cada una basta:
# MAGIC
# MAGIC 1. **`SPLIT(x, '-')[0]` devuelve CADENA VACÍA** para códigos negativos como `-1` o
# MAGIC    `-26-Fénix`, porque el primer trozo antes del guion está vacío.
# MAGIC 2. **Conviven dos formatos** en Bronze: las órdenes pendientes traen `993 - VARIACION`
# MAGIC    (con espacios) y la crítica trae `102010-ANALIZAR` (sin espacios).
# MAGIC
# MAGIC `REGEXP_EXTRACT(x, '^\\s*(-?[0-9]+)', 1)` resuelve los tres casos.

# COMMAND ----------
titulo("D7 - Por qué SPLIT no sirve")

demo = spark.createDataFrame(
    [("993 - VARIACION SIGNIFICATIVA",), ("102010-ANALIZAR ORDEN",),
     ("-1-SIN INFORMACION",), ("3-ACTIVA",)], "valor string")
display(demo.select(
    "valor",
    F.split("valor", "-")[0].alias("SPLIT_mal"),
    F.regexp_extract("valor", r"^\s*(-?[0-9]+)", 1).alias("REGEXP_bien")))

print("  Fíjate en la fila del código negativo: SPLIT devuelve cadena vacía.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## D8 — `es_recuperacion` por parseo posicional, no por `LIKE '%PR%'`
# MAGIC
# MAGIC El documento soporte tiene forma `CO-PR-202606-...`. La bandera se arma con **dos
# MAGIC componentes que se publican por separado**:
# MAGIC
# MAGIC - `documento_tiene_token_recuperacion` — el token `PR` está **en su posición**.
# MAGIC - `periodo_consumo_difiere_de_cuenta` — el cargo es de un periodo distinto al de la
# MAGIC   cuenta.
# MAGIC
# MAGIC `es_recuperacion` es la conjunción de ambas. Se publican los componentes para que el
# MAGIC agente vea **por qué** se marcó, no solo **que** se marcó.
# MAGIC
# MAGIC Un `LIKE '%PR%'` daría positivo con cualquier texto que contenga esas dos letras en
# MAGIC cualquier posición — incluido un nombre propio dentro del documento.

# COMMAND ----------
titulo("D8 - Las 5 banderas de cargos")

cg = tabla("midas_historial_cargos_silver")
n_cg = cg.count()
for flag in ["es_facturacion_normal", "es_pno", "documento_tiene_token_recuperacion",
             "periodo_consumo_difiere_de_cuenta", "es_recuperacion"]:
    n = cg.filter(F.col(flag)).count()
    print(f"  {flag:38s} {n:7,} / {n_cg:,}  ({100.0*n/n_cg if n_cg else 0:5.2f}%)")

print("\n  Coherencia: es_recuperacion debe ser la conjunción de los dos componentes.")
incoh = cg.filter(F.col("es_recuperacion") !=
                  (F.col("documento_tiene_token_recuperacion") &
                   F.col("periodo_consumo_difiere_de_cuenta"))).count()
print(f"  Filas incoherentes: {incoh}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## D9 — El número de actividad vive en UN solo objeto
# MAGIC
# MAGIC `midas_ordenes_variacion_consumo_silver` es el **único** objeto de la capa que filtra
# MAGIC por actividad, y el número lo lee de `midas_parametros`
# MAGIC (`orden` / `actividad_variacion_consumo`).
# MAGIC
# MAGIC Todo lo demás —Bronze, tablas, vistas de Nivel 1— es agnóstico. Por eso un tercer
# MAGIC caso de uso se resuelve **agregando** una vista, sin tocar una línea de lo existente.

# COMMAND ----------
titulo("D9 - Nivel 2 y su parámetro")

n2 = tabla("midas_ordenes_variacion_consumo_silver")
actividades = [r[0] for r in n2.select("actividad_cod").distinct().collect()]
esperado = par.filter((F.col("dominio") == "orden") &
                      (F.col("clave") == "actividad_variacion_consumo")).collect()
print(f"  Órdenes en la vista:        {n2.count():,}")
print(f"  Actividades distintas:      {actividades}  (debe ser UNA)")
print(f"  Parámetro que la gobierna:  {esperado[0]['valor'] if esperado else 'NO SEMBRADO'}")
display(n2.select("id_orden", "servicio_suscrito", "fecha_creacion", "actividad",
                  "estado_orden", "estado_corte_facturable_desc", "saldo_vencido").limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## D10 — `detalle_solicitudes` es una tabla ADOPTADA
# MAGIC
# MAGIC Es el único objeto de la capa que **no definimos nosotros**. La creó el autor
# MAGIC original de MIDAS el 2026-05-21, tres minutos después de su Bronze. Ya existía, tiene
# MAGIC consumidores, y **su schema manda**: nuestro pipeline solo refresca su contenido.
# MAGIC
# MAGIC Por eso **no lleva DDL en el repo**. Un `CREATE TABLE IF NOT EXISTS` contra una tabla
# MAGIC que ya existe no hace nada y no falla: dejaría creer que el contrato es nuestro
# MAGIC cuando no lo es.
# MAGIC
# MAGIC Carga con `INSERT OVERWRITE ... BY NAME`, y eso es deliberado: si su dueño le agrega
# MAGIC o quita una columna, **la carga falla**. La forma posicional escribiría los valores
# MAGIC corridos sin avisar. Ante un schema que no controlamos, fallar es lo correcto.
# MAGIC
# MAGIC **Consecuencia para quien la consuma:** no tiene `tipo_solicitud_cod` (esa derivación
# MAGIC vive ahora en `features_consumo`), ni `run_id`, ni `fecha_carga_silver`. Su
# MAGIC trazabilidad está en `midas_log_cargas`, como la de todos.

# COMMAND ----------
titulo("D10 - La tabla adoptada y su contrato")

ADOPTADA = "midas_datos_detalle_solicitudes_silver"
ds = tabla(ADOPTADA)
ESPERADAS = ["servicio_suscrito", "id_solicitud", "usuario", "tipo_solicitud",
             "fecha_solicitud", "estado_solicitud", "fecha_atencion_solicitud",
             "comentario", "medio_recepcion", "analista", "area_organizacional"]
print(f"  Columnas en la tabla: {len(ds.columns)} | esperadas por el load: {len(ESPERADAS)}")
print(f"  Sobran en la tabla:   {[c for c in ds.columns if c not in ESPERADAS] or 'ninguna'}")
print(f"  Faltan en la tabla:   {[c for c in ESPERADAS if c not in ds.columns] or 'ninguna'}")
print("\n  Si alguna lista deja de estar vacía, BY NAME va a fallar en la próxima carga.")
print("  Se corrige el .sql del load, NUNCA la tabla.")
display(ds.limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## D11 — Las señales con varias fuentes no se colapsan
# MAGIC
# MAGIC Tres señales tienen más de una fuente independiente. **Ninguna reemplaza a las
# MAGIC otras**, y el modelo no elige por el agente.
# MAGIC
# MAGIC **Investigación** — tres fuentes:
# MAGIC 1. `midas_datos_investigacion_consumo_silver` (el registro del proceso),
# MAGIC 2. la solicitud tipo `100207` (el trámite radicado),
# MAGIC 3. `funcion_calculo` / `calificacion` en el propio consumo — **la más fiable**,
# MAGIC    porque vive en la fila del consumo y no requiere join.
# MAGIC
# MAGIC **PNO** — dos vías: `es_pno` en cargos **detecta**; la Silver de PNO es el
# MAGIC **expediente** (qué irregularidad, en qué ventana). Que no coincidan al 100% es un
# MAGIC **hallazgo de negocio, no un defecto del modelo**.
# MAGIC
# MAGIC **Cambio de medidor** — cuatro medidas. La serie del medidor no siempre se actualiza
# MAGIC en Oracle, así que `medidor_cambio_detectado_por_serie = false` **no descarta** el
# MAGIC cambio.

# COMMAND ----------
titulo("D11 - PNO: detección vs expediente")

ss_cargo = cg.filter(F.col("es_pno")).select("servicio_suscrito").distinct()
ss_exped = tabla("midas_datos_perdidas_no_operacionales_silver").select("servicio_suscrito").distinct()
n_cargo, n_exped = ss_cargo.count(), ss_exped.count()
n_ambos = ss_cargo.join(ss_exped, "servicio_suscrito", "inner").count()
print(f"  SS con cargo de PNO (detección): {n_cargo:,}")
print(f"  SS con expediente de PNO:        {n_exped:,}")
print(f"  En ambos:                        {n_ambos:,}")
print("\n  La coincidencia parcial es esperada: son dos vías independientes.")
print("  Los que tienen cargo SIN expediente son la pregunta interesante para negocio.")

titulo("D11 - Cambio de medidor: las 4 medidas")
display(fc.select("servicio_suscrito", "id_periodo_consumo", "tipo_consumo_cod",
                  "n_medidores_periodo", "medidor_cambio_detectado_por_serie",
                  "consumo_calculado_negativo", "tiene_observacion_cambio_medidor", "medidores")
          .filter((F.col("n_medidores_periodo") > 1) |
                  F.col("consumo_calculado_negativo") |
                  F.col("tiene_observacion_cambio_medidor")))

# COMMAND ----------
# MAGIC %md
# MAGIC # 3. Problemas que se resolvieron
# MAGIC
# MAGIC Los cuatro fallos reales de la puesta en marcha, con su causa **real** (no la
# MAGIC aparente) y lo que se hizo para que no vuelvan.

# COMMAND ----------
# MAGIC %md
# MAGIC ### P1 — `query_padre_id=nan no está entre los objetos activos`
# MAGIC
# MAGIC **Síntoma:** la capa abortaba antes de construir un solo objeto.
# MAGIC
# MAGIC **Causa real:** `get_active_tables()` hace `.toPandas()`. Pandas no tiene enteros
# MAGIC nulos en su dtype por defecto, así que una columna `BIGINT` con NULLs se convierte a
# MAGIC `float64` y el NULL llega como **`NaN`, no como `None`**. Como `NaN is not None` es
# MAGIC `True`, el chequeo leía *"sin padre"* como *"padre inexistente"*.
# MAGIC
# MAGIC Silver es el **primer consumidor real** de `query_padre_id` — en Bronze siempre fue
# MAGIC informativo — por eso el bug estuvo latente meses.
# MAGIC
# MAGIC **Arreglo:** `orden_topologico()` normaliza a `int`/`None` antes de usar los ids.
# MAGIC Eso además eliminó dos problemas latentes: `orden_ejecucion` con `NaN` habría hecho
# MAGIC un `sorted()` incoherente (`NaN` es *truthy*, el `or 0` no lo atrapaba), y los
# MAGIC `numpy.int64` iban directo a las sentencias SQL del log.
# MAGIC
# MAGIC ### P2 — `CANNOT_RESOLVE_STAR_EXPAND: b.*`
# MAGIC
# MAGIC **Causa:** `SELECT b.*` con `FROM base`, sin alias. Un error de una palabra que
# MAGIC tumbó el objeto **y a sus dos hijos**, porque el orquestador omite a los hijos de un
# MAGIC padre fallido (construirlos sobre datos de ayer sería peor).
# MAGIC
# MAGIC **Arreglo:** `FROM base AS b`, más un test que verifica que **todo `alias.*` tenga su
# MAGIC alias definido** en los 16 archivos. No es un parser de SQL: solo cubre los `.*`, que
# MAGIC es donde el error se paga caro.
# MAGIC
# MAGIC ### P3 — `EXPECT_VIEW_NOT_TABLE`
# MAGIC
# MAGIC **Síntoma:** `detalle_solicitudes_silver` no aceptaba `CREATE OR REPLACE VIEW`.
# MAGIC
# MAGIC **Causa:** ya existía como TABLA, creada por el autor original de MIDAS.
# MAGIC
# MAGIC **Arreglo:** se **adoptó** (ver D10) en vez de borrarla. Se descartó pedir permisos
# MAGIC `MANAGE`: el error nunca fue de permisos, y el pipeline ya sobrescribe otras cuatro
# MAGIC tablas del mismo autor sin problema.
# MAGIC
# MAGIC ### P4 — `Falta .../view/midas_datos_detalle_solicitudes_silver.sql`
# MAGIC
# MAGIC **Causa:** el `tipo_carga` cambió en el código pero **el control seguía con el valor
# MAGIC viejo**, porque se reparó solo `bronze_to_silver` sin re-ejecutar `crear_objetos`.
# MAGIC
# MAGIC **Lo grave no fue el error, sino cuándo:** falló en el objeto 10 de 13, con los 9
# MAGIC anteriores **ya sobrescritos**.
# MAGIC
# MAGIC **Arreglo:** una **verificación previa**. Ahora lee y resuelve las 16 sentencias
# MAGIC —archivos, placeholders, `tipo_carga` conocido— *antes* de ejecutar la primera. Si
# MAGIC algo falta, aborta con `No se tocó ningún dato` y lista **todos** los problemas de
# MAGIC una vez. Esa corrida habría fallado en 2 segundos sin escribir nada.

# COMMAND ----------
titulo("P - La bitácora de la última corrida")

log = spark.sql(f"""
    SELECT c.tabla_destino, c.tipo_carga, l.estado, l.filas_escritas, l.fecha_fin, l.run_id
      FROM {PREFIJO}.midas_log_cargas l
      JOIN {PREFIJO}.midas_control_cargas c ON c.id_carga = l.id_carga
     WHERE c.job_name = 'midas_silver'
""")
# Por FECHA, no por MAX(run_id): run_id es STRING y su orden lexicográfico pone "9" > "10".
ult = log.filter(F.col("fecha_fin").isNotNull()).orderBy(F.col("fecha_fin").desc()).limit(1).collect()
if ult:
    display(log.filter(F.col("run_id") == ult[0]["run_id"]).orderBy("fecha_fin"))
else:
    print("  Sin corridas registradas todavía.")

# COMMAND ----------
# MAGIC %md
# MAGIC # 4. Recorrido interactivo: un servicio suscrito de punta a punta
# MAGIC
# MAGIC Este es el camino que recorre el agente. Cambia el widget `servicio_suscrito` y
# MAGIC vuelve a correr desde aquí.

# COMMAND ----------
titulo("4. Eligiendo el servicio suscrito")

_ss = dbutils.widgets.get("servicio_suscrito").strip()
n2_todas = tabla("midas_ordenes_variacion_consumo_silver")

if _ss:
    SS = int(_ss)
    print(f"  Usando el del widget: {SS}")
else:
    # Se elige por PREFERENCIA, no al azar. Un servicio cualquiera suele ser aburrido y
    # deja media sección en blanco. Se busca, en orden:
    #   1. con orden abierta Y varios medidores  -> el recorrido completo tiene contenido
    #   2. con orden abierta                     -> al menos entra por donde entra el agente
    #   3. cualquiera con historial              -> último recurso
    con_varios = (hc.groupBy("servicio_suscrito", "id_periodo_consumo", "tipo_consumo_cod")
                    .agg(F.countDistinct("medidor").alias("n")).filter(F.col("n") > 1)
                    .select("servicio_suscrito").distinct())
    con_orden = n2_todas.select("servicio_suscrito").distinct()

    for criterio, df in (("con orden abierta y varios medidores", con_orden.join(con_varios, "servicio_suscrito")),
                         ("con orden abierta",                    con_orden.join(hc.select("servicio_suscrito").distinct(), "servicio_suscrito")),
                         ("cualquiera con historial",             hc.select("servicio_suscrito").distinct())):
        cand = df.limit(1).collect()
        if cand:
            SS = cand[0]["servicio_suscrito"]
            print(f"  Widget vacío -> elegido automáticamente ({criterio}): {SS}")
            break
    else:
        raise RuntimeError("No hay ningún servicio suscrito en midas_historial_consumo_silver.")

if n2_todas.filter(F.col("servicio_suscrito") == SS).count() == 0:
    print("\n  AVISO: este servicio NO tiene orden abierta de variación de consumo.")
    print("  Las secciones 4.1 y 4.2 saldrán vacías; el resto del recorrido sí funciona.")

# COMMAND ----------
# MAGIC %md ### 4.1 — La orden: por dónde entra el agente

# COMMAND ----------
display(tabla("midas_ordenes_variacion_consumo_silver").filter(F.col("servicio_suscrito") == SS))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.2 — Los servicios hermanos del contrato
# MAGIC
# MAGIC Sirve para descartar que el problema sea del predio y no del servicio.

# COMMAND ----------
# El contrato se resuelve desde el propio roster, no desde la orden: así funciona
# también para servicios sin orden abierta.
sc = tabla("midas_datos_servicios_contrato_silver")
contratos = [r["contrato"] for r in
             sc.filter(F.col("servicio_suscrito") == SS).select("contrato").distinct().collect()]
print(f"  Contrato(s) del servicio {SS}: {contratos or 'no está en el roster'}")
display(sc.filter(F.col("contrato").isin(contratos)) if contratos else sc.limit(0))

# COMMAND ----------
# MAGIC %md ### 4.3 — La serie de consumo, al grano de medidor

# COMMAND ----------
display(hc.filter(F.col("servicio_suscrito") == SS)
          .select("id_periodo_consumo", "tipo_consumo", "medidor", "medidor_desconocido",
                  "fecha_ini_consumo", "fecha_fin_consumo", "dias_consumo",
                  "lectura_anterior", "lectura_actual", "consumo_calculado",
                  "consumo_facturado", "consumo_facturado_periodo", "cuadra_consumo_periodo",
                  "calificacion", "n_calificaciones", "observacion_lectura")
          .orderBy("id_periodo_consumo", "tipo_consumo", "medidor"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.4 — Las features: las 7 reglas para este servicio
# MAGIC
# MAGIC Recuerda: son **medidas**. Ninguna dice si hay que ajustar o cerrar.

# COMMAND ----------
display(fc.filter(F.col("servicio_suscrito") == SS)
          .select("id_periodo_consumo", "tipo_consumo", "consumo_facturado_periodo",
                  "promedio_periodos_previos", "n_periodos_usados_en_promedio",
                  "desviacion_vs_promedio_pct", "n_medidores_periodo",
                  "medidor_cambio_detectado_por_serie", "consumo_calculado_negativo",
                  "hay_lectura_decreciente", "n_periodos_lectura_decreciente_consecutivos",
                  "constante_declarada", "constante_efectiva_min", "constante_efectiva_max",
                  "constante_efectiva_difiere_entre_tipos", "ratio_vuelta_falsa",
                  "flag_investigacion", "tiene_cargo_pno", "tiene_cargo_recuperacion",
                  "valor_cargos_periodo", "valor_cargos_programa_anormal", "delta_valor_pct")
          .orderBy("id_periodo_consumo", "tipo_consumo"))

# COMMAND ----------
# MAGIC %md ### 4.5 — Los cargos: de dónde sale la plata cobrada

# COMMAND ----------
display(cg.filter(F.col("servicio_suscrito") == SS)
          .select("id_cuenta_cobro", "id_periodo_consumo", "anio_facturacion", "mes_facturacion",
                  "concepto", "causal", "programa",
                  "documento_soporte", "unidades", "valor", "valor_con_signo",
                  "es_facturacion_normal", "es_pno", "es_recuperacion")
          .orderBy("id_periodo_consumo", "id_cuenta_cobro"))

# COMMAND ----------
# MAGIC %md ### 4.6 — Trámites, investigaciones y expedientes de PNO

# COMMAND ----------
print("Solicitudes (tabla adoptada):")
display(ds.filter(F.col("servicio_suscrito") == SS).orderBy(F.col("fecha_solicitud").desc()))

print("Investigaciones de consumo:")
display(tabla("midas_datos_investigacion_consumo_silver").filter(F.col("servicio_suscrito") == SS))

print("Expedientes de PNO:")
display(tabla("midas_datos_perdidas_no_operacionales_silver").filter(F.col("servicio_suscrito") == SS))

# COMMAND ----------
# MAGIC %md
# MAGIC # 5. Verificaciones pendientes
# MAGIC
# MAGIC Lo que **todavía no está comprobado**. Esto no es una lista de defectos: son cosas
# MAGIC que hay que mirar antes de dar la capa por cerrada.

# COMMAND ----------
titulo("5.1 - Frescura de Bronze: ¿sobre qué datos se construyó Silver?")

# Silver es tan fresca como su Bronze. Si una Bronze quedó rezagada, los objetos que la
# leen publican datos viejos SIN dar ninguna señal de error.
display(spark.sql(f"""
    SELECT table_name, last_altered,
           DATEDIFF(CURRENT_DATE(), CAST(last_altered AS DATE)) AS dias_desde_actualizacion
      FROM {CATALOG}.information_schema.tables
     WHERE table_schema = '{SCHEMA}' AND table_name LIKE 'midas_%_bronze'
     ORDER BY last_altered
"""))
print("  CERRADO el 2026-08-03: las 12 Bronze ACTIVAS están frescas (misma corrida).")
print("  Las 2 que aparecen rezagadas son las RETIRADAS en la v3 —")
print("  midas_dim_estado_corte_facturable_bronze y midas_datos_servicios_contrato_bronze —")
print("  que están desactivadas en el control, así que nada las escribe. Es lo esperado.")
print("  Que sigan existiendo confirma que scripts/migracion_v3_limpieza.sql NO se ha")
print("  ejecutado: su DROP sigue pendiente y es una decisión, no un olvido.")

# COMMAND ----------
titulo("5.2 - ¿Hay un segundo escritor sobre la tabla adoptada?")

# En el inventario del 2026-07-31 la _bronze se actualizó a las 14:00:20 y la _silver a
# las 14:01:05 — un minuto después, y NO fue nuestro pipeline (que corrió a las 18:32).
display(spark.sql(f"""
    SELECT table_name, created, created_by, last_altered
      FROM {CATALOG}.information_schema.tables
     WHERE table_schema = '{SCHEMA}'
       AND table_name IN ('midas_datos_detalle_solicitudes_bronze',
                          'midas_datos_detalle_solicitudes_silver')
"""))
print("  Si last_altered de la _silver no coincide con la última corrida de midas_silver,")
print("  hay otro proceso escribiéndola. Dos pipelines sobre el mismo objeto es un")
print("  problema real: hay que identificar cuál antes de que se pisen.")

# COMMAND ----------
titulo("5.3 - Cuadre del consumo: ¿la suma por medidor coincide con el periodo?")

# Si cuadra alto, el join grueso de D2 es coherente y agregar `medidor` a la Bronze de
# consumos no aportaria nada. Si cuadra bajo, hay que entender por que.
n_ok = hc.filter(F.col("cuadra_consumo_periodo")).count()
print(f"  Filas que cuadran: {n_ok:,} / {n_grano_medidor:,} ({100.0*n_ok/n_grano_medidor if n_grano_medidor else 0:.2f}%)")
display(hc.filter(~F.col("cuadra_consumo_periodo"))
          .select("servicio_suscrito", "id_periodo_consumo", "tipo_consumo_cod", "medidor",
                  "consumo_facturado", "consumo_facturado_periodo", "consumo_facturado_medidores")
          .limit(50))

# COMMAND ----------
titulo("5.4 - R6: la desviación conocida del promedio")

# La regla pide "los 5 periodos válidos más recientes, mirando hasta 6 atrás". La
# implementación usa el frame `6 PRECEDING AND 1 PRECEDING` con los periodos inválidos
# enmascarados a NULL (AVG los ignora). Cuando los 6 son válidos, promedia 6, no 5.
display(fc.groupBy("n_periodos_usados_en_promedio").count().orderBy("n_periodos_usados_en_promedio"))
print("  DESVIACION CONOCIDA, no defecto: se publica n_periodos_usados_en_promedio")
print("  justamente para que el cálculo sea auditable fila por fila.")
print("  El documento de negocio ya acepta tolerancia aquí (caso de gas verificado:")
print("  el promedio real era 28,34 y el analista trabajó con 30).")

# COMMAND ----------
# MAGIC %md
# MAGIC ### 5.5 — Pendientes que este notebook no puede comprobar solo
# MAGIC
# MAGIC | Pendiente | Por qué importa |
# MAGIC |---|---|
# MAGIC | **Una corrida verde desde el bundle desplegado** | Hoy se itera desde el Git folder. Lo que llega a uat/pdn es el bundle: validar una ruta y promover otra es un riesgo real |
# MAGIC | **Cobertura de periodos en `detalle_cargos`** (81,24% en la v3) | Los cargos sin periodo no entran a las features de R2 |
# MAGIC | **`COUNT(DISTINCT calificacion)` por grano con método 4** | Cuantifica cuántos grupos son *de verdad* ambiguos (ver D3) |
# MAGIC | **Caso 1 intacto** | Los conteos de sus 4 Silver antes/después. El notebook `32` lo cubre |
# MAGIC | **Contrato `1070378` de §10.6** | Puede no existir en `dllo`; si no está, se reporta como *no reproducible*, no como fallo |

# COMMAND ----------
# MAGIC %md
# MAGIC # 6. Preguntas abiertas para negocio (`PENDIENTE-NEG`)
# MAGIC
# MAGIC Ninguna bloquea la capa. Todas cambian lo que el agente puede concluir.
# MAGIC
# MAGIC ### 1. `delta_valor_pct` está a un grano distinto al de su fuente
# MAGIC La cuenta de cobro es por `(SS, periodo)`, **sin tipo de consumo**, así que ese
# MAGIC porcentaje **se repite igual** en activa y reactiva. ¿Debe repartirse por tipo usando
# MAGIC el concepto del cargo? Está anotado en el `COMMENT` de la columna.
# MAGIC
# MAGIC ### 2. `tolerancia_vuelta_falsa`
# MAGIC ¿A partir de qué proporción de `consumo_facturado / 10^dígitos` se considera vuelta
# MAGIC falsa? Hoy `flag_vuelta_falsa` sale `NULL` en toda la tabla.
# MAGIC
# MAGIC ### 3. Los tipos de solicitud `300` y `56`
# MAGIC El plan original los traía **invertidos**. Según el diccionario del proyecto:
# MAGIC **`300` = Reconexión por Pago**, **`56` = Suspensión por no Pago**. Se sembraron
# MAGIC corregidos y **apagados**. Confirmarlos enciende cuatro columnas de R5.
# MAGIC
# MAGIC ### 4. El concepto `87` como cargo de consumo normal
# MAGIC Es **inferencia sobre capturas de pantalla**, no un dato confirmado contra el sistema.
# MAGIC De él dependen `unidades_consumo_cobradas` y `delta_unidades_consumo_pct`.
# MAGIC
# MAGIC ### 5. `estado_pno` va crudo
# MAGIC No se verificó si tiene catálogo asociado. Si lo tuviera, se resolvería inline en la
# MAGIC query de Bronze (invariante I11), no en Silver.

# COMMAND ----------
titulo("6 - Estado de los PENDIENTE-NEG hoy")

display(par.filter(~F.col("activo")).select("dominio", "clave", "valor", "descripcion"))
print("\n  Cada fila de arriba es una feature apagada esperando confirmación de negocio.")
print("  Se encienden con un UPDATE a midas_parametros: no requieren desplegar código.")

# COMMAND ----------
# MAGIC %md
# MAGIC # Cierre
# MAGIC
# MAGIC **Dónde seguir:**
# MAGIC - `docs/contrato_silver.md` — el contrato por objeto, y **qué NO garantiza** cada uno.
# MAGIC - `docs/adr/0002-grano-historial-consumo.md` — la decisión del grano y su *seam*:
# MAGIC   el punto exacto donde habría que revisarla.
# MAGIC - `notebooks/32_validacion_silver_caso2.py` — la validación automática (OK / REVISAR /
# MAGIC   FALLA). Este notebook explica; ese decide.
# MAGIC - `scripts/migracion_silver_v1.sql` — el chequeo del schema de la tabla adoptada.
# MAGIC
# MAGIC **La idea que sostiene toda la capa:** Silver publica hechos y el agente emite juicios.
# MAGIC Cada vez que dudes de si algo va aquí o allá, esa es la pregunta.
