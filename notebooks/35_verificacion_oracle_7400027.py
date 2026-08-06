# Databricks notebook source
# MAGIC %md # 35 - ¿Por qué no llegan las órdenes 7400027?
# MAGIC
# MAGIC La rama 4 de `QUERY_ORDENES_CRITICA_PEVIA` **existe** y filtra por
# MAGIC `oa.activity_id = 7400027`. Pero `midas_datos_ordenes_previa_critica_bronze` no tiene
# MAGIC ni una sola fila con esa actividad: 779 filas repartidas en seis actividades, todas de
# MAGIC crítica o investigación.
# MAGIC
# MAGIC Hay **dos hipótesis** y este notebook las separa. No se toca la query hasta saber cuál es.
# MAGIC
# MAGIC | Hipótesis | Qué significaría |
# MAGIC |---|---|
# MAGIC | **H1 — No hay datos** | Ninguno de los servicios que carga la cadena tiene órdenes de decisión. La lógica está bien y no hay nada que arreglar |
# MAGIC | **H2 — El filtro de fecha las excluye** | Sí existen, pero `o.created_date between pefafimo and pefaffmo` las deja fuera. Una orden de DECISIÓN se crea cuando el analista decide, que suele ser DESPUÉS de que cerró el periodo analizado |
# MAGIC
# MAGIC La ventana de fecha **no es decorativa**: sin ella la misma orden se repetiría en cada
# MAGIC una de las ~8 iteraciones de periodo que hace el driver. Está documentada en la query
# MAGIC como efecto lateral asumido. Lo que este notebook decide es si ese efecto lateral nos
# MAGIC está costando el 100% de las órdenes.
# MAGIC
# MAGIC ### Es de SOLO LECTURA
# MAGIC Solo `SELECT` contra Oracle y contra Unity Catalog. No escribe en ningún lado.

# COMMAND ----------
# MAGIC %md
# MAGIC ### Instalación del conector (obligatoria, reinicia Python)
# MAGIC Va primero porque `restartPython()` borra todo el estado del notebook.

# COMMAND ----------
# MAGIC %pip install JayDeBeApi JPype1

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo", "1. Catálogo")
dbutils.widgets.text("schema_destino", "facturacion", "2. Esquema")
dbutils.widgets.text("db_user", "SQL_EPMBOTPD05", "3. Usuario Oracle")
dbutils.widgets.text("secret_scope", "AZ-SecretScopeDBKS-EPM-NP-KV-DLLO", "4. Secret scope")
dbutils.widgets.text("password_key", "AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA", "5. Secret key")
dbutils.widgets.text("oracle_host", "epm-to34.corp.epm.com.co", "6. Host")
dbutils.widgets.text("oracle_port", "1521", "7. Puerto")
dbutils.widgets.text("oracle_service", "SFUAT", "8. Service")
dbutils.widgets.text("jar_path", "/Volumes/epm_datalake_vol_np/facturacion_vol/facturacion_bronze_vol/_oracle_client/ojdbc11-23.26.2.0.0.jar", "9. Jar JDBC")

CATALOG = dbutils.widgets.get("catalog_destino").strip()
SCHEMA  = dbutils.widgets.get("schema_destino").strip()
PREFIJO = f"{CATALOG}.{SCHEMA}"
print(f"Bronze: {PREFIJO}")

# COMMAND ----------
# MAGIC %md ## Paso 1 — Los servicios que realmente carga la cadena

# COMMAND ----------
from pyspark.sql import functions as F

# La cadena itera sobre los servicios que salen de datos_basicos, que a su vez sale de
# ordenes_calidad_pendientes. Es EXACTAMENTE la poblacion que ve la rama 4.
ss = [r["servicio_suscrito"] for r in
      spark.table(f"{PREFIJO}.midas_datos_basicos_producto_bronze")
           .select("servicio_suscrito").distinct().collect()]
print(f"Servicios suscritos en la cadena: {len(ss):,}")

# Las actividades que SI llegaron, para tener la linea base a la vista.
display(spark.table(f"{PREFIJO}.midas_datos_ordenes_previa_critica_bronze")
             .groupBy("actividad").count().orderBy(F.col("count").desc()))

# COMMAND ----------
# MAGIC %md ## Paso 2 — Conexión a Oracle

# COMMAND ----------
import os
import jaydebeapi

HOST = dbutils.widgets.get("oracle_host").strip()
PORT = dbutils.widgets.get("oracle_port").strip()
SERV = dbutils.widgets.get("oracle_service").strip()
USER = dbutils.widgets.get("db_user").strip()
JAR  = dbutils.widgets.get("jar_path").strip()
PASS = dbutils.secrets.get(scope=dbutils.widgets.get("secret_scope").strip(),
                           key=dbutils.widgets.get("password_key").strip())

# Sin `//` despues del @: es la forma que usa el conector del bundle.
URL = f"jdbc:oracle:thin:@{HOST}:{PORT}/{SERV}"
conn = jaydebeapi.connect("oracle.jdbc.OracleDriver", URL, [USER, PASS], JAR)
print(f"Conectado: {URL}")


def q(sql, params=None):
    """SELECT -> lista de tuplas. Solo lectura."""
    cur = conn.cursor()
    try:
        cur.execute(sql, params or [])
        return cur.fetchall(), [c[0] for c in cur.description]
    finally:
        cur.close()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 3 — H1: ¿existen órdenes 7400027 para ESTOS servicios?
# MAGIC
# MAGIC Sin filtro de fecha, sin nada. Solo servicio + actividad.
# MAGIC
# MAGIC Si esto da **cero**, la hipótesis del usuario es correcta: la lógica está bien y
# MAGIC simplemente no hay órdenes de decisión en la muestra. No habría nada que arreglar.

# COMMAND ----------
# Se consulta en lotes: una lista IN con miles de elementos revienta el limite de Oracle
# (1000 expresiones) y ademas el plan se degrada.
LOTE = 900
total_sin_filtro = 0
detalle = []

for i in range(0, len(ss), LOTE):
    chunk = ss[i:i + LOTE]
    marcas = ",".join(["?"] * len(chunk))
    filas, _ = q(f"""
        SELECT COUNT(*), COUNT(DISTINCT oa.product_id), MIN(o.created_date), MAX(o.created_date)
          FROM or_order_activity oa, or_order o
         WHERE o.order_id = oa.order_id
           AND oa.activity_id = 7400027
           AND oa.product_id IN ({marcas})
    """, chunk)
    n, n_ss, fmin, fmax = filas[0]
    total_sin_filtro += (n or 0)
    if n:
        detalle.append((i // LOTE, n, n_ss, str(fmin), str(fmax)))

print(f"Ordenes 7400027 para los {len(ss):,} servicios de la cadena: {total_sin_filtro:,}")
for lote, n, n_ss, fmin, fmax in detalle:
    print(f"  lote {lote}: {n:,} ordenes en {n_ss} servicios | fechas {fmin} .. {fmax}")

if total_sin_filtro == 0:
    print("\n>>> H1 CONFIRMADA: no existen ordenes de decision para esta poblacion.")
    print("    La logica de la rama 4 esta bien. No hay nada que corregir en la query.")
    print("    Para ver ordenes 7400027 hay que ampliar la poblacion de servicios, no la query.")
else:
    print("\n>>> H1 DESCARTADA: si existen. Sigue al Paso 4 para ver si es el filtro de fecha.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 4 — H2: ¿las excluye la ventana de fecha?
# MAGIC
# MAGIC Solo tiene sentido si el Paso 3 encontró órdenes. Compara la fecha de creación de esas
# MAGIC órdenes contra las ventanas de periodo de facturación que recorre el driver.

# COMMAND ----------
if total_sin_filtro == 0:
    print("Paso 3 dio cero: este bloque no aplica.")
else:
    # Las ventanas que usa el driver: los ultimos N periodos de facturacion.
    periodos, cols = q("""
        SELECT * FROM (
            SELECT pf.pefacodi, pf.pefafimo, pf.pefaffmo
              FROM perifact pf
             ORDER BY pf.pefafimo DESC
        ) WHERE rownum <= 8
    """)
    print("Ventanas de periodo de facturacion que recorre el driver:")
    for p in periodos:
        print(f"  periodo {p[0]}: {p[1]} .. {p[2]}")

    # Cuantas ordenes caen DENTRO de alguna de esas ventanas.
    dentro = 0
    for i in range(0, len(ss), LOTE):
        chunk = ss[i:i + LOTE]
        marcas = ",".join(["?"] * len(chunk))
        filas, _ = q(f"""
            SELECT COUNT(*)
              FROM or_order_activity oa, or_order o
             WHERE o.order_id = oa.order_id
               AND oa.activity_id = 7400027
               AND oa.product_id IN ({marcas})
               AND EXISTS (SELECT 1 FROM (
                        SELECT * FROM (SELECT pf.pefafimo f1, pf.pefaffmo f2
                                         FROM perifact pf ORDER BY pf.pefafimo DESC)
                         WHERE rownum <= 8) v
                    WHERE o.created_date BETWEEN v.f1 AND v.f2)
        """, chunk)
        dentro += (filas[0][0] or 0)

    fuera = total_sin_filtro - dentro
    print(f"\n  Total de ordenes 7400027 : {total_sin_filtro:,}")
    print(f"  DENTRO de alguna ventana : {dentro:,}")
    print(f"  FUERA de toda ventana    : {fuera:,}  ({100.0*fuera/total_sin_filtro:.1f}%)")

    if dentro == 0:
        print("\n>>> H2 CONFIRMADA: el filtro de fecha las excluye TODAS.")
        print("    La orden de decision se crea cuando el analista decide, despues de que")
        print("    cerro el periodo. Hay que relacionarla por la investigacion que resuelve,")
        print("    no por su fecha de creacion. ES UN CAMBIO DE DISENO, no un ajuste.")
    elif dentro < total_sin_filtro:
        print(f"\n>>> H2 PARCIAL: {fuera:,} ordenes se pierden por la ventana de fecha.")
    else:
        print("\n>>> Ninguna se pierde por la fecha. Si Bronze sigue vacio, el problema")
        print("    esta en otro lado: revisa que la carga corrio con el codigo actual.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Paso 5 — Contexto: el universo completo en Oracle
# MAGIC
# MAGIC Para dimensionar. Si en Oracle hay decenas de miles pero cero para nuestros servicios,
# MAGIC el problema no es la query: es que la cadena carga una porción pequeña del universo.

# COMMAND ----------
filas, _ = q("""
    SELECT COUNT(*), COUNT(DISTINCT oa.product_id),
           TO_CHAR(MIN(o.created_date),'YYYY-MM-DD'), TO_CHAR(MAX(o.created_date),'YYYY-MM-DD')
      FROM or_order_activity oa, or_order o
     WHERE o.order_id = oa.order_id AND oa.activity_id = 7400027
""")
n, n_ss, fmin, fmax = filas[0]
print(f"  Ordenes 7400027 en TODO Oracle : {n:,}")
print(f"  Servicios distintos            : {n_ss:,}")
print(f"  Rango de fechas                : {fmin} .. {fmax}")
print(f"\n  Servicios de nuestra cadena    : {len(ss):,}")
print(f"  Con orden de decision          : {total_sin_filtro:,} ordenes")

# COMMAND ----------
# MAGIC %md ## Paso 6 — Cerrar la conexión

# COMMAND ----------
try:
    conn.close()
    print("Conexión cerrada.")
except Exception as e:                                          # noqa: BLE001
    print(f"Al cerrar: {type(e).__name__}: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cómo leer el resultado
# MAGIC
# MAGIC - **Paso 3 en cero** → la lógica está bien, no se toca la query. Para tener órdenes de
# MAGIC   decisión hay que ampliar la población de servicios que carga la cadena, que es una
# MAGIC   decisión de alcance, no un arreglo.
# MAGIC - **Paso 3 con datos y Paso 4 en cero** → el filtro de fecha las excluye todas. El
# MAGIC   arreglo **no** es quitar la ventana —volvería a duplicar la orden en cada iteración de
# MAGIC   periodo—, sino relacionarla por la investigación que resuelve. Es cambio de diseño.
# MAGIC - **Paso 4 parcial** → se pierde una fracción; hay que decidir si compensa cambiar el
# MAGIC   criterio de relación.
