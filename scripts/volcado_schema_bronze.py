# Databricks notebook source
# =============================================================================
# VOLCADO DE SCHEMA BRONZE — script de UN SOLO USO
#
# Para que `crear_objetos` gobierne el schema, este repo necesita un DDL explicito
# por tabla Bronze. Este script produce el insumo de esos DDL leyendo la realidad,
# en vez de escribirla a mano: `insertInto` es POSICIONAL, y un DDL con las columnas
# en otro orden NO falla — corre los valores en silencio.
#
# Vuelca DOS fuentes y las compara:
#   * el PARQUET, que es lo que produce la query de Oracle y por tanto manda sobre
#     el orden posicional;
#   * la TABLA actual, que aporta los tipos ya asentados en Unity Catalog.
#
# La comparacion no es un extra: la diferencia entre ambas es exactamente el
# diagnostico del fallo abierto (`estado_corte_facturable` ausente en la tabla).
#
# NO forma parte del pipeline. Se corre, se revisa la salida y se commitea el DDL.
# =============================================================================

dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo")
dbutils.widgets.text("schema_destino", "facturacion")
dbutils.widgets.text("source_catalog", "epm_datalake_vol_np")
dbutils.widgets.text("source_schema", "facturacion_vol")
dbutils.widgets.text("source_volume", "facturacion_bronze_vol")
dbutils.widgets.text("source_base_path", "midas_agent")

CATALOG = dbutils.widgets.get("catalog_destino")
SCHEMA = dbutils.widgets.get("schema_destino")
VOL = (f"/Volumes/{dbutils.widgets.get('source_catalog')}"
       f"/{dbutils.widgets.get('source_schema')}"
       f"/{dbutils.widgets.get('source_volume')}"
       f"/{dbutils.widgets.get('source_base_path')}")

# (tabla, archivo_parquet). Mismo orden que SEED en 00_creacion_objetos_midas.
#
# HALLAZGO de la corrida del 2026-08-11 (con los nombres de entonces, sin `c2`): los
# Parquet de las 6 tablas de la cadena Caso 1 estaban STALE — no traian las columnas que
# sus queries proyectan desde v3 (`estado_corte_facturable`, `anio_facturacion`,
# `fecha_ini_consumo`...). Esa es la causa real del UNRESOLVED_COLUMN, no un escritor
# externo. Por eso los DDL se reconstruyeron como Parquet + la cola de `_MIGRACION_V3`,
# y por eso la primera corrida del fork tiene que incluir `extraer_datos_oracle`.
TABLAS = [
    ("midas_ordenes_calidad_pendientes_c2_bronze",   "ordenes_calidad_pendientes.parquet"),
    ("midas_datos_basicos_producto_c2_bronze",       "datos_basicos_producto.parquet"),
    ("midas_datos_lecturas_producto_c2_bronze",      "datos_lecturas_producto.parquet"),
    ("midas_datos_consumos_producto_c2_bronze",      "datos_consumos_producto.parquet"),
    ("midas_datos_ordenes_previa_critica_c2_bronze", "datos_ordenes_previa_critica.parquet"),
    ("midas_datos_cometarios_ordenes_c2_bronze",     "datos_comentarios_ordenes.parquet"),
    ("midas_datos_cuentas_cobro_c2_bronze",          "datos_cuentas_cobro.parquet"),
    ("midas_datos_detalle_cargos_c2_bronze",         "datos_detalle_cargos.parquet"),
    ("midas_datos_detalle_solicitudes_c2_bronze",    "datos_detalle_solicitudes.parquet"),
    ("midas_datos_consumos_contrato_bronze",         "datos_consumos_contrato.parquet"),
    ("midas_datos_investigacion_consumo_bronze",     "datos_investigacion_consumo.parquet"),
    ("midas_datos_perdidas_no_operacionales_bronze", "perdidas_no_operacionales.parquet"),
]

print(f"catalogo : {CATALOG}.{SCHEMA}")
print(f"volumen  : {VOL}\n")


def cols_tabla(nombre):
    full = f"{CATALOG}.{SCHEMA}.{nombre}"
    if not spark.catalog.tableExists(full):
        return None
    return [(f.name, f.dataType.simpleString()) for f in spark.table(full).schema.fields]


def cols_parquet(archivo):
    try:
        df = spark.read.format("parquet").load(f"{VOL}/{archivo}")
    except Exception as e:                                       # noqa: BLE001
        return f"ERROR: {type(e).__name__}: {e}"
    # Espeja lo que hace ingestion.load_parquet_to_delta antes de escribir.
    cols = [c.lower() for c in df.columns]
    tipos = dict(zip(cols, [f.dataType.simpleString() for f in df.schema.fields]))
    cols = [c for c in cols if c != "iteration"]
    return [(c, tipos[c]) for c in cols]


for tabla, archivo in TABLAS:
    print("=" * 100)
    print(f"### {tabla}")
    print("=" * 100)

    t = cols_tabla(tabla)
    p = cols_parquet(archivo)

    if isinstance(p, str):
        print(f"  PARQUET -> {p}")
        p = None
    if t is None:
        print("  TABLA   -> NO EXISTE")

    if p is not None:
        print(f"\n  PARQUET ({len(p)} columnas, EN ORDEN — manda sobre la posicion):")
        for i, (c, ty) in enumerate(p):
            print(f"    {i:>3}  {c:<42} {ty}")

    if t is not None:
        print(f"\n  TABLA ({len(t)} columnas, EN ORDEN):")
        for i, (c, ty) in enumerate(t):
            print(f"    {i:>3}  {c:<42} {ty}")

    # La comparacion: lo que de verdad importa para el insertInto posicional.
    if p is not None and t is not None:
        np_, nt = [c for c, _ in p], [c for c, _ in t]
        solo_parquet = [c for c in np_ if c not in nt]
        solo_tabla = [c for c in nt if c not in np_]
        print("\n  DIFERENCIA:")
        if not solo_parquet and not solo_tabla and np_ == nt:
            print("    OK  identicas y en el mismo orden")
        else:
            if solo_parquet:
                print(f"    !!  solo en PARQUET : {solo_parquet}")
            if solo_tabla:
                print(f"    !!  solo en TABLA   : {solo_tabla}")
            if not solo_parquet and not solo_tabla and np_ != nt:
                print("    !!  MISMAS columnas pero DISTINTO ORDEN — insertInto las corre "
                      "sin lanzar error")
    print()

print("=" * 100)
print("FIN. Copia TODA la salida y pasala al chat.")
print("=" * 100)
