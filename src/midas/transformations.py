"""
Orquestador de la capa Silver.

QUÉ CAMBIÓ RESPECTO DE LA VERSIÓN ANTERIOR
------------------------------------------
Antes: 4 `CREATE OR REPLACE TABLE` con el SQL inline, sin PKs, sin comentarios, sin
logging y sin manejo de errores. Si una tabla fallaba, la capa quedaba parcialmente
actualizada (unas de hoy, otras de ayer) y nadie se enteraba hasta que el agente
fallaba.

Ahora:
- El SQL vive en `src/midas/sql/silver/{ddl,load,view}/<query_key>.sql`.
- El orden de ejecución y las dependencias son DATO, en `midas_control_cargas` con
  `job_name = 'midas_silver'`. Activar o desactivar un objeto es un UPDATE, no un deploy.
- Cada objeto registra INICIADO/EXITOSO/FALLIDO en `midas_log_cargas`.
- Los errores se acumulan: un objeto que falla no impide los que no dependen de él.

TRES PATRONES DE MATERIALIZACIÓN, según `tipo_carga`:
- `SILVER_TABLE`  -> ddl/ (CREATE TABLE IF NOT EXISTS, idempotente) + load/ (INSERT OVERWRITE)
- `SILVER_VIEW`   -> view/ (CREATE OR REPLACE VIEW)
- `SILVER_LEGACY` -> load/ (CREATE OR REPLACE TABLE) — las 4 del Caso 1. Se orquestan y
  se loguean, pero NO se les cambia el patrón: arreglar eso no es de este trabajo.

Por qué las tablas nuevas usan INSERT OVERWRITE y no CREATE OR REPLACE TABLE: el
segundo recrea el objeto y pierde PK, NOT NULL, comentarios y grants; exige ser owner;
y deja a los lectores sin tabla durante la operación. Es el mismo argumento que ya está
escrito en `ingestion.py` para Bronze.
"""
import logging
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from .silver_params import cargar_parametros, resolver, sql_desnudo

log = logging.getLogger(__name__)

# tipo_carga -> subcarpeta de la que sale la sentencia de CARGA
_FASE_CARGA = {
    "SILVER_TABLE": "load",
    "SILVER_LEGACY": "load",
    "SILVER_VIEW": "view",
}
# Solo estos tienen DDL previo.
_CON_DDL = {"SILVER_TABLE"}


def _entero_o_none(valor) -> Optional[int]:
    """Normaliza un entero que viene de pandas.

    `get_active_tables()` hace `.toPandas()`, y pandas no tiene enteros nulos en su
    dtype por defecto: una columna BIGINT con NULLs se convierte a float64 y el NULL
    llega como **NaN, no como None**. Como `NaN is not None` es True, un chequeo
    ingenuo lee "sin padre" como "padre inexistente" y aborta la corrida entera.

    Silver es el primer consumidor real de `query_padre_id` — en Bronze la columna
    siempre fue informativa — así que este es el primer punto donde importa.
    """
    if valor is None:
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None
    return int(valor)


def orden_topologico(objetos: List[dict]) -> List[dict]:
    """Ordena por dependencias (`query_padre_id`), desempatando por `orden_ejecucion`.

    Falla ante un ciclo o un padre inactivo. Es preferible no correr a correr en un
    orden que construya una tabla sobre los datos de ayer.

    Devuelve copias con `id_carga` y `query_padre_id` ya normalizados a int/None, para
    que nadie corriente abajo tenga que volver a lidiar con los tipos de pandas.
    """
    objetos = [
        {**o,
         "id_carga": _entero_o_none(o["id_carga"]),
         "query_padre_id": _entero_o_none(o.get("query_padre_id")),
         "orden_ejecucion": _entero_o_none(o.get("orden_ejecucion")) or 0}
        for o in objetos
    ]

    por_id = {o["id_carga"]: o for o in objetos}
    for o in objetos:
        padre = o["query_padre_id"]
        if padre is not None and padre not in por_id:
            raise ValueError(
                f"{o['tabla_destino']}: su query_padre_id={padre} no está entre los objetos "
                f"activos. O el padre se desactivó, o quedó apuntando a otro job_name."
            )

    ordenado, visitando, visitado = [], set(), set()

    def visitar(obj):
        clave = obj["id_carga"]
        if clave in visitado:
            return
        if clave in visitando:
            raise ValueError(
                f"Ciclo de dependencias en el control de Silver, detectado en "
                f"'{obj['tabla_destino']}'. Revisa query_padre_id."
            )
        visitando.add(clave)
        padre = obj["query_padre_id"]
        if padre is not None:
            visitar(por_id[padre])
        visitando.discard(clave)
        visitado.add(clave)
        ordenado.append(obj)

    for obj in sorted(objetos, key=lambda o: (o["orden_ejecucion"], o["tabla_destino"])):
        visitar(obj)
    return ordenado


class SilverTransformer:
    def __init__(self, spark, catalog: str, schema: str, control, sql_dir: Optional[str] = None):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.control = control
        self.sql_dir = (Path(sql_dir) if sql_dir
                        else Path(__file__).resolve().parent / "sql" / "silver")
        self.ctx: Dict[str, str] = {}

    # ---------------------------------------------------------------- lectura
    def _leer_sql(self, fase: str, query_key: str) -> str:
        ruta = self.sql_dir / fase / f"{query_key}.sql"
        if not ruta.exists():
            raise FileNotFoundError(
                f"Falta {ruta}. El control declara el objeto '{query_key}' pero no hay SQL "
                f"para él: o se sembró de más, o el archivo no se sincronizó al workspace."
            )
        sql = ruta.read_text(encoding="utf-8").strip().rstrip(";").strip()
        # spark.sql ejecuta UNA sentencia: un ';' interno haría que la segunda mitad se
        # ignore en silencio. Se ignoran comentarios Y literales de texto: un COMMENT de
        # columna con punto y coma dentro es legítimo y frecuente.
        if ";" in sql_desnudo(sql):
            raise ValueError(f"{ruta}: un archivo = una sentencia (hay un ';' interno).")
        return resolver(sql, self.ctx, str(ruta))

    def _contar(self, tabla: str) -> Optional[int]:
        try:
            return self.spark.table(f"{self.catalog}.{self.schema}.{tabla}").count()
        except Exception:  # noqa: BLE001
            return None

    # -------------------------------------------------------------- ejecución
    def transform_bronze_to_silver(self) -> None:
        # Los .sql legacy usan nombres sin cualificar (se extrajeron verbatim); los
        # nuevos usan {catalog}.{schema}. El USE cubre a los primeros.
        self.spark.sql(f"USE CATALOG {self.catalog}")
        self.spark.sql(f"USE SCHEMA {self.schema}")

        self.ctx = {
            "catalog": self.catalog,
            "schema": self.schema,
            "run_id": self.control.run_id,
            **cargar_parametros(self.spark, self.catalog, self.schema),
        }
        permitir_vacia = self.ctx.get("p_permitir_carga_vacia", "false") == "true"

        objetos = self.control.get_active_tables().to_dict("records")
        if not objetos:
            raise RuntimeError(
                f"No hay objetos activos con job_name='{self.control.job_name}' en "
                f"midas_control_cargas. ¿Corrió la task crear_objetos?"
            )
        plan = orden_topologico(objetos)
        log.info("Plan Silver (%d objetos): %s", len(plan),
                 " -> ".join(o["tabla_destino"] for o in plan))

        # ── Fase 1: DDL. Idempotente, no toca datos, no se loguea como carga. ──
        for obj in plan:
            if obj["tipo_carga"] in _CON_DDL:
                self.spark.sql(self._leer_sql("ddl", obj["query_key"]))
                log.info("DDL OK: %s", obj["tabla_destino"])

        # ── Fase 2: cargas y vistas, en orden topológico, envueltas en control ──
        errores, fallidos = [], set()
        nombre_por_id = {o["id_carga"]: o["tabla_destino"] for o in plan}

        for obj in plan:
            tabla, query_key, tipo = obj["tabla_destino"], obj["query_key"], obj["tipo_carga"]
            # orden_topologico ya normalizó los ids: aquí es int o None, nunca NaN.
            padre = nombre_por_id.get(obj["query_padre_id"])

            if padre in fallidos:
                log.warning("[%s] OMITIDO: su padre '%s' falló.", tabla, padre)
                fallidos.add(tabla)
                errores.append((tabla, f"omitido: el padre '{padre}' falló"))
                continue

            fase = _FASE_CARGA.get(tipo)
            if fase is None:
                errores.append((tabla, f"tipo_carga desconocido: {tipo}"))
                fallidos.add(tabla)
                continue

            fecha_inicio = datetime.utcnow()
            self.control.log_inicio(tabla, query_key, obj["id_carga"], fecha_inicio)
            try:
                self.spark.sql(self._leer_sql(fase, query_key))
                n = None if tipo == "SILVER_VIEW" else self._contar(tabla)

                # Un INSERT OVERWRITE con Bronze vacía BORRA la Silver. Publicar cero
                # filas es peor que fallar: el agente respondería sobre la nada.
                if n == 0 and not permitir_vacia:
                    raise RuntimeError(
                        f"{tabla}: 0 filas tras la carga. Se aborta en vez de publicar la "
                        f"tabla vacía. Rollback: RESTORE TABLE "
                        f"{self.catalog}.{self.schema}.{tabla} VERSION AS OF <anterior>. "
                        f"Para permitirlo: midas_parametros.permitir_carga_vacia = true."
                    )

                self.control.log_exito(
                    tabla_destino=tabla, query_key=query_key, id_carga=obj["id_carga"],
                    fecha_inicio=fecha_inicio, filas_leidas=n, filas_escritas=n,
                )
                log.info("[%s] EXITOSO (%s)", tabla, "vista" if n is None else f"{n} filas")
            except Exception as exc:  # noqa: BLE001
                log.exception("[%s] FALLIDO", tabla)
                self.control.log_fallo(
                    tabla_destino=tabla, query_key=query_key, id_carga=obj["id_carga"],
                    fecha_inicio=fecha_inicio, mensaje_error=str(exc),
                )
                errores.append((tabla, str(exc)))
                fallidos.add(tabla)

        if errores:
            raise RuntimeError(f"Capa Silver incompleta: {errores}")
        log.info("== Capa Silver OK (%d objetos) ==", len(plan))
