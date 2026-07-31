"""
Resolución de parámetros y placeholders para el SQL externalizado de Silver.

POR QUÉ FORMATEO EN PYTHON Y NO SUBCONSULTA ESCALAR
---------------------------------------------------
La alternativa obvia sería `(SELECT valor FROM midas_parametros WHERE clave='x')`
dentro del SQL. No sirve, por dos razones que no son de estilo:

1. **No cubre todos los sitios.** El bound del frame de una ventana
   (`ROWS BETWEEN N PRECEDING AND 1 PRECEDING`) y las listas `IN (...)` son
   literales en tiempo de parseo: no admiten subconsulta. Con ese mecanismo habría
   que cablear justo los valores más discutibles — entre ellos la ventana de 8
   periodos, que negocio pidió explícitamente que fuera parametrizable. Un
   mecanismo que cubre el 70% obliga a un segundo mecanismo para el 30% restante,
   y dos mecanismos es drift garantizado.

2. **Falla en silencio.** Una subconsulta con la clave ausente devuelve NULL, el
   predicado se vuelve NULL, las filas desaparecen y la tabla queda incompleta
   SIN lanzar error. Aquí una clave faltante es un KeyError con nombre de archivo.

Invariante I17: ningún umbral, código de catálogo ni ventana temporal se cablea en
el SQL. Todos salen de midas_parametros.
"""
import logging
import re
from typing import Dict

log = logging.getLogger(__name__)

# Un placeholder es {nombre_valido}. Las llaves literales del SQL deben escribirse
# duplicadas ({{ }}) — el validador de `resolver` las delata si no lo están.
_PLACEHOLDER = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")

# Centinela para parámetros sembrados pero NO confirmados por negocio (activo=false).
# No matchea ningún código real, así que la feature que dependa de él sale NULL en vez
# de un número calculado con un valor inventado. Es visible en el log y en el SQL
# resuelto, de modo que "esta columna está en NULL" tiene una causa rastreable.
SIN_PARAMETRIZAR = "__SIN_PARAMETRIZAR__"

_TIPOS_ENTEROS = {"INT", "INTEGER", "BIGINT", "LONG", "SMALLINT"}
_TIPOS_DECIMALES = {"DOUBLE", "FLOAT", "DECIMAL", "NUMERIC"}
_TIPOS_BOOLEANOS = {"BOOLEAN", "BOOL"}


def sql_desnudo(sql: str) -> str:
    """El SQL sin comentarios `--` ni literales entre comillas simples.

    Sirve para buscar estructura (un ';' de más, un TEMP VIEW) sin que un punto y coma
    dentro de un COMMENT de columna dispare un falso positivo. Los `COMMENT 'texto;'`
    son frecuentes y legítimos.
    """
    sin_comentarios = "\n".join(
        l for l in sql.splitlines() if not l.strip().startswith("--")
    )
    # '' es la comilla escapada dentro de un literal: se consume junto con el literal.
    return re.sub(r"'(?:[^']|'')*'", "''", sin_comentarios)


def _literal_sql(valor: str, tipo_dato: str, clave: str) -> str:
    """Convierte el valor de midas_parametros en un literal SQL seguro.

    Los numéricos se VALIDAN antes de interpolarse: es la única defensa contra que
    alguien edite la tabla de parámetros a mano y meta texto arbitrario en algo que
    va a concatenarse dentro de una sentencia SQL.
    """
    tipo = (tipo_dato or "").strip().upper()
    texto = "" if valor is None else str(valor).strip()

    if tipo in _TIPOS_ENTEROS:
        try:
            return str(int(texto))
        except (TypeError, ValueError):
            raise ValueError(
                f"midas_parametros: la clave '{clave}' declara tipo_dato={tipo_dato} "
                f"pero su valor {texto!r} no es un entero."
            )
    if tipo in _TIPOS_DECIMALES:
        try:
            return repr(float(texto))
        except (TypeError, ValueError):
            raise ValueError(
                f"midas_parametros: la clave '{clave}' declara tipo_dato={tipo_dato} "
                f"pero su valor {texto!r} no es un número."
            )
    if tipo in _TIPOS_BOOLEANOS:
        if texto.lower() in ("true", "1", "si", "sí"):
            return "true"
        if texto.lower() in ("false", "0", "no"):
            return "false"
        raise ValueError(
            f"midas_parametros: la clave '{clave}' declara tipo_dato={tipo_dato} "
            f"pero su valor {texto!r} no es booleano."
        )
    # STRING y cualquier otro: se escapa la comilla simple y se comilla.
    return "'" + texto.replace("'", "''") + "'"


def cargar_parametros(spark, catalog: str, schema: str) -> Dict[str, str]:
    """midas_parametros -> {'p_<clave>': '<literal SQL>'}.

    Los parámetros con activo=false devuelven el centinela SIN_PARAMETRIZAR.
    """
    filas = spark.sql(f"""
        SELECT dominio, clave, valor, tipo_dato, activo
        FROM {catalog}.{schema}.midas_parametros
    """).collect()

    ctx, inactivos = {}, []
    for fila in filas:
        clave = fila["clave"]
        destino = f"p_{clave}"
        if destino in ctx:
            raise ValueError(
                f"midas_parametros: la clave '{clave}' está repetida en más de un dominio. "
                f"El placeholder {{{destino}}} sería ambiguo."
            )
        if not fila["activo"]:
            ctx[destino] = f"'{SIN_PARAMETRIZAR}'"
            inactivos.append(clave)
            continue
        ctx[destino] = _literal_sql(fila["valor"], fila["tipo_dato"], clave)

    log.info("Parámetros cargados: %d (%d inactivos: %s)",
             len(ctx), len(inactivos), ", ".join(sorted(inactivos)) or "ninguno")
    return ctx


def resolver(sql: str, ctx: Dict[str, str], origen: str) -> str:
    """Sustituye los placeholders y FALLA listando los que no pudo resolver.

    `origen` es la ruta del .sql: sin ella, un KeyError en una capa de 13 objetos es
    inútil para diagnosticar.
    """
    faltantes = set(_PLACEHOLDER.findall(sql)) - set(ctx)
    if faltantes:
        raise KeyError(
            f"{origen}: placeholders sin resolver {sorted(faltantes)}. "
            f"O falta la fila en midas_parametros, o hay una llave literal en el SQL "
            f"que debe escribirse duplicada ({{{{ }}}})."
        )
    return sql.format_map(ctx)
