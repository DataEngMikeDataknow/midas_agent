"""
Tests de la capa Silver.

El test anterior hacía substring matching sobre los strings SQL inline de
`transformations.py`: pasaba aunque el SQL fuera sintácticamente inválido, porque el
único requisito era que el nombre de la tabla apareciera en algún lugar de algún string.
Ese SQL ya no existe (vive en `src/midas/sql/silver/`), así que aquí se verifica lo que
sí se puede verificar sin un cluster:

- Un archivo = una sentencia, y sin TEMP VIEW.
- Todo placeholder tiene nombre válido y es resoluble.
- El orden topológico respeta las dependencias y detecta ciclos.
- El SQL legacy del Caso 1 conserva su patrón de materialización.
- Los parámetros numéricos se validan antes de interpolarse en SQL.
"""
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.modules.setdefault("pyspark", MagicMock())
sys.modules.setdefault("pyspark.sql", MagicMock())

from src.midas.silver_params import (  # noqa: E402
    SIN_PARAMETRIZAR, _literal_sql, resolver, sql_desnudo,
)
from src.midas.transformations import SilverTransformer, orden_topologico  # noqa: E402

SQL_DIR = Path(__file__).resolve().parents[1] / "src" / "midas" / "sql" / "silver"
LEGACY = [
    "midas_datos_basicos_producto_silver",
    "midas_ordenes_calidad_pendientes_silver",
    "midas_historial_critica_silver",
    "midas_historial_facturacion_silver",
]


def _codigo(texto: str) -> str:
    """El SQL sin comentarios ni literales de texto. Los comentarios mencionan cosas
    (TEMP VIEW, CREATE OR REPLACE) y los COMMENT de columna llevan puntos y coma:
    ninguno de los dos debe confundirse con el código."""
    return sql_desnudo(texto)


class TestArchivosSql(unittest.TestCase):
    def test_un_archivo_una_sentencia(self):
        """spark.sql ejecuta UNA sentencia: un ';' interno haría que la segunda mitad
        se ignore en silencio."""
        for ruta in SQL_DIR.rglob("*.sql"):
            with self.subTest(ruta=ruta.name):
                self.assertNotIn(";", _codigo(ruta.read_text(encoding="utf-8")))

    def test_sin_temp_view(self):
        """Todo lo intermedio va en CTEs: una TEMP VIEW obligaría a más de una sentencia."""
        for ruta in SQL_DIR.rglob("*.sql"):
            with self.subTest(ruta=ruta.name):
                self.assertNotIn("TEMP VIEW", _codigo(ruta.read_text(encoding="utf-8")).upper())

    def test_punto_y_coma_en_comentario_no_es_falso_positivo(self):
        """Un COMMENT de columna con ';' dentro es legítimo: el DDL de historial_consumo
        tiene varios y no debe leerse como más de una sentencia."""
        ddl = (SQL_DIR / "ddl" / "midas_historial_consumo_silver.sql").read_text(encoding="utf-8")
        self.assertIn(";", ddl, "el caso de regresión perdió su punto y coma")
        self.assertNotIn(";", sql_desnudo(ddl))

    def test_toda_estrella_calificada_tiene_su_alias(self):
        """REGRESIÓN (dllo, 2026-07-31): `SELECT b.*` con `FROM base` sin alias.

        No es un parser de SQL: solo verifica los `alias.*`, que es donde el error se
        paga caro. Un alias mal escrito en una columna suelta lo atrapa el analyzer
        rápido; un `x.*` inexistente tumba el objeto entero y a todos sus hijos."""
        palabras = {"select", "from", "where", "join", "left", "right", "inner", "outer",
                    "on", "and", "or", "group", "order", "by", "as", "when", "then",
                    "else", "end", "case", "over", "partition", "window", "insert",
                    "overwrite", "table", "into", "name", "create", "replace", "view",
                    "not", "null", "is", "in", "using", "distinct", "having", "cross"}
        for ruta in SQL_DIR.rglob("*.sql"):
            cod = _codigo(ruta.read_text(encoding="utf-8"))
            definidos = {m.lower() for m in
                         re.findall(r"(?:^|[\s,(])(\w+)\s+AS\s*\(", cod, re.I | re.M)}
            for m in re.finditer(r"\b(?:FROM|JOIN)\s+([\w{}.]+)(?:\s+(?:AS\s+)?(\w+))?",
                                 cod, re.I):
                definidos.add(m.group(1).split(".")[-1].lower())
                if m.group(2) and m.group(2).lower() not in palabras:
                    definidos.add(m.group(2).lower())
            for calificador in {m.lower() for m in re.findall(r"\b(\w+)\.\*", cod)}:
                with self.subTest(ruta=ruta.name, alias=calificador):
                    self.assertIn(calificador, definidos,
                                  f"{ruta.name}: usa `{calificador}.*` sin definirlo")

    def test_placeholders_con_nombre_valido(self):
        """Una llave literal sin duplicar reventaría en runtime dentro del cluster."""
        patron = re.compile(r"\{([^}]*)\}")
        for ruta in SQL_DIR.rglob("*.sql"):
            for nombre in patron.findall(ruta.read_text(encoding="utf-8")):
                with self.subTest(ruta=ruta.name, ph=nombre):
                    self.assertRegex(nombre, r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class TestLegacyCaso1(unittest.TestCase):
    """Las 4 del Caso 1 se orquestan y se loguean, pero NO cambian de patrón."""

    def test_conservan_create_or_replace_table(self):
        for nombre in LEGACY:
            ruta = SQL_DIR / "load" / f"{nombre}.sql"
            with self.subTest(tabla=nombre):
                self.assertTrue(ruta.exists(), f"falta {ruta}")
                self.assertIn("CREATE OR REPLACE TABLE",
                              _codigo(ruta.read_text(encoding="utf-8")).upper())

    def test_no_tienen_ddl(self):
        """Solo las tablas NUEVAS llevan DDL: las legacy se recrean con su CTAS."""
        for nombre in LEGACY:
            with self.subTest(tabla=nombre):
                self.assertFalse((SQL_DIR / "ddl" / f"{nombre}.sql").exists())


class TestOrdenTopologico(unittest.TestCase):
    @staticmethod
    def _obj(id_carga, tabla, padre=None, orden=0):
        return {"id_carga": id_carga, "tabla_destino": tabla, "query_padre_id": padre,
                "orden_ejecucion": orden, "tipo_carga": "SILVER_TABLE", "query_key": tabla}

    def test_el_padre_va_antes_que_el_hijo(self):
        # El hijo tiene orden MENOR que el padre a propósito: la dependencia manda.
        objetos = [self._obj(2, "hijo", padre=1, orden=1), self._obj(1, "padre", orden=9)]
        nombres = [o["tabla_destino"] for o in orden_topologico(objetos)]
        self.assertLess(nombres.index("padre"), nombres.index("hijo"))

    def test_desempata_por_orden_ejecucion(self):
        objetos = [self._obj(2, "b", orden=20), self._obj(1, "a", orden=10)]
        self.assertEqual([o["tabla_destino"] for o in orden_topologico(objetos)], ["a", "b"])

    def test_detecta_ciclo(self):
        objetos = [self._obj(1, "a", padre=2), self._obj(2, "b", padre=1)]
        with self.assertRaisesRegex(ValueError, "Ciclo"):
            orden_topologico(objetos)

    def test_falla_si_el_padre_no_esta_activo(self):
        """Correr un hijo cuyo padre se desactivó lo construiría sobre datos de ayer."""
        with self.assertRaisesRegex(ValueError, "no está entre los objetos"):
            orden_topologico([self._obj(2, "hijo", padre=99)])

    def test_padre_nulo_de_pandas_no_es_padre_inexistente(self):
        """REGRESIÓN (dllo, 2026-07-31): `get_active_tables()` hace `.toPandas()`, y
        pandas no tiene enteros nulos en su dtype por defecto: una columna BIGINT con
        NULLs pasa a float64 y el NULL llega como NaN. `NaN is not None` es True, así
        que "sin padre" se leía como "padre inexistente" y abortaba la capa entera
        antes de ejecutar un solo objeto."""
        import pandas as pd

        # Construido como lo entrega .toPandas(): la columna con un NULL es float64.
        df = pd.DataFrame([
            {"id_carga": 1, "tabla_destino": "sin_padre", "query_padre_id": None,
             "orden_ejecucion": 31, "tipo_carga": "SILVER_LEGACY", "query_key": "sin_padre"},
            {"id_carga": 2, "tabla_destino": "con_padre", "query_padre_id": 1,
             "orden_ejecucion": 61, "tipo_carga": "SILVER_VIEW", "query_key": "con_padre"},
        ])
        self.assertEqual(df["query_padre_id"].dtype.kind, "f",
                         "el caso de regresión perdió su NaN: pandas ya no usa float64")

        plan = orden_topologico(df.to_dict("records"))
        self.assertEqual([o["tabla_destino"] for o in plan], ["sin_padre", "con_padre"])
        # Y quedan normalizados para quien los consuma después.
        self.assertIsNone(plan[0]["query_padre_id"])
        self.assertEqual(plan[1]["query_padre_id"], 1)
        self.assertIsInstance(plan[1]["id_carga"], int)


class TestParametros(unittest.TestCase):
    def test_valida_los_numericos(self):
        """La única defensa contra que alguien edite midas_parametros a mano y meta
        texto arbitrario en algo que se concatena dentro de una sentencia SQL."""
        self.assertEqual(_literal_sql("8", "INT", "v"), "8")
        with self.assertRaises(ValueError):
            _literal_sql("ocho", "INT", "ventana_periodos_analisis")
        with self.assertRaises(ValueError):
            _literal_sql("1 OR 1=1", "INT", "clave")

    def test_escapa_las_cadenas(self):
        self.assertEqual(_literal_sql("PR", "STRING", "t"), "'PR'")
        self.assertEqual(_literal_sql("O'Brien", "STRING", "t"), "'O''Brien'")

    def test_resolver_falla_con_placeholder_desconocido(self):
        with self.assertRaises(KeyError) as ctx:
            resolver("SELECT {p_inexistente}", {"catalog": "c"}, "load/x.sql")
        # El mensaje debe decir QUÉ falta y DÓNDE: en una capa de 13 objetos, un
        # KeyError pelado no sirve para diagnosticar.
        self.assertIn("p_inexistente", str(ctx.exception))
        self.assertIn("load/x.sql", str(ctx.exception))

    def test_resolver_sustituye(self):
        sql = resolver("FROM {catalog}.{schema}.t WHERE m = {p_metodo}",
                       {"catalog": "c", "schema": "s", "p_metodo": "4"}, "x")
        self.assertEqual(sql, "FROM c.s.t WHERE m = 4")


class TestOrquestador(unittest.TestCase):
    def test_rechaza_archivo_con_varias_sentencias(self):
        t = SilverTransformer(MagicMock(), "cat", "sch", MagicMock(), sql_dir=str(SQL_DIR))
        t.ctx = {"catalog": "cat", "schema": "sch"}
        ruta = SQL_DIR / "load" / "midas_datos_basicos_producto_silver.sql"
        original = ruta.read_text(encoding="utf-8")
        try:
            ruta.write_text(original + "\nSELECT 1;\nSELECT 2\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "una sentencia"):
                t._leer_sql("load", "midas_datos_basicos_producto_silver")
        finally:
            ruta.write_text(original, encoding="utf-8")

    def test_falla_si_falta_el_sql_declarado(self):
        t = SilverTransformer(MagicMock(), "cat", "sch", MagicMock(), sql_dir=str(SQL_DIR))
        t.ctx = {}
        with self.assertRaisesRegex(FileNotFoundError, "no hay SQL"):
            t._leer_sql("load", "objeto_que_no_existe")

    def test_centinela_para_parametros_sin_confirmar(self):
        """Un parámetro inactivo no debe producir un número inventado."""
        self.assertEqual(SIN_PARAMETRIZAR, "__SIN_PARAMETRIZAR__")


def _seed_silver():
    """(tabla_destino, tipo_carga) de SEED_SILVER, leído del notebook de creación.

    El notebook es la fuente de verdad de qué objetos existen; los .sql son la
    implementación. Si divergen, el job falla en el cluster con un FileNotFoundError
    que ya nadie relaciona con un commit. Aquí falla en CI."""
    texto = (Path(__file__).resolve().parents[1]
             / "notebooks" / "00_creacion_objetos_midas.py").read_text(encoding="utf-8")
    ini = texto.index("SEED_SILVER = [")
    bloque = texto[ini:texto.index("\n]", ini)]
    # [A-Z_]+ y no [A-Z]+: SILVER_TABLE_ADOPTADA lleva guion bajo interno.
    filas = re.findall(r'\(\s*"([^"]+)"\s*,\s*"([^"]+)"\s*,\s*"(SILVER_[A-Z_]+)"', bloque)
    return [(tabla, key, tipo) for tabla, key, tipo in filas]


class TestSeedContraArchivos(unittest.TestCase):
    def test_el_seed_no_esta_vacio(self):
        """Si el regex deja de casar, los demás tests de esta clase pasarían en vacío."""
        self.assertEqual(len(_seed_silver()), 13)

    def test_todo_objeto_sembrado_tiene_su_sql(self):
        for tabla, key, tipo in _seed_silver():
            carpeta = "view" if tipo == "SILVER_VIEW" else "load"
            with self.subTest(tabla=tabla):
                self.assertEqual(tabla, key, "el query_key debe ser el nombre del archivo")
                self.assertTrue((SQL_DIR / carpeta / f"{key}.sql").exists(),
                                f"falta {carpeta}/{key}.sql")

    def test_toda_tabla_nueva_tiene_ddl_y_carga(self):
        """SILVER_TABLE = DDL + INSERT OVERWRITE (I18), nunca CREATE OR REPLACE TABLE."""
        for tabla, key, tipo in _seed_silver():
            if tipo != "SILVER_TABLE":
                continue
            with self.subTest(tabla=tabla):
                self.assertTrue((SQL_DIR / "ddl" / f"{key}.sql").exists())
                codigo = _codigo((SQL_DIR / "load" / f"{key}.sql").read_text(encoding="utf-8")).upper()
                self.assertIn("INSERT OVERWRITE", codigo)
                self.assertNotIn("CREATE OR REPLACE TABLE", codigo)

    def test_la_tabla_adoptada_no_declara_ddl(self):
        """Una tabla ADOPTADA no es nuestra: su schema lo fija su dueño.

        Declararle un `CREATE TABLE IF NOT EXISTS` sería peor que inútil — contra una
        tabla que ya existe no hace nada, no falla, y deja creído que el contrato es el
        nuestro. Y debe cargar con BY NAME: si el dueño le cambia una columna, queremos
        que falle, no que escriba los valores corridos."""
        adoptadas = [k for _, k, t in _seed_silver() if t == "SILVER_TABLE_ADOPTADA"]
        self.assertEqual(adoptadas, ["midas_datos_detalle_solicitudes_silver"])
        for key in adoptadas:
            with self.subTest(tabla=key):
                self.assertFalse((SQL_DIR / "ddl" / f"{key}.sql").exists(),
                                 "una tabla adoptada no lleva DDL propio")
                self.assertFalse((SQL_DIR / "view" / f"{key}.sql").exists(),
                                 "quedó el .sql de vista de cuando era SILVER_VIEW")
                codigo = _codigo((SQL_DIR / "load" / f"{key}.sql").read_text(encoding="utf-8")).upper()
                self.assertIn("INSERT OVERWRITE", codigo)
                self.assertIn("BY NAME", codigo)

    def test_todo_tipo_carga_sembrado_lo_conoce_el_orquestador(self):
        """Un tipo_carga que el orquestador no mapea se registra como fallo en la
        bitácora y el objeto nunca se construye."""
        from src.midas.transformations import _FASE_CARGA
        for tabla, _, tipo in _seed_silver():
            with self.subTest(tabla=tabla):
                self.assertIn(tipo, _FASE_CARGA)

    def test_no_hay_sql_huerfano(self):
        """Un .sql que nadie sembró no se ejecuta nunca: es código muerto que aparenta vivir."""
        sembrados = {key for _, key, _ in _seed_silver()}
        for carpeta in ("load", "view"):
            for ruta in (SQL_DIR / carpeta).glob("*.sql"):
                with self.subTest(ruta=f"{carpeta}/{ruta.name}"):
                    self.assertIn(ruta.stem, sembrados)


class TestNivel2(unittest.TestCase):
    """I15: el número de actividad vive en UN solo objeto."""

    NIVEL2 = "midas_ordenes_variacion_consumo_silver"

    def test_es_el_unico_que_filtra_por_actividad(self):
        for ruta in SQL_DIR.rglob("*.sql"):
            if ruta.stem == self.NIVEL2:
                continue
            with self.subTest(ruta=ruta.name):
                self.assertNotIn("p_actividad_", _codigo(ruta.read_text(encoding="utf-8")))

    def test_no_cablea_el_numero(self):
        codigo = _codigo((SQL_DIR / "view" / f"{self.NIVEL2}.sql").read_text(encoding="utf-8"))
        self.assertIn("{p_actividad_variacion_consumo}", codigo)
        self.assertNotIn("993", codigo, "el número debe salir de midas_parametros, no del SQL")

    def test_extrae_el_codigo_con_regexp_no_con_split(self):
        """Conviven DOS formatos ('993 - X' con espacios y '102010-X' sin ellos), y
        SPLIT(x,'-')[0] devuelve cadena vacía para códigos negativos."""
        codigo = _codigo((SQL_DIR / "view" / f"{self.NIVEL2}.sql").read_text(encoding="utf-8"))
        self.assertIn("REGEXP_EXTRACT", codigo)
        self.assertNotIn("SPLIT(", codigo.upper())

    def test_lee_de_silver_no_de_bronze(self):
        """Nivel 2 se apoya en Nivel 1; saltarse la capa duplicaría el LEFT JOIN."""
        codigo = _codigo((SQL_DIR / "view" / f"{self.NIVEL2}.sql").read_text(encoding="utf-8"))
        self.assertNotIn("_bronze", codigo)


class TestCaso1Aditivo(unittest.TestCase):
    """I13: lo del Caso 1 solo crece por el final, y sin cambiar el conteo de filas."""

    RUTA = SQL_DIR / "load" / "midas_ordenes_calidad_pendientes_silver.sql"

    def test_las_columnas_nuevas_van_al_final(self):
        codigo = _codigo(self.RUTA.read_text(encoding="utf-8"))
        proyeccion = codigo[codigo.upper().index("SELECT"):codigo.upper().index("FROM ")]
        columnas = [c.strip() for c in proyeccion.split(",")]
        self.assertEqual(columnas[-2:],
                         ["p.estado_corte_facturable", "p.estado_corte_facturable_desc"])

    def test_no_agrega_joins_ni_filtros(self):
        """El conteo de filas debe ser idéntico antes y después: las columnas nuevas
        salen del LEFT JOIN que ya existía."""
        codigo = _codigo(self.RUTA.read_text(encoding="utf-8")).upper()
        self.assertEqual(codigo.count("JOIN"), 1)
        self.assertIn("LEFT JOIN", codigo)
        self.assertNotIn("WHERE", codigo)


class TestNotebookValidacion(unittest.TestCase):
    """El notebook 32 declara su contrato LITERAL (es autocontenido a propósito: así
    detecta drift entre el repo y lo desplegado). El precio es que puede quedarse atrás
    del DDL y reportar FALLAs que no existen. Esto lo ancla."""

    NB = Path(__file__).resolve().parents[1] / "notebooks" / "32_validacion_silver_caso2.py"

    @classmethod
    def setUpClass(cls):
        import ast
        arbol = ast.parse(cls.NB.read_text(encoding="utf-8"))
        cls.const = {}
        for nodo in arbol.body:
            if isinstance(nodo, ast.Assign) and isinstance(nodo.targets[0], ast.Name):
                try:
                    cls.const[nodo.targets[0].id] = ast.literal_eval(nodo.value)
                except ValueError:
                    pass

    @staticmethod
    def _columnas_ddl(tabla):
        texto = (SQL_DIR / "ddl" / f"{tabla}.sql").read_text(encoding="utf-8")
        return set(re.findall(
            r"^\s{4}(\w+)\s+(?:BIGINT|STRING|DOUBLE|BOOLEAN|DATE|TIMESTAMP|INT|ARRAY)",
            texto, re.M))

    def test_el_notebook_declara_los_13_objetos(self):
        self.assertEqual(len(self.const["OBJETOS"]), 13)
        self.assertEqual({t for t, _ in self.const["OBJETOS"]},
                         {t for t, _, _ in _seed_silver()})

    def test_el_grano_del_notebook_existe_en_el_ddl(self):
        for tabla, claves in self.const["GRANO"].items():
            cols = self._columnas_ddl(tabla)
            with self.subTest(tabla=tabla):
                self.assertEqual([c for c in claves if c not in cols], [])

    def test_las_columnas_de_features_existen(self):
        cols = self._columnas_ddl("midas_features_consumo_silver")
        referidas = [c for c, _ in self.const["NULL_ESPERADO"]] + self.const["CON_SENAL"]
        self.assertEqual([c for c in referidas if c not in cols], [])

    def test_las_columnas_null_esperado_dependen_de_un_parametro_inactivo(self):
        """Si alguien activa el parámetro, la columna deja de ser NULL y el notebook
        empezaría a reportar FALLA sobre algo que en realidad se arregló."""
        texto = (Path(__file__).resolve().parents[1]
                 / "notebooks" / "00_creacion_objetos_midas.py").read_text(encoding="utf-8")
        for _, param in self.const["NULL_ESPERADO"]:
            with self.subTest(param=param):
                fila = re.search(rf'"{param}",.*?(True|False)\)', texto, re.S)
                self.assertIsNotNone(fila, f"{param} no está sembrado")
                self.assertEqual(fila.group(1), "False",
                                 f"{param} quedó activo: actualiza el notebook 32")


if __name__ == "__main__":
    unittest.main()
