"""
Tests del hallazgo F02: un fallo de extracción NO puede terminar publicando datos viejos.

El modo de fallo, confirmado en el repo antes de arreglarlo:

    Oracle falla  ->  execute_query devolvía DataFrame vacío
                  ->  save_to_parquet veía vacío y NO escribía
                  ->  el Parquet de AYER sobrevivía (nombre fijo)
                  ->  chain_runner registraba EXITOSO con 0 filas
                  ->  la ingesta cargaba los datos de ayer como si fueran de hoy

Job verde, datos viejos, nadie se entera. Ya se manifestó una vez: los 782 "huérfanos"
de servicios_contrato que en la v3 se atribuyeron a "residuo de corridas anteriores".

NO se instala pyarrow para estos tests (decisión previa: las dependencias de test se
mantienen mínimas). Se parchea `to_parquet`, que además es lo correcto: lo que hay que
probar es NUESTRA lógica de cuándo escribir y cuándo fallar, no el escritor de Parquet.
La guarda de frescura mira `mtime`, así que le basta un archivo cualquiera.
"""
import os
import sys
import time
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

sys.modules.setdefault("pyspark", MagicMock())
sys.modules.setdefault("pyspark.sql", MagicMock())

from src.midas.db.processing import save_to_parquet  # noqa: E402
from src.midas.main_ingestion import antiguedad_horas, verificar_frescura  # noqa: E402

SCRATCH = os.environ.get("TMPDIR") or os.environ.get("TEMP") or "."


class TestSaveToParquet(unittest.TestCase):
    RUTA = "/Volumes/x/y/z/tabla.parquet"

    def test_escribe_el_vacio_con_columnas(self):
        """Un resultado legítimamente vacío SE ESCRIBE, para no dejar el de ayer.

        Es el corazón del arreglo: antes se devolvía sin escribir y el archivo previo
        —con el mismo nombre— seguía ahí esperando a que la ingesta lo leyera."""
        df = pd.DataFrame(columns=["servicio_suscrito", "consumo"])
        with patch.object(pd.DataFrame, "to_parquet") as escribir:
            save_to_parquet(df, self.RUTA)
        escribir.assert_called_once()
        self.assertEqual(escribir.call_args.args[0], self.RUTA)

    def test_escribe_tambien_cuando_hay_filas(self):
        with patch.object(pd.DataFrame, "to_parquet") as escribir:
            save_to_parquet(pd.DataFrame({"a": [1, 2, 3]}), self.RUTA)
        escribir.assert_called_once()

    def test_lanza_sin_columnas(self):
        """Un DataFrame sin columnas ya no debería llegar: execute_query lanza.

        Si llega, es que alguien construyó un vacío a mano — exactamente el patrón que
        producía el bug. Se rechaza en vez de escribir un Parquet sin schema."""
        with patch.object(pd.DataFrame, "to_parquet") as escribir:
            with self.assertRaises(ValueError):
                save_to_parquet(pd.DataFrame(), self.RUTA)
        escribir.assert_not_called()

    def test_lanza_con_none(self):
        with self.assertRaises(ValueError):
            save_to_parquet(None, self.RUTA)

    def test_lanza_si_no_puede_escribir(self):
        """Antes se logueaba y se seguía, dejando el archivo viejo intacto."""
        with patch.object(pd.DataFrame, "to_parquet",
                          side_effect=OSError("disco lleno")):
            with self.assertRaises(RuntimeError) as ctx:
                save_to_parquet(pd.DataFrame({"a": [1]}), self.RUTA)
        self.assertIn("disco lleno", str(ctx.exception))


class TestGuardaDeFrescura(unittest.TestCase):
    def setUp(self):
        self.ruta = os.path.join(SCRATCH, f"midas_fresco_{os.getpid()}.parquet")
        with open(self.ruta, "wb") as fh:
            fh.write(b"contenido irrelevante: la guarda mira el mtime")
        self.config = [{"name": "midas_tabla_bronze", "path": self.ruta}]

    def tearDown(self):
        if os.path.exists(self.ruta):
            os.remove(self.ruta)

    def test_acepta_un_parquet_recien_escrito(self):
        self.assertEqual(verificar_frescura(self.config, max_horas=12), [])

    def test_rechaza_un_parquet_viejo(self):
        """Se envejece el archivo 25h: es el Parquet de ayer con el nombre de hoy."""
        viejo = time.time() - 25 * 3600
        os.utime(self.ruta, (viejo, viejo))
        rancios = verificar_frescura(self.config, max_horas=12)
        self.assertEqual(len(rancios), 1)
        self.assertEqual(rancios[0][0], "midas_tabla_bronze")
        self.assertIn("corrida anterior", rancios[0][1])

    def test_rechaza_un_parquet_que_no_existe(self):
        """La extracción nunca escribió y tampoco había archivo previo."""
        rancios = verificar_frescura([{"name": "t", "path": self.ruta + ".nope"}],
                                     max_horas=12)
        self.assertEqual(len(rancios), 1)
        self.assertIn("no existe", rancios[0][1])

    def test_reporta_TODOS_los_rancios(self):
        """Uno por corrida sería una tortura: se listan juntos."""
        viejo = time.time() - 99 * 3600
        os.utime(self.ruta, (viejo, viejo))
        config = self.config + [{"name": "otra", "path": self.ruta + ".nope"}]
        self.assertEqual(len(verificar_frescura(config, max_horas=12)), 2)

    def test_se_puede_desactivar(self):
        """max_horas=0 es la válvula de escape para una recarga manual."""
        viejo = time.time() - 400 * 3600
        os.utime(self.ruta, (viejo, viejo))
        self.assertEqual(verificar_frescura(self.config, max_horas=0), [])

    def test_maneja_la_ruta_con_prefijo_dbfs(self):
        """En el job el path viene como `dbfs:/Volumes/...`; los Volumes de UC están
        montados por FUSE, así que la ruta POSIX equivalente sí responde a getmtime."""
        self.assertIsNotNone(antiguedad_horas(self.ruta))
        self.assertIsNone(antiguedad_horas("dbfs:/Volumes/no/existe/x.parquet"))


if __name__ == "__main__":
    unittest.main()
