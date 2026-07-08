"""
Tests del discriminador job_name en el plano de control compartido.

midas_control_cargas / midas_log_cargas son COMPARTIDAS con otros jobs
(p. ej. vera_framework). Sin filtrar por job_name cada job leeria las filas
del otro. Se verifica que los SELECT de lectura filtran por
job_name = 'midas_bronze' y que ese es el default del cliente.
"""
import sys
import unittest
from unittest.mock import MagicMock

sys.modules["pyspark"] = MagicMock()
sys.modules["pyspark.sql"] = MagicMock()

from src.midas.framework.control_cargas import ControlCargasClient  # noqa: E402


class TestJobName(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.client = ControlCargasClient(
            spark=self.spark, catalog="cat", schema="facturacion",
        )

    def test_default_job_name(self):
        self.assertEqual(self.client.job_name, "midas_bronze")

    def test_get_active_tables_filtra_por_job_name(self):
        self.client.get_active_tables()
        sql = self.spark.sql.call_args.args[0]
        self.assertIn("job_name = 'midas_bronze'", sql)

    def test_get_id_carga_filtra_por_job_name(self):
        self.spark.sql.return_value.collect.return_value = []
        self.client.get_id_carga("midas_x_bronze")
        sql = self.spark.sql.call_args.args[0]
        self.assertIn("job_name = 'midas_bronze'", sql)

    def test_job_name_personalizado(self):
        client = ControlCargasClient(
            spark=self.spark, catalog="cat", schema="facturacion",
            job_name="vera_framework",
        )
        client.get_active_tables()
        sql = self.spark.sql.call_args.args[0]
        self.assertIn("job_name = 'vera_framework'", sql)


if __name__ == "__main__":
    unittest.main()
