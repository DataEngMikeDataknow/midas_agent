import unittest
from unittest.mock import MagicMock
import sys

# Mocks para evitar errores si pyspark no está instalado
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()

from src.midas.transformations import SilverTransformer

class TestSilverTransformations(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.transformer = SilverTransformer(self.spark)

    def test_transform_bronze_to_silver(self):
        # Configuración
        catalog = "test_catalog"
        schema = "test_schema"

        # Ejecución
        self.transformer.transform_bronze_to_silver(catalog, schema)

        # Verificaciones
        self.spark.sql.assert_any_call(f"USE CATALOG {catalog}")
        self.spark.sql.assert_any_call(f"USE SCHEMA {schema}")
        
        # Verificar que se intentaron crear las tablas silver
        # Buscamos partes de los nombres de las tablas para ser flexibles
        sql_calls = [call.args[0] for call in self.spark.sql.call_args_list]
        
        self.assertTrue(any("midas_ordenes_calidad_pendientes_silver" in sql for sql in sql_calls))
        self.assertTrue(any("midas_historial_facturacion_silver" in sql for sql in sql_calls))
        self.assertTrue(any("midas_datos_basicos_producto_silver" in sql for sql in sql_calls))
        self.assertTrue(any("midas_historial_critica_silver" in sql for sql in sql_calls))

if __name__ == '__main__':
    unittest.main()
