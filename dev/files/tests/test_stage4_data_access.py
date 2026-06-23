import unittest

from src.midas.stage4.constants import QueryKey
from src.midas.stage4.data_access import full_table_name, normalize_query_key, validate_identifier


class TestStage4DataAccess(unittest.TestCase):
    def test_validate_identifier_rejects_injection(self):
        with self.assertRaises(ValueError):
            validate_identifier("facturacion; DROP TABLE x")

    def test_full_table_name_quotes_valid_identifiers(self):
        self.assertEqual(
            full_table_name("epm_datalabs_catalog_dllo", "facturacion", "midas_x"),
            "`epm_datalabs_catalog_dllo`.`facturacion`.`midas_x`",
        )

    def test_query_key_allowlist(self):
        self.assertEqual(normalize_query_key("QUERY_DATOS_CONSUMOS"), QueryKey.DATOS_CONSUMOS)
        with self.assertRaises(ValueError):
            normalize_query_key("QUERY_SQL_LIBRE")


if __name__ == "__main__":
    unittest.main()
