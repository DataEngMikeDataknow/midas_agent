import unittest

from src.midas.stage4.normalization import ContextNormalizer
from src.midas.stage4.rules import DeterministicRuleEngine


class TestStage4Rules(unittest.TestCase):
    def setUp(self):
        self.normalizer = ContextNormalizer()

    def test_insufficient_data_forces_review(self):
        context = self.normalizer.normalize(
            {
                "orden": {"id_orden": "1", "servicio_suscrito": "10"},
                "producto": None,
                "lecturas": [],
                "consumos": [],
                "critica_previa": [],
                "comentarios": [],
                "cuentas_cobro": [],
                "detalle_cargos": [],
            },
            "2026-06-23",
        )
        result = DeterministicRuleEngine().evaluate(context)
        self.assertEqual(result.categoria.value, "DATOS_INSUFICIENTES")
        self.assertTrue(result.requiere_revision_humana)
        self.assertTrue(result.hard_stop)

    def test_significant_variation_without_justification(self):
        context = self.normalizer.normalize(
            {
                "orden": {"id_orden": "1", "servicio_suscrito": "10"},
                "producto": {"servicio_suscrito": "10", "categoria": "RES"},
                "lecturas": [{"servicio_suscrito": "10", "constante": 1, "consumo_calculado": 100, "consumo_facturado": 100}],
                "consumos": [
                    {"servicio_suscrito": "10", "anio_facturacion": 2026, "mes_facturacion": 6, "consumo": 200},
                    {"servicio_suscrito": "10", "anio_facturacion": 2026, "mes_facturacion": 5, "consumo": 100},
                ],
                "critica_previa": [],
                "comentarios": [],
                "cuentas_cobro": [{"id_cuenta_cobro": "99"}],
                "detalle_cargos": [],
            },
            "2026-06-23",
        )
        result = DeterministicRuleEngine(umbral_variacion=0.30).evaluate(context)
        self.assertEqual(result.categoria.value, "VARIACION_SIGNIFICATIVA_NO_JUSTIFICADA")
        self.assertTrue(result.requiere_revision_humana)

    def test_reclamo_escalates(self):
        context = self.normalizer.normalize(
            {
                "orden": {"id_orden": "1", "servicio_suscrito": "10"},
                "producto": {"servicio_suscrito": "10"},
                "lecturas": [{"constante": 1, "consumo_calculado": 100, "consumo_facturado": 100}],
                "consumos": [
                    {"anio_facturacion": 2026, "mes_facturacion": 6, "consumo": 130},
                    {"anio_facturacion": 2026, "mes_facturacion": 5, "consumo": 100},
                ],
                "critica_previa": [],
                "comentarios": [{"comentario": "Cliente radicó reclamo por facturación"}],
                "cuentas_cobro": [],
                "detalle_cargos": [],
            },
            "2026-06-23",
        )
        result = DeterministicRuleEngine().evaluate(context)
        self.assertEqual(result.categoria.value, "RECLAMO_RELACIONADO")
        self.assertEqual(result.decision.value, "ESCALAR")


if __name__ == "__main__":
    unittest.main()
