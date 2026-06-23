import unittest
from datetime import datetime, timezone

from src.midas.stage4.validator import OutputValidationError, validate_final_output


class TestStage4Validator(unittest.TestCase):
    def _payload(self):
        return {
            "orden_id": "123",
            "producto_id": "456",
            "fecha_proceso": "2026-06-23",
            "categoria": "NORMAL",
            "decision": "APROBAR",
            "confianza": 0.77,
            "resumen_ejecutivo": "Caso normal.",
            "explicacion_tecnica": "Sin señales críticas.",
            "senales_detectadas": [
                {"senal": "sin_variacion_material", "fuente": "reglas", "valor": "ok", "peso": "MEDIO"}
            ],
            "datos_consultados": {
                "ordenes_pendientes": True,
                "datos_basicos": True,
                "lecturas": True,
                "consumos": True,
                "critica_previa": False,
                "comentarios": False,
                "cuentas_cobro": True,
                "detalle_cargos": False,
            },
            "recomendacion_operativa": "Aprobar.",
            "requiere_revision_humana": False,
            "motivo_revision_humana": None,
            "version_prompt": "stage4-v1.0.0",
            "version_modelo": "test-model",
            "timestamp_inferencia": datetime.now(timezone.utc).isoformat(),
        }

    def test_validate_ok(self):
        payload = self._payload()
        self.assertEqual(validate_final_output(payload)["categoria"], "NORMAL")

    def test_reject_invalid_category(self):
        payload = self._payload()
        payload["categoria"] = "OTRA"
        with self.assertRaises(OutputValidationError):
            validate_final_output(payload)

    def test_requires_motivo_when_human_review(self):
        payload = self._payload()
        payload["requiere_revision_humana"] = True
        payload["motivo_revision_humana"] = None
        with self.assertRaises(OutputValidationError):
            validate_final_output(payload)


if __name__ == "__main__":
    unittest.main()
