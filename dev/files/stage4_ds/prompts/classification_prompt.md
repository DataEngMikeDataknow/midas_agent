Analiza la orden usando únicamente el `INPUT_JSON`.

Debes devolver un objeto JSON que cumpla exactamente el contrato `output_contract`.

Criterios de uso:
- Usa los `few_shots` solo como guía de estilo y consistencia; no copies evidencias que no estén en la orden actual.
- Toda afirmación en `justification` debe estar respaldada por `evidence` o por campos del contexto.
- `evidence.source` debe ser uno de: `orden`, `historial_consumo`, `observaciones_calidad`, `regla_obligatoria`.
- `rules_applied` debe listar reglas concretas, por ejemplo: `consumo_actual_y_anterior_disponibles`, `observacion_lectura_detectada`, `historial_insuficiente`, `variacion_alta_no_explicada`.
- Si el caso no es concluyente, no fuerces clasificación: usa `REVISION_MANUAL`.
