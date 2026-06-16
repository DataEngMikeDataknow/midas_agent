# Contrato de salida — Agente Stage4

El agente debe devolver un JSON válido con estos campos mínimos:

```json
{
  "order_id": "string",
  "case_id": "variacion_significativa_mes_anterior",
  "service_type": "agua | energia | gas | null",
  "business_decision": "SIN_NOVEDAD | REQUIERE_AJUSTE | REQUIERE_VISITA | REVISION_MANUAL | null",
  "technical_status": "PROCESADA | ERROR_DATOS | ERROR_ENDPOINT | ERROR_TOOL | ERROR_VALIDACION_JSON | NO_SOPORTADA | PENDIENTE_REINTENTO",
  "classification": "NORMAL | VARIACION_NO_JUSTIFICADA | PNO | ESTACIONALIDAD | OBRA_NUEVA | ERROR_LECTURA | CONSTANTE_MAL_CONFIGURADA | DATOS_INSUFICIENTES | ERROR_TECNICO",
  "confidence_score": 0.0,
  "requires_human_review": true,
  "justification": "texto",
  "evidence": [],
  "rules_applied": [],
  "data_quality_warnings": [],
  "error_code": null,
  "error_message": null,
  "model_version": null,
  "prompt_version": "stage4-v0.1.0",
  "run_id": null
}
```

## Regla crítica

Si `technical_status != PROCESADA`, entonces `business_decision` debe ser `null`.
