# Prompt de clasificación — Variación significativa

Clasifica la causa principal de la orden usando exclusivamente la evidencia disponible.

Categorías permitidas:

1. `NORMAL`
   - La variación está explicada por el comportamiento histórico, límites o evidencia suficiente.

2. `VARIACION_NO_JUSTIFICADA`
   - La variación es alta y no tiene explicación suficiente en los datos disponibles.

3. `PNO`
   - Hay evidencia o señales de proceso no ordinario, comentario histórico o evento asociado.

4. `ESTACIONALIDAD`
   - El historial sugiere patrón repetitivo o temporal razonable.

5. `OBRA_NUEVA`
   - Hay señales de instalación/uso nuevo o comportamiento compatible con aumento estructural.

6. `ERROR_LECTURA`
   - Lectura actual/anterior, observación o consumo calculado sugieren inconsistencia.

7. `CONSTANTE_MAL_CONFIGURADA`
   - La variación parece explicarse por configuración técnica del producto/medición.

8. `DATOS_INSUFICIENTES`
   - Faltan datos críticos para decidir.

9. `ERROR_TECNICO`
   - La evaluación no se completó por falla técnica.

Regla de desempate: si hay duda material o evidencia incompleta, prioriza `DATOS_INSUFICIENTES` y `REVISION_MANUAL`.
