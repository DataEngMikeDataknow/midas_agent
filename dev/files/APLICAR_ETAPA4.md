# Cómo aplicar la Etapa 4

Desde la raíz del repositorio:

```powershell
Expand-Archive .\midas_stage4_data_science_files.zip -DestinationPath . -Force
```

Validar pruebas locales:

```powershell
cd dev/files
$env:PYTHONPATH="src"
pytest tests/agent -q
```

Crear SQL Functions en Databricks:

```bash
python src/midas/main_stage4_tools.py   --catalog <catalog>   --schema <schema>   --pipeline_sp <service-principal-id>
```

Desplegar agente Stage4:

```bash
python src/midas/main_stage4_deploy.py   --catalog <catalog>   --schema <schema>   --model_name midas_stage4_variacion_significativa_agent   --endpoint_name midas-stage4-variacion-significativa   --case_id variacion_significativa_mes_anterior
```

Ejecutar inferencia de prueba:

```bash
python src/midas/main_stage4_inference.py   --catalog <catalog>   --schema <schema>   --model_endpoint midas-stage4-variacion-significativa   --source_table midas_ordenes_calidad_pendientes_silver   --target_table midas_predicciones_agente_stage4_gold   --activity_filter "VARIACION"   --limit 20
```

Antes de inferir, confirmar el texto real de la actividad:

```sql
SELECT actividad, COUNT(*)
FROM <catalog>.<schema>.midas_ordenes_calidad_pendientes_silver
GROUP BY actividad
ORDER BY COUNT(*) DESC;
```
