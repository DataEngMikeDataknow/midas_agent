# Anexos

## A. Matriz de ambientes

| Ambiente | Host | Endpoint | Estado |
| --- | --- | --- | --- |
| DEV | `adb-1161217326944529.9.azuredatabricks.net` | `agente_ordenes_calidad_dev_v3` | operativo |
| UAT | `adb-6159907084216583.3.azuredatabricks.net` | `agente_ordenes_calidad_uat` | condicionado por credenciales/permisos |
| PROD | `adb-5768199714870840.0.azuredatabricks.net` | `agente_ordenes_calidad_prod` | no habilitado |

## B. Jobs declarados

| Job | Proposito | Schedule |
| --- | --- | --- |
| `midas_load_transform_data` | carga, transformacion, tools, trigger inference | diario |
| `midas_agent_deploy_ops` | despliegue del agente | bajo demanda |
| `midas_agent_inference` | inferencia batch | por encadenamiento o manual |

## C. Artefactos de salida

| Artefacto | Tipo | Descripcion |
| --- | --- | --- |
| `midas_predicciones_agente_gold` | tabla Delta | resultados batch del agente |
| `midas_results_inferences.csv` | CSV | publicacion externa de resultados |

## D. Checklist de salida a produccion
- [ ] definir catalogos y paths reales de PROD en `databricks.yml`
- [ ] validar SP de PROD y secrets vigentes
- [ ] confirmar permisos UC, cluster y serving
- [ ] validar pipeline PROD
- [ ] ejecutar smoke test controlado
- [ ] definir monitoreo y rollback

## E. Checklist de revision documental
- [ ] glosario validado con negocio
- [ ] arquitectura validada con datos y DevOps
- [ ] manual tecnico validado por mantenimiento
- [ ] manual de usuario validado por operacion funcional
- [ ] matriz de credenciales validada por custodios

## F. Historial resumido de decisiones tecnicas
- migracion de notebooks a scripts Python
- uso de Databricks Asset Bundles
- root path compartido en `/Shared/bundles/...`
- endpoint DEV nuevo por conflictos de permisos con endpoints previos
- instalacion explicita de Terraform en pipeline CD

## G. Riesgos abiertos
1. PROD aun no esta operativo
2. parte del deploy del agente depende de workaround tecnico
3. dos pruebas unitarias relevantes estan desalineadas con el runtime actual
4. la operacion de UAT/PROD depende de gestion correcta de SP y secrets fuera del repo
