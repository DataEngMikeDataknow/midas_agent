# Anexos

## A. Matriz de ambientes

| Ambiente | Rama | Host | Catalogo destino | Service Principal | Estado |
| --- | --- | --- | --- | --- | --- |
| dllo | desarrollo | `adb-1161217326944529.9.azuredatabricks.net` | `epm_datalabs_catalog_dllo` | `2080d313-3074-4adf-8265-916ef7645dc2` | cadena implementada; schedule PAUSED |
| uat | pruebas | `adb-6159907084216583.3.azuredatabricks.net` | `epm_datalake_catalog_np` | `c0b3fdca-baec-4bab-9f08-c6e222d921ae` | preparado; sujeto a red/GRANTs/jar |
| pdn | produccion | `adb-5768199714870840.0.azuredatabricks.net` | `epm_datalake_catalog_prod` | `1db15634-31b7-4f92-a976-fc8920216152` | simetrico a uat; requiere apertura propia |

## B. Jobs declarados

| Job | Proposito | Targets | Schedule |
| --- | --- | --- | --- |
| `midas_bronze_silver_<target>` | cadena crear_objetos -> extraccion -> bronze -> silver | dllo/uat/pdn | diario 07:00 America/Bogota |
| `midas_check_conectividad_<target>` | preflight de conectividad Oracle | uat/pdn | on-demand |

## C. Conexion Oracle por ambiente

| Ambiente | Host:Puerto/Servicio | Secret scope | Password key |
| --- | --- | --- | --- |
| dllo | `epm-to34.corp.epm.com.co:1521/SFUAT` | `AZ-SecretScopeDBKS-EPM-NP-KV-DLLO` | `AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA` |
| uat | `epm-to34.corp.epm.com.co:1521/SFUAT` | `AZ-SecretScopeDBKS-EPM-NP-KV-UAT` | `AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA` |
| pdn | `epm-po34.corp.epm.com.co:1522/SFPDN` | `AZ-SecretScopeDBKS-EPM-PROD-KV` | `AZ-SECRET-EPM-OPEN-DB-CTATECNICA` |

Usuario Oracle (los tres): `SQL_EPMBOTPD05` (explicito, no en el scope).
Jar ojdbc11: `.../facturacion_bronze_vol/_oracle_client/ojdbc11-23.26.2.0.0.jar` en el
Volume del catalogo np (dllo/uat) o prod (pdn).

## D. Tablas producidas

### Bronze (8)
| Tabla | query_key | orden |
| --- | --- | --- |
| `midas_ordenes_calidad_pendientes_bronze` | QUERY_ORDENES_PENDIENTES | 1 |
| `midas_datos_basicos_producto_bronze` | QUERY_DATOS_BASICOS | 2 |
| `midas_datos_lecturas_producto_bronze` | QUERY_DATOS_LECTURA | 3 |
| `midas_datos_consumos_producto_bronze` | QUERY_DATOS_CONSUMOS | 4 |
| `midas_datos_ordenes_previa_critica_bronze` | QUERY_ORDENES_CRITICA_PEVIA | 5 |
| `midas_datos_cometarios_ordenes_bronze` | QUERY_COMENTARIOS_ORDENES | 6 |
| `midas_datos_cuentas_cobro_bronze` | QUERY_CUENTAS_COBRO | 7 |
| `midas_datos_detalle_cargos_bronze` | QUERY_DETALLE_CARGOS | 8 |

### Silver (4)
| Tabla | Origen principal |
| --- | --- |
| `midas_ordenes_calidad_pendientes_silver` | ordenes pendientes + datos basicos |
| `midas_historial_facturacion_silver` | cuentas de cobro + consumos + lecturas + cargos |
| `midas_datos_basicos_producto_silver` | datos basicos producto |
| `midas_historial_critica_silver` | ordenes previa/critica + comentarios |

## E. Plano de control compartido

| job_name | bundle | motor | schedule |
| --- | --- | --- | --- |
| `vera_framework` | `vera_framework` | QUERY_FULL_OVERWRITE | 04:00 America/Bogota |
| `midas_bronze` | `midas_data_platform` | FULL_CHAINED | 07:00 America/Bogota |

Advertencia: `midas_control_cargas` y `midas_log_cargas` son compartidas. Ningun bundle las
posee en exclusiva; nunca hacer `bundle destroy` asumiendo que las limpia.

## F. Tabla de diagnostico rapido

| Sintoma | Interpretacion |
| --- | --- |
| `DPY-6005` / timeout | red/firewall (no credenciales) |
| `ORA-01017` | credenciales Oracle |
| `ORA-12514` | service/listener |
| `SIGSEGV` / exit 139 | mismatch runtime/JDK (usar 16.4) |
| `EMPTY_SCHEMA_NOT_SUPPORTED_FOR_DATASOURCE` | objetos Java sin convertir |
| `42501` / `INSUFFICIENT_PERMISSIONS` | falta GRANT o se uso Spark JDBC |

## G. Checklist de salida a produccion (pdn)
- [ ] jar ojdbc11 subido al Volume de prod con el nombre exacto
- [ ] GRANTs del SP de pdn (UC, CREATE TABLE, READ/WRITE VOLUME, secret scope)
- [ ] apertura de red de la subnet de pdn hacia `epm-po34:1522` (independiente de uat)
- [ ] secret scope/password de pdn validados (`AZ-SecretScopeDBKS-EPM-PROD-KV`)
- [ ] `midas_check_conectividad -t pdn` en verde
- [ ] `bundle validate -t pdn` sin errores
- [ ] verificar 8 filas activas de control tras `crear_objetos`

## H. Historial resumido de decisiones tecnicas
- separacion del bundle de datos respecto al del agente (`midas_agent`)
- migracion del conector Oracle de driver Python a JDBC ojdbc11 (verifier 10G)
- conexion driver-side (no Spark JDBC) por restriccion de Unity Catalog
- runtime unico 16.4 (JDK 17) por compatibilidad con ojdbc11
- plano de control compartido discriminado por `job_name`
- convergencia con `vera_framework` (ver ADR 0001)
- patron `crear_objetos` idempotente en lugar de seed manual en SQL
- nomenclatura de targets `dllo/uat/pdn`

## I. Riesgos abiertos
1. uat/pdn dependen de prerrequisitos de plataforma (red, jar, GRANTs) fuera del repo
2. un test unitario (`test_ingestion::test_ensure_schema_exists`) esta desfasado
3. el acoplamiento con `midas_agent` es temporal (schedule), no un contrato fuerte
4. se reutiliza el variable group `databricks-MIDAS` en lugar de uno propio

## J. Referencias
- `docs/adr/0001-bundles-separados-vera-midas.md`
- `README.md`
- `BUGS_TRAZABILIDAD.md`
- Repo de referencia (solo lectura): `vera_framework` (`feature/ojdbc-all-envs`)
