-- =============================================================================
-- midas_historial_cargos_silver
--
-- GRANO: la linea de cargo individual.
--
-- SIN PRIMARY KEY, a proposito. `midas_datos_detalle_cargos_bronze` no tiene llave
-- natural en FLEX tal como se extrae: en dllo son 21.844 filas para 4.480 cuentas, y
-- ninguna combinacion de las columnas disponibles resulto unica. Declarar una PK que
-- los datos no cumplen seria peor que no declararla — es exactamente la deuda que ya
-- arrastran siete tablas de Bronze. En su lugar se expone `n_filas_grano` sobre la
-- mejor llave candidata, para MEDIR el problema en vez de taparlo.
--
-- Si algun dia la unicidad se confirma, agregar la PK es un ALTER TABLE, no un rediseño.
-- =============================================================================
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.midas_historial_cargos_silver (
    servicio_suscrito              BIGINT  NOT NULL COMMENT 'cargos.cargnuse.',
    id_cuenta_cobro                BIGINT  NOT NULL COMMENT 'cargos.cargcuco. La cuenta que agrupa este cargo.',
    id_periodo_facturacion         BIGINT           COMMENT 'Periodo de facturacion derivado de la cuenta (factura + cuencobr), NO de cargos.CARGPEFA.',
    id_periodo_consumo             BIGINT           COMMENT 'cargos.cargpeco. NULO en ~19% de las filas: un cargo SIN periodo de consumo es un cargo que NO es de consumo (mantenimiento, control de perdidas, Fenix). Eso es informacion, no un dato faltante.',

    concepto                       STRING           COMMENT 'codigo-descripcion desde concepto.',
    concepto_cod                   INT              COMMENT 'Codigo numerico del concepto.',
    causal                         STRING           COMMENT 'codigo-descripcion desde causcarg, salvo el consumo normal que llega como el literal "-1".',
    causal_cod                     INT              COMMENT 'Codigo numerico de la causal. OJO: se extrae con REGEXP, no con SPLIT: el valor "-1" empieza por guion y SPLIT devolveria cadena vacia.',
    signo                          STRING           COMMENT 'DB (debito, suma) o CR (credito, resta).',
    programa                       STRING           COMMENT 'codigo-descripcion desde procesos. Identifica el sistema que origino el cargo.',
    programa_cod                   INT              COMMENT 'Codigo numerico del programa. Puede ser NEGATIVO (-26 Fenix, -51 control de perdidas): son cargos inyectados por otro sistema. Por eso REGEXP y no SPLIT.',
    documento_soporte              STRING           COMMENT 'Documento que respalda el cargo. Patron CO-PR-202606-TC-0007 (recuperacion) frente a CO-202606-TC-0007 (normal).',
    id_tarifa                      STRING           COMMENT 'cargos.cargtaco.',
    periodo_consumo_texto          STRING           COMMENT 'AAAA-MM derivado en Bronze. Redundante con id_periodo_consumo, se conserva por trazabilidad.',
    fecha_creacion_cargo           DATE             COMMENT 'cargos.cargfecr.',
    fecha_ini_consumo              DATE             COMMENT 'Inicio de la ventana de consumo del cargo (pericose por cargpeco).',
    fecha_fin_consumo              DATE             COMMENT 'Fin de la ventana de consumo del cargo.',
    anio_facturacion               BIGINT           COMMENT 'Anio del periodo de facturacion del cargo.',
    mes_facturacion                BIGINT           COMMENT 'Mes del periodo de facturacion del cargo.',
    unidades                       DOUBLE           COMMENT 'cargos.cargunid (nvl a 1). Negocio pidio tomar las unidades de AQUI y no de conssesu: es lo que realmente se cobro.',
    valor                          DOUBLE           COMMENT 'cargos.cargvalo, siempre positivo. Para sumar usa valor_con_signo.',
    valor_con_signo                DOUBLE           COMMENT 'valor con el signo aplicado: negativo si signo es credito. Es la columna que se suma; sumar `valor` a secas cuenta los creditos como cargos.',

    id_periodo_consumo_cuenta      BIGINT           COMMENT 'Periodo de consumo canonico de la CUENTA. Es el discriminador contra el que se compara el del cargo.',
    anio_facturacion_cuenta        BIGINT           COMMENT 'Anio del periodo de facturacion de la cuenta.',
    mes_facturacion_cuenta         BIGINT           COMMENT 'Mes del periodo de facturacion de la cuenta.',
    valor_total_cuenta             DOUBLE           COMMENT 'Total de la cuenta de cobro.',
    valor_pendiente_cuenta         DOUBLE           COMMENT 'Saldo pendiente de la cuenta.',
    valor_periodo_cuenta           DOUBLE           COMMENT 'Suma de cargos del periodo propio de la cuenta.',
    valor_recuperado_cuenta        DOUBLE           COMMENT 'Suma de cargos de periodos anteriores. Cuando tiene valor, el analista lo lee como "esta recuperando".',
    fecha_pago_cuenta              DATE             COMMENT 'Fecha de pago de la cuenta.',

    es_facturacion_normal          BOOLEAN          COMMENT 'programa = 5 (FGCA), el proceso normal. FALSE significa cargo inyectado por otra funcionalidad: es la base de la regla del Caso 17 ("otros cobros").',
    es_pno                         BOOLEAN          COMMENT 'causal 74 Y programa 307. DETECTA la perdida no operacional. El EXPEDIENTE esta en midas_datos_perdidas_no_operacionales_silver: ninguna reemplaza a la otra.',
    documento_tiene_token_recuperacion BOOLEAN      COMMENT 'El documento soporte trae PR en la SEGUNDA posicion del patron separado por guiones. Parseo posicional, nunca LIKE: un LIKE daria falsos positivos con cualquier documento que contenga esas letras.',
    periodo_consumo_difiere_de_cuenta  BOOLEAN      COMMENT 'El periodo de consumo del cargo no es el de la cuenta. Un cargo sin periodo propio NO cuenta como diferente, igual que hace la query de cuentas de cobro con nvl(cargpeco, pecscons).',
    es_recuperacion                BOOLEAN          COMMENT 'Las DOS senales a la vez, que es la definicion que dio negocio. Las dos banderas quedan expuestas por separado para que aguas abajo se puedan combinar de otro modo sin reprocesar.',

    n_filas_grano                  BIGINT           COMMENT 'Filas que comparten la mejor llave candidata (cuenta + concepto + causal + periodo + documento + tarifa + signo). > 1 mide cuanto le falta a esa llave para ser PK.',
    run_id                         STRING           COMMENT 'run_id de midas_log_cargas que produjo la fila.',
    fecha_carga_silver             TIMESTAMP        COMMENT 'Marca de materializacion.'
)
USING DELTA
COMMENT 'Cargos individuales enriquecidos con su cuenta de cobro y tres banderas derivadas (facturacion normal, PNO, recuperacion). Contiene los seis campos del struct detalle_cargos que consume el Caso 1, de modo que ese array sea reconstruible por proyeccion el dia de la migracion.'
