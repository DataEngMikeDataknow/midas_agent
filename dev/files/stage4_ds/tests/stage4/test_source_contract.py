import pytest

from midas_stage4.source_contract import validate_silver_only_sources


VALID_SOURCES = {
    "ordenes_pendientes": "midas_ordenes_calidad_pendientes_silver",
    "datos_basicos": "midas_datos_basicos_producto_silver",
    "lecturas": "midas_datos_lecturas_producto_silver",
    "consumos": "midas_datos_consumos_producto_silver",
    "ordenes_previa_critica": "midas_datos_ordenes_previa_critica_silver",
    "comentarios_ordenes": "midas_datos_comentarios_ordenes_silver",
    "cuentas_cobro": "midas_datos_cuentas_cobro_silver",
    "detalle_cargos": "midas_datos_detalle_cargos_silver",
}


def test_validate_silver_only_sources_accepts_silver_tables():
    validate_silver_only_sources(VALID_SOURCES)


def test_validate_silver_only_sources_rejects_bronze_tables():
    invalid = dict(VALID_SOURCES)
    invalid["consumos"] = "midas_datos_consumos_producto_bronze"

    with pytest.raises(ValueError, match="Silver"):
        validate_silver_only_sources(invalid)
