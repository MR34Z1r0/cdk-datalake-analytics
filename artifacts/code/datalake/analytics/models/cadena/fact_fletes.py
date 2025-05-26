from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_fletes"

try:
    cod_pais = COD_PAIS.split(",")
    periodos = spark_controller.get_periods()
    logger.info(periodos)
    t_fletes = spark_controller.read_table(data_paths.DOMINIO, "t_fletes")

except Exception as e:
    logger.error(e)
    raise

try:
    t_fletes = t_fletes.filter(
        (col("id_pais").isin(cod_pais)) & (col("id_periodo").isin(periodos))
    )

    tmp = t_fletes.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_localidad_origen").cast("string").alias("id_localidad_origen"),
        col("id_transportista").cast("string").alias("id_transportista"),
        col("id_medio_transporte").cast("string").alias("id_medio_transporte"),
        col("id_localidad_destino").cast("string").alias("id_localidad_destino"),
        col("id_empleado").cast("string").alias("id_empleado"),
        col("id_fletex").cast("string").alias("id_fletex"),
        col("cod_procedimiento").cast("string").alias("cod_procedimiento"),
        col("nro_orden_carga").cast("string").alias("nro_orden_carga"),
        col("nro_pre_impreso").cast("string").alias("nro_pre_impreso"),
        col("fecha_almacen").cast("date").alias("fecha_almacen"),
        col("nro_documento_flete").cast("string").alias("nro_documento_flete"),
        col("nro_documento").cast("string").alias("nro_documento"),
        col("documento_pago").cast("string").alias("documento_pago"),
        col("nro_serie_pago").cast("string").alias("nro_serie_pago"),
        col("nro_pago").cast("string").alias("nro_pago"),
        col("fecha_liquidacion").cast("date").alias("fecha_liquidacion"),
        col("tarifa_negociacion_mn").cast("numeric(38, 12)").alias("tarifa_negociacion_mn"),
        col("tarifa_negociacion_me").cast("numeric(38, 12)").alias("tarifa_negociacion_me"),
        col("tarifa_base_mn").cast("numeric(38, 12)").alias("tarifa_base_mn"),
        col("tarifa_base_me").cast("numeric(38, 12)").alias("tarifa_base_me"),
        col("total_peso").cast("numeric(38, 12)").alias("total_peso"),
        col("fecha_confirmacion").cast("date").alias("fecha_confirmacion"),
        col("distancia_km").cast("numeric(38, 12)").alias("distancia_km"),
        col("cant_dias").cast("numeric(38, 12)").alias("cant_dias"),
        col("cant_tiempo").cast("numeric(38, 12)").alias("cant_tiempo"),
        col("nro_comprobante").cast("string").alias("nro_comprobante"),
        col("cod_documento_transaccion").cast("string").alias("cod_documento_transaccion"),
        col("nro_documento_almacen").cast("string").alias("nro_documento_almacen"),
        col("documento_flete").cast("string").alias("documento_flete"),
        col("nro_flete").cast("string").alias("nro_flete"),
        col("cod_prioridad").cast("string").alias("cod_prioridad"),
        col("desc_efletex").cast("string").alias("desc_efletex"),
        col("cod_antencion_logistica").cast("string").alias("cod_antencion_logistica"),
        col("desc_estado_atencion").cast("string").alias("desc_estado_atencion"),
        col("cod_chofer_servicio").cast("string").alias("cod_chofer_servicio"),
        col("cod_tipo_gasto_adicional").cast("string").alias("cod_tipo_gasto_adicional"),
        col("desc_tipo_gasto_adicional").cast("string").alias("desc_tipo_gasto_adicional"),
        col("imp_gasto_adicional_mn").cast("numeric(38, 12)").alias("imp_gasto_adicional_mn"),
        col("imp_gasto_adicional_me").cast("numeric(38, 12)").alias("imp_gasto_adicional_me"),
        col("desc_tipo_tarifa").cast("string").alias("desc_tipo_tarifa"),
        col("cod_estado_comprobante").cast("string").alias("cod_estado_comprobante"),
        col("idtv").cast("string").alias("idtv"),
        col("cod_centro_costo").cast("string").alias("cod_centro_costo"),
        col("flag_pago").cast("integer").alias("flag_pago"),
        col("desc_estado_envio").cast("string").alias("desc_estado_envio"),
        col("cod_envio").cast("string").alias("cod_envio"),
        col("desc_tipo_vehiculo_servicio").cast("string").alias("desc_tipo_vehiculo_servicio"),
        col("desc_procedimiento").cast("string").alias("desc_procedimiento"),
        col("tarifa_distancia_km_mn").cast("numeric(38, 12)").alias("tarifa_distancia_km_mn"),
        col("tarifa_distancia_km_me").cast("numeric(38, 12)").alias("tarifa_distancia_km_me"),
        col("cod_moneda").cast("string").alias("cod_moneda"),
        col("incluye_impuesto").cast("integer").alias("incluye_impuesto"),
        col("cant_cajavolumen_principal").cast("numeric(22, 8)").alias("cant_cajavolumen_principal"),
        col("cant_cajafisica_principal").cast("numeric(18, 8)").alias("cant_cajafisica_principal"),
        col("cant_tarimas_principal").cast("numeric(18, 8)").alias("cant_tarimas_principal"),
        col("cant_peso_principal").cast("numeric(18, 8)").alias("cant_peso_principal"),
        col("cant_cajavolumen_asociada").cast("numeric(18, 8)").alias("cant_cajavolumen_asociada"),
        col("cant_cajafisica_asociada").cast("numeric(18, 8)").alias("cant_cajafisica_asociada"),
        col("cant_tarimas_asociada").cast("numeric(18, 8)").alias("cant_tarimas_asociada"),
        col("cant_peso_asociada").cast("numeric(18, 8)").alias("cant_peso_asociada"),
    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.CADENA, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
