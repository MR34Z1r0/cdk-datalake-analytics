from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col, lit, when

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_gastos_compras"

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)
    t_gastos_compras = spark_controller.read_table(data_paths.DOMINIO, "t_gastos_compras", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

try:
    t_gastos_compras = t_gastos_compras.filter((col("id_periodo").isin(periodos)) & (col("id_pais").isin(cod_pais)))

    tmp = t_gastos_compras.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_proveedor").cast("string").alias("id_proveedor"),
        col("id_forma_pago").cast("string").alias("id_forma_pago"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("id_terminos").cast("string").alias("id_terminos"),
        col("id_solicitante").cast("string").alias("id_solicitante"),
        col("id_empleado").cast("string").alias("id_empleado"),
        col("id_via_embarque").cast("string").alias("id_via_embarque"),
        col("nro_orden_compra").cast("string").alias("nro_orden_compra"),
        col("cod_area").cast("string").alias("cod_area"),
        col("cod_estado").cast("string").alias("cod_estado"),
        col("desc_pais_embarque").cast("string").alias("desc_pais_embarque"),
        col("cod_pais_embarque").cast("string").alias("cod_pais_embarque"),
        col("cod_tipo_orden").cast("string").alias("cod_tipo_orden"),
        col("cod_moneda").cast("string").alias("cod_moneda"),
        col("cod_unidad_proveedor").cast("string").alias("cod_unidad_proveedor"),
        col("fecha_emision").cast("date").alias("fecha_emision"),
        col("fecha_entrega").cast("date").alias("fecha_entrega"),
        col("precio_compra_mn").cast("numeric(38, 12)").alias("precio_compra_mn"),
        col("precio_compra_me").cast("numeric(38, 12)").alias("precio_compra_me"),
        col("tipo_cambio_mn").cast("numeric(38, 12)").alias("tipo_cambio_mn"),
        col("tipo_cambio_me").cast("numeric(38, 12)").alias("tipo_cambio_me"),
        col("cant_atendida").cast("numeric(38, 12)").alias("cant_atendida"),
        col("cant_facturada").cast("numeric(38, 12)").alias("cant_facturada"),
        col("cant_cancelada").cast("numeric(38, 12)").alias("cant_cancelada"),
        col("cant_pedida").cast("numeric(38, 12)").alias("cant_pedida"),
        col("imp_atendido_me").cast("numeric(38, 12)").alias("imp_atendido_me"),
        col("imp_cancelado_me").cast("numeric(38, 12)").alias("imp_cancelado_me"),
        col("imp_facturado_me").cast("numeric(38, 12)").alias("imp_facturado_me"),
        col("imp_pedido_me").cast("numeric(38, 12)").alias("imp_pedido_me"),
        col("imp_neto_mn").cast("numeric(38, 12)").alias("imp_neto_mn"),
        col("imp_neto_me").cast("numeric(38, 12)").alias("imp_neto_me"),
        col("imp_venta_mn").cast("numeric(38, 12)").alias("imp_venta_mn"),
        col("imp_venta_me").cast("numeric(38, 12)").alias("imp_venta_me"),
    )

    partition_columns_array = ["id_pais", "id_periodo"]    
    spark_controller.write_table(tmp, data_paths.CADENA, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise