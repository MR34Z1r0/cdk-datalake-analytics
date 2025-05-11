from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col, coalesce, lit, when

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_evaluacion_proveedores"

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)
    t_orden_compra_detalle = spark_controller.read_table(data_paths.DOMINIO, "t_orden_compra_detalle", cod_pais=cod_pais)
    t_orden_compra_cabecera = spark_controller.read_table(data_paths.DOMINIO, "t_orden_compra_cabecera", cod_pais=cod_pais)
    t_orden_compra_cronograma_entrega = spark_controller.read_table(data_paths.DOMINIO, "t_orden_compra_cronograma_entrega", cod_pais=cod_pais)
    t_orden_compra_ingreso = spark_controller.read_table(data_paths.DOMINIO, "t_orden_compra_ingreso", cod_pais=cod_pais)
    t_movimiento_inventario = spark_controller.read_table(data_paths.DOMINIO, "t_movimiento_inventario", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

try:
    t_orden_compra_detalle = t_orden_compra_detalle.filter((col("id_periodo").isin(periodos)) & (col("id_pais").isin(cod_pais)))

    tmp_evaluacion_proveedores = (
        t_orden_compra_detalle.alias("tocd")
        .join(t_orden_compra_cabecera.alias("tocc"), col("tocd.id_orden_compra") == col("tocc.id_orden_compra"), "inner")
        .join(t_orden_compra_cronograma_entrega.alias("tocce"), 
            (col("tocce.id_sucursal") == col("tocd.id_sucursal"))
            & (col("tocce.cod_transaccion") == col("tocd.cod_tipo_documento"))
            & (col("tocce.nro_documento") == col("tocd.nro_orden_compra"))
            & (col("tocce.id_articulo") == col("tocd.id_articulo"))
            & (col("tocce.nro_secuencia") == col("tocd.nro_secuencia"))
            ,"left"
        )
        .join(t_orden_compra_ingreso.alias("toci"), 
            (col("toci.id_sucursal_compra") == col("tocc.id_sucursal"))
            & (col("toci.cod_tipo_documento_compra") == col("tocc.cod_tipo_documento"))
            & (col("toci.nro_orden_compra") == col("tocc.nro_orden_compra"))
            & (col("toci.id_articulo") == col("tocce.id_articulo"))
            & (col("toci.nro_secuencia_compra") == col("tocce.nro_secuencia"))
            & (col("toci.fecha_entrega") == col("tocce.fecha_inicio"))
            ,"left"
        )
        .join(t_movimiento_inventario.alias("tmi"), 
            (col("toci.id_sucursal_ingreso") == col("tmi.id_sucursal_origen"))
            & (col("toci.cod_tipo_documento_compra") == col("tmi.cod_documento_transaccion_ref1"))
            & (col("toci.nro_orden_compra") == col("tmi.nro_documento_almacen_ref1"))
            & (col("toci.nro_nota_ingreso") == col("tmi.nro_documento_movimiento"))
            & (coalesce(col("toci.nro_doc_referencia"), lit("")) == coalesce(col("tmi.nro_documento_almacen_ref2"), lit("")))
            ,"left"
        )

        .select(
            ########### Campos para el dropDuplicates ###########
            col("tocd.id_pais").alias("tocd_id_pais"),
            col("tocd.id_periodo").alias("tocd_id_periodo"),
            col("tocd.id_sucursal").alias("tocd_id_sucursal"),
            col("tocc.id_proveedor").alias("tocc_id_proveedor"),
            col("tocd.cod_tipo_documento").alias("tocd_cod_tipo_documento"),
            col("tocd.nro_orden_compra").alias("tocd_nro_orden_compra"),
            col("tocd.nro_secuencia").alias("tocd_nro_secuencia"),
            col("tocc.fecha_emision").alias("tocc_fecha_emision"),
            col("tocd.id_articulo").alias("tocd_id_articulo"),
            col("tocce.fecha_inicio").alias("tocce_fecha_inicio"),
            col("tocce.cant_atendida").alias("tocce_cant_atendida"),
            col("tmi.fecha_almacen").alias("tmi_fecha_almacen"),
            col("tocce.cant_perdida").alias("tocce_cant_perdida"),
            col("toci.cant_ingresada").alias("toci_cant_ingresada"),
            col("tocd.cant_cancelada").alias("tocd_cant_cancelada"),
            col("tocc.es_anulado").alias("tocc_es_anulado"),
            col("tocc.es_aprobado").alias("tocc_es_aprobado"),
            col("tocc.desc_tipo_orden").alias("tocc_desc_tipo_orden"),
            col("tocc.imp_neto_mo").alias("tocc_imp_neto_mo"),
            col("tocd.imp_compra_mo").alias("tocd_imp_compra_mo"),
            col("tocc.imp_neto_me").alias("tocc_imp_neto_me"),
            col("tocc.desc_estado_orden_compra").alias("tocc_desc_estado_orden_compra"),
            col("tocd.es_atendido").alias("tocd_es_atendido"),
            #####################################################
            col("tocd.id_pais").alias("id_pais"),
            col("tocd.id_periodo").alias("id_periodo"),
            col("tocd.id_sucursal").alias("id_sucursal"),
            col("tocc.id_proveedor").alias("id_proveedor"),
            col("tocd.cod_tipo_documento").alias("cod_tipo_documento"),
            col("tocd.nro_orden_compra").alias("nro_orden_compra"),
            col("tocc.fecha_emision").alias("fecha_emision"),
            col("tocd.nro_secuencia").alias("nro_secuencia"),
            col("tocd.id_articulo").alias("id_articulo"),
            col("tocce.fecha_inicio").alias("fecha_entrega"),
            when(col("tocce.cant_atendida") == 0, lit(None)).otherwise(col("tmi.fecha_almacen")).alias("fecha_ingreso_almacen"),
            col("tocce.cant_perdida").alias("cant_perdida"),
            col("tocce.cant_atendida").alias("cant_atendida"),
            col("toci.cant_ingresada").alias("cant_ingresada"),
            col("tocd.cant_cancelada").alias("cant_cancelada"),
            when(col("tocd.cant_cancelada") > 0, lit(1)).otherwise(lit(0)).alias("es_cancelado"),
            (col("tocce.cant_perdida") - col("tocce.cant_atendida")).alias("cant_pendiente"),
            col("tocc.es_anulado").alias("es_anulado"),
            col("tocc.es_aprobado").alias("es_aprobado"),
            col("tocc.desc_tipo_orden").alias("desc_tipo_orden"),
            col("tocc.imp_neto_mo").alias("imp_neto_mo"),
            col("tocd.imp_compra_mo").alias("imp_compra_mo"),
            col("tocc.imp_neto_me").alias("imp_neto_me"),
            col("tocc.desc_estado_orden_compra").alias("desc_estado_orden_compra"),
            col("tocd.es_atendido").alias("es_atendido"),
        )
    )

    tmp_evaluacion_proveedores = tmp_evaluacion_proveedores.dropDuplicates([
        "tocd_id_pais",
        "tocd_id_periodo",
        "tocd_id_sucursal",
        "tocc_id_proveedor",
        "tocd_cod_tipo_documento",
        "tocd_nro_orden_compra",
        "tocd_nro_secuencia",
        "tocc_fecha_emision",
        "tocd_id_articulo",
        "tocce_fecha_inicio",
        "tocce_cant_atendida",
        "tmi_fecha_almacen",
        "tocce_cant_perdida",
        "toci_cant_ingresada",
        "tocd_cant_cancelada",
        "tocc_es_anulado",
        "tocc_es_aprobado",
        "tocc_desc_tipo_orden",
        "tocc_imp_neto_mo",
        "tocd_imp_compra_mo",
        "tocc_imp_neto_me",
        "tocc_desc_estado_orden_compra",
        "tocd_es_atendido",
    ])


    tmp = tmp_evaluacion_proveedores.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_proveedor").cast("string").alias("id_proveedor"),
        col("cod_tipo_documento").cast("string").alias("cod_tipo_documento"),
        col("nro_orden_compra").cast("string").alias("nro_orden_compra"),
        col("fecha_emision").cast("date").alias("fecha_emision"),
        col("nro_secuencia").cast("string").alias("nro_secuencia"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("fecha_entrega").cast("date").alias("fecha_entrega"),
        col("fecha_ingreso_almacen").cast("date").alias("fecha_ingreso_almacen"),
        col("cant_perdida").cast("numeric(38, 12)").alias("cant_perdida"),
        col("cant_atendida").cast("numeric(38, 12)").alias("cant_atendida"),
        col("cant_ingresada").cast("numeric(38, 12)").alias("cant_ingresada"),
        col("cant_cancelada").cast("numeric(38, 12)").alias("cant_cancelada"),
        col("es_cancelado").cast("integer").alias("es_cancelado"),
        col("cant_pendiente").cast("numeric(38, 12)").alias("cant_pendiente"),
        col("es_anulado").cast("integer").alias("es_anulado"),
        col("es_aprobado").cast("integer").alias("es_aprobado"),
        col("desc_tipo_orden").cast("string").alias("desc_tipo_orden"),
        col("imp_neto_mo").cast("numeric(38, 12)").alias("imp_neto_mo"),
        col("imp_compra_mo").cast("numeric(38, 12)").alias("imp_compra_mo"),
        col("imp_neto_me").cast("numeric(38, 12)").alias("imp_neto_me"),
        col("desc_estado_orden_compra").cast("string").alias("desc_estado_orden_compra"),
        col("es_atendido").cast("integer").alias("es_atendido"),
    )

    partition_columns_array = ["id_pais", "id_periodo"]
    spark_controller.write_table(tmp, data_paths.CADENA, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise