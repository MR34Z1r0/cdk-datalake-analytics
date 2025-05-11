from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, lit, max, sum, when,round

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos = spark_controller.get_periods()
    logger.info(periodos)
    m_unidad_medida_equivalente = spark_controller.read_table(data_paths.DOMINIO, "m_unidad_medida_equivalente", cod_pais=cod_pais)
    t_movimiento_inventario = spark_controller.read_table(data_paths.DOMINIO, "t_movimiento_inventario", cod_pais=cod_pais)
    t_movimiento_inventario_detalle = spark_controller.read_table(data_paths.DOMINIO, "t_movimiento_inventario_detalle", cod_pais=cod_pais)
    t_orden_compra_cabecera = spark_controller.read_table(data_paths.DOMINIO, "t_orden_compra_cabecera", cod_pais=cod_pais)
    t_orden_compra_detalle = spark_controller.read_table(data_paths.DOMINIO, "t_orden_compra_detalle", cod_pais=cod_pais)
    t_orden_compra_ingreso = spark_controller.read_table(data_paths.DOMINIO, "t_orden_compra_ingreso", cod_pais=cod_pais)
    dim_articulo = spark_controller.read_table(data_paths.CADENA, "dim_articulo", cod_pais=cod_pais)

    target_table_name = "fact_compra_ingresada"

except Exception as e:
    logger.error(e)
    raise

try:
    t_orden_compra_ingreso = t_orden_compra_ingreso.filter((col("es_eliminado") == lit("1")) & (col("id_pais").isin(cod_pais)))
    t_orden_compra_cabecera = t_orden_compra_cabecera.filter(col("es_anulado") == lit('0'))
    t_movimiento_inventario = t_movimiento_inventario.filter((col("cod_estado_comprobante").isin('PLI','LIQ')) & (col("id_periodo").isin(periodos)))

    tmp_movimientos = (
        t_orden_compra_ingreso.alias("toci")
        .join(t_orden_compra_cabecera.alias("tocc"), col("toci.id_orden_compra") == col("tocc.id_orden_compra"), "inner")
        .join(t_orden_compra_detalle.alias("tocd"), col("toci.id_orden_ingreso") == col("tocd.id_orden_ingreso"), "inner")
        .join(t_movimiento_inventario.alias("tmi"), col("toci.id_movimiento_ingreso") == col("tmi.id_movimiento_ingreso"), "inner")
        .join(t_movimiento_inventario_detalle.alias("tmid"), 
                (col("toci.id_sucursal_ingreso") == col("tmid.id_sucursal"))
                & (col("toci.id_articulo") == col("tmid.id_articulo"))
                & (col("tmi.nro_documento_movimiento") == col("tmid.nro_documento_movimiento"))
                & (col("tmi.cod_procedimiento") == col("tmid.cod_procedimiento"))
                & (col("toci.nro_secuencia_ingreso") == col("tmid.nro_linea_comprobante"))
                ,"inner")
        .select(
            col("toci.id_pais"),
            col("tmi.id_periodo"),
            col("toci.id_compania_compra"),
            col("toci.id_sucursal_compra").alias("id_sucursal"),
            col("tmi.fecha_almacen").alias("fecha_ingreso_almacen"),
            col("tmi.cod_procedimiento"),
            col("tocd.id_articulo"),
            col("toci.nro_orden_compra"),
            col("tocd.cod_unidad_compra"),
            col("tocd.cant_comprada").alias("cant_compra"),
            col("tmid.cod_unidad_almacen"),
            coalesce(col("tmid.cant_cajafisica"), lit(0)).alias("cant_cajafisica"),
            coalesce(col("tocd.precio_compra_me"), lit(0)).alias("precio_compra_me"),
            coalesce(col("tocd.precio_compra_mo"), lit(0)).alias("precio_compra_mo"),
            coalesce(col("tocd.precio_compra_mn"), lit(0)).alias("precio_compra_mn"),
            coalesce(col("tocd.precio_std_me"), lit(0)).alias("precio_std_me"),
        )
    )

    dim_articulo = dim_articulo.filter((col("id_pais").isin(cod_pais)) & (col("cod_categoria_compra").isNotNull()))
    tmp_movimientos_ope = (
        tmp_movimientos.alias("tm")
        .join(dim_articulo.alias("da"), col("tm.id_articulo") == col("da.id_articulo"), "inner")
        .select(
            col("tm.id_pais"),
            col("tm.id_periodo"),
            col("tm.fecha_ingreso_almacen"),
            col("tm.id_compania_compra"),
            col("tm.id_sucursal"),
            col("tm.id_articulo"),
            col("tm.nro_orden_compra"),
            col("tm.cod_unidad_compra"),
            col("tm.cant_compra"),
            col("tm.cod_unidad_almacen"),
            col("tm.cant_cajafisica"),
            col("da.cod_unidad_global"),
            col("tm.precio_compra_me"),
            col("tm.precio_compra_mo"),
            col("tm.precio_compra_mn"),
            col("tm.precio_std_me"),
        )
    )

    tmp_orden_compras_ni = (
        tmp_movimientos_ope.alias("a")
        .join(m_unidad_medida_equivalente.alias("b"), (col("a.id_compania_compra") == col("b.id_compania")) & (col("b.COD_UNIDAD_MEDIDA_DESDE") == col("a.cod_unidad_almacen")) & (col("b.COD_UNIDAD_MEDIDA_HASTA") == col("a.cod_unidad_global")), "left")
        .join(m_unidad_medida_equivalente.alias("c"), (col("a.id_compania_compra") == col("c.id_compania")) & (col("c.COD_UNIDAD_MEDIDA_DESDE") == col("a.cod_unidad_compra")) & (col("c.COD_UNIDAD_MEDIDA_HASTA") == col("a.cod_unidad_almacen")), "left")
        .groupBy(
            col("a.id_compania_compra"),
            col("a.id_pais"),
            col("a.id_periodo"),
            col("a.fecha_ingreso_almacen"),
            col("a.id_sucursal"),
            col("a.id_articulo"),
            col("a.nro_orden_compra"),
            col("a.cod_unidad_compra"),
            col("a.cant_compra"),
            col("a.cod_unidad_almacen"),
            col("a.cod_unidad_global"),
            col("a.precio_compra_me"),
            col("a.precio_compra_mo"),
            col("a.precio_compra_mn"),
            col("b.cod_operador"),
            col("b.cant_operacion"),
            col("c.cod_operador"),
            col("c.cant_operacion"),
            col("a.precio_std_me"),
        )
        .agg(
            sum(col("a.cant_cajafisica")).alias("cant_almacen"),
            when(col("b.cod_operador") == 'D', round(sum(col("a.cant_cajafisica")),4) / round(col("b.cant_operacion"),4)).otherwise(round(sum(col("a.cant_cajafisica")),4) * round(col("b.cant_operacion"),4)).alias("cant_almacen_global"),
            when(col("c.cod_operador") == 'D', round(col("a.precio_compra_mo"),4) * round(col("c.cant_operacion"),4)).otherwise(round(col("a.precio_compra_mo"),4) / round(col("c.cant_operacion"),4)).alias("precio_orden_mo"),
            when(col("c.cod_operador") == 'D', round(col("a.precio_compra_me"),4) * round(col("c.cant_operacion"),4)).otherwise(round(col("a.precio_compra_me"),4) / round(col("c.cant_operacion"),4)).alias("precio_orden_me"),
            when(col("c.cod_operador") == 'D', round(col("a.precio_compra_mn"),4) * round(col("c.cant_operacion"),4)).otherwise(round(col("a.precio_compra_mn"),4) / round(col("c.cant_operacion"),4)).alias("precio_orden_mn"),
            when(col("b.cod_operador") == 'D', (round(col("a.precio_compra_mo"),4) * round(col("b.cant_operacion"),4)) / round(col("c.cant_operacion"),4)).otherwise(round(col("a.precio_compra_mo"),4) * round(col("b.cant_operacion"),4) * round(col("c.cant_operacion"),4)).alias("precio_ingreso_mo"),
            when(col("b.cod_operador") == 'D', (round(col("a.precio_compra_me"),4) * round(col("b.cant_operacion"),4)) / round(col("c.cant_operacion"),4)).otherwise(round(col("a.precio_compra_me"),4) * round(col("b.cant_operacion"),4) * round(col("c.cant_operacion"),4)).alias("precio_ingreso_me"),
            when(col("b.cod_operador") == 'D', (round(col("a.precio_compra_mn"),4) * round(col("b.cant_operacion"),4)) / round(col("c.cant_operacion"),4)).otherwise(round(col("a.precio_compra_mn"),4) * round(col("b.cant_operacion"),4) * round(col("c.cant_operacion"),4)).alias("precio_ingreso_mn")
        )
        .select(
            col("a.id_compania_compra"),
            col("a.id_pais"),
            col("a.id_periodo"),
            col("a.fecha_ingreso_almacen"),
            col("a.id_sucursal"),
            col("a.id_articulo"),
            col("a.nro_orden_compra"),
            col("a.cod_unidad_compra"),
            col("a.cant_compra"),
            col("a.cod_unidad_almacen"),
            col("a.cod_unidad_global"),
            col("cant_almacen"),
            col("a.precio_compra_me"),
            col("a.precio_compra_mo"),
            col("a.precio_compra_mn"),
            col("cant_almacen_global"),
            col("precio_orden_mo"),
            col("precio_orden_me"),
            col("precio_orden_mn"),
            col("precio_ingreso_mo"),
            col("precio_ingreso_me"),
            col("precio_ingreso_mn"),
            lit(None).alias("imp_compra_ingreso_mo"),
            lit(None).alias("imp_compra_ingreso_mn"),
            lit(None).alias("imp_compra_ingreso_me"),
            col("a.precio_std_me"),
            lit(None).alias("imp_compra_std_me"),
        )
    )

    tmp_orden_compras = (
        tmp_orden_compras_ni.alias("a")
        .join(m_unidad_medida_equivalente.alias("b"), (col("a.id_compania_compra") == col("b.id_compania")) & (col("b.COD_UNIDAD_MEDIDA_DESDE") == col("a.cod_unidad_almacen")) & (col("b.COD_UNIDAD_MEDIDA_HASTA") == col("a.cod_unidad_global")), "left")
        .join(m_unidad_medida_equivalente.alias("c"), (col("a.id_compania_compra") == col("c.id_compania")) & (col("c.COD_UNIDAD_MEDIDA_DESDE") == col("a.cod_unidad_compra")) & (col("c.COD_UNIDAD_MEDIDA_HASTA") == col("a.cod_unidad_almacen")), "left")
        .select(
            col("a.id_pais"),
            col("a.id_periodo"),
            col("a.fecha_ingreso_almacen"),
            col("a.id_sucursal"),
            col("a.id_articulo"),
            col("a.nro_orden_compra"),
            col("a.cod_unidad_compra"),
            col("a.cant_compra"),
            col("a.cod_unidad_almacen"),
            col("a.cod_unidad_global"),
            col("a.cant_almacen"),
            col("a.precio_compra_me"),
            col("a.precio_compra_mo"),
            col("a.precio_compra_mn"),
            col("a.cant_almacen_global"),
            col("a.precio_orden_mo"),
            col("a.precio_orden_me"),
            col("a.precio_orden_mn"),
            col("a.precio_ingreso_mo"),
            col("a.precio_ingreso_me"),
            col("a.precio_ingreso_mn"),
            (round(col("a.cant_almacen_global"),4) * round(col("a.precio_ingreso_mo"),4)).alias("imp_compra_ingreso_mo"),
            (round(col("a.cant_almacen_global"),4) * round(col("a.precio_ingreso_mn"),4)).alias("imp_compra_ingreso_mn"),
            (round(col("a.cant_almacen_global"),4) * round(col("a.precio_ingreso_me"),4)).alias("imp_compra_ingreso_me"),
            col("a.precio_std_me"),
            (col("a.precio_std_me") * col("a.cant_almacen_global")).alias("imp_compra_std_me"),
        )
    )

    tmp_compra_ingresada = (
        tmp_orden_compras
        .groupBy(
            col("id_pais"),
            col("id_periodo"),
            col("fecha_ingreso_almacen"),
            col("id_sucursal"),
            col("id_articulo"),
            col("nro_orden_compra"),
            col("cod_unidad_compra"),
            col("cod_unidad_almacen"),
            col("cod_unidad_global"),
        )
        .agg(
            sum(col("cant_compra")).alias("cant_compra"),
            sum(col("cant_almacen")).alias("cant_almacen"),
            sum(col("cant_almacen_global")).alias("cant_almacen_global"),
            max(col("precio_compra_me")).alias("precio_compra_me"),
            max(col("precio_compra_mo")).alias("precio_compra_mo"),
            max(col("precio_compra_mn")).alias("precio_compra_mn"),
            max(col("precio_orden_mo")).alias("precio_orden_mo"),
            max(col("precio_orden_me")).alias("precio_orden_me"),
            max(col("precio_orden_mn")).alias("precio_orden_mn"),
            max(col("precio_ingreso_mo")).alias("precio_ingreso_mo"),
            max(col("precio_ingreso_me")).alias("precio_ingreso_me"),
            max(col("precio_ingreso_mn")).alias("precio_ingreso_mn"),
            max(col("precio_std_me")).alias("precio_std_me"),
            sum(col("imp_compra_ingreso_mo")).alias("imp_compra_ingreso_mo"),
            sum(col("imp_compra_ingreso_mn")).alias("imp_compra_ingreso_mn"),
            sum(col("imp_compra_ingreso_me")).alias("imp_compra_ingreso_me"),
            sum(col("imp_compra_std_me")).alias("imp_compra_std_me"),
        )
        .select(
            col("id_pais"),
            col("id_periodo"),
            col("fecha_ingreso_almacen"),
            col("id_sucursal"),
            col("id_articulo"),
            col("nro_orden_compra"),
            col("cod_unidad_compra"),
            col("cod_unidad_almacen"),
            col("cant_compra"),
            col("cant_almacen"),
            col("cant_almacen_global"),
            col("precio_orden_mo"),
            col("precio_orden_me"),
            col("precio_orden_mn"),
            col("precio_ingreso_mo"),
            col("precio_ingreso_me"),
            col("precio_ingreso_mn"),
            col("imp_compra_ingreso_mo"),
            col("imp_compra_ingreso_mn"),
            col("imp_compra_ingreso_me"),
            col("precio_std_me"),
            col("imp_compra_std_me"),
        )
    )

    tmp = tmp_compra_ingresada.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("fecha_ingreso_almacen").cast("date").alias("fecha_ingreso_almacen"),
        col("nro_orden_compra").cast("string").alias("nro_orden_compra"),
        col("cod_unidad_compra").cast("string").alias("cod_unidad_compra"),
        col("cod_unidad_almacen").cast("string").alias("cod_unidad_almacen"),
        col("cant_compra").cast("numeric(38, 12)").alias("cant_compra"),
        col("cant_almacen").cast("numeric(38, 12)").alias("cant_almacen"),
        col("cant_almacen_global").cast("numeric(38, 12)").alias("cant_almacen_global"),
        col("precio_orden_mo").cast("numeric(38, 12)").alias("precio_orden_mo"),
        col("precio_orden_me").cast("numeric(38, 12)").alias("precio_orden_me"),
        col("precio_orden_mn").cast("numeric(38, 12)").alias("precio_orden_mn"),
        col("precio_ingreso_mo").cast("numeric(38, 12)").alias("precio_ingreso_mo"),
        col("precio_ingreso_me").cast("numeric(38, 12)").alias("precio_ingreso_me"),
        col("precio_ingreso_mn").cast("numeric(38, 12)").alias("precio_ingreso_mn"),
        col("precio_std_me").cast("numeric(38, 12)").alias("precio_std_me"),
        col("imp_compra_ingreso_mo").cast("numeric(38, 12)").alias("imp_compra_ingreso_mo"),
        col("imp_compra_ingreso_me").cast("numeric(38, 12)").alias("imp_compra_ingreso_me"),
        col("imp_compra_ingreso_mn").cast("numeric(38, 12)").alias("imp_compra_ingreso_mn"),
        col("imp_compra_std_me").cast("numeric(38, 12)").alias("imp_compra_std_me"),
    )

    partition_columns_array = ["id_pais", "id_periodo"]    
    spark_controller.write_table(tmp, data_paths.CADENA, target_table_name, partition_by=partition_columns_array)


except Exception as e:
    logger.error(e)
    raise

