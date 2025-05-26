from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col, date_format, lit, sum, when,round

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_categoria_compra = spark_controller.read_table(data_paths.DOMINIO, "m_categoria_compra", cod_pais=cod_pais)
    m_unidad_medida_equivalente = spark_controller.read_table(data_paths.DOMINIO, "m_unidad_medida_equivalente", cod_pais=cod_pais)
    t_hoja_costos_cabecera = spark_controller.read_table(data_paths.DOMINIO, "t_hoja_costos_cabecera", cod_pais=cod_pais)

    target_table_name = "fact_orden_produccion"

except Exception as e:
    logger.error(e)
    raise
 
try:
    m_categoria_compra = m_categoria_compra.filter(col("id_pais").isin(cod_pais))
    m_unidad_medida_equivalente = m_unidad_medida_equivalente.filter(col("id_pais").isin(cod_pais))
    
    t_hoja_costos_cabecera = t_hoja_costos_cabecera \
    .filter(
            (col("cant_girada") > lit(0))
            & (col("id_pais").isin(cod_pais))
            & (date_format(col("fecha_liquidacion"), "yyyyMM").isin(periodos))
        )
    
    tmp_movimientos_ope = (
        t_hoja_costos_cabecera.alias("thcc")
        .join(m_categoria_compra.alias("mcc"), col("thcc.id_articulo") == col("mcc.id_articulo"), "left")
        .select(
            col("thcc.id_pais").alias("id_pais"),
            col("thcc.id_sucursal").alias("id_sucursal"),
            col("thcc.id_compania").alias("id_compania"),
            col("thcc.fecha_liquidacion").alias("fecha_liquidacion"),
            date_format(col("thcc.fecha_liquidacion"), "yyyyMM").alias("id_periodo"),
            col("thcc.id_articulo").alias("id_articulo"),
            col("thcc.id_centro_costo").alias("id_centro_costo"),
            col("thcc.nro_operacion").alias("nro_operacion"),
            round(col("thcc.cant_girada"),4).alias("cant_girada"), 
            round(col("thcc.precio_cpm_me"),4).alias("precio_cpm_me"), 
            round(col("thcc.precio_cpm_mn"),4).alias("precio_cpm_mn"), 
            col("thcc.cod_unidad_manejo").alias("cod_unidad_manejo"),
            col("thcc.cod_unidad_medida").alias("cod_unidad_medida"),
            round(col("thcc.precio_std_me"),4).alias("precio_std_me"), 
            round(col("thcc.precio_std_mn"),4).alias("precio_std_mn") 
        )
    )

    tmp_orden_produccion = (
        tmp_movimientos_ope.alias("a")
        .join(m_unidad_medida_equivalente.alias("b"), (col("a.id_compania") == col("b.id_compania")) & (col("a.cod_unidad_manejo") == col("b.cod_unidad_medida_desde")) & (col("a.cod_unidad_medida") == col("b.cod_unidad_medida_hasta")),"left")
        .groupby(
            col("a.id_pais"),
            col("a.id_periodo"),
            col("a.fecha_liquidacion"),
            col("a.id_sucursal"),
            col("a.id_articulo"),
            col("a.nro_operacion"),
            col("a.id_centro_costo"),
            col("a.cod_unidad_manejo"),
            col("a.cod_unidad_medida"),
            col("b.cod_operador"),
            col("b.cant_operacion"),
            col("a.precio_cpm_me"),
            col("a.precio_cpm_mn"),
            col("a.precio_std_me"),
            col("a.precio_std_mn")
        )
        .agg(
            sum(col("a.cant_girada")).alias("cant_girada"),
            when(col("b.cod_operador") == 'D', sum(col("a.cant_girada")) / col("b.cant_operacion")).otherwise(sum(col("a.cant_girada")) * col("b.cant_operacion")).alias("cant_girada_global"),
            (col("a.precio_cpm_me") * sum(col("a.cant_girada"))).alias("imp_ingreso_me"),
            (col("a.precio_cpm_mn") * sum(col("a.cant_girada"))).alias("imp_ingreso_mn"),
            (col("a.precio_std_me") * sum(col("a.cant_girada"))).alias("imp_girada_me"),
            (col("a.precio_std_mn") * sum(col("a.cant_girada"))).alias("imp_girada_mn"),
            )
        .select(
            col("a.id_pais"),
            col("a.id_periodo"),
            col("a.fecha_liquidacion"),
            col("a.id_sucursal"),
            col("a.id_articulo"),
            col("a.nro_operacion"),
            col("a.id_centro_costo"),
            col("a.cod_unidad_manejo"),
            col("cant_girada"),
            col("a.cod_unidad_medida"),
            col("cant_girada_global"),
            (col("a.precio_cpm_me") * lit(1000)).alias("precio_cpm_me"),
            col("a.precio_cpm_mn").alias("precio_cpm_mn"),
            col("imp_ingreso_me").alias("imp_ingreso_me"),
            col("imp_ingreso_mn").alias("imp_ingreso_mn"),
            (col("a.precio_std_me") * lit(1000)).alias("precio_std_me"),
            col("imp_girada_me").alias("imp_girada_me"),
            col("imp_girada_mn").alias("imp_girada_mn"),
            col("a.precio_std_mn").alias("precio_std_mn")
        )
    )

    tmp = tmp_orden_produccion.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("fecha_liquidacion").cast("date").alias("fecha_liquidacion"),
        col("nro_operacion").cast("string").alias("nro_operacion"),
        col("precio_cpm_mn").cast("numeric(38, 12)").alias("precio_cpm_mn"),
        col("precio_cpm_me").cast("numeric(38, 12)").alias("precio_cpm_me"),
        col("precio_std_mn").cast("numeric(38, 12)").alias("precio_std_mn"),
        col("precio_std_me").cast("numeric(38, 12)").alias("precio_std_me"),
        col("cant_girada").cast("numeric(38, 12)").alias("cant_girada"),
        col("cant_girada_global").cast("numeric(38, 12)").alias("cant_girada_global"),
        col("imp_girada_mn").cast("numeric(38, 12)").alias("imp_girada_mn"),
        col("imp_girada_me").cast("numeric(38, 12)").alias("imp_girada_me"),
        col("imp_ingreso_mn").cast("numeric(38, 12)").alias("imp_ingreso_mn"),
        col("imp_ingreso_me").cast("numeric(38, 12)").alias("imp_ingreso_me")
    )

    partition_columns_array = ["id_pais", "id_periodo"]    
    spark_controller.write_table(tmp, data_paths.CADENA, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise