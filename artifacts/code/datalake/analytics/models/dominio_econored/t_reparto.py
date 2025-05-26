from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col, concat_ws, date_format, desc, row_number
from pyspark.sql.window import Window

spark_controller = SPARK_CONTROLLER()
target_table_name = "t_reparto"
try:
    PERIODOS= spark_controller.get_periods()
    cod_pais = COD_PAIS.split(",")
    logger.info(f"Databases: {cod_pais}")

    df_m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais, have_principal=True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
    df_m_parametro = spark_controller.read_table(data_paths.BIG_BAGIC, "m_parametro", cod_pais=cod_pais)
    df_t_historico_almacen = spark_controller.read_table(data_paths.BIG_BAGIC, "t_movimiento_inventario", cod_pais=cod_pais)
    
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("starting filter of pais and periodo") 
    df_t_historico_almacen = df_t_historico_almacen.filter(date_format(col("fecha_almacen"), "yyyyMM").isin(PERIODOS))

    logger.info("Starting creation of df_m_compania")
    df_m_compania = (
        df_m_compania.alias("mc")
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("mc.cod_pais"), "inner")
        .select(col("mc.cod_compania"), col("mp.id_pais"))
    )

    logger.info("Starting creation of df_t_historico_almacen")
    window_spec = Window.partitionBy(
        "cod_compania",
        "cod_sucursal",
        "cod_almacen_emisor_origen",
        "cod_documento_transaccion",
        "nro_documento_almacen"
    ).orderBy(col("nro_documento_movimiento").desc())

    logger.info("Starting creation of df_t_historico_almacen_filter")    
    df_t_historico_almacen_filter = (
        df_t_historico_almacen.alias("ta").withColumn("orden", row_number().over(window_spec))
        .join(df_m_compania.alias("mc"), col("ta.cod_compania") == col("mc.cod_compania", "inner"))
        .select(
            col("mc.id_pais"),
            date_format(col("ta.fecha_almacen"), "yyyyMM").alias("id_periodo"),
            concat_ws("|", col("tmi.cod_compania"), col("tmi.cod_sucursal"), col("tmi.cod_almacen"), col("tmi.cod_documento_almacen"), col("tmi.nro_documento_almacen")).alias("id_reparto"),
            concat_ws("|", col("tmi.cod_compania"), col("cod_transportista")).alias("id_transportista"),
            concat_ws("|", col("tmi.cod_compania"), col("tmi.cod_vehiculo")).alias("id_medio_transporte"),
            concat_ws("|", col("tmi.cod_compania"), col("tmi.cod_chofer")).alias("id_chofer"),
            col("tmi.fecha_emision").alias("fecha_orden_carga"),
            col("tmi.fecha_almacen").alias("fecha_reparto"),
            col("tmi.fecha_creacion"),
            col("tmi.fecha_modificacion"),
            col("tmi.cod_estado_comprobante").alias("estado_guia")
        )
    ) 

    logger.info("Starting creation of df_t_reparto")  
    df_t_reparto = df_t_historico_almacen_filter.select(
        col("id_pais").cast("string"),
        col("id_periodo").cast("string"),
        col("id_reparto").cast("string"),
        col("id_transportista").cast("string"),
        col("id_medio_transporte").cast("string"),
        col("id_chofer").cast("string"),
        col("fecha_orden_carga").cast("date"),
        col("fecha_reparto").cast("date"),
        col("estado_guia").cast("string"),
        col("fecha_creacion").cast("timestamp"),
        col("fecha_modificacion").cast("timestamp")
    )

    partition_columns_array = ["id_pais", "id_periodo"]
    logger.info(f"starting write of {target_table_name}")
    spark_controller.write_table(df_t_reparto, data_paths.DOMINIO_ECONORED, target_table_name, partition_columns_array)
except Exception as e:
    logger.error(e)
    raise