from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_sucursal"
try: 
    cod_pais = COD_PAIS.split(",") 
    df_m_sucursal = spark_controller.read_table(data_paths.DOMINIO, "m_sucursal", cod_pais=cod_pais) 
    df_m_pais = spark_controller.read_table(data_paths.DOMINIO, "m_pais", cod_pais=cod_pais) 
    df_m_compania = spark_controller.read_table(data_paths.DOMINIO, "m_compania", cod_pais=cod_pais) 
    
    logger.info("Dataframes load successfully")
except Exception as e:
    logger.error(e)
    raise
try:
    logger.info("Starting creation of df_dim_sucursal")
    df_dim_sucursal = (
        df_m_sucursal.alias("ms")
        .join(df_m_pais.alias("mp"), (col("mp.id_pais") == col("ms.id_pais")), "inner")
        .join(
            df_m_compania.alias("mc"),
            (col("ms.id_compania") == col("mc.id_compania"))
            & (col("ms.id_pais") == col("mc.id_pais")),
            "inner",
        )
        .where(
            (col('ms.id_pais').isin(cod_pais))
        )
        .select(
            col('ms.id_sucursal').cast("string").alias("id_sucursal"),
            col('ms.id_pais').cast("string").alias("id_pais"),
            col('mc.cod_compania').cast("string").alias("cod_compania"),
            col('mc.nomb_compania').cast("string").alias("desc_compania"),
            col('mc.cod_tipo_compania').cast("string").alias("cod_tipo_compania"),
            col('ms.cod_sucursal').cast("string").alias("cod_sucursal"),
            col('ms.nomb_sucursal').cast("string").alias("desc_sucursal"),
            col('ms.cod_tipo_sucursal').cast("string").alias("cod_tipo_sucursal"),
        )
    )

    id_columns = ["id_sucursal"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(df_dim_sucursal, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)    
except Exception as e:
    logger.error(e)
    raise