from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    m_proveedor = spark_controller.read_table(data_paths.DOMINIO, "m_proveedor", cod_pais=cod_pais)
    
    target_table_name = "dim_proveedor"

except Exception as e:
    logger.error(e)
    raise

try:
    tmp_dim_proveedor = (
        m_proveedor.alias("mp")
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            col("mp.id_proveedor").alias("id_proveedor"),
            col("mp.id_pais").alias("id_pais"),
            col("mp.cod_proveedor").alias("cod_proveedor"),
            col("mp.nomb_proveedor").alias("nomb_proveedor"),
            col("mp.ruc_proveedor").alias("ruc_proveedor"),
            col("mp.desc_estado").alias("desc_estado"),
        )
    )

    tmp = tmp_dim_proveedor.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_proveedor").cast("string").alias("id_proveedor"),
        col("cod_proveedor").cast("string").alias("cod_proveedor"),
        col("nomb_proveedor").cast("string").alias("nomb_proveedor"),
        col("ruc_proveedor").cast("string").alias("ruc_proveedor"),
        col("desc_estado").cast("string").alias("desc_estado"),
    )

    id_columns = ["id_proveedor", "id_pais"]
    partition_columns_array = ["id_pais"]
    
    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
