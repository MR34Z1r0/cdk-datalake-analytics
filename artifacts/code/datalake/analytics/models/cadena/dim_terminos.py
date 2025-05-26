from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_terminos"

try:
    cod_pais = COD_PAIS.split(",")
    m_terminos = spark_controller.read_table(data_paths.DOMINIO, "m_terminos", cod_pais=cod_pais)
    

except Exception as e:
    logger.error(e)
    raise

try:
    tmp = (
        m_terminos
        .filter(col("id_pais").isin(cod_pais))
        .select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_compania").cast("string").alias("id_compania"),
            col("id_terminos").cast("string").alias("id_terminos"),
            col("cod_terminos").cast("string").alias("cod_terminos"),
            col("desc_termino").cast("string").alias("desc_termino"),
            col("cod_estado").cast("string").alias("cod_estado"),
            col("fecha_creacion").cast("timestamp").alias("fecha_creacion"),
            col("fecha_modificacion").cast("timestamp").alias("fecha_modificacion"),
            col("es_seguro").cast("int").alias("es_seguro"),
        )
    )

    id_columns = ["id_terminos"]
    partition_columns_array = ["id_pais"]
    
    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
