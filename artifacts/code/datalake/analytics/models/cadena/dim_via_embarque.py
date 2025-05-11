from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_via_embarque"

try:
    cod_pais = COD_PAIS.split(",")
    m_terminos = spark_controller.read_table(data_paths.DOMINIO, "m_via_embarque", cod_pais=cod_pais)
    

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
            col("id_via_embarque").cast("string").alias("id_via_embarque"),
            col("cod_via_embarque").cast("string").alias("cod_via_embarque"),
            col("desc_via_embarque1").cast("string").alias("desc_via_embarque1"),
            col("desc_via_embarque2").cast("string").alias("desc_via_embarque2"),
            col("cod_estado").cast("string").alias("cod_estado"),
            col("fecha_creacion").cast("date").alias("fecha_creacion"),
            col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_via_embarque"]
    partition_columns_array = ["id_pais"]
    
    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
