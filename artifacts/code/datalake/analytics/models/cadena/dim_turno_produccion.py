from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_turno_produccion"

try:
    cod_pais = COD_PAIS.split(",")
    m_turno = spark_controller.read_table(data_paths.DOMINIO, "m_turno", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

try:
    m_turno = m_turno.filter(col("id_pais").isin(cod_pais))

    tmp = m_turno.select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_turno").cast("string").alias("id_turno"),
            col("id_sucursal").cast("string").alias("id_sucursal"),
            col("cod_turno").cast("string").alias("cod_turno"),
            col("desc_turno").cast("string").alias("desc_turno"),
    )

    id_columns = ["id_turno", "id_pais"]
    partition_columns_array = ["id_pais"]
    
    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
