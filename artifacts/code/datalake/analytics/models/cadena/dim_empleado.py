from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_empleado"

try:
    cod_pais = COD_PAIS.split(",")
    m_empleado = spark_controller.read_table(data_paths.DOMINIO, "m_empleado", cod_pais=cod_pais)
    

except Exception as e:
    logger.error(e)
    raise

try:
    tmp = (
        m_empleado
        .filter(col("id_pais").isin(cod_pais))
        .select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_empleado").cast("string").alias("id_empleado"),
            col("cod_empleado").cast("string").alias("cod_empleado"),
            col("nomb_empleado").cast("string").alias("nomb_empleado"),
            col("estado").cast("string").alias("estado"),
            col("fecha_creacion").cast("date").alias("fecha_creacion"),
            col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_empleado", "id_pais"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
