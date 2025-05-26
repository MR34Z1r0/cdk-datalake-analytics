from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_almacen"

try:
    cod_pais = COD_PAIS.split(",")
    m_almacen = spark_controller.read_table(data_paths.DOMINIO, "m_almacen", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

# CREATE
try:
    m_almacen = m_almacen.filter(col("id_pais").isin(cod_pais))
    tmp = m_almacen.select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_compania").cast("string").alias("id_compania"),
            col("id_almacen").cast("string").alias("id_almacen"),
            col("cod_almacen").cast("string").alias("cod_almacen"),
            col("desc_almacen").cast("string").alias("desc_almacen"),
            col("desc_tipo_almacen").cast("string").alias("desc_tipo_almacen"),
    )

    id_columns = ["id_almacen", "id_pais"]
    partition_columns_array = ["id_pais"]
    
    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise