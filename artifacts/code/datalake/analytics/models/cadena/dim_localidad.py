from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_localidad"

try:
    cod_pais = COD_PAIS.split(",")
    m_localidad = spark_controller.read_table(data_paths.DOMINIO, "m_localidad", cod_pais=cod_pais)
    

except Exception as e:
    logger.error(e)
    raise

try:
    tmp = (
        m_localidad
        .filter(col("id_pais").isin(cod_pais))
        .select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_compania").cast("string").alias("id_compania"),
            col("id_localidad").cast("string").alias("id_localidad"),
            col("cod_localidad").cast("string").alias("cod_localidad"),
            col("cod_persona").cast("string").alias("cod_persona"),
            col("cod_persona_direccion").cast("string").alias("cod_persona_direccion"),
            col("cod_almacen").cast("string").alias("cod_almacen"),
            col("desc_localidad").cast("string").alias("desc_localidad"),
            col("cod_canal_visibilidad").cast("string").alias("cod_canal_visibilidad"),
            col("desc_canal").cast("string").alias("desc_canal"),
            col("cod_ng1").cast("string").alias("cod_ng1"),
            col("desc_ng1").cast("string").alias("desc_ng1"),
            col("cod_ng2").cast("string").alias("cod_ng2"),
            col("desc_ng2").cast("string").alias("desc_ng2"),
            col("direccion").cast("string").alias("direccion"),
            col("estado").cast("string").alias("estado"),
            col("fecha_creacion").cast("date").alias("fecha_creacion"),
            col("fecha_modificacion").cast("date").alias("fecha_modificacion"),
        )
    )

    id_columns = ["id_localidad", "id_pais"]
    partition_columns_array = ["id_pais"]
    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
