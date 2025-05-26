from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()
target_table_name = "dim_linea_produccion"

try:
    cod_pais = COD_PAIS.split(",")
    m_linea_produccion = spark_controller.read_table(data_paths.DOMINIO, "m_linea_produccion", cod_pais=cod_pais)
    m_familia_equipo = spark_controller.read_table(data_paths.DOMINIO, "m_familia_equipo", cod_pais=cod_pais)

except Exception as e:
    logger.error(e)
    raise

try:
    m_linea_produccion = m_linea_produccion.filter(col("id_pais").isin(cod_pais))

    tmp_cadena_dim_linea_produccion = (
        m_linea_produccion.alias("mlp")
        .join(m_familia_equipo.alias("mfe"), col("mlp.id_familia_equipo") == col("mfe.id_familia_equipo"), "inner")
        .select(
            col("mlp.id_pais"),
            col("mlp.id_linea_produccion"),
            col("mlp.id_sucursal"),
            col("mfe.cod_familia_equipo"),
            col("mlp.cod_linea_produccion"),
            col("mfe.desc_familia_equipo"),
            col("mlp.desc_linea_produccion"),
            col("mlp.estado"),
        )
    )

    tmp = tmp_cadena_dim_linea_produccion.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_linea_produccion").cast("string").alias("id_linea_produccion"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("cod_familia_equipo").cast("string").alias("cod_familia_equipo"),
        col("cod_linea_produccion").cast("string").alias("cod_linea_produccion"),
        col("desc_familia_equipo").cast("string").alias("desc_familia_equipo"),
        col("desc_linea_produccion").cast("string").alias("desc_linea_produccion"),
        col("estado").cast("string").alias("estado"),
    )

    id_columns = ["id_linea_produccion", "id_pais"]
    partition_columns_array = ["id_pais"]
    
    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
