from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)
    t_compra_rendimiento = spark_controller.read_table(data_paths.DOMINIO, "t_compra_rendimiento", cod_pais=cod_pais)

    target_table_name = "fact_compra_rendimiento"

except Exception as e:
    logger.error(e)
    raise

try:
    tmp = t_compra_rendimiento \
    .filter(col("id_pais").isin(cod_pais)) \
    .select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("fecha_rendimiento").cast("date").alias("fecha_rendimiento"),
        col("cant_consumida").cast("numeric(38, 12)").alias("cant_consumida"),
        col("cant_producida").cast("numeric(38, 12)").alias("cant_producida"),
        col("precio_std_me").cast("numeric(38, 12)").alias("precio_std_me"),
        col("cod_categoria").cast("string").alias("cod_categoria_compra"),
    ) \
    .distinct()

    partition_columns_array = ["id_pais", "id_periodo"]    
    spark_controller.write_table(tmp, data_paths.CADENA, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise