from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS, PERIODOS
from pyspark.sql.functions import col

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)
    t_movimiento_contable = spark_controller.read_table(data_paths.DOMINIO, "t_movimiento_contable")
    
    target_table_name = "fact_saldo_contable_centro_costo"

    
except Exception as e:
    logger.error(e)
    raise

try:
    t_movimiento_contable = t_movimiento_contable.filter(
        col("id_pais").isin(cod_pais) & col("id_periodo").isin(periodos)
    )
   
    # Filtrar los registros que no están en t_voucher_eliminados
    tmp_fact_saldo_contable_centro_costo = t_movimiento_contable \
        .select(
            col("id_pais").cast("string").alias("id_pais"),
            col("id_compania").cast("string").alias("id_compania"),
            col("id_sucursal").cast("string").alias("id_sucursal"),
            col("id_periodo").cast("string").alias("id_periodo"),
            col("id_periodo_contable").cast("string").alias("id_periodo_contable"),
            col("id_cuenta_contable").cast("string").alias("id_cuenta_contable"),
            col("id_centro_costo").cast("string").alias("id_centro_costo"),
            col("debe_mn").cast("decimal(38, 12)").alias("debe_mn"),
            col("debe_me").cast("decimal(38, 12)").alias("debe_me"),
            col("haber_mn").cast("decimal(38, 12)").alias("haber_mn"),
            col("haber_me").cast("decimal(38, 12)").alias("haber_me"),
            col("cod_tipo_gasto_cds").cast("string").alias("cod_tipo_gasto_cds"),
            col("id_tipo_gasto_cds").cast("string").alias("id_tipo_gasto_cds"),
            col("cod_clasificacion_pl").cast("string").alias("cod_clasificacion_pl"),
            col("id_clasificacion_pl").cast("string").alias("id_clasificacion_pl"),
        )

    # id_columns = ["id_pais", "id_periodo", "id_compania", "id_sucursal", "cod_cuenta_contable", "cod_centro_costo"]
    partition_columns_array = ["id_pais", "id_periodo"]

    
    spark_controller.upsert(tmp_fact_saldo_contable_centro_costo, data_paths.BACK_OFFICE, target_table_name, partition_by=partition_columns_array)

except Exception as e:
    logger.error(e)
    raise