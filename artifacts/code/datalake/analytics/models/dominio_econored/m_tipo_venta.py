import datetime as dt
from common_jobs_functions import logger, SPARK_CONTROLLER, data_paths, COD_PAIS
from pyspark.sql.functions import col, concat, lit, coalesce, when, trim, row_number,current_date,upper
from pyspark.sql.types import StringType, DateType

spark_controller = SPARK_CONTROLLER()
try:
    cod_pais = COD_PAIS.split(",")
 
    df_m_tipo_documento = spark_controller.read_table(data_paths.BIG_BAGIC, "m_tipo_documento", cod_pais=cod_pais)
    df_m_procedimiento = spark_controller.read_table(data_paths.BIG_BAGIC, "m_procedimiento", cod_pais=cod_pais)
    df_m_tipo_transaccion = spark_controller.read_table(data_paths.BIG_BAGIC, "m_tipo_transaccion", cod_pais=cod_pais)
    df_m_pais = spark_controller.read_table(data_paths.BIG_BAGIC, "m_pais", cod_pais=cod_pais,have_principal = True)
    df_m_compania = spark_controller.read_table(data_paths.BIG_BAGIC, "m_compania", cod_pais=cod_pais)
 
    target_table_name = "m_tipo_venta"

except Exception as e:
    logger.error(e)
    raise 
try:
    st_tipo_transaccion = df_m_tipo_transaccion.where(
        col("cod_tipo_transaccion") == "DCV"
    ).select(col("cod_compania"), col("cod_documento_transaccion"))

    tmp_m_tipo_venta = (
        df_m_procedimiento.alias("d")
        .join(
            df_m_tipo_documento.alias("c"),
            (col("d.cod_compania") == col("c.cod_compania"))
            & (col("d.cod_documento_transaccion") == col("c.cod_tipo_documento")),
            "inner",
        )
        .join(
            df_m_compania.alias("e"),
            col("d.cod_compania") == col("e.cod_compania"),
            "inner",
        )
        .join(df_m_pais.alias("mp"), col("mp.cod_pais") == col("e.cod_pais"), "inner")
        .join(
            st_tipo_transaccion.alias("tt"),
            (col("c.cod_compania") == col("tt.cod_compania"))
            & (col("c.cod_tipo_documento") == col("tt.cod_documento_transaccion")),
            "inner",
        )
        .where(col("mp.id_pais").isin(cod_pais))
        .select(
            concat(
                trim(col("d.cod_compania")),
                lit("|"),
                trim(col("d.cod_documento_transaccion")),
                lit("|"),
                trim(col("d.cod_procedimiento")),
            ).cast(StringType()).alias("id_tipo_venta"),
            col("mp.id_pais").cast(StringType()).alias("id_pais"),
            col("d.cod_procedimiento").cast(StringType()).alias("cod_tipo_venta"),
            coalesce(col("d.desc_procedimiento"), lit("ninguno")).cast(StringType()).alias("nomb_tipo_venta"),
            col("d.cod_tipo_operacion").cast(StringType()).alias("cod_tipo_operacion"),
            current_date().cast(DateType()).alias("fecha_creacion"),
            current_date().cast(DateType()).alias("fecha_modificacion")
        )
    )

    id_columns = ["id_tipo_venta"]
    partition_columns_array = ["id_pais"]
    logger.info(f"starting upsert of {target_table_name}")
    spark_controller.upsert(tmp_m_tipo_venta, data_paths.DOMINIO_ECONORED, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise