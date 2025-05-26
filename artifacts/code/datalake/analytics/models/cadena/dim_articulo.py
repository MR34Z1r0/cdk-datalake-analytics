from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import coalesce, col, lit, when

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    m_articulo = spark_controller.read_table(data_paths.DOMINIO, "m_articulo", cod_pais=cod_pais)
    m_categoria_compra = spark_controller.read_table(data_paths.DOMINIO, "m_categoria_compra", cod_pais=cod_pais)

    target_table_name = "dim_articulo"

except Exception as e:
    logger.error(e)
    raise


try:
    tmp_dim_articulo = (
        m_articulo.alias("ma")
        .filter(col("ma.id_pais").isin(cod_pais))
        .join(m_categoria_compra.alias("mcc"), col("mcc.id_articulo") == col("ma.id_articulo"), "left")
        .select(
            col("ma.id_articulo").alias("id_articulo"),
            col("ma.id_pais").alias("id_pais"),
            col("ma.id_articulo_ref").alias("id_articulo_corp"),
            when((col("ma.id_pais") == "PE") & (col("ma.cod_linea") == "02") & (col("ma.cod_familia").isin("008","016","017","020","021","022","027","000")), col("ma.id_articulo"))
            .when((col("ma.id_pais") == "MX") & (col("ma.cod_linea") == "20") & (col("ma.cod_familia") == "001"), col("ma.id_articulo"))
            .when((col("ma.id_pais").isin("GT", "PA", "NI", "HN", "CR", "SV")) & (col("ma.cod_linea") == "02") & (col("ma.cod_familia").isin("008","016","017","020","021","022","027","001","002","003","004","006","009")), col("ma.id_articulo"))
            .when((col("ma.id_pais") == "EC") & (col("ma.cod_linea") == "02") & (col("ma.cod_familia") == "022"), col("ma.id_articulo"))
            .when(col("ma.id_pais") == "BO", col("ma.id_articulo_ref"))
            .when(col("ma.id_pais") == "CO", col("ma.id_articulo"))
            .otherwise(col("ma.id_articulo_ref"))
            .alias("id_articulo_ext"),
            col("ma.cod_articulo").alias("cod_articulo"),
            col("ma.cod_articulo_corp").alias("cod_articulo_corp"),
            when((col("ma.id_pais") == "PE") & (col("ma.cod_linea") == "02") & (col("ma.cod_familia").isin("008","016","017","020","021","022","027","000")), col("ma.cod_articulo"))
            .when((col("ma.id_pais") == "MX") & (col("ma.cod_linea") == "20") & (col("ma.cod_familia") == "001"), col("ma.cod_articulo"))
            .when((col("ma.id_pais").isin("GT", "PA", "NI", "HN", "CR", "SV")) & (col("ma.cod_linea") == "02") & (col("ma.cod_familia").isin("008","016","017","020","021","022","027","001","002","003","004","006","009")), col("ma.cod_articulo"))
            .when((col("ma.id_pais") == "EC") & (col("ma.cod_linea") == "02") & (col("ma.cod_familia") == "022"), col("ma.cod_articulo"))
            .when(col("ma.id_pais") == "BO", col("ma.cod_articulo_corp"))
            .when(col("ma.id_pais") == "CO", col("ma.cod_articulo"))
            .otherwise(col("ma.cod_articulo_corp"))
            .alias("cod_articulo_ext"),
            col("ma.desc_articulo_corp").alias("desc_articulo_corp"),
            col("ma.desc_articulo").alias("desc_articulo"),
            col("ma.cod_categoria").alias("cod_categoria"),
            col("ma.desc_categoria").alias("desc_categoria"),
            col("ma.cod_marca").alias("cod_marca"),
            col("ma.desc_marca").alias("desc_marca"),
            col("ma.cod_formato").alias("cod_formato"),
            col("ma.desc_formato").alias("desc_formato"),
            col("ma.cod_sabor").alias("cod_sabor"),
            col("ma.desc_sabor").alias("desc_sabor"),
            col("ma.cod_presentacion").alias("cod_presentacion"),
            col("ma.desc_presentacion").alias("desc_presentacion"),
            col("ma.cod_tipo_envase").alias("cod_tipo_envase"),
            col("ma.desc_tipo_envase").alias("desc_tipo_envase"),
            col("ma.cod_aroma").alias("cod_aroma"),
            col("ma.desc_aroma").alias("desc_aroma"),
            col("ma.cod_gasificado").alias("cod_gasificado"),
            col("ma.desc_gasificado").alias("desc_gasificado"),
            col("ma.cod_linea").alias("cod_linea"),
            col("ma.desc_linea").alias("desc_linea"),
            col("ma.flg_explosion").alias("flg_explosion"),
            col("ma.cod_familia").alias("cod_familia"),
            col("ma.desc_familia").alias("desc_familia"),
            col("ma.cod_subfamilia").alias("cod_subfamilia"),
            col("ma.desc_subfamilia").alias("desc_subfamilia"),
            col("mcc.cod_categoria").alias("cod_categoria_compra"),
            col("mcc.desc_categoria").alias("desc_categoria_compra"),
            col("mcc.cod_subcategoria").alias("cod_subcategoria_compra"),
            col("mcc.desc_subcategoria").alias("desc_subcategoria_compra"),
            col("mcc.cod_subcategoria2").alias("cod_subcategoria2_compra"),
            col("mcc.desc_subcategoria2").alias("desc_subcategoria2_compra"),
            col("mcc.cod_unidad_global").alias("cod_unidad_global"),
            col("ma.cod_unidad_compra").alias("cod_unidad_compra"),
            col("ma.cod_unidad_manejo").alias("cod_unidad_manejo"),
            coalesce(col("ma.cod_unidad_volumen"), lit("000")).alias("cod_unidad_volumen"),
            col("ma.cant_unidad_peso").alias("cant_unidad_peso"),
            col("ma.cant_unidad_volumen").alias("cant_unidad_volumen"),
            col("ma.cant_unidad_paquete").alias("cant_unidad_paquete"),
            col("ma.cant_paquete_caja").alias("cant_paquete_caja"),
            col("ma.flgskuplan").alias("flgskuplan"),
        )
    )

    tmp = tmp_dim_articulo.select(
        col("id_articulo").cast("string").alias("id_articulo"),
        col("id_pais").cast("string").alias("id_pais"),
        col("id_articulo_corp").cast("string").alias("id_articulo_corp"),
        col("id_articulo_ext").cast("string").alias("id_articulo_ext"),
        col("cod_articulo").cast("string").alias("cod_articulo"),
        col("cod_articulo_corp").cast("string").alias("cod_articulo_corp"),
        col("cod_articulo_ext").cast("string").alias("cod_articulo_ext"),
        col("desc_articulo_corp").cast("string").alias("desc_articulo_corp"),
        col("desc_articulo").cast("string").alias("desc_articulo"),
        col("cod_categoria").cast("string").alias("cod_categoria"),
        col("desc_categoria").cast("string").alias("desc_categoria"),
        col("cod_marca").cast("string").alias("cod_marca"),
        col("desc_marca").cast("string").alias("desc_marca"),
        col("cod_formato").cast("string").alias("cod_formato"),
        col("desc_formato").cast("string").alias("desc_formato"),
        col("cod_sabor").cast("string").alias("cod_sabor"),
        col("desc_sabor").cast("string").alias("desc_sabor"),
        col("cod_presentacion").cast("string").alias("cod_presentacion"),
        col("desc_presentacion").cast("string").alias("desc_presentacion"),
        col("cod_tipo_envase").cast("string").alias("cod_tipo_envase"),
        col("desc_tipo_envase").cast("string").alias("desc_tipo_envase"),
        col("cod_aroma").cast("string").alias("cod_aroma"),
        col("desc_aroma").cast("string").alias("desc_aroma"),
        col("cod_gasificado").cast("string").alias("cod_gasificado"),
        col("desc_gasificado").cast("string").alias("desc_gasificado"),
        col("cod_linea").cast("string").alias("cod_linea"),
        col("desc_linea").cast("string").alias("desc_linea"),
        col("flg_explosion").cast("string").alias("flg_explosion"),
        col("cod_familia").cast("string").alias("cod_familia"),
        col("desc_familia").cast("string").alias("desc_familia"),
        col("cod_subfamilia").cast("string").alias("cod_subfamilia"),
        col("desc_subfamilia").cast("string").alias("desc_subfamilia"),
        col("cod_categoria_compra").cast("string").alias("cod_categoria_compra"),
        col("desc_categoria_compra").cast("string").alias("desc_categoria_compra"),
        col("cod_subcategoria_compra").cast("string").alias("cod_subcategoria_compra"),
        col("desc_subcategoria_compra").cast("string").alias("desc_subcategoria_compra"),
        col("cod_subcategoria2_compra").cast("string").alias("cod_subcategoria2_compra"),
        col("desc_subcategoria2_compra").cast("string").alias("desc_subcategoria2_compra"),
        col("cod_unidad_global").cast("string").alias("cod_unidad_global"),
        col("cod_unidad_compra").cast("string").alias("cod_unidad_compra"),
        col("cod_unidad_manejo").cast("string").alias("cod_unidad_manejo"),
        col("cod_unidad_volumen").cast("string").alias("cod_unidad_volumen"),
        col("cant_unidad_peso").cast("string").alias("cant_unidad_peso"),
        col("cant_unidad_volumen").cast("numeric(38, 12)").alias("cant_unidad_volumen"),
        col("cant_unidad_paquete").cast("numeric(38, 12)").alias("cant_unidad_paquete"),
        col("cant_paquete_caja").cast("numeric(38, 12)").alias("cant_paquete_caja"),
        col("flgskuplan").cast("string").alias("flgskuplan"),
    )

    id_columns = ["id_articulo", "id_pais"]
    partition_columns_array = ["id_pais"]

    spark_controller.upsert(tmp, data_paths.CADENA, target_table_name, id_columns, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise
