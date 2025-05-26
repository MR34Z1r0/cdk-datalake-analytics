from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER
from pyspark.sql.functions import abs, coalesce, col, concat, lit, max, round, substring, sum, upper, when

spark_controller = SPARK_CONTROLLER()

try:
    cod_pais = COD_PAIS.split(",")
    periodos= spark_controller.get_periods()
    logger.info(periodos)

    m_articulo = spark_controller.read_table(data_paths.DOMINIO, "m_articulo", cod_pais=cod_pais)
    m_formula_fabricacion = spark_controller.read_table(data_paths.DOMINIO, "m_formula_fabricacion", cod_pais=cod_pais)
    t_orden_produccion = spark_controller.read_table(data_paths.DOMINIO, "t_orden_produccion", cod_pais=cod_pais)
    t_orden_produccion_material = spark_controller.read_table(data_paths.DOMINIO, "t_orden_produccion_material", cod_pais=cod_pais)

    target_table_name = "fact_produccion"

except Exception as e:
    logger.error(e)
    raise


try:
    t_orden_produccion = t_orden_produccion.filter((col("id_periodo").isin(periodos)) & (col("id_pais").isin(cod_pais)))
    t_orden_produccion_material = t_orden_produccion_material.filter(col("id_pais").isin(cod_pais))
    m_articulo = m_articulo.filter(col("id_pais").isin(cod_pais))
    m_formula_fabricacion = m_formula_fabricacion.filter(col("id_pais").isin(cod_pais))

    temp_orden_produccion = (
        t_orden_produccion
        .select(
            col("id_pais"),
            col("id_orden_produccion"),
            col("id_sucursal"),
            col("id_periodo"),
            col("id_turno"),
            col("id_articulo"),
            col("id_familia_equipo"),
            col("nro_operacion"),
            col("cod_avail"),
            col("cod_linea_equipo"),
            col("cant_cajafisica_girada"),
            col("cant_cajafisica_fabricada"),
            col("cant_cajafisica_plan"),
            col("cant_unidades_fabricada"),
            col("cant_paradas_por_linea"),
            col("cant_paradas_ajena_linea"),
            col("cant_tiempo_ideal"),
            col("cant_velocidad"),
            col("cant_velocidad_hora").alias("cant_velocidad_por_hora"),
            col("cant_jornada_laboral"),
            col("cant_volumen_fabricado"),
            col("cant_cajavolumen_girada"),
            col("cant_velocidad_volumen").cast("decimal(18,5)"),
            col("fecha_inicio")
        )
        .orderBy(
            col("fecha_inicio"), 
            col("nro_operacion")
        )
    )

    temp_orden_produccion_material_inicial = (
        t_orden_produccion_material.alias("opm")
        .join(m_articulo.alias("ma"), col("opm.id_articulo") == col("ma.id_articulo"), "inner")
        .select(
            col("opm.id_pais"),
            col("opm.id_sucursal"),
            col("opm.id_articulo"),
            col("opm.id_orden_produccion"),
            col("ma.flg_jarabe"),
            col("ma.flg_co2"),
            col("ma.flg_jarabe_conver"),
            col("ma.flg_azucar"),
            col("ma.desc_articulo"),
            col("opm.cant_unidades_entregada"),
            col("opm.cant_unidades_teorica_fabricada")
        )
    )

    temp_orden_produccion_material = (
        temp_orden_produccion.alias("op")
        .join(temp_orden_produccion_material_inicial.alias("opm"), col("op.id_orden_produccion") == col("opm.id_orden_produccion"), "inner")
        .where(
            (col("opm.flg_jarabe") == 1) | (col("opm.flg_jarabe_conver") == 1)
            & (upper(substring(col("opm.desc_articulo"), 1, 6)) == "JARABE")
        )
        .groupby(
            col("op.id_pais"),
            col("op.id_orden_produccion"),
            col("op.id_sucursal"),
            col("op.id_articulo")
        )
        .agg(
            sum(col("opm.cant_unidades_entregada")).alias("cant_unidades_entregada"),
        )
        .select(
            col("op.id_pais"),
            col("op.id_orden_produccion"),
            col("op.id_sucursal"),
            col("op.id_articulo"),
            col("cant_unidades_entregada")
        )
    )

    temp_cosumo_standar_jarabe = (
        temp_orden_produccion_material_inicial
        .where(col("flg_jarabe") == 1)
        .groupby(col("id_orden_produccion"))
        .agg(max(col("cant_unidades_teorica_fabricada")).alias("cons_stand_jarabe"))
        .select(
            col("id_orden_produccion"),
            coalesce(col("cons_stand_jarabe"), lit(0)).alias("cons_stand_jarabe")
        )
    )

    temp_cosumo_standar_co2 = (
        temp_orden_produccion_material_inicial
        .where(col("flg_co2") == 1)
        .groupby(col("id_orden_produccion"))
        .agg(max(col("cant_unidades_teorica_fabricada")).alias("cons_stand_co2"))
        .select(
            col("id_orden_produccion"),
            coalesce(col("cons_stand_co2"), lit(0)).alias("cons_stand_co2")
        )
    )

    temp_factor_fabricacion_0 = (
        temp_orden_produccion_material_inicial
        .filter(col("flg_jarabe") == 1)
        .groupby(
            col("id_orden_produccion"),
            col("id_sucursal"),
        )
        .agg(
            max(col("id_articulo")).alias("id_articulo")
        )
        .select(
            col("id_orden_produccion"),
            col("id_sucursal"),
            col("id_articulo")
        )
    )

    temp_factor_fabricacion_1 = (
        temp_orden_produccion_material_inicial.alias("opm")
        .join(temp_factor_fabricacion_0.alias("tmp0"), (col("opm.id_orden_produccion") == col("tmp0.id_orden_produccion")) & (col("tmp0.id_articulo") == col("opm.id_articulo")), "inner")
        .where(col("opm.flg_jarabe") == 1)
        .groupby(
            col("opm.id_orden_produccion"),
            col("opm.id_sucursal"),
            col("opm.id_articulo")
        )
        .agg(max(col("opm.cant_unidades_teorica_fabricada")).alias("LCATTIOFAB"))
        .select(
            col("opm.id_orden_produccion"),
            col("opm.id_sucursal"),
            col("opm.id_articulo"),
            col("LCATTIOFAB")
        )
    )

    temp_factor_fabricacion_2 = (
        m_formula_fabricacion.alias("opm")
        .join(temp_factor_fabricacion_1.alias("tmp1"), (col("opm.id_articulo") == col("tmp1.id_articulo")) & (col("tmp1.id_sucursal") == col("opm.id_sucursal")), "inner")
        .join(m_articulo.alias("ma"), col("opm.id_material") == col("ma.id_articulo"), "inner")
        .where(col("ma.flg_jarabe_conver") == 1)
        .groupby(
            col("opm.id_material"),
            col("opm.id_sucursal"),
            col("opm.id_articulo")
        )
        .agg(max(col("opm.factor_conversion")).alias("LFACTCONV1"))
        .select(
            col("opm.id_sucursal"),
            col("opm.id_material"),
            col("opm.id_articulo"),
            col("LFACTCONV1")
        )
    )

    temp_factor_fabricacion_3 = (
        m_formula_fabricacion.alias("opm")
        .join(temp_factor_fabricacion_2.alias("tmp2"), (col("tmp2.id_material") == col("opm.id_articulo")) & (col("tmp2.id_sucursal") == col("opm.id_sucursal")), "inner")
        .join(m_articulo.alias("ma"), col("opm.id_material") == col("ma.id_articulo"), "inner")
        .where(col("ma.flg_azucar") == 1)
        .groupby(
            col("opm.id_material"),
            col("opm.id_sucursal"),
            col("opm.id_articulo")
        )
        .agg(max(col("opm.factor_conversion")).alias("LFACTCONV2"))
        .select(
            col("opm.id_sucursal"),
            col("opm.id_material"),
            col("opm.id_articulo"),
            col("LFACTCONV2")
        )
    )

    temp_factor_fabricacion_final_tmp1 = (
        temp_factor_fabricacion_1.alias("tff1")
        .join(temp_factor_fabricacion_2.alias("tff2"), (col("tff1.id_articulo") == col("tff2.id_articulo")) & (col("tff1.id_sucursal") == col("tff2.id_sucursal")), "left")
        .join(temp_factor_fabricacion_3.alias("tff3"), (col("tff2.id_material") == col("tff3.id_articulo")) & (col("tff2.id_sucursal") == col("tff3.id_sucursal")), "left")
        .join(m_articulo.alias("ma"), col("tff1.id_articulo") == col("ma.id_articulo"), "left")
        .select(
            col("tff1.id_orden_produccion"),
            col("tff1.id_sucursal"),
            col("tff1.id_articulo"),
            coalesce(col("tff1.LCATTIOFAB"), lit(0)).alias("LCATTIOFAB"),
            coalesce(col("tff2.LFACTCONV1"), lit(0)).alias("LFACTCONV1"),
            coalesce(col("tff3.LFACTCONV2"), lit(0)).alias("LFACTCONV2"),

        )
    )
    temp_factor_fabricacion_final_tmp2 = (
        temp_factor_fabricacion_final_tmp1
        .withColumn("cons_stand_azucar", round(col("LCATTIOFAB"), 4) * round(col("LFACTCONV1"), 4) * round(col("LFACTCONV2"), 4))
    )
    temp_factor_fabricacion_final = (
        temp_factor_fabricacion_final_tmp2
        .select(
            col("id_orden_produccion"),
            col("id_sucursal"),
            col("id_articulo"),
            col("LCATTIOFAB"),
            col("LFACTCONV1"),
            col("LFACTCONV2"),
            coalesce(col("cons_stand_azucar"), lit(0)).alias("cons_stand_azucar")
        )
    )

    temp_jarabe_lit_batch = (
        m_formula_fabricacion.alias("opm")
        .join(m_articulo.alias("ma"), col("opm.id_material") == col("ma.id_articulo"), "inner")
        .where(col("ma.flg_jarabe") == 1)
        .select(
            col("opm.id_sucursal"),
            col("opm.id_articulo"),
            col("opm.id_material"),
            col("opm.cant_fabricacion").alias("cons_stand_bach_jarabe")
        )
        .distinct()
    )

    temp_orden_produccion_calculo = (
        temp_orden_produccion.alias("op")
        .join(temp_orden_produccion_material.alias("opm"), col("op.id_orden_produccion") == col("opm.id_orden_produccion"), "left")
        .join(temp_cosumo_standar_jarabe.alias("tcsj"), col("op.id_orden_produccion") == col("tcsj.id_orden_produccion"), "left")
        .join(temp_cosumo_standar_co2.alias("tcsc"), col("op.id_orden_produccion") == col("tcsc.id_orden_produccion"), "left")
        .join(temp_factor_fabricacion_final.alias("tfff"), col("op.id_orden_produccion") == col("tfff.id_orden_produccion"), "left")
        .join(temp_jarabe_lit_batch.alias("tjlb"), (col("opm.id_articulo") == col("tjlb.id_articulo")) & (col("opm.id_sucursal") == col("tjlb.id_sucursal")), "left")
        .select(
            col("op.id_pais"),
            col("op.id_orden_produccion"),
            col("op.id_sucursal"),
            col("op.id_periodo"),
            col("op.id_turno"),
            col("op.id_articulo"),
            col("op.id_familia_equipo"),
            col("op.nro_operacion"),
            col("op.cod_avail"),
            col("op.cod_linea_equipo"),
            col("op.cant_cajafisica_girada"),
            col("op.cant_cajafisica_fabricada"),
            col("op.cant_cajafisica_plan"),
            col("op.cant_paradas_por_linea"), 
            col("op.cant_paradas_ajena_linea"), 
            col("op.cant_tiempo_ideal"), 
            col("op.cant_velocidad").cast("decimal(18,6)").alias("cant_velocidad"), 
            col("op.cant_velocidad_por_hora"), 
            col("op.cant_jornada_laboral"), 
            col("op.cant_volumen_fabricado").cast("decimal(18,6)").alias("cant_volumen_fabricado"),
            col("tcsj.cons_stand_jarabe").cast("decimal(18,6)").alias("cons_stand_jarabe"),
            round(col("tfff.cons_stand_azucar"), 4).alias("cons_stand_azucar"),
            col("tcsc.cons_stand_co2").alias("cons_stand_co2"),
            (col("op.cant_volumen_fabricado") / 1000).alias("lt_x_tn"),
            (col("op.cant_cajavolumen_girada") / 30).alias("cant_volumen_programado"),
            round(((col("op.cant_velocidad_volumen") / 30) * 60), 5).alias("cant_volumen_velocidad"),
            coalesce(round(col("tjlb.cons_stand_bach_jarabe"), 4), lit(0)).alias("cons_stand_bach_jarabe"),
            col("op.cant_unidades_fabricada").cast("decimal(18,6)").alias("cant_unidades_fabricada"),
            coalesce(col("opm.cant_unidades_entregada"), lit(0)).alias("cant_unidades_entregada"), 
            col("op.fecha_inicio")
        )
    )

    temp_fact_orden_produccion = (
        temp_orden_produccion_calculo.alias("opc")
        .join(m_articulo.alias("ar"), col("opc.id_articulo") == col("ar.id_articulo"), "left")
        .select(
            col("opc.id_pais"),
            col("opc.id_periodo"), 
            col("opc.id_orden_produccion"),
            col("opc.id_sucursal"), 
            col("opc.id_articulo"),
            concat(
                col("opc.id_familia_equipo"),
                lit("|"),
                col("opc.cod_linea_equipo")
            ).alias("id_linea_produccion"),
            col("opc.id_turno").alias("id_turno_produccion"),
            col("opc.fecha_inicio").alias("fecha_produccion"),
            col("opc.nro_operacion"),
            col("opc.cod_avail"),
            col("opc.cant_cajafisica_girada").alias("cant_cajafisica_programada"),
            col("opc.cant_cajafisica_fabricada").alias("cant_cajafisica_producida"),
            col("opc.cant_cajafisica_plan"),
            col("opc.cant_unidades_entregada").alias("cant_consumo_jarabe_real"),
            col("opc.cant_jornada_laboral"),
            col("opc.cant_paradas_por_linea"),
            col("opc.cant_paradas_ajena_linea"),
            col("opc.cant_tiempo_ideal"),
            col("opc.cant_velocidad"),
            col("opc.cant_velocidad_por_hora"),
            col("opc.cant_unidades_fabricada"),
            col("opc.cant_volumen_fabricado"),
            col("opc.cons_stand_jarabe").alias("cant_consumo_estandar_jarabe"), 
            col("opc.cons_stand_azucar").alias("cant_consumo_estandar_azucar"), 
            col("opc.cons_stand_co2").alias("cant_consumo_estandar_co2"),
            col("opc.lt_x_tn").cast("decimal(18,6)").alias("cant_volumen_por_tonelada"),
            when((col("ar.cant_unidad_paquete") == 0) | (col("ar.cant_cajas_por_palet") == 0), 0)
            .otherwise((col("opc.cant_unidades_fabricada")) / ((col("ar.cant_unidad_paquete") * col("ar.cant_cajas_por_palet")).cast("decimal(18,6)")))
            .cast("decimal(18,6)").alias("cant_pallets"),
            
            when(col("ar.cant_unidad_paquete") == 0, 0)
            .otherwise(abs((col("opc.cant_unidades_fabricada") / col("ar.cant_unidad_paquete")) - col("opc.cant_cajafisica_girada")))
            .cast("decimal(18,6)").alias("cant_absoluto"),

            when(col("opc.cons_stand_jarabe") == 0, 0)
            .otherwise(((col("opc.cant_unidades_entregada") * col("opc.cons_stand_azucar")).cast("decimal(18,6)") / col("opc.cons_stand_jarabe")))
            .cast("decimal(18,6)").alias("cant_consumo_real_azucar"),

            when(col("opc.cant_velocidad") == 0, 0)
            .otherwise(((col("opc.cant_cajafisica_fabricada") * col("ar.cant_unidad_paquete")).cast("decimal(18,6)") / col("opc.cant_velocidad")))
            .cast("decimal(18,6)").alias("cant_unidades_por_velocidad"),
            
            when(col("ar.cant_unidad_paquete") == 0, 0)
            .otherwise((col("opc.cant_unidades_fabricada") / (col("ar.cant_unidad_paquete")).cast("decimal(18,6)")))
            .cast("decimal(18,6)").alias("cant_cajafisica_fabricada"),
            
            when(col("opc.cons_stand_bach_jarabe") == 0, 0)
            .otherwise((col("opc.cant_unidades_entregada") / col("opc.cons_stand_bach_jarabe")))
            .cast("decimal(18,6)").alias("cant_consumo_standar_batch_jarabe"),
            
            col("opc.cant_volumen_programado").cast("decimal(18,6)").alias("cant_volumen_programado"),
            col("opc.cant_volumen_velocidad").alias("cant_volumen_velocidad")
        )
    )

    tmp = temp_fact_orden_produccion.select(
        col("id_pais").cast("string").alias("id_pais"),
        col("id_periodo").cast("string").alias("id_periodo"),
        col("id_orden_produccion").cast("string").alias("id_orden_produccion"),
        col("id_sucursal").cast("string").alias("id_sucursal"),
        col("id_articulo").cast("string").alias("id_articulo"),
        col("id_linea_produccion").cast("string").alias("id_linea_produccion"),
        col("id_turno_produccion").cast("string").alias("id_turno_produccion"),
        col("fecha_produccion").cast("date").alias("fecha_produccion"),
        col("nro_operacion").cast("string").alias("nro_operacion"),
        col("cod_avail").cast("string").alias("cod_avail"),
        col("cant_cajafisica_programada").cast("numeric(32,12)").alias("cant_cajafisica_programada"),
        col("cant_cajafisica_producida").cast("numeric(32,12)").alias("cant_cajafisica_producida"),
        col("cant_cajafisica_plan").cast("numeric(32,12)").alias("cant_cajafisica_plan"),
        col("cant_consumo_jarabe_real").cast("numeric(32,12)").alias("cant_consumo_jarabe_real"),
        col("cant_jornada_laboral").cast("numeric(32,12)").alias("cant_jornada_laboral"),
        col("cant_paradas_por_linea").cast("numeric(32,12)").alias("cant_paradas_por_linea"),
        col("cant_paradas_ajena_linea").cast("numeric(32,12)").alias("cant_paradas_ajena_linea"),
        round(col("cant_tiempo_ideal"), 6).cast("numeric(32,12)").alias("cant_tiempo_ideal"),
        col("cant_velocidad").cast("numeric(32,12)").alias("cant_velocidad"),
        col("cant_velocidad_por_hora").cast("numeric(32,12)").alias("cant_velocidad_por_hora"),
        col("cant_unidades_fabricada").cast("numeric(32,12)").alias("cant_unidades_fabricada"),
        col("cant_volumen_fabricado").cast("numeric(32,12)").alias("cant_volumen_fabricado"),
        coalesce(col("cant_consumo_estandar_jarabe"), lit(0)).cast("numeric(32,12)").alias("cant_consumo_estandar_jarabe"),
        coalesce(col("cant_consumo_estandar_azucar"), lit(0)).cast("numeric(32,12)").alias("cant_consumo_estandar_azucar"),
        coalesce(col("cant_consumo_estandar_co2"), lit(0)).cast("numeric(32,12)").alias("cant_consumo_estandar_co2"),
        col("cant_volumen_por_tonelada").cast("numeric(32,12)").alias("cant_volumen_por_tonelada"),
        round(col("cant_pallets"), 6).cast("numeric(32,12)").alias("cant_pallets"),
        col("cant_absoluto").cast("numeric(32,12)").alias("cant_absoluto"),
        coalesce(col("cant_consumo_real_azucar"), lit(0)).cast("numeric(32,12)").alias("cant_consumo_real_azucar"),
        round(col("cant_unidades_por_velocidad"), 6).cast("numeric(32,12)").alias("cant_unidades_por_velocidad"),
        col("cant_cajafisica_fabricada").cast("numeric(32,12)").alias("cant_cajafisica_fabricada"),
        col("cant_consumo_standar_batch_jarabe").cast("numeric(32,12)").alias("cant_consumo_standar_batch_jarabe"),
        col("cant_volumen_programado").cast("numeric(32,12)").alias("cant_volumen_programado"),
        col("cant_volumen_velocidad").cast("numeric(32,12)").alias("cant_volumen_velocidad"),
        )

    partition_columns_array = ["id_pais", "id_periodo"]    
    spark_controller.write_table(tmp, data_paths.CADENA, target_table_name, partition_columns_array)

except Exception as e:
    logger.error(e)
    raise