import calendar
import datetime as dt
import sys
from awsglue.utils import getResolvedOptions
from common_jobs_functions import data_paths, logger, COD_PAIS, SPARK_CONTROLLER, NOW_LIMA,S3_PATH_ANALYTICS
from pyspark.sql.window import Window
from pyspark.sql.functions import coalesce,col,date_add,date_format,datediff,expr,lag,last_day,lit,max,row_number,sum,to_date,trunc,when

args = getResolvedOptions(
    sys.argv,
    [
        "USE_HARDCODED_DATE",
    ],
)
USE_HARDCODED_DATE = args["USE_HARDCODED_DATE"]

spark_controller = SPARK_CONTROLLER()
target_table_name = "fact_movimiento_inventario_diario"

try:
    cod_pais = COD_PAIS.split(",")
    # periodos = spark_controller.get_periods()
    # logger.info(periodos)

    m_articulo = spark_controller.read_table(data_paths.DOMINIO, "m_articulo", cod_pais=cod_pais)
    m_almacen = spark_controller.read_table(data_paths.DOMINIO, "m_almacen", cod_pais=cod_pais)
    t_saldos_iniciales = spark_controller.read_table(data_paths.DOMINIO, "t_saldos_iniciales", cod_pais=cod_pais)
    t_movimiento_inventario = spark_controller.read_table(data_paths.DOMINIO, "t_movimiento_inventario", cod_pais=cod_pais)
    t_movimiento_inventario_detalle = spark_controller.read_table(data_paths.DOMINIO, "t_movimiento_inventario_detalle", cod_pais=cod_pais)


except Exception as e:
    logger.error(e)
    raise


try:
    current_date = NOW_LIMA
    current_date = current_date.date()
    if USE_HARDCODED_DATE == "yyyy-mm-dd":
        yesterday_date = current_date - dt.timedelta(days=1)
        start_date = yesterday_date.replace(day=1)  # Start of the current month
        periodos = [yesterday_date.strftime("%Y%m")]

    else:
        start_date = dt.datetime.strptime(USE_HARDCODED_DATE, "%Y-%m-%d").date()
        # Set start_date to the last day of the month
        last_day_of_month = calendar.monthrange(start_date.year, start_date.month)[1]
        start_date = start_date.replace(day=last_day_of_month)
        yesterday_date = start_date
        # Set current_date to 6 months after the start_date
        next_month = (start_date.month + 5) % 12 + 1
        next_year = start_date.year + ((start_date.month + 5) // 12)
        current_date = start_date.replace(year=next_year, month=next_month, day=1) - dt.timedelta(days=1)



    while start_date <= current_date:
        periodos = [start_date.strftime("%Y%m")]
        logger.info("Start while loop")
        logger.info(f"Start date: {start_date} | Yesterday date: {yesterday_date} | Current date: {current_date} | Periodos: {periodos}")

        logger.info("tmp_parametro")
        parametro_df = spark_controller.spark.createDataFrame([(yesterday_date,)], ["fecha_hoy"])
        parametro_df.show()

        tmp_almacenes = (
            m_almacen.alias("ma")
            .filter(col("ma.id_pais").isin(cod_pais))
            .crossJoin(parametro_df.alias("p"))
            .select(
                col("ma.id_compania"),
                col("ma.id_sucursal"),
                col("ma.id_almacen"),
                last_day(trunc(col("p.fecha_hoy"), "mm") - expr("INTERVAL 1 day")).alias("fecha_inventario"),
                col("p.fecha_hoy").alias("fecha_inventario_fin"),

            )
        )

        tmp_t_historico_inventario = (
            t_saldos_iniciales.alias("tsi")          
            .filter((col("tsi.estado") == lit("C")) & (col("tsi.id_pais").isin(cod_pais)))
            .select(
                col("tsi.id_compania"),
                col("tsi.id_sucursal"),
                col("tsi.id_almacen"),
                col("tsi.fecha_inventario"),
            )
            .distinct()
        )

        t_historico_inventario_mes_actual = (
            tmp_t_historico_inventario.alias("t")
            .join(parametro_df.alias("p"), trunc(col("t.fecha_inventario"), "mm") == trunc(col("p.fecha_hoy"), "mm"), "inner")
            .groupBy(
                col("t.id_compania"),
                col("t.id_sucursal"),
                col("t.id_almacen")
            )
            .agg(
                max(col("t.fecha_inventario")).alias("max_fecha_inventario"),
                max(col("p.fecha_hoy")).alias("fecha_inventario_fin")
            )
            .withColumn(
                "fecha_inventario",
                last_day(
                    trunc(col("max_fecha_inventario"), "mm") - expr("INTERVAL 1 day")
                )
            )
            .select(
                col("t.id_compania"),
                col("t.id_sucursal"),
                col("t.id_almacen"),
                col("fecha_inventario"),
                col("fecha_inventario_fin")
            )
        )

        t_historico_inventario = (
            tmp_t_historico_inventario.alias("t")
            .join(
                parametro_df.alias("p"), (col("t.fecha_inventario") < col("p.fecha_hoy")) & (last_day(col("t.fecha_inventario")) == col("t.fecha_inventario")), "inner")
            .groupBy(
                "t.id_compania", 
                "t.id_sucursal", 
                "t.id_almacen"
            )
            .agg(
                coalesce(
                    max("t.fecha_inventario"),
                    to_date(lit("1900-01-01"), "yyyy-MM-dd") # nosotros hemos estado trabajando las fechas vacías como null; confirmar
                ).alias("fecha_inventario"),
                max("p.fecha_hoy").alias("fecha_inventario_fin")
            )
            .select(
                "t.id_compania",
                "t.id_sucursal",
                "t.id_almacen",
                "fecha_inventario",
                "fecha_inventario_fin"
            )
        )

        tmp_new_rows_mes_actual = (
            t_historico_inventario_mes_actual.alias("tmp")
            .join(t_historico_inventario.alias("thii"), col("thii.id_almacen") == col("tmp.id_almacen"), "left_anti")
        )
        t_historico_inventario = t_historico_inventario.unionByName(tmp_new_rows_mes_actual)

        tmp_new_rows_almacenes = (
            tmp_almacenes.alias("tmp")
            .join(t_historico_inventario.alias("thii"), col("thii.id_almacen") == col("tmp.id_almacen"), "left_anti")
        )
        t_historico_inventario = t_historico_inventario.unionByName(tmp_new_rows_almacenes)

        t_historico_inventario_inicial = (
            t_saldos_iniciales.alias("s")
            .join(t_historico_inventario.alias("tf"), (col("s.id_almacen") == col("tf.id_almacen")) & (col("s.fecha_inventario") == col("tf.fecha_inventario")), "inner")
            .select(
                col("s.id_pais"),
                col("s.id_compania"),
                col("s.id_sucursal"),
                col("s.id_almacen"),
                col("s.id_articulo"),
                col("s.fecha_inventario").alias("fecha_almacens"),
                col("s.cant_cajafisica_inicial").alias("cant_cajafisica"),
                col("s.cant_unidades_inicial").alias("cant_unidades"),
                lit(0).alias("imp_total_transito"),
                lit(0).alias("cant_unidades_transito"),
                col("s.precio_unitario_mn"),
                col("s.precio_unitario_me"),
                col("s.imp_valorizado_mn"),
                col("s.imp_valorizado_me"),
                col("s.imp_saldo_inicial"),
                col("s.imp_saldo_final"),
                col("s.imp_valorizado_ingreso"),
                col("s.imp_valorizado_salida")
            )
        )

        t_historico_inventario_movimiento_previo = (
            t_movimiento_inventario.alias("ha")
            .join(t_movimiento_inventario_detalle.alias("had"), 
                    (
                        (col("ha.id_sucursal_origen") == col("had.id_sucursal")) 
                        & (col("ha.nro_documento_movimiento") == col("had.nro_documento_movimiento"))
                    )
                    , "inner")
            .join(t_historico_inventario.alias("thi"), 
                    (
                        (col("had.id_almacen") == col("thi.id_almacen")) 
                        & (col("had.fecha_almacen") >= date_add(col("thi.fecha_inventario"), 1)) 
                        & (col("had.fecha_almacen") <= col("thi.fecha_inventario_fin"))
                    )
                    , "inner")
            .groupBy(
                col("had.id_pais"),
                col("ha.id_compania_origen"),
                col("ha.id_sucursal_origen"),
                col("ha.id_almacen_origen"),
                col("had.id_articulo"),
                col("had.fecha_almacen")                
            )
            .agg(
                sum(col("had.cant_cajafisica")).alias("cant_cajafisica"),
                sum(col("had.cant_cajafisica_ingresada")).alias("cant_cajafisica_ingresada"),
                sum(col("had.cant_cajafisica_salida")).alias("cant_cajafisica_salida"),
                sum(col("had.cant_cajafisica_total")).alias("cant_cajafisica_total"),
                sum(col("had.cant_cajafisica_ingresada_total")).alias("cant_cajafisica_ingresada_total"),
                sum(col("had.cant_cajafisica_salida_total")).alias("cant_cajafisica_salida_total"),
                sum(col("had.cant_unidades")).alias("cant_unidades"),
                sum(col("had.cant_unidades_total")).alias("cant_unidades_total"),
                sum(col("had.cant_unidades_total_ingresada")).alias("cant_unidades_total_ingresada"),
                sum(col("had.cant_unidades_total_salida")).alias("cant_unidades_total_salida"),
                sum(col("had.cant_unidades_transito")).alias("cant_unidades_transito"),
                sum(col("had.imp_total_transito")).alias("imp_total_transito"),
                max(col("had.precio_unitario_mn")).alias("precio_unitario_mn"),
                max(col("had.precio_unitario_me")).alias("precio_unitario_me"),
                sum(col("had.imp_valorizado_mn")).alias("imp_valorizado_mn"),
                sum(col("had.imp_valorizado_me")).alias("imp_valorizado_me"),
                max(col("had.imp_saldo_inicial")).alias("imp_saldo_inicial"),
                max(col("had.imp_saldo_final")).alias("imp_saldo_final"),
                sum(col("had.imp_valorizado_ingreso")).alias("imp_valorizado_ingreso"),
                sum(col("had.imp_valorizado_salida")).alias("imp_valorizado_salida")
            )
            .select(
                col("had.id_pais"),
                col("ha.id_compania_origen"),
                col("ha.id_sucursal_origen"),
                col("ha.id_almacen_origen"),
                col("had.id_articulo"),
                col("had.fecha_almacen"),
                col("cant_cajafisica") ,
                col("cant_cajafisica_ingresada"),
                col("cant_cajafisica_salida"),
                col("cant_cajafisica_total") ,
                col("cant_cajafisica_ingresada_total") ,
                col("cant_cajafisica_salida_total") ,
                col("cant_unidades") ,
                col("cant_unidades_total") ,
                col("cant_unidades_total_ingresada"),
                col("cant_unidades_total_salida") ,
                col("cant_unidades_transito"),
                col("imp_total_transito"),
                col("precio_unitario_mn"),
                col("precio_unitario_me"),
                col("imp_valorizado_mn"),
                col("imp_valorizado_me"),
                col("imp_saldo_inicial"),
                col("imp_saldo_final"),
                col("imp_valorizado_ingreso"),
                col("imp_valorizado_salida")
            )
        )

        logger.info("Starting creation of calendar logic")
        earliest_date = t_historico_inventario.selectExpr("min(fecha_inventario)").collect()[0][0]
        logger.info(f"Earliest date: {earliest_date}; Yesterday date: {yesterday_date}")

        datediff_value = (yesterday_date - earliest_date).days
        logger.info(f"Calculated datediff_value: {datediff_value}")

        df_calendar = (
            spark_controller.spark.range(0, datediff_value + 1)
            .select(
                expr(f"date_add('{earliest_date}', CAST(id AS INT))").alias("gen_date")
            )
            #.filter(col("gen_date") <= lit(yesterday_date))  # just in case
            .orderBy("gen_date")
        )

        tmp_calendario_mov_previo = (
            df_calendar.alias("c")
            .join(t_historico_inventario.alias("th"), 
                    (
                        (col("c.gen_date") >= col("th.fecha_inventario")) 
                        & (col("c.gen_date") <= col("th.fecha_inventario_fin"))
                    ), "left")
            .join(t_historico_inventario_movimiento_previo.alias("th2"), ((col("th.id_almacen") == col("th2.id_almacen_origen"))), "left")
            .where(
                (col("th.id_compania").isNotNull()) & (col("th2.id_articulo").isNotNull())
            )
            .select(
                col("th2.id_pais"),
                col("th.id_compania"),
                col("th.id_sucursal"),
                col("th.id_almacen"),
                col("th2.id_articulo"),
                col("c.gen_date").alias("fecha_almacens")
            )
            .distinct()
        )
        tmp_calendario_inicial = (
            df_calendar.alias("c")
            .join(t_historico_inventario.alias("th"), ((col("c.gen_date") >= col("th.fecha_inventario")) & (col("c.gen_date") <= col("th.fecha_inventario_fin"))), "left")
            .join(t_historico_inventario_inicial.alias("th2"), ((col("th.id_almacen") == col("th2.id_almacen"))), "left")
            .where(
                (col("th.id_compania").isNotNull()) & (col("th2.id_articulo").isNotNull())
            )
            .select(
                col("th2.id_pais"),
                col("th.id_compania"),
                col("th.id_sucursal"),
                col("th.id_almacen"),
                col("th2.id_articulo"),
                col("c.gen_date").alias("fecha_almacens")
            )
            .distinct()
        )
        union_fecha = (
            tmp_calendario_mov_previo
            .unionByName(tmp_calendario_inicial)
        )
        tmp_calendario_total = (
            union_fecha
            .select(
                col("id_pais"),
                col("id_compania"),
                col("id_sucursal"),
                col("id_almacen"),
                col("id_articulo"),
                col("fecha_almacens")
            )
            .orderBy("id_compania", "id_sucursal", "id_almacen", "id_articulo", "fecha_almacens")
            .distinct()
        )

        t_historico_inventario_movimiento = (
            tmp_calendario_total.alias("m")
            .join(t_historico_inventario_movimiento_previo.alias("t"), 
                    (
                        (col("m.id_almacen") == col("t.id_almacen_origen")) 
                        & (col("m.id_articulo") == col("t.id_articulo")) 
                        & (col("m.fecha_almacens") == col("t.fecha_almacen"))
                    ), "left")
            .select(
                col("m.id_pais"),
                col("m.id_compania"),
                col("m.id_sucursal"),
                col("m.id_almacen"),
                col("m.fecha_almacens"),
                col("m.id_articulo"),
                coalesce(col("t.cant_cajafisica"), lit(0)).alias("cant_cajafisica"),
                coalesce(col("t.cant_cajafisica_ingresada"), lit(0)).alias("cant_cajafisica_ingresada"),
                coalesce(col("t.cant_cajafisica_salida"), lit(0)).alias("cant_cajafisica_salida"),
                coalesce(col("t.cant_cajafisica_total"), lit(0)).alias("cant_cajafisica_total") ,
                coalesce(col("t.cant_cajafisica_ingresada_total"), lit(0)).alias("cant_cajafisica_ingresada_total"),
                coalesce(col("t.cant_cajafisica_salida_total"), lit(0)).alias("cant_cajafisica_salida_total"),
                coalesce(col("t.cant_unidades"), lit(0)).alias("cant_unidades"),
                coalesce(col("t.cant_unidades_total"), lit(0)).alias("cant_unidades_total"),
                coalesce(col("t.cant_unidades_total_ingresada"), lit(0)).alias("cant_unidades_total_ingresada"),
                coalesce(col("t.cant_unidades_total_salida"), lit(0)).alias("cant_unidades_total_salida"),
                coalesce(col("t.cant_unidades_transito"), lit(0)).alias("cant_unidades_transito"),
                coalesce(col("t.imp_total_transito"), lit(0)).alias("imp_total_transito"),
                coalesce(col("t.precio_unitario_mn"), lit(0)).alias("precio_unitario_mn"),
                coalesce(col("t.precio_unitario_me"), lit(0)).alias("precio_unitario_me"),
                coalesce(col("t.imp_valorizado_mn"), lit(0)).alias("imp_valorizado_mn"),
                coalesce(col("t.imp_valorizado_me"), lit(0)).alias("imp_valorizado_me"),
                coalesce(col("t.imp_saldo_inicial"), lit(0)).alias("imp_saldo_inicial"),
                coalesce(col("t.imp_saldo_final"), lit(0)).alias("imp_saldo_final"),
                coalesce(col("t.imp_valorizado_ingreso"), lit(0)).alias("imp_valorizado_ingreso"),
                coalesce(col("t.imp_valorizado_salida"), lit(0)).alias("imp_valorizado_salida")
            )
            .orderBy("m.id_compania", "m.id_sucursal", "m.id_almacen", "m.fecha_almacens")
        )

        window_spec = (
            Window.partitionBy("id_almacen", "id_articulo")
            .orderBy("fecha_almacens")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        rownum_spec = Window.partitionBy("id_almacen", "id_articulo").orderBy("fecha_almacens")

        df_joined = (
            t_historico_inventario_movimiento.alias("ha")
            .join(m_articulo.alias("ma"),col("ha.id_articulo") == col("ma.id_articulo"), "inner")
            .select(
                col("ha.id_pais").alias("id_pais"),
                col("ha.id_compania").alias("id_compania"),
                col("ha.id_sucursal").alias("id_sucursal"),
                col("ha.id_almacen").alias("id_almacen"),
                col("ha.id_articulo").alias("id_articulo"),
                col("ma.cod_unidad_manejo").alias("cod_unidad_manejo"),
                col("ha.fecha_almacens").alias("fecha_almacens"),
                col("ha.cant_cajafisica_total").alias("cant_cajafisica_total"),
                col("ha.cant_cajafisica_ingresada_total").alias("cant_cajafisica_ingresada_total"),
                col("ha.cant_cajafisica_salida_total").alias("cant_cajafisica_salida_total"),
                col("ha.cant_unidades_total").alias("cant_unidades_total"),
                col("ha.cant_unidades_total_ingresada").alias("cant_unidades_total_ingresada"),
                col("ha.cant_unidades_total_salida").alias("cant_unidades_total_salida"),
                col("ha.cant_unidades_transito").alias("cant_unidades_transito"),
                col("ha.imp_total_transito").alias("imp_total_transito"),
                col("ha.precio_unitario_mn").alias("precio_unitario_mn"),
                col("ha.precio_unitario_me").alias("precio_unitario_me"),
                col("ha.imp_valorizado_mn").alias("imp_valorizado_mn"),
                col("ha.imp_valorizado_me").alias("imp_valorizado_me"),
                col("ha.imp_saldo_inicial").alias("imp_saldo_inicial"),
                col("ha.imp_saldo_final").alias("imp_saldo_final"),
                col("ha.imp_valorizado_ingreso").alias("imp_valorizado_ingreso"),
                col("ha.imp_valorizado_salida").alias("imp_valorizado_salida")
            )
        )

        t_historico_inventario_movimiento_acc = (
            df_joined
            .withColumn("orden", row_number().over(rownum_spec))
            .withColumn("cant_cajafisica_acumulado", sum(col("cant_cajafisica_total")).over(window_spec))
            .withColumn("cant_unidades_acumulado", sum(col("cant_unidades_total")).over(window_spec))
            .withColumn("cant_unidades_transito_acumulado", sum(col("cant_unidades_transito")).over(window_spec))
            .withColumn("imp_total_transito_acumulado", sum(col("imp_total_transito")).over(window_spec))
            .withColumn("imp_valorizado_mn_acumulado", sum(col("imp_valorizado_mn")).over(window_spec))
            .withColumn("imp_valorizado_me_acumulado", sum(col("imp_valorizado_me")).over(window_spec))
        )

        df_new_candidates = t_historico_inventario_movimiento_acc.alias("ha").filter(col("orden") == lit(1))
        
        df_new_rows = (
            df_new_candidates.alias("mov")
            .join(t_historico_inventario_inicial.alias("thii"), 
                    (
                        (col("mov.id_almacen") == col("thii.id_almacen")) 
                        & (col("mov.id_articulo") == col("thii.id_articulo"))
                    ), "left_anti")
            .select(
                col("mov.id_pais"),
                col("mov.id_compania"),
                col("mov.id_sucursal"),
                col("mov.id_almacen"),
                col("mov.id_articulo"),
                col("mov.fecha_almacens"),
                col("mov.cant_cajafisica_total").alias("cant_cajafisica"),
                col("mov.cant_unidades_total").alias("cant_unidades"),
                col("mov.imp_total_transito"),
                col("mov.cant_unidades_transito"),
                col("mov.precio_unitario_mn"),
                col("mov.precio_unitario_me"),
                col("mov.imp_valorizado_mn"),
                col("mov.imp_valorizado_me"),
                col("mov.imp_saldo_inicial"),
                col("mov.imp_saldo_final"),
                col("mov.imp_valorizado_ingreso"),
                col("mov.imp_valorizado_salida")
            )
        )
        t_historico_inventario_inicial = (
            t_historico_inventario_inicial.unionByName(df_new_rows)
            .select(
                col("id_pais"),
                col("id_compania"),
                col("id_sucursal"),
                col("id_almacen"),
                col("id_articulo"),
                col("fecha_almacens"),
                col("cant_cajafisica"),
                col("cant_unidades"),
                col("imp_total_transito"),
                col("cant_unidades_transito"),
                col("precio_unitario_mn"),
                col("precio_unitario_me"),
                col("imp_valorizado_mn"),
                col("imp_valorizado_me"),
                col("imp_saldo_inicial"),
                col("imp_saldo_final"),
                col("imp_valorizado_ingreso"),
                col("imp_valorizado_salida")
            )
        )

        t_historico_inventario_saldo = (
            t_historico_inventario_inicial.alias("hii")
            .join(t_historico_inventario_movimiento_acc.alias("hiiacc"), ((col("hii.id_almacen") == col("hiiacc.id_almacen")) & (col("hii.id_articulo") == col("hiiacc.id_articulo"))), "inner")
            .select(
                col("hii.id_pais").alias("id_pais"),
                col("hiiacc.id_compania").alias("id_compania"),
                col("hiiacc.id_sucursal").alias("id_sucursal"),
                col("hiiacc.id_almacen").alias("id_almacen"),
                col("hiiacc.id_articulo").alias("id_articulo"),
                col("hiiacc.cod_unidad_manejo").alias("cod_unidad_manejo"),
                col("hiiacc.fecha_almacens").alias("fecha_almacens"),

                # For partition + lag logic:
                (col("hii.cant_cajafisica") + col("hiiacc.cant_cajafisica_acumulado")).alias("lag_source_cajafisica"),
                (col("hii.cant_unidades") + col("hiiacc.cant_unidades_acumulado")).alias("lag_source_unidades"),
                # Keep original columns for default values:
                col("hii.cant_cajafisica").alias("base_cajafisica"),
                col("hii.cant_unidades").alias("base_unidades"),

                col("hiiacc.cant_cajafisica_ingresada_total").alias("cant_cajafisica_ingresada_total"),
                col("hiiacc.cant_cajafisica_salida_total").alias("cant_cajafisica_salida_total"),
                col("hiiacc.cant_cajafisica_acumulado").alias("cant_cajafisica_movimiento"),
                col("hiiacc.cant_unidades_total_ingresada").alias("cant_unidades_total_ingresada"),
                col("hiiacc.cant_unidades_total_salida").alias("cant_unidades_total_salida"),
                col("hiiacc.cant_unidades_acumulado").alias("cant_unidades_movimiento"),
                col("hiiacc.imp_total_transito_acumulado").alias("imp_total_transito_movimiento"),
                col("hiiacc.cant_unidades_transito_acumulado").alias("unidades_transito_movimiento"),
                (col("hii.cant_cajafisica") + col("hiiacc.cant_unidades_acumulado")).alias("cant_unidades_saldo"),

                col("hiiacc.precio_unitario_mn").alias("precio_unitario_mn"),
                col("hiiacc.precio_unitario_me").alias("precio_unitario_me"),

                (col("hii.imp_valorizado_mn") + col("hiiacc.imp_valorizado_mn_acumulado")).alias("imp_valorizado_mn"),
                (col("hii.imp_valorizado_me") + col("hiiacc.imp_valorizado_me_acumulado")).alias("imp_valorizado_me"),
                col("hiiacc.imp_saldo_inicial").alias("imp_saldo_inicial"),
                col("hiiacc.imp_saldo_final").alias("imp_saldo_final"),
                col("hiiacc.imp_valorizado_ingreso").alias("imp_valorizado_ingreso"),
                col("hiiacc.imp_valorizado_salida").alias("imp_valorizado_salida")  
            )
        )
        w_cajafisica = (
            Window.partitionBy("id_almacen", "id_articulo")
                .orderBy("fecha_almacens")
        )
        w_unidades = (
            Window.partitionBy("id_compania", "id_sucursal", "id_almacen", "id_articulo") # es necesario utilizar id_compania e id_sucursal? Esta info la trae id_almacen 🤔
                .orderBy("fecha_almacens")
        )

        t_historico_inventario_saldo = (
            t_historico_inventario_saldo
            .withColumn(
                "cant_cajafisica_inicial",
                coalesce(
                    lag("lag_source_cajafisica", 1).over(w_cajafisica),
                    col("base_cajafisica")
                )
            )
            .withColumn(
                "unidades_ini",
                coalesce(
                    lag("lag_source_unidades", 1).over(w_unidades),
                    col("base_unidades")
                )
            )
        )

        temp_movimientos_inventario_sc_1 = (
            t_historico_inventario_saldo.alias("a")
            .join(m_articulo.alias("b"), col("a.id_articulo") == col("b.id_articulo"), "left")
            .select(
                col("a.id_pais"),
                col("a.id_compania"),
                col("a.id_sucursal"),
                col("a.id_almacen"),
                col("a.id_articulo"),
                date_format(col("a.fecha_almacens"), "yyyyMM").alias("id_periodo"),
                col("a.fecha_almacens").alias("fecha_fin_movimiento"),

                col("a.cod_unidad_manejo"),
                col("a.unidades_ini").alias("cant_unidades_inicial"),
                col("a.cant_unidades_total_ingresada").alias("cant_unidades_ingreso"),
                col("a.cant_unidades_total_salida").alias("cant_unidades_salida"),
                col("a.cant_unidades_saldo"),
                when(col("b.cant_unidad_paquete") == 0, 0)
                .otherwise(col("a.cant_unidades_saldo") / col("b.cant_unidad_paquete"))
                .alias("cant_cajas_saldo"),
                (col("a.cant_unidades_saldo") * col("b.cant_unidad_volumen")).alias("cant_unidades_volumen"),
                col("a.imp_total_transito_movimiento").alias("valorizado_transito_mn"),
                when((col("a.precio_unitario_mn") == 0) | (col("a.precio_unitario_me") == 0), 0)
                .otherwise(col("a.imp_total_transito_movimiento") / (col("a.precio_unitario_mn") / col("a.precio_unitario_me")))
                .alias("valorizado_transito_me"),
                col("a.unidades_transito_movimiento").alias("cant_transito_a_recepcionar"),
                col("a.cant_unidades_movimiento").alias("cant_unidades"),
                
                col("a.precio_unitario_mn"),
                col("a.precio_unitario_me"),
                (col("a.cant_unidades_saldo") * col("a.precio_unitario_mn")).alias("imp_valorizado_mn"),
                (col("a.cant_unidades_saldo") * col("a.precio_unitario_me")).alias("imp_valorizado_me"),
                col("a.imp_saldo_inicial"),
                col("a.imp_saldo_final"),
                col("a.imp_valorizado_ingreso"),
                col("a.imp_valorizado_salida")
            )
        )

        tmp_movimientos_inventario_sc = (
            temp_movimientos_inventario_sc_1.alias("b")
            .join(t_historico_inventario.alias("tf"), col("b.id_almacen") == col("tf.id_almacen"), "left")
            .select(
                col("b.id_pais"),
                # col("b.id_compania"),
                col("b.id_sucursal"),
                col("b.id_almacen").alias("id_almacen"),
                col("b.id_articulo"),
                col("b.id_periodo"),
                date_add(col("tf.fecha_inventario"), 1).alias("fecha_ini_movimiento"),
                col("b.fecha_fin_movimiento"),      
                col("b.cod_unidad_manejo"),
                col("b.cant_unidades_inicial"),
                col("b.cant_unidades_ingreso"),
                col("b.cant_unidades_salida"),
                col("b.cant_unidades_saldo"),
                col("b.cant_cajas_saldo"),
                col("b.valorizado_transito_mn").alias("imp_transito_mn"),
                col("b.valorizado_transito_me").alias("imp_transito_me"),
                col("b.cant_transito_a_recepcionar").alias("cant_unidades_transito"),
                col("b.cant_unidades"),
                col("b.cant_unidades_volumen"),      
                col("b.precio_unitario_mn"),
                col("b.precio_unitario_me"),
                col("b.imp_valorizado_mn"),
                col("b.imp_valorizado_me"),
                col("b.imp_saldo_inicial"),
                col("b.imp_saldo_final"),
                col("b.imp_valorizado_ingreso"),
                col("b.imp_valorizado_salida"),
                col("b.precio_unitario_mn").alias("imp_costo_promedio_mensual")
            )
        )
        # logger.info(f"tmp_movimientos_inventario_sc count: {tmp_movimientos_inventario_sc.count()}")
        # logger.info(f"tmp_movimientos_inventario_sc")
        # tmp_movimientos_inventario_sc.show(1)
        # tmp_movimientos_inventario_sc.repartition(1).write.format("csv").option("header", True).mode("overwrite").save(f"{S3_PATH_ANALYTICS}/tmp/{start_date}.csv")
        # logger.info(f"tmp_movimientos_inventario_sc saved in {S3_PATH_ANALYTICS}/tmp/{start_date}.csv")

        tmp = tmp_movimientos_inventario_sc \
            .where(col("id_periodo").isin(periodos)) \
            .select(
                col("id_pais").cast("string").alias("id_pais"),
                col("id_periodo").cast("string").alias("id_periodo"),
                col("id_sucursal").cast("string").alias("id_sucursal"),
                col("id_almacen").cast("string").alias("id_almacen"),
                col("id_articulo").cast("string").alias("id_articulo"),
                col("fecha_ini_movimiento").cast("date").alias("fecha_ini_movimiento"),
                col("fecha_fin_movimiento").cast("date").alias("fecha_fin_movimiento"),
                col("cod_unidad_manejo").cast("string").alias("cod_unidad_manejo"),
                col("precio_unitario_mn").cast("numeric(38, 12)").alias("precio_unitario_mn"),
                col("precio_unitario_me").cast("numeric(38, 12)").alias("precio_unitario_me"),
                col("cant_unidades_inicial").cast("numeric(38, 12)").alias("cant_unidades_inicial"),
                col("cant_unidades_ingreso").cast("numeric(38, 12)").alias("cant_unidades_ingreso"),
                col("cant_unidades_salida").cast("numeric(38, 12)").alias("cant_unidades_salida"),
                col("cant_unidades_saldo").cast("numeric(38, 12)").alias("cant_unidades_saldo"),
                col("cant_cajas_saldo").cast("numeric(38, 12)").alias("cant_cajas_saldo"),
                col("imp_valorizado_mn").cast("numeric(38, 12)").alias("imp_valorizado_mn"),
                col("imp_valorizado_me").cast("numeric(38, 12)").alias("imp_valorizado_me"),
                col("cant_unidades").cast("numeric(38, 12)").alias("cant_unidades"),
                col("cant_unidades_volumen").cast("numeric(38, 12)").alias("cant_unidades_volumen"),
                col("imp_saldo_inicial").cast("numeric(38, 12)").alias("imp_saldo_inicial"),
                col("imp_saldo_final").cast("numeric(38, 12)").alias("imp_saldo_final"),
                col("imp_valorizado_ingreso").cast("numeric(38, 12)").alias("imp_valorizado_ingreso"),
                col("imp_valorizado_salida").cast("numeric(38, 12)").alias("imp_valorizado_salida"),
                col("imp_costo_promedio_mensual").cast("numeric(38, 12)").alias("imp_costo_promedio_mensual"),
                col("cant_unidades_transito").cast("numeric(38, 12)").alias("cant_unidades_transito"),
                col("imp_transito_mn").cast("numeric(38, 12)").alias("imp_transito_mn"),
                col("imp_transito_me").cast("numeric(38, 12)").alias("imp_transito_me"),
            )
        # logger.info(f"tmp")
        # tmp.show(1)
        # logger.info(f"tmp count: {tmp.count()}")
        partition_columns_array = ["id_pais", "id_periodo"]
        spark_controller.write_table(tmp, data_paths.CADENA, target_table_name, partition_by=partition_columns_array)
        logger.info(f"Table {target_table_name} written successfully")

        # Increment start_date to the next month
        next_month = start_date.month % 12 + 1
        next_year = start_date.year + (start_date.month // 12)
        start_date = start_date.replace(year=next_year, month=next_month, day=1)
        last_day_of_month = calendar.monthrange(start_date.year, start_date.month)[1]
        start_date = start_date.replace(day=last_day_of_month)
        yesterday_date = start_date
        logger.info(f"Next start_date: {start_date} | Yesterday date: {yesterday_date}")



except Exception as e:
    logger.error(e)
    raise
