CREATE
OR REPLACE VIEW "comercial_analytics_prod"."vw_cuadre_cliente_venta" AS
SELECT
   fcv.id_pais,
   fcv.id_periodo,
   fcv.id_compania AS cod_compania,
   fcv.cod_modulo,
   to_char(
      fcv.ult_fecha_compra_cliente:: timestamp without time zone,
      'YYYYMMDD':: character varying:: text
   ) AS ult_fecha,
   to_char(
      fcv.ult_fecha_compra_12meses_nn:: timestamp without time zone,
      'YYYYMMDD':: character varying:: text
   ) AS ult_fecha_nn,
   sum(fcv.cant_caja_fisica_ven_3meses) AS cant_caja_fisica_ven_3meses,
   sum(fcv.cant_caja_fisica_ven_12meses) AS cant_caja_fisica_ven_12meses,
   sum(fcv.cant_caja_fisica_pro_3meses) AS cant_caja_fisica_pro_3meses,
   sum(fcv.cant_caja_fisica_pro_12meses) AS cant_caja_fisica_pro_12meses,
   sum(fcv.imp_neto_mn_3meses) AS imp_neto_mn_3meses,
   sum(fcv.imp_neto_mn_12meses) AS imp_neto_mn_12meses,
   sum(fcv.imp_neto_me_3meses) AS imp_neto_me_3meses,
   sum(fcv.imp_neto_me_12meses) AS imp_neto_me_12meses,
   sum(fcv.imp_bruto_mn_3meses) AS imp_bruto_mn_3meses,
   sum(fcv.imp_bruto_mn_12meses) AS imp_bruto_mn_12meses,
   sum(fcv.imp_bruto_me_3meses) AS imp_bruto_me_3meses,
   sum(fcv.imp_bruto_me_12meses) AS imp_bruto_me_12meses,
   sum(fcv.cant_producto) AS cant_producto,
   sum(fcv.cant_venta) AS cant_venta,
   sum(fcv.cant_marca) AS cant_marca,
   sum(fcv.cant_caja_unit_venta_12meses_nn) AS cant_caja_unit_venta_12meses_nn,
   sum(fcv.cant_caja_unit_venta_3meses_nn) AS cant_caja_unit_venta_3meses_nn,
   sum(fcv.imp_neto_mn_12meses_nn) AS imp_neto_mn_12meses_nn,
   sum(fcv.imp_neto_mn_3meses_nn) AS imp_neto_mn_3meses_nn,
   sum(fcv.cant_venta_nn) AS cant_venta_nn,
   sum(fcv.cant_marca_nn) AS cant_marca_nn
FROM
   comercial_analytics_prod.fact_cliente_venta fcv
GROUP BY
   fcv.id_pais,
   fcv.id_periodo,
   fcv.id_compania,
   fcv.cod_modulo,
   to_char(
      fcv.ult_fecha_compra_cliente:: timestamp without time zone,
      'YYYYMMDD':: character varying:: text
   ),
   to_char(
      fcv.ult_fecha_compra_12meses_nn:: timestamp without time zone,
      'YYYYMMDD':: character varying:: text
   );