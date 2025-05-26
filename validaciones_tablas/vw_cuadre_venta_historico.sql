CREATE
OR REPLACE VIEW "comercial_analytics_prod"."vw_cuadre_venta_historico" AS
SELECT
   tv.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   ma.cod_producto,
   tv.id_periodo,
   sum(tv.cant_cajafisica_vta) AS cant_caja_fisica_ven,
   sum(tv.cant_cajafisica_pro) AS cant_cajafisica_pro,
   sum(tv.cant_cajafisica_vta) + sum(tv.cant_cajafisica_pro) AS cant_caja_fisica,
   sum(tv.cant_cajaunitaria_vta):: double precision / 30:: numeric:: numeric(18, 0):: double precision AS cant_caja_unitaria_ven,
   sum(tv.cant_cajaunitaria_pro):: double precision / 30:: numeric:: numeric(18, 0):: double precision AS cant_caja_unitaria_pro,
   (
      sum(tv.cant_cajaunitaria_vta) + sum(tv.cant_cajaunitaria_pro)
   ):: double precision / 30:: numeric:: numeric(18, 0):: double precision AS cant_caja_unitaria,
   sum(tv.imp_neto_vta_mn) AS imp_neto_vta_mn,
   sum(tv.imp_neto_vta_me) AS imp_neto_vta_me,
   sum(tv.imp_bruto_vta_mn) AS imp_bruto_vta_mn,
   sum(tv.imp_bruto_vta_me) AS imp_bruto_vta_me
FROM
   comercial_analytics_prod.fact_venta_cliente_historico tv
   JOIN comercial_analytics_prod.dim_cliente mc ON tv.id_cliente:: text = mc.id_cliente:: text
   JOIN comercial_analytics_prod.dim_producto ma ON tv.id_producto:: text = ma.id_producto:: text
   JOIN comercial_analytics_prod.dim_sucursal ms ON tv.id_sucursal:: text = ms.id_sucursal:: text
WHERE
   mc.cod_tipo_cliente:: text = 'N':: character varying:: text
   OR mc.cod_tipo_cliente:: text = 'T':: character varying:: text
GROUP BY
   tv.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   ma.cod_producto,
   tv.id_periodo;