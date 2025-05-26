CREATE
OR REPLACE VIEW "comercial_analytics_prod"."vw_cuadre_pedido_detalle" AS
SELECT
   fvra.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   ma.cod_producto,
   fvra.id_periodo,
   dop.desc_origen_pedido,
   dtp.desc_tipo_pedido,
   sum(fvra.cant_cajafisica_ped) AS cant_cajafisica_ped,
   sum(fvra.cant_cajavolumen_ped) AS cant_cajavolumen_ped,
   sum(fvra.cant_cajafisica_ped_pro) AS cant_cajafisica_ped_pro,
   sum(fvra.cant_cajavolumen_ped_pro) AS cant_cajavolumen_ped_pro,
   sum(fvra.cant_cajafisica_asignado_ped) AS cant_cajafisica_asignado_ped,
   sum(fvra.cant_cajavolumen_asignado_ped) AS cant_cajavolumen_asignado_ped,
   sum(fvra.cant_cajafisica_asignado_ped_pro) AS cant_cajafisica_asignado_ped_pro,
   sum(fvra.cant_cajavolumen_asignado_ped_pro) AS cant_cajavolumen_asignado_ped_pro,
   sum(fvra.imp_neto_ped_mn) AS imp_neto_ped_mn,
   sum(fvra.imp_bruto_ped_mn) AS imp_bruto_ped_mn
FROM
   comercial_analytics_prod.fact_pedido_detalle fvra
   JOIN comercial_analytics_prod.dim_cliente mc ON fvra.id_cliente:: text = mc.id_cliente:: text
   JOIN comercial_analytics_prod.dim_producto ma ON fvra.id_articulo:: text = ma.id_producto:: text
   JOIN comercial_analytics_prod.dim_sucursal ms ON fvra.id_sucursal:: text = ms.id_sucursal:: text
   JOIN comercial_analytics_prod.dim_origen_pedido dop ON fvra.id_origen_pedido:: text = dop.id_origen_pedido:: text
   JOIN comercial_analytics_prod.dim_tipo_pedido dtp ON fvra.id_tipo_pedido:: text = dtp.id_tipo_pedido:: text
WHERE
   (
      mc.cod_tipo_cliente:: text = 'N':: character varying:: text
      OR mc.cod_tipo_cliente:: text = 'T':: character varying:: text
   )
   AND to_char(
      fvra.fecha_pedido:: timestamp without time zone,
      'YYYY':: character varying:: text
   ) = '2024':: character varying:: text
GROUP BY
   fvra.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   ma.cod_producto,
   fvra.id_periodo,
   dop.desc_origen_pedido,
   dtp.desc_tipo_pedido;