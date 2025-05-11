CREATE
OR REPLACE VIEW "comercial_analytics_prod"."vw_cuadre_kpi_resumen" AS
SELECT
   fvra.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   ma.cod_producto,
   fvra.id_periodo,
   count(DISTINCT fvra.id_cliente) AS cant_clientes,
   count(DISTINCT fvra.id_cliente_visita) AS cant_clientes_visita,
   count(DISTINCT fvra.id_cliente_visita_pedido) AS cant_clientes_visita_pedido,
   count(DISTINCT fvra.id_cliente_visita_venta) AS cant_clientes_visita_venta,
   sum(fvra.cant_cajafisica_vta) AS cant_cajafisicavta,
   sum(fvra.cant_cajaunitaria_vta) AS cant_cajaunitvta,
   sum(fvra.imp_neto_vta_mn) AS imp_netovta,
   sum(fvra.imp_bruto_vta_mn) AS imp_cobrarvta
FROM
   comercial_analytics_prod.fact_kpi_detalle fvra
   JOIN comercial_analytics_prod.dim_cliente mc ON fvra.id_cliente:: text = mc.id_cliente:: text
   JOIN comercial_analytics_prod.dim_producto ma ON fvra.id_producto:: text = ma.id_producto:: text
   JOIN comercial_analytics_prod.dim_sucursal ms ON fvra.id_sucursal:: text = ms.id_sucursal:: text
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
   fvra.id_periodo;