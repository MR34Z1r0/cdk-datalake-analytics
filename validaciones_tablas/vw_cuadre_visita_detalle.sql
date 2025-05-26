CREATE
OR REPLACE VIEW "comercial_analytics_prod"."vw_cuadre_visita_detalle" AS
SELECT
   fvra.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   to_char(
      fvra.fecha_visita:: timestamp without time zone,
      'YYYYMM':: character varying:: text
   ) AS id_periodo,
   fvra.id_modelo_atencion,
   fvra.desc_zona,
   fvra.desc_ruta,
   count(DISTINCT fvra.id_visita) AS visitas,
   count(DISTINCT fvra.id_cliente) AS clientes,
   sum(fvra.es_activo) AS total_activo,
   sum(fvra.es_programado) AS total_programado,
   sum(fvra.es_pedido_sugerido) AS total_pedido_sugerido,
   sum(fvra.es_visita_auto) AS total_visita_auto,
   sum(fvra.es_check_out_automatico) AS total_check_out_automatico,
   sum(fvra.es_pedido_levantado) AS total_pedido_levantado
FROM
   comercial_analytics_prod.fact_visita_detalle fvra
   JOIN comercial_analytics_prod.dim_sucursal ms ON fvra.id_sucursal:: text = ms.id_sucursal:: text
WHERE
   to_char(
      fvra.fecha_visita:: timestamp without time zone,
      'YYYY':: character varying:: text
   ) = '2024':: character varying:: text
GROUP BY
   fvra.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   to_char(
      fvra.fecha_visita:: timestamp without time zone,
      'YYYYMM':: character varying:: text
   ),
   fvra.id_modelo_atencion,
   fvra.desc_zona,
   fvra.desc_ruta;