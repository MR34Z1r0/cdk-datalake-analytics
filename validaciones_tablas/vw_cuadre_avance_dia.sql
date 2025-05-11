CREATE
OR REPLACE VIEW "comercial_analytics_prod"."vw_cuadre_avance_dia" AS
SELECT
   fvra.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   to_char(
      fvra.fecha_avance_dia:: timestamp without time zone,
      'YYYYMM':: text
   ) AS id_periodo,
   fvra.modelo_atencion__c,
   sum(fvra.total_cajas_cliente_nuevo__c) AS total_cajas_cliente_nuevo__c,
   sum(fvra.total_cajas_extra_ruta__c) AS total_cajas_extra_ruta__c,
   sum(fvra.total_cajas_fisicas__c) AS total_cajas_fisicas__c,
   sum(fvra.total_cajas_prospectos__c) AS total_cajas_prospectos__c,
   sum(fvra.total_cajas_vendedor__c) AS total_cajas_vendedor__c,
   sum(fvra.total_clientes_activos__c) AS total_clientes_activos__c,
   sum(fvra.total_clientes_compra2__c) AS total_clientes_compra2__c,
   sum(fvra.total_clientes__c) AS total_clientes__c,
   sum(fvra.total_de_cajas__c) AS total_de_cajas__c,
   sum(fvra.total_de_caja_unidades__c) AS total_de_caja_unidades__c,
   sum(fvra.total_importe_cliente_nuevo__c) AS total_importe_cliente_nuevo__c,
   sum(fvra.total_importe__c) AS total_importe__c
FROM
   comercial_analytics_prod.fact_avance_dia_detalle fvra
   JOIN comercial_analytics_prod.dim_sucursal ms ON fvra.id_sucursal:: text = ms.id_sucursal:: text
WHERE
   to_char(
      fvra.fecha_avance_dia:: timestamp without time zone,
      'YYYY':: character varying:: text
   ) = '2024':: character varying:: text
GROUP BY
   fvra.id_pais,
   ms.cod_compania,
   ms.cod_sucursal,
   to_char(
      fvra.fecha_avance_dia:: timestamp without time zone,
      'YYYYMM':: text
   ),
   fvra.modelo_atencion__c;