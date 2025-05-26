CREATE
OR REPLACE VIEW "comercial_analytics_prod"."vw_fact_venta_detalle" AS
SELECT
    tv.id_pais,
    tv.id_periodo,
    tv.id_sucursal,
    tv.id_cliente,
    tv.id_producto,
    tv.id_vendedor,
    tv.id_supervisor,
    tv.id_forma_pago,
    tv.id_fuerza_venta,
    tv.id_modelo_atencion,
    tv.id_lista_precio,
    tv.id_origen_pedido,
    tv.id_tipo_venta,
    tv.id_documento_venta,
    tv.id_documento_pedido,
    tv.fecha_emision,
    tv.fecha_liquidacion,
    tv.fecha_pedido,
    tv.nro_venta,
    tv.nro_pedido,
    NULL:: character varying:: character varying(20) AS fuente,
    tv.desc_region,
    tv.desc_subregion,
    tv.desc_division,
    tv.cod_zona,
    tv.cod_ruta,
    tv.cod_modulo,
    tv.cant_cajafisica_vta,
    tv.cant_cajaunitaria_vta,
    tv.cant_cajafisica_pro,
    tv.cant_cajaunitaria_pro,
    tv.imp_neto_vta_mn,
    tv.imp_neto_vta_me,
    tv.imp_bruto_vta_mn,
    tv.imp_bruto_vta_me,
    tv.imp_dscto_mn,
    tv.imp_dscto_me,
    tv.imp_dscto_sinimpvta_mn,
    tv.imp_dscto_sinimpvta_me,
    tv.imp_cobrar_vta_mn,
    tv.imp_cobrar_vta_me,
    tv.imp_paquete_vta_mn,
    tv.imp_paquete_vta_me,
    tv.imp_sugerido_mn,
    tv.imp_sugerido_me,
    tv.imp_full_vta_mn,
    tv.imp_full_vta_me,
    tv.imp_valorizado_pro_mn,
    tv.imp_valorizado_pro_me,
    tv.imp_impuesto1_mn,
    tv.imp_impuesto1_me,
    tv.imp_impuesto2_mn,
    tv.imp_impuesto2_me,
    tv.imp_impuesto3_mn,
    tv.imp_impuesto3_me,
    tv.imp_impuesto4_mn,
    tv.imp_impuesto4_me,
    tv.imp_impuesto5_mn,
    tv.imp_impuesto5_me,
    tv.imp_impuesto6_mn,
    tv.imp_impuesto6_me
FROM
    comercial_analytics_prod.fact_venta_detalle tv
WHERE
    CASE
    WHEN pgdate_part(
        'day':: character varying:: text,
        'now':: character varying:: date:: timestamp without time zone
    ) <= 5:: double precision THEN date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date:: timestamp without time zone
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '1 mon':: interval
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '2 mons':: interval
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '1 year':: interval
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '1 year 1 mon':: interval
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '2 years':: interval
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '2 years 1 mon':: interval
    )
    ELSE date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date:: timestamp without time zone
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '1 mon':: interval
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '2 mons':: interval
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '1 year':: interval
    )
    OR date_trunc(
        'month':: character varying:: text,
        tv.fecha_liquidacion:: timestamp without time zone
    ) = date_trunc(
        'month':: character varying:: text,
        'now':: character varying:: date - '2 years':: interval
    ) END;