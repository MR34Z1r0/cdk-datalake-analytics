CREATE
OR REPLACE VIEW "comercial_analytics_prod"."vw_cuadre_venta_manejantes" AS
SELECT
    fvra.id_pais,
    ms.cod_compania,
    ms.cod_sucursal,
    ma.cod_producto,
    fvra.id_periodo,
    sum(fvra.imp_neto_vta_mn) AS imp_neto_vta_mn,
    sum(fvra.imp_neto_vta_me) AS imp_neto_vta_me,
    sum(fvra.imp_bruto_vta_mn) AS imp_bruto_vta_mn,
    sum(fvra.imp_bruto_vta_me) AS imp_bruto_vta_me,
    sum(fvra.cant_cajafisica_vta) AS cant_cajafisica_vta,
    sum(fvra.cant_cajaunitaria_vta) AS cant_cajaunitaria_vta,
    sum(
        CASE
        WHEN fvra.m0:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m0_manejante,
    sum(
        CASE
        WHEN fvra.m1:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m1_manejante,
    sum(
        CASE
        WHEN fvra.m2:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m2_manejante,
    sum(
        CASE
        WHEN fvra.m3:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m3_manejante,
    sum(
        CASE
        WHEN fvra.m4:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m4_manejante,
    sum(
        CASE
        WHEN fvra.m5:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m5_manejante,
    sum(
        CASE
        WHEN fvra.m6:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m6_manejante,
    sum(
        CASE
        WHEN fvra.m7:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m7_manejante,
    sum(
        CASE
        WHEN fvra.m8:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m8_manejante,
    sum(
        CASE
        WHEN fvra.m9:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m9_manejante,
    sum(
        CASE
        WHEN fvra.m10:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m10_manejante,
    sum(
        CASE
        WHEN fvra.m11:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m11_manejante,
    sum(
        CASE
        WHEN fvra.m12:: text = 'Manejante':: character varying:: text THEN 1
        ELSE 0 END
    ) AS m12_manejante
FROM
    comercial_analytics_prod.fact_venta_manejantes fvra
    JOIN comercial_analytics_prod.dim_cliente mc ON fvra.id_cliente:: text = mc.id_cliente:: text
    JOIN comercial_analytics_prod.dim_producto ma ON fvra.id_producto:: text = ma.id_producto:: text
    JOIN comercial_analytics_prod.dim_sucursal ms ON mc.id_sucursal:: text = ms.id_sucursal:: text
WHERE
    (
        mc.cod_tipo_cliente:: text = 'N':: character varying:: text
        OR mc.cod_tipo_cliente:: text = 'T':: character varying:: text
    )
    AND to_char(
        fvra.fecha_liquidacion:: timestamp without time zone,
        'YYYY':: character varying:: text
    ) = '2024':: character varying:: text
    AND ma.es_activo:: text = 'A':: character varying:: text
GROUP BY
    fvra.id_pais,
    ms.cod_compania,
    ms.cod_sucursal,
    ma.cod_producto,
    fvra.id_periodo;