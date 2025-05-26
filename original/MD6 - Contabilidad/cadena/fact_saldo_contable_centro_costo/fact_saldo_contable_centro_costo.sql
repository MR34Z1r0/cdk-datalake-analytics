drop table if exists cadena_dev.tmp_fact_sado_contable_centro_costo;
create temp table tmp_fact_saldo_contable_centro_costo as
(
    select
        tvr.id_pais,
        tvr.id_compania,
        tvr.id_sucursal,
        tvr.id_periodo,
        tvr.id_periodo_contable,
        tvr.id_cuenta_contable,
        tvr.id_centro_costo,
        tvr.debe_mn,
        tvr.debe_me,
        tvr.haber_mn,
        tvr.haber_me,
        tvr.cod_tipo_gasto_cds,
        tvr.id_tipo_gasto_cds,
        tvr.cod_clasificacion_pl,
        tvr.id_clasificacion_pl
    from dominio_dev.t_voucher_resumen tvr
);

delete from cadena_dev.fact_saldo_contable_centro_costo where id_periodo in ${periodos} and id_pais in ${cod_pais};

insert into cadena_dev.fact_saldo_contable_centro_costo(
    id_pais,
    id_compania,
    id_sucursal,
    id_periodo,
    id_periodo_contable,
    id_cuenta_contable,
    id_centro_costo,
    debe_mn,
    debe_me,
    haber_mn,
    haber_me,
    cod_tipo_gasto_cds,
    id_tipo_gasto_cds,
    cod_clasificacion_pl,
    id_clasificacion_pl
)
select
    id_pais,
    id_compania,
    id_sucursal,
    id_periodo,
    id_periodo_contable,
    id_cuenta_contable,
    id_centro_costo,
    sum(debe_mn) as debe_mn,
    sum(debe_me) as debe_me,
    sum(haber_mn) as haber_mn,
    sum(haber_me) as haber_me,
    cod_tipo_gasto_cds,
    id_tipo_gasto_cds,
    cod_clasificacion_pl,
    id_clasificacion_pl
from tmp_fact_saldo_contable_centro_costo
group by
    id_pais,
    id_compania,
    id_sucursal,
    id_periodo,
    id_periodo_contable,
    id_cuenta_contable,
    id_centro_costo,
    cod_tipo_gasto_cds,
    id_tipo_gasto_cds,
    cod_clasificacion_pl,
    id_clasificacion_pl;
