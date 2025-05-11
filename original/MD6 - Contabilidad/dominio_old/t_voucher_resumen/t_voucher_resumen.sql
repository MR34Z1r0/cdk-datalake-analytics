drop table if exists tmp_t_voucher_resumen_1;
create temp table tmp_t_voucher_resumen_1 as (
    select
        p.desc_abreviada cod_pais,
        a.cod_compania,
        cod_sucursal,
        cod_ejercicio::varchar(4) as cod_ejercicio,
        lpad(cod_periodo::varchar(2), 2, '0')::varchar(6) as cod_periodo,
        cod_cuenta_contable,
        cod_centro_costo,
        --id_periodo_contable --formed by cod_compania, cod_ejercicio, cod_periodo
        --id_sucursal --formed by cod_compania, cod_sucursal
        --id_cuenta_contable --formed by cod_compania, cod_cuenta_contable
        --id_centro_costo --formed by cod_compania, cod_centro_costo
        debe as debe_mn, 
        debeme as debe_me,
        haber as haber_mn,
        haberme as haber_me
        --cod_tipo_gasto_cds --from external table: aje_cadena_global_sccc_configuracion_cds
        --id_tipo_gasto_cds --from external table: dim_tipo_gasto_cds
        --cod_clasificacion_pl --from external table: aje_cadena_global_sccc_configuracion_ebitda_centro_costo_plan_cuenta
        --id_clasificacion_pl 
    from big_magic_incoming_${instance}.t_voucher_resumen a 
    inner join big_magic_incoming_${instance}.m_compania c on a.cod_compania = c.cod_compania 
    inner join big_magic_incoming_${instance}.m_pais p on c.cod_pais = p.cod_pais 
    where cod_ejercicio::varchar(4) || lpad(cod_periodo::varchar(2), 2, '0')::varchar(6) in ${periodos}
    and p.desc_abreviada in ${cod_pais}
);

drop table if exists tmp_t_voucher_resumen_2;
create temp table tmp_t_voucher_resumen_2 as (
    select
        tv1.cod_pais as id_pais,
        tv1.cod_compania as id_compania,
        tv1.cod_compania || '|' || tv1.cod_sucursal as id_sucursal,
        tv1.cod_ejercicio || tv1.cod_periodo as id_periodo,
        tv1.cod_compania || '|' || tv1.cod_ejercicio || '|' || tv1.cod_periodo as id_periodo_contable,
        tv1.cod_compania || '|' || tv1.cod_ejercicio || '|' || tv1.cod_cuenta_contable as id_cuenta_contable,
        tv1.cod_compania || '|' || tv1.cod_ejercicio || '|' || tv1.cod_centro_costo as id_centro_costo,
        tv1.debe_mn,
        tv1.debe_me,
        tv1.haber_mn,
        tv1.haber_me,
        tv1.cod_pais,
        tv1.cod_compania,
        tv1.cod_sucursal,
        tv1.cod_cuenta_contable,
        tv1.cod_centro_costo
    from tmp_t_voucher_resumen_1 tv1
);

drop table if exists tmp_t_voucher_resumen_3;
create temp table tmp_t_voucher_resumen_3 as (
    select
        tv2.id_pais,
        tv2.id_compania,
        tv2.id_sucursal,
        tv2.id_periodo,
        tv2.id_periodo_contable,
        tv2.id_cuenta_contable,
        tv2.id_centro_costo,
        coalesce(cfcds.cod_tipo_gasto_cds, '00') as cod_tipo_gasto_cds,
        tv2.cod_pais || '|' || coalesce(cfcds.cod_tipo_gasto_cds, '00') as id_tipo_gasto_cds,
        coalesce(cfpl.cod_clasificacion_pl, '0') as cod_clasificacion_pl,
        tv2.cod_pais || '|' || ISNULL(cfpl.cod_clasificacion_pl, '0') as id_clasificacion_pl,
        tv2.debe_mn,
        tv2.debe_me,
        tv2.haber_mn,
        tv2.haber_me
        from tmp_t_voucher_resumen_2 tv2
        left join external_data_raw.aje_cadena_global_sccc_configuracion_cds cfcds on tv2.id_periodo between cfcds.periodo_inicio::varchar(6) and cfcds.periodo_fin::varchar(6)
            and tv2.cod_compania = lpad(cfcds.cod_compania::varchar(4), 4, '0')::varchar(4)
            and tv2.cod_sucursal = lpad(cfcds.cod_sucursal::varchar(2), 2, '0')::varchar(2)
            and tv2.cod_centro_costo = cfcds.cod_centro_costo
            and tv2.cod_cuenta_contable = cfcds.cod_cuenta_contable
        left join external_data_raw.aje_cadena_global_sccc_configuracion_ebitda_centro_costo_plan_cuenta cfpl on tv2.id_pais = cfpl.cod_pais
            and tv2.cod_compania = lpad(cfpl.cod_compania::varchar(4), 4, '0')::varchar(4)
            and tv2.cod_cuenta_contable = cfpl.cod_cuenta_contable
            and tv2.cod_centro_costo = cfpl.cod_centro_costo
            and tv2.id_periodo >= cfpl.periodo_inicio::varchar(6)
            and tv2.id_periodo <= cfpl.periodo_fin::varchar(6)
);

delete from dominio_dev.t_voucher_resumen where id_periodo in ${periodos} and id_pais in ${cod_pais};

insert into dominio_dev.t_voucher_resumen(
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
    debe_mn,
    debe_me,
    haber_mn,
    haber_me,
    cod_tipo_gasto_cds,
    id_tipo_gasto_cds,
    cod_clasificacion_pl,
    id_clasificacion_pl
from tmp_t_voucher_resumen_3;
