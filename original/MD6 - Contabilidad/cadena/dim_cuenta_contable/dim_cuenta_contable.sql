drop table if exists tmp_dim_cuenta_contable;
create temp table tmp_dim_cuenta_contable as
(
    select
        mcc.id_pais, 
        mcc.id_compania, 
        mcc.id_ejercicio, 
        mcc.id_cuenta_contable,
        mcc.id_cuenta_contable_corp,
        mcc.cod_ejercicio, 
        mcc.cod_cuenta_contable, 
        mcc.desc_cuenta_contable, 
        mcc.cod_tipo_cuenta, 
        mcc.cod_tipo_moneda, 
        mcc.cod_naturaleza, 
        mcc.cod_indicador_balance, 
        mcc.cod_indicador_resultado, 
        mcc.tiene_centro_costo, 
        mcc.tiene_cuenta_auxiliar, 
        mcc.cod_tipo_cuenta_auxiliar, 
        mcc.tiene_ifrs,
        case when trim(mccorp.cod_cuenta_contable_corporativa) = '' then '0000' else trim(mccorp.cod_cuenta_contable_corporativa) end as cod_cuenta_contable_corporativa, 
        case when trim(mccorp.desc_cuenta_contable_corporativa) = '' then '[NO IDENTIFICADO]' else trim(mccorp.desc_cuenta_contable_corporativa) end as desc_cuenta_contable_corporativa,
        mcc.flg_tipres,
        length(trim(mcc.cod_cuenta_contable)) as longitud
    from dominio_dev.m_plan_cuentas mcc
    left join dominio_dev.m_plan_cuentas_corporativo mccorp on mcc.id_cuenta_contable_corp = mccorp.id_cuenta_contable_corp
    where mcc.id_pais in ${cod_pais}
);

drop table if exists tmp_dim_cuenta_contable_add_cuenta_contable_bp;
create temp table tmp_dim_cuenta_contable_add_cuenta_contable_bp as
(
    select
        mcc.id_pais, 
        mcc.id_compania, 
        mcc.id_ejercicio, 
        mcc.id_cuenta_contable,
        mcc.id_cuenta_contable_corp,
        mcc.cod_ejercicio, 
        mcc.cod_cuenta_contable, 
        mcc.desc_cuenta_contable, 
        mcc.cod_tipo_cuenta, 
        mcc.cod_tipo_moneda, 
        mcc.cod_naturaleza, 
        mcc.cod_indicador_balance, 
        mcc.cod_indicador_resultado, 
        mcc.tiene_centro_costo, 
        mcc.tiene_cuenta_auxiliar, 
        mcc.cod_tipo_cuenta_auxiliar, 
        mcc.tiene_ifrs,
        mcc.flg_tipres,
        mcc.cod_cuenta_contable_corporativa,
        mcc.desc_cuenta_contable_corporativa,
        mcc.longitud,
        coalesce(ebitdapc.COD_CUENTA_CONTABLE_BP, '00') as cod_cuenta_contable_bp,
        coalesce(ebitdapc.DESC_CUENTA_CONTABLE_BP, '[NO IDENTIFICADO]') as desc_cuenta_contable_bp,
        coalesce(ebitdapc.DESC_CUENTA_CONTABLE_BP_CORP, '[NO IDENTIFICADO]') as desc_cuenta_contable_bp_corp,
        case when ebitdapcnc.cod_cuenta_contable is null then 1 else 0 end as tiene_indicador_bp
    from tmp_dim_cuenta_contable mcc
    left join external_data_raw.aje_cadena_global_sccc_configuracion_ebitda_plan_cuenta ebitdapc on mcc.cod_cuenta_contable = ebitdapc.cod_cuenta_contable
        and ebitdapc.cod_pais = mcc.id_pais
        and mcc.cod_ejercicio <= left(ebitdapc.periodo_fin::varchar(6), 4)
        and mcc.cod_ejercicio >= left(ebitdapc.periodo_inicio::varchar(6), 4)
        and ebitdapc.cod_compania = mcc.id_compania
    left join external_data_raw.aje_cadena_global_sccc_configuracion_ebitda_plan_cuenta_no_considerada ebitdapcnc on mcc.cod_cuenta_contable = ebitdapcnc.cod_cuenta_contable
        and ebitdapcnc.cod_pais = mcc.id_pais
        and mcc.cod_ejercicio <= left(ebitdapcnc.periodo_fin::varchar(6), 4)
        and mcc.cod_ejercicio >= left(ebitdapcnc.periodo_inicio::varchar(6), 4)
        and ebitdapcnc.cod_compania = mcc.id_compania
);


drop table if exists tmp_dim_cuenta_contable_add_cuenta_clase;
create temp table tmp_dim_cuenta_contable_add_cuenta_clase as
(
    select
        mcc.id_pais, 
        mcc.id_compania, 
        mcc.id_ejercicio, 
        mcc.id_cuenta_contable,
        mcc.id_cuenta_contable_corp,
        mcc.cod_ejercicio, 
        mcc.cod_cuenta_contable, 
        mcc.desc_cuenta_contable, 
        mcc.cod_tipo_cuenta, 
        mcc.cod_tipo_moneda, 
        mcc.cod_naturaleza, 
        mcc.cod_indicador_balance, 
        mcc.cod_indicador_resultado, 
        mcc.tiene_centro_costo, 
        mcc.tiene_cuenta_auxiliar, 
        mcc.cod_tipo_cuenta_auxiliar, 
        mcc.tiene_ifrs,
        mcc.flg_tipres,
        mcc.cod_cuenta_contable_corporativa,
        mcc.desc_cuenta_contable_corporativa,
        mcc.longitud,
        mcc.cod_cuenta_contable_bp,
        mcc.desc_cuenta_contable_bp,
        mcc.desc_cuenta_contable_bp_corp,
        mcc.tiene_indicador_bp,
        case when coalesce(mccc.cuenta_clase, '') = '' then '[NO IDENTIFICADO]' else mccc.cuenta_clase end as cuenta_clase_02,
        case when coalesce(mcccc.cuenta_clase, '') = '' then '[NO IDENTIFICADO]' else mcccc.cuenta_clase end as cuenta_clase_01
    from tmp_dim_cuenta_contable_add_cuenta_contable_bp mcc
    left join (
        select
            id_compania,
            cod_ejercicio,
            cod_cuenta_contable,
            case when coalesce(desc_cuenta_contable, '') = '' then '[NO IDENTIFICADO]' else trim(cod_cuenta_contable) || '-' || desc_cuenta_contable end as cuenta_clase
        from tmp_dim_cuenta_contable
        where longitud = 2
    ) mccc on mccc.id_compania = mcc.id_compania
        and mccc.cod_ejercicio = mcc.cod_ejercicio
        and mccc.cod_cuenta_contable = left(mcc.cod_cuenta_contable, 2)
        and mcc.longitud > 2
    left join (
        select
            id_compania,
            cod_ejercicio,
            cod_cuenta_contable,
            case when coalesce(desc_cuenta_contable, '') = '' then '[NO IDENTIFICADO]' else trim(cod_cuenta_contable) || '-' || desc_cuenta_contable end as cuenta_clase
        from tmp_dim_cuenta_contable
        where longitud = 1
    ) mcccc on mcccc.id_compania = mcc.id_compania
        and mcccc.cod_ejercicio = mcc.cod_ejercicio
        and mcccc.cod_cuenta_contable = left(mcc.cod_cuenta_contable, 1)
        and mcc.longitud > 1 
);

drop table if exists tmp_clasificacion_grupos_cogs;
create temp table tmp_clasificacion_grupos_cogs as
(
    select
        cgcogs.id_COMPANIA,
        cgcogs.id_clasificacion_cogs,
        cgcogs.id_ejercicio,
        cgcogs.id_cuenta_contable,
        cgcogs.cod_clasificacion_cogs,
        cgcogs.cod_ejercicio,
        cgcogs.cod_cuenta_contable,
        ccogs.desc_clasificacion_cogs_esp,
        ccogs.desc_clasificacion_cogs_ing 
    from dominio_dev.m_clasificacion_cuenta_contable_cogs cgcogs
    inner join dominio_dev.m_clasificacion_cogs ccogs on cgcogs.id_clasificacion_cogs = ccogs.id_clasificacion_cogs
    where cgcogs.estado = 'A'
);

drop table if exists tmp_dim_cuenta_contable_add_grupo_cogs;
create temp table tmp_dim_cuenta_contable_add_grupo_cogs as
(
    select
        mcc.id_pais, 
        mcc.id_compania, 
        mcc.id_ejercicio, 
        mcc.id_cuenta_contable,
        mcc.id_cuenta_contable_corp,
        mcc.cod_ejercicio, 
        mcc.cod_cuenta_contable, 
        mcc.desc_cuenta_contable, 
        mcc.cod_tipo_cuenta, 
        mcc.cod_tipo_moneda, 
        mcc.cod_naturaleza, 
        mcc.cod_indicador_balance, 
        mcc.cod_indicador_resultado, 
        mcc.tiene_centro_costo, 
        mcc.tiene_cuenta_auxiliar, 
        mcc.cod_tipo_cuenta_auxiliar, 
        mcc.tiene_ifrs,
        mcc.flg_tipres,
        mcc.cod_cuenta_contable_corporativa,
        mcc.desc_cuenta_contable_corporativa,
        mcc.longitud,
        mcc.cod_cuenta_contable_bp,
        mcc.desc_cuenta_contable_bp,
        mcc.desc_cuenta_contable_bp_corp,
        mcc.tiene_indicador_bp,
        mcc.cuenta_clase_02,
        mcc.cuenta_clase_01,
        case when coalesce(tcg.cod_clasificacion_cogs, '') = '' then '000' else tcg.cod_clasificacion_cogs end as cod_clasificacion_cogs,
        case when coalesce(tcg.desc_clasificacion_cogs_esp, '') = '' then 'No Identificado' else tcg.desc_clasificacion_cogs_esp end as desc_clasificacion_cogs_esp,
        case when coalesce(tcg.desc_clasificacion_cogs_ing, '') = '' then 'Not Defined' else tcg.desc_clasificacion_cogs_ing end as desc_clasificacion_cogs_ing
    from tmp_dim_cuenta_contable_add_cuenta_clase mcc
    left join tmp_clasificacion_grupos_cogs tcg on mcc.id_compania = tcg.id_COMPANIA
        and mcc.cod_ejercicio = tcg.cod_ejercicio
        and mcc.cod_cuenta_contable = tcg.cod_cuenta_contable
);

insert into cadena_dev.dim_cuenta_contable(
    id_pais,
    id_compania,
    id_ejercicio,
    id_cuenta_contable,
    id_cuenta_contable_corp,
    cod_ejercicio,
    cod_cuenta_contable,
    desc_cuenta_contable,
    cod_tipo_cuenta,
    cod_tipo_moneda,
    cod_naturaleza,
    cod_indicador_balance,
    cod_indicador_resultado,
    tiene_centro_costo,
    tiene_cuenta_auxiliar,
    cod_tipo_cuenta_auxiliar,
    tiene_ifrs,
    cod_cuenta_contable_corporativa,
    desc_cuenta_contable_corporativa,
    cod_cuenta_contable_bp,
    desc_cuenta_contable_bp,
    desc_cuenta_contable_bp_corp,
    flg_tipres,
    cuenta_clase_02,
    cuenta_clase_01,
    cod_clasificacion_cogs,
    desc_clasificacion_cogs_esp,
    desc_clasificacion_cogs_ing
)
select
    id_pais,
    id_compania,
    id_ejercicio,
    id_cuenta_contable,
    id_cuenta_contable_corp,
    cod_ejercicio,
    cod_cuenta_contable,
    desc_cuenta_contable,
    cod_tipo_cuenta,
    cod_tipo_moneda,
    cod_naturaleza,
    cod_indicador_balance,
    cod_indicador_resultado,
    tiene_centro_costo,
    tiene_cuenta_auxiliar,
    cod_tipo_cuenta_auxiliar,
    tiene_ifrs,
    cod_cuenta_contable_corporativa,
    desc_cuenta_contable_corporativa,
    cod_cuenta_contable_bp,
    desc_cuenta_contable_bp,
    desc_cuenta_contable_bp_corp,
    flg_tipres,
    cuenta_clase_02,
    cuenta_clase_01,
    cod_clasificacion_cogs,
    desc_clasificacion_cogs_esp,
    desc_clasificacion_cogs_ing
from tmp_dim_cuenta_contable_add_grupo_cogs
where not exists(
    select 1
    from cadena_dev.dim_cuenta_contable b
    where tmp_dim_cuenta_contable_add_grupo_cogs.id_cuenta_contable = b.id_cuenta_contable
);

update cadena_dev.dim_cuenta_contable
set
    id_pais = b.id_pais,
    id_compania = b.id_compania,
    id_ejercicio = b.id_ejercicio,
    id_cuenta_contable = b.id_cuenta_contable,
    id_cuenta_contable_corp = b.id_cuenta_contable_corp,
    cod_ejercicio = b.cod_ejercicio,
    cod_cuenta_contable = b.cod_cuenta_contable,
    desc_cuenta_contable = b.desc_cuenta_contable,
    cod_tipo_cuenta = b.cod_tipo_cuenta,
    cod_tipo_moneda = b.cod_tipo_moneda,
    cod_naturaleza = b.cod_naturaleza,
    cod_indicador_balance = b.cod_indicador_balance,
    cod_indicador_resultado = b.cod_indicador_resultado,
    tiene_centro_costo = b.tiene_centro_costo,
    tiene_cuenta_auxiliar = b.tiene_cuenta_auxiliar,
    cod_tipo_cuenta_auxiliar = b.cod_tipo_cuenta_auxiliar,
    tiene_ifrs = b.tiene_ifrs,
    cod_cuenta_contable_corporativa = b.cod_cuenta_contable_corporativa,
    desc_cuenta_contable_corporativa = b.desc_cuenta_contable_corporativa,
    cod_cuenta_contable_bp = b.cod_cuenta_contable_bp,
    desc_cuenta_contable_bp = b.desc_cuenta_contable_bp,
    desc_cuenta_contable_bp_corp = b.desc_cuenta_contable_bp_corp,
    flg_tipres = b.flg_tipres,
    cuenta_clase_02 = b.cuenta_clase_02,
    cuenta_clase_01 = b.cuenta_clase_01,
    cod_clasificacion_cogs = b.cod_clasificacion_cogs,
    desc_clasificacion_cogs_esp = b.desc_clasificacion_cogs_esp,
    desc_clasificacion_cogs_ing = b.desc_clasificacion_cogs_ing
from cadena_dev.dim_cuenta_contable a
inner join tmp_dim_cuenta_contable_add_grupo_cogs b on a.id_cuenta_contable = b.id_cuenta_contable;