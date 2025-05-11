--peru aprox 1.5 millones de registros

drop table if exists tmp_m_plan_cuentas;
create temp table tmp_m_plan_cuentas as  
( 
    select
        mp.desc_abreviada as cod_pais,
        mpc.cod_compania,
        mpc.cod_ejercicio::varchar(4) as cod_ejercicio,
        mpc.cod_cuenta_contable,
        trim(mpc.descripcion) as desc_cuenta_contable,
        trim(mpc.tipo_cuenta) as cod_tipo_cuenta,
        trim(mpc.tipo_moneda) as cod_tipo_moneda,
        trim(mpc.naturaleza) as cod_naturaleza,
        trim(mpc.ind_balance) as cod_indicador_balance,
        trim(mpc.ind_result) as cod_indicador_resultado,
        case when mpc.flg_ccosto in ('54') then 1 else 0 end as tiene_centro_costo,
        case when mpc.flg_codauxiliar in ('54') then 1 else 0 end as tiene_cuenta_auxiliar,
        trim(mpc.tipo_codauxiliar) as cod_tipo_cuenta_auxiliar,
        case when mpc.flg_ifrs in ('54') then 1 else 0 end as tiene_ifrs,
        trim(mpc.cod_cuenta_contable_corp) as cod_cuenta_contable_corp,
        mpc.descripcion_corp as desc_cuenta_contable_corp,
        trim(isnull(mpc.flg_tipres, 'N')) as flg_tipres,
        current_date as fecha_creacion,
        current_date as fecha_modificacion
    from big_magic_incoming_${instance}.m_plan_cuentas mpc
    left join big_magic_incoming_${instance}.m_compania mc on mpc.cod_compania = mc.cod_compania
    left join big_magic_incoming_${instance}.m_pais mp on mc.cod_pais = mp.cod_pais
    where mp.desc_abreviada in ${cod_pais}
);

drop table if exists tmp_m_plan_cuentas_1;
create temp table tmp_m_plan_cuentas_1 as  
( 
    select
        mp1.cod_pais as id_pais,
        mp1.cod_compania as id_compania,
        mp1.cod_compania || '|' || mp1.cod_ejercicio as id_ejercicio,
        mp1.cod_compania || '|' || mp1.cod_ejercicio || '|' || mp1.cod_cuenta_contable as id_cuenta_contable,
        mp1.cod_ejercicio || '|' || mp1.cod_cuenta_contable_corp as id_cuenta_contable_corp,
        mp1.cod_ejercicio,
        mp1.cod_cuenta_contable,
        mp1.desc_cuenta_contable,
        mp1.cod_tipo_cuenta,
        mp1.cod_tipo_moneda,
        mp1.cod_naturaleza,
        mp1.cod_indicador_balance,
        mp1.cod_indicador_resultado,
        mp1.tiene_centro_costo,
        mp1.tiene_cuenta_auxiliar,
        mp1.cod_tipo_cuenta_auxiliar,
        mp1.tiene_ifrs,
        mp1.cod_cuenta_contable_corp,
        mp1.desc_cuenta_contable_corp,
        mp1.flg_tipres,
        mp1.fecha_creacion,
        mp1.fecha_modificacion
    from tmp_m_plan_cuentas mp1
);

insert into dominio_dev.m_plan_cuentas(
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
    cod_cuenta_contable_corp, 
    desc_cuenta_contable_corp, 
    flg_tipres, 
    fecha_creacion, 
    fecha_modificacion
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
    cod_cuenta_contable_corp, 
    desc_cuenta_contable_corp, 
    flg_tipres, 
    fecha_creacion, 
    fecha_modificacion
from tmp_m_plan_cuentas_1 a
where not exists (
    select 1 from dominio_dev.m_plan_cuentas b 
    where a.id_pais = b.id_pais
    and a.id_cuenta_contable = b.id_cuenta_contable
);

update dominio_dev.m_plan_cuentas
set  
    id_pais = b.id_pais, 
    id_compania = b.id_compania, 
    id_ejercicio = b.id_ejercicio,
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
    cod_cuenta_contable_corp = b.cod_cuenta_contable_corp, 
    desc_cuenta_contable_corp = b.desc_cuenta_contable_corp, 
    flg_tipres = b.flg_tipres,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_plan_cuentas a
inner join tmp_m_plan_cuentas_1 b on a.id_pais = b.id_pais
and a.id_cuenta_contable = b.id_cuenta_contable;
