--peru aprox 38 mil registros

drop table if exists tmp_m_plan_cuentas_corporativo_1;
create temp table tmp_m_plan_cuentas_corporativo_1 as  
( 
    select
        mc.cod_ejercicio,
        mc.cod_cuenta_contable_corporativa,
        mc.descripcion as desc_cuenta_contable_corporativa,
        mc.cod_cuenta_abono,
        mc.cod_cuenta_cargo,
        case when mc.flg_codauxiliar in ('54') then 1 else 0 end as tiene_cuenta_auxiliar,
        mc.naturaleza as cod_naturaleza,
        mc.ind_balance as cod_indicador_balance,
        mc.ind_result as cod_indicador_resultado,
        mc.ind_rubro as cod_indicador_rubro,
        case when mc.flg_analitic in ('54') then 1 else 0 end as tiene_analitic,
        case when mc.tipo_enlace in ('54') then 1 else 0 end as es_enlace,
        trim(mc.tipo_codauxiliar) as cod_tipo_cuenta_auxiliar,
        case when mc.flg_ccosto in ('54') then 1 else 0 end as tiene_centro_costo,
        mc.tipo_moneda as cod_tipo_moneda,
        case when mc.flg_unidad_negocio in ('54') then 1 else 0 end as tiene_unidad_negocio,
        case when mc.flg_sku in ('54') then 1 else 0 end as tiene_sku,
        case when mc.flg_marca in ('54') then 1 else 0 end as tiene_marca,
        case when mc.flg_categoria in ('54') then 1 else 0 end as tiene_categoria,
        case when mc.flg_canal in ('54') then 1 else 0 end as tiene_canal,
        case when mc.flg_orden in ('54') then 1 else 0 end as tiene_orden,
        mc.estado,
        public.cast_datetime_magic(mc.fecha_creacion, mc.hora_creacion) as fecha_creacion,
        public.cast_datetime_magic(mc.fecha_ultima_modificacion, mc.hora_ultima_modificacion) as fecha_modificacion
    from big_magic_incoming_${instance}.m_plan_cuentas_corporativo mc
);

drop table if exists tmp_m_plan_cuentas_corporativo_2;
create temp table tmp_m_plan_cuentas_corporativo_2 as (
    select
        mc.cod_ejercicio || '|' || mc.cod_cuenta_contable_corporativa as id_cuenta_contable_corp, --id_plan_cuentas_corporativo
        mc.cod_ejercicio,
        mc.cod_cuenta_contable_corporativa,
        mc.desc_cuenta_contable_corporativa,
        mc.cod_cuenta_abono,
        mc.cod_cuenta_cargo,
        mc.cod_naturaleza,
        mc.cod_indicador_balance,
        mc.cod_indicador_resultado,
        mc.cod_indicador_rubro,
        mc.cod_tipo_moneda,
        mc.tiene_cuenta_auxiliar,
        mc.cod_tipo_cuenta_auxiliar,
        mc.tiene_analitic,
        mc.es_enlace,
        mc.tiene_centro_costo,
        mc.tiene_unidad_negocio,
        mc.tiene_sku,
        mc.tiene_marca,
        mc.tiene_categoria,
        mc.tiene_canal,
        mc.tiene_orden,
        mc.estado,
        mc.fecha_creacion,
        mc.fecha_modificacion
    from tmp_m_plan_cuentas_corporativo_1 mc
);

insert into dominio_dev.m_plan_cuentas_corporativo(
    id_cuenta_contable_corp,
    cod_ejercicio,
    cod_cuenta_contable_corporativa,
    desc_cuenta_contable_corporativa,
    cod_cuenta_abono,
    cod_cuenta_cargo,
    cod_naturaleza,
    cod_indicador_balance,
    cod_indicador_resultado,
    cod_indicador_rubro,
    cod_tipo_moneda,
    tiene_cuenta_auxiliar,
    cod_tipo_cuenta_auxiliar,
    tiene_analitic,
    es_enlace,
    tiene_centro_costo,
    tiene_unidad_negocio,
    tiene_sku,
    tiene_marca,
    tiene_categoria,
    tiene_canal,
    tiene_orden,
    estado,
    fecha_creacion,
    fecha_modificacion
)
select
    id_cuenta_contable_corp,
    cod_ejercicio,
    cod_cuenta_contable_corporativa,
    desc_cuenta_contable_corporativa,
    cod_cuenta_abono,
    cod_cuenta_cargo,
    cod_naturaleza,
    cod_indicador_balance,
    cod_indicador_resultado,
    cod_indicador_rubro,
    cod_tipo_moneda,
    tiene_cuenta_auxiliar,
    cod_tipo_cuenta_auxiliar,
    tiene_analitic,
    es_enlace,
    tiene_centro_costo,
    tiene_unidad_negocio,
    tiene_sku,
    tiene_marca,
    tiene_categoria,
    tiene_canal,
    tiene_orden,
    estado,
    fecha_creacion,
    fecha_modificacion
from tmp_m_plan_cuentas_corporativo_2 a
where not exists (
    select 1
    from dominio_dev.m_plan_cuentas_corporativo b
    where a.id_cuenta_contable_corp = b.id_cuenta_contable_corp
);

update dominio_dev.m_plan_cuentas_corporativo
set
    cod_ejercicio = b.cod_ejercicio,
    cod_cuenta_contable_corporativa = b.cod_cuenta_contable_corporativa,
    desc_cuenta_contable_corporativa = b.desc_cuenta_contable_corporativa,
    cod_cuenta_abono = b.cod_cuenta_abono,
    cod_cuenta_cargo = b.cod_cuenta_cargo,
    cod_naturaleza = b.cod_naturaleza,
    cod_indicador_balance = b.cod_indicador_balance,
    cod_indicador_resultado = b.cod_indicador_resultado,
    cod_indicador_rubro = b.cod_indicador_rubro,
    cod_tipo_moneda = b.cod_tipo_moneda,
    tiene_cuenta_auxiliar = b.tiene_cuenta_auxiliar,
    cod_tipo_cuenta_auxiliar = b.cod_tipo_cuenta_auxiliar,
    tiene_analitic = b.tiene_analitic,
    es_enlace = b.es_enlace,
    tiene_centro_costo = b.tiene_centro_costo,
    tiene_unidad_negocio = b.tiene_unidad_negocio,
    tiene_sku = b.tiene_sku,
    tiene_marca = b.tiene_marca,
    tiene_categoria = b.tiene_categoria,
    tiene_canal = b.tiene_canal,
    tiene_orden = b.tiene_orden,
    estado = b.estado,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_plan_cuentas_corporativo a
inner join tmp_m_plan_cuentas_corporativo_2 b on a.id_cuenta_contable_corp = b.id_cuenta_contable_corp;

