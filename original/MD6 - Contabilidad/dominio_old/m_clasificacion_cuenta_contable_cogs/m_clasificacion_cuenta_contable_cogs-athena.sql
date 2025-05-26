select 
        a.cod_pais as id_pais,
        a.id_compania,
        a.id_clasificacion_cuenta_contable_cogs_query as id_clasificacion_cuenta_contable_cogs,
        a.id_ejercicio,
        a.id_cuenta_contable,
        a.id_clasificacion_cogs,
        a.cod_ejercicio,
        a.cod_cuenta_contable,
        a.cod_clasificacion_cogs,
        a.estado
        -- ,a.fecha_creacion
        -- ,a.fecha_modificacion

from
athenea_sqlserver_pebdajep1qa_stage.m_clasificacion_cuenta_contable_cogs a
INNER JOIN athenea_sqlserver_pebdajep1qa_stage.m_compania mco ON mc.cod_compania = mco.cod_compania
INNER JOIN athenea_sqlserver_pebdajep1qa_stage.m_pais mp ON mco.cod_pais = mp.cod_pais
WHERE
  mp.id_pais = 'PE'

drop table if exists tmp_m_clasificacion_cuenta_contable_cogs_1;
create temp table tmp_m_clasificacion_cuenta_contable_cogs_1
as (
    select 
        mp.desc_abreviada as cod_pais,
        a.cod_compania,
        a.cod_ejercicio::varchar(4),
        a.cod_contable as cod_cuenta_contable,
        a.cod_gpocogs,
        a.estado,
        current_date as fecha_creacion,
        current_date as fecha_modificacion
    from big_magic_incoming_${instance}.m_clasificacion_cuenta_contable_cogs a
    left join big_magic_incoming_${instance}.m_compania mc on a.cod_compania = mc.cod_compania
    left join big_magic_incoming_${instance}.m_pais mp on mc.cod_pais = mp.cod_pais
    where mp.desc_abreviada in ${cod_pais}
);

drop table if exists tmp_m_clasificacion_cuenta_contable_cogs_2;
create temp table tmp_m_clasificacion_cuenta_contable_cogs_2
as(
    select 
        a.cod_pais as id_pais,
        a.cod_compania as id_compania,
        a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_cuenta_contable || '|' || a.cod_gpocogs as id_clasificacion_cuenta_contable_cogs,
        a.cod_compania || '|' || a.cod_ejercicio as id_ejercicio,
        a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_cuenta_contable as id_cuenta_contable,
        a.cod_compania || '|' || a.cod_gpocogs as id_clasificacion_cogs,
        a.cod_ejercicio,
        a.cod_cuenta_contable,
        a.cod_gpocogs as cod_clasificacion_cogs,
        a.estado,
        a.fecha_creacion,
        a.fecha_modificacion
    from tmp_m_clasificacion_cuenta_contable_cogs_1 a
);

insert into dominio_dev.m_clasificacion_cuenta_contable_cogs(
    id_pais,
    id_compania,
    id_clasificacion_cuenta_contable_cogs,
    id_ejercicio,
    id_cuenta_contable,
    id_clasificacion_cogs,
    cod_ejercicio,
    cod_cuenta_contable,
    cod_clasificacion_cogs,
    estado,
    fecha_creacion,
    fecha_modificacion
)  
select
    a.id_pais,
    a.id_compania,
    a.id_clasificacion_cuenta_contable_cogs,
    a.id_ejercicio,
    a.id_cuenta_contable,
    a.id_clasificacion_cogs,
    a.cod_ejercicio,
    a.cod_cuenta_contable,
    a.cod_clasificacion_cogs,
    a.estado,
    a.fecha_creacion,
    a.fecha_modificacion
from tmp_m_clasificacion_cuenta_contable_cogs_2 a
where not exists(
    select 1
    from dominio_dev.m_clasificacion_cuenta_contable_cogs b
    where a.id_clasificacion_cuenta_contable_cogs = b.id_clasificacion_cuenta_contable_cogs
);

update dominio_dev.m_clasificacion_cuenta_contable_cogs
set
    id_pais = b.id_pais,
    id_compania = b.id_compania,
    id_clasificacion_cuenta_contable_cogs = b.id_clasificacion_cuenta_contable_cogs,
    id_ejercicio = b.id_ejercicio,
    id_cuenta_contable = b.id_cuenta_contable,
    id_clasificacion_cogs = b.id_clasificacion_cogs,
    cod_ejercicio = b.cod_ejercicio,
    cod_cuenta_contable = b.cod_cuenta_contable,
    cod_clasificacion_cogs = b.cod_clasificacion_cogs,
    estado = b.estado,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_clasificacion_cuenta_contable_cogs a
inner join tmp_m_clasificacion_cuenta_contable_cogs_2 b on a.id_pais = b.id_pais
and a.id_clasificacion_cuenta_contable_cogs = b.id_clasificacion_cuenta_contable_cogs;