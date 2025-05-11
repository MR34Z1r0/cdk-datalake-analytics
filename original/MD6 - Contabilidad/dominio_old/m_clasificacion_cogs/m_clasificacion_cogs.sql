-- peru aprox 400 registros

drop table if exists tmp_m_clasificacion_cogs_1;
create temp table tmp_m_clasificacion_cogs_1
as(
    select 
        mp.desc_abreviada as cod_pais,
        a.cod_compania,
        a.cod_gpocogs,
        a.descripcion_espanol,
        a.descripcion_ingles,
        current_date as fecha_creacion,
        current_date as fecha_modificacion
    from big_magic_incoming_${instance}.m_clasificacion_cogs a
    left join big_magic_incoming_${instance}.m_compania mc on a.cod_compania = mc.cod_compania
    left join big_magic_incoming_${instance}.m_pais mp on mc.cod_pais = mp.cod_pais
    where mp.desc_abreviada in ${cod_pais}
);

drop table if exists tmp_m_clasificacion_cogs_2;
create temp table tmp_m_clasificacion_cogs_2
as(
    select 
        a.cod_pais as id_pais,
        a.cod_compania as id_compania,
        a.cod_compania || '|' || a.cod_gpocogs as id_clasificacion_cogs,
        a.cod_gpocogs as cod_clasificacion_cogs,
        a.descripcion_espanol as desc_clasificacion_cogs_esp,
        a.descripcion_ingles as desc_clasificacion_cogs_ing,
        a.fecha_creacion,
        a.fecha_modificacion
    from tmp_m_clasificacion_cogs_1 a
);

insert into dominio_dev.m_clasificacion_cogs(
    id_pais,
    id_compania,
    id_clasificacion_cogs,
    cod_clasificacion_cogs,
    desc_clasificacion_cogs_esp,
    desc_clasificacion_cogs_ing,
    fecha_creacion,
    fecha_modificacion
)
select
    a.id_pais,
    a.id_compania,
    a.id_clasificacion_cogs,
    a.cod_clasificacion_cogs,
    a.desc_clasificacion_cogs_esp,
    a.desc_clasificacion_cogs_ing,
    a.fecha_creacion,
    a.fecha_modificacion
from tmp_m_clasificacion_cogs_2 a
where not exists(
    select 1
    from dominio_dev.m_clasificacion_cogs b
    where a.id_clasificacion_cogs = b.id_clasificacion_cogs
);

update dominio_dev.m_clasificacion_cogs
set
    id_pais = b.id_pais,
    id_compania = b.id_compania,
    cod_clasificacion_cogs = b.cod_clasificacion_cogs,
    desc_clasificacion_cogs_esp = b.desc_clasificacion_cogs_esp,
    desc_clasificacion_cogs_ing = b.desc_clasificacion_cogs_ing,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_clasificacion_cogs a
join tmp_m_clasificacion_cogs_2 b on a.id_clasificacion_cogs = b.id_clasificacion_cogs
and a.id_pais = b.id_pais;
