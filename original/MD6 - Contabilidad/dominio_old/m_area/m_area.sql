drop table if exists tmp_m_area_1;
create temp table tmp_m_area_1 as
(
    select 
        mp.desc_abreviada as cod_pais,
        a.cod_compania,
        a.cod_area,
        a.cod_ejercicio::varchar(4) as cod_ejercicio,
        a.cod_gerencia,
        a.cod_sucursal,
        a.descripcion as desc_area,
        a.estado,
        current_date as fecha_creacion,
        current_date as fecha_modificacion
    from big_magic_incoming_${instance}.m_area a
    left join big_magic_incoming_${instance}.m_compania mc on a.cod_compania = mc.cod_compania
    left join big_magic_incoming_${instance}.m_pais mp on mc.cod_pais = mp.cod_pais
    where mp.desc_abreviada in ${cod_pais}
);

drop table if exists tmp_m_area_2;
create temp table tmp_m_area_2 as
(
    select 
        a.cod_pais as id_pais,
        a.cod_compania as id_compania,
        a.cod_compania || '|' || a.cod_sucursal as id_sucursal,
        a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_area as id_area,
        a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_gerencia as id_gerencia,
        a.cod_ejercicio,
        a.cod_area,
        a.cod_gerencia,
        a.cod_sucursal,
        a.desc_area,
        a.estado,
        a.fecha_creacion,
        a.fecha_modificacion
    from tmp_m_area_1 a
);

insert into dominio_dev.m_area(
    id_pais,
    id_compania,
    id_sucursal,
    id_area,
    id_gerencia,
    cod_ejercicio,
    cod_area,
    cod_gerencia,
    cod_sucursal,
    desc_area,
    estado,
    fecha_creacion,
    fecha_modificacion
)
select
    a.id_pais,
    a.id_compania,
    a.id_sucursal,
    a.id_area,
    a.id_gerencia,
    a.cod_ejercicio,
    a.cod_area,
    a.cod_gerencia,
    a.cod_sucursal,
    a.desc_area,
    a.estado,
    a.fecha_creacion,
    a.fecha_modificacion
from tmp_m_area_2 a
where not exists(
    select 1
    from dominio_dev.m_area b
    where a.id_area = b.id_area
);

update dominio_dev.m_area
set
    id_pais = b.id_pais,
    id_compania = b.id_compania,
    id_sucursal = b.id_sucursal,
    id_area = b.id_area,
    id_gerencia = b.id_gerencia,
    cod_ejercicio = b.cod_ejercicio,
    cod_area = b.cod_area,
    cod_gerencia = b.cod_gerencia,
    cod_sucursal = b.cod_sucursal,
    desc_area = b.desc_area,
    estado = b.estado,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_area a
inner join tmp_m_area_2 b on a.id_area = b.id_area
and a.id_pais = b.id_pais;
