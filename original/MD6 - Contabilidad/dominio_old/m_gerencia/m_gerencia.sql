drop table if exists tmp_m_gerencia_1;
create temp table tmp_m_gerencia_1 as
(
    select 
        mp.desc_abreviada as cod_pais,
        a.cod_compania,
        a.cod_ejercicio::varchar(4) as cod_ejercicio,
        a.cod_gerencia,
        a.cod_sucursal,
        a.nombre as desc_gerencia,
        a.estado,
        current_date as fecha_creacion,
        current_date as fecha_modificacion
    from big_magic_incoming_${instance}.m_gerencia a
    left join big_magic_incoming_${instance}.m_compania mc on a.cod_compania = mc.cod_compania
    left join big_magic_incoming_${instance}.m_pais mp on mc.cod_pais = mp.cod_pais
    where mp.desc_abreviada in ${cod_pais}
);

drop table if exists tmp_m_gerencia_2;
create temp table tmp_m_gerencia_2 as
(
    select 
        a.cod_pais as id_pais,
        a.cod_compania as id_compania, --id_compania
        a.cod_compania || '|' || a.cod_sucursal as id_sucursal, --id_sucursal
        a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_gerencia as id_gerencia, -- id_gerencia
        a.cod_ejercicio,
        a.cod_gerencia,
        a.cod_sucursal,
        a.desc_gerencia,
        a.estado,
        a.fecha_creacion,
        a.fecha_modificacion
    from tmp_m_gerencia_1 a
);

insert into dominio_dev.m_gerencia(
    id_pais,
    id_compania,
    id_sucursal,
    id_gerencia,
    cod_ejercicio,
    cod_gerencia,
    cod_sucursal,
    desc_gerencia,
    estado,
    fecha_creacion,
    fecha_modificacion
)
select
    a.id_pais,
    a.id_compania,
    a.id_sucursal,
    a.id_gerencia,
    a.cod_ejercicio,
    a.cod_gerencia,
    a.cod_sucursal,
    a.desc_gerencia,
    a.estado,
    a.fecha_creacion,
    a.fecha_modificacion
from tmp_m_gerencia_2 a
where not exists(
    select 1
    from dominio_dev.m_gerencia b
    where a.id_gerencia = b.id_gerencia
);

update dominio_dev.m_gerencia
set
    id_pais = b.id_pais,
    id_compania = b.id_compania,
    id_sucursal = b.id_sucursal,
    cod_ejercicio = b.cod_ejercicio,
    cod_gerencia = b.cod_gerencia,
    cod_sucursal = b.cod_sucursal,
    desc_gerencia = b.desc_gerencia,
    estado = b.estado,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_gerencia a
join tmp_m_gerencia_2 b on a.id_gerencia = b.id_gerencia
and a.id_pais = b.id_pais;