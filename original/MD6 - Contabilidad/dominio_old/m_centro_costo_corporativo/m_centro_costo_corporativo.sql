drop table if exists tmp_m_centro_costo_corporativo_1;
create temp table tmp_m_centro_costo_corporativo_1 as
(
    select 
        a.cod_ejercicio::varchar(4) as cod_ejercicio,
        a.cod_centro_costo_corporativo,
        a.nombre as desc_centro_costo_corporativo,
        a.cod_area,
        a.cod_gerencia,
        case when a.ind_trabcur in ('54') then 1 else 0 end as ind_trabcur,
        a.estado,
        public.cast_datetime_magic(a.fecha_creacion, a.hora_creacion) as fecha_creacion,
        public.cast_datetime_magic(a.fecha_ultima_modificacion, a.hora_ultima_modificacion) as fecha_modificacion
    from big_magic_incoming_${instance}.m_centro_costo_corporativo a
);

drop table if exists tmp_m_centro_costo_corporativo_2;
create temp table tmp_m_centro_costo_corporativo_2 as
(
    select 
        a.cod_ejercicio || '|' || a.cod_centro_costo_corporativo as id_centro_costo_corp,
        a.cod_ejercicio,
        a.cod_centro_costo_corporativo,
        a.desc_centro_costo_corporativo,
        a.cod_area,
        a.cod_gerencia,
        a.ind_trabcur,
        a.estado,
        a.fecha_creacion,
        a.fecha_modificacion
    from tmp_m_centro_costo_corporativo_1 a
);

insert into dominio_dev.m_centro_costo_corporativo(
    id_centro_costo_corp,
    cod_ejercicio,
    cod_centro_costo_corporativo,
    desc_centro_costo_corporativo,
    cod_area,
    cod_gerencia,
    ind_trabcur,
    estado,
    fecha_creacion,
    fecha_modificacion
)
select
    a.id_centro_costo_corp,
    a.cod_ejercicio,
    a.cod_centro_costo_corporativo,
    a.desc_centro_costo_corporativo,
    a.cod_area,
    a.cod_gerencia,
    a.ind_trabcur,
    a.estado,
    a.fecha_creacion,
    a.fecha_modificacion
from tmp_m_centro_costo_corporativo_2 a
where not exists (
    select 1
    from dominio_dev.m_centro_costo_corporativo b
    where a.id_centro_costo_corp = b.id_centro_costo_corp
);

update dominio_dev.m_centro_costo_corporativo
set
    cod_ejercicio = b.cod_ejercicio,
    cod_centro_costo_corporativo = b.cod_centro_costo_corporativo,
    desc_centro_costo_corporativo = b.desc_centro_costo_corporativo,
    cod_area = b.cod_area,
    cod_gerencia = b.cod_gerencia,
    ind_trabcur = b.ind_trabcur,
    estado = b.estado,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_centro_costo_corporativo a
inner join tmp_m_centro_costo_corporativo_2 b on a.id_centro_costo_corp = b.id_centro_costo_corp;