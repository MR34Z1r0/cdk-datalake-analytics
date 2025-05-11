drop table if exists tmp_m_centro_costo_1;
create temp table tmp_m_centro_costo_1 as
(
    select 
        mp.desc_abreviada as cod_pais,
        mc.cod_compania,
        mcc.cod_centro_costo,
        mcc.cod_ejercicio::varchar(4) as cod_ejercicio,
        mcc.nombre as desc_centro_costo,
        mcc.cod_area,
        mcc.cod_gerencia,
        mcc.tipo as cod_tipo,
        --mcc.tipo_almacen as cod_tipo_almacen, --viene como null o cadena vacia desde ODS
        null as cod_tipo_almacen,
        mcc.estado,
        mcc.cod_centro_costo_corp,
        mcc.nombre_corp as desc_centro_costo_corp,
        current_date as fecha_creacion,
        current_date as fecha_modificacion
    from big_magic_incoming_${instance}.m_centro_costo mcc
    left join big_magic_incoming_${instance}.m_compania mc on mcc.cod_compania = mc.cod_compania
    left join big_magic_incoming_${instance}.m_pais mp on mc.cod_pais = mp.cod_pais
    where mp.desc_abreviada in ${cod_pais}
);

drop table if exists tmp_m_centro_costo_2;
create temp table tmp_m_centro_costo_2 as
(
    select 
        a.cod_pais as id_pais,
        a.cod_compania as id_compania,
        a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_centro_costo as id_centro_costo,
        a.cod_ejercicio || '|' || a.cod_centro_costo_corp as id_centro_costo_corp,
        a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_area as id_area,
        a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_gerencia as id_gerencia,
        a.cod_ejercicio,
        a.cod_centro_costo,
        a.desc_centro_costo,
        a.cod_centro_costo_corp,
        a.desc_centro_costo_corp,
        a.cod_area,
        a.cod_gerencia,
        a.cod_tipo,
        a.cod_tipo_almacen,
        a.estado,        
        a.fecha_creacion,
        a.fecha_modificacion
    from tmp_m_centro_costo_1 a
);

insert into dominio_dev.m_centro_costo(
    id_pais,
    id_compania,
    id_centro_costo,
    id_centro_costo_corp,
    id_area,
    id_gerencia,
    cod_ejercicio,
    cod_centro_costo,
    desc_centro_costo,
    cod_centro_costo_corp,
    desc_centro_costo_corp,
    cod_area,
    cod_gerencia,
    cod_tipo,
    cod_tipo_almacen,
    estado,
    fecha_creacion,
    fecha_modificacion
)
select
    a.id_pais,
    a.id_compania,
    a.id_centro_costo,
    a.id_centro_costo_corp,
    a.id_area,
    a.id_gerencia,
    a.cod_ejercicio,
    a.cod_centro_costo,
    a.desc_centro_costo,
    a.cod_centro_costo_corp,
    a.desc_centro_costo_corp,
    a.cod_area,
    a.cod_gerencia,
    a.cod_tipo,
    a.cod_tipo_almacen,
    a.estado,
    a.fecha_creacion,
    a.fecha_modificacion
from tmp_m_centro_costo_2 a
where not exists(
    select 1
    from dominio_dev.m_centro_costo b
    where a.id_centro_costo = b.id_centro_costo
);

update dominio_dev.m_centro_costo
set 
    id_pais = b.id_pais,
    id_compania = b.id_compania,
    id_centro_costo_corp = b.id_centro_costo_corp,
    id_area = b.id_area,
    id_gerencia = b.id_gerencia,
    cod_ejercicio = b.cod_ejercicio,
    cod_centro_costo = b.cod_centro_costo,
    desc_centro_costo = b.desc_centro_costo,
    cod_centro_costo_corp = b.cod_centro_costo_corp,
    desc_centro_costo_corp = b.desc_centro_costo_corp,
    cod_area = b.cod_area,
    cod_gerencia = b.cod_gerencia,
    cod_tipo = b.cod_tipo,
    cod_tipo_almacen = b.cod_tipo_almacen,
    estado = b.estado,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_centro_costo a
join tmp_m_centro_costo_2 b on a.id_centro_costo = b.id_centro_costo
and a.id_pais = b.id_pais;
