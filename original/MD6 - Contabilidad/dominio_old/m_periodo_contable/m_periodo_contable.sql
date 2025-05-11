--peru aprox 250 mil registros

drop table if exists tmp_periodo_contable_1;
create temp table tmp_periodo_contable_1 as  
( 
    select
        mpa.desc_abreviada as cod_pais,
        mp.cod_compania,
        mp.cod_sucursal,
        mp.cod_ejercicio::varchar(4) as cod_ejercicio,
        lpad(cod_periodo::varchar(2), 2, '0')::varchar(6) as cod_periodo,
        mf.formato_fecha as cierre_mes,
        mp.estado_periodo as estado
    from big_magic_incoming_${instance}.m_periodo mp
    left join big_magic_incoming_${instance}.m_fecha mf on mp.cierremes = mf.cod_fecha
    left join big_magic_incoming_${instance}.m_compania mco on mp.cod_compania = mco.cod_compania
    left join big_magic_incoming_${instance}.m_pais mpa on mco.cod_pais = mpa.cod_pais
    where mpa.desc_abreviada in ${cod_pais}
    
);

drop table if exists tmp_periodo_contable_2;
create temp table tmp_periodo_contable_2 as  
( 
    select
        tp1.cod_compania || '|' || tp1.cod_ejercicio || '|' || tp1.cod_periodo as id_periodo_contable,
        tp1.cod_pais as id_pais,
        tp1.cod_compania as id_compania,
        tp1.cod_compania || '|' || tp1.cod_sucursal as id_sucursal,
        tp1.cod_ejercicio,
        tp1.cod_periodo,
        tp1.cierre_mes,
        tp1.estado,
        current_date as fecha_creacion,
        current_date as fecha_modificacion
    from tmp_periodo_contable_1 tp1
);

insert into dominio_dev.m_periodo_contable(
    id_periodo_contable, 
    id_pais, 
    id_compania, 
    id_sucursal, 
    cod_ejercicio, 
    cod_periodo, 
    cierre_mes, 
    estado,
    fecha_creacion,
    fecha_modificacion
)
select 
    id_periodo_contable, 
    id_pais, 
    id_compania, 
    id_sucursal, 
    cod_ejercicio, 
    cod_periodo, 
    cierre_mes, 
    estado,
    fecha_creacion,
    fecha_modificacion
from tmp_periodo_contable_2 a
where not exists (
    select 1 from dominio_dev.m_periodo_contable b 
    where a.id_periodo_contable = b.id_periodo_contable
);

update dominio_dev.m_periodo_contable
set  
    id_pais = b.id_pais, 
    id_compania = b.id_compania, 
    id_sucursal = b.id_sucursal, 
    cod_ejercicio = b.cod_ejercicio, 
    cod_periodo = b.cod_periodo, 
    cierre_mes = b.cierre_mes, 
    estado = b.estado,
    fecha_modificacion = b.fecha_modificacion
from dominio_dev.m_periodo_contable a
inner join tmp_periodo_contable_2 b on a.id_periodo_contable = b.id_periodo_contable;
