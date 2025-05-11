drop table if exists tmp_m_centro_costo_1;
create temp table tmp_m_centro_costo_1 as
(
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
        coalesce(b.cod_centro_costo_corporativo, '000000000') as cod_centro_costo_corp,
        coalesce(b.desc_centro_costo_corporativo,'CENTRO COSTO CORP') as desc_centro_costo_corp,
        coalesce(c.cod_area, '000') as cod_area,
        coalesce(c.desc_area, 'AREA DEFAULT') as desc_area,
        coalesce(d.cod_gerencia, '00') as cod_gerencia,
        coalesce(d.desc_gerencia, 'GERENCIA DEFAULT') as desc_gerencia,
        a.cod_tipo,
        a.cod_tipo_almacen,
        a.estado
    from dominio_dev.m_centro_costo a
    left join dominio_dev.m_centro_costo_corporativo b on a.id_centro_costo_corp = b.id_centro_costo_corp
    left join dominio_dev.m_area c on a.id_area = c.id_area
    left join dominio_dev.m_gerencia d on a.id_gerencia = d.id_gerencia
    where a.id_pais in ${cod_pais}
);

drop table if exists tmp_m_centro_costo_add_centro_costo_bp;
create temp table tmp_m_centro_costo_add_centro_costo_bp as
(
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
        coalesce(a.cod_centro_costo_corp, '0000') as cod_centro_costo_corp,
        coalesce(a.desc_centro_costo_corp, '[NO IDENTIFICADO]') as desc_centro_costo_corp,
        a.cod_area,
        a.desc_area,
        a.cod_gerencia,
        a.desc_gerencia,
        a.cod_tipo,
        a.cod_tipo_almacen,
        a.estado,
        coalesce(b.cod_centro_costo_bp, '0') as cod_centro_costo_bp,
        coalesce(b.desc_centro_costo_bp, '[NO IDENTIFICADO]') as desc_centro_costo_bp,
        coalesce(b.desc_centro_costo_bp_corp, '[NO IDENTIFICADO]') as desc_centro_costo_bp_corp
    from tmp_m_centro_costo_1 a
    left join external_data_raw.aje_cadena_global_sccc_configuracion_ebitda_centro_costo b on a.cod_ejercicio <= left(b.PERIODO_FIN::varchar(6),4)
        and a.cod_ejercicio >= left(b.PERIODO_INICIO::varchar(6),4)
        and a.cod_centro_costo = b.cod_centro_costo
        and a.id_compania = b.cod_compania
);

insert into cadena_dev.dim_centro_costo(
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
    desc_area,
    cod_gerencia,
    desc_gerencia,
    cod_tipo,
    cod_tipo_almacen,
    estado,
    cod_centro_costo_bp,
    desc_centro_costo_bp,
    desc_centro_costo_bp_corp
)
select 
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
    desc_area,
    cod_gerencia,
    desc_gerencia,
    cod_tipo,
    cod_tipo_almacen,
    estado,
    cod_centro_costo_bp,
    desc_centro_costo_bp,
    desc_centro_costo_bp_corp
from tmp_m_centro_costo_add_centro_costo_bp a
where not exists (
    select 1 from cadena_dev.dim_centro_costo b 
    where a.id_centro_costo = b.id_centro_costo
);

update cadena_dev.dim_centro_costo
set  
    id_pais = b.id_pais, 
    id_compania = b.id_compania, 
    id_centro_costo = b.id_centro_costo, 
    id_centro_costo_corp = b.id_centro_costo_corp, 
    id_area = b.id_area, 
    id_gerencia = b.id_gerencia, 
    cod_ejercicio = b.cod_ejercicio, 
    cod_centro_costo = b.cod_centro_costo, 
    desc_centro_costo = b.desc_centro_costo, 
    cod_centro_costo_corp = b.cod_centro_costo_corp, 
    desc_centro_costo_corp = b.desc_centro_costo_corp, 
    cod_area = b.cod_area, 
    desc_area = b.desc_area, 
    cod_gerencia = b.cod_gerencia, 
    desc_gerencia = b.desc_gerencia, 
    cod_tipo = b.cod_tipo, 
    cod_tipo_almacen = b.cod_tipo_almacen, 
    estado = b.estado, 
    cod_centro_costo_bp = b.cod_centro_costo_bp, 
    desc_centro_costo_bp = b.desc_centro_costo_bp, 
    desc_centro_costo_bp_corp = b.desc_centro_costo_bp_corp
from cadena_dev.dim_centro_costo a
inner join tmp_m_centro_costo_add_centro_costo_bp b
on a.id_centro_costo = b.id_centro_costo;
