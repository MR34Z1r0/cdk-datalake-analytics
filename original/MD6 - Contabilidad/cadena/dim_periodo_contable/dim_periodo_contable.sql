drop table if exists tmp_dim_periodo_contable;
create temp table tmp_dim_periodo_contable as
(
    select
        id_periodo_contable, 
        id_pais, 
        id_compania, 
        id_sucursal, 
        cod_ejercicio, 
        cod_periodo, 
        cierre_mes, 
        estado
    from dominio_dev.m_periodo_contable
    where id_pais in ${cod_pais}
);

insert into cadena_dev.dim_periodo_contable(
    id_periodo_contable, 
    id_pais, 
    id_compania, 
    id_sucursal, 
    cod_ejercicio, 
    cod_periodo, 
    cierre_mes, 
    estado
)
select 
    id_periodo_contable, 
    id_pais, 
    id_compania, 
    id_sucursal, 
    cod_ejercicio, 
    cod_periodo, 
    cierre_mes, 
    estado
from tmp_dim_periodo_contable a
where not exists (
    select 1 from cadena_dev.dim_periodo_contable b 
    where a.id_periodo_contable = b.id_periodo_contable
);

update cadena_dev.dim_periodo_contable
set  
    id_pais = b.id_pais, 
    id_compania = b.id_compania, 
    id_sucursal = b.id_sucursal, 
    cod_ejercicio = b.cod_ejercicio, 
    cod_periodo = b.cod_periodo, 
    cierre_mes = b.cierre_mes, 
    estado = b.estado
from cadena_dev.dim_periodo_contable a
inner join tmp_dim_periodo_contable b on a.id_periodo_contable = b.id_periodo_contable;
