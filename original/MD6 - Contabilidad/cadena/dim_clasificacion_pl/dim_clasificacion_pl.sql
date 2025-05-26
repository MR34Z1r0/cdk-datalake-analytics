drop table if exists tmp_dim_clasificacion_pl;
create temp table tmp_dim_clasificacion_pl as
(
    select 
        DISTINCT
        a.COD_PAIS,
        a.COD_CLASIFICACION_PL,
        a.DESC_CLASIFICACION_PL,
        a.DESC_CLASIFICACION_PL_CORP
    from external_data_raw.aje_cadena_global_sccc_configuracion_ebitda_centro_costo_plan_cuenta a
    where a.COD_PAIS in ${cod_pais}
);

drop table if exists tmp_dim_clasificacion_pl_2;
create temp table tmp_dim_clasificacion_pl_2 as
(
    select 
        a.cod_pais as id_pais,
        a.cod_pais || '|' || a.cod_clasificacion_pl as id_clasificacion_pl,
        a.cod_clasificacion_pl,
        a.desc_clasificacion_pl,
        a.desc_clasificacion_pl_corp
    from tmp_dim_clasificacion_pl a
);

insert into cadena_dev.dim_clasificacion_pl(
    id_pais,
    id_clasificacion_pl,
    cod_clasificacion_pl,
    desc_clasificacion_pl,
    desc_clasificacion_pl_corp
)
select 
    id_pais,
    id_clasificacion_pl,
    cod_clasificacion_pl,
    desc_clasificacion_pl,
    desc_clasificacion_pl_corp
from tmp_dim_clasificacion_pl_2 a
where not exists (
    select 1 from cadena_dev.dim_clasificacion_pl b 
    where a.id_clasificacion_pl = b.id_clasificacion_pl
);

update cadena_dev.dim_clasificacion_pl
set  
    id_pais = b.id_pais,
    cod_clasificacion_pl = b.cod_clasificacion_pl, 
    desc_clasificacion_pl = b.desc_clasificacion_pl,
    desc_clasificacion_pl_corp = b.desc_clasificacion_pl_corp
from cadena_dev.dim_clasificacion_pl a
inner join tmp_dim_clasificacion_pl_2 b on a.id_clasificacion_pl = b.id_clasificacion_pl;