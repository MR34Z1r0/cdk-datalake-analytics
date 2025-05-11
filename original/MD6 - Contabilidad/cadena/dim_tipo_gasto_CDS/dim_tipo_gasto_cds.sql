--in variable ${cod_pais} it recieves a tuple of string with the values of cod_pais
--process to pass the values in tuple into rows of temp table (for redshift)
drop table if exists tuple_paises;
create temp table tuple_paises  as (
    select 
        array${cod_pais} as cod_pais
);

drop table if exists paises_list;
create temp table paises_list  as (
    select cod_pais2::text as cod_pais2
    from tuple_paises tupla,
        tupla.cod_pais as cod_pais2
);

drop table if exists tmp_dim_tipo_gasto_cds;
create temp table tmp_dim_tipo_gasto_cds as
(
    select 
        co.cod_pais2 as id_pais,
        co.cod_pais2 || '|' || a.cod_tipo_gasto_cds as id_tipo_gasto_cds,
        a.cod_tipo_gasto_cds,
        a.desc_tipo_gasto_cds
    from external_data_raw.aje_cadena_global_sccc_dim_tipo_gasto_cds a
    join paises_list co on 1=1
);

insert into cadena_dev.dim_tipo_gasto_cds(
    id_pais,
    id_tipo_gasto_cds,
    cod_tipo_gasto_cds,
    desc_tipo_gasto_cds
)
select 
    id_pais,
    id_tipo_gasto_cds,
    cod_tipo_gasto_cds,
    desc_tipo_gasto_cds
from tmp_dim_tipo_gasto_cds a
where not exists (
    select 1 from cadena_dev.dim_tipo_gasto_cds b 
    where a.id_tipo_gasto_cds = b.id_tipo_gasto_cds
);

update cadena_dev.dim_tipo_gasto_cds
set  
    id_pais = b.id_pais,
    cod_tipo_gasto_cds = b.cod_tipo_gasto_cds, 
    desc_tipo_gasto_cds = b.desc_tipo_gasto_cds
from cadena_dev.dim_tipo_gasto_cds a
inner join tmp_dim_tipo_gasto_cds b on a.id_tipo_gasto_cds = b.id_tipo_gasto_cds;
