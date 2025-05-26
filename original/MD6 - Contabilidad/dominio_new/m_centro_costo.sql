with tmp_m_centro_costo_1 as
(
    select 
        mp.id_pais,
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
    from athenea_sqlserver_pebdajep1qa_stage.m_centro_costo a
    left join athenea_sqlserver_pebdajep1qa_stage.m_centro_costo_corporativo b on a.id_centro_costo_corp = b.id_centro_costo_corporativo_query
    left join athenea_sqlserver_pebdajep1qa_stage.m_area c on a.id_area = c.id_area
    left join athenea_sqlserver_pebdajep1qa_stage.m_gerencia d on a.id_gerencia = d.id_gerencia
  LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_compania mc ON a.id_compania = mc.cod_compania
  LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_pais mp ON mc.cod_pais = mp.cod_pais
WHERE
  mp.id_pais IN ('PE')
), tmp_m_centro_costo_add_centro_costo_bp as
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
        coalesce(cast(b.cod_centro_costo_bp as varchar), '0') as cod_centro_costo_bp,
        coalesce(b.desc_centro_costo_bp, '[NO IDENTIFICADO]') as desc_centro_costo_bp,
        coalesce(b.desc_centro_costo_bp_corp, '[NO IDENTIFICADO]') as desc_centro_costo_bp_corp
    from tmp_m_centro_costo_1 a
    left join "aje-qa-athenea-modelo-cadena-db".configuracion_ebitda_centro_costo b ON a.cod_ejercicio <= substr(cast(b.PERIODO_FIN as varchar), 1,4)
        and a.cod_ejercicio >= substr(cast(b.PERIODO_INICIO as varchar),1,4)
        and a.cod_centro_costo = cast(b.cod_centro_costo as varchar)
        and a.id_compania = lpad(cast(b.cod_compania as varchar),4,'0')
)
select * from tmp_m_centro_costo_add_centro_costo_bp;
