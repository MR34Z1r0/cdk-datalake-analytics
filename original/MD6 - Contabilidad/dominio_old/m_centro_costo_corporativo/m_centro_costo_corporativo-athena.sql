SELECT
  a.id_centro_costo_corporativo_query AS id_centro_costo_corp,
  a.cod_ejercicio,
  a.cod_centro_costo_corporativo,
  a.desc_centro_costo_corporativo,
  a.cod_area,
  a.cod_gerencia,
  CASE
    WHEN a.flg_ind_trabcur = 'T' THEN 1
    ELSE 0
  END AS ind_trabcur,
  a.estado,
  a.fecha_creacion,
  a.fecha_modificacion
FROM
  athenea_sqlserver_pebdajep1qa_stage.m_centro_costo_corporativo a