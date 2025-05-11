SELECT
  mp.id_pais,
  mcc.id_compania,
  mcc.id_centro_costo,
  mcc.id_centro_costo_corp,
  mcc.id_area,
  mcc.id_gerencia,
  mcc.cod_ejercicio,
  mcc.cod_centro_costo,
  mcc.desc_centro_costo,
  mcc.cod_centro_costo_corp,
  -- mcc.desc_centro_costo_corp, --no encuentro este campo en la ingesta
  mcc.cod_area,
  mcc.cod_gerencia,
  mcc.cod_tipo,
  mcc.cod_tipo_almacen,
  mcc.estado,
  mcc.fecha_creacion,
  mcc.fecha_modificacion
FROM
  athenea_sqlserver_pebdajep1qa_stage.m_centro_costo mcc
  LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_compania mc ON mcc.cod_compania = mc.cod_compania
  LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_pais mp ON mc.cod_pais = mp.cod_pais
WHERE
  mp.id_pais IN ('PE')