SELECT
  mp.id_pais,
  a.id_compania,
  a.id_sucursal,
  a.id_gerencia,
  a.cod_ejercicio,
  a.cod_gerencia,
  a.cod_sucursal,
  a.desc_gerencia,
  a.estado,
  a.fecha_creacion,
  a.fecha_modificacion
FROM
  athenea_sqlserver_pebdajep1qa_stage.m_gerencia a
  LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_compania mc ON a.cod_compania = mc.cod_compania
  LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_pais mp ON mc.cod_pais = mp.cod_pais
WHERE
  mp.id_pais IN ('PE')