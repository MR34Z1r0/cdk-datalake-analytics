
    SELECT
      mp.id_pais,
      a.cod_compania AS id_compania,
      a.cod_compania || '|' || a.cod_sucursal AS id_sucursal,
      a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_area AS id_area,
      a.cod_compania || '|' || a.cod_ejercicio || '|' || a.cod_gerencia AS id_gerencia,
      a.cod_ejercicio,
      a.cod_area,
      a.cod_gerencia,
      a.cod_sucursal,
      a.desc_area,
      a.estado,
      a.fecha_creacion,
      a.fecha_modificacion
    FROM
      athenea_sqlserver_pebdajep1qa_stage.m_area a
      LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_compania mc ON a.cod_compania = mc.cod_compania
      LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_pais mp ON mc.cod_pais = mp.cod_pais
    WHERE
      mp.id_pais IN ('PE')
