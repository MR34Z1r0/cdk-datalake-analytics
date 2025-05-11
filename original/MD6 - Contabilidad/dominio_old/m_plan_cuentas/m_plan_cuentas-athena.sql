SELECT
  mp.id_pais,
  mpc.id_compania,
  mpc.id_ejercicio,
  mpc.id_cuenta_contable,
  mpc.id_cuenta_contable_corp,
  mpc.cod_ejercicio,
  mpc.cod_cuenta_contable,
  mpc.desc_cuenta_contable,
  mpc.cod_tipo_cuenta,
  mpc.cod_tipo_moneda,
  mpc.cod_naturaleza,
  mpc.cod_indicador_balance,
  mpc.cod_indicador_resultado,
  CASE
    WHEN mpc.flg_centro_costo IN ('T') THEN 1
    ELSE 0
  END AS tiene_centro_costo,
  CASE
    WHEN mpc.flg_cuenta_auxiliar IN ('T') THEN 1
    ELSE 0
  END AS tiene_cuenta_auxiliar,
  mpc.cod_tipo_cuenta_auxiliar,
  CASE
    WHEN mpc.flg_ifrs IN ('T') THEN 1
    ELSE 0
  END AS tiene_ifrs,
  mpc.cod_cuenta_contable_corp,
  --mpc.desc_cuenta_contable_corp, --no existe en ingesta

  coalesce (mpc.flg_tipres, 'N') AS flg_tipres,
  mpc.fecha_creacion,
  mpc.fecha_modificacion
FROM
  athenea_sqlserver_pebdajep1qa_stage.m_plan_cuentas mpc
  LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_compania mc ON mpc.cod_compania = mc.cod_compania
  LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_pais mp ON mc.cod_pais = mp.cod_pais
WHERE
  mp.id_pais IN ('PE')