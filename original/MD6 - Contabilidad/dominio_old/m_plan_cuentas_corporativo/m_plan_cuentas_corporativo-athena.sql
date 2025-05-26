SELECT
  mc.id_plan_cuentas_corporativo AS id_cuenta_contable_corp,
  mc.cod_ejercicio,
  mc.cod_cuenta_contable_corporativa,
  mc.desc_cuenta_contable_corporativa,
  mc.cod_cuenta_abono,
  mc.cod_cuenta_cargo,
  CASE
    WHEN mc.flg_codauxiliar IN ('T') THEN 1
    ELSE 0
  END AS tiene_cuenta_auxiliar,
  mc.cod_naturaleza,
  mc.cod_indicador_balance,
  mc.cod_indicador_resultado,
  mc.cod_indicador_rubro,
  CASE
    WHEN mc.flg_analitic IN ('T') THEN 1
    ELSE 0
  END AS tiene_analitic,
  CASE
    WHEN mc.flg_tipo_enlace IN ('T') THEN 1
    ELSE 0
  END AS es_enlace,
  mc.cod_tipo_cuenta_auxiliar,
  CASE
    WHEN mc.flg_centro_costo IN ('T') THEN 1
    ELSE 0
  END AS tiene_centro_costo,
  mc.cod_tipo_moneda,
  CASE
    WHEN mc.flg_unidad_negocio IN ('T') THEN 1
    ELSE 0
  END AS tiene_unidad_negocio,
  CASE
    WHEN mc.flg_sku IN ('T') THEN 1
    ELSE 0
  END AS tiene_sku,
  CASE
    WHEN mc.flg_marca IN ('T') THEN 1
    ELSE 0
  END AS tiene_marca,
  CASE
    WHEN mc.flg_categoria IN ('T') THEN 1
    ELSE 0
  END AS tiene_categoria,
  CASE
    WHEN mc.flg_canal IN ('T') THEN 1
    ELSE 0
  END AS tiene_canal,
  CASE
    WHEN mc.flg_orden IN ('T') THEN 1
    ELSE 0
  END AS tiene_orden,
  mc.estado,
  mc.fecha_creacion,
  mc.fecha_modificacion
FROM
  athenea_sqlserver_pebdajep1qa_stage.m_plan_cuentas_corporativo mc