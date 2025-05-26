WITH
  tmp_dim_cuenta_contable AS (
    SELECT
      mp.id_pais,
      mcc.id_compania,
      mcc.id_ejercicio,
      mcc.id_cuenta_contable,
      mcc.id_cuenta_contable_corp,
      mcc.cod_ejercicio,
      mcc.cod_cuenta_contable,
      mcc.desc_cuenta_contable,
      mcc.cod_tipo_cuenta,
      mcc.cod_tipo_moneda,
      mcc.cod_naturaleza,
      mcc.cod_indicador_balance,
      mcc.cod_indicador_resultado,
  CASE
    WHEN mcc.flg_centro_costo IN ('T') THEN 1
    ELSE 0
  END AS tiene_centro_costo,
  CASE
    WHEN mcc.flg_cuenta_auxiliar IN ('T') THEN 1
    ELSE 0
  END AS tiene_cuenta_auxiliar,
      mcc.cod_tipo_cuenta_auxiliar,
        CASE
    WHEN mcc.flg_ifrs IN ('T') THEN 1
    ELSE 0
  END AS tiene_ifrs,
      CASE
        WHEN mccorp.cod_cuenta_contable_corporativa = '' THEN '0000'
        ELSE mccorp.cod_cuenta_contable_corporativa
      END AS cod_cuenta_contable_corporativa,
      CASE
        WHEN TRIM(mccorp.desc_cuenta_contable_corporativa) = '' THEN '[NO IDENTIFICADO]'
        ELSE TRIM(mccorp.desc_cuenta_contable_corporativa)
      END AS desc_cuenta_contable_corporativa,
  coalesce (mcc.flg_tipres, 'N') AS flg_tipres,
      length (TRIM(mcc.cod_cuenta_contable)) AS longitud
    FROM
      athenea_sqlserver_pebdajep1qa_stage.m_plan_cuentas mcc
      LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_plan_cuentas_corporativo mccorp ON mcc.id_cuenta_contable_corp = mccorp.id_plan_cuentas_corporativo
      LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_compania mc ON mcc.id_compania = mc.cod_compania
      LEFT JOIN athenea_sqlserver_pebdajep1qa_stage.m_pais mp ON mc.cod_pais = mp.cod_pais
    WHERE
      mp.id_pais IN ('PE')
  ),
  tmp_dim_cuenta_contable_add_cuenta_contable_bp AS (
    SELECT
      mcc.id_pais,
      mcc.id_compania,
      mcc.id_ejercicio,
      mcc.id_cuenta_contable,
      mcc.id_cuenta_contable_corp,
      mcc.cod_ejercicio,
      mcc.cod_cuenta_contable,
      mcc.desc_cuenta_contable,
      mcc.cod_tipo_cuenta,
      mcc.cod_tipo_moneda,
      mcc.cod_naturaleza,
      mcc.cod_indicador_balance,
      mcc.cod_indicador_resultado,
      mcc.tiene_centro_costo,
      mcc.tiene_cuenta_auxiliar,
      mcc.cod_tipo_cuenta_auxiliar,
      mcc.tiene_ifrs,
      mcc.flg_tipres,
      mcc.cod_cuenta_contable_corporativa,
      mcc.desc_cuenta_contable_corporativa,
      mcc.longitud,
      COALESCE(ebitdapc.COD_CUENTA_CONTABLE_BP, '00') AS cod_cuenta_contable_bp,
      COALESCE(ebitdapc.DESC_CUENTA_CONTABLE_BP, '[NO IDENTIFICADO]') AS desc_cuenta_contable_bp,
      COALESCE(ebitdapc.DESC_CUENTA_CONTABLE_BP_CORP, '[NO IDENTIFICADO]') AS desc_cuenta_contable_bp_corp,
      CASE
        WHEN ebitdapcnc.cod_cuenta_contable IS NULL THEN 1
        ELSE 0
      END AS tiene_indicador_bp
    FROM
      tmp_dim_cuenta_contable mcc
      LEFT JOIN "aje-qa-athenea-modelo-cadena-db".configuracion_ebitda_plan_cuenta ebitdapc ON mcc.cod_cuenta_contable = cast(ebitdapc.cod_cuenta_contable as varchar)
      AND ebitdapc.cod_pais = mcc.id_pais
      AND mcc.cod_ejercicio <= substr (cast(ebitdapc.periodo_fin as varchar), 1, 4)
      AND mcc.cod_ejercicio >= substr (cast(ebitdapc.periodo_inicio as varchar), 1, 4)
      AND lpad(cast(ebitdapc.cod_compania as varchar),4,'0') = mcc.id_compania

      LEFT JOIN "aje-qa-athenea-modelo-cadena-db".configuracion_ebitda_plan_cuenta_no_considerada ebitdapcnc ON mcc.cod_cuenta_contable = cast(ebitdapcnc.cod_cuenta_contable as varchar)
      AND ebitdapcnc.cod_pais = mcc.id_pais
      AND mcc.cod_ejercicio <= substr (cast(ebitdapcnc.periodo_fin as varchar), 1, 4)
      AND mcc.cod_ejercicio >= substr (cast(ebitdapcnc.periodo_inicio as varchar), 1, 4)
      AND lpad(cast(ebitdapcnc.cod_compania as varchar),4,'0') = mcc.id_compania
      
  ),
  tmp_dim_cuenta_contable_add_cuenta_clase AS (
    SELECT
      mcc.id_pais,
      mcc.id_compania,
      mcc.id_ejercicio,
      mcc.id_cuenta_contable,
      mcc.id_cuenta_contable_corp,
      mcc.cod_ejercicio,
      mcc.cod_cuenta_contable,
      mcc.desc_cuenta_contable,
      mcc.cod_tipo_cuenta,
      mcc.cod_tipo_moneda,
      mcc.cod_naturaleza,
      mcc.cod_indicador_balance,
      mcc.cod_indicador_resultado,
      mcc.tiene_centro_costo,
      mcc.tiene_cuenta_auxiliar,
      mcc.cod_tipo_cuenta_auxiliar,
      mcc.tiene_ifrs,
      mcc.flg_tipres,
      mcc.cod_cuenta_contable_corporativa,
      mcc.desc_cuenta_contable_corporativa,
      mcc.longitud,
      mcc.cod_cuenta_contable_bp,
      mcc.desc_cuenta_contable_bp,
      mcc.desc_cuenta_contable_bp_corp,
      mcc.tiene_indicador_bp,
      CASE
        WHEN COALESCE(mccc.cuenta_clase, '') = '' THEN '[NO IDENTIFICADO]'
        ELSE mccc.cuenta_clase
      END AS cuenta_clase_02,
      CASE
        WHEN COALESCE(mcccc.cuenta_clase, '') = '' THEN '[NO IDENTIFICADO]'
        ELSE mcccc.cuenta_clase
      END AS cuenta_clase_01
    FROM
      tmp_dim_cuenta_contable_add_cuenta_contable_bp mcc
      LEFT JOIN (
        SELECT
          id_compania,
          cod_ejercicio,
          cod_cuenta_contable,
          CASE
            WHEN COALESCE(desc_cuenta_contable, '') = '' THEN '[NO IDENTIFICADO]'
            ELSE TRIM(cod_cuenta_contable) || '-' || desc_cuenta_contable
          END AS cuenta_clase
        FROM
          tmp_dim_cuenta_contable
        WHERE
          longitud = 2
      ) mccc ON mccc.id_compania = mcc.id_compania
      AND mccc.cod_ejercicio = mcc.cod_ejercicio
      AND mccc.cod_cuenta_contable = substr (mcc.cod_cuenta_contable, 1, 2)
      AND mcc.longitud > 2
      LEFT JOIN (
        SELECT
          id_compania,
          cod_ejercicio,
          cod_cuenta_contable,
          CASE
            WHEN COALESCE(desc_cuenta_contable, '') = '' THEN '[NO IDENTIFICADO]'
            ELSE TRIM(cod_cuenta_contable) || '-' || desc_cuenta_contable
          END AS cuenta_clase
        FROM
          tmp_dim_cuenta_contable
        WHERE
          longitud = 1
      ) mcccc ON mcccc.id_compania = mcc.id_compania
      AND mcccc.cod_ejercicio = mcc.cod_ejercicio
      AND mcccc.cod_cuenta_contable = substr (mcc.cod_cuenta_contable, 1, 1)
      AND mcc.longitud > 1
  ),
  tmp_clasificacion_grupos_cogs AS (
    SELECT
      cgcogs.id_COMPANIA,
      cgcogs.id_clasificacion_cogs,
      cgcogs.id_ejercicio,
      cgcogs.id_cuenta_contable,
      cgcogs.cod_clasificacion_cogs,
      cgcogs.cod_ejercicio,
      cgcogs.cod_cuenta_contable,
      ccogs.desc_clasificacion_cogs_esp,
      ccogs.desc_clasificacion_cogs_ing
    FROM
      athenea_sqlserver_pebdajep1qa_stage.m_clasificacion_cuenta_contable_cogs cgcogs
      INNER JOIN athenea_sqlserver_pebdajep1qa_stage.m_clasificacion_cogs ccogs ON cgcogs.id_clasificacion_cogs = ccogs.id_clasificacion_cogs
    WHERE
      cgcogs.estado = 'A'
  ),
  tmp_dim_cuenta_contable_add_grupo_cogs AS (
    SELECT
      mcc.id_pais,
      mcc.id_compania,
      mcc.id_ejercicio,
      mcc.id_cuenta_contable,
      mcc.id_cuenta_contable_corp,
      mcc.cod_ejercicio,
      mcc.cod_cuenta_contable,
      mcc.desc_cuenta_contable,
      mcc.cod_tipo_cuenta,
      mcc.cod_tipo_moneda,
      mcc.cod_naturaleza,
      mcc.cod_indicador_balance,
      mcc.cod_indicador_resultado,
      mcc.tiene_centro_costo,
      mcc.tiene_cuenta_auxiliar,
      mcc.cod_tipo_cuenta_auxiliar,
      mcc.tiene_ifrs,
      mcc.flg_tipres,
      mcc.cod_cuenta_contable_corporativa,
      mcc.desc_cuenta_contable_corporativa,
      mcc.longitud,
      mcc.cod_cuenta_contable_bp,
      mcc.desc_cuenta_contable_bp,
      mcc.desc_cuenta_contable_bp_corp,
      mcc.tiene_indicador_bp,
      mcc.cuenta_clase_02,
      mcc.cuenta_clase_01,
      CASE
        WHEN COALESCE(tcg.cod_clasificacion_cogs, '') = '' THEN '000'
        ELSE tcg.cod_clasificacion_cogs
      END AS cod_clasificacion_cogs,
      CASE
        WHEN COALESCE(tcg.desc_clasificacion_cogs_esp, '') = '' THEN 'No Identificado'
        ELSE tcg.desc_clasificacion_cogs_esp
      END AS desc_clasificacion_cogs_esp,
      CASE
        WHEN COALESCE(tcg.desc_clasificacion_cogs_ing, '') = '' THEN 'Not Defined'
        ELSE tcg.desc_clasificacion_cogs_ing
      END AS desc_clasificacion_cogs_ing
    FROM
      tmp_dim_cuenta_contable_add_cuenta_clase mcc
      LEFT JOIN tmp_clasificacion_grupos_cogs tcg ON mcc.id_compania = tcg.id_COMPANIA
      AND mcc.cod_ejercicio = tcg.cod_ejercicio
      AND mcc.cod_cuenta_contable = tcg.cod_cuenta_contable
  )
SELECT
  *
FROM
  tmp_dim_cuenta_contable_add_grupo_cogs