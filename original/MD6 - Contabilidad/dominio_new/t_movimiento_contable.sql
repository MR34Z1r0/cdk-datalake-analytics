drop table if exists cadena_dev.tmp_fact_sado_contable_centro_costo;
create temp table tmp_fact_saldo_contable_centro_costo as
(
    select
        tvc.id_pais,
        tvc.id_compania,
        tvc.id_sucursal,
        tvc.id_periodo,
        tvc.id_periodo_contable,
        tvc.id_cuenta_contable,
        tvc.id_centro_costo,
        tvc.debe_mn,
        tvc.debe_me,
        tvc.haber_mn,
        tvc.haber_me,
        tvc.cod_tipo_gasto_cds,
        tvc.id_tipo_gasto_cds,
        tvc.cod_clasificacion_pl,
        tvc.id_clasificacion_pl
    from athenea_sqlserver_pebdajep1qa_stage.t_voucher_cabecera tvc
    left join athenea_sqlserver_pebdajep1qa_stage.t_voucher_detalle tvd on tvd.id_voucher_cabecera = tvc.id_voucher_cabecera
);
-- Eliminar registros presentes en athenea_sqlserver_pebdajep1qa_stage.t_voucher_eliminados tve on tvc.id_voucher_cabecera = tve.id_voucher_eliminados

-- Lógica debe y haber mn/me:
SELECT    CASE WHEN NATURALEZA = 'D' THEN  IMPORTE_SOL ELSE 0 END DEBE_MN, 
				CASE WHEN NATURALEZA = 'H' THEN  -1 * IMPORTE_SOL ELSE 0 END HABER_MN,
				CASE WHEN NATURALEZA = 'D' THEN  IMPORTE_DOLAR ELSE 0 END DEBE_ME, 
				CASE WHEN NATURALEZA = 'H' THEN  -1 * IMPORTE_DOLAR ELSE 0 END HABER_ME,
IMPORTE_DOLAR,  NATURALEZA, * 
FROM ODSDWAJE.dbo.T_VOUCHER_DETALLE

-- para el cruce con configuracion_cds, seguir la lógica que se encuentra en dominio_old/t_voucher_resumen
-- tmp_t_voucher_resumen_3

