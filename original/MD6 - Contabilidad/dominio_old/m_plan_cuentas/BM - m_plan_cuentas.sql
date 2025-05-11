use [odsdwaje]
go
/****** object:  storedprocedure [dbo].[usp_tran_m_plan_cuentas]    script date: 2/27/2025 5:26:39 pm ******/
set ansi_nulls on
go
set quoted_identifier on
go
alter procedure [dbo].[usp_tran_m_plan_cuentas] @fechaejecucion varchar(20)=''
/* 
sistema        : ajegroup_ssis_ecuador
nombre         : [usp_tran_m_plan_cuentas]
descripción    : insertar y actualizar registros a m_plan_cuentas
autor          : daphne orihuela - cosapi
fecha creación : 24-03-2014
-------------------------------------------------------------------------------------
fecha       descripción                                              usuario
-------------------------------------------------------------------------------------
25/03/2015	se agregaron los códigos a los que pertenece cada validación	lpachas
14/07/2016  se agregan las columnas de cuentas corporativas				    mcollazos
*/  
as
begin 
declare @nombretabla varchar(50)
declare @valorvalidacion int
declare @flagcorporativo int 
set @nombretabla = 'm_plan_cuentas' 
begin try
	 
	select cod_compania, cod_ejercicio, '0' cod_cuenta_contable, 'plan de cuentas default' descripcion, '0' tipo_cuenta, '0' tipo_moneda, '0' naturaleza, '0' ind_balance, '0' ind_result, 
	0x46 flg_ccosto, 0x46 flg_codauxiliar, '0' tipo_codauxiliar, 0x46 flg_ifrs, '' cod_cuenta_contable_corp, '' descripcion_corp, '0' flg_tipres 
	into #m_plan_cuentas_default --drop table #m_plan_cuentas_default
	from m_ejercicio 
	
	insert into m_plan_cuentas(cod_compania, cod_ejercicio, cod_cuenta_contable, descripcion, tipo_cuenta, tipo_moneda, naturaleza, ind_balance, 
	ind_result,	flg_ccosto, flg_codauxiliar, tipo_codauxiliar, flg_ifrs, cod_cuenta_contable_corp, descripcion_corp, flg_tipres) 
	select cod_compania, cod_ejercicio, cod_cuenta_contable, descripcion, tipo_cuenta, tipo_moneda, naturaleza, ind_balance, 
	ind_result,	flg_ccosto, flg_codauxiliar, tipo_codauxiliar, flg_ifrs, cod_cuenta_contable_corp, descripcion_corp, flg_tipres 
	from #m_plan_cuentas_default t
	where not exists(select 1 from m_plan_cuentas p where t.cod_compania = p.cod_compania and t.cod_ejercicio = p.cod_ejercicio and t.cod_cuenta_contable = p.cod_cuenta_contable)
	--ejecutar si se encuentra validado la tabla para su ejecucion
	exec stgdwaje.dbo.conf_validar_tabla @nombretabla, @fechaejecucion, @valorvalidacion output 
	if @valorvalidacion = 1
	begin 
	
	-- capturando data desde la temporal
	select compania, ejercicio, codcontab, ltrim(rtrim(isnull(descripcta, ''))) descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza, 
	flgbalance, flgresult, flgrubro, tipoimputa, flganalitic, tipoenlace, cresponsab, tipcodauxi, flgccosto, monfunc, tipocuenta, tipomoneda, 
	ctaacm, flgcuentacm, estado, flgundneg, flgsku, flgmarca, flgcateg, flgcanal, ltrim(rtrim(isnull(descripctacorp,''))) descripctacorp, flgtipres, flgorden, 
	isnull(ctactbcorp,'') ctactbcorp, flgifrs, flgvalida, feccrea, fecultmod, horcrea, horultimod, ultusumod, usucrea, fecha_proceso, bd_origen  	
	into #mplcta1f
	from stgdwaje.dbo.stg_mplcta1f a  
	
	-- validando cuando la tabla se encuentre nula
	if @@rowcount = 0
	begin
		insert into stgdwaje.dbo.rej_mplcta1f(id_error, fecha_proceso, desc_error) 
		values (3, @fechaejecucion, 'stage sin data')
		execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0
	end 
	else
	begin 
		
		--validar bd origen vacia
		insert into stgdwaje.dbo.rej_mplcta1f(id_error, fecha_proceso,bd_origen,desc_error )
		select valor = 3, fecha_ejecucion = @fechaejecucion, a.descripcion, 'bd: ' + a.descripcion + ' vacia'
		from stgdwaje..conf_tabla_dominio a where id_grupo=2 and id_orden >0 and valor2 ='activo' 
		and not exists (select 1 from #mplcta1f b where a.descripcion = b.bd_origen )
		
		update a 
		set a.descripcta = 'plan de cuentas default'
		from #mplcta1f a
		where descripcta = ''
		
		update a 
		set a.flgbalance = '0'
		from #mplcta1f a
		where not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo ='balance_cuenta' and m.valor = a.flgbalance) 
		
		update a 
		set a.tipomoneda = '0'
		from #mplcta1f a
		where not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo ='tipo_moneda' and m.valor = a.tipomoneda) 
		
		update a 
		set a.flgresult = '0'
		from #mplcta1f a
		where not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo ='tipo_resultado' and m.valor = a.flgresult) 
		 
		update a 
		set a.tipocuenta = '0'
		from #mplcta1f a
		where not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo ='tipo_cuenta' and m.valor = a.tipocuenta) 
		
		update a 
		set a.tipcodauxi = '0'
		from #mplcta1f a
		where not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo ='tipo_codauxiliar' and m.valor = a.tipcodauxi) 
		
		update a 
		set a.naturaleza = '0'
		from #mplcta1f a
		where not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo ='naturaleza_cuenta' and m.valor = a.naturaleza) 
	   
		select compania, ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom,
		naturaleza, flgbalance, flgresult, flgrubro, tipoimputa, flganalitic, tipoenlace, cresponsab, tipcodauxi, flgccosto,
		monfunc, tipocuenta, tipomoneda, ctaacm, flgcuentacm, estado, flgundneg, flgsku, flgmarca, flgcateg, flgcanal,
		flgorden, ctactbcorp, flgifrs, flgvalida, feccrea, fecultmod, horcrea, horultimod, ultusumod, usucrea, fecha_proceso, bd_origen, 
		descripctacorp, flgtipres,
		id_error = 9,
		desc_error = case 	when not exists (select 1 from m_ejercicio m where m.cod_compania = a.compania and m.cod_ejercicio = a.ejercicio)
							then 'no existe foreign para m_ejercicio' 																
							end
		into #mplcta1f_inconsistencia
		from #mplcta1f a
		where not exists (select 1 from m_ejercicio m where m.cod_compania = a.compania and m.cod_ejercicio = a.ejercicio) 
		 
		-- limpieza e insertar en la tabla rej las inconsistentes
		insert into stgdwaje.dbo.rej_mplcta1f(compania, ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza, 
		flgbalance, flgresult, flgrubro, tipoimputa, flganalitic, tipoenlace, cresponsab, tipcodauxi, flgccosto, monfunc, tipocuenta, tipomoneda, 
		ctaacm, flgcuentacm, estado, flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, ctactbcorp, flgifrs, flgvalida, feccrea, fecultmod, 
		horcrea, horultimod, ultusumod, usucrea, descripctacorp, flgtipres, fecha_proceso, bd_origen, id_error, desc_error)
		select compania, ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza, 
		flgbalance, flgresult, flgrubro, tipoimputa, flganalitic, tipoenlace, cresponsab, tipcodauxi, flgccosto, monfunc, tipocuenta, tipomoneda, 
		ctaacm, flgcuentacm, estado, flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, ctactbcorp, flgifrs, flgvalida, feccrea, fecultmod, 
		horcrea, horultimod, ultusumod, usucrea, descripctacorp, flgtipres, fecha_proceso, bd_origen, id_error, desc_error 
		from #mplcta1f_inconsistencia						
					
		delete t
		from #mplcta1f_inconsistencia t
		where exists(select 1 from #mplcta1f p where t.compania = p.compania and t.ejercicio = p.ejercicio and t.codcontab = p.codcontab)
		-------------------------------------------------------------------------------
		--limpieza de duplicados
		select * 
		into #mplcta1f_duplicado
		from #mplcta1f a
		where exists (select 1 from #mplcta1f b
						where a.compania = b.compania and a.ejercicio = b.ejercicio and a.codcontab = b.codcontab
						group by compania, ejercicio, codcontab
						having count(*) > 1)
						
		insert into stgdwaje.dbo.rej_mplcta1f(compania, ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza, 
		flgbalance, flgresult, flgrubro, tipoimputa, flganalitic, tipoenlace, cresponsab, tipcodauxi, flgccosto, monfunc, tipocuenta, tipomoneda, 
		ctaacm, flgcuentacm, estado, flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, ctactbcorp, flgifrs, flgvalida, feccrea, fecultmod, 
		horcrea, horultimod, ultusumod, usucrea, descripctacorp, flgtipres, fecha_proceso, bd_origen, id_error, desc_error)
		select compania, ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza, 
		flgbalance, flgresult, flgrubro, tipoimputa, flganalitic, tipoenlace, cresponsab, tipcodauxi, flgccosto, monfunc, tipocuenta, tipomoneda, 
		ctaacm, flgcuentacm, estado, flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, ctactbcorp, flgifrs, flgvalida, feccrea, fecultmod, 
		horcrea, horultimod, ultusumod, usucrea, descripctacorp, flgtipres, fecha_proceso, bd_origen, 2, 'clave duplicada' 
		from #mplcta1f_duplicado
		 
		delete t
		from #mplcta1f_duplicado t
		where exists(select 1 from #mplcta1f p where t.compania = p.compania and t.ejercicio = p.ejercicio and t.codcontab = p.codcontab)

		--------------------------------------------------------------------------------------------		
		/*insertar registro nuevo*/
		insert into m_plan_cuentas (cod_compania, cod_ejercicio, cod_cuenta_contable, descripcion, tipo_cuenta, tipo_moneda, naturaleza, ind_balance, 
		ind_result, flg_ccosto, flg_codauxiliar, tipo_codauxiliar, flg_ifrs, cod_cuenta_contable_corp, descripcion_corp, flg_tipres)
		select compania, ejercicio, codcontab, descripcta, tipocuenta, tipomoneda, naturaleza, flgbalance, flgresult, 
		flgccosto, flgcodauxi, tipcodauxi, flgifrs, ctactbcorp, descripctacorp, flgtipres
		from #mplcta1f t
		where not exists (select 1 from m_plan_cuentas d 
			where d.cod_compania = t.compania and d.cod_ejercicio = t.ejercicio and d.cod_cuenta_contable = t.codcontab)
 																 
		/*actualizar registo*/
		update d 
		set descripcion = m.descripcta, 
		tipo_cuenta = m.tipocuenta, 
		tipo_moneda = m.tipomoneda,
		naturaleza = m.naturaleza, 
		ind_balance = m.flgbalance,
		ind_result =m.flgresult, 
		flg_ccosto = flgccosto,
		flg_codauxiliar = m.flgcodauxi,
		tipo_codauxiliar = m.tipcodauxi, 
		flg_ifrs = m.flgifrs, 
		cod_cuenta_contable_corp = m.ctactbcorp,
		descripcion_corp = m.descripctacorp,
		flg_tipres = m.flgtipres
		from m_plan_cuentas d 
		inner join #mplcta1f m on d.cod_compania = m.compania and d.cod_ejercicio = m.ejercicio and d.cod_cuenta_contable = m.codcontab 
		
		-- acreditando que el procedure no ha tenido error
		execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0
		
		-- truncate staging
		truncate table stgdwaje.dbo.stg_mplcta1f
		
		end
	end
end try 
begin catch 
	execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 1
	execute stgdwaje.dbo.conf_inserta_log_error_bd 
end catch 
end
