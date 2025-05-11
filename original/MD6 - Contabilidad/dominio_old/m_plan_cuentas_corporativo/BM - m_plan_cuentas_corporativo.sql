use [odsdwaje]
go
/****** object:  storedprocedure [dbo].[usp_tran_m_plan_cuentas_corporativo]    script date: 2/27/2025 5:26:19 pm ******/
set ansi_nulls on
go
set quoted_identifier on
go
alter procedure [dbo].[usp_tran_m_plan_cuentas_corporativo] @fechaejecucion varchar(20)=''
	/* 
	sistema        : ajegroup_ssis_ecuador
	nombre         : [usp_tran_m_plan_cuentas_corporativo]
	descripci�n    : insertar registros a m_plan_cuentas_corporativo
	autor          : giancarlo ramirez - vooxell
	fecha creaci�n : 05/07/2019
	*/ 
	as
	begin 
	declare @nombretabla varchar(50)
	declare @valorvalidacion int 
	set @nombretabla = 'm_plan_cuentas_corporativo' 
	begin try 
		exec stgdwaje.dbo.conf_validar_tabla @nombretabla, @fechaejecucion, @valorvalidacion output 
		if @valorvalidacion = 1
		begin 
		
		select ejercicio, codcontab, descripcta, ctaabono,  ctacargo, flgcodauxi, flgctautom, naturaleza, flgbalance, flgresult, 
		flgrubro, flganalitic, tipoenlace, tipcodauxi, flgccosto, monfunc, tipomoneda, flgundneg, flgsku, flgmarca, flgcateg, 
		flgcanal, flgorden, estado, feccrea, horcrea, usucrea, fecultmod, horultimod, ultusumod, fecha_proceso, bd_origen
		into #mpccor1f
		from stgdwaje.dbo.stg_mpccor1f
	 
		if @@rowcount = 0
		begin
			insert into stgdwaje.dbo.rej_mpccor1f(id_error, fecha_proceso, desc_error) 
			values (3, @fechaejecucion, 'stage sin data')
			execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0
		end 
		else
		begin 
			--validar bd origen vacia
			insert into stgdwaje.dbo.rej_mpccor1f(id_error, fecha_proceso, bd_origen, desc_error)
			select valor= 3, fecha_ejecucion= @fechaejecucion, a.descripcion, 'bd: ' + a.descripcion + ' vacia'
			from stgdwaje.dbo.conf_tabla_dominio a 
			where id_grupo = 2 and id_orden > 0 and valor2 = 'activo' 
			and not exists (select 1 from #mpccor1f b where a.descripcion = b.bd_origen )
		
			select ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza,
			flgbalance, flgresult, flgrubro, flganalitic, tipoenlace, tipcodauxi, flgccosto, monfunc, tipomoneda,
			flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, estado, feccrea, horcrea, usucrea, fecultmod,
			horultimod, ultusumod, fecha_proceso, bd_origen,
			id_error = 9,
			desc_error = case when flgbalance <> '' and not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'balance_cuenta' and m.valor = a.flgbalance)
							then 'no existe flgbalance en tabla dominio (balance_cuenta)' 
							when not exists (select 1 from stgdwaje ..conf_maestra_dominio m where tipo = 'tipo_moneda' and m.valor = a.tipomoneda)
							then 'no existe tipomoneda en tabla dominio (tipo_moneda)' 
							when flgresult <> '' and not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'tipo_resultado' and m.valor = a.flgresult)
							then 'no existe flgresult en tabla dominio (tipo_resultado)' 
							when flganalitic = 0x54 and not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'naturaleza_cuenta' and m.valor = a.naturaleza)
							then 'no existe naturaleza en tabla dominio (naturaleza_cuenta)' 
							when flgcodauxi = 0x54 and not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'tipo_codauxiliar' and m.valor = a.tipcodauxi)
							then 'no existe tipcodauxi en tabla dominio (tipo_codauxiliar)' 																
			end
			into #mpccor1f_inconsistencia
			from #mpccor1f a
			where (flgbalance <> '' and not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'balance_cuenta' and m.valor = a.flgbalance))
			or not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'tipo_moneda' and m.valor = a.tipomoneda)
			or (flgresult <> '' and not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'tipo_resultado' and m.valor = a.flgresult))
			or (flganalitic = 0x54 and not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'naturaleza_cuenta' and m.valor = a.naturaleza))				
			or (flgcodauxi = 0x54 and not exists (select 1 from stgdwaje.dbo.conf_maestra_dominio m where tipo = 'tipo_codauxiliar' and m.valor = a.tipcodauxi))

			insert into stgdwaje.dbo.rej_mpccor1f(ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza,
			flgbalance, flgresult, flgrubro, flganalitic, tipoenlace, tipcodauxi, flgccosto, monfunc, tipomoneda,
			flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, estado, feccrea, horcrea, usucrea, fecultmod,
			horultimod, ultusumod, fecha_proceso, bd_origen, id_error, desc_error)
			select ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza,
			flgbalance, flgresult, flgrubro, flganalitic, tipoenlace, tipcodauxi, flgccosto, monfunc, tipomoneda,
			flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, estado, feccrea, horcrea, usucrea, fecultmod,
			horultimod, ultusumod, fecha_proceso, bd_origen, id_error, desc_error
			from #mpccor1f_inconsistencia
						
			delete m 
			from #mpccor1f m
			where exists(select 1 from #mpccor1f_inconsistencia t where m.ejercicio = t.ejercicio and m.codcontab = t.codcontab)			
			  
			select * 
			into #mpccor1f_duplicado 
			from #mpccor1f a
			where exists (select 1 from #mpccor1f b where a.ejercicio = b.ejercicio and a.codcontab = b.codcontab
			group by ejercicio, codcontab
			having count(*) > 1)
			
			insert into stgdwaje.dbo.rej_mpccor1f(ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza,
			flgbalance, flgresult, flgrubro, flganalitic, tipoenlace, tipcodauxi, flgccosto, monfunc, tipomoneda,
			flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, estado, feccrea, horcrea, usucrea, fecultmod,
			horultimod, ultusumod, fecha_proceso, bd_origen, id_error, desc_error)
			select ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, flgctautom, naturaleza,
			flgbalance, flgresult, flgrubro, flganalitic, tipoenlace, tipcodauxi, flgccosto, monfunc, tipomoneda,
			flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, estado, feccrea, horcrea, usucrea, fecultmod,
			horultimod, ultusumod, fecha_proceso, bd_origen, 2, 'clave duplicada'
			from #mpccor1f_duplicado
							
			delete m 
			from #mpccor1f m
			where exists(select 1 from #mpccor1f_duplicado t where m.ejercicio = t.ejercicio and m.codcontab = t.codcontab)	
	 
			update d 
			set descripcion = m.descripcta, 
			cod_cuenta_abono = m.ctaabono, 
			cod_cuenta_cargo= m.ctacargo,
			flg_codauxiliar = m.flgcodauxi, 
			naturaleza = m.naturaleza, 
			ind_balance = m.flgbalance, 
			ind_result = m.flgresult, 
			ind_rubro = m.flgrubro, 
			flg_analitic = m.flganalitic, 
			tipo_enlace = m.tipoenlace, 
			tipo_codauxiliar = m.tipcodauxi, 
			flg_ccosto = m.flgccosto, 
			tipo_moneda = m.tipomoneda, 
			flg_unidad_negocio = m.flgundneg, 
			flg_sku = m.flgsku, 
			flg_marca = m.flgmarca, 
			flg_categoria = m.flgcateg, 
			flg_canal = m.flgcanal, 
			flg_orden = m.flgorden, 
			estado = m.estado
			from dbo.m_plan_cuentas_corporativo d 
			inner join #mpccor1f m on d.cod_ejercicio = m.ejercicio and d.cod_cuenta_contable_corporativa = m.codcontab 

			insert dbo.m_plan_cuentas_corporativo (cod_ejercicio, cod_cuenta_contable_corporativa, descripcion, cod_cuenta_abono, cod_cuenta_cargo, flg_codauxiliar, naturaleza,
			ind_balance, ind_result, ind_rubro, flg_analitic, tipo_enlace, tipo_codauxiliar, flg_ccosto, tipo_moneda, flg_unidad_negocio, flg_sku, flg_marca, flg_categoria, 
			flg_canal, flg_orden, estado, fecha_creacion, hora_creacion, usuario_creacion, fecha_ultima_modificacion, hora_ultima_modificacion, usuario_ultima_modificacion)
			select ejercicio, codcontab, descripcta, ctaabono, ctacargo, flgcodauxi, naturaleza, flgbalance, flgresult, flgrubro, flganalitic, tipoenlace, tipcodauxi, 
			flgccosto, tipomoneda, flgundneg, flgsku, flgmarca, flgcateg, flgcanal, flgorden, estado, feccrea, horcrea, usucrea, fecultmod, horultimod, ultusumod
			from #mpccor1f t
			where not exists (select 1 from dbo.m_plan_cuentas_corporativo d where d.cod_ejercicio = t.ejercicio and d.cod_cuenta_contable_corporativa = t.codcontab) 
		 
			execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 0 
		 
			--truncate table stgdwaje.dbo.stg_mpccor1f 
		end
		end
	end try
	begin catch	
		execute stgdwaje.dbo.conf_actualizar_estado_matriz @nombretabla, @fechaejecucion, 1
		execute stgdwaje.dbo.conf_inserta_log_error_bd	
	end catch
end
